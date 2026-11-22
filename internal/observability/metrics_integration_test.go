package observability

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func setupMetricsTest(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if os.Getenv("RUN_DB_TESTS") != "1" {
		t.Skip("database integration tests are disabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	admin, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}

	schema := "metrics_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}

	var pool *pgxpool.Pool
	t.Cleanup(func() {
		if pool != nil {
			pool.Close()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop isolated metrics schema: %v", err)
		}
		admin.Close()
	})

	for _, table := range []string{"notifications", "delivery_attempts"} {
		_, err := admin.Exec(ctx, "CREATE TABLE "+identifier+"."+table+
			" (LIKE public."+table+" INCLUDING ALL)")
		if err != nil {
			t.Fatalf("create isolated %s table: %v", table, err)
		}
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err = pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestMetricsSnapshotIntegration(t *testing.T) {
	pool := setupMetricsTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows := []struct {
		channel, status, timing string
		attempts                int
	}{
		{"sms", "pending", "ready", 0},
		{"email", "retrying", "ready", 1},
		{"push", "processing", "ready", 1},
		{"sms", "pending", "scheduled", 0},
		{"push", "pending", "blocked", 4},
		{"email", "accepted", "ready", 1},
		{"push", "failed", "ready", 1},
	}

	var ids []string
	for _, item := range rows {
		var id string
		err := pool.QueryRow(ctx, `
			INSERT INTO notifications (
				recipient, channel, content, priority, status, attempt_count,
				next_attempt_at, created_at, accepted_at
			) VALUES (
				'example@example.com', $1, 'metrics test', 'normal', $2, $3,
				CASE WHEN $4 = 'scheduled' THEN NOW() + INTERVAL '10 minutes'
				ELSE NOW() - INTERVAL '1 minute' END,
				NOW() - INTERVAL '30 seconds',
				CASE WHEN $2 = 'accepted' THEN NOW() - INTERVAL '5 seconds'
				ELSE NULL END
			)
			RETURNING id::text
		`, item.channel, item.status, item.attempts, item.timing).Scan(&id)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	outcomes := []struct {
		id      int
		outcome string
	}{
		{5, "accepted"},
		{1, "retryable_failure"},
		{6, "permanent_failure"},
		{0, "unknown"},
	}
	for _, item := range outcomes {
		_, err := pool.Exec(ctx, `
			INSERT INTO delivery_attempts (
				notification_id, attempt_number, outcome, started_at, finished_at
			) VALUES (
				$1::uuid, 1, $2, NOW() - INTERVAL '7 seconds',
				NOW() - INTERVAL '6 seconds'
			)
		`, ids[item.id], item.outcome)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO delivery_attempts (notification_id, attempt_number, outcome)
		VALUES ($1::uuid, 1, 'started')
	`, ids[2])
	if err != nil {
		t.Fatal(err)
	}

	repo := NewMetricsRepository(pool)
	first, err := repo.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Queue.Depth != 5 || first.Queue.Ready != 2 ||
		first.Queue.Scheduled != 1 || first.Queue.Blocked != 1 ||
		first.Queue.Processing != 1 {
		t.Errorf("unexpected queue metrics: %+v", first.Queue)
	}
	if first.Queue.ByChannel["sms"].Pending != 2 ||
		first.Queue.ByChannel["push"].Blocked != 1 {
		t.Errorf("unexpected channel breakdown: %+v", first.Queue.ByChannel)
	}

	d := first.Delivery
	if d.CompletedAttempts != 4 || d.Accepted != 1 || d.RetryableFailures != 1 ||
		d.PermanentFailures != 1 || d.Unknown != 1 ||
		d.SuccessRate != 0.25 || d.FailureRate != 0.5 || d.UnknownRate != 0.25 {
		t.Errorf("unexpected delivery statistics: %+v", d)
	}
	if d.AttemptLatencyMs.P50Ms < 900 || d.AttemptLatencyMs.P50Ms > 1100 ||
		d.EndToEndLatencyMs.AverageMs < 20000 {
		t.Errorf("unexpected delivery latencies: %+v", d)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO notifications (recipient, channel, content, priority)
		VALUES ('example@example.com', 'sms', 'new job', 'normal')
	`)
	if err != nil {
		t.Fatal(err)
	}

	cached, err := repo.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cached.Queue.Depth != first.Queue.Depth {
		t.Error("metrics cache was not reused")
	}

	fresh, err := NewMetricsRepository(pool).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Queue.Depth != first.Queue.Depth+1 {
		t.Errorf("fresh metrics snapshot did not see new job: %d", fresh.Queue.Depth)
	}
}
