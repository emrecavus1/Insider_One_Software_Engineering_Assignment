package storage_test

import (
	"context"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/ratelimit"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/worker"
)

func setupRateLimitRedis(
	t *testing.T,
	limit int,
) *ratelimit.RedisLimiter {
	t.Helper()

	if os.Getenv("RUN_REDIS_TESTS") != "1" {
		t.Skip("Redis integration tests are disabled")
	}

	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "16379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(host, port),
	})

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("connect to Redis: %v", err)
	}

	namespace := "insider-one:test:" + uuid.NewString()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 3*time.Second,
		)
		defer cleanupCancel()

		_ = client.Del(
			cleanupCtx,
			namespace+":sms",
			namespace+":email",
			namespace+":push",
		).Err()

		_ = client.Close()
	})

	limiter, err := ratelimit.NewRedisLimiter(
		client, namespace, limit,
	)
	if err != nil {
		t.Fatal(err)
	}

	return limiter
}

func TestRateLimitedWorkerPipelineIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)
	limiter := setupRateLimitRedis(t, 1)

	ctx, cancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer cancel()

	firstID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityNormal,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	secondID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityNormal,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	var sends atomic.Int32

	sender := worker.SenderFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) (notification.DeliveryResult, error) {
			sends.Add(1)

			return notification.DeliveryResult{
				Outcome:           notification.OutcomeAccepted,
				HTTPStatus:        202,
				ProviderMessageID: "test-" + job.ID,
			}, nil
		},
	)

	processor, err := worker.NewRateLimitedDeliveryProcessor(
		repo, sender, limiter, 5*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := repo.ClaimReady(
		ctx, "rate-worker", 2, 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs) != 2 {
		t.Fatalf("expected two claims, got %d", len(jobs))
	}

	if err := processor.Process(ctx, jobs[0]); err != nil {
		t.Fatalf("first delivery: %v", err)
	}

	if err := processor.Process(ctx, jobs[1]); err != nil {
		t.Fatalf("rate-limited deferral: %v", err)
	}

	accepted, err := repo.GetByID(ctx, jobs[0].ID)
	if err != nil {
		t.Fatal(err)
	}

	deferred, err := repo.GetByID(ctx, jobs[1].ID)
	if err != nil {
		t.Fatal(err)
	}

	if accepted.Status != notification.StatusAccepted {
		t.Errorf("first notification should be accepted")
	}

	if deferred.Status != notification.StatusRetrying {
		t.Errorf("second notification should be retrying")
	}

	if sends.Load() != 1 {
		t.Errorf("expected one send, got %d", sends.Load())
	}

	var attempts, auditRows int

	err = pool.QueryRow(ctx, `
		SELECT
			n.attempt_count,
			(
				SELECT COUNT(*)
				FROM delivery_attempts
				WHERE notification_id = n.id
			)
		FROM notifications AS n
		WHERE n.id = $1::uuid
	`, deferred.ID).Scan(&attempts, &auditRows)

	if err != nil {
		t.Fatal(err)
	}

	if attempts != 0 || auditRows != 0 {
		t.Errorf(
			"rate limiting consumed an attempt: count=%d audit=%d",
			attempts, auditRows,
		)
	}

	time.Sleep(1100 * time.Millisecond)

	remaining, err := repo.ClaimReady(
		ctx, "second-rate-worker", 1, 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(remaining) != 1 ||
		remaining[0].ID != deferred.ID {
		t.Fatalf("expected deferred notification, got %+v", remaining)
	}

	if err := processor.Process(ctx, remaining[0]); err != nil {
		t.Fatalf("delivery after capacity recovery: %v", err)
	}

	if sends.Load() != 2 {
		t.Errorf("expected two total sends, got %d", sends.Load())
	}

	for _, id := range []string{firstID, secondID} {
		item, err := repo.GetByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}

		if item.Status != notification.StatusAccepted {
			t.Errorf("notification %s not accepted", id)
		}
	}

	var acceptedAttempts int

	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM delivery_attempts
		WHERE outcome = 'accepted'
	`).Scan(&acceptedAttempts)

	if err != nil {
		t.Fatal(err)
	}

	if acceptedAttempts != 2 {
		t.Errorf(
			"expected two accepted attempts, got %d",
			acceptedAttempts,
		)
	}
}

func TestRedisOutageDoesNotSendIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	id := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	jobs, err := repo.ClaimReady(
		ctx, "outage-worker", 1, 30*time.Second,
	)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim failed: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:1",
	})
	defer redisClient.Close()

	limiter, err := ratelimit.NewRedisLimiter(
		redisClient, "insider-one:test:outage", 100,
	)
	if err != nil {
		t.Fatal(err)
	}

	var sends atomic.Int32

	sender := worker.SenderFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) (notification.DeliveryResult, error) {
			sends.Add(1)
			return notification.DeliveryResult{
				Outcome: notification.OutcomeAccepted,
			}, nil
		},
	)

	processor, err := worker.NewRateLimitedDeliveryProcessor(
		repo, sender, limiter, 5*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := processor.Process(ctx, jobs[0]); err == nil {
		t.Fatal("expected Redis connection error")
	}

	if sends.Load() != 0 {
		t.Error("sender ran while Redis was unavailable")
	}

	var status string
	var attemptCount int
	var unowned bool

	err = pool.QueryRow(ctx, `
		SELECT
			status,
			attempt_count,
			claim_token IS NULL AND worker_id IS NULL
		FROM notifications
		WHERE id = $1::uuid
	`, id).Scan(&status, &attemptCount, &unowned)

	if err != nil {
		t.Fatal(err)
	}

	if status != "retrying" || attemptCount != 0 || !unowned {
		t.Errorf(
			"unexpected state: status=%s attempts=%d unowned=%v",
			status, attemptCount, unowned,
		)
	}
}

func TestClaimDeferralFencingIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	id := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityNormal,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	first, err := repo.ClaimReady(
		ctx, "first-worker", 1, 30*time.Second,
	)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim failed: %v", err)
	}

	err = repo.DeferClaim(
		ctx, first[0], time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = repo.DeferClaim(
		ctx, first[0], time.Millisecond,
	)
	if !errors.Is(err, storage.ErrClaimNotOwned) {
		t.Errorf("stale deferral should be rejected: %v", err)
	}

	_, err = pool.Exec(ctx, `
		UPDATE notifications
		SET next_attempt_at = NOW() - INTERVAL '1 second'
		WHERE id = $1::uuid
	`, id)
	if err != nil {
		t.Fatal(err)
	}

	second, err := repo.ClaimReady(
		ctx, "second-worker", 1, 30*time.Second,
	)
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim failed: %v", err)
	}

	if first[0].ClaimToken == second[0].ClaimToken {
		t.Fatal("reclaimed notification reused its claim token")
	}

	attempt, err := repo.BeginDeliveryAttempt(
		ctx, second[0],
	)
	if err != nil || attempt != 1 {
		t.Fatalf("begin attempt failed: %v", err)
	}

	err = repo.DeferClaim(
		ctx, second[0], time.Second,
	)

	if !errors.Is(err, storage.ErrClaimNotOwned) {
		t.Errorf(
			"claim with active delivery attempt must not be deferred: %v",
			err,
		)
	}

	var attemptCount int

	err = pool.QueryRow(ctx, `
		SELECT attempt_count
		FROM notifications
		WHERE id = $1::uuid
	`, id).Scan(&attemptCount)

	if err != nil {
		t.Fatal(err)
	}

	if attemptCount != 1 {
		t.Errorf("expected one recorded attempt, got %d", attemptCount)
	}
}
