package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

// Each claiming test uses its own schema

// Existing development notifications are never selected

func setupClaimTest(
	t *testing.T,
) (*storage.NotificationRepository, *pgxpool.Pool) {
	t.Helper()

	if os.Getenv("RUN_DB_TESTS") != "1" {
		t.Skip("database integration tests are not enabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	admin, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}

	schema := "claim_test_" +
		strings.ReplaceAll(uuid.NewString(), "-", "")

	schemaSQL := pgx.Identifier{schema}.Sanitize()

	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schemaSQL)
	if err != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", err)
	}

	var pool *pgxpool.Pool

	t.Cleanup(func() {
		if pool != nil {
			pool.Close()
		}

		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 10*time.Second,
		)
		defer cleanupCancel()

		_, err := admin.Exec(
			cleanupCtx,
			"DROP SCHEMA "+schemaSQL+" CASCADE",
		)
		if err != nil {
			t.Errorf("drop test schema: %v", err)
		}

		admin.Close()
	})

	// Copying columns, constraints, generated fields, defaults, and indexes from the production table
	_, err = admin.Exec(ctx,
		"CREATE TABLE "+schemaSQL+
			".notifications (LIKE public.notifications INCLUDING ALL)",
	)
	if err != nil {
		t.Fatalf("create isolated notifications table: %v", err)
	}

	_, err = admin.Exec(ctx,
		"CREATE TABLE "+schemaSQL+
			".delivery_attempts (LIKE public.delivery_attempts INCLUDING ALL)",
	)
	if err != nil {
		t.Fatalf("create isolated delivery attempts table: %v", err)
	}

	_, err = admin.Exec(ctx,
		"ALTER TABLE "+schemaSQL+
			".delivery_attempts "+
			"ADD CONSTRAINT fk_test_notification "+
			"FOREIGN KEY (notification_id) REFERENCES "+
			schemaSQL+".notifications(id)",
	)
	if err != nil {
		t.Fatalf("add isolated attempt foreign key: %v", err)
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("parse database configuration: %v", err)
	}

	// Every connection used by this pool looks up notifications in our isolated schema
	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams =
			make(map[string]string)
	}

	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	poolConfig.MaxConns = 16

	pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("open isolated pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping isolated database: %v", err)
	}

	return storage.NewNotificationRepository(pool), pool
}

// Inserting a notification with a controlled status and eligibility timestamp
func insertClaimTestNotification(
	t *testing.T,
	ctx context.Context,
	repo *storage.NotificationRepository,
	pool *pgxpool.Pool,
	priority notification.Priority,
	status notification.Status,
	nextAttemptAt time.Time,
) string {
	t.Helper()

	created, err := repo.Create(
		ctx,
		notification.CreateRequest{
			Recipient: uuid.NewString() + "@example.com",
			Channel:   notification.ChannelEmail,
			Content:   "Worker claiming test",
			Priority:  priority,
		},
	)
	if err != nil {
		t.Fatalf("create notification: %v", err)
	}

	_, err = pool.Exec(ctx, `
		UPDATE notifications
		SET status = $1,
		    next_attempt_at = $2
		WHERE id = $3::uuid
	`, string(status), nextAttemptAt, created.ID)

	if err != nil {
		t.Fatalf("prepare notification: %v", err)
	}

	return created.ID
}

func TestClaimReadyEligibilityIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second,
	)
	defer cancel()

	past := time.Now().Add(-10 * time.Minute)
	future := time.Now().Add(time.Hour)

	// These three notifications should be claimed
	highID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		past,
	)

	normalID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityNormal,
		notification.StatusRetrying,
		past,
	)

	lowID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityLow,
		notification.StatusPending,
		past,
	)

	// Future notifications must be ignored
	futureID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		future,
	)

	// Cancelled notifications must be ignored
	cancelledID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		past,
	)

	if _, err := repo.Cancel(ctx, cancelledID); err != nil {
		t.Fatalf("cancel test notification: %v", err)
	}

	// Notifications that exhausted their attempt limits must not be claimed
	exhaustedID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		past,
	)

	_, err := pool.Exec(ctx, `
		UPDATE notifications
		SET attempt_count = max_attempts
		WHERE id = $1::uuid
	`, exhaustedID)
	if err != nil {
		t.Fatalf("exhaust test notification: %v", err)
	}

	t.Run("invalid claiming parameters", func(t *testing.T) {
		tests := []struct {
			worker string
			limit  int
			lease  time.Duration
		}{
			{"", 1, 30 * time.Second},
			{"worker-a", 0, 30 * time.Second},
			{"worker-a", 101, 30 * time.Second},
			{"worker-a", 1, 0},
		}

		for _, tt := range tests {
			if _, err := repo.ClaimReady(
				ctx, tt.worker, tt.limit, tt.lease,
			); err == nil {
				t.Error("expected parameter validation error")
			}
		}
	})

	t.Run("high priority claimed first", func(t *testing.T) {
		result, err := repo.ClaimReady(
			ctx, "worker-A", 2, 30*time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(result) != 2 {
			t.Fatalf("expected 2 claims, got %d", len(result))
		}

		if result[0].ID != highID {
			t.Errorf("expected high-priority notification first")
		}

		if result[1].ID != normalID {
			t.Errorf("expected normal-priority notification second")
		}

		for _, item := range result {
			if item.Status != notification.StatusProcessing {
				t.Errorf("expected processing, got %s", item.Status)
			}

			if item.WorkerID != "worker-A" {
				t.Errorf("wrong worker ID: %s", item.WorkerID)
			}

			if !item.LeaseExpiresAt.After(
				time.Now().Add(15 * time.Second),
			) {
				t.Error("claim lease was not set correctly")
			}

			if item.AttemptCount != 0 {
				t.Error("claiming should not increment attempt_count")
			}
		}
	})

	t.Run("claimed rows are not claimed again", func(t *testing.T) {
		result, err := repo.ClaimReady(
			ctx, "worker-B", 10, 30*time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(result) != 1 || result[0].ID != lowID {
			t.Fatalf("expected only remaining low-priority job, got %+v",
				result)
		}

		empty, err := repo.ClaimReady(
			ctx, "worker-C", 10, 30*time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(empty) != 0 {
			t.Errorf("expected empty queue, got %d claims", len(empty))
		}
	})

	t.Run("ineligible notifications stay unchanged", func(t *testing.T) {
		expected := map[string]notification.Status{
			futureID:    notification.StatusPending,
			cancelledID: notification.StatusCancelled,
			exhaustedID: notification.StatusPending,
		}

		for id, status := range expected {
			stored, err := repo.GetByID(ctx, id)
			if err != nil {
				t.Fatal(err)
			}

			if stored.Status != status {
				t.Errorf(
					"notification %s: expected %s, got %s",
					id, status, stored.Status,
				)
			}
		}
	})
}

// Test for worker to claim notifications
func TestConcurrentClaimReadyIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	const totalNotifications = 24
	const workerCount = 8
	const claimLimit = 3

	// Populating the isolated queue
	expectedIDs := make(map[string]bool)

	for i := 0; i < totalNotifications; i++ {
		id := insertClaimTestNotification(
			t, ctx, repo, pool,
			notification.PriorityNormal,
			notification.StatusPending,
			time.Now().Add(-time.Minute),
		)
		expectedIDs[id] = true
	}

	type workerResult struct {
		items  []notification.ClaimedNotification
		err    error
		worker string
	}

	results := make(chan workerResult, workerCount)
	start := make(chan struct{})

	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			workerID := fmt.Sprintf("worker-%d", index)

			<-start

			items, err := repo.ClaimReady(
				ctx,
				workerID,
				claimLimit,
				30*time.Second,
			)

			results <- workerResult{
				items:  items,
				err:    err,
				worker: workerID,
			}
		}(i)
	}

	// All workers start claiming concurrently
	close(start)

	wg.Wait()
	close(results)

	seen := make(map[string]bool)

	for result := range results {
		if result.err != nil {
			t.Errorf(
				"%s claim failed: %v",
				result.worker, result.err,
			)
			continue
		}

		for _, item := range result.items {
			if !expectedIDs[item.ID] {
				t.Errorf("unexpected notification %s", item.ID)
			}

			if seen[item.ID] {
				t.Errorf(
					"notification %s claimed more than once",
					item.ID,
				)
			}
			seen[item.ID] = true

			if item.WorkerID != result.worker {
				t.Errorf("notification assigned to wrong worker")
			}

			if item.Status != notification.StatusProcessing {
				t.Errorf("unexpected status: %s", item.Status)
			}
		}
	}

	if len(seen) != totalNotifications {
		t.Errorf(
			"expected %d unique claims, got %d",
			totalNotifications, len(seen),
		)
	}

	var processingCount int

	err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM notifications
		WHERE status = 'processing'
	`).Scan(&processingCount)

	if err != nil {
		t.Fatal(err)
	}

	if processingCount != totalNotifications {
		t.Errorf(
			"expected %d processing records, got %d",
			totalNotifications, processingCount,
		)
	}
}

// This test connects cancellation with claiming
func TestCancellationClaimRaceIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("race_%d", i), func(t *testing.T) {
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

			start := make(chan struct{})
			var wg sync.WaitGroup

			var claimed []notification.ClaimedNotification
			var claimErr error
			var cancelErr error

			wg.Add(2)

			go func() {
				defer wg.Done()
				<-start

				claimed, claimErr = repo.ClaimReady(
					ctx,
					"race-worker",
					1,
					30*time.Second,
				)
			}()

			go func() {
				defer wg.Done()
				<-start

				_, cancelErr = repo.Cancel(ctx, id)
			}()

			close(start)
			wg.Wait()

			if claimErr != nil {
				t.Fatalf("claim failed: %v", claimErr)
			}

			if len(claimed) > 1 {
				t.Fatalf("expected at most one claim")
			}

			if len(claimed) == 1 {
				// Worker won: cancellation must fail.
				if claimed[0].ID != id {
					t.Fatal("claimed unexpected notification")
				}

				if !errors.Is(
					cancelErr,
					storage.ErrNotificationNotCancellable,
				) {
					t.Errorf(
						"expected cancellation conflict, got %v",
						cancelErr,
					)
				}
			} else {
				// Cancellation won: worker cannot claim it.
				if cancelErr != nil {
					t.Errorf(
						"expected successful cancellation, got %v",
						cancelErr,
					)
				}
			}

			stored, err := repo.GetByID(ctx, id)
			if err != nil {
				t.Fatal(err)
			}

			if len(claimed) == 1 &&
				stored.Status != notification.StatusProcessing {
				t.Errorf(
					"worker won but status is %s",
					stored.Status,
				)
			}

			if len(claimed) == 0 &&
				stored.Status != notification.StatusCancelled {
				t.Errorf(
					"cancellation won but status is %s",
					stored.Status,
				)
			}
		})
	}
}

// This test makes the locking behaviour easy to see
func TestClaimReadySkipsLockedRowsIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer cancel()

	past := time.Now().Add(-time.Minute)

	highID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		past,
	)

	lowID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityLow,
		notification.StatusPending,
		past,
	)

	// Holding a database row lock on the high-priority job
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())

	var lockedID string

	err = tx.QueryRow(ctx, `
		SELECT id::text
		FROM notifications
		WHERE id = $1::uuid
		FOR UPDATE
	`, highID).Scan(&lockedID)

	if err != nil {
		t.Fatal(err)
	}

	// Another connection should skip the locked row and claim the available low-priority job
	claimed, err := repo.ClaimReady(
		ctx, "skip-locked-worker", 1, 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(claimed) != 1 || claimed[0].ID != lowID {
		t.Fatalf("expected unlocked low-priority job, got %+v",
			claimed)
	}

	// Releasing the high-priority row
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// The high-priority job is now claimable
	remaining, err := repo.ClaimReady(
		ctx, "second-worker", 1, 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(remaining) != 1 || remaining[0].ID != highID {
		t.Fatalf("expected previously locked job, got %+v",
			remaining)
	}
}
