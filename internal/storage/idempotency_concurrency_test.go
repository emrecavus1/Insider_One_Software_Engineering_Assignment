package storage_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

const concurrentRequests = 20

type concurrentIdempotencyResult struct {
	notification notification.Notification
	replayed     bool
	err          error
}

// Preparing a real PostgreSQL connection for concurrency tests
func setupConcurrentIdempotencyTest(
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

	pool, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}

	// Parent cleanup runs after the subtests finish
	t.Cleanup(func() {
		pool.Close()
	})

	return storage.NewNotificationRepository(pool), pool
}

// Removing records created by a test, keeping the database clean
func registerConcurrentCleanup(
	t *testing.T,
	pool *pgxpool.Pool,
	key string,
	recipient string,
) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()

		// Removing the referencing idempotency record first
		if _, err := pool.Exec(
			ctx,
			"DELETE FROM idempotency_keys WHERE key = $1",
			key,
		); err != nil {
			t.Errorf("cleanup idempotency key: %v", err)
		}

		if _, err := pool.Exec(
			ctx,
			"DELETE FROM notifications WHERE recipient = $1",
			recipient,
		); err != nil {
			t.Errorf("cleanup notifications: %v", err)
		}
	})
}

// Releasing all goroutines together and collect their results
// where each goroutine writes to its own slice index

func runConcurrentIdempotencyRequests(
	ctx context.Context,
	repo *storage.NotificationRepository,
	key string,
	requests []notification.CreateRequest,
) []concurrentIdempotencyResult {
	results := make([]concurrentIdempotencyResult, len(requests))

	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := range requests {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			// Waiting until every goroutine has been started
			<-start

			result, replayed, err := repo.CreateIdempotent(
				ctx,
				requests[index],
				key,
			)

			results[index] = concurrentIdempotencyResult{
				notification: result,
				replayed:     replayed,
				err:          err,
			}
		}(i)
	}

	// Releasing all waiting goroutines
	close(start)

	wg.Wait()
	return results
}

// Verifying persistence independently of repository return values
func assertOneIdempotentNotification(
	t *testing.T,
	pool *pgxpool.Pool,
	key string,
	recipient string,
	expectedID string,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	var notificationCount int

	err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM notifications
		WHERE recipient = $1
	`, recipient).Scan(&notificationCount)

	if err != nil {
		t.Fatalf("count notifications: %v", err)
	}

	if notificationCount != 1 {
		t.Errorf(
			"expected exactly 1 notification, got %d",
			notificationCount,
		)
	}

	var keyCount int

	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM idempotency_keys
		WHERE key = $1
	`, key).Scan(&keyCount)

	if err != nil {
		t.Fatalf("count idempotency keys: %v", err)
	}

	if keyCount != 1 {
		t.Errorf(
			"expected exactly 1 idempotency record, got %d",
			keyCount,
		)
	}

	var storedID string

	err = pool.QueryRow(ctx, `
		SELECT notification_id::text
		FROM idempotency_keys
		WHERE key = $1
	`, key).Scan(&storedID)

	if err != nil {
		t.Fatalf("retrieve idempotency record: %v", err)
	}

	if storedID != expectedID {
		t.Errorf(
			"expected stored notification ID %s, got %s",
			expectedID,
			storedID,
		)
	}
}

func TestConcurrentIdempotencyIntegration(t *testing.T) {
	repo, pool := setupConcurrentIdempotencyTest(t)

	t.Run("20 identical requests create one notification", func(t *testing.T) {
		key := "concurrent-same-" + uuid.NewString()
		recipient := "same-" + uuid.NewString() + "@example.com"

		registerConcurrentCleanup(t, pool, key, recipient)

		request := notification.CreateRequest{
			Recipient: recipient,
			Channel:   notification.ChannelEmail,
			Content:   "Your order has shipped!",
			Priority:  notification.PriorityHigh,
		}

		requests := make([]notification.CreateRequest, concurrentRequests)

		for i := range requests {
			requests[i] = request
		}

		ctx, cancel := context.WithTimeout(
			context.Background(), 30*time.Second,
		)
		defer cancel()

		results := runConcurrentIdempotencyRequests(
			ctx, repo, key, requests,
		)

		var firstID string
		createdCount := 0
		replayCount := 0

		for i, result := range results {
			if result.err != nil {
				t.Errorf("request %d failed: %v", i, result.err)
				continue
			}

			if firstID == "" {
				firstID = result.notification.ID
			}

			if result.notification.ID != firstID {
				t.Errorf(
					"request %d returned different ID: %s",
					i,
					result.notification.ID,
				)
			}

			if result.replayed {
				replayCount++
			} else {
				createdCount++
			}
		}

		if createdCount != 1 {
			t.Errorf(
				"expected 1 newly created notification, got %d",
				createdCount,
			)
		}

		if replayCount != concurrentRequests-1 {
			t.Errorf(
				"expected %d replays, got %d",
				concurrentRequests-1,
				replayCount,
			)
		}

		if firstID == "" {
			t.Fatal("no notification ID returned")
		}

		assertOneIdempotentNotification(
			t, pool, key, recipient, firstID,
		)
	})

	t.Run("same key with different payloads causes conflicts", func(t *testing.T) {
		key := "concurrent-conflict-" + uuid.NewString()
		recipient := "mixed-" + uuid.NewString() + "@example.com"

		registerConcurrentCleanup(t, pool, key, recipient)

		requests := make([]notification.CreateRequest, concurrentRequests)

		// Ten requests carry payload A, ten carry payload B, both groups use the same idempotency key
		for i := range requests {
			content := "Payload A"

			if i >= concurrentRequests/2 {
				content = "Payload B"
			}

			requests[i] = notification.CreateRequest{
				Recipient: recipient,
				Channel:   notification.ChannelEmail,
				Content:   content,
				Priority:  notification.PriorityNormal,
			}
		}

		ctx, cancel := context.WithTimeout(
			context.Background(), 30*time.Second,
		)
		defer cancel()

		results := runConcurrentIdempotencyRequests(
			ctx, repo, key, requests,
		)

		var winnerID string
		var winnerContent string
		createdCount := 0

		// Identifying which payload won the initial insert
		for i, result := range results {
			if result.err == nil && !result.replayed {
				createdCount++
				winnerID = result.notification.ID
				winnerContent = requests[i].Content
			}
		}

		if createdCount != 1 || winnerID == "" {
			t.Fatalf(
				"expected exactly one successful creation, got %d",
				createdCount,
			)
		}

		replayCount := 0
		conflictCount := 0

		for i, result := range results {
			if requests[i].Content == winnerContent {
				// All requests matching the winning payload must succeed and return the same ID
				if result.err != nil {
					t.Errorf(
						"request %d should succeed: %v",
						i,
						result.err,
					)
					continue
				}

				if result.notification.ID != winnerID {
					t.Errorf(
						"request %d returned unexpected ID",
						i,
					)
				}

				if result.replayed {
					replayCount++
				}
			} else {
				// Every request with the losing payload must receive an idempotency conflict
				if errors.Is(
					result.err,
					storage.ErrIdempotencyConflict,
				) {
					conflictCount++
				} else {
					t.Errorf(
						"request %d expected conflict, got %v",
						i,
						result.err,
					)
				}
			}
		}

		if replayCount != 9 {
			t.Errorf("expected 9 replays, got %d", replayCount)
		}

		if conflictCount != 10 {
			t.Errorf("expected 10 conflicts, got %d", conflictCount)
		}

		assertOneIdempotentNotification(
			t, pool, key, recipient, winnerID,
		)
	})
}
