package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

type concurrentBatchResult struct {
	result   notification.BatchCreateResult
	replayed bool
	err      error
}

// Releasing concurrent batch requests using a shared start barrier
func runConcurrentBatchRequests(
	ctx context.Context,
	repo *storage.NotificationRepository,
	key string,
	requests [][]notification.CreateRequest,
) []concurrentBatchResult {
	results := make([]concurrentBatchResult, len(requests))

	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := range requests {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()

			<-start

			result, replayed, err := repo.CreateBatchIdempotent(
				ctx,
				requests[index],
				key,
			)

			results[index] = concurrentBatchResult{
				result:   result,
				replayed: replayed,
				err:      err,
			}
		}(i)
	}

	close(start)
	wg.Wait()

	return results
}

// Creating three notifications with unique recipients so database assertions cannot match development data
func newConcurrentBatchRequests(
	prefix string,
) []notification.CreateRequest {
	return []notification.CreateRequest{
		{
			Recipient: prefix + "1@example.com",
			Channel:   notification.ChannelEmail,
			Content:   "First message",
			Priority:  notification.PriorityHigh,
		},
		{
			Recipient: prefix + "2@example.com",
			Channel:   notification.ChannelEmail,
			Content:   "Second message",
			Priority:  notification.PriorityNormal,
		},
		{
			Recipient: prefix + "3@example.com",
			Channel:   notification.ChannelEmail,
			Content:   "Third message",
			Priority:  notification.PriorityLow,
		},
	}
}

// Cleaning up the idempotency key, notifications, and batch
// The recipient prefix uniquely identifies this test's records

func registerConcurrentBatchCleanup(
	t *testing.T,
	pool *pgxpool.Pool,
	key string,
	prefix string,
) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()

		pattern := prefix + "%"

		// Finding batches associated with our test notifications
		rows, err := pool.Query(ctx, `
			SELECT DISTINCT batch_id::text
			FROM notifications
			WHERE recipient LIKE $1
			  AND batch_id IS NOT NULL
		`, pattern)

		if err != nil {
			t.Errorf("find test batches: %v", err)
			return
		}

		var batchIDs []string

		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Errorf("scan test batch ID: %v", err)
				rows.Close()
				return
			}
			batchIDs = append(batchIDs, id)
		}

		if err := rows.Err(); err != nil {
			t.Errorf("iterate test batches: %v", err)
			rows.Close()
			return
		}
		rows.Close()

		// Removing foreign-key references before deleting batches
		if _, err := pool.Exec(ctx,
			"DELETE FROM idempotency_keys WHERE key = $1",
			key,
		); err != nil {
			t.Errorf("cleanup idempotency key: %v", err)
		}

		if _, err := pool.Exec(ctx,
			"DELETE FROM notifications WHERE recipient LIKE $1",
			pattern,
		); err != nil {
			t.Errorf("cleanup batch notifications: %v", err)
		}

		for _, id := range batchIDs {
			if _, err := pool.Exec(ctx,
				"DELETE FROM batches WHERE id = $1::uuid",
				id,
			); err != nil {
				t.Errorf("cleanup batch %s: %v", id, err)
			}
		}
	})
}

// Checking persisted rows and the stored replay response
func assertConcurrentBatchPersistence(
	t *testing.T,
	pool *pgxpool.Pool,
	key string,
	prefix string,
	expected notification.BatchCreateResult,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	var batchCount, notificationCount, keyCount, recipientCount int

	checks := []struct {
		query    string
		arg      string
		expected int
		target   *int
	}{
		{
			"SELECT COUNT(*) FROM batches WHERE id = $1::uuid",
			expected.BatchID, 1, &batchCount,
		},
		{
			"SELECT COUNT(*) FROM notifications WHERE batch_id = $1::uuid",
			expected.BatchID, expected.Count, &notificationCount,
		},
		{
			"SELECT COUNT(*) FROM idempotency_keys WHERE key = $1",
			key, 1, &keyCount,
		},
		{
			"SELECT COUNT(*) FROM notifications WHERE recipient LIKE $1",
			prefix + "%", expected.Count, &recipientCount,
		},
	}

	for _, check := range checks {
		if err := pool.QueryRow(
			ctx, check.query, check.arg,
		).Scan(check.target); err != nil {
			t.Fatalf("database count failed: %v", err)
		}

		if *check.target != check.expected {
			t.Errorf(
				"expected count %d, got %d for %s",
				check.expected, *check.target, check.query,
			)
		}
	}

	var storedBatchID string
	var responseJSON []byte

	err := pool.QueryRow(ctx, `
		SELECT batch_id::text, batch_result
		FROM idempotency_keys
		WHERE key = $1
	`, key).Scan(&storedBatchID, &responseJSON)

	if err != nil {
		t.Fatalf("retrieve stored batch result: %v", err)
	}

	if storedBatchID != expected.BatchID {
		t.Errorf("stored batch ID differs from winning batch ID")
	}

	var stored notification.BatchCreateResult
	if err := json.Unmarshal(responseJSON, &stored); err != nil {
		t.Fatalf("decode stored batch result: %v", err)
	}

	if !reflect.DeepEqual(stored, expected) {
		t.Errorf("stored batch result differs from winning result")
	}
}

func TestConcurrentBatchIdempotencyIntegration(t *testing.T) {
	repo, pool := setupConcurrentIdempotencyTest(t)

	t.Run("20 identical batches create one batch", func(t *testing.T) {
		key := "concurrent-batch-same-" + uuid.NewString()
		prefix := "samebatch-" + uuid.NewString() + "-"

		registerConcurrentBatchCleanup(t, pool, key, prefix)

		batch := newConcurrentBatchRequests(prefix)

		requests := make(
			[][]notification.CreateRequest,
			concurrentRequests,
		)

		for i := range requests {
			requests[i] = batch
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cancel()

		results := runConcurrentBatchRequests(
			ctx, repo, key, requests,
		)

		var winner notification.BatchCreateResult
		createdCount := 0
		replayCount := 0

		for i, result := range results {
			if result.err != nil {
				t.Errorf("request %d failed: %v", i, result.err)
				continue
			}

			if result.replayed {
				replayCount++
			} else {
				createdCount++
				winner = result.result
			}
		}

		if createdCount != 1 {
			t.Fatalf("expected 1 creation, got %d", createdCount)
		}

		if replayCount != concurrentRequests-1 {
			t.Errorf(
				"expected %d replays, got %d",
				concurrentRequests-1, replayCount,
			)
		}

		for i, result := range results {
			if result.err == nil &&
				!reflect.DeepEqual(result.result, winner) {
				t.Errorf("request %d returned a different batch", i)
			}
		}

		assertConcurrentBatchPersistence(
			t, pool, key, prefix, winner,
		)

		// Verifying that the stored ID sequence corresponds to the original request order
		for i, id := range winner.NotificationIDs {
			var recipient string

			err := pool.QueryRow(ctx, `
				SELECT recipient
				FROM notifications
				WHERE id = $1::uuid
				  AND batch_id = $2::uuid
			`, id, winner.BatchID).Scan(&recipient)

			if err != nil {
				t.Fatalf("retrieve notification %d: %v", i, err)
			}

			if recipient != batch[i].Recipient {
				t.Errorf("notification %d is out of order", i)
			}
		}
	})

	t.Run("different batch payloads cause conflicts", func(t *testing.T) {
		key := "concurrent-batch-mixed-" + uuid.NewString()
		prefix := "mixedbatch-" + uuid.NewString() + "-"

		registerConcurrentBatchCleanup(t, pool, key, prefix)

		base := newConcurrentBatchRequests(prefix)

		requests := make(
			[][]notification.CreateRequest,
			concurrentRequests,
		)

		for i := range requests {
			requests[i] = append(
				[]notification.CreateRequest(nil),
				base...,
			)

			// Ten requests have payload A; ten have B.
			if i >= concurrentRequests/2 {
				requests[i][1].Content = "Different second message"
			}
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cancel()

		results := runConcurrentBatchRequests(
			ctx, repo, key, requests,
		)

		var winner notification.BatchCreateResult
		var winningContent string
		createdCount := 0

		for i, result := range results {
			if result.err == nil && !result.replayed {
				createdCount++
				winner = result.result
				winningContent = requests[i][1].Content
			}
		}

		if createdCount != 1 {
			t.Fatalf("expected 1 creation, got %d", createdCount)
		}

		replayCount := 0
		conflictCount := 0

		for i, result := range results {
			content := requests[i][1].Content

			if content == winningContent {
				if result.err != nil {
					t.Errorf(
						"request %d should succeed: %v",
						i, result.err,
					)
					continue
				}

				if !reflect.DeepEqual(result.result, winner) {
					t.Errorf(
						"request %d returned a different batch",
						i,
					)
				}

				if result.replayed {
					replayCount++
				}
			} else {
				if errors.Is(
					result.err,
					storage.ErrIdempotencyConflict,
				) {
					conflictCount++
				} else {
					t.Errorf(
						"request %d expected conflict, got %v",
						i, result.err,
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

		if winner.Count != 3 {
			t.Fatalf(
				"expected 3 notifications in winning batch, got %d",
				winner.Count,
			)
		}

		assertConcurrentBatchPersistence(
			t, pool, key, prefix, winner,
		)
	})
}
