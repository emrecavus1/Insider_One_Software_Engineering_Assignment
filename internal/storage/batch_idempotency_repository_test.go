package storage_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func TestBatchIdempotencyIntegration(t *testing.T) {
	if os.Getenv("RUN_DB_TESTS") != "1" {
		t.Skip("database integration tests are not enabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	pool, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}

	// Registering pool closure first so database cleanup runs before the pool closes
	t.Cleanup(func() { pool.Close() })

	repo := storage.NewNotificationRepository(pool)

	key := "test-batch-idempotency-" + uuid.NewString()
	prefix := uuid.NewString()

	requests := []notification.CreateRequest{
		{
			Recipient: "first-" + prefix + "@example.com",
			Channel:   notification.ChannelEmail,
			Content:   "First notification",
			Priority:  notification.PriorityHigh,
		},
		{
			Recipient: "second-" + prefix + "@example.com",
			Channel:   notification.ChannelEmail,
			Content:   "Second notification",
			Priority:  notification.PriorityNormal,
		},
		{
			Recipient: "third-" + prefix + "@example.com",
			Channel:   notification.ChannelEmail,
			Content:   "Third notification",
			Priority:  notification.PriorityLow,
		},
	}

	first, replayed, err := repo.CreateBatchIdempotent(
		ctx, requests, key,
	)
	if err != nil {
		t.Fatalf("create initial batch: %v", err)
	}

	if replayed {
		t.Fatal("initial batch request should not be replayed")
	}

	if first.Count != 3 || len(first.NotificationIDs) != 3 {
		t.Fatalf("unexpected initial batch result: %+v", first)
	}

	// Cleanup must remove the referencing idempotency record and notifications before deleting the batch
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()

		statements := []string{
			"DELETE FROM idempotency_keys WHERE key = $1",
			"DELETE FROM notifications WHERE batch_id = $1::uuid",
			"DELETE FROM batches WHERE id = $1::uuid",
		}

		for i, statement := range statements {
			value := first.BatchID
			if i == 0 {
				value = key
			}

			if _, err := pool.Exec(
				cleanupCtx, statement, value,
			); err != nil {
				t.Errorf("cleanup statement %d: %v", i+1, err)
			}
		}
	})

	t.Run("identical request returns original batch", func(t *testing.T) {
		second, wasReplayed, err := repo.CreateBatchIdempotent(
			ctx, requests, key,
		)
		if err != nil {
			t.Fatal(err)
		}

		if !wasReplayed {
			t.Error("expected request to be replayed")
		}

		if !reflect.DeepEqual(first, second) {
			t.Errorf(
				"replayed batch differs from original:\nfirst=%+v\nsecond=%+v",
				first, second,
			)
		}
	})

	t.Run("notification IDs preserve request order", func(t *testing.T) {
		for i, id := range first.NotificationIDs {
			var recipient string

			err := pool.QueryRow(ctx, `
				SELECT recipient
				FROM notifications
				WHERE id = $1::uuid
				  AND batch_id = $2::uuid
			`, id, first.BatchID).Scan(&recipient)

			if err != nil {
				t.Fatalf("retrieve notification %d: %v", i, err)
			}

			if recipient != requests[i].Recipient {
				t.Errorf(
					"position %d: expected %s, got %s",
					i, requests[i].Recipient, recipient,
				)
			}
		}
	})

	t.Run("different payload causes conflict", func(t *testing.T) {
		modified := append(
			[]notification.CreateRequest(nil),
			requests...,
		)
		modified[0].Content = "Different content"

		_, _, err := repo.CreateBatchIdempotent(
			ctx, modified, key,
		)

		if !errors.Is(err, storage.ErrIdempotencyConflict) {
			t.Errorf("expected idempotency conflict, got %v", err)
		}
	})

	t.Run("different request order causes conflict", func(t *testing.T) {
		reordered := append(
			[]notification.CreateRequest(nil),
			requests...,
		)

		reordered[0], reordered[1] = reordered[1], reordered[0]

		_, _, err := repo.CreateBatchIdempotent(
			ctx, reordered, key,
		)

		if !errors.Is(err, storage.ErrIdempotencyConflict) {
			t.Errorf("expected idempotency conflict, got %v", err)
		}
	})

	t.Run("batch key cannot create single notification", func(t *testing.T) {
		_, _, err := repo.CreateIdempotent(
			ctx, requests[0], key,
		)

		if !errors.Is(err, storage.ErrIdempotencyConflict) {
			t.Errorf("expected cross-type conflict, got %v", err)
		}
	})

	t.Run("single key cannot create batch", func(t *testing.T) {
		singleKey := "test-single-key-" + uuid.NewString()

		single, _, err := repo.CreateIdempotent(
			ctx,
			notification.CreateRequest{
				Recipient: "single-" + prefix + "@example.com",
				Channel:   notification.ChannelEmail,
				Content:   "Single request",
			},
			singleKey,
		)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(), 5*time.Second,
			)
			defer cleanupCancel()

			if _, err := pool.Exec(
				cleanupCtx,
				"DELETE FROM idempotency_keys WHERE key = $1",
				singleKey,
			); err != nil {
				t.Errorf("cleanup single key: %v", err)
			}

			if _, err := pool.Exec(
				cleanupCtx,
				"DELETE FROM notifications WHERE id = $1::uuid",
				single.ID,
			); err != nil {
				t.Errorf("cleanup single notification: %v", err)
			}
		})

		_, _, err = repo.CreateBatchIdempotent(
			ctx, requests, singleKey,
		)

		if !errors.Is(err, storage.ErrIdempotencyConflict) {
			t.Errorf("expected cross-type conflict, got %v", err)
		}
	})

	t.Run("exactly one batch and three notifications exist", func(t *testing.T) {
		var batchCount, notificationCount, keyCount int

		queries := []struct {
			sql   string
			arg   string
			count *int
		}{
			{
				"SELECT COUNT(*) FROM batches WHERE id = $1::uuid",
				first.BatchID,
				&batchCount,
			},
			{
				"SELECT COUNT(*) FROM notifications WHERE batch_id = $1::uuid",
				first.BatchID,
				&notificationCount,
			},
			{
				"SELECT COUNT(*) FROM idempotency_keys WHERE key = $1",
				key,
				&keyCount,
			},
		}

		for _, query := range queries {
			if err := pool.QueryRow(
				ctx, query.sql, query.arg,
			).Scan(query.count); err != nil {
				t.Fatal(err)
			}
		}

		if batchCount != 1 ||
			notificationCount != 3 ||
			keyCount != 1 {
			t.Errorf(
				"unexpected counts: batches=%d notifications=%d keys=%d",
				batchCount, notificationCount, keyCount,
			)
		}
	})
}
