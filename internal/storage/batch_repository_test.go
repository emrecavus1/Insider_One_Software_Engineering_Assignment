package storage_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func TestBatchRepositoryIntegration(t *testing.T) {
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
	t.Cleanup(func() {
		pool.Close()
	})

	repo := storage.NewNotificationRepository(pool)

	requests := []notification.CreateRequest{
		{
			Recipient: "+905551234567",
			Channel:   notification.ChannelSMS,
			Content:   "Flash sale starts now!",
			Priority:  notification.PriorityHigh,
		},
		{
			Recipient: "customer@example.com",
			Channel:   notification.ChannelEmail,
			Content:   "Check out our offers!",
			Priority:  notification.PriorityNormal,
		},
	}

	result, err := repo.CreateBatch(ctx, requests)
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	// Remove test records when finished.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()

		_, err := pool.Exec(
			cleanupCtx,
			"DELETE FROM notifications WHERE batch_id = $1::uuid",
			result.BatchID,
		)
		if err != nil {
			t.Errorf("delete test notifications: %v", err)
		}

		_, err = pool.Exec(
			cleanupCtx,
			"DELETE FROM batches WHERE id = $1::uuid",
			result.BatchID,
		)
		if err != nil {
			t.Errorf("delete test batch: %v", err)
		}
	})

	t.Run("batch contains two notifications", func(t *testing.T) {
		if result.Count != 2 {
			t.Errorf("expected 2 notifications, got %d", result.Count)
		}

		if len(result.NotificationIDs) != 2 {
			t.Errorf("expected 2 IDs, got %d",
				len(result.NotificationIDs))
		}
	})

	t.Run("notifications persisted correctly", func(t *testing.T) {
		var count int

		err := pool.QueryRow(
			ctx,
			"SELECT COUNT(*) FROM notifications WHERE batch_id = $1::uuid",
			result.BatchID,
		).Scan(&count)

		if err != nil {
			t.Fatalf("count batch notifications: %v", err)
		}

		if count != 2 {
			t.Errorf("expected 2 stored notifications, got %d", count)
		}

		for _, id := range result.NotificationIDs {
			stored, err := repo.GetByID(ctx, id)
			if err != nil {
				t.Fatalf("retrieve notification: %v", err)
			}

			if stored.BatchID == nil || *stored.BatchID != result.BatchID {
				t.Errorf("incorrect batch ID for notification %s", id)
			}
		}
	})

	t.Run("reject empty batch", func(t *testing.T) {
		_, err := repo.CreateBatch(ctx, nil)
		if err == nil {
			t.Error("expected error for empty batch")
		}
	})

	t.Run("reject batch larger than 1000", func(t *testing.T) {
		oversized := make([]notification.CreateRequest, 1001)

		_, err := repo.CreateBatch(ctx, oversized)
		if err == nil {
			t.Error("expected error for oversized batch")
		}
	})

	t.Run("reject invalid notification", func(t *testing.T) {
		invalid := []notification.CreateRequest{
			requests[0],
			{
				Recipient: "invalid-email",
				Channel:   notification.ChannelEmail,
				Content:   "Invalid message",
			},
		}

		_, err := repo.CreateBatch(ctx, invalid)
		if err == nil {
			t.Error("expected validation error")
		}
	})

	t.Run("batch status aggregation", func(t *testing.T) {
		// Initially, both notifications should be pending
		summary, err := repo.GetBatchStatus(ctx, result.BatchID)
		if err != nil {
			t.Fatalf("retrieve batch status: %v", err)
		}

		if summary.Total != 2 {
			t.Fatalf("expected 2 notifications, got %d", summary.Total)
		}

		if summary.StatusCounts.Pending != 2 {
			t.Errorf(
				"expected 2 pending notifications, got %d",
				summary.StatusCounts.Pending,
			)
		}

		// simulating provider acceptance for one notification.
		_, err = pool.Exec(
			ctx,
			"UPDATE notifications SET status = 'accepted' WHERE id = $1::uuid",
			result.NotificationIDs[0],
		)
		if err != nil {
			t.Fatalf("update notification status: %v", err)
		}

		summary, err = repo.GetBatchStatus(ctx, result.BatchID)
		if err != nil {
			t.Fatalf("retrieve updated batch status: %v", err)
		}

		if summary.StatusCounts.Accepted != 1 ||
			summary.StatusCounts.Pending != 1 {
			t.Errorf("unexpected status counts: %+v", summary.StatusCounts)
		}
	})

	t.Run("batch not found", func(t *testing.T) {
		_, err := repo.GetBatchStatus(
			ctx,
			"00000000-0000-0000-0000-000000000000",
		)

		if !errors.Is(err, storage.ErrBatchNotFound) {
			t.Errorf("expected ErrBatchNotFound, got %v", err)
		}
	})

}
