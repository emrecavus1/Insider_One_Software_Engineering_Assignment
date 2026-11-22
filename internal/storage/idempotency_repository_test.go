package storage_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func TestSingleIdempotencyIntegration(t *testing.T) {
	if os.Getenv("RUN_DB_TESTS") != "1" {
		t.Skip("database integration tests are not enabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second,
	)
	defer cancel()

	pool, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}

	// closing the pool after cleaning up test records
	t.Cleanup(func() { pool.Close() })

	repo := storage.NewNotificationRepository(pool)

	key := "test-idempotency-" + uuid.NewString()
	recipient := "idem-" + uuid.NewString() + "@example.com"

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()

		// deleting the referencing key first
		if _, err := pool.Exec(
			cleanupCtx,
			"DELETE FROM idempotency_keys WHERE key = $1",
			key,
		); err != nil {
			t.Errorf("cleanup idempotency key: %v", err)
		}

		if _, err := pool.Exec(
			cleanupCtx,
			"DELETE FROM notifications WHERE recipient = $1",
			recipient,
		); err != nil {
			t.Errorf("cleanup notifications: %v", err)
		}
	})

	request := notification.CreateRequest{
		Recipient: recipient,
		Channel:   notification.ChannelEmail,
		Content:   "Your order has shipped!",
		Priority:  notification.PriorityHigh,
	}

	first, replayed, err := repo.CreateIdempotent(ctx, request, key)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	if replayed {
		t.Fatal("first request should create a new notification")
	}

	t.Run("identical request returns existing notification", func(t *testing.T) {
		second, wasReplayed, err := repo.CreateIdempotent(
			ctx, request, key,
		)
		if err != nil {
			t.Fatal(err)
		}

		if !wasReplayed {
			t.Error("expected request to be replayed")
		}

		if first.ID != second.ID {
			t.Errorf(
				"duplicate created another notification: %s vs %s",
				first.ID, second.ID,
			)
		}
	})

	t.Run("different payload causes conflict", func(t *testing.T) {
		modified := request
		modified.Content = "Your order was cancelled!"

		_, _, err := repo.CreateIdempotent(
			ctx, modified, key,
		)

		if !errors.Is(err, storage.ErrIdempotencyConflict) {
			t.Errorf("expected idempotency conflict, got %v", err)
		}
	})

	t.Run("only one notification persisted", func(t *testing.T) {
		var count int

		err := pool.QueryRow(
			ctx,
			"SELECT COUNT(*) FROM notifications WHERE recipient = $1",
			recipient,
		).Scan(&count)

		if err != nil {
			t.Fatal(err)
		}

		if count != 1 {
			t.Errorf("expected one notification, got %d", count)
		}
	})

	t.Run("idempotency key persisted", func(t *testing.T) {
		var linkedID string

		err := pool.QueryRow(ctx, `
			SELECT notification_id::text
			FROM idempotency_keys
			WHERE key = $1
		`, key).Scan(&linkedID)

		if err != nil {
			t.Fatal(err)
		}

		if linkedID != first.ID {
			t.Errorf(
				"expected key to reference %s, got %s",
				first.ID, linkedID,
			)
		}
	})
}
