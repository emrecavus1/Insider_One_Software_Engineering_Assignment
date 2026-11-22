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

func TestNotificationRepositoryIntegration(t *testing.T) {
	if os.Getenv("RUN_DB_TESTS") != "1" {
		t.Skip("database integration tests are not enabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := storage.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	repo := storage.NewNotificationRepository(pool)

	request := notification.CreateRequest{
		Recipient: "+905551234567",
		Channel:   notification.ChannelSMS,
		Content:   "Repository integration test",
		Priority:  notification.PriorityHigh,
	}

	created, err := repo.Create(ctx, request)
	if err != nil {
		t.Fatalf("create notification: %v", err)
	}

	// Clean up the test record before closing the pool.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()

		_, cleanupErr := pool.Exec(
			cleanupCtx,
			"DELETE FROM notifications WHERE id = $1::uuid",
			created.ID,
		)
		if cleanupErr != nil {
			t.Errorf("clean up test notification: %v", cleanupErr)
		}
	}()

	if created.ID == "" {
		t.Fatal("expected a generated notification ID")
	}

	if created.Status != notification.StatusPending {
		t.Errorf("expected pending status, got %s", created.Status)
	}

	t.Run("retrieve created notification", func(t *testing.T) {
		retrieved, err := repo.GetByID(ctx, created.ID)
		if err != nil {
			t.Fatalf("retrieve notification: %v", err)
		}

		if retrieved.ID != created.ID {
			t.Errorf("retrieved notification ID does not match")
		}

		if retrieved.Content != request.Content {
			t.Errorf("retrieved notification content does not match")
		}

		if retrieved.Priority != notification.PriorityHigh {
			t.Errorf("expected high priority, got %s", retrieved.Priority)
		}
	})

	t.Run("notification not found", func(t *testing.T) {
		_, err := repo.GetByID(
			ctx,
			"00000000-0000-0000-0000-000000000000",
		)

		if !errors.Is(err, storage.ErrNotificationNotFound) {
			t.Errorf("expected ErrNotificationNotFound, got %v", err)
		}
	})
}
