// This integration test verifies the actual database behavior for all six notification statuses, repeated cancellation, cancellation timestamps, and nonexistent IDs
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

func TestCancelNotificationIntegration(t *testing.T) {
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
	defer pool.Close()

	repo := storage.NewNotificationRepository(pool)

	tests := []struct {
		name          string
		initialStatus notification.Status
		shouldCancel  bool
	}{
		{"pending notification", notification.StatusPending, true},
		{"retrying notification", notification.StatusRetrying, true},
		{"processing notification", notification.StatusProcessing, false},
		{"accepted notification", notification.StatusAccepted, false},
		{"failed notification", notification.StatusFailed, false},
		{"already cancelled", notification.StatusCancelled, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			created, err := repo.Create(
				ctx,
				notification.CreateRequest{
					Recipient: "+905551234567",
					Channel:   notification.ChannelSMS,
					Content:   "Cancellation integration test",
				},
			)
			if err != nil {
				t.Fatalf("create notification: %v", err)
			}

			// Cleanup runs after this subtest, before the parent test closes the database pool
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(
					context.Background(), 5*time.Second,
				)
				defer cleanupCancel()

				_, err := pool.Exec(
					cleanupCtx,
					"DELETE FROM notifications WHERE id = $1::uuid",
					created.ID,
				)
				if err != nil {
					t.Errorf("cleanup notification: %v", err)
				}
			})

			// Simulate the notification's initial state.
			if tt.initialStatus != notification.StatusPending {
				_, err := pool.Exec(
					ctx,
					"UPDATE notifications SET status = $1 WHERE id = $2::uuid",
					string(tt.initialStatus),
					created.ID,
				)
				if err != nil {
					t.Fatalf("set initial status: %v", err)
				}
			}

			result, err := repo.Cancel(ctx, created.ID)

			if !tt.shouldCancel {
				if !errors.Is(
					err,
					storage.ErrNotificationNotCancellable,
				) {
					t.Fatalf("expected cancellation conflict, got %v", err)
				}

				stored, err := repo.GetByID(ctx, created.ID)
				if err != nil {
					t.Fatal(err)
				}

				if stored.Status != tt.initialStatus {
					t.Error("ineligible notification status was changed")
				}
				return
			}

			if err != nil {
				t.Fatalf("cancel notification: %v", err)
			}

			if result.Status != notification.StatusCancelled {
				t.Errorf("expected cancelled, got %s", result.Status)
			}

			var cancelledAt *time.Time

			err = pool.QueryRow(
				ctx,
				"SELECT cancelled_at FROM notifications WHERE id = $1::uuid",
				created.ID,
			).Scan(&cancelledAt)

			if err != nil || cancelledAt == nil {
				t.Errorf("expected cancellation timestamp, got %v", err)
			}

			// Cancelling the same notification again is a conflict.
			_, err = repo.Cancel(ctx, created.ID)
			if !errors.Is(err, storage.ErrNotificationNotCancellable) {
				t.Errorf("expected repeat cancellation conflict, got %v", err)
			}
		})
	}

	t.Run("nonexistent notification", func(t *testing.T) {
		_, err := repo.Cancel(
			ctx,
			"00000000-0000-0000-0000-000000000000",
		)

		if !errors.Is(err, storage.ErrNotificationNotFound) {
			t.Errorf("expected not found, got %v", err)
		}
	})
}
