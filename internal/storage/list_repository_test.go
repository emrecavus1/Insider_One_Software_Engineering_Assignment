package storage_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/config"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func TestListRepositoryIntegration(t *testing.T) {
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

	// Register pool closing before data cleanup,
	// so the cleanup runs first.
	t.Cleanup(func() {
		pool.Close()
	})

	createdIDs := []string{}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()

		for _, id := range createdIDs {
			if _, err := pool.Exec(
				cleanupCtx,
				"DELETE FROM notifications WHERE id = $1::uuid",
				id,
			); err != nil {
				t.Errorf("cleanup notification %s: %v", id, err)
			}
		}
	})

	repo := storage.NewNotificationRepository(pool)

	// Use an isolated historical date range so ordinary
	// development notifications don't affect the counts.
	base := time.Date(2001, 1, 2, 3, 4, 5, 0, time.UTC)
	end := base.Add(3 * time.Second)

	type testRecord struct {
		recipient string
		channel   notification.Channel
		status    notification.Status
		createdAt time.Time
	}

	records := []testRecord{
		{
			recipient: "+905551234567",
			channel:   notification.ChannelSMS,
			status:    notification.StatusPending,
			createdAt: base,
		},
		{
			recipient: "+905551234568",
			channel:   notification.ChannelSMS,
			status:    notification.StatusAccepted,
			createdAt: base.Add(time.Second),
		},
		{
			recipient: "customer@example.com",
			channel:   notification.ChannelEmail,
			status:    notification.StatusPending,
			createdAt: base.Add(2 * time.Second),
		},
	}

	for _, record := range records {
		created, err := repo.Create(
			ctx,
			notification.CreateRequest{
				Recipient: record.recipient,
				Channel:   record.channel,
				Content:   "Pagination integration test",
			},
		)
		if err != nil {
			t.Fatalf("create notification: %v", err)
		}

		createdIDs = append(createdIDs, created.ID)

		_, err = pool.Exec(
			ctx,
			`UPDATE notifications
			 SET status = $1, created_at = $2
			 WHERE id = $3::uuid`,
			string(record.status),
			record.createdAt,
			created.ID,
		)
		if err != nil {
			t.Fatalf("prepare test notification: %v", err)
		}
	}

	// Two SMS notifications fall within the date range.
	filter := notification.ListFilter{
		Channel:     notification.ChannelSMS,
		CreatedFrom: &base,
		CreatedTo:   &end,
		Page:        1,
		Limit:       1,
	}

	t.Run("first page", func(t *testing.T) {
		result, err := repo.List(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}

		if result.Total != 2 {
			t.Errorf("expected total 2, got %d", result.Total)
		}

		if len(result.Notifications) != 1 {
			t.Fatalf("expected 1 notification, got %d",
				len(result.Notifications))
		}

		// The newer accepted SMS should appear first.
		if result.Notifications[0].ID != createdIDs[1] {
			t.Error("notifications are not sorted newest first")
		}
	})

	t.Run("second page", func(t *testing.T) {
		secondPage := filter
		secondPage.Page = 2

		result, err := repo.List(ctx, secondPage)
		if err != nil {
			t.Fatal(err)
		}

		if result.Total != 2 ||
			len(result.Notifications) != 1 {
			t.Fatalf("unexpected second page: %+v", result)
		}

		if result.Notifications[0].ID != createdIDs[0] {
			t.Error("second page contains incorrect notification")
		}
	})

	t.Run("combined status and channel filters", func(t *testing.T) {
		pendingFilter := filter
		pendingFilter.Status = notification.StatusPending
		pendingFilter.Limit = 20

		result, err := repo.List(ctx, pendingFilter)
		if err != nil {
			t.Fatal(err)
		}

		if result.Total != 1 ||
			len(result.Notifications) != 1 {
			t.Fatalf("expected one pending SMS, got %+v", result)
		}

		if result.Notifications[0].ID != createdIDs[0] {
			t.Error("incorrect notification returned")
		}
	})

	t.Run("no matching notifications", func(t *testing.T) {
		emptyFilter := filter
		emptyFilter.Status = notification.StatusFailed
		emptyFilter.Limit = 20

		result, err := repo.List(ctx, emptyFilter)
		if err != nil {
			t.Fatal(err)
		}

		if result.Total != 0 ||
			len(result.Notifications) != 0 {
			t.Errorf("expected empty results, got %+v", result)
		}
	})
}
