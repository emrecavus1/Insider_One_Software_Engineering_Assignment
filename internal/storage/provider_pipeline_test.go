package storage_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/provider"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/worker"
)

func TestProviderHTTPPipelineIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)
	limiter := setupRateLimitRedis(t, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	id := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Notification-ID") != id {
			t.Error("unexpected notification correlation header")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_, _ = fmt.Fprint(w, `{"messageId":"provider-integration-1","status":"accepted","timestamp":"2026-11-22T19:00:00Z"}`)
	}))
	defer webhook.Close()

	sender, err := provider.NewHTTPSender(webhook.URL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	processor, err := worker.NewRateLimitedDeliveryProcessor(repo, sender, limiter, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.SetRetryPolicy(worker.RetryPolicy{BaseDelay: 2 * time.Second, MaxDelay: time.Minute}); err != nil {
		t.Fatal(err)
	}

	jobs, err := repo.ClaimReady(ctx, "provider-integration-worker", 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim failed: %v jobs=%d", err, len(jobs))
	}

	if err := processor.Process(ctx, jobs[0]); err != nil {
		t.Fatal(err)
	}

	var status, outcome, providerMessageID string
	var attempts int
	err = pool.QueryRow(ctx, `
        SELECT n.status, n.attempt_count, a.outcome, n.provider_message_id
        FROM notifications AS n
        JOIN delivery_attempts AS a ON a.notification_id = n.id
        WHERE n.id = $1::uuid
    `, id).Scan(&status, &attempts, &outcome, &providerMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if status != "accepted" || attempts != 1 || outcome != "accepted" || providerMessageID != "provider-integration-1" {
		t.Fatalf("unexpected final status=%s attempts=%d outcome=%s providerID=%s", status, attempts, outcome, providerMessageID)
	}
}

func TestProviderHTTPRetryPipelineIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)
	limiter := setupRateLimitRedis(t, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	id := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityNormal, notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	var calls atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprint(w, `{"messageId":"provider-after-retry","status":"accepted","timestamp":"2026-11-22T19:00:00Z"}`)
	}))
	defer webhook.Close()

	sender, err := provider.NewHTTPSender(webhook.URL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := worker.NewRateLimitedDeliveryProcessor(repo, sender, limiter, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.SetRetryPolicy(worker.RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute}); err != nil {
		t.Fatal(err)
	}

	first, err := repo.ClaimReady(ctx, "retry-worker-1", 1, 30*time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim: %v", err)
	}
	if err := processor.Process(ctx, first[0]); err != nil {
		t.Fatal(err)
	}

	var status string
	var scheduledDelay float64
	err = pool.QueryRow(ctx, `
        SELECT status, EXTRACT(EPOCH FROM (next_attempt_at - NOW()))
        FROM notifications
        WHERE id = $1::uuid
    `, id).Scan(&status, &scheduledDelay)
	if err != nil {
		t.Fatal(err)
	}
	if status != "retrying" || scheduledDelay < 2.0 {
		t.Fatalf("expected 429 retry scheduled at least 2s ahead, got %s delay=%.3fs", status, scheduledDelay)
	}

	_, err = pool.Exec(ctx, `
        UPDATE notifications
        SET next_attempt_at = NOW() - INTERVAL '1 second'
        WHERE id = $1::uuid AND status = 'retrying'
    `, id)
	if err != nil {
		t.Fatal(err)
	}

	second, err := repo.ClaimReady(ctx, "retry-worker-2", 1, 30*time.Second)
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim: %v", err)
	}
	if err := processor.Process(ctx, second[0]); err != nil {
		t.Fatal(err)
	}

	var attempts, accepted, retryable int
	err = pool.QueryRow(ctx, `
        SELECT n.attempt_count, n.status,
               COUNT(*) FILTER (WHERE a.outcome = 'accepted'),
               COUNT(*) FILTER (WHERE a.outcome = 'retryable_failure')
        FROM notifications n
        JOIN delivery_attempts a ON a.notification_id = n.id
        WHERE n.id = $1::uuid
        GROUP BY n.id
    `, id).Scan(&attempts, &status, &accepted, &retryable)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || attempts != 2 || status != "accepted" || accepted != 1 || retryable != 1 {
		t.Fatalf("unexpected final state calls=%d attempts=%d status=%s accepted=%d retryable=%d", calls.Load(), attempts, status, accepted, retryable)
	}
}
