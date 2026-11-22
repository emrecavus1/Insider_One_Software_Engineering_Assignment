package storage_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

func TestLeaseRecoveryIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second,
	)
	defer cancel()

	past := time.Now().Add(-time.Minute)

	retryID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		past,
	)

	failedID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityNormal,
		notification.StatusPending,
		past,
	)

	activeID := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityLow,
		notification.StatusPending,
		past,
	)

	claimed, err := repo.ClaimReady(
		ctx, "initial-worker", 3, 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(claimed) != 3 {
		t.Fatalf("expected three claims, got %d", len(claimed))
	}

	oldTokens := make(map[string]string)
	for _, job := range claimed {
		oldTokens[job.ID] = job.ClaimToken

		if _, err := uuid.Parse(job.ClaimToken); err != nil {
			t.Errorf("invalid claim token: %v", err)
		}
	}

	// Simulating two expired leases
	// The first is recoverable
	// The second has used all delivery attempts

	_, err = pool.Exec(ctx, `
		UPDATE notifications
		SET
			lease_expires_at = CASE
				WHEN id = $1::uuid
					THEN NOW() - INTERVAL '2 minutes'
				ELSE NOW() - INTERVAL '1 minute'
			END,
			attempt_count = CASE
				WHEN id = $2::uuid
					THEN max_attempts
				ELSE attempt_count
			END
		WHERE id IN ($1::uuid, $2::uuid)
	`, retryID, failedID)

	if err != nil {
		t.Fatalf("prepare expired leases: %v", err)
	}

	t.Run("recover oldest expired lease", func(t *testing.T) {
		result, err := repo.RecoverExpiredLeases(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}

		if result.Requeued != 1 || result.Failed != 0 {
			t.Errorf("unexpected recovery result: %+v", result)
		}

		stored, err := repo.GetByID(ctx, retryID)
		if err != nil {
			t.Fatal(err)
		}

		if stored.Status != notification.StatusRetrying {
			t.Errorf("expected retrying, got %s", stored.Status)
		}

		var cleared, ready bool

		err = pool.QueryRow(ctx, `
			SELECT
				worker_id IS NULL
				AND claim_token IS NULL
				AND lease_expires_at IS NULL,
				next_attempt_at <= NOW()
			FROM notifications
			WHERE id = $1::uuid
		`, retryID).Scan(&cleared, &ready)

		if err != nil {
			t.Fatal(err)
		}

		if !cleared || !ready {
			t.Error("recovered notification is not ready and unowned")
		}
	})

	t.Run("exhausted lease becomes failed", func(t *testing.T) {
		result, err := repo.RecoverExpiredLeases(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}

		if result.Requeued != 0 || result.Failed != 1 {
			t.Errorf("unexpected recovery result: %+v", result)
		}

		stored, err := repo.GetByID(ctx, failedID)
		if err != nil {
			t.Fatal(err)
		}

		if stored.Status != notification.StatusFailed {
			t.Errorf("expected failed, got %s", stored.Status)
		}
	})

	t.Run("active lease stays processing", func(t *testing.T) {
		stored, err := repo.GetByID(ctx, activeID)
		if err != nil {
			t.Fatal(err)
		}

		if stored.Status != notification.StatusProcessing {
			t.Errorf("expected processing, got %s", stored.Status)
		}
	})

	t.Run("recovery is idempotent", func(t *testing.T) {
		result, err := repo.RecoverExpiredLeases(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}

		if result.Requeued != 0 || result.Failed != 0 {
			t.Errorf("expected nothing left to recover: %+v", result)
		}
	})

	t.Run("recovered notification can be reclaimed", func(t *testing.T) {
		jobs, err := repo.ClaimReady(
			ctx, "new-worker", 1, 30*time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(jobs) != 1 || jobs[0].ID != retryID {
			t.Fatalf("expected recovered job, got %+v", jobs)
		}

		if jobs[0].ClaimToken == oldTokens[retryID] {
			t.Error("reclaimed job reused old claim token")
		}
	})

	t.Run("invalid recovery limit", func(t *testing.T) {
		for _, limit := range []int{0, -1, 1001} {
			if _, err := repo.RecoverExpiredLeases(
				ctx, limit,
			); err == nil {
				t.Errorf("expected error for limit %d", limit)
			}
		}
	})
}

func TestRenewLeaseFencingIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second,
	)
	defer cancel()

	id := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	first, err := repo.ClaimReady(
		ctx, "shared-worker", 1, 30*time.Second,
	)
	if err != nil || len(first) != 1 {
		t.Fatalf("initial claim failed: %v, %+v", err, first)
	}

	oldClaim := first[0]

	t.Run("valid owner can renew", func(t *testing.T) {
		expiresAt, err := repo.RenewLease(
			ctx, id, oldClaim.ClaimToken, 2*time.Minute,
		)
		if err != nil {
			t.Fatal(err)
		}

		if !expiresAt.After(oldClaim.LeaseExpiresAt) {
			t.Error("lease was not extended")
		}
	})

	t.Run("wrong token is rejected", func(t *testing.T) {
		_, err := repo.RenewLease(
			ctx, id, uuid.NewString(), 30*time.Second,
		)

		if !errors.Is(err, storage.ErrClaimNotOwned) {
			t.Errorf("expected ownership error, got %v", err)
		}
	})

	// Simulating the owner exceeding its lease deadline
	_, err = pool.Exec(ctx, `
		UPDATE notifications
		SET lease_expires_at = NOW() - INTERVAL '1 minute'
		WHERE id = $1::uuid
	`, id)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("expired token cannot renew", func(t *testing.T) {
		_, err := repo.RenewLease(
			ctx, id, oldClaim.ClaimToken, 30*time.Second,
		)

		if !errors.Is(err, storage.ErrClaimNotOwned) {
			t.Errorf("expected expired ownership error, got %v", err)
		}
	})

	recovery, err := repo.RecoverExpiredLeases(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}

	if recovery.Requeued != 1 {
		t.Fatalf("expected one requeued notification: %+v", recovery)
	}

	// Deliberately reusing the same worker identifier
	// The claim token must still change
	second, err := repo.ClaimReady(
		ctx, "shared-worker", 1, 30*time.Second,
	)
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim failed: %v, %+v", err, second)
	}

	newClaim := second[0]

	if oldClaim.ClaimToken == newClaim.ClaimToken {
		t.Fatal("new claim reused the previous token")
	}

	t.Run("stale owner cannot renew new claim", func(t *testing.T) {
		_, err := repo.RenewLease(
			ctx, id, oldClaim.ClaimToken, 30*time.Second,
		)

		if !errors.Is(err, storage.ErrClaimNotOwned) {
			t.Errorf("expected stale-token rejection, got %v", err)
		}
	})

	t.Run("new owner can renew", func(t *testing.T) {
		_, err := repo.RenewLease(
			ctx, id, newClaim.ClaimToken, 2*time.Minute,
		)
		if err != nil {
			t.Errorf("new owner could not renew: %v", err)
		}
	})
}

func TestConcurrentLeaseRecoveryIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	const total = 24
	const recoverers = 8

	for i := 0; i < total; i++ {
		insertClaimTestNotification(
			t, ctx, repo, pool,
			notification.PriorityNormal,
			notification.StatusPending,
			time.Now().Add(-time.Minute),
		)
	}

	claimed, err := repo.ClaimReady(
		ctx, "crashed-worker", total, 30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(claimed) != total {
		t.Fatalf("expected %d claims, got %d", total, len(claimed))
	}

	// Simulate worker crash by expiring all leases.
	_, err = pool.Exec(ctx, `
		UPDATE notifications
		SET lease_expires_at = NOW() - INTERVAL '1 minute'
		WHERE status = 'processing'
	`)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan notification.LeaseRecoveryResult, recoverers)
	errs := make(chan error, recoverers)

	var wg sync.WaitGroup

	for i := 0; i < recoverers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			result, err := repo.RecoverExpiredLeases(
				ctx, 10,
			)
			if err != nil {
				errs <- err
				return
			}

			results <- result
		}()
	}

	close(start)
	wg.Wait()

	close(results)
	close(errs)

	for err := range errs {
		t.Errorf("concurrent recovery failed: %v", err)
	}

	var requeued, failed int64

	for result := range results {
		requeued += result.Requeued
		failed += result.Failed
	}

	if requeued != total || failed != 0 {
		t.Errorf(
			"expected %d unique recoveries, got requeued=%d failed=%d",
			total, requeued, failed,
		)
	}

	var recoveredRows int

	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM notifications
		WHERE status = 'retrying'
		  AND worker_id IS NULL
		  AND claim_token IS NULL
		  AND lease_expires_at IS NULL
	`).Scan(&recoveredRows)

	if err != nil {
		t.Fatal(err)
	}

	if recoveredRows != total {
		t.Errorf(
			"expected %d recovered database rows, got %d",
			total, recoveredRows,
		)
	}

	// A second recovery pass must not count the same notifications again
	extra, err := repo.RecoverExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}

	if extra.Requeued != 0 || extra.Failed != 0 {
		t.Errorf("duplicate recovery detected: %+v", extra)
	}
}
