package storage_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/worker"
)

func TestDeliveryAttemptLifecycleIntegration(t *testing.T) {
	tests := []struct {
		name           string
		result         notification.DeliveryResult
		maxAttempts    int
		expectedStatus notification.Status
	}{
		{
			name: "accepted delivery",
			result: notification.DeliveryResult{
				Outcome:           notification.OutcomeAccepted,
				HTTPStatus:        202,
				ProviderMessageID: "provider-123",
			},
			maxAttempts:    4,
			expectedStatus: notification.StatusAccepted,
		},
		{
			name: "retryable failure",
			result: notification.DeliveryResult{
				Outcome:      notification.OutcomeRetryableFailure,
				HTTPStatus:   503,
				ErrorMessage: "provider unavailable",
			},
			maxAttempts:    4,
			expectedStatus: notification.StatusRetrying,
		},
		{
			name: "permanent failure",
			result: notification.DeliveryResult{
				Outcome:      notification.OutcomePermanentFailure,
				HTTPStatus:   400,
				ErrorMessage: "invalid provider request",
			},
			maxAttempts:    4,
			expectedStatus: notification.StatusFailed,
		},
		{
			name: "retry budget exhausted",
			result: notification.DeliveryResult{
				Outcome:      notification.OutcomeRetryableFailure,
				HTTPStatus:   503,
				ErrorMessage: "provider unavailable",
			},
			maxAttempts:    1,
			expectedStatus: notification.StatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, pool := setupClaimTest(t)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				15*time.Second,
			)
			defer cancel()

			id := insertClaimTestNotification(
				t, ctx, repo, pool,
				notification.PriorityHigh,
				notification.StatusPending,
				time.Now().Add(-time.Minute),
			)

			_, err := pool.Exec(ctx, `
				UPDATE notifications
				SET max_attempts = $2
				WHERE id = $1::uuid
			`, id, tt.maxAttempts)
			if err != nil {
				t.Fatal(err)
			}

			jobs, err := repo.ClaimReady(
				ctx, "delivery-worker", 1, 30*time.Second,
			)
			if err != nil || len(jobs) != 1 {
				t.Fatalf("claim failed: %v, %+v", err, jobs)
			}

			job := jobs[0]

			attempt, err := repo.BeginDeliveryAttempt(ctx, job)
			if err != nil {
				t.Fatal(err)
			}

			if attempt != 1 {
				t.Fatalf("expected attempt 1, got %d", attempt)
			}

			status, err := repo.FinishDeliveryAttempt(
				ctx, job, attempt, tt.result, time.Minute,
			)
			if err != nil {
				t.Fatal(err)
			}

			if status != tt.expectedStatus {
				t.Errorf(
					"expected status %s, got %s",
					tt.expectedStatus, status,
				)
			}

			var storedStatus, storedOutcome string
			var attemptCount int
			var ownerCleared, finished bool

			err = pool.QueryRow(ctx, `
				SELECT
					n.status,
					n.attempt_count,
					n.worker_id IS NULL
						AND n.claim_token IS NULL
						AND n.lease_expires_at IS NULL,
					a.outcome,
					a.finished_at IS NOT NULL
				FROM notifications AS n
				JOIN delivery_attempts AS a
					ON a.notification_id = n.id
				WHERE n.id = $1::uuid
			`, id).Scan(
				&storedStatus,
				&attemptCount,
				&ownerCleared,
				&storedOutcome,
				&finished,
			)
			if err != nil {
				t.Fatal(err)
			}

			if storedStatus != string(tt.expectedStatus) {
				t.Errorf("unexpected persisted status: %s", storedStatus)
			}

			if attemptCount != 1 {
				t.Errorf("expected attempt count 1, got %d", attemptCount)
			}

			if storedOutcome != string(tt.result.Outcome) {
				t.Errorf("unexpected audit outcome: %s", storedOutcome)
			}

			if !ownerCleared || !finished {
				t.Error("attempt or ownership was not finalized")
			}

			if tt.expectedStatus == notification.StatusRetrying {
				var scheduled bool

				err = pool.QueryRow(ctx, `
					SELECT next_attempt_at > NOW() + INTERVAL '30 seconds'
					FROM notifications
					WHERE id = $1::uuid
				`, id).Scan(&scheduled)

				if err != nil {
					t.Fatal(err)
				}

				if !scheduled {
					t.Error("retry was not scheduled in the future")
				}
			}

			_, err = repo.FinishDeliveryAttempt(
				ctx, job, attempt, tt.result, time.Minute,
			)

			if !errors.Is(err, storage.ErrClaimNotOwned) {
				t.Errorf("expected repeated completion rejection, got %v", err)
			}
		})
	}
}

func TestAbandonedDeliveryAttemptIntegration(t *testing.T) {
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
		ctx, "worker-A", 1, 30*time.Second,
	)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim failed: %v", err)
	}

	oldClaim := first[0]

	attempt, err := repo.BeginDeliveryAttempt(ctx, oldClaim)
	if err != nil || attempt != 1 {
		t.Fatalf("begin first attempt: %v", err)
	}

	// Simulate the worker failing while the attempt is active
	_, err = pool.Exec(ctx, `
		UPDATE notifications
		SET lease_expires_at = NOW() - INTERVAL '1 minute'
		WHERE id = $1::uuid
	`, id)
	if err != nil {
		t.Fatal(err)
	}

	recovered, err := repo.RecoverExpiredLeases(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}

	if recovered.Requeued != 1 {
		t.Fatalf("expected one recovered job: %+v", recovered)
	}

	var outcome string
	var finished bool

	err = pool.QueryRow(ctx, `
		SELECT outcome, finished_at IS NOT NULL
		FROM delivery_attempts
		WHERE notification_id = $1::uuid
		  AND attempt_number = 1
	`, id).Scan(&outcome, &finished)
	if err != nil {
		t.Fatal(err)
	}

	if outcome != "unknown" || !finished {
		t.Error("abandoned attempt was not closed as unknown")
	}

	second, err := repo.ClaimReady(
		ctx, "worker-B", 1, 30*time.Second,
	)
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim failed: %v", err)
	}

	newClaim := second[0]

	_, err = repo.FinishDeliveryAttempt(
		ctx,
		oldClaim,
		1,
		notification.DeliveryResult{
			Outcome: notification.OutcomeAccepted,
		},
		time.Second,
	)

	if !errors.Is(err, storage.ErrClaimNotOwned) {
		t.Errorf("stale worker should be rejected, got %v", err)
	}

	nextAttempt, err := repo.BeginDeliveryAttempt(ctx, newClaim)
	if err != nil {
		t.Fatal(err)
	}

	if nextAttempt != 2 {
		t.Errorf("expected second attempt, got %d", nextAttempt)
	}

	status, err := repo.FinishDeliveryAttempt(
		ctx,
		newClaim,
		nextAttempt,
		notification.DeliveryResult{
			Outcome:    notification.OutcomeAccepted,
			HTTPStatus: 202,
		},
		time.Second,
	)

	if err != nil || status != notification.StatusAccepted {
		t.Fatalf("new worker could not finish: %v, %s", err, status)
	}

	var unknownCount, acceptedCount int

	err = pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE outcome = 'unknown'),
			COUNT(*) FILTER (WHERE outcome = 'accepted')
		FROM delivery_attempts
		WHERE notification_id = $1::uuid
	`, id).Scan(&unknownCount, &acceptedCount)

	if err != nil {
		t.Fatal(err)
	}

	if unknownCount != 1 || acceptedCount != 1 {
		t.Errorf(
			"expected one unknown and one accepted attempt, got %d and %d",
			unknownCount, acceptedCount,
		)
	}
}

func TestConcurrentDeliveryAttemptStartIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second,
	)
	defer cancel()

	id := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityNormal,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	jobs, err := repo.ClaimReady(
		ctx, "worker-A", 1, 30*time.Second,
	)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim failed: %v", err)
	}

	type result struct {
		number int
		err    error
	}

	start := make(chan struct{})
	results := make(chan result, 2)

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			number, err := repo.BeginDeliveryAttempt(
				ctx, jobs[0],
			)

			results <- result{number: number, err: err}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	started := 0
	rejected := 0

	for result := range results {
		switch {
		case result.err == nil && result.number == 1:
			started++

		case errors.Is(result.err, storage.ErrAttemptAlreadyStarted):
			rejected++

		default:
			t.Errorf("unexpected start result: %+v", result)
		}
	}

	if started != 1 || rejected != 1 {
		t.Errorf(
			"expected one start and one rejection, got %d and %d",
			started, rejected,
		)
	}

	var attemptCount, auditCount int

	err = pool.QueryRow(ctx, `
		SELECT
			n.attempt_count,
			(
				SELECT COUNT(*)
				FROM delivery_attempts
				WHERE notification_id = n.id
			)
		FROM notifications AS n
		WHERE n.id = $1::uuid
	`, id).Scan(&attemptCount, &auditCount)

	if err != nil {
		t.Fatal(err)
	}

	if attemptCount != 1 || auditCount != 1 {
		t.Errorf(
			"expected one attempt and one audit row, got %d and %d",
			attemptCount, auditCount,
		)
	}
}

func TestWorkerDeliveryPipelineIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	const totalNotifications = 3

	for i := 0; i < totalNotifications; i++ {
		insertClaimTestNotification(
			t, ctx, repo, pool,
			notification.PriorityNormal,
			notification.StatusPending,
			time.Now().Add(-time.Minute),
		)
	}

	sender := worker.SenderFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) (notification.DeliveryResult, error) {
			return notification.DeliveryResult{
				Outcome:           notification.OutcomeAccepted,
				HTTPStatus:        202,
				ProviderMessageID: "test-" + job.ID,
			}, nil
		},
	)

	processor, err := worker.NewDeliveryProcessor(
		repo, sender, 5*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	runner, err := worker.NewRunner(
		repo,
		processor,
		worker.Options{
			Concurrency:       2,
			PollInterval:      20 * time.Millisecond,
			LeaseDuration:     10 * time.Second,
			ProcessingTimeout: 5 * time.Second,
			WorkerIDPrefix:    "delivery-test",
		},
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}

	recovery, err := worker.NewRecoveryLoop(
		repo,
		worker.RecoveryOptions{
			Interval:  50 * time.Millisecond,
			BatchSize: 10,
		},
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}

	service, err := worker.NewService(runner, recovery)
	if err != nil {
		t.Fatal(err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	finished := make(chan struct{})

	go func() {
		service.Run(runCtx)
		close(finished)
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		var accepted int

		err := pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM notifications
			WHERE status = 'accepted'
		`).Scan(&accepted)

		if err != nil {
			stop()
			<-finished
			t.Fatal(err)
		}

		if accepted == totalNotifications {
			break
		}

		select {
		case <-ctx.Done():
			stop()
			<-finished
			t.Fatal("worker pipeline did not finish all notifications")
		case <-ticker.C:
		}
	}

	stop()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("worker service did not shut down")
	}

	var acceptedAttempts, activeAttempts int

	err = pool.QueryRow(
		context.Background(),
		`
		SELECT
			COUNT(*) FILTER (WHERE outcome = 'accepted'),
			COUNT(*) FILTER (WHERE outcome = 'started')
		FROM delivery_attempts
	`,
	).Scan(&acceptedAttempts, &activeAttempts)

	if err != nil {
		t.Fatal(err)
	}

	if acceptedAttempts != totalNotifications || activeAttempts != 0 {
		t.Errorf(
			"unexpected audit counts: accepted=%d started=%d",
			acceptedAttempts, activeAttempts,
		)
	}

	var processingCount int

	err = pool.QueryRow(
		context.Background(),
		`
		SELECT COUNT(*)
		FROM notifications
		WHERE status = 'processing'
	`,
	).Scan(&processingCount)

	if err != nil {
		t.Fatal(err)
	}

	if processingCount != 0 {
		t.Errorf("expected no processing notifications, got %d",
			processingCount)
	}
}

func TestWorkerUncertainDeliveryIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer cancel()

	id := insertClaimTestNotification(
		t, ctx, repo, pool,
		notification.PriorityHigh,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	jobs, err := repo.ClaimReady(
		ctx, "uncertain-worker", 1, 30*time.Second,
	)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim failed: %v", err)
	}

	sender := worker.SenderFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) (notification.DeliveryResult, error) {
			return notification.DeliveryResult{},
				errors.New("connection reset before response")
		},
	)

	processor, err := worker.NewDeliveryProcessor(
		repo, sender, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = processor.Process(ctx, jobs[0])
	if err == nil {
		t.Fatal("expected uncertain delivery error")
	}

	var storedStatus, attemptOutcome string

	err = pool.QueryRow(ctx, `
		SELECT n.status, a.outcome
		FROM notifications AS n
		JOIN delivery_attempts AS a
			ON a.notification_id = n.id
		WHERE n.id = $1::uuid
	`, id).Scan(&storedStatus, &attemptOutcome)

	if err != nil {
		t.Fatal(err)
	}

	if storedStatus != "retrying" ||
		attemptOutcome != "unknown" {
		t.Errorf(
			"expected retrying/unknown, got %s/%s",
			storedStatus, attemptOutcome,
		)
	}
}
