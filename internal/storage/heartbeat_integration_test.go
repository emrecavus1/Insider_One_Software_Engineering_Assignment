package storage_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/worker"
)

func TestWorkerHeartbeatIntegration(t *testing.T) {
	repo, pool := setupClaimTest(t)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		12*time.Second,
	)
	defer cancel()

	id := insertClaimTestNotification(
		t,
		ctx,
		repo,
		pool,
		notification.PriorityHigh,
		notification.StatusPending,
		time.Now().Add(-time.Minute),
	)

	var senderCalls atomic.Int32

	sender := worker.SenderFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) (notification.DeliveryResult, error) {
			senderCalls.Add(1)

			timer := time.NewTimer(2500 * time.Millisecond)
			defer timer.Stop()

			select {
			case <-ctx.Done():
				return notification.DeliveryResult{}, ctx.Err()

			case <-timer.C:
				return notification.DeliveryResult{
					Outcome:           notification.OutcomeAccepted,
					HTTPStatus:        202,
					ProviderMessageID: "heartbeat-test-provider",
				}, nil
			}
		},
	)

	processor, err := worker.NewDeliveryProcessor(
		repo,
		sender,
		5*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)

	runner, err := worker.NewRunner(
		repo,
		processor,
		worker.Options{
			Concurrency:       1,
			PollInterval:      20 * time.Millisecond,
			LeaseDuration:     2 * time.Second,
			ProcessingTimeout: 5 * time.Second,
			HeartbeatInterval: 200 * time.Millisecond,
			WorkerIDPrefix:    "heartbeat-integration",
		},
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}

	recovery, err := worker.NewRecoveryLoop(
		repo,
		worker.RecoveryOptions{
			Interval:  100 * time.Millisecond,
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

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		stored, err := repo.GetByID(ctx, id)
		if err != nil {
			stop()
			<-finished
			t.Fatal(err)
		}

		if stored.Status == notification.StatusAccepted {
			break
		}

		select {
		case <-ctx.Done():
			stop()
			<-finished
			t.Fatal("notification was not accepted")
		case <-ticker.C:
		}
	}

	stop()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("worker service did not stop")
	}

	var attemptCount, auditCount, acceptedCount int

	err = pool.QueryRow(
		context.Background(),
		`
		SELECT
			n.attempt_count,
			(
				SELECT COUNT(*)
				FROM delivery_attempts
				WHERE notification_id = n.id
			),
			(
				SELECT COUNT(*)
				FROM delivery_attempts
				WHERE notification_id = n.id
				  AND outcome = 'accepted'
			)
		FROM notifications AS n
		WHERE n.id = $1::uuid
	`,
		id,
	).Scan(
		&attemptCount,
		&auditCount,
		&acceptedCount,
	)

	if err != nil {
		t.Fatal(err)
	}

	if attemptCount != 1 {
		t.Errorf("expected one attempt, got %d", attemptCount)
	}

	if auditCount != 1 || acceptedCount != 1 {
		t.Errorf(
			"expected one accepted audit record, got total=%d accepted=%d",
			auditCount,
			acceptedCount,
		)
	}

	if senderCalls.Load() != 1 {
		t.Errorf(
			"expected one sender invocation, got %d",
			senderCalls.Load(),
		)
	}
}
