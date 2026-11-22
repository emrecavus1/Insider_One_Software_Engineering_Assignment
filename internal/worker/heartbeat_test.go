package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type heartbeatTestClaimer struct {
	fakeClaimer

	mu           sync.Mutex
	renewalCount int
	renewErr     error
}

func (f *heartbeatTestClaimer) RenewLease(
	ctx context.Context,
	notificationID string,
	claimToken string,
	duration time.Duration,
) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.renewalCount++

	if f.renewErr != nil {
		return time.Time{}, f.renewErr
	}

	return time.Now().Add(duration), nil
}

func (f *heartbeatTestClaimer) renewals() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.renewalCount
}

func heartbeatTestOptions() Options {
	options := testOptions()

	options.Concurrency = 1
	options.LeaseDuration = 2 * time.Second
	options.ProcessingTimeout = 4 * time.Second
	options.HeartbeatInterval = 100 * time.Millisecond

	return options
}

func heartbeatTestJob() notification.ClaimedNotification {
	return notification.ClaimedNotification{
		Notification: notification.Notification{
			ID: "heartbeat-test-job",
		},
		ClaimToken: "heartbeat-test-token",
	}
}

func TestHeartbeatRenewsDuringProcessing(t *testing.T) {
	store := &heartbeatTestClaimer{}

	processor := ProcessorFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) error {
			timer := time.NewTimer(450 * time.Millisecond)
			defer timer.Stop()

			select {
			case <-timer.C:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	)

	runner, err := NewRunner(
		store,
		processor,
		heartbeatTestOptions(),
		testLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	if err := runner.processWithLeaseRenewal(
		ctx,
		heartbeatTestJob(),
	); err != nil {
		t.Fatalf("processing failed: %v", err)
	}

	if store.renewals() < 2 {
		t.Errorf(
			"expected multiple lease renewals, got %d",
			store.renewals(),
		)
	}
}

func TestHeartbeatFailureCancelsProcessing(t *testing.T) {
	errOwnershipLost := errors.New("claim ownership lost")

	store := &heartbeatTestClaimer{
		renewErr: errOwnershipLost,
	}

	processor := ProcessorFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) error {
			<-ctx.Done()
			return ctx.Err()
		},
	)

	runner, err := NewRunner(
		store,
		processor,
		heartbeatTestOptions(),
		testLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	err = runner.processWithLeaseRenewal(
		ctx,
		heartbeatTestJob(),
	)

	if !errors.Is(err, errOwnershipLost) {
		t.Errorf(
			"expected ownership error, got %v",
			err,
		)
	}

	if store.renewals() != 1 {
		t.Errorf(
			"expected one failed renewal, got %d",
			store.renewals(),
		)
	}
}

func TestHeartbeatConfigurationValidation(t *testing.T) {
	store := &heartbeatTestClaimer{}

	processor := ProcessorFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) error {
			return nil
		},
	)

	tests := []struct {
		name   string
		modify func(*Options)
	}{
		{
			name: "heartbeat interval too short",
			modify: func(o *Options) {
				o.HeartbeatInterval = 10 * time.Millisecond
			},
		},
		{
			name: "heartbeat interval too long",
			modify: func(o *Options) {
				o.HeartbeatInterval = time.Second
			},
		},
		{
			name: "processing exceeds lease without heartbeat",
			modify: func(o *Options) {
				o.HeartbeatInterval = 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := heartbeatTestOptions()
			tt.modify(&options)

			_, err := NewRunner(
				store,
				processor,
				options,
				testLogger(),
			)

			if err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}

	t.Run("claimer without renewal support", func(t *testing.T) {
		_, err := NewRunner(
			&fakeClaimer{},
			processor,
			heartbeatTestOptions(),
			testLogger(),
		)

		if err == nil {
			t.Fatal("expected missing renewal support error")
		}
	})

	t.Run("long processing with heartbeat", func(t *testing.T) {
		_, err := NewRunner(
			store,
			processor,
			heartbeatTestOptions(),
			testLogger(),
		)

		if err != nil {
			t.Fatalf("valid heartbeat configuration rejected: %v", err)
		}
	})
}
