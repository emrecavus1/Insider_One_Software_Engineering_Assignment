package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type fakeLeaseRecoverer struct {
	mu              sync.Mutex
	calls           int
	errorsRemaining int
	results         notification.LeaseRecoveryResult
	called          chan struct{}
}

func (f *fakeLeaseRecoverer) RecoverExpiredLeases(
	ctx context.Context,
	limit int,
) (notification.LeaseRecoveryResult, error) {
	f.mu.Lock()
	f.calls++

	fail := f.errorsRemaining > 0
	if fail {
		f.errorsRemaining--
	}

	result := f.results
	f.mu.Unlock()

	if f.called != nil {
		select {
		case f.called <- struct{}{}:
		default:
		}
	}

	if err := ctx.Err(); err != nil {
		return notification.LeaseRecoveryResult{}, err
	}

	if fail {
		return notification.LeaseRecoveryResult{},
			errors.New("temporary PostgreSQL failure")
	}

	return result, nil
}

func (f *fakeLeaseRecoverer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestRecoveryLoopRunsAndStops(t *testing.T) {
	store := &fakeLeaseRecoverer{
		results: notification.LeaseRecoveryResult{
			Requeued: 2,
			Failed:   1,
		},
		called: make(chan struct{}, 10),
	}

	loop, err := NewRecoveryLoop(
		store,
		RecoveryOptions{
			Interval:  20 * time.Millisecond,
			BatchSize: 10,
		},
		testLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finished := make(chan struct{})

	go func() {
		loop.Run(ctx)
		close(finished)
	}()

	// Confirming at least two recovery passes occur
	for i := 0; i < 2; i++ {
		select {
		case <-store.called:
		case <-time.After(2 * time.Second):
			cancel()
			<-finished
			t.Fatal("recovery loop did not poll")
		}
	}

	cancel()

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery loop did not stop")
	}

	if store.callCount() < 2 {
		t.Error("expected repeated recovery attempts")
	}
}

func TestRecoveryLoopContinuesAfterError(t *testing.T) {
	store := &fakeLeaseRecoverer{
		errorsRemaining: 1,
		called:          make(chan struct{}, 10),
	}

	loop, err := NewRecoveryLoop(
		store,
		RecoveryOptions{
			Interval:  20 * time.Millisecond,
			BatchSize: 5,
		},
		testLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finished := make(chan struct{})

	go func() {
		loop.Run(ctx)
		close(finished)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-store.called:
		case <-time.After(2 * time.Second):
			cancel()
			<-finished
			t.Fatal("recovery loop stopped after database error")
		}
	}

	cancel()
	<-finished

	if store.callCount() < 2 {
		t.Error("expected recovery to continue after error")
	}
}

func TestRecoveryLoopInvalidConfiguration(t *testing.T) {
	store := &fakeLeaseRecoverer{}

	tests := []RecoveryOptions{
		{Interval: 0, BatchSize: 10},
		{Interval: time.Second, BatchSize: 0},
		{Interval: time.Second, BatchSize: 1001},
	}

	for _, options := range tests {
		if _, err := NewRecoveryLoop(
			store, options, testLogger(),
		); err == nil {
			t.Errorf("expected error for options %+v", options)
		}
	}
}
