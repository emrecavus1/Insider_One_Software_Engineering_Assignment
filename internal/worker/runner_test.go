package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type fakeClaimer struct {
	mu              sync.Mutex
	jobs            []notification.ClaimedNotification
	calls           int
	errorsRemaining int
}

func (f *fakeClaimer) ClaimReady(
	ctx context.Context,
	workerID string,
	limit int,
	leaseDuration time.Duration,
) ([]notification.ClaimedNotification, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++

	if f.errorsRemaining > 0 {
		f.errorsRemaining--
		return nil, errors.New("temporary database failure")
	}

	if len(f.jobs) == 0 {
		return nil, nil
	}

	job := f.jobs[0]
	f.jobs = f.jobs[1:]
	job.WorkerID = workerID

	return []notification.ClaimedNotification{job}, nil
}

func (f *fakeClaimer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func testOptions() Options {
	return Options{
		Concurrency:       3,
		PollInterval:      20 * time.Millisecond,
		LeaseDuration:     5 * time.Second,
		ProcessingTimeout: 2 * time.Second,
		WorkerIDPrefix:    "test-worker",
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunnerProcessesConcurrently(t *testing.T) {
	queue := &fakeClaimer{
		jobs: []notification.ClaimedNotification{
			{Notification: notification.Notification{ID: "job-1"}},
			{Notification: notification.Notification{ID: "job-2"}},
			{Notification: notification.Notification{ID: "job-3"}},
		},
	}

	started := make(chan notification.ClaimedNotification, 3)
	release := make(chan struct{})

	processor := ProcessorFunc(
		func(ctx context.Context, job notification.ClaimedNotification) error {
			started <- job

			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	)

	runner, err := NewRunner(
		queue,
		processor,
		testOptions(),
		testLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finished := make(chan struct{})

	go func() {
		runner.Run(ctx)
		close(finished)
	}()

	received := make(map[string]bool)
	workers := make(map[string]bool)

	// All three handlers must start before we release any of them, proving they run concurrently
	for i := 0; i < 3; i++ {
		select {
		case job := <-started:
			if received[job.ID] {
				t.Errorf("duplicate job: %s", job.ID)
			}

			received[job.ID] = true
			workers[job.WorkerID] = true

		case <-time.After(3 * time.Second):
			cancel()
			close(release)
			<-finished
			t.Fatal("workers did not process concurrently")
		}
	}

	// Stopping new claims, then releasing active handlers
	cancel()
	close(release)

	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not shut down")
	}

	if len(received) != 3 {
		t.Errorf("expected 3 processed jobs, got %d", len(received))
	}

	if len(workers) != 3 {
		t.Errorf(
			"expected 3 distinct workers, got %d",
			len(workers),
		)
	}
}

func TestRunnerRetriesAfterClaimFailure(t *testing.T) {
	queue := &fakeClaimer{
		errorsRemaining: 1,
		jobs: []notification.ClaimedNotification{
			{Notification: notification.Notification{ID: "retry-job"}},
		},
	}

	processed := make(chan string, 1)

	processor := ProcessorFunc(
		func(ctx context.Context, job notification.ClaimedNotification) error {
			processed <- job.ID
			return nil
		},
	)

	options := testOptions()
	options.Concurrency = 1

	runner, err := NewRunner(
		queue,
		processor,
		options,
		testLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finished := make(chan struct{})

	go func() {
		runner.Run(ctx)
		close(finished)
	}()

	select {
	case id := <-processed:
		if id != "retry-job" {
			t.Errorf("unexpected job: %s", id)
		}

	case <-time.After(3 * time.Second):
		cancel()
		<-finished
		t.Fatal("worker did not recover from claiming error")
	}

	cancel()
	<-finished

	if queue.callCount() < 2 {
		t.Error("worker should retry after a claiming failure")
	}
}

func TestRunnerWaitsWhenQueueIsEmpty(t *testing.T) {
	queue := &fakeClaimer{}

	processor := ProcessorFunc(
		func(ctx context.Context, job notification.ClaimedNotification) error {
			t.Error("processor should not run on an empty queue")
			return nil
		},
	)

	options := testOptions()
	options.Concurrency = 1
	options.PollInterval = 25 * time.Millisecond

	runner, err := NewRunner(
		queue,
		processor,
		options,
		testLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		120*time.Millisecond,
	)
	defer cancel()

	runner.Run(ctx)

	calls := queue.callCount()

	if calls == 0 {
		t.Error("expected at least one polling attempt")
	}

	if calls > 10 {
		t.Errorf(
			"worker appears to be busy-looping: %d polls",
			calls,
		)
	}
}

func TestRunnerRejectsInvalidConfiguration(t *testing.T) {
	queue := &fakeClaimer{}

	processor := ProcessorFunc(
		func(ctx context.Context, job notification.ClaimedNotification) error {
			return nil
		},
	)

	tests := []struct {
		name   string
		change func(*Options)
	}{
		{
			name: "zero concurrency",
			change: func(o *Options) {
				o.Concurrency = 0
			},
		},
		{
			name: "too many workers",
			change: func(o *Options) {
				o.Concurrency = 100
			},
		},
		{
			name: "zero polling interval",
			change: func(o *Options) {
				o.PollInterval = 0
			},
		},
		{
			name: "processing timeout exceeds lease",
			change: func(o *Options) {
				o.ProcessingTimeout = o.LeaseDuration
			},
		},
		{
			name: "empty worker prefix",
			change: func(o *Options) {
				o.WorkerIDPrefix = ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := testOptions()
			tt.change(&options)

			_, err := NewRunner(
				queue,
				processor,
				options,
				testLogger(),
			)

			if err == nil {
				t.Error("expected configuration validation error")
			}
		})
	}
}
