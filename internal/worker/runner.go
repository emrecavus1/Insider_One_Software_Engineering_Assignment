package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

// Claimer defines the database operation from worker claims
type Claimer interface {
	ClaimReady(
		context.Context,
		string,
		int,
		time.Duration,
	) ([]notification.ClaimedNotification, error)
}

// Processor handles an already claimed notification
type Processor interface {
	Process(context.Context, notification.ClaimedNotification) error
}

// ProcessorFunc lets us inject functions in tests
type ProcessorFunc func(context.Context, notification.ClaimedNotification) error

func (f ProcessorFunc) Process(
	ctx context.Context,
	job notification.ClaimedNotification,
) error {
	return f(ctx, job)
}

type Options struct {
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	ProcessingTimeout time.Duration
	HeartbeatInterval time.Duration
	WorkerIDPrefix    string
}

type Runner struct {
	claimer   Claimer
	processor Processor
	options   Options
	logger    *slog.Logger
}

func NewRunner(
	claimer Claimer,
	processor Processor,
	options Options,
	logger *slog.Logger,
) (*Runner, error) {

	if claimer == nil || processor == nil {
		return nil, fmt.Errorf("claimer and processor are required")
	}

	if options.Concurrency < 1 || options.Concurrency > 32 {
		return nil, fmt.Errorf("concurrency must be between 1 and 32")
	}

	if options.PollInterval < 10*time.Millisecond ||
		options.PollInterval > time.Minute {
		return nil, fmt.Errorf(
			"poll interval must be between 10ms and 1 minute",
		)
	}

	if options.LeaseDuration < 2*time.Second ||
		options.LeaseDuration > 24*time.Hour {
		return nil, fmt.Errorf(
			"lease duration must be between 2 seconds and 24 hours",
		)
	}

	if options.ProcessingTimeout < time.Second ||
		options.ProcessingTimeout > 24*time.Hour {
		return nil, fmt.Errorf(
			"processing timeout must be between 1 second and 24 hours",
		)
	}

	if options.HeartbeatInterval == 0 {
		if options.ProcessingTimeout >
			options.LeaseDuration-time.Second {
			return nil, fmt.Errorf(
				"processing timeout must leave at least 1 second of lease headroom without heartbeats",
			)
		}
	} else {
		if options.HeartbeatInterval < 100*time.Millisecond ||
			options.HeartbeatInterval > options.LeaseDuration/3 {
			return nil, fmt.Errorf(
				"heartbeat interval must be between 100ms and one third of the lease duration",
			)
		}

		if _, ok := claimer.(LeaseRenewer); !ok {
			return nil, fmt.Errorf(
				"claimer does not support lease renewal",
			)
		}
	}

	prefix := strings.TrimSpace(options.WorkerIDPrefix)

	if prefix == "" ||
		len(fmt.Sprintf("%s-%d", prefix, options.Concurrency)) > 128 {
		return nil, fmt.Errorf("invalid worker ID prefix")
	}

	options.WorkerIDPrefix = prefix

	if logger == nil {
		logger = slog.Default()
	}

	return &Runner{
		claimer:   claimer,
		processor: processor,
		options:   options,
		logger:    logger,
	}, nil
}

// Run starts the configured number of worker goroutines

// It returns after all goroutines have stopped
func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < r.options.Concurrency; i++ {
		workerID := fmt.Sprintf(
			"%s-%d",
			r.options.WorkerIDPrefix,
			i+1,
		)

		wg.Add(1)

		go func(id string) {
			defer wg.Done()
			r.runWorker(ctx, id)
		}(workerID)
	}

	r.logger.Info(
		"worker pool started",
		"concurrency", r.options.Concurrency,
	)

	wg.Wait()

	r.logger.Info("worker pool stopped")
}

func (r *Runner) runWorker(
	ctx context.Context,
	workerID string,
) {
	r.logger.Info("worker started", "worker_id", workerID)

	defer r.logger.Info(
		"worker stopped",
		"worker_id", workerID,
	)

	for {
		if ctx.Err() != nil {
			return
		}

		// Claim one job per worker
		// This avoids holding multiple leases while processing sequentially
		jobs, err := r.claimer.ClaimReady(
			ctx,
			workerID,
			1,
			r.options.LeaseDuration,
		)

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			r.logger.Error(
				"notification claim failed",
				"worker_id", workerID,
				"error", err,
			)

			if !waitForNextPoll(ctx, r.options.PollInterval) {
				return
			}

			continue
		}

		if len(jobs) == 0 {
			if !waitForNextPoll(ctx, r.options.PollInterval) {
				return
			}

			continue
		}

		for _, job := range jobs {
			r.logger.Info(
				"notification claimed",
				"worker_id", workerID,
				"notification_id", job.ID,
				"correlation_id", job.ID,
				"channel", job.Channel,
				"priority", job.Priority,
			)

			// Once a job is claimed, allow an active handler to drain during graceful shutdown
			// A bounded timeout still applies
			processCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				r.options.ProcessingTimeout,
			)

			err := r.processWithLeaseRenewal(processCtx, job)
			cancel()

			if err != nil {
				r.logger.Error(
					"notification processing failed",
					"worker_id", workerID,
					"notification_id", job.ID,
					"correlation_id", job.ID,
					"error", err,
				)
			}
		}
	}
}

func waitForNextPoll(
	ctx context.Context,
	interval time.Duration,
) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
