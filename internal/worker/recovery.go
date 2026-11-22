package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

// LeaseRecoverer abstracts the database recovery operation
type LeaseRecoverer interface {
	RecoverExpiredLeases(
		context.Context,
		int,
	) (notification.LeaseRecoveryResult, error)
}

type RecoveryOptions struct {
	Interval  time.Duration
	BatchSize int
}

type RecoveryLoop struct {
	recoverer LeaseRecoverer
	options   RecoveryOptions
	logger    *slog.Logger
}

func NewRecoveryLoop(
	recoverer LeaseRecoverer,
	options RecoveryOptions,
	logger *slog.Logger,
) (*RecoveryLoop, error) {

	if recoverer == nil {
		return nil, fmt.Errorf("lease recoverer is required")
	}

	if options.Interval < 10*time.Millisecond ||
		options.Interval > time.Hour {
		return nil, fmt.Errorf(
			"recovery interval must be between 10ms and 1 hour",
		)
	}

	if options.BatchSize < 1 || options.BatchSize > 1000 {
		return nil, fmt.Errorf(
			"recovery batch size must be between 1 and 1000",
		)
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &RecoveryLoop{
		recoverer: recoverer,
		options:   options,
		logger:    logger,
	}, nil
}

// Run performs recovery immediately, then periodically until the context is cancelled
func (r *RecoveryLoop) Run(ctx context.Context) {
	ticker := time.NewTicker(r.options.Interval)
	defer ticker.Stop()

	r.logger.Info(
		"lease recovery loop started",
		"interval", r.options.Interval.String(),
		"batch_size", r.options.BatchSize,
	)

	for {
		if ctx.Err() != nil {
			break
		}

		result, err := r.recoverer.RecoverExpiredLeases(
			ctx,
			r.options.BatchSize,
		)

		if err != nil {
			if ctx.Err() == nil {
				r.logger.Error(
					"lease recovery failed",
					"error", err,
				)
			}
		} else if result.Requeued > 0 || result.Failed > 0 {
			r.logger.Info(
				"expired leases recovered",
				"requeued", result.Requeued,
				"failed", result.Failed,
			)
		}

		select {
		case <-ctx.Done():
			r.logger.Info("lease recovery loop stopped")
			return
		case <-ticker.C:
		}
	}

	r.logger.Info("lease recovery loop stopped")
}
