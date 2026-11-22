package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

// LeaseRenewer extends ownership while a claimed job is active
type LeaseRenewer interface {
	RenewLease(
		context.Context,
		string,
		string,
		time.Duration,
	) (time.Time, error)
}

func (r *Runner) processWithLeaseRenewal(
	ctx context.Context,
	job notification.ClaimedNotification,
) error {
	if r.options.HeartbeatInterval == 0 {
		return r.processor.Process(ctx, job)
	}

	renewer := r.claimer.(LeaseRenewer)

	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()

	heartbeatDone := make(chan struct{})
	heartbeatErrors := make(chan error, 1)

	go func() {
		defer close(heartbeatDone)

		ticker := time.NewTicker(r.options.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-workCtx.Done():
				return

			case <-ticker.C:
				if workCtx.Err() != nil {
					return
				}

				renewCtx, cancelRenew := context.WithTimeout(
					workCtx,
					r.options.HeartbeatInterval,
				)

				_, err := renewer.RenewLease(
					renewCtx,
					job.ID,
					job.ClaimToken,
					r.options.LeaseDuration,
				)
				cancelRenew()

				if err != nil {
					if workCtx.Err() != nil {
						return
					}

					heartbeatErrors <- fmt.Errorf(
						"renew notification lease: %w",
						err,
					)

					cancelWork()
					return
				}
			}
		}
	}()

	processErr := r.processor.Process(workCtx, job)

	cancelWork()
	<-heartbeatDone

	select {
	case heartbeatErr := <-heartbeatErrors:
		return errors.Join(processErr, heartbeatErr)
	default:
		return processErr
	}
}
