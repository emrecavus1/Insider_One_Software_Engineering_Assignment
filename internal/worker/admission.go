package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type AdmissionLimiter interface {
	Allow(
		context.Context,
		notification.Channel,
	) (bool, time.Duration, error)
}

type ClaimDeferrer interface {
	DeferClaim(
		context.Context,
		notification.ClaimedNotification,
		time.Duration,
	) error
}

func NewRateLimitedDeliveryProcessor(
	store AttemptStore,
	sender Sender,
	limiter AdmissionLimiter,
	retryDelay time.Duration,
) (*DeliveryProcessor, error) {
	if limiter == nil {
		return nil, fmt.Errorf("admission limiter is required")
	}

	deferrer, ok := store.(ClaimDeferrer)
	if !ok {
		return nil, fmt.Errorf(
			"attempt store does not support claim deferral",
		)
	}

	processor, err := NewDeliveryProcessor(
		store, sender, retryDelay,
	)
	if err != nil {
		return nil, err
	}

	processor.limiter = limiter
	processor.deferrer = deferrer

	return processor, nil
}

// reserveCapacity controls admission before recording a delivery attempt
func (p *DeliveryProcessor) reserveCapacity(
	ctx context.Context,
	job notification.ClaimedNotification,
) (bool, error) {
	allowed, delay, limitErr := p.limiter.Allow(
		ctx, job.Channel,
	)

	if limitErr == nil && allowed {
		return true, nil
	}

	if limitErr != nil {
		delay = time.Second
	}

	if delay < time.Millisecond {
		delay = time.Millisecond
	}

	deferCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		3*time.Second,
	)
	defer cancel()

	if err := p.deferrer.DeferClaim(
		deferCtx, job, delay,
	); err != nil {
		if limitErr != nil {
			return false, errors.Join(
				fmt.Errorf("Redis admission: %w", limitErr),
				fmt.Errorf("defer claim: %w", err),
			)
		}

		return false, fmt.Errorf("defer claim: %w", err)
	}

	slog.InfoContext(ctx, "delivery_deferred",
		"correlation_id", job.ID,
		"notification_id", job.ID,
		"channel", job.Channel,
		"delay_ms", delay.Milliseconds(),
		"limiter_unavailable", limitErr != nil,
	)
	if limitErr != nil {
		return false, fmt.Errorf(
			"Redis admission unavailable: %w", limitErr,
		)
	}

	return false, nil
}
