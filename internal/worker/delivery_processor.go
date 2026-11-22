package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type AttemptStore interface {
	BeginDeliveryAttempt(
		context.Context,
		notification.ClaimedNotification,
	) (int, error)

	FinishDeliveryAttempt(
		context.Context,
		notification.ClaimedNotification,
		int,
		notification.DeliveryResult,
		time.Duration,
	) (notification.Status, error)
}

type Sender interface {
	Send(
		context.Context,
		notification.ClaimedNotification,
	) (notification.DeliveryResult, error)
}

type SenderFunc func(
	context.Context,
	notification.ClaimedNotification,
) (notification.DeliveryResult, error)

func (f SenderFunc) Send(
	ctx context.Context,
	job notification.ClaimedNotification,
) (notification.DeliveryResult, error) {
	return f(ctx, job)
}

type DeliveryProcessor struct {
	store      AttemptStore
	sender     Sender
	retryDelay time.Duration
}

func NewDeliveryProcessor(
	store AttemptStore,
	sender Sender,
	retryDelay time.Duration,
) (*DeliveryProcessor, error) {
	if store == nil || sender == nil {
		return nil, fmt.Errorf("attempt store and sender are required")
	}

	if retryDelay < time.Second || retryDelay > 24*time.Hour {
		return nil, fmt.Errorf("invalid retry delay")
	}

	return &DeliveryProcessor{
		store:      store,
		sender:     sender,
		retryDelay: retryDelay,
	}, nil
}

// Process records an attempt, invokes the sender, and persists the outcome
func (p *DeliveryProcessor) Process(
	ctx context.Context,
	job notification.ClaimedNotification,
) error {
	attemptNumber, err := p.store.BeginDeliveryAttempt(ctx, job)
	if err != nil {
		return fmt.Errorf("begin delivery attempt: %w", err)
	}

	var result notification.DeliveryResult
	var sendErr error

	if err := ctx.Err(); err != nil {
		sendErr = err
	} else {
		result, sendErr = p.sender.Send(ctx, job)
	}

	if sendErr != nil {
		result.Outcome = notification.OutcomeUnknown
		result.ErrorMessage = sendErr.Error()
	}

	if !validDeliveryOutcome(result.Outcome) {
		sendErr = errors.New("sender returned invalid delivery outcome")
		result.Outcome = notification.OutcomeUnknown
		result.ErrorMessage = sendErr.Error()
	}

	// Preserving a bounded opportunity to record uncertain outcomes
	finishCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		3*time.Second,
	)
	defer cancel()

	_, err = p.store.FinishDeliveryAttempt(
		finishCtx,
		job,
		attemptNumber,
		result,
		p.retryDelay,
	)

	if err != nil {
		return fmt.Errorf("finish delivery attempt: %w", err)
	}

	if sendErr != nil {
		return fmt.Errorf("delivery outcome uncertain: %w", sendErr)
	}

	return nil
}

func validDeliveryOutcome(outcome notification.AttemptOutcome) bool {
	switch outcome {
	case notification.OutcomeAccepted,
		notification.OutcomeRetryableFailure,
		notification.OutcomePermanentFailure,
		notification.OutcomeUnknown:
		return true
	default:
		return false
	}
}
