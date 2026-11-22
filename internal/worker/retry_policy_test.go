package worker

import (
	"context"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

func TestRetryPolicyExponentialAndJitter(t *testing.T) {
	policy := RetryPolicy{BaseDelay: 2 * time.Second, MaxDelay: time.Minute, JitterFraction: 0.2}
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	for attempt, base := range map[int]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second, 4: 16 * time.Second} {
		delay := policy.Delay("job-1", attempt, 0)
		if delay < base || delay > base+base/5 {
			t.Errorf("attempt %d: unexpected delay %v", attempt, delay)
		}
		if delay != policy.Delay("job-1", attempt, 0) {
			t.Error("jitter must be stable for a given notification and attempt")
		}
	}
}

func TestRetryPolicyRespectsRetryAfterAndCap(t *testing.T) {
	policy := RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute}
	if got := policy.Delay("job", 1, 20*time.Second); got != 20*time.Second {
		t.Errorf("expected Retry-After 20s, got %v", got)
	}
	if got := policy.Delay("job", 10, 2*time.Hour); got != time.Minute {
		t.Errorf("expected maximum delay 1m, got %v", got)
	}
}

func TestRetryPolicyConfig(t *testing.T) {
	for _, p := range []RetryPolicy{
		{},
		{BaseDelay: time.Millisecond, MaxDelay: time.Minute},
		{BaseDelay: time.Minute, MaxDelay: time.Second},
		{BaseDelay: time.Second, MaxDelay: 25 * time.Hour},
		{BaseDelay: time.Second, MaxDelay: time.Minute, JitterFraction: 0.8},
	} {
		if err := p.Validate(); err == nil {
			t.Errorf("expected invalid policy: %+v", p)
		}
	}
}

type retryTestStore struct {
	nextAttempt int
	lastDelay   time.Duration
	lastResult  notification.DeliveryResult
}

func (s *retryTestStore) BeginDeliveryAttempt(context.Context, notification.ClaimedNotification) (int, error) {
	s.nextAttempt++
	return s.nextAttempt, nil
}

func (s *retryTestStore) FinishDeliveryAttempt(
	_ context.Context, _ notification.ClaimedNotification, _ int,
	result notification.DeliveryResult, delay time.Duration,
) (notification.Status, error) {
	s.lastDelay = delay
	s.lastResult = result
	return notification.StatusRetrying, nil
}

func TestDeliveryProcessorUsesRetryPolicy(t *testing.T) {
	store := &retryTestStore{}
	sender := SenderFunc(func(context.Context, notification.ClaimedNotification) (notification.DeliveryResult, error) {
		return notification.DeliveryResult{
			Outcome:    notification.OutcomeRetryableFailure,
			RetryAfter: 10 * time.Second,
		}, nil
	})
	processor, err := NewDeliveryProcessor(store, sender, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.SetRetryPolicy(RetryPolicy{BaseDelay: 2 * time.Second, MaxDelay: time.Minute}); err != nil {
		t.Fatal(err)
	}
	job := notification.ClaimedNotification{Notification: notification.Notification{ID: "job-1"}}
	if err := processor.Process(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if store.lastDelay != 10*time.Second || store.lastResult.Outcome != notification.OutcomeRetryableFailure {
		t.Errorf("unexpected retry: %+v delay=%v", store.lastResult, store.lastDelay)
	}
}
