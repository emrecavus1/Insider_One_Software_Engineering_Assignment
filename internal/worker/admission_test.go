package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type admissionTestStore struct {
	begins    int
	finishes  int
	deferrals int
	lastDelay time.Duration
}

func (s *admissionTestStore) BeginDeliveryAttempt(
	ctx context.Context,
	job notification.ClaimedNotification,
) (int, error) {
	s.begins++
	return 1, nil
}

func (s *admissionTestStore) FinishDeliveryAttempt(
	ctx context.Context,
	job notification.ClaimedNotification,
	number int,
	result notification.DeliveryResult,
	retryDelay time.Duration,
) (notification.Status, error) {
	s.finishes++
	return notification.StatusAccepted, nil
}

func (s *admissionTestStore) DeferClaim(
	ctx context.Context,
	job notification.ClaimedNotification,
	delay time.Duration,
) error {
	s.deferrals++
	s.lastDelay = delay
	return nil
}

type admissionTestLimiter struct {
	allowed bool
	delay   time.Duration
	err     error
}

func (l admissionTestLimiter) Allow(
	ctx context.Context,
	channel notification.Channel,
) (bool, time.Duration, error) {
	return l.allowed, l.delay, l.err
}

func TestRateLimitAdmission(t *testing.T) {
	redisUnavailable := errors.New("Redis unavailable")

	tests := []struct {
		name          string
		limiter       admissionTestLimiter
		wantBegins    int
		wantDeferrals int
		wantSends     int
		wantError     bool
	}{
		{
			name:          "capacity available",
			limiter:       admissionTestLimiter{allowed: true},
			wantBegins:    1,
			wantDeferrals: 0,
			wantSends:     1,
		},
		{
			name: "channel saturated",
			limiter: admissionTestLimiter{
				delay: 750 * time.Millisecond,
			},
			wantBegins:    0,
			wantDeferrals: 1,
			wantSends:     0,
		},
		{
			name: "Redis unavailable",
			limiter: admissionTestLimiter{
				err: redisUnavailable,
			},
			wantBegins:    0,
			wantDeferrals: 1,
			wantSends:     0,
			wantError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &admissionTestStore{}
			sends := 0

			sender := SenderFunc(
				func(
					ctx context.Context,
					job notification.ClaimedNotification,
				) (notification.DeliveryResult, error) {
					sends++

					return notification.DeliveryResult{
						Outcome: notification.OutcomeAccepted,
					}, nil
				},
			)

			processor, err := NewRateLimitedDeliveryProcessor(
				store,
				sender,
				tt.limiter,
				5*time.Second,
			)
			if err != nil {
				t.Fatal(err)
			}

			job := notification.ClaimedNotification{
				Notification: notification.Notification{
					ID:      "test-notification",
					Channel: notification.ChannelSMS,
				},
			}

			err = processor.Process(context.Background(), job)

			if (err != nil) != tt.wantError {
				t.Errorf("unexpected processing error: %v", err)
			}

			if store.begins != tt.wantBegins {
				t.Errorf(
					"expected %d attempts, got %d",
					tt.wantBegins, store.begins,
				)
			}

			if store.deferrals != tt.wantDeferrals {
				t.Errorf(
					"expected %d deferrals, got %d",
					tt.wantDeferrals, store.deferrals,
				)
			}

			if sends != tt.wantSends {
				t.Errorf(
					"expected %d sends, got %d",
					tt.wantSends, sends,
				)
			}

			if tt.wantDeferrals == 1 &&
				store.lastDelay <= 0 {
				t.Error("deferral must have a positive delay")
			}
		})
	}
}

func TestRateLimitedProcessorValidation(t *testing.T) {
	store := &admissionTestStore{}

	sender := SenderFunc(
		func(
			ctx context.Context,
			job notification.ClaimedNotification,
		) (notification.DeliveryResult, error) {
			return notification.DeliveryResult{
				Outcome: notification.OutcomeAccepted,
			}, nil
		},
	)

	if _, err := NewRateLimitedDeliveryProcessor(
		store, sender, nil, time.Second,
	); err == nil {
		t.Error("expected nil limiter rejection")
	}

	if _, err := NewRateLimitedDeliveryProcessor(
		store,
		sender,
		admissionTestLimiter{allowed: true},
		time.Second,
	); err != nil {
		t.Errorf("valid configuration rejected: %v", err)
	}
}
