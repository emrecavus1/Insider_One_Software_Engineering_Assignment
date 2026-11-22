package worker

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"time"
)

type RetryPolicy struct {
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	JitterFraction float64
}

func (p RetryPolicy) Validate() error {
	if p.BaseDelay < time.Second || p.MaxDelay < p.BaseDelay ||
		p.MaxDelay > 24*time.Hour {
		return fmt.Errorf("retry delay must satisfy 1s <= base <= max <= 24h")
	}
	if p.JitterFraction < 0 || p.JitterFraction > 0.5 {
		return fmt.Errorf("retry jitter fraction must be between 0 and 0.5")
	}
	return nil
}

// Delay calculates bounded exponential backoff with stable per-notification jitter
func (p RetryPolicy) Delay(id string, attempt int, retryAfter time.Duration) time.Duration {
	delay := p.BaseDelay
	for i := 1; i < attempt && delay < p.MaxDelay; i++ {
		if delay > p.MaxDelay/2 {
			delay = p.MaxDelay
			break
		}
		delay *= 2
	}

	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}

	if p.JitterFraction > 0 && delay < p.MaxDelay {
		capacity := time.Duration(float64(delay) * p.JitterFraction)
		if capacity > p.MaxDelay-delay {
			capacity = p.MaxDelay - delay
		}
		if capacity > 0 {
			h := fnv.New64a()
			_, _ = h.Write([]byte(id))
			_, _ = h.Write([]byte(":" + strconv.Itoa(attempt)))
			delay += time.Duration(h.Sum64() % (uint64(capacity) + 1))
		}
	}

	if retryAfter > delay {
		delay = retryAfter
	}
	if delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

// SetRetryPolicy configures delivery backoff before the processor begins serving jobs
func (p *DeliveryProcessor) SetRetryPolicy(policy RetryPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	p.retryPolicy = &policy
	return nil
}
