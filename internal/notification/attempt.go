package notification

import "time"

type AttemptOutcome string

const (
	OutcomeStarted          AttemptOutcome = "started"
	OutcomeAccepted         AttemptOutcome = "accepted"
	OutcomeRetryableFailure AttemptOutcome = "retryable_failure"
	OutcomePermanentFailure AttemptOutcome = "permanent_failure"
	OutcomeUnknown          AttemptOutcome = "unknown"
)

type DeliveryResult struct {
	Outcome           AttemptOutcome
	HTTPStatus        int
	ProviderMessageID string
	ErrorMessage      string
	RetryAfter        time.Duration
}
