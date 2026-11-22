package notification

import "time"

// ClaimedNotification represents a notification reserved by a worker for processing
type ClaimedNotification struct {
	Notification

	AttemptCount   int       `json:"attempt_count"`
	MaxAttempts    int       `json:"max_attempts"`
	NextAttemptAt  time.Time `json:"next_attempt_at"`
	WorkerID       string    `json:"worker_id"`
	ClaimToken     string    `json:"claim_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

// LeaseRecoveryResult summarizes one recovery pass
type LeaseRecoveryResult struct {
	Requeued int64 `json:"requeued"`
	Failed   int64 `json:"failed"`
}
