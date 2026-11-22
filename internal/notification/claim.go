package notification

import "time"

// ClaimedNotification represents a notification reserved by a worker for processing
type ClaimedNotification struct {
	Notification

	AttemptCount   int       `json:"attempt_count"`
	MaxAttempts    int       `json:"max_attempts"`
	NextAttemptAt  time.Time `json:"next_attempt_at"`
	WorkerID       string    `json:"worker_id"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}
