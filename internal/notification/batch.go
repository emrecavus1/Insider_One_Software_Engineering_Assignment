package notification

import "time"

// Result returned after creating a batch.
type BatchCreateResult struct {
	BatchID         string   `json:"batch_id"`
	Count           int      `json:"count"`
	NotificationIDs []string `json:"notification_ids"`
}

// Number of notifications in each lifecycle status.
type BatchStatusCounts struct {
	Pending    int64 `json:"pending"`
	Processing int64 `json:"processing"`
	Retrying   int64 `json:"retrying"`
	Accepted   int64 `json:"accepted"`
	Failed     int64 `json:"failed"`
	Cancelled  int64 `json:"cancelled"`
}

// Summary returned when querying a batch.
type BatchStatus struct {
	BatchID      string            `json:"batch_id"`
	Total        int64             `json:"total"`
	StatusCounts BatchStatusCounts `json:"status_counts"`
	CreatedAt    time.Time         `json:"created_at"`
}
