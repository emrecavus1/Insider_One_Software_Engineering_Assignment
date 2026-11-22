package notification

// BatchCreateResult represents a successfully created batch
type BatchCreateResult struct {
	BatchID         string   `json:"batch_id"`
	Count           int      `json:"count"`
	NotificationIDs []string `json:"notification_ids"`
}
