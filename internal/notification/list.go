// This file is a foundation for our pagination system
package notification

import "time"

// ListFilter contains optional filters and pagination settings
type ListFilter struct {
	Status      Status
	Channel     Channel
	CreatedFrom *time.Time
	CreatedTo   *time.Time
	Page        int
	Limit       int
}

// ListResult represents a paginated response
type ListResult struct {
	Notifications []Notification `json:"notifications"`
	Total         int64          `json:"total"`
	Page          int            `json:"page"`
	Limit         int            `json:"limit"`
}
