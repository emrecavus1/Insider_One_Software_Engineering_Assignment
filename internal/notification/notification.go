package notification

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Supported delivery channels in the guidelines
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Supported notification priorities in the guidelines
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Notification lifecycle statuses
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusAccepted   Status = "accepted"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// CreateRequest represents an incoming notification
type CreateRequest struct {
	Recipient string   `json:"recipient"`
	Channel   Channel  `json:"channel"`
	Content   string   `json:"content"`
	Priority  Priority `json:"priority"`
}

// Notification represents a persisted notification
type Notification struct {
	ID        string    `json:"id"`
	BatchID   *string   `json:"batch_id,omitempty"`
	Recipient string    `json:"recipient"`
	Channel   Channel   `json:"channel"`
	Content   string    `json:"content"`
	Priority  Priority  `json:"priority"`
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// Simplified E.164 phone number validation
var phonePattern = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

// NormalizeAndValidate checks request fields and applies default values.
func (r CreateRequest) NormalizeAndValidate() (CreateRequest, error) {
	r.Recipient = strings.TrimSpace(r.Recipient)

	if r.Priority == "" {
		r.Priority = PriorityNormal
	}

	if r.Recipient == "" {
		return r, fmt.Errorf("recipient is required")
	}

	if strings.TrimSpace(r.Content) == "" {
		return r, fmt.Errorf("content is required")
	}

	switch r.Priority {
	case PriorityHigh, PriorityNormal, PriorityLow:
		// Valid priority.
	default:
		return r, fmt.Errorf("invalid priority: %s", r.Priority)
	}

	contentLength := utf8.RuneCountInString(r.Content)

	switch r.Channel {
	case ChannelSMS:
		if !phonePattern.MatchString(r.Recipient) {
			return r, fmt.Errorf("invalid SMS phone number")
		}
		if contentLength > 160 {
			return r, fmt.Errorf("SMS content exceeds 160 characters")
		}

	case ChannelEmail:
		address, err := mail.ParseAddress(r.Recipient)
		if err != nil || address.Address != r.Recipient {
			return r, fmt.Errorf("invalid email address")
		}
		if contentLength > 10000 {
			return r, fmt.Errorf("email content exceeds 10000 characters")
		}

	case ChannelPush:
		if utf8.RuneCountInString(r.Recipient) > 255 {
			return r, fmt.Errorf("push recipient token is too long")
		}
		if contentLength > 256 {
			return r, fmt.Errorf("push content exceeds 256 characters")
		}

	default:
		return r, fmt.Errorf("invalid channel: %s", r.Channel)
	}

	return r, nil
}
