package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

const maxResponseBytes = 64 << 10

type HTTPSender struct {
	endpoint string
	client   *http.Client
}

type requestBody struct {
	To      string               `json:"to"`
	Channel notification.Channel `json:"channel"`
	Content string               `json:"content"`
}

type acceptedBody struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// NewHTTPSender creates a bounded HTTP sender without following redirects
func NewHTTPSender(rawURL string, timeout time.Duration) (*HTTPSender, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed == nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid provider URL")
	}

	if timeout < time.Second || timeout > time.Minute {
		return nil, fmt.Errorf("provider timeout must be between 1 second and 1 minute")
	}

	return &HTTPSender{
		endpoint: parsed.String(),
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// ValidateWebhookSiteURL restricts live deliveries to a webhook.site UUID endpoint
func ValidateWebhookSiteURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed == nil || parsed.Scheme != "https" ||
		!strings.EqualFold(parsed.Hostname(), "webhook.site") ||
		parsed.Port() != "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("provider URL must be https://webhook.site/{uuid}")
	}

	path := strings.TrimPrefix(parsed.EscapedPath(), "/")
	if strings.Contains(path, "/") {
		return fmt.Errorf("provider URL must contain one UUID path component")
	}

	if _, err := uuid.Parse(path); err != nil {
		return fmt.Errorf("invalid webhook.site token UUID: %w", err)
	}

	return nil
}

// Send submits one notification and classifies the provider response
func (s *HTTPSender) Send(
	ctx context.Context,
	job notification.ClaimedNotification,
) (notification.DeliveryResult, error) {
	payload, err := json.Marshal(requestBody{
		To:      job.Recipient,
		Channel: job.Channel,
		Content: job.Content,
	})
	if err != nil {
		return notification.DeliveryResult{}, fmt.Errorf("encode provider request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, s.endpoint, bytes.NewReader(payload),
	)
	if err != nil {
		return notification.DeliveryResult{}, fmt.Errorf("create provider request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Notification-ID", job.ID)
	req.Header.Set("Idempotency-Key", job.ID)
	req.Header.Set("X-Delivery-Attempt", strconv.Itoa(job.AttemptCount+1))

	resp, err := s.client.Do(req)
	if err != nil {
		return notification.DeliveryResult{}, fmt.Errorf("send provider request: %w", err)
	}
	defer resp.Body.Close()

	result := notification.DeliveryResult{HTTPStatus: resp.StatusCode}

	if resp.StatusCode != http.StatusAccepted {
		result.ErrorMessage = fmt.Sprintf("provider returned HTTP %d", resp.StatusCode)

		switch {
		case resp.StatusCode == http.StatusRequestTimeout,
			resp.StatusCode == http.StatusTooEarly,
			resp.StatusCode == http.StatusTooManyRequests,
			resp.StatusCode >= 500:
			result.Outcome = notification.OutcomeRetryableFailure
			result.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		case resp.StatusCode >= 400 && resp.StatusCode < 500,
			resp.StatusCode >= 300 && resp.StatusCode < 400:
			result.Outcome = notification.OutcomePermanentFailure
		default:
			result.Outcome = notification.OutcomeUnknown
		}
		return result, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		result.Outcome = notification.OutcomeUnknown
		result.ErrorMessage = fmt.Sprintf("read provider acceptance: %v", err)
		return result, nil
	}
	if len(body) > maxResponseBytes {
		result.Outcome = notification.OutcomeUnknown
		result.ErrorMessage = "provider acceptance response too large"
		return result, nil
	}

	var parsed acceptedBody
	if err := json.Unmarshal(body, &parsed); err != nil ||
		strings.TrimSpace(parsed.MessageID) == "" ||
		parsed.Status != "accepted" {
		result.Outcome = notification.OutcomeUnknown
		result.ErrorMessage = "invalid provider acceptance response"
		return result, nil
	}

	if _, err := time.Parse(time.RFC3339Nano, parsed.Timestamp); err != nil {
		result.Outcome = notification.OutcomeUnknown
		result.ErrorMessage = "invalid provider acceptance timestamp"
		return result, nil
	}

	result.Outcome = notification.OutcomeAccepted
	result.ProviderMessageID = parsed.MessageID
	return result, nil
}

// parseRetryAfter handles both delta-seconds and HTTP dates
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}

	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds >= 24*60*60 {
			return 24 * time.Hour
		}
		return time.Duration(seconds) * time.Second
	}

	deadline, err := http.ParseTime(value)
	if err != nil || !deadline.After(now) {
		return 0
	}

	delay := deadline.Sub(now)
	if delay > 24*time.Hour {
		return 24 * time.Hour
	}
	return delay
}
