package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

const successfulResponse = `{"messageId":"provider-message-1","status":"accepted","timestamp":"2026-11-22T19:00:00Z"}`

func testJob() notification.ClaimedNotification {
	return notification.ClaimedNotification{
		Notification: notification.Notification{
			ID: "notification-123", Recipient: "+905551234567",
			Channel: notification.ChannelSMS, Content: "Your message",
		},
		AttemptCount: 1,
	}
}

func TestHTTPSenderRequestAndAcceptance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Error("expected JSON POST")
		}
		if r.Header.Get("Idempotency-Key") != "notification-123" || r.Header.Get("X-Delivery-Attempt") != "2" {
			t.Error("missing correlation and attempt headers")
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if len(payload) != 3 || payload["to"] != "+905551234567" || payload["channel"] != "sms" || payload["content"] != "Your message" {
			t.Errorf("incorrect provider payload: %v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(successfulResponse))
	}))
	defer server.Close()

	sender, err := NewHTTPSender(server.URL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := sender.Send(context.Background(), testJob())
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != notification.OutcomeAccepted || result.ProviderMessageID != "provider-message-1" || result.HTTPStatus != 202 {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestHTTPSenderClassifiesProviderResponses(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		retryAfter string
		outcome    notification.AttemptOutcome
	}{
		{"rate limited", 429, "", "3", notification.OutcomeRetryableFailure},
		{"service unavailable", 503, "", "", notification.OutcomeRetryableFailure},
		{"request timeout", 408, "", "", notification.OutcomeRetryableFailure},
		{"too early", 425, "", "", notification.OutcomeRetryableFailure},
		{"invalid recipient", 422, "", "", notification.OutcomePermanentFailure},
		{"unauthorized", 401, "", "", notification.OutcomePermanentFailure},
		{"unexpected 200", 200, successfulResponse, "", notification.OutcomeUnknown},
		{"malformed acceptance", 202, `{"messageId":""}`, "", notification.OutcomeUnknown},
		{"invalid timestamp", 202, `{"messageId":"m","status":"accepted","timestamp":"invalid"}`, "", notification.OutcomeUnknown},
		{"invalid JSON", 202, `{broken`, "", notification.OutcomeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.retryAfter != "" {
					w.Header().Set("Retry-After", tt.retryAfter)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			sender, err := NewHTTPSender(server.URL, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			result, err := sender.Send(context.Background(), testJob())
			if err != nil {
				t.Fatal(err)
			}
			if result.Outcome != tt.outcome || result.HTTPStatus != tt.status {
				t.Errorf("unexpected result: %+v", result)
			}
			if tt.retryAfter != "" && result.RetryAfter != 3*time.Second {
				t.Errorf("expected Retry-After 3s, got %v", result.RetryAfter)
			}
		})
	}
}

func TestHTTPSenderDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Store(true)
	}))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusFound)
	}))
	defer server.Close()
	sender, err := NewHTTPSender(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := sender.Send(context.Background(), testJob())
	if err != nil {
		t.Fatal(err)
	}
	if redirected.Load() || result.Outcome != notification.OutcomePermanentFailure {
		t.Errorf("redirect should be rejected: %+v", result)
	}
}

func TestHTTPSenderNetworkFailureIsUncertain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
	}))
	defer server.Close()
	sender, err := NewHTTPSender(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, err = sender.Send(ctx, testJob())
	if err == nil {
		t.Fatal("expected request timeout")
	}
}

func TestWebhookSiteURLValidation(t *testing.T) {
	token := "5e7540cc-19ac-4348-b9c7-afdadf2a0fac"
	valid := "https://webhook.site/" + token
	if err := ValidateWebhookSiteURL(valid); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{
		"http://webhook.site/" + token,
		"https://webhook.site/",
		"https://example.com/" + token,
		"https://webhook.site/" + token + "/extra",
		"https://webhook.site/" + token + "?debug=1",
		"https://webhook.site:8443/" + token,
		"https://user@webhook.site/" + token,
	} {
		t.Run(input, func(t *testing.T) {
			if err := ValidateWebhookSiteURL(input); err == nil {
				t.Error("expected unsafe URL rejection")
			}
		})
	}
}

func TestRetryAfterDate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	header := now.Add(10 * time.Second).Format(http.TimeFormat)
	delay := parseRetryAfter(header, now)
	if delay != 10*time.Second {
		t.Errorf("expected 10s, got %v", delay)
	}
	if parseRetryAfter("invalid", now) != 0 {
		t.Error("invalid header must not add a delay")
	}
}

func TestHTTPSenderResponseSizeLimit(t *testing.T) {
	body := fmt.Sprintf(`{"messageId":"%s","status":"accepted","timestamp":"2026-11-22T19:00:00Z"}`, strings.Repeat("a", maxResponseBytes))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	sender, err := NewHTTPSender(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := sender.Send(context.Background(), testJob())
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != notification.OutcomeUnknown {
		t.Errorf("oversized response should be unknown, got %+v", result)
	}
}
