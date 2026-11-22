package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

const testID = "11111111-1111-4111-8111-111111111111"

type fakeRepository struct {
	saved notification.Notification
}

func (f *fakeRepository) Create(
	ctx context.Context,
	req notification.CreateRequest,
) (notification.Notification, error) {
	f.saved = notification.Notification{
		ID:        testID,
		Recipient: req.Recipient,
		Channel:   req.Channel,
		Content:   req.Content,
		Priority:  req.Priority,
		Status:    notification.StatusPending,
	}
	return f.saved, nil
}

func (f *fakeRepository) GetByID(
	ctx context.Context,
	id string,
) (notification.Notification, error) {
	if id != f.saved.ID {
		return notification.Notification{},
			storage.ErrNotificationNotFound
	}
	return f.saved, nil
}

func TestCreateNotification(t *testing.T) {
	repo := &fakeRepository{}
	mux := http.NewServeMux()
	RegisterNotificationRoutes(mux, repo)

	body := []byte(`{
		"recipient": "+905551234567",
		"channel": "sms",
		"content": "Hello!",
		"priority": "high"
	}`)

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/notifications",
		bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s",
			rec.Code, rec.Body.String())
	}

	var result notification.Notification
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if result.ID != testID {
		t.Errorf("unexpected notification ID: %s", result.ID)
	}

	if result.Status != notification.StatusPending {
		t.Errorf("expected pending status, got %s", result.Status)
	}

	if result.Priority != notification.PriorityHigh {
		t.Errorf("expected high priority, got %s", result.Priority)
	}
}

func TestCreateNotificationValidation(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		contentType string
		expected    int
	}{
		{
			name:        "missing recipient",
			body:        `{"channel":"sms","content":"Hello"}`,
			contentType: "application/json",
			expected:    http.StatusBadRequest,
		},
		{
			name:        "invalid JSON",
			body:        `{invalid}`,
			contentType: "application/json",
			expected:    http.StatusBadRequest,
		},
		{
			name:        "unknown field",
			body:        `{"recipient":"x","unknown":"value"}`,
			contentType: "application/json",
			expected:    http.StatusBadRequest,
		},
		{
			name:        "incorrect content type",
			body:        `{}`,
			contentType: "text/plain",
			expected:    http.StatusUnsupportedMediaType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			RegisterNotificationRoutes(mux, &fakeRepository{})

			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/notifications",
				bytes.NewBufferString(tt.body),
			)
			req.Header.Set("Content-Type", tt.contentType)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.expected {
				t.Errorf("expected %d, got %d",
					tt.expected, rec.Code)
			}
		})
	}
}

func TestGetNotification(t *testing.T) {
	repo := &fakeRepository{
		saved: notification.Notification{
			ID:     testID,
			Status: notification.StatusPending,
		},
	}

	mux := http.NewServeMux()
	RegisterNotificationRoutes(mux, repo)

	tests := []struct {
		name     string
		id       string
		expected int
	}{
		{"existing notification", testID, http.StatusOK},
		{"nonexistent notification",
			"22222222-2222-4222-8222-222222222222",
			http.StatusNotFound},
		{"invalid UUID", "invalid-id", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodGet,
				"/api/v1/notifications/"+tt.id,
				nil,
			)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.expected {
				t.Errorf("expected %d, got %d: %s",
					tt.expected, rec.Code, rec.Body.String())
			}
		})
	}
}
