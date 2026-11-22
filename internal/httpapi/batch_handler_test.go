package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type fakeBatchRepository struct {
	calls    int
	received []notification.CreateRequest
	err      error
}

func (f *fakeBatchRepository) CreateBatch(
	ctx context.Context,
	requests []notification.CreateRequest,
) (notification.BatchCreateResult, error) {
	f.calls++
	f.received = requests

	if f.err != nil {
		return notification.BatchCreateResult{}, f.err
	}

	return notification.BatchCreateResult{
		BatchID:         "11111111-1111-4111-8111-111111111111",
		Count:           len(requests),
		NotificationIDs: make([]string, len(requests)),
	}, nil
}

func TestBatchCreation(t *testing.T) {
	valid := notification.CreateRequest{
		Recipient: "+905551234567",
		Channel:   notification.ChannelSMS,
		Content:   "Flash sale!",
		Priority:  notification.PriorityHigh,
	}

	invalid := notification.CreateRequest{
		Recipient: "invalid-email",
		Channel:   notification.ChannelEmail,
		Content:   "Hello!",
	}

	tests := []struct {
		name          string
		notifications []notification.CreateRequest
		expected      int
	}{
		{
			name:          "valid batch",
			notifications: []notification.CreateRequest{valid, valid},
			expected:      http.StatusCreated,
		},
		{
			name:          "maximum batch size",
			notifications: makeValidBatch(valid, 1000),
			expected:      http.StatusCreated,
		},
		{
			name:          "empty batch",
			notifications: nil,
			expected:      http.StatusBadRequest,
		},
		{
			name:          "oversized batch",
			notifications: makeValidBatch(valid, 1001),
			expected:      http.StatusBadRequest,
		},
		{
			name:          "invalid notification",
			notifications: []notification.CreateRequest{valid, invalid},
			expected:      http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeBatchRepository{}
			mux := http.NewServeMux()
			RegisterBatchRoutes(mux, repo)

			body, err := json.Marshal(map[string]interface{}{
				"notifications": tt.notifications,
			})
			if err != nil {
				t.Fatal(err)
			}

			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/notifications/batch",
				bytes.NewReader(body),
			)
			req.Header.Set("Content-Type", "application/json")

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.expected {
				t.Fatalf("expected %d, got %d: %s",
					tt.expected, rec.Code, rec.Body.String())
			}

			if tt.expected == http.StatusCreated {
				if repo.calls != 1 {
					t.Fatal("expected exactly one repository call")
				}

				var result notification.BatchCreateResult
				if err := json.Unmarshal(
					rec.Body.Bytes(), &result,
				); err != nil {
					t.Fatal(err)
				}

				if result.Count != len(tt.notifications) {
					t.Errorf("unexpected notification count: %d",
						result.Count)
				}
			} else if repo.calls != 0 {
				t.Error("invalid request reached the repository")
			}
		})
	}
}

func TestBatchRepositoryFailure(t *testing.T) {
	repo := &fakeBatchRepository{
		err: errors.New("database unavailable"),
	}

	mux := http.NewServeMux()
	RegisterBatchRoutes(mux, repo)

	body := `{
		"notifications": [
			{
				"recipient": "+905551234567",
				"channel": "sms",
				"content": "Hello"
			}
		]
	}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/notifications/batch",
		bytes.NewBufferString(body),
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func makeValidBatch(
	request notification.CreateRequest,
	count int,
) []notification.CreateRequest {
	batch := make([]notification.CreateRequest, count)

	for i := range batch {
		batch[i] = request
	}

	return batch
}
