package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

const httpIdempotencyTestID = "11111111-1111-4111-8111-111111111111"

type idempotencyHTTPFake struct {
	singleCalls     int
	singleIdemCalls int
	batchCalls      int
	batchIdemCalls  int
	lastKey         string
	replayed        bool
	err             error
}

func (f *idempotencyHTTPFake) Create(
	ctx context.Context,
	request notification.CreateRequest,
) (notification.Notification, error) {
	f.singleCalls++
	return notification.Notification{
		ID:     httpIdempotencyTestID,
		Status: notification.StatusPending,
	}, nil
}

func (f *idempotencyHTTPFake) GetByID(
	ctx context.Context,
	id string,
) (notification.Notification, error) {
	return notification.Notification{
		ID: id,
	}, nil
}

func (f *idempotencyHTTPFake) CreateIdempotent(
	ctx context.Context,
	request notification.CreateRequest,
	key string,
) (notification.Notification, bool, error) {
	f.singleIdemCalls++
	f.lastKey = key

	if f.err != nil {
		return notification.Notification{}, false, f.err
	}

	return notification.Notification{
		ID:     httpIdempotencyTestID,
		Status: notification.StatusPending,
	}, f.replayed, nil
}

func (f *idempotencyHTTPFake) CreateBatch(
	ctx context.Context,
	requests []notification.CreateRequest,
) (notification.BatchCreateResult, error) {
	f.batchCalls++
	return f.batchResult(requests), nil
}

func (f *idempotencyHTTPFake) CreateBatchIdempotent(
	ctx context.Context,
	requests []notification.CreateRequest,
	key string,
) (notification.BatchCreateResult, bool, error) {
	f.batchIdemCalls++
	f.lastKey = key

	if f.err != nil {
		return notification.BatchCreateResult{}, false, f.err
	}

	return f.batchResult(requests), f.replayed, nil
}

func (f *idempotencyHTTPFake) batchResult(
	requests []notification.CreateRequest,
) notification.BatchCreateResult {
	return notification.BatchCreateResult{
		BatchID: "22222222-2222-4222-8222-222222222222",
		Count:   len(requests),
		NotificationIDs: []string{
			httpIdempotencyTestID,
		},
	}
}

func TestIdempotencyHTTP(t *testing.T) {
	singleBody := `{
		"recipient": "+905551234567",
		"channel": "sms",
		"content": "Hello",
		"priority": "high"
	}`

	batchBody := `{
		"notifications": [
			{
				"recipient": "+905551234567",
				"channel": "sms",
				"content": "Hello"
			}
		]
	}`

	tests := []struct {
		name       string
		batch      bool
		keys       []string
		replayed   bool
		repoErr    error
		wantStatus int
	}{
		{"single without key", false, nil, false, nil, 201},
		{"single new key", false, []string{"single-123"}, false, nil, 201},
		{"single replay", false, []string{"single-123"}, true, nil, 200},
		{"single conflict", false, []string{"single-123"}, false, storage.ErrIdempotencyConflict, 409},
		{"single empty key", false, []string{""}, false, nil, 400},
		{"single repeated header", false, []string{"a", "b"}, false, nil, 400},
		{"single repository failure", false, []string{"single-123"}, false, errors.New("DB error"), 500},
		{"batch without key", true, nil, false, nil, 201},
		{"batch new key", true, []string{"batch-123"}, false, nil, 201},
		{"batch replay", true, []string{"batch-123"}, true, nil, 200},
		{"batch conflict", true, []string{"batch-123"}, false, storage.ErrIdempotencyConflict, 409},
		{"batch empty key", true, []string{""}, false, nil, 400},
		{"batch repeated header", true, []string{"a", "b"}, false, nil, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &idempotencyHTTPFake{
				replayed: tt.replayed,
				err:      tt.repoErr,
			}

			mux := http.NewServeMux()
			RegisterNotificationRoutes(mux, repo)
			RegisterBatchRoutes(mux, repo)

			path := "/api/v1/notifications"
			body := singleBody

			if tt.batch {
				path += "/batch"
				body = batchBody
			}

			req := httptest.NewRequest(
				http.MethodPost,
				path,
				strings.NewReader(body),
			)

			req.Header.Set("Content-Type", "application/json")

			for _, key := range tt.keys {
				req.Header.Add("Idempotency-Key", key)
			}

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf(
					"expected HTTP %d, got %d: %s",
					tt.wantStatus,
					rec.Code,
					rec.Body.String(),
				)
			}

			if tt.wantStatus == http.StatusBadRequest {
				if repo.singleCalls+repo.singleIdemCalls+
					repo.batchCalls+repo.batchIdemCalls != 0 {
					t.Error("invalid key reached repository")
				}
				return
			}

			if len(tt.keys) == 0 {
				if tt.batch && repo.batchCalls != 1 {
					t.Error("expected normal batch creation")
				}
				if !tt.batch && repo.singleCalls != 1 {
					t.Error("expected normal single creation")
				}
			} else {
				if repo.lastKey != tt.keys[0] {
					t.Error("idempotency key not forwarded")
				}
				if tt.batch && repo.batchIdemCalls != 1 {
					t.Error("expected idempotent batch creation")
				}
				if !tt.batch && repo.singleIdemCalls != 1 {
					t.Error("expected idempotent single creation")
				}
			}

			if tt.replayed &&
				rec.Header().Get("Idempotency-Replayed") != "true" {
				t.Error("missing replay response header")
			}

			if tt.wantStatus == http.StatusOK ||
				tt.wantStatus == http.StatusCreated {
				if !strings.Contains(
					rec.Body.String(),
					httpIdempotencyTestID,
				) {
					t.Error("response does not contain expected resource ID")
				}
			}
		})
	}
}
