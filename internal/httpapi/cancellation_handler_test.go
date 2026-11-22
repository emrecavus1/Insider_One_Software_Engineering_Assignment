package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

const cancellationTestID = "44444444-4444-4444-8444-444444444444"

type fakeCancellationRepository struct {
	err    error
	called bool
}

func (f *fakeCancellationRepository) Cancel(
	ctx context.Context,
	id string,
) (notification.Notification, error) {
	f.called = true

	if f.err != nil {
		return notification.Notification{}, f.err
	}

	return notification.Notification{
		ID:     id,
		Status: notification.StatusCancelled,
	}, nil
}

func TestCancelNotification(t *testing.T) {
	tests := []struct {
		name           string
		id             string
		repositoryErr  error
		expectedStatus int
	}{
		{
			name:           "successful cancellation",
			id:             cancellationTestID,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "invalid UUID",
			id:             "invalid-id",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "notification not found",
			id:             cancellationTestID,
			repositoryErr:  storage.ErrNotificationNotFound,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "notification already processing",
			id:             cancellationTestID,
			repositoryErr:  storage.ErrNotificationNotCancellable,
			expectedStatus: http.StatusConflict,
		},
		{
			name:           "database failure",
			id:             cancellationTestID,
			repositoryErr:  errors.New("database unavailable"),
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeCancellationRepository{
				err: tt.repositoryErr,
			}

			mux := http.NewServeMux()
			RegisterCancellationRoutes(mux, repo)

			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/notifications/"+tt.id+"/cancel",
				nil,
			)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Fatalf(
					"expected %d, got %d: %s",
					tt.expectedStatus,
					rec.Code,
					rec.Body.String(),
				)
			}

			if tt.expectedStatus == http.StatusBadRequest {
				if repo.called {
					t.Error("invalid ID should not reach repository")
				}
				return
			}

			if !repo.called {
				t.Error("expected repository to be called")
			}

			if tt.expectedStatus == http.StatusOK {
				var result notification.Notification
				if err := json.Unmarshal(
					rec.Body.Bytes(), &result,
				); err != nil {
					t.Fatalf("decode response: %v", err)
				}

				if result.Status != notification.StatusCancelled {
					t.Errorf(
						"expected cancelled, got %s",
						result.Status,
					)
				}
			}
		})
	}
}
