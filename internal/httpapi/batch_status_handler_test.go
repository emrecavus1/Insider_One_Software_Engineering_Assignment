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

const batchStatusTestID = "33333333-3333-4333-8333-333333333333"

type fakeBatchStatusRepository struct {
	result notification.BatchStatus
	err    error
}

func (f *fakeBatchStatusRepository) GetBatchStatus(
	ctx context.Context,
	id string,
) (notification.BatchStatus, error) {
	return f.result, f.err
}

func TestGetBatchStatus(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		repoErr  error
		expected int
	}{
		{
			name:     "existing batch",
			id:       batchStatusTestID,
			expected: http.StatusOK,
		},
		{
			name:     "nonexistent batch",
			id:       batchStatusTestID,
			repoErr:  storage.ErrBatchNotFound,
			expected: http.StatusNotFound,
		},
		{
			name:     "invalid UUID",
			id:       "invalid-id",
			expected: http.StatusBadRequest,
		},
		{
			name:     "database error",
			id:       batchStatusTestID,
			repoErr:  errors.New("database unavailable"),
			expected: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeBatchStatusRepository{
				result: notification.BatchStatus{
					BatchID: batchStatusTestID,
					Total:   2,
					StatusCounts: notification.BatchStatusCounts{
						Pending: 2,
					},
				},
				err: tt.repoErr,
			}

			mux := http.NewServeMux()
			RegisterBatchStatusRoutes(mux, repo)

			req := httptest.NewRequest(
				http.MethodGet,
				"/api/v1/batches/"+tt.id,
				nil,
			)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.expected {
				t.Fatalf(
					"expected %d, got %d: %s",
					tt.expected,
					rec.Code,
					rec.Body.String(),
				)
			}

			if tt.expected == http.StatusOK {
				var response notification.BatchStatus

				if err := json.Unmarshal(
					rec.Body.Bytes(), &response,
				); err != nil {
					t.Fatalf("decode response: %v", err)
				}

				if response.Total != 2 ||
					response.StatusCounts.Pending != 2 {
					t.Errorf("unexpected batch summary: %+v", response)
				}
			}
		})
	}
}
