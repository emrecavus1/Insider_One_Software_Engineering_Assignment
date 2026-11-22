// Similar methodology with our previous handler files:
// Validate the incoming batch UUID
// Call the appropriate repository method
// Convert database results or errors into HTTP responses
// Return JSON to the client

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

type BatchStatusRepository interface {
	GetBatchStatus(
		context.Context,
		string,
	) (notification.BatchStatus, error)
}

type batchStatusHandler struct {
	repo BatchStatusRepository
}

func RegisterBatchStatusRoutes(
	mux *http.ServeMux,
	repo BatchStatusRepository,
) {
	handler := &batchStatusHandler{repo: repo}

	mux.HandleFunc(
		"GET /api/v1/batches/{id}",
		handler.getBatchStatus,
	)
}

func (h *batchStatusHandler) getBatchStatus(
	w http.ResponseWriter,
	r *http.Request,
) {
	batchID := r.PathValue("id")

	if _, err := uuid.Parse(batchID); err != nil {
		http.Error(w, "invalid batch ID", http.StatusBadRequest)
		return
	}

	result, err := h.repo.GetBatchStatus(r.Context(), batchID)

	if errors.Is(err, storage.ErrBatchNotFound) {
		http.Error(w, "batch not found", http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, "failed to retrieve batch status",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
