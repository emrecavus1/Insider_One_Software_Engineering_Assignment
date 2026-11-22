// This file integrates cancellation rules implemented in notification_repository.go
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

type CancellationRepository interface {
	Cancel(
		context.Context,
		string,
	) (notification.Notification, error)
}

type cancellationHandler struct {
	repo CancellationRepository
}

func RegisterCancellationRoutes(
	mux *http.ServeMux,
	repo CancellationRepository,
) {
	handler := &cancellationHandler{repo: repo}

	mux.HandleFunc(
		"POST /api/v1/notifications/{id}/cancel",
		handler.cancel,
	)
}

func (h *cancellationHandler) cancel(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := r.PathValue("id")

	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "invalid notification ID",
			http.StatusBadRequest)
		return
	}

	result, err := h.repo.Cancel(r.Context(), id)

	if errors.Is(err, storage.ErrNotificationNotFound) {
		http.Error(w, "notification not found",
			http.StatusNotFound)
		return
	}

	if errors.Is(err, storage.ErrNotificationNotCancellable) {
		http.Error(w, "notification cannot be cancelled",
			http.StatusConflict)
		return
	}

	if err != nil {
		http.Error(w, "failed to cancel notification",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(result)
}
