// This file implements the API Endpoints for Notification handling
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"github.com/google/uuid"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/observability"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

// describes the database operations our handlers require.
type NotificationRepository interface {
	Create(context.Context, notification.CreateRequest) (notification.Notification, error)
	GetByID(context.Context, string) (notification.Notification, error)
}

type notificationHandler struct {
	repo NotificationRepository
}

// registering notification routes on the application's HTTP router.
func RegisterNotificationRoutes(
	mux *http.ServeMux,
	repo NotificationRepository,
) {
	handler := &notificationHandler{repo: repo}

	mux.HandleFunc("POST /api/v1/notifications", handler.create)
	mux.HandleFunc("GET /api/v1/notifications/{id}", handler.getByID)
}

// POST /api/v1/notifications
func (h *notificationHandler) create(
	w http.ResponseWriter,
	r *http.Request,
) {
	mediaType, _, err := mime.ParseMediaType(
		r.Header.Get("Content-Type"),
	)
	if err != nil || mediaType != "application/json" {
		http.Error(w, "Content-Type must be application/json",
			http.StatusUnsupportedMediaType)
		return
	}

	// limiting a single request body to 64 KiB.
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var request notification.CreateRequest
	if err := decoder.Decode(&request); err != nil {
		var sizeErr *http.MaxBytesError
		if errors.As(err, &sizeErr) {
			http.Error(w, "request body too large",
				http.StatusRequestEntityTooLarge)
			return
		}

		http.Error(w, "invalid JSON request: "+err.Error(),
			http.StatusBadRequest)
		return
	}

	// rejecting multiple JSON objects in the request body.
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "request must contain one JSON object",
			http.StatusBadRequest)
		return
	}

	validated, err := request.NormalizeAndValidate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	key, hasKey, err := readIdempotencyKey(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	created, replayed, err := createSingleWithIdempotency(
		r.Context(),
		h.repo,
		validated,
		key,
		hasKey,
	)

	if errors.Is(err, storage.ErrIdempotencyConflict) {
		http.Error(
			w,
			"idempotency key already used for a different request",
			http.StatusConflict,
		)
		return
	}

	if err != nil {
		http.Error(
			w,
			"failed to create notification",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(
		"Location",
		"/api/v1/notifications/"+created.ID,
	)

	statusCode := http.StatusCreated

	if replayed {
		statusCode = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}

	w.Header().Set("X-Correlation-ID", created.ID)
	slog.InfoContext(r.Context(), "notification_created",
		"request_id", observability.RequestID(r.Context()),
		"correlation_id", created.ID,
		"notification_id", created.ID,
		"channel", created.Channel,
		"priority", created.Priority,
		"replayed", replayed,
	)
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(created)

}

// GET /api/v1/notifications/{id}
func (h *notificationHandler) getByID(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := r.PathValue("id")

	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "invalid notification ID",
			http.StatusBadRequest)
		return
	}

	result, err := h.repo.GetByID(r.Context(), id)
	if errors.Is(err, storage.ErrNotificationNotFound) {
		http.Error(w, "notification not found",
			http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "failed to retrieve notification",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(result); err != nil {
		slog.ErrorContext(r.Context(), "encode notification response failed", "error", err)
	}
}
