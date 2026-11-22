// This file is introducing a seperate handler for batches
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/observability"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/storage"
)

// Operations required for batch creation
type BatchRepository interface {
	CreateBatch(
		context.Context,
		[]notification.CreateRequest,
	) (notification.BatchCreateResult, error)
}

type batchHandler struct {
	repo BatchRepository
}

func RegisterBatchRoutes(
	mux *http.ServeMux,
	repo BatchRepository,
) {
	handler := &batchHandler{repo: repo}
	mux.HandleFunc(
		"POST /api/v1/notifications/batch",
		handler.createBatch,
	)
}

func (h *batchHandler) createBatch(
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

	// allowing larger payloads for batch requests
	r.Body = http.MaxBytesReader(w, r.Body, 64<<20)

	var payload struct {
		Notifications []notification.CreateRequest `json:"notifications"`
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&payload); err != nil {
		writeBatchDecodeError(w, err)
		return
	}

	// rejecting trailing JSON objects or other content
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			http.Error(w, "multiple JSON values are not allowed",
				http.StatusBadRequest)
		} else {
			writeBatchDecodeError(w, err)
		}
		return
	}

	if len(payload.Notifications) == 0 ||
		len(payload.Notifications) > 1000 {
		http.Error(w,
			"batch must contain between 1 and 1000 notifications",
			http.StatusBadRequest)
		return
	}

	// validating all notifications before database insertion
	for i, request := range payload.Notifications {
		if _, err := request.NormalizeAndValidate(); err != nil {
			http.Error(w,
				fmt.Sprintf("notification %d: %v", i+1, err),
				http.StatusBadRequest)
			return
		}
	}

	key, hasKey, err := readIdempotencyKey(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	created, replayed, err := createBatchWithIdempotency(
		r.Context(),
		h.repo,
		payload.Notifications,
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
			"failed to create notification batch",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	statusCode := http.StatusCreated

	if replayed {
		statusCode = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}

	w.Header().Set("X-Correlation-ID", created.BatchID)
	slog.InfoContext(r.Context(), "notification_batch_created",
		"request_id", observability.RequestID(r.Context()),
		"correlation_id", created.BatchID,
		"batch_id", created.BatchID,
		"count", created.Count,
		"replayed", replayed,
	)
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(created)

}

func writeBatchDecodeError(w http.ResponseWriter, err error) {
	var sizeErr *http.MaxBytesError

	if errors.As(err, &sizeErr) {
		http.Error(w, "batch request body too large",
			http.StatusRequestEntityTooLarge)
		return
	}

	http.Error(w, "invalid JSON request: "+err.Error(),
		http.StatusBadRequest)
}
