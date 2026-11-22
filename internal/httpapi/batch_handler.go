// This file is introducing a seperate handler for batches
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
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

	result, err := h.repo.CreateBatch(
		r.Context(),
		payload.Notifications,
	)
	if err != nil {
		http.Error(w, "failed to create notification batch",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(result)
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
