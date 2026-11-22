// This layer is responsible for interpreting query parameters, checking their validity
// and passing them to our repository
package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

type ListRepository interface {
	List(
		context.Context,
		notification.ListFilter,
	) (notification.ListResult, error)
}

type listHandler struct {
	repo ListRepository
}

func RegisterListRoutes(
	mux *http.ServeMux,
	repo ListRepository,
) {
	handler := &listHandler{repo: repo}
	mux.HandleFunc(
		"GET /api/v1/notifications",
		handler.list,
	)
}

func (h *listHandler) list(
	w http.ResponseWriter,
	r *http.Request,
) {
	query := r.URL.Query()

	filter := notification.ListFilter{
		Page:  1,
		Limit: 20,
	}

	// validating status
	if value := query.Get("status"); value != "" {
		filter.Status = notification.Status(value)

		switch filter.Status {
		case notification.StatusPending,
			notification.StatusProcessing,
			notification.StatusRetrying,
			notification.StatusAccepted,
			notification.StatusFailed,
			notification.StatusCancelled:
		default:
			http.Error(w, "invalid status filter",
				http.StatusBadRequest)
			return
		}
	}

	// validating channel
	if value := query.Get("channel"); value != "" {
		filter.Channel = notification.Channel(value)

		switch filter.Channel {
		case notification.ChannelSMS,
			notification.ChannelEmail,
			notification.ChannelPush:
		default:
			http.Error(w, "invalid channel filter",
				http.StatusBadRequest)
			return
		}
	}

	// parsing start date
	if query.Has("created_from") {
		parsed, err := time.Parse(
			time.RFC3339,
			query.Get("created_from"),
		)
		if err != nil {
			http.Error(w, "created_from must be RFC3339",
				http.StatusBadRequest)
			return
		}
		filter.CreatedFrom = &parsed
	}

	// parsing end date
	if query.Has("created_to") {
		parsed, err := time.Parse(
			time.RFC3339,
			query.Get("created_to"),
		)
		if err != nil {
			http.Error(w, "created_to must be RFC3339",
				http.StatusBadRequest)
			return
		}
		filter.CreatedTo = &parsed
	}

	if filter.CreatedFrom != nil && filter.CreatedTo != nil &&
		!filter.CreatedFrom.Before(*filter.CreatedTo) {
		http.Error(w, "created_from must be before created_to",
			http.StatusBadRequest)
		return
	}

	// parsing page number
	if query.Has("page") {
		page, err := strconv.Atoi(query.Get("page"))
		if err != nil || page < 1 || page > 100000 {
			http.Error(w, "page must be between 1 and 100000",
				http.StatusBadRequest)
			return
		}
		filter.Page = page
	}

	// parsing page size
	if query.Has("limit") {
		limit, err := strconv.Atoi(query.Get("limit"))
		if err != nil || limit < 1 || limit > 100 {
			http.Error(w, "limit must be between 1 and 100",
				http.StatusBadRequest)
			return
		}
		filter.Limit = limit
	}

	result, err := h.repo.List(r.Context(), filter)
	if err != nil {
		http.Error(w, "failed to list notifications",
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
