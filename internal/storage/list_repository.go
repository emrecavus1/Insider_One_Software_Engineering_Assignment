// This component will construct a SQL query based on the filters supplied by the client
package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

// List retrieves notifications using optional filters and offset-based pagination
func (r *NotificationRepository) List(
	ctx context.Context,
	filter notification.ListFilter,
) (notification.ListResult, error) {

	if filter.Page < 1 || filter.Page > 100000 {
		return notification.ListResult{},
			fmt.Errorf("page must be between 1 and 100000")
	}

	if filter.Limit < 1 || filter.Limit > 100 {
		return notification.ListResult{},
			fmt.Errorf("limit must be between 1 and 100")
	}

	if filter.CreatedFrom != nil && filter.CreatedTo != nil &&
		!filter.CreatedFrom.Before(*filter.CreatedTo) {
		return notification.ListResult{},
			fmt.Errorf("created_from must be before created_to")
	}

	var conditions []string
	var args []any

	// appending a condition with its parameter index
	addCondition := func(column string, operator string, value any) {
		args = append(args, value)
		conditions = append(
			conditions,
			fmt.Sprintf("%s %s $%d", column, operator, len(args)),
		)
	}

	if filter.Status != "" {
		addCondition("status", "=", string(filter.Status))
	}

	if filter.Channel != "" {
		addCondition("channel", "=", string(filter.Channel))
	}

	if filter.CreatedFrom != nil {
		addCondition("created_at", ">=", *filter.CreatedFrom)
	}

	if filter.CreatedTo != nil {
		addCondition("created_at", "<", *filter.CreatedTo)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = " WHERE " + strings.Join(conditions, " AND ")
	}

	// counting all matching notifications before pagination
	countQuery := "SELECT COUNT(*) FROM notifications" + whereClause

	var total int64
	if err := r.db.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return notification.ListResult{},
			fmt.Errorf("count notifications: %w", err)
	}

	// adding LIMIT and OFFSET after the filter parameters
	offset := (filter.Page - 1) * filter.Limit

	listQuery := `
		SELECT
			id::text,
			COALESCE(batch_id::text, ''),
			recipient,
			channel,
			content,
			priority,
			status,
			created_at
		FROM notifications
	` + whereClause + fmt.Sprintf(`
		ORDER BY created_at DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, len(args)+1, len(args)+2)

	listArgs := append([]any{}, args...)
	listArgs = append(listArgs, filter.Limit, offset)

	rows, err := r.db.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return notification.ListResult{},
			fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()

	items := make([]notification.Notification, 0)

	for rows.Next() {
		var item notification.Notification
		var batchID string
		var channel, priority, status string

		err := rows.Scan(
			&item.ID,
			&batchID,
			&item.Recipient,
			&channel,
			&item.Content,
			&priority,
			&status,
			&item.CreatedAt,
		)
		if err != nil {
			return notification.ListResult{},
				fmt.Errorf("scan notification: %w", err)
		}

		if batchID != "" {
			item.BatchID = &batchID
		}

		item.Channel = notification.Channel(channel)
		item.Priority = notification.Priority(priority)
		item.Status = notification.Status(status)

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return notification.ListResult{},
			fmt.Errorf("iterate notifications: %w", err)
	}

	return notification.ListResult{
		Notifications: items,
		Total:         total,
		Page:          filter.Page,
		Limit:         filter.Limit,
	}, nil
}
