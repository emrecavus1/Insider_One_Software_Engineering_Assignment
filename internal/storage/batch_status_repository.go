// This file is responsible for storing the status of the batches for good representation
package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

var ErrBatchNotFound = errors.New("batch not found")

// GetBatchStatus retrieves aggregated status counts for all notifications belonging to a batch.
func (r *NotificationRepository) GetBatchStatus(
	ctx context.Context,
	batchID string,
) (notification.BatchStatus, error) {

	query := `
		SELECT
			b.id::text,
			b.created_at,
			COUNT(n.id),
			COUNT(n.id) FILTER (WHERE n.status = 'pending'),
			COUNT(n.id) FILTER (WHERE n.status = 'processing'),
			COUNT(n.id) FILTER (WHERE n.status = 'retrying'),
			COUNT(n.id) FILTER (WHERE n.status = 'accepted'),
			COUNT(n.id) FILTER (WHERE n.status = 'failed'),
			COUNT(n.id) FILTER (WHERE n.status = 'cancelled')
		FROM batches b
		LEFT JOIN notifications n ON n.batch_id = b.id
		WHERE b.id = $1::uuid
		GROUP BY b.id, b.created_at
	`

	var result notification.BatchStatus

	err := r.db.QueryRow(ctx, query, batchID).Scan(
		&result.BatchID,
		&result.CreatedAt,
		&result.Total,
		&result.StatusCounts.Pending,
		&result.StatusCounts.Processing,
		&result.StatusCounts.Retrying,
		&result.StatusCounts.Accepted,
		&result.StatusCounts.Failed,
		&result.StatusCounts.Cancelled,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return notification.BatchStatus{}, ErrBatchNotFound
	}

	if err != nil {
		return notification.BatchStatus{},
			fmt.Errorf("retrieve batch status: %w", err)
	}

	return result, nil
}
