package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

// ClaimReady atomically reserves up to limit eligible notifications for one worker

// Concurrent calls cannot claim the same notification.

func (r *NotificationRepository) ClaimReady(
	ctx context.Context,
	workerID string,
	limit int,
	leaseDuration time.Duration,
) ([]notification.ClaimedNotification, error) {

	workerID = strings.TrimSpace(workerID)

	if workerID == "" || len(workerID) > 128 {
		return nil, fmt.Errorf(
			"worker ID must contain 1 to 128 characters",
		)
	}

	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf(
			"claim limit must be between 1 and 100",
		)
	}

	if leaseDuration < time.Second ||
		leaseDuration > 24*time.Hour {
		return nil, fmt.Errorf(
			"lease duration must be between 1 second and 24 hours",
		)
	}

	const query = `
		WITH candidates AS (
			SELECT id
			FROM notifications
			WHERE status IN ('pending', 'retrying')
			  AND next_attempt_at <= NOW()
			  AND attempt_count < max_attempts
			ORDER BY
				priority_rank DESC,
				next_attempt_at ASC,
				created_at ASC,
				id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		),
		claimed AS (
			UPDATE notifications AS n
			SET
				status = 'processing',
				worker_id = $2,
				lease_expires_at = NOW() +
					($3::bigint * INTERVAL '1 microsecond'),
				updated_at = NOW()
			FROM candidates AS c
			WHERE n.id = c.id
			RETURNING
				n.id::text AS id,
				COALESCE(n.batch_id::text, '') AS batch_id,
				n.recipient,
				n.channel,
				n.content,
				n.priority,
				n.status,
				n.created_at,
				n.attempt_count,
				n.max_attempts,
				n.next_attempt_at,
				n.worker_id,
				n.lease_expires_at,
				n.priority_rank
		)
		SELECT
			id,
			batch_id,
			recipient,
			channel,
			content,
			priority,
			status,
			created_at,
			attempt_count,
			max_attempts,
			next_attempt_at,
			worker_id,
			lease_expires_at
		FROM claimed
		ORDER BY
			priority_rank DESC,
			next_attempt_at ASC,
			created_at ASC,
			id ASC
	`

	rows, err := r.db.Query(
		ctx,
		query,
		limit,
		workerID,
		leaseDuration.Microseconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("claim notifications: %w", err)
	}
	defer rows.Close()

	claimed := make([]notification.ClaimedNotification, 0)

	for rows.Next() {
		var item notification.ClaimedNotification
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
			&item.AttemptCount,
			&item.MaxAttempts,
			&item.NextAttemptAt,
			&item.WorkerID,
			&item.LeaseExpiresAt,
		)

		if err != nil {
			return nil, fmt.Errorf(
				"scan claimed notification: %w", err,
			)
		}

		if batchID != "" {
			item.BatchID = &batchID
		}

		item.Channel = notification.Channel(channel)
		item.Priority = notification.Priority(priority)
		item.Status = notification.Status(status)

		claimed = append(claimed, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate claimed notifications: %w", err,
		)
	}

	return claimed, nil
}
