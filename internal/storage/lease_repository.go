package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

var ErrClaimNotOwned = errors.New(
	"notification claim is no longer owned or its lease has expired",
)

// RecoverExpiredLeases atomically recovers up to limit notifications abandoned by their workers
func (r *NotificationRepository) RecoverExpiredLeases(
	ctx context.Context,
	limit int,
) (notification.LeaseRecoveryResult, error) {
	if limit < 1 || limit > 1000 {
		return notification.LeaseRecoveryResult{},
			fmt.Errorf("recovery limit must be between 1 and 1000")
	}

	const query = `
		WITH expired AS (
			SELECT id
			FROM notifications
			WHERE status = 'processing'
			  AND lease_expires_at <= NOW()
			ORDER BY lease_expires_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		),
		recovered AS (
			UPDATE notifications AS n
			SET
				status = CASE
					WHEN n.attempt_count >= n.max_attempts
						THEN 'failed'
					ELSE 'retrying'
				END,
				worker_id = NULL,
				claim_token = NULL,
				lease_expires_at = NULL,
				next_attempt_at = CASE
					WHEN n.attempt_count < n.max_attempts
						THEN NOW()
					ELSE n.next_attempt_at
				END,
				last_error = CASE
					WHEN n.attempt_count >= n.max_attempts
						THEN 'worker lease expired after maximum attempts'
					ELSE n.last_error
				END,
				updated_at = NOW()
			FROM expired AS e
			WHERE n.id = e.id
			RETURNING n.status
		)
		SELECT
			COUNT(*) FILTER (WHERE status = 'retrying'),
			COUNT(*) FILTER (WHERE status = 'failed')
		FROM recovered
	`

	var result notification.LeaseRecoveryResult

	err := r.db.QueryRow(ctx, query, limit).Scan(
		&result.Requeued,
		&result.Failed,
	)

	if err != nil {
		return notification.LeaseRecoveryResult{},
			fmt.Errorf("recover expired leases: %w", err)
	}

	return result, nil
}

// RenewLease extends an active lease only when the caller still owns the specific claim

// A stale claim token, expired lease, or finalized notification causes ErrClaimNotOwned
func (r *NotificationRepository) RenewLease(
	ctx context.Context,
	notificationID string,
	claimToken string,
	leaseDuration time.Duration,
) (time.Time, error) {

	if _, err := uuid.Parse(notificationID); err != nil {
		return time.Time{}, fmt.Errorf("invalid notification ID: %w", err)
	}

	if _, err := uuid.Parse(claimToken); err != nil {
		return time.Time{}, fmt.Errorf("invalid claim token: %w", err)
	}

	if leaseDuration < time.Second ||
		leaseDuration > 24*time.Hour {
		return time.Time{},
			fmt.Errorf("lease duration must be between 1 second and 24 hours")
	}

	const query = `
		UPDATE notifications
		SET
			lease_expires_at = GREATEST(
				lease_expires_at,
				NOW() + ($3::bigint * INTERVAL '1 microsecond')
			),
			updated_at = NOW()
		WHERE id = $1::uuid
		  AND claim_token = $2::uuid
		  AND status = 'processing'
		  AND lease_expires_at > NOW()
		RETURNING lease_expires_at
	`

	var expiresAt time.Time

	err := r.db.QueryRow(
		ctx,
		query,
		notificationID,
		claimToken,
		leaseDuration.Microseconds(),
	).Scan(&expiresAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrClaimNotOwned
	}

	if err != nil {
		return time.Time{},
			fmt.Errorf("renew notification lease: %w", err)
	}

	return expiresAt, nil
}
