package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

// DeferClaim releases an owned notification before delivery begins
func (r *NotificationRepository) DeferClaim(
	ctx context.Context,
	job notification.ClaimedNotification,
	delay time.Duration,
) error {
	if err := validateClaimIdentity(job); err != nil {
		return err
	}

	if delay < time.Millisecond || delay > 24*time.Hour {
		return fmt.Errorf(
			"claim deferral must be between 1ms and 24 hours",
		)
	}

	const query = `
		UPDATE notifications AS n
		SET
			status = 'retrying',
			worker_id = NULL,
			claim_token = NULL,
			lease_expires_at = NULL,
			next_attempt_at = clock_timestamp() +
				($3::bigint * INTERVAL '1 microsecond'),
			updated_at = clock_timestamp()
		WHERE n.id = $1::uuid
		  AND n.claim_token = $2::uuid
		  AND n.status = 'processing'
		  AND n.lease_expires_at > clock_timestamp()
		  AND NOT EXISTS (
			SELECT 1
			FROM delivery_attempts AS a
			WHERE a.notification_id = n.id
			  AND a.outcome = 'started'
		  )
	`

	command, err := r.db.Exec(
		ctx,
		query,
		job.ID,
		job.ClaimToken,
		delay.Microseconds(),
	)
	if err != nil {
		return fmt.Errorf("defer notification claim: %w", err)
	}

	if command.RowsAffected() != 1 {
		return ErrClaimNotOwned
	}

	return nil
}
