-- Support rolling delivery-attempt and end-to-end latency metrics
CREATE INDEX idx_delivery_attempts_finished_at
    ON delivery_attempts (finished_at)
    WHERE finished_at IS NOT NULL;

CREATE INDEX idx_notifications_accepted_at
    ON notifications (accepted_at)
    WHERE status = 'accepted';
