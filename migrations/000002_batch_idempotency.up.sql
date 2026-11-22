-- Preserving the original batch creation response for idempotent replays.
ALTER TABLE idempotency_keys
ADD COLUMN batch_result JSONB;
