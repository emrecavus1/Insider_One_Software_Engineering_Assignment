-- Every claim receives a unique fencing token
-- This prevents a stale worker from modifying a notification after another worker reclaims it

ALTER TABLE notifications
ADD COLUMN claim_token UUID;
