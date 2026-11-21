-- The task of this file is to reverses the changes in init.up.sql so we can roll back the migration
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS batches;
