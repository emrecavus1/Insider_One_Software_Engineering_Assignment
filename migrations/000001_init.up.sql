-- Our Database system will have the following entities:
    -- Batches
    -- Notifications 
    -- Delivery Attempts
    -- Idempotency_keys

-- Starting with batches
CREATE TABLE batches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    -- TIMESTAMPTZ allows consistent timestamp handling regardless of time zone
);

-- One batch may have many notifications, so there's one to many relationship
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    batch_id UUID REFERENCES batches(id),

    -- UUID identifiers are useful for distributed systems and external API consumers

    recipient TEXT NOT NULL
        CHECK (LENGTH(TRIM(recipient)) > 0),

    channel TEXT NOT NULL
        CHECK (channel IN ('sms', 'email', 'push')),

    content TEXT NOT NULL
        CHECK (LENGTH(TRIM(content)) > 0),

    priority TEXT NOT NULL DEFAULT 'normal'
        CHECK (priority IN ('high', 'normal', 'low')),

    priority_rank SMALLINT GENERATED ALWAYS AS (
        CASE priority
            WHEN 'high' THEN 3
            WHEN 'normal' THEN 2
            ELSE 1
        END
    ) STORED,

    -- priority rank allows workers to order notifications without changing the human-readable priorities

    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'processing', 'retrying',
            'accepted', 'failed', 'cancelled'
        )),

    attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK (attempt_count >= 0),

    max_attempts INTEGER NOT NULL DEFAULT 4
        CHECK (max_attempts > 0),

    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- next_attempt_at supports retries without requiring workers to sleep


    worker_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    -- lease_expires_at supports recovering jobs abandoned by crashed workers

    provider_message_id TEXT,
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    accepted_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,

    CHECK (attempt_count <= max_attempts)
);

-- One notification may have many attempts, this table maintains an audit trail of every send attempt
CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    notification_id UUID NOT NULL
        REFERENCES notifications(id),

    attempt_number INTEGER NOT NULL
        CHECK (attempt_number > 0),

    outcome TEXT NOT NULL
        CHECK (outcome IN (
            'started', 'accepted',
            'retryable_failure',
            'permanent_failure', 'unknown'
        )),

    http_status INTEGER
        CHECK (http_status BETWEEN 100 AND 599),

    provider_message_id TEXT,
    error_message TEXT,

    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    UNIQUE (notification_id, attempt_number)
);

-- Idempotency keys prevent duplicate notification creation
CREATE TABLE idempotency_keys (
    key TEXT PRIMARY KEY
        CHECK (LENGTH(key) BETWEEN 1 AND 255),

    request_hash TEXT NOT NULL
        CHECK (LENGTH(request_hash) = 64),

    -- request hash helps distinguish genuine repeated requests from reuse of an idempotency key with a different payload

    notification_id UUID REFERENCES notifications(id),
    batch_id UUID REFERENCES batches(id),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (num_nonnulls(notification_id, batch_id) = 1)
);

-- Indexes for notification retrieval and processing
CREATE INDEX idx_notifications_ready
    ON notifications (
        priority_rank DESC,
        next_attempt_at,
        created_at,
        id
    )
    WHERE status IN ('pending', 'retrying');

CREATE INDEX idx_notifications_expired_leases
    ON notifications (lease_expires_at)
    WHERE status = 'processing';

CREATE INDEX idx_notifications_batch
    ON notifications (batch_id)
    WHERE batch_id IS NOT NULL;

CREATE INDEX idx_notifications_status_created
    ON notifications (status, created_at DESC);

CREATE INDEX idx_notifications_channel_created
    ON notifications (channel, created_at DESC);

CREATE INDEX idx_notifications_created
    ON notifications (created_at DESC, id DESC);
