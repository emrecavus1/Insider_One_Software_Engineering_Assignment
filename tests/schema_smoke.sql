
-- Tests for our database schema integrity
BEGIN;

DO $$
DECLARE
    test_batch UUID;
    test_notification UUID;
    generated_rank SMALLINT;
    generated_status TEXT;
BEGIN
    -- Test 1: creating a batch
    INSERT INTO batches DEFAULT VALUES
    RETURNING id INTO test_batch;

    -- Test 2: creating a valid notification
    INSERT INTO notifications (
        batch_id, recipient, channel, content, priority
    )
    VALUES (
        test_batch,
        '+905551234567',
        'sms',
        'Test notification',
        'high'
    )
    RETURNING id INTO test_notification;

    -- Test 3: verifying automatically generated values
    SELECT priority_rank, status
    INTO generated_rank, generated_status
    FROM notifications
    WHERE id = test_notification;

    IF generated_rank <> 3
       OR generated_status <> 'pending' THEN
        RAISE EXCEPTION 'Default values test failed';
    END IF;

    -- Test 4: inserting a delivery attempt
    INSERT INTO delivery_attempts (
        notification_id, attempt_number,
        outcome, http_status
    )
    VALUES (
        test_notification, 1, 'accepted', 202
    );

    -- Test 5: inserting an idempotency record
    INSERT INTO idempotency_keys (
        key, request_hash, notification_id
    )
    VALUES (
        'test-request-123',
        repeat('a', 64),
        test_notification
    );

    -- Test 6: rejecting invalid channels
    BEGIN
        INSERT INTO notifications (
            recipient, channel, content
        )
        VALUES (
            '+905551234567',
            'whatsapp',
            'Invalid channel'
        );

        RAISE EXCEPTION 'Invalid channel was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    -- Test 7: rejecting empty content
    BEGIN
        INSERT INTO notifications (
            recipient, channel, content
        )
        VALUES (
            '+905551234567',
            'sms',
            '   '
        );

        RAISE EXCEPTION 'Empty content was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    -- Test 8: preventing duplicate delivery attempts
    BEGIN
        INSERT INTO delivery_attempts (
            notification_id, attempt_number, outcome
        )
        VALUES (
            test_notification, 1, 'started'
        );

        RAISE EXCEPTION 'Duplicate attempt was accepted';
    EXCEPTION WHEN unique_violation THEN
        NULL;
    END;

    -- Test 9: preventing invalid foreign keys
    BEGIN
        INSERT INTO notifications (
            batch_id, recipient, channel, content
        )
        VALUES (
            '00000000-0000-0000-0000-000000000000',
            '+905551234567',
            'sms',
            'Invalid batch reference'
        );

        RAISE EXCEPTION 'Invalid batch reference accepted';
    EXCEPTION WHEN foreign_key_violation THEN
        NULL;
    END;

    -- Test 10: ensuring idempotency references exactly one resource
    BEGIN
        INSERT INTO idempotency_keys (
            key, request_hash
        )
        VALUES (
            'invalid-idempotency',
            repeat('b', 64)
        );

        RAISE EXCEPTION 'Invalid idempotency record accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;

    RAISE NOTICE 'All 10 schema tests passed!';
END $$;

-- After the tests, discarding all the test data
ROLLBACK;
