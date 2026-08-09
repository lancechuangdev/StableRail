CREATE INDEX outbox_events_dead_lettered_idx
    ON outbox_events (failed_at, sequence_number)
    WHERE failed_at IS NOT NULL;
