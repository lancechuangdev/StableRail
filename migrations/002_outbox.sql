-- The outbox relay publishes pending rows to Kafka.
CREATE TABLE outbox_events (
    sequence_number  BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    id               TEXT PRIMARY KEY,
    topic            TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    event_version    INTEGER NOT NULL CHECK (event_version > 0),
    aggregate_id     TEXT NOT NULL,
    aggregate_type   TEXT NOT NULL,
    payload          JSONB NOT NULL,
    occurred_at      TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at     TIMESTAMPTZ,
    attempt_count    INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error       TEXT,
    failed_at        TIMESTAMPTZ,
    dlq_published_at TIMESTAMPTZ,
    redriven_at      TIMESTAMPTZ
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (next_attempt_at, sequence_number)
    WHERE published_at IS NULL AND failed_at IS NULL;

CREATE INDEX outbox_events_aggregate_pending_idx
    ON outbox_events (aggregate_type, aggregate_id, sequence_number)
    WHERE published_at IS NULL;

CREATE INDEX outbox_events_dead_lettered_idx
    ON outbox_events (failed_at, sequence_number)
    WHERE failed_at IS NOT NULL;
