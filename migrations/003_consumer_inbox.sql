CREATE TABLE inbox_events (
    consumer_name  TEXT NOT NULL,
    event_id       TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    event_version  INTEGER NOT NULL CHECK (event_version > 0),
    aggregate_id   TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX inbox_events_aggregate_idx
    ON inbox_events (consumer_name, aggregate_type, aggregate_id, occurred_at);
