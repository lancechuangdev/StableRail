CREATE TABLE payments (
    id                  TEXT PRIMARY KEY,
    external_reference  TEXT NOT NULL,
    currency            TEXT NOT NULL,
    amount_minor        BIGINT NOT NULL CHECK (amount_minor > 0),
    customer_id         TEXT NOT NULL,
    state               TEXT NOT NULL CHECK (state IN ('created', 'processing', 'settled', 'failed')),
    ledger_balance      BIGINT NOT NULL DEFAULT 0,
    idempotency_key     TEXT NOT NULL UNIQUE,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE payment_audit_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id  TEXT NOT NULL REFERENCES payments(id),
    event       TEXT NOT NULL,
    message     TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE payment_timeline_entries (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id  TEXT NOT NULL REFERENCES payments(id),
    state       TEXT NOT NULL,
    note        TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

-- The relay added in the next step will publish pending rows to Kafka.
CREATE TABLE outbox_events (
    id             TEXT PRIMARY KEY,
    topic          TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    event_version  INTEGER NOT NULL CHECK (event_version > 0),
    aggregate_id   TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    payload         JSONB NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at    TIMESTAMPTZ
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (created_at, id)
    WHERE published_at IS NULL;

