CREATE TABLE settlement_submissions (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id            TEXT NOT NULL REFERENCES payments(id),
    command_event_id      TEXT NOT NULL UNIQUE,
    provider              TEXT NOT NULL,
    provider_reference    TEXT NOT NULL UNIQUE,
    status                TEXT NOT NULL CHECK (status IN ('pending', 'on_hold', 'succeeded', 'failed')),
    failure_code          TEXT,
    failure_message       TEXT,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    CHECK (status <> 'failed' OR failure_code IS NOT NULL)
);

CREATE INDEX settlement_submissions_payment_idx
    ON settlement_submissions (payment_id, created_at);
