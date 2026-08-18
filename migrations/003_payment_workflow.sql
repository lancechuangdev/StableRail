CREATE TABLE payment_sagas (
    id             TEXT PRIMARY KEY,
    payment_id     TEXT NOT NULL UNIQUE REFERENCES payments(id),
    correlation_id TEXT NOT NULL UNIQUE,
    state          TEXT NOT NULL CHECK (state IN (
        'awaiting_policy', 'awaiting_ledger', 'awaiting_settlement', 'on_hold', 'manual_review',
        'releasing_ledger', 'returning', 'settling_payment', 'completed',
        'ledger_released', 'returned', 'failed'
    )),
    deadline_at    TIMESTAMPTZ,
    failure_reason TEXT,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX payment_sagas_deadline_idx
    ON payment_sagas (deadline_at)
    WHERE deadline_at IS NOT NULL;

CREATE TABLE saga_manual_review_actions (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    saga_id     TEXT NOT NULL REFERENCES payment_sagas(id),
    action      TEXT NOT NULL CHECK (action IN ('retry','complete','fail','return')),
    operator    TEXT NOT NULL,
    note        TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX saga_manual_review_actions_saga_idx
    ON saga_manual_review_actions (saga_id, occurred_at, id);

CREATE TABLE settlement_submissions (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id         TEXT NOT NULL REFERENCES payments(id),
    command_event_id   TEXT NOT NULL UNIQUE,
    provider           TEXT NOT NULL,
    provider_reference TEXT NOT NULL UNIQUE,
    status             TEXT NOT NULL CHECK (status IN ('pending', 'on_hold', 'succeeded', 'failed')),
    failure_code       TEXT,
    failure_message    TEXT,
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    CHECK (status <> 'failed' OR failure_code IS NOT NULL)
);

CREATE INDEX settlement_submissions_payment_idx
    ON settlement_submissions (payment_id, created_at);
