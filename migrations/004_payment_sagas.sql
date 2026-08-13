CREATE TABLE payment_sagas (
    id             TEXT PRIMARY KEY,
    payment_id     TEXT NOT NULL UNIQUE REFERENCES payments(id),
    correlation_id TEXT NOT NULL UNIQUE,
    state          TEXT NOT NULL CHECK (state IN (
        'awaiting_policy', 'awaiting_ledger', 'awaiting_settlement', 'on_hold', 'manual_review',
        'releasing_ledger', 'refunding', 'recording_refund', 'settling_payment', 'completed',
        'ledger_released', 'refunded', 'failed'
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
    action      TEXT NOT NULL CHECK (action IN ('retry','complete','fail','refund')),
    operator    TEXT NOT NULL,
    note        TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX saga_manual_review_actions_saga_idx
    ON saga_manual_review_actions (saga_id, occurred_at, id);
