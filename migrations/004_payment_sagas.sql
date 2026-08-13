CREATE TABLE payment_sagas (
    id             TEXT PRIMARY KEY,
    payment_id     TEXT NOT NULL UNIQUE REFERENCES payments(id),
    correlation_id TEXT NOT NULL UNIQUE,
    state          TEXT NOT NULL CHECK (state IN (
        'awaiting_policy', 'awaiting_ledger', 'awaiting_settlement',
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
