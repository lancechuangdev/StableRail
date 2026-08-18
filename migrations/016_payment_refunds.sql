CREATE TABLE payment_refunds (
    id                    TEXT PRIMARY KEY,
    payment_id            TEXT NOT NULL REFERENCES payments(id),
    tenant_id             TEXT NOT NULL,
    idempotency_key       TEXT NOT NULL,
    amount_minor          BIGINT NOT NULL CHECK (amount_minor > 0),
    currency              TEXT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('created', 'processing', 'succeeded', 'failed')),
    reason                TEXT NOT NULL,
    provider_reference    TEXT,
    failure_reason        TEXT,
    ledger_transaction_id TEXT UNIQUE REFERENCES ledger_transactions(id),
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX payment_refunds_tenant_idempotency_idx
    ON payment_refunds (tenant_id, idempotency_key);

CREATE INDEX payment_refunds_payment_idx
    ON payment_refunds (payment_id, created_at, id);
