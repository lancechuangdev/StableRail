CREATE TABLE payment_refunds (
    id                    TEXT PRIMARY KEY,
    payment_id            TEXT NOT NULL REFERENCES payments(id),
    refund_payment_id     TEXT NOT NULL UNIQUE REFERENCES payments(id),
    tenant_id             TEXT NOT NULL,
    idempotency_key       TEXT NOT NULL,
    amount_minor          BIGINT NOT NULL CHECK (amount_minor > 0),
    currency              TEXT NOT NULL,
    reason                TEXT NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    CHECK (payment_id <> refund_payment_id)
);

CREATE UNIQUE INDEX payment_refunds_tenant_idempotency_idx
    ON payment_refunds (tenant_id, idempotency_key);

CREATE INDEX payment_refunds_payment_idx
    ON payment_refunds (payment_id, created_at, id);
