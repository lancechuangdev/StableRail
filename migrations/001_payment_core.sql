CREATE TABLE payments (
    id                  TEXT PRIMARY KEY,
    external_reference  TEXT NOT NULL,
    currency            TEXT NOT NULL,
    amount_minor        BIGINT NOT NULL CHECK (amount_minor > 0),
    tenant_id          TEXT NOT NULL,
    payment_status      TEXT NOT NULL CHECK (payment_status IN ('created', 'processing', 'succeeded', 'failed')),
    funds_status        TEXT NOT NULL CHECK (funds_status IN ('available', 'reserved', 'consumed', 'returned')),
    idempotency_key     TEXT NOT NULL UNIQUE,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    CHECK (
        (payment_status = 'created' AND funds_status = 'available') OR
        (payment_status = 'processing' AND funds_status = 'reserved') OR
        (payment_status = 'succeeded' AND funds_status = 'consumed') OR
        (payment_status = 'failed' AND funds_status IN ('available', 'reserved', 'returned'))
    )
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
    payment_status TEXT NOT NULL,
    note        TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE payment_destinations (
    payment_id   TEXT PRIMARY KEY REFERENCES payments(id),
    kind         TEXT NOT NULL CHECK (kind = 'blockchain_address'),
    chain        TEXT,
    address      TEXT,
    created_at   TIMESTAMPTZ NOT NULL,
    CHECK (kind = 'blockchain_address' AND chain IS NOT NULL AND address IS NOT NULL)
);

CREATE TABLE payment_refunds (
    id                TEXT PRIMARY KEY,
    payment_id        TEXT NOT NULL REFERENCES payments(id),
    refund_payment_id TEXT NOT NULL UNIQUE REFERENCES payments(id),
    tenant_id         TEXT NOT NULL,
    idempotency_key   TEXT NOT NULL,
    amount_minor      BIGINT NOT NULL CHECK (amount_minor > 0),
    currency          TEXT NOT NULL,
    reason            TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL,
    CHECK (payment_id <> refund_payment_id)
);

CREATE UNIQUE INDEX payment_refunds_tenant_idempotency_idx
    ON payment_refunds (tenant_id, idempotency_key);
CREATE INDEX payment_refunds_payment_idx
    ON payment_refunds (payment_id, created_at, id);
