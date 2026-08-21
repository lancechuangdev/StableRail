CREATE TABLE payments (
    id                  TEXT PRIMARY KEY,
    direction           TEXT NOT NULL CHECK (direction IN ('payin', 'payout')),
    external_reference  TEXT NOT NULL,
    currency            TEXT NOT NULL,
    amount_minor        BIGINT NOT NULL CHECK (amount_minor > 0),
    tenant_id          TEXT NOT NULL,
    payment_status      TEXT NOT NULL CHECK (payment_status IN ('created', 'processing', 'succeeded', 'failed')),
    idempotency_key     TEXT NOT NULL UNIQUE,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
);

-- Provider resources are opaque routing endpoints shared by pay-ins and payouts.
-- Provider-specific customer, account, and wallet data remains adapter-owned.
CREATE TABLE provider_resources (
    id                 TEXT PRIMARY KEY,
    tenant_id          TEXT NOT NULL,
    provider           TEXT NOT NULL,
    resource_type      TEXT NOT NULL CHECK (resource_type IN ('account','payment_instrument')),
    provider_reference TEXT NOT NULL,
    metadata           JSONB NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    UNIQUE (provider, resource_type, provider_reference)
);

CREATE INDEX provider_resources_tenant_idx
    ON provider_resources (tenant_id, resource_type, created_at, id);

CREATE TABLE payment_quotes (
    id                        TEXT PRIMARY KEY,
    direction                 TEXT NOT NULL CHECK (direction IN ('payin', 'payout')),
    provider                  TEXT NOT NULL,
    provider_quote_id         TEXT NOT NULL,
    tenant_id                 TEXT NOT NULL,
    idempotency_key           TEXT NOT NULL,
    source_resource_id        TEXT REFERENCES provider_resources(id),
    destination_resource_id   TEXT NOT NULL REFERENCES provider_resources(id),
    payment_method            TEXT NOT NULL,
    source_currency           TEXT NOT NULL,
    destination_currency      TEXT NOT NULL,
    currency_type             TEXT NOT NULL CHECK (currency_type IN ('sender', 'receiver')),
    cover_fees                BOOLEAN NOT NULL,
    request_amount_minor      BIGINT NOT NULL CHECK (request_amount_minor > 0),
    sender_amount_minor       BIGINT NOT NULL CHECK (sender_amount_minor > 0),
    receiver_amount_minor     BIGINT NOT NULL CHECK (receiver_amount_minor > 0),
    commercial_rate           TEXT,
    provider_rate             TEXT,
    flat_fee_minor            BIGINT CHECK (flat_fee_minor >= 0),
    partner_fee_minor         BIGINT CHECK (partner_fee_minor >= 0),
    billing_fee_minor         BIGINT,
    status                    TEXT NOT NULL CHECK (status IN ('open', 'accepted', 'expired')),
    expires_at                TIMESTAMPTZ NOT NULL,
    provider_payload          JSONB NOT NULL,
    provider_execution_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    payment_id                TEXT UNIQUE REFERENCES payments(id),
    created_at                TIMESTAMPTZ NOT NULL,
    updated_at                TIMESTAMPTZ NOT NULL,
    UNIQUE (provider, provider_quote_id),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX payment_quotes_expiry_idx
    ON payment_quotes (direction, status, expires_at);
CREATE INDEX payment_quotes_tenant_idx
    ON payment_quotes (tenant_id, direction, status);

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

CREATE TABLE payment_refunds (
    refund_payment_id   TEXT PRIMARY KEY REFERENCES payments(id),
    original_payment_id TEXT NOT NULL REFERENCES payments(id),
    tenant_id           TEXT NOT NULL,
    idempotency_key     TEXT NOT NULL,
    amount_minor        BIGINT NOT NULL CHECK (amount_minor > 0),
    currency            TEXT NOT NULL,
    reason              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    CHECK (original_payment_id <> refund_payment_id),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX payment_refunds_original_payment_idx
    ON payment_refunds (original_payment_id, created_at, refund_payment_id);
