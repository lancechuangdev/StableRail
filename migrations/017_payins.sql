CREATE TABLE payin_quotes (
    id                    TEXT PRIMARY KEY,
    provider              TEXT NOT NULL,
    provider_quote_id     TEXT NOT NULL,
    tenant_id             TEXT NOT NULL,
    idempotency_key       TEXT NOT NULL,
    payment_method        TEXT NOT NULL,
    currency_type         TEXT NOT NULL CHECK (currency_type IN ('sender','receiver')),
    cover_fees            BOOLEAN NOT NULL,
    request_amount_minor  BIGINT NOT NULL CHECK (request_amount_minor > 0),
    token                 TEXT NOT NULL,
    destination_type      TEXT NOT NULL CHECK (destination_type IN ('managed_wallet','blockchain_wallet')),
    destination_id        TEXT NOT NULL,
    source_currency       TEXT NOT NULL,
    destination_currency  TEXT NOT NULL,
    sender_amount_minor   BIGINT NOT NULL CHECK (sender_amount_minor > 0),
    receiver_amount_minor BIGINT NOT NULL CHECK (receiver_amount_minor > 0),
    status                TEXT NOT NULL CHECK (status IN ('open','accepted','expired')),
    expires_at            TIMESTAMPTZ NOT NULL,
    provider_payload      JSONB NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (provider, provider_quote_id)
);

CREATE TABLE payins (
    id                 TEXT PRIMARY KEY,
    quote_id           TEXT NOT NULL UNIQUE REFERENCES payin_quotes(id),
    tenant_id          TEXT NOT NULL,
    idempotency_key    TEXT NOT NULL,
    provider           TEXT NOT NULL,
    provider_payin_id  TEXT NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ('processing','on_hold','succeeded','failed','refunded')),
    instructions       JSONB NOT NULL,
    provider_payload   JSONB NOT NULL,
    failure_reason     TEXT,
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    UNIQUE (provider, provider_payin_id),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX payins_tenant_idx ON payins (tenant_id, created_at, id);

ALTER TABLE ledger_transactions ALTER COLUMN payment_id DROP NOT NULL;
ALTER TABLE ledger_transactions ADD COLUMN payin_id TEXT REFERENCES payins(id);
ALTER TABLE ledger_transactions ADD CHECK (num_nonnulls(payment_id, payin_id) = 1);
ALTER TABLE ledger_transactions ADD UNIQUE (payin_id, event_type);

CREATE TABLE payin_webhook_applications (
    svix_id    TEXT PRIMARY KEY REFERENCES blindpay_webhook_events(svix_id),
    payin_id   TEXT NOT NULL REFERENCES payins(id),
    applied_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE webhook_deliveries ALTER COLUMN payment_id DROP NOT NULL;
ALTER TABLE webhook_deliveries ADD COLUMN payin_id TEXT REFERENCES payins(id);
ALTER TABLE webhook_deliveries ADD CHECK (num_nonnulls(payment_id, payin_id) = 1);
CREATE INDEX webhook_deliveries_payin_idx ON webhook_deliveries (payin_id, created_at, id);
