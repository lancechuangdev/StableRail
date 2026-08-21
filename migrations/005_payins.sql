CREATE TABLE payins (
    id                 TEXT PRIMARY KEY,
    payment_id         TEXT NOT NULL UNIQUE REFERENCES payments(id),
    quote_id           TEXT UNIQUE REFERENCES payment_quotes(id),
    tenant_id          TEXT NOT NULL,
    idempotency_key    TEXT NOT NULL,
    funding_method     TEXT NOT NULL,
    source_instrument_id TEXT REFERENCES provider_resources(id),
    destination_account_id TEXT NOT NULL REFERENCES provider_resources(id),
    source_amount_minor BIGINT NOT NULL CHECK (source_amount_minor > 0),
    source_currency    TEXT NOT NULL,
    destination_amount_minor BIGINT NOT NULL CHECK (destination_amount_minor > 0),
    destination_currency TEXT NOT NULL,
    provider           TEXT NOT NULL,
    provider_payin_id  TEXT,
    settlement_status  TEXT NOT NULL CHECK (settlement_status IN ('created','submission_pending','unknown','processing','on_hold','received','failed','refunded')),
    reconciliation_status TEXT NOT NULL DEFAULT 'unmatched' CHECK (reconciliation_status IN ('unmatched','matched','exception')),
    instructions       JSONB NOT NULL,
    provider_payload   JSONB NOT NULL,
    failure_reason     TEXT,
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    UNIQUE (provider, provider_payin_id),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX payins_tenant_idx ON payins (tenant_id, created_at, id);
CREATE INDEX payins_status_idx ON payins (settlement_status, updated_at);
