CREATE TABLE payouts (
    payment_id            TEXT PRIMARY KEY REFERENCES payments(id),
    quote_id              TEXT UNIQUE REFERENCES payment_quotes(id),
    tenant_id             TEXT NOT NULL,
    source_account_id     TEXT NOT NULL REFERENCES provider_resources(id),
    destination_instrument_id TEXT NOT NULL REFERENCES provider_resources(id),
    payout_method         TEXT NOT NULL,
    source_amount_minor   BIGINT NOT NULL CHECK (source_amount_minor > 0),
    source_currency       TEXT NOT NULL,
    destination_amount_minor BIGINT NOT NULL CHECK (destination_amount_minor > 0),
    destination_currency  TEXT NOT NULL,
    provider              TEXT NOT NULL,
    provider_payout_id    TEXT,
    provider_status       TEXT NOT NULL CHECK (provider_status IN (
        'submission_pending', 'unknown', 'submission_failed', 'processing',
        'on_hold', 'completed', 'failed', 'refunded'
    )),
    idempotency_key       TEXT NOT NULL,
    provider_payload      JSONB,
    last_error            TEXT,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    submitted_at          TIMESTAMPTZ,
    UNIQUE (provider, provider_payout_id),
    UNIQUE (provider, idempotency_key)
);

CREATE INDEX payouts_status_idx ON payouts (provider_status, updated_at);
CREATE INDEX payouts_tenant_idx ON payouts (tenant_id, created_at, payment_id);
