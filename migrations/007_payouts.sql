CREATE TABLE payout_quotes (
    id                        TEXT PRIMARY KEY,
    provider                  TEXT NOT NULL,
    provider_quote_id         TEXT NOT NULL,
    tenant_id                 TEXT NOT NULL,
    bank_account_id           TEXT NOT NULL,
    managed_wallet_id         TEXT NOT NULL,
    source_currency           TEXT NOT NULL,
    destination_currency      TEXT NOT NULL,
    currency_type             TEXT NOT NULL CHECK (currency_type IN ('sender', 'receiver')),
    cover_fees                BOOLEAN NOT NULL,
    sender_amount_minor       BIGINT NOT NULL CHECK (sender_amount_minor > 0),
    receiver_amount_minor     BIGINT NOT NULL CHECK (receiver_amount_minor > 0),
    commercial_rate           TEXT NOT NULL,
    provider_rate             TEXT NOT NULL,
    flat_fee_minor            BIGINT NOT NULL CHECK (flat_fee_minor >= 0),
    partner_fee_minor         BIGINT NOT NULL CHECK (partner_fee_minor >= 0),
    billing_fee_minor         BIGINT,
    status                    TEXT NOT NULL CHECK (status IN ('open', 'accepted', 'expired')),
    expires_at                TIMESTAMPTZ NOT NULL,
    provider_payload          JSONB NOT NULL,
    payment_id                TEXT UNIQUE REFERENCES payments(id),
    created_at                TIMESTAMPTZ NOT NULL,
    updated_at                TIMESTAMPTZ NOT NULL,
    UNIQUE (provider, provider_quote_id)
);

CREATE INDEX payout_quotes_expiry_idx ON payout_quotes (status, expires_at);
CREATE INDEX payout_quotes_tenant_idx ON payout_quotes (tenant_id, status);

CREATE TABLE payouts (
    payment_id            TEXT PRIMARY KEY REFERENCES payments(id),
    quote_id              TEXT NOT NULL UNIQUE REFERENCES payout_quotes(id),
    provider              TEXT NOT NULL,
    provider_payout_id    TEXT,
    provider_status       TEXT NOT NULL CHECK (provider_status IN (
        'submission_pending', 'unknown', 'submission_failed', 'processing',
        'on_hold', 'completed', 'failed', 'refunded'
    )),
    idempotency_key       TEXT NOT NULL,
    sender_wallet_id      TEXT NOT NULL,
    sender_wallet_address TEXT NOT NULL,
    provider_payload      JSONB,
    last_error            TEXT,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,
    submitted_at          TIMESTAMPTZ,
    UNIQUE (provider, provider_payout_id),
    UNIQUE (provider, idempotency_key)
);

CREATE INDEX payouts_status_idx ON payouts (provider_status, updated_at);
