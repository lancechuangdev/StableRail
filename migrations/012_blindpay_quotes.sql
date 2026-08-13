CREATE TABLE blindpay_quotes (
    id                        TEXT PRIMARY KEY,
    provider                  TEXT NOT NULL CHECK (provider = 'blindpay'),
    provider_quote_id         TEXT NOT NULL,
    tenant_id         TEXT NOT NULL REFERENCES blindpay_customers(tenant_id),
    provider_bank_account_id  TEXT NOT NULL REFERENCES blindpay_bank_accounts(provider_bank_account_id),
    provider_wallet_id        TEXT NOT NULL REFERENCES blindpay_managed_wallets(provider_wallet_id),
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

CREATE INDEX blindpay_quotes_expiry_idx ON blindpay_quotes (status, expires_at);
CREATE INDEX blindpay_quotes_tenant_idx ON blindpay_quotes (tenant_id, status);
