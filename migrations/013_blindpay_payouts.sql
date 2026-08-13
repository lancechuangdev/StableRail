CREATE TABLE blindpay_payouts (
    payment_id          TEXT PRIMARY KEY REFERENCES payments(id),
    quote_id            TEXT NOT NULL UNIQUE REFERENCES blindpay_quotes(id),
    provider_payout_id  TEXT UNIQUE,
    provider_status     TEXT NOT NULL CHECK (provider_status IN (
        'submission_pending', 'unknown', 'submission_failed', 'processing',
        'on_hold', 'completed', 'failed', 'refunded'
    )),
    idempotency_key     TEXT NOT NULL UNIQUE,
    sender_wallet_id    TEXT NOT NULL REFERENCES blindpay_managed_wallets(provider_wallet_id),
    sender_wallet_address TEXT NOT NULL,
    provider_payload    JSONB,
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    submitted_at        TIMESTAMPTZ
);

CREATE INDEX blindpay_payouts_status_idx ON blindpay_payouts (provider_status, updated_at);
