CREATE TABLE blindpay_customers (
    local_customer_id   TEXT PRIMARY KEY,
    provider_customer_id TEXT NOT NULL UNIQUE CHECK (provider_customer_id LIKE 're\_%' ESCAPE '\'),
    kyc_status          TEXT NOT NULL CHECK (kyc_status IN (
        'verifying', 'approved', 'rejected', 'compliance_request', 'approved_rfi'
    )),
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE blindpay_bank_accounts (
    provider_bank_account_id TEXT PRIMARY KEY CHECK (provider_bank_account_id LIKE 'ba\_%' ESCAPE '\'),
    local_customer_id        TEXT NOT NULL REFERENCES blindpay_customers(local_customer_id),
    rail                     TEXT NOT NULL,
    display_name             TEXT NOT NULL,
    account_last_four        TEXT NOT NULL CHECK (char_length(account_last_four) <= 4),
    status                   TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at               TIMESTAMPTZ NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL
);

CREATE INDEX blindpay_bank_accounts_customer_idx
    ON blindpay_bank_accounts (local_customer_id, status);

CREATE TABLE blindpay_managed_wallets (
    provider_wallet_id TEXT PRIMARY KEY CHECK (provider_wallet_id LIKE 'bl\_%' ESCAPE '\'),
    local_customer_id  TEXT NOT NULL REFERENCES blindpay_customers(local_customer_id),
    network            TEXT NOT NULL,
    address            TEXT NOT NULL,
    display_name       TEXT NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    UNIQUE (network, address)
);

CREATE INDEX blindpay_managed_wallets_customer_idx
    ON blindpay_managed_wallets (local_customer_id, status);
