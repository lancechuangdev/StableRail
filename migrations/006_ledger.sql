CREATE TABLE ledger_accounts (
    code          TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    account_type  TEXT NOT NULL CHECK (account_type IN ('asset', 'liability', 'equity', 'revenue', 'expense'))
);

INSERT INTO ledger_accounts (code, name, account_type) VALUES
    ('cash:operating', 'Operating cash', 'asset'),
    ('settlement:payable', 'Settlement payable', 'liability');

CREATE TABLE ledger_transactions (
    id           TEXT PRIMARY KEY,
    payment_id   TEXT NOT NULL REFERENCES payments(id),
    event_type   TEXT NOT NULL,
    ledger_status TEXT NOT NULL DEFAULT 'posted' CHECK (ledger_status IN ('pending','posted','failed')),
    occurred_at  TIMESTAMPTZ NOT NULL,
    UNIQUE (payment_id, event_type)
);

CREATE TABLE ledger_entries (
    id              TEXT PRIMARY KEY,
    transaction_id  TEXT NOT NULL REFERENCES ledger_transactions(id),
    account_code    TEXT NOT NULL REFERENCES ledger_accounts(code),
    side            TEXT NOT NULL CHECK (side IN ('debit', 'credit')),
    amount_minor    BIGINT NOT NULL CHECK (amount_minor > 0),
    currency        TEXT NOT NULL
);

CREATE INDEX ledger_transactions_payment_idx
    ON ledger_transactions (payment_id, occurred_at, id);
CREATE INDEX ledger_entries_transaction_idx
    ON ledger_entries (transaction_id, id);

CREATE TABLE payment_returns (
    id                    TEXT PRIMARY KEY,
    payment_id            TEXT NOT NULL UNIQUE REFERENCES payments(id),
    provider              TEXT NOT NULL,
    provider_event_id     TEXT NOT NULL UNIQUE,
    amount_minor          BIGINT NOT NULL CHECK (amount_minor > 0),
    currency              TEXT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('created', 'processing', 'succeeded', 'failed')),
    reason                TEXT NOT NULL,
    ledger_transaction_id TEXT NOT NULL UNIQUE REFERENCES ledger_transactions(id),
    occurred_at           TIMESTAMPTZ NOT NULL
);

CREATE INDEX payment_returns_payment_idx ON payment_returns (payment_id, occurred_at, id);
