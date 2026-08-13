CREATE TABLE payments (
    id                  TEXT PRIMARY KEY,
    external_reference  TEXT NOT NULL,
    currency            TEXT NOT NULL,
    amount_minor        BIGINT NOT NULL CHECK (amount_minor > 0),
    tenant_id          TEXT NOT NULL,
    state               TEXT NOT NULL CHECK (state IN ('created', 'processing', 'settled', 'failed', 'refunded')),
    idempotency_key     TEXT NOT NULL UNIQUE,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
);

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
    state       TEXT NOT NULL,
    note        TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);
