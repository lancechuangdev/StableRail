CREATE TABLE quotes (
    id                        TEXT PRIMARY KEY,
    source_currency           TEXT NOT NULL CHECK (char_length(source_currency) = 3),
    destination_currency      TEXT NOT NULL CHECK (char_length(destination_currency) = 3),
    source_amount_minor       BIGINT NOT NULL CHECK (source_amount_minor > 0),
    destination_amount_minor  BIGINT NOT NULL CHECK (destination_amount_minor > 0),
    rate_scaled               BIGINT NOT NULL CHECK (rate_scaled > 0),
    fee_minor                 BIGINT NOT NULL CHECK (fee_minor >= 0),
    status                    TEXT NOT NULL CHECK (status IN ('open', 'accepted', 'expired')),
    expires_at                TIMESTAMPTZ NOT NULL,
    created_at                TIMESTAMPTZ NOT NULL
);

ALTER TABLE payments ADD COLUMN quote_id TEXT UNIQUE REFERENCES quotes(id);
CREATE INDEX quotes_expiry_idx ON quotes (status, expires_at);
