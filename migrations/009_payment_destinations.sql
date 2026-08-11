CREATE TABLE payment_destinations (
    payment_id   TEXT PRIMARY KEY REFERENCES payments(id),
    kind         TEXT NOT NULL CHECK (kind IN ('circle_recipient','blockchain_address')),
    recipient_id TEXT,
    chain        TEXT,
    address      TEXT,
    created_at   TIMESTAMPTZ NOT NULL,
    CHECK (
        (kind = 'circle_recipient' AND recipient_id IS NOT NULL AND chain IS NULL AND address IS NULL) OR
        (kind = 'blockchain_address' AND recipient_id IS NULL AND chain IS NOT NULL AND address IS NOT NULL)
    )
);
