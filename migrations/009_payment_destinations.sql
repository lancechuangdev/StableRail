CREATE TABLE payment_destinations (
    payment_id   TEXT PRIMARY KEY REFERENCES payments(id),
    kind         TEXT NOT NULL CHECK (kind = 'blockchain_address'),
    chain        TEXT,
    address      TEXT,
    created_at   TIMESTAMPTZ NOT NULL,
    CHECK (
        kind = 'blockchain_address' AND chain IS NOT NULL AND address IS NOT NULL
    )
);
