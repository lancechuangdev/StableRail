CREATE TABLE webhook_endpoints (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    url         TEXT NOT NULL,
    secret      TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX webhook_endpoints_tenant_idx ON webhook_endpoints (tenant_id) WHERE active;

CREATE TABLE webhook_deliveries (
    id              TEXT PRIMARY KEY,
    endpoint_id     TEXT NOT NULL REFERENCES webhook_endpoints(id),
    event_id        TEXT NOT NULL,
    payment_id      TEXT NOT NULL REFERENCES payments(id),
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','delivered','failed')),
    attempt_count   INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    response_status INTEGER,
    delivered_at    TIMESTAMPTZ,
    failed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (endpoint_id, event_id)
);

CREATE INDEX webhook_deliveries_pending_idx
    ON webhook_deliveries (next_attempt_at, created_at) WHERE status = 'pending';
CREATE INDEX webhook_deliveries_payment_idx
    ON webhook_deliveries (payment_id, created_at, id);
