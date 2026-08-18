CREATE TABLE tenant_api_keys (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    name         TEXT NOT NULL,
    secret_hash  BYTEA NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    last_used_at TIMESTAMPTZ,
    revoked_at  TIMESTAMPTZ
);

CREATE INDEX tenant_api_keys_tenant_idx
    ON tenant_api_keys (tenant_id, created_at, id)
    WHERE revoked_at IS NULL;
