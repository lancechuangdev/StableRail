CREATE TABLE blindpay_webhook_events (
    svix_id          TEXT PRIMARY KEY,
    webhook_event    TEXT NOT NULL,
    provider_payout_id TEXT,
    payload          JSONB NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX blindpay_webhook_events_payout_idx
    ON blindpay_webhook_events (provider_payout_id, received_at);
