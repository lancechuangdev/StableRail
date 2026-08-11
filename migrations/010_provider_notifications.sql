CREATE TABLE provider_notifications (
    provider        TEXT NOT NULL,
    notification_id TEXT NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload         JSONB NOT NULL,
    PRIMARY KEY (provider, notification_id)
);
