CREATE TABLE reconciliation_runs (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    discrepancy_count   INTEGER NOT NULL DEFAULT 0,
    error_message       TEXT
);

CREATE TABLE reconciliation_discrepancies (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fingerprint         TEXT NOT NULL UNIQUE,
    kind                TEXT NOT NULL,
    payment_id          TEXT REFERENCES payments(id),
    details             JSONB NOT NULL,
    status              TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','resolved')),
    first_detected_at   TIMESTAMPTZ NOT NULL,
    last_detected_at    TIMESTAMPTZ NOT NULL,
    last_seen_run_id    BIGINT NOT NULL REFERENCES reconciliation_runs(id),
    resolved_at         TIMESTAMPTZ,
    resolved_by         TEXT,
    resolution_note     TEXT
);

CREATE INDEX reconciliation_discrepancies_open_idx
    ON reconciliation_discrepancies (last_detected_at, id) WHERE status = 'open';
CREATE INDEX reconciliation_discrepancies_payment_idx
    ON reconciliation_discrepancies (payment_id, first_detected_at, id);
