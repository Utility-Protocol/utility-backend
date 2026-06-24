CREATE TABLE IF NOT EXISTS sync_cursors (
    contract_id TEXT PRIMARY KEY,
    last_ledger_sequence BIGINT NOT NULL DEFAULT 0,
    last_timestamp TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS soroban_events (
    contract_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (contract_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_sequence
    ON soroban_events (contract_id, ledger_sequence ASC);

CREATE TABLE IF NOT EXISTS soroban_event_gaps (
    contract_id TEXT NOT NULL,
    start_sequence BIGINT NOT NULL,
    end_sequence BIGINT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    UNIQUE (contract_id, start_sequence, end_sequence)
);

CREATE INDEX IF NOT EXISTS idx_soroban_event_gaps_unresolved
    ON soroban_event_gaps (contract_id, detected_at DESC)
    WHERE resolved_at IS NULL;
