CREATE TABLE IF NOT EXISTS webhook_endpoints (
    id VARCHAR PRIMARY KEY,
    url VARCHAR NOT NULL,
    secret VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dead_letter_webhooks (
    id UUID PRIMARY KEY,
    endpoint_id VARCHAR NOT NULL,
    event_id UUID NOT NULL,
    payload JSONB NOT NULL,
    event_type VARCHAR NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_endpoint_id
    ON dead_letter_webhooks (endpoint_id, failed_at DESC);
