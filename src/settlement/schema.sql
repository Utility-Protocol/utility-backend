-- Settlement schema for resource token minting

CREATE TABLE IF NOT EXISTS pending_mints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    destination_wallet TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_mints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(batch_id, resource_type)
);

CREATE INDEX IF NOT EXISTS idx_pending_mints_batch_id ON pending_mints(batch_id);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    error_reason TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'failed',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(queue_name, message_id)
);

CREATE INDEX IF NOT EXISTS idx_dlq_status ON dead_letter_queue(status);
