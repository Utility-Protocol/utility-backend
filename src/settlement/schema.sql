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
