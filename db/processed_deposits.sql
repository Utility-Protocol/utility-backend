CREATE TABLE IF NOT EXISTS processed_deposits (
    id BIGSERIAL PRIMARY KEY,
    deposit_id TEXT NOT NULL,
    idempotency_key UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT processed_deposits_deposit_id_key UNIQUE (deposit_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS processed_deposits_idempotency_key_idx
    ON processed_deposits (idempotency_key);
