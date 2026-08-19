CREATE TABLE IF NOT EXISTS rate_limit_configs (
    id BIGSERIAL PRIMARY KEY,
    scope_type VARCHAR(16) NOT NULL CHECK (scope_type IN ('global', 'service', 'user')),
    scope_key VARCHAR(255) NOT NULL,
    max_tokens BIGINT NOT NULL CHECK (max_tokens > 0),
    refill_rate BIGINT NOT NULL CHECK (refill_rate >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (scope_type, scope_key)
);

CREATE INDEX IF NOT EXISTS idx_rate_limit_configs_scope
    ON rate_limit_configs (scope_type, scope_key);
