-- TimescaleDB hypertable continuous compression policy
-- Compatible with both PostgreSQL and TimescaleDB
--
-- The static compression/retention policies below serve as reasonable
-- defaults.  At runtime the CompressionPolicyManager (see
-- src/time_series/compression.rs) monitors chunk-level compression lag,
-- dynamically adjusts the compression window based on ingestion rates,
-- and prioritises the oldest uncompressed chunks first.
--
-- Dry-run mode lets operators preview what WOULD be compressed before
-- applying changes.

CREATE TABLE IF NOT EXISTS meter_readings (
    meter_id TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    reading_kwh DOUBLE PRECISION,
    voltage DOUBLE PRECISION,
    current_amps DOUBLE PRECISION,
    temperature_c DOUBLE PRECISION,
    metadata JSONB
);

-- TimescaleDB-specific setup (silently skipped on plain PostgreSQL)
DO $$
BEGIN
    PERFORM * FROM pg_extension WHERE extname = 'timescaledb';
    IF FOUND THEN
        PERFORM create_hypertable('meter_readings', 'recorded_at', if_not_exists => TRUE);
        ALTER TABLE meter_readings SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'meter_id',
            timescaledb.compress_orderby = 'recorded_at DESC'
        );

        -- Base compression policy: compress chunks older than 3 days.
        -- The CompressionPolicyManager may override this value at runtime
        -- when ingestion spikes (halving the interval, min 1 day) or
        -- drops below baseline (doubling the interval, max 7 days).
        PERFORM add_compression_policy('meter_readings', INTERVAL '3 days', if_not_exists => TRUE);

        -- Retention policy: automatically drop data older than 365 days.
        -- Retention is never shortened and data is never deleted before
        -- 365 days to meet compliance requirements.
        PERFORM add_retention_policy('meter_readings', INTERVAL '365 days', if_not_exists => TRUE);

        -- Alert: compression lag monitoring
        -- The CompressionPolicyManager background task queries
        -- timescaledb_information.chunks every 60 s and fires a warning
        -- when any uncompressed chunk has lag > 2 days.
    END IF;
END
$$;

-- Composite index for efficient per-meter time-range queries
CREATE INDEX IF NOT EXISTS idx_meter_readings_meter_id_ts
    ON meter_readings (meter_id, recorded_at DESC);

-- Telemetry events table with per-meter monotonic sequencing
CREATE TABLE IF NOT EXISTS telemetry_events (
    id BIGSERIAL PRIMARY KEY,
    meter_id TEXT NOT NULL,
    resource_type TEXT NOT NULL DEFAULT 'electricity',
    recorded_at TIMESTAMPTZ NOT NULL,
    reading DOUBLE PRECISION NOT NULL,
    sequence INTEGER NOT NULL,
    UNIQUE(meter_id, sequence)
);

CREATE TABLE IF NOT EXISTS materialization_state (
    name TEXT PRIMARY KEY,
    high_watermark BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS materialization_pending (
    batch_id UUID PRIMARY KEY,
    lower_watermark BIGINT NOT NULL,
    upper_watermark BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('staging', 'committed')),
    started_at TIMESTAMPTZ NOT NULL,
    committed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_materialization_pending_staging
    ON materialization_pending (started_at ASC)
    WHERE status = 'staging';

CREATE TABLE IF NOT EXISTS materialization_checkpoint (
    batch_id UUID PRIMARY KEY,
    new_watermark BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS materialized_hourly_usage (
    hour TIMESTAMPTZ NOT NULL,
    resource_type TEXT NOT NULL,
    usage DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (hour, resource_type)
);

-- Index for efficient sequence-based retrieval by the tariff engine
CREATE INDEX IF NOT EXISTS idx_telemetry_events_meter_id_sequence
    ON telemetry_events (meter_id, sequence ASC);

CREATE INDEX IF NOT EXISTS idx_telemetry_events_id
    ON telemetry_events (id ASC);

-- Hypertable setup for telemetry_events (if TimescaleDB is present)
DO $$
BEGIN
    PERFORM * FROM pg_extension WHERE extname = 'timescaledb';
    IF FOUND THEN
        PERFORM create_hypertable('telemetry_events', 'recorded_at', if_not_exists => TRUE);
    END IF;
END
$$;
