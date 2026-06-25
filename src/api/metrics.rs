use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_histogram,
    register_histogram_vec, Counter, CounterVec, Gauge, Histogram, HistogramVec,
};

lazy_static! {
    pub static ref GC_PAUSE_SECONDS: Gauge = register_gauge!(
        "utility_gc_pause_seconds",
        "Cumulative allocator pause time in seconds"
    )
    .unwrap();
    pub static ref DB_POOL_STARVATION: Gauge = register_gauge!(
        "utility_db_pool_starvation_count",
        "Number of database pool starvation events"
    )
    .unwrap();
    pub static ref INGESTED_EVENTS: CounterVec = register_counter_vec!(
        "utility_ingested_events_total",
        "Total number of ingested meter events",
        &["meter_id", "status"]
    )
    .unwrap();
    pub static ref RPC_LATENCY: HistogramVec = register_histogram_vec!(
        "utility_soroban_rpc_latency_seconds",
        "Soroban RPC call latency in seconds",
        &["method"]
    )
    .unwrap();
    pub static ref ACTIVE_CONNECTIONS: Gauge = register_gauge!(
        "utility_active_gateway_connections",
        "Number of currently active gateway connections"
    )
    .unwrap();
    pub static ref DB_ACTIVE_CONNECTIONS: Gauge = register_gauge!(
        "utility_db_active_connections",
        "Number of active database connections from pg_stat_activity"
    )
    .unwrap();
    pub static ref DB_IDLE_CONNECTIONS: Gauge = register_gauge!(
        "utility_db_idle_connections",
        "Number of idle database connections"
    )
    .unwrap();
    pub static ref DB_WAITING_REQUESTS: Gauge = register_gauge!(
        "utility_db_waiting_requests",
        "Number of waiting database requests"
    )
    .unwrap();
    pub static ref COMPACTION_ATTEMPTS: Counter = register_counter!(
        "utility_compaction_attempts_total",
        "Total chunk compaction attempts"
    )
    .unwrap();
    pub static ref COMPACTION_SKIPPED: Counter = register_counter!(
        "utility_compaction_skipped_total",
        "Total chunk compactions skipped due to hot chunk leases or lock contention"
    )
    .unwrap();
    pub static ref COMPACTION_LOCK_CONTENTIONS: Counter = register_counter!(
        "utility_compaction_lock_contentions_total",
        "Total critical compaction lock contention alerts"
    )
    .unwrap();
    pub static ref COMPACTION_DURATION_MS: Histogram = register_histogram!(
        "utility_compaction_duration_ms",
        "Chunk compaction duration in milliseconds"
    )
    .unwrap();
}

pub fn record_ingestion(meter_id: &str, status: &str) {
    INGESTED_EVENTS.with_label_values(&[meter_id, status]).inc();
}

pub fn record_db_starvation() {
    DB_POOL_STARVATION.inc();
}

pub fn record_rpc_latency(method: &str, latency_seconds: f64) {
    RPC_LATENCY
        .with_label_values(&[method])
        .observe(latency_seconds);
}

pub fn set_db_active_connections(count: f64) {
    DB_ACTIVE_CONNECTIONS.set(count);
}

pub fn set_db_idle_connections(count: f64) {
    DB_IDLE_CONNECTIONS.set(count);
}

pub fn set_db_waiting_requests(count: f64) {
    DB_WAITING_REQUESTS.set(count);
}

pub fn get_starvation_count() -> f64 {
    DB_POOL_STARVATION.get()
}

pub fn record_compaction_attempt() {
    COMPACTION_ATTEMPTS.inc();
}

pub fn record_compaction_skipped() {
    COMPACTION_SKIPPED.inc();
}

pub fn record_compaction_lock_contention() {
    COMPACTION_LOCK_CONTENTIONS.inc();
}

pub fn record_compaction_duration(duration_ms: f64) {
    COMPACTION_DURATION_MS.observe(duration_ms);
}
