use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_gauge, register_histogram_vec, CounterVec, Gauge, HistogramVec,
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

lazy_static! {
    pub static ref MERKLE_TREE_BUILD_DURATION_MS: HistogramVec = register_histogram_vec!(
        "utility_merkle_tree_build_duration_ms",
        "Merkle tree construction duration in milliseconds",
        &["commodity_type"]
    )
    .unwrap();
    pub static ref BATCH_PROOF_SUBMISSION_COUNT: CounterVec = register_counter_vec!(
        "utility_batch_proof_submission_count_total",
        "Total number of Merkle batch proof submissions",
        &["commodity_type", "status"]
    )
    .unwrap();
    pub static ref ONCHAIN_VERIFICATION_GAS_USED: HistogramVec = register_histogram_vec!(
        "utility_onchain_verification_gas_used",
        "Soroban on-chain Merkle batch verification gas used",
        &["commodity_type"]
    )
    .unwrap();
}

pub fn record_merkle_tree_build_duration_ms(commodity_type: &str, duration_ms: f64) {
    MERKLE_TREE_BUILD_DURATION_MS
        .with_label_values(&[commodity_type])
        .observe(duration_ms);
}

pub fn record_batch_proof_submission(commodity_type: &str, status: &str) {
    BATCH_PROOF_SUBMISSION_COUNT
        .with_label_values(&[commodity_type, status])
        .inc();
}

pub fn record_onchain_verification_gas_used(commodity_type: &str, gas_used: f64) {
    ONCHAIN_VERIFICATION_GAS_USED
        .with_label_values(&[commodity_type])
        .observe(gas_used);
}
