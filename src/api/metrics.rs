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
    pub static ref TCP_PARTIAL_FRAMES_BUFFERED: Counter = register_counter!(
        "tcp_partial_frames_buffered",
        "Number of TCP reads buffered by the frame reassembly layer"
    )
    .unwrap();
    pub static ref TCP_COMPLETE_FRAMES_DELIVERED: Counter = register_counter!(
        "tcp_complete_frames_delivered",
        "Number of complete TCP frames delivered by the reassembly layer"
    )
    .unwrap();
    pub static ref TCP_FRAME_TOO_LARGE_ERRORS: Counter = register_counter!(
        "tcp_frame_too_large_errors",
        "Number of TCP frames rejected because their payload exceeded the configured maximum"
    )
    .unwrap();
    pub static ref TCP_BUFFER_EXCEEDED_RESETS: Counter = register_counter!(
        "tcp_buffer_exceeded_resets",
        "Number of TCP connections reset because their reassembly buffer exceeded the configured maximum"
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
pub fn record_tcp_partial_frame_buffered() {
    TCP_PARTIAL_FRAMES_BUFFERED.inc();
}

pub fn record_tcp_complete_frame_delivered() {
    TCP_COMPLETE_FRAMES_DELIVERED.inc();
}

pub fn record_tcp_frame_too_large_error() {
    TCP_FRAME_TOO_LARGE_ERRORS.inc();
}

pub fn record_tcp_buffer_exceeded_reset() {
    TCP_BUFFER_EXCEEDED_RESETS.inc();
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

pub fn record_compaction_skipped() {
    COMPACTION_SKIPPED.inc();
}

pub fn record_compaction_lock_contention() {
    COMPACTION_LOCK_CONTENTIONS.inc();
}

pub fn record_compaction_duration(duration_ms: f64) {
    COMPACTION_DURATION_MS.observe(duration_ms);
}

lazy_static! {
    pub static ref FD_CURRENT_OPEN: Gauge = register_gauge!(
        "utility_fd_current_open",
        "Current number of open file descriptors for the process"
    )
    .unwrap();
    pub static ref FD_SOFT_LIMIT: Gauge = register_gauge!(
        "utility_fd_soft_limit",
        "Soft file-descriptor limit (ratio of RLIMIT_NOFILE) for proactive reclamation"
    )
    .unwrap();
    pub static ref FD_HARD_LIMIT: Gauge = register_gauge!(
        "utility_fd_hard_limit",
        "Hard file-descriptor limit (ratio of RLIMIT_NOFILE) triggering emergency reclamation"
    )
    .unwrap();
    pub static ref FD_EVICTION_COUNT: Counter = register_counter!(
        "utility_fd_eviction_count_total",
        "Total connections evicted to reclaim file descriptors"
    )
    .unwrap();
    pub static ref FD_CONNECTION_RESETS: Counter = register_counter!(
        "utility_fd_connection_resets_total",
        "Total stale meter connections reset (TCP RST) on reconnect"
    )
    .unwrap();
    pub static ref TCP_ACTIVE_CONNECTIONS: Gauge = register_gauge!(
        "utility_tcp_active_connections",
        "Number of meter TCP connections currently tracked by the connection manager"
    )
    .unwrap();
}

pub fn set_fd_current_open(count: f64) {
    FD_CURRENT_OPEN.set(count);
}

pub fn set_fd_soft_limit(limit: f64) {
    FD_SOFT_LIMIT.set(limit);
}

pub fn set_fd_hard_limit(limit: f64) {
    FD_HARD_LIMIT.set(limit);
}

pub fn inc_fd_eviction_count(by: u64) {
    FD_EVICTION_COUNT.inc_by(by as f64);
}

pub fn inc_fd_connection_resets() {
    FD_CONNECTION_RESETS.inc();
}

pub fn set_tcp_active_connections(count: f64) {
    TCP_ACTIVE_CONNECTIONS.set(count);
}
