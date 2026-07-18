use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_gauge_vec, register_histogram,
    register_histogram_vec, Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec,
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

    pub static ref REPLICATION_LAG_MS: GaugeVec = register_gauge_vec!(
        "utility_replication_lag_ms",
        "Current cross-region replication lag in milliseconds",
        &["region"]
    )
    .unwrap();
    pub static ref DR_FAILOVER_ATTEMPTS: CounterVec = register_counter_vec!(
        "utility_dr_failover_attempts_total",
        "Total disaster-recovery failover attempts by target region and outcome",
        &["target_region", "outcome"]
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

    pub static ref KAFKA_CONSUMER_GROUP_LAG: GaugeVec = register_gauge_vec!(
        "utility_kafka_consumer_group_lag",
        "Total lag per Kafka consumer group and topic",
        &["group", "topic"]
    )
    .unwrap();
    pub static ref KAFKA_CONSUMER_GROUP_PARTITION_LAG: GaugeVec = register_gauge_vec!(
        "utility_kafka_consumer_group_partition_lag",
        "Lag per Kafka consumer group topic partition",
        &["group", "topic", "partition"]
    )
    .unwrap();
    pub static ref KAFKA_CONSUMER_GROUP_DESIRED_REPLICAS: GaugeVec = register_gauge_vec!(
        "utility_kafka_consumer_group_desired_replicas",
        "Desired replicas computed by the Kafka consumer group autoscaler",
        &["group"]
    )
    .unwrap();
    pub static ref KAFKA_CONSUMER_GROUP_SCALING_DECISIONS: CounterVec = register_counter_vec!(
        "utility_kafka_consumer_group_scaling_decisions_total",
        "Kafka consumer group autoscaling decisions",
        &["group", "reason"]
    )
    .unwrap();
    pub static ref KAFKA_CONSUMER_GROUP_LAG_ALERTS: CounterVec = register_counter_vec!(
        "utility_kafka_consumer_group_lag_alerts_total",
        "Kafka consumer group lag alerts emitted by severity",
        &["group", "severity"]
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

    pub static ref SLO_REQUESTS_TOTAL: CounterVec = register_counter_vec!(
        "utility_slo_requests_total",
        "Total HTTP requests counted for SLO evaluation",
        &["route", "status_class"]
    )
    .unwrap();
    pub static ref SLO_REQUEST_LATENCY_SECONDS: HistogramVec = register_histogram_vec!(
        "utility_slo_request_latency_seconds",
        "HTTP request latency in seconds for SLO evaluation",
        &["route"]
    )
    .unwrap();
    pub static ref SLO_AVAILABILITY_BURN_RATE: GaugeVec = register_gauge_vec!(
        "utility_slo_availability_burn_rate",
        "Availability error-budget burn rate by SLO window",
        &["window"]
    )
    .unwrap();
    pub static ref SLO_LATENCY_BURN_RATE: GaugeVec = register_gauge_vec!(
        "utility_slo_latency_burn_rate",
        "Latency error-budget burn rate by SLO window",
        &["window"]
    )
    .unwrap();
    pub static ref SLO_ALERT_ACTIVE: Gauge = register_gauge!(
        "utility_slo_alert_active",
        "Whether any multi-window SLO burn-rate alert is active"
    )
    .unwrap();
    pub static ref TCP_BUFFER_EXCEEDED_RESETS: Counter = register_counter!(
        "tcp_buffer_exceeded_resets",
        "Number of TCP connections reset because their reassembly buffer exceeded the configured maximum"
    )
    .unwrap();
    pub static ref MESH_MTLS_HANDSHAKES: CounterVec = register_counter_vec!(
        "utility_mesh_mtls_handshakes_total",
        "Total service mesh mutual TLS handshakes by peer service and result",
        &["service", "result"]
    )
    .unwrap();
    pub static ref MESH_MTLS_HANDSHAKE_LATENCY_SECONDS: HistogramVec = register_histogram_vec!(
        "utility_mesh_mtls_handshake_latency_seconds",
        "Service mesh mutual TLS handshake latency in seconds",
        &["service"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1]
    )
    .unwrap();
}

lazy_static! {
    pub static ref CONFIG_RELOAD_SUCCESS_TOTAL: Counter = register_counter!(
        "utility_config_reload_success_total",
        "Total successful configuration loads and hot reloads"
    )
    .unwrap();
    pub static ref CONFIG_RELOAD_FAILURE_TOTAL: Counter = register_counter!(
        "utility_config_reload_failure_total",
        "Total failed configuration hot reload attempts"
    )
    .unwrap();
    pub static ref CONFIG_SCHEMA_VERSION: Gauge = register_gauge!(
        "utility_config_schema_version",
        "Currently active validated configuration schema version"
    )
    .unwrap();
}

pub fn record_config_reload_success() {
    CONFIG_RELOAD_SUCCESS_TOTAL.inc();
}

pub fn record_config_reload_failure() {
    CONFIG_RELOAD_FAILURE_TOTAL.inc();
}

pub fn set_config_schema_version(version: f64) {
    CONFIG_SCHEMA_VERSION.set(version);
}

pub fn set_replication_lag_ms(region: &str, lag_ms: f64) {
    REPLICATION_LAG_MS.with_label_values(&[region]).set(lag_ms);
}

pub fn record_dr_failover_attempt(target_region: &str, outcome: &str) {
    DR_FAILOVER_ATTEMPTS
        .with_label_values(&[target_region, outcome])
        .inc();
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

pub fn set_kafka_consumer_group_lag(group: &str, topic: &str, lag: f64) {
    KAFKA_CONSUMER_GROUP_LAG
        .with_label_values(&[group, topic])
        .set(lag);
}

pub fn set_kafka_consumer_group_partition_lag(group: &str, topic: &str, partition: i32, lag: f64) {
    KAFKA_CONSUMER_GROUP_PARTITION_LAG
        .with_label_values(&[group, topic, &partition.to_string()])
        .set(lag);
}

pub fn set_kafka_consumer_group_desired_replicas(group: &str, replicas: u32) {
    KAFKA_CONSUMER_GROUP_DESIRED_REPLICAS
        .with_label_values(&[group])
        .set(replicas as f64);
}

pub fn record_kafka_consumer_group_scaling_decision(group: &str, reason: &str) {
    KAFKA_CONSUMER_GROUP_SCALING_DECISIONS
        .with_label_values(&[group, reason])
        .inc();
}

pub fn record_kafka_consumer_group_lag_alert(group: &str, severity: &str) {
    KAFKA_CONSUMER_GROUP_LAG_ALERTS
        .with_label_values(&[group, severity])
        .inc();
}

pub fn record_compaction_attempt() {
    COMPACTION_ATTEMPTS.inc();
}

pub fn record_mesh_mtls_handshake(service: &str, result: &str) {
    MESH_MTLS_HANDSHAKES
        .with_label_values(&[service, result])
        .inc();
}

pub fn record_mesh_mtls_handshake_latency(service: &str, latency_seconds: f64) {
    MESH_MTLS_HANDSHAKE_LATENCY_SECONDS
        .with_label_values(&[service])
        .observe(latency_seconds);
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

lazy_static! {
    pub static ref ARENA_ALLOC_COUNT: Gauge = register_gauge!(
        "utility_arena_alloc_count",
        "Cumulative arena block allocations"
    )
    .unwrap();
    pub static ref ARENA_FREE_COUNT: Gauge =
        register_gauge!("utility_arena_free_count", "Cumulative arena block frees").unwrap();
    pub static ref ARENA_GLOBAL_ACQUIRE_COUNT: Gauge = register_gauge!(
        "utility_arena_global_acquire_count",
        "Cumulative bulk acquisitions from a size class's global slab"
    )
    .unwrap();
    pub static ref ARENA_PAGE_FAULT_COUNT: Gauge = register_gauge!(
        "utility_arena_page_fault_count",
        "Cumulative new slabs mapped (proxy for hot-path page faults)"
    )
    .unwrap();
}

pub fn set_arena_counters(alloc: f64, free: f64, global_acquire: f64, page_fault: f64) {
    ARENA_ALLOC_COUNT.set(alloc);
    ARENA_FREE_COUNT.set(free);
    ARENA_GLOBAL_ACQUIRE_COUNT.set(global_acquire);
    ARENA_PAGE_FAULT_COUNT.set(page_fault);
}

lazy_static! {
    pub static ref POOL_CONNECTIONS_ACTIVE: GaugeVec = register_gauge_vec!(
        "utility_pool_connections_active",
        "Active connections per priority class",
        &["class"]
    )
    .unwrap();
    pub static ref POOL_CONNECTIONS_IDLE: GaugeVec = register_gauge_vec!(
        "utility_pool_connections_idle",
        "Idle connection slots per priority class",
        &["class"]
    )
    .unwrap();
    pub static ref POOL_WAIT_TIME_MS: HistogramVec = register_histogram_vec!(
        "utility_pool_wait_time_ms",
        "Connection acquisition wait time per priority class, in milliseconds",
        &["class"]
    )
    .unwrap();
    pub static ref POOL_PRIORITY_INHERITANCE_COUNT: Counter = register_counter!(
        "utility_pool_priority_inheritance_count_total",
        "Total priority-inheritance events (lower-priority slot lent to a higher-priority task)"
    )
    .unwrap();
    pub static ref POOL_CLASS_STARVATION_EVENTS: CounterVec = register_counter_vec!(
        "utility_pool_class_starvation_events_total",
        "Total starvation events per priority class",
        &["class"]
    )
    .unwrap();
}

pub fn set_pool_active(class: &str, count: f64) {
    POOL_CONNECTIONS_ACTIVE
        .with_label_values(&[class])
        .set(count);
}

pub fn set_pool_idle(class: &str, count: f64) {
    POOL_CONNECTIONS_IDLE.with_label_values(&[class]).set(count);
}

pub fn observe_pool_wait_ms(class: &str, wait_ms: f64) {
    POOL_WAIT_TIME_MS
        .with_label_values(&[class])
        .observe(wait_ms);
}

pub fn inc_priority_inheritance() {
    POOL_PRIORITY_INHERITANCE_COUNT.inc();
}

pub fn inc_pool_starvation(class: &str) {
    POOL_CLASS_STARVATION_EVENTS
        .with_label_values(&[class])
        .inc();
}

lazy_static! {
    pub static ref DLQ_MESSAGES_COUNT: GaugeVec = register_gauge_vec!(
        "utility_dlq_messages_count",
        "Current number of failed messages in the Dead Letter Queue",
        &["queue_name", "status"]
    )
    .unwrap();
    pub static ref DLQ_RETRIES_TOTAL: CounterVec = register_counter_vec!(
        "utility_dlq_retries_total",
        "Total number of DLQ message retry attempts",
        &["queue_name", "result"]
    )
    .unwrap();
}

pub fn set_dlq_messages_count(queue_name: &str, status: &str, count: f64) {
    DLQ_MESSAGES_COUNT
        .with_label_values(&[queue_name, status])
        .set(count);
}

pub fn record_dlq_retry(queue_name: &str, result: &str) {
    DLQ_RETRIES_TOTAL
        .with_label_values(&[queue_name, result])
        .inc();
}

pub fn spawn_dlq_metrics_poller(pool: sqlx::PgPool, interval: std::time::Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;

            let rows = sqlx::query_as::<_, (String, String, i64)>(
                "SELECT queue_name, status, COUNT(*) FROM dead_letter_queue GROUP BY queue_name, status"
            )
            .fetch_all(&pool)
            .await;

            match rows {
                Ok(counts) => {
                    // Reset gauge to zero first if needed or just update seen ones.
                    // Since Prometheus gauges retain their values, we update seen combinations:
                    for (q_name, status, count) in counts {
                        set_dlq_messages_count(&q_name, &status, count as f64);
                    }
                }
                Err(e) => {
                    tracing::warn!("failed to poll DLQ metrics: {}", e);
                }
            }
        }
    });
}
