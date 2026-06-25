use lazy_static::lazy_static;
use prometheus::{register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram};

lazy_static! {
    pub static ref WATERMARK_DIVERGENCE_COUNT: Counter = register_counter!(
        "utility_watermark_divergence_count",
        "Number of watermark merges that detected offset divergence requiring reconciliation"
    )
    .unwrap();
    pub static ref RECONCILIATION_DURATION_MS: Histogram = register_histogram!(
        "utility_reconciliation_duration_ms",
        "Duration of offset reconciliation scans in milliseconds"
    )
    .unwrap();
    pub static ref PARTITION_SECONDS_TOTAL: Gauge = register_gauge!(
        "utility_partition_seconds_total",
        "Total seconds collectors have spent in suspected partition state"
    )
    .unwrap();
}

pub fn record_watermark_divergence() {
    WATERMARK_DIVERGENCE_COUNT.inc();
}

pub fn record_reconciliation_duration_ms(duration_ms: f64) {
    RECONCILIATION_DURATION_MS.observe(duration_ms);
}

pub fn set_partition_seconds_total(seconds: f64) {
    PARTITION_SECONDS_TOTAL.set(seconds);
}
