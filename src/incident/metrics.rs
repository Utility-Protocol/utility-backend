use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};

lazy_static! {
    pub static ref INCIDENTS_TRIGGERED: CounterVec = register_counter_vec!(
        "utility_incidents_triggered_total",
        "Total number of incidents triggered",
        &["component", "severity"]
    )
    .unwrap();
    pub static ref INCIDENTS_RESOLVED: CounterVec = register_counter_vec!(
        "utility_incidents_resolved_total",
        "Total number of incidents resolved",
        &["component"]
    )
    .unwrap();
    pub static ref RUNBOOK_EXECUTION_LATENCY: HistogramVec = register_histogram_vec!(
        "utility_runbook_execution_latency_seconds",
        "Duration of automated runbook execution in seconds",
        &["runbook_name", "action_type"]
    )
    .unwrap();
    pub static ref PAGERDUTY_API_REQUESTS: CounterVec = register_counter_vec!(
        "utility_pagerduty_api_requests_total",
        "Total PagerDuty Events API V2 requests made",
        &["status", "event_type"]
    )
    .unwrap();
}

pub fn record_incident_triggered(component: &str, severity: &str) {
    INCIDENTS_TRIGGERED
        .with_label_values(&[component, severity])
        .inc();
}

pub fn record_incident_resolved(component: &str) {
    INCIDENTS_RESOLVED.with_label_values(&[component]).inc();
}

pub fn record_runbook_latency(runbook_name: &str, action_type: &str, duration_seconds: f64) {
    RUNBOOK_EXECUTION_LATENCY
        .with_label_values(&[runbook_name, action_type])
        .observe(duration_seconds);
}

pub fn record_pagerduty_api_request(status: &str, event_type: &str) {
    PAGERDUTY_API_REQUESTS
        .with_label_values(&[status, event_type])
        .inc();
}
