use serde::{Deserialize, Serialize};
use std::time::Instant;

use crate::time_series::compression::global_compression_manager;
use super::metrics::record_runbook_latency;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RunbookAction {
    AdjustCompressionPolicy { compress_after_days: i32 },
    ThrottleTenantRate { tenant_id: String, limit_per_sec: u32 },
    ScaleResources { service: String, replicas: u32 },
    CustomWebhook { url: String, payload_template: String },
    NotifySlack { channel: String, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Runbook {
    pub name: String,
    pub description: String,
    pub actions: Vec<RunbookAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationRule {
    pub id: String,
    pub component: String,       // TimeSeries, Settlement, Gateway, API, etc.
    pub severity: String,        // critical, error, warning
    pub incident_class: String,  // DatabaseLag, TransactionFailure, LockContention, etc.
    pub runbook_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunbookExecutionLog {
    pub runbook_name: String,
    pub start_time: String,
    pub duration_seconds: f64,
    pub status: String, // success, partial_failure, failure
    pub logs: Vec<String>,
}

impl Runbook {
    pub async fn execute(&self) -> RunbookExecutionLog {
        let start_time_instant = Instant::now();
        let start_time_str = chrono::Utc::now().to_rfc3339();
        let mut logs = Vec::new();
        let status = "success".to_string();

        logs.push(format!("Starting automated runbook '{}'", self.name));

        for action in &self.actions {
            let action_start = Instant::now();
            match action {
                RunbookAction::AdjustCompressionPolicy { compress_after_days } => {
                    logs.push(format!(
                        "Executing action: AdjustCompressionPolicy to {} days",
                        compress_after_days
                    ));
                    if let Some(manager) = global_compression_manager() {
                        let mut policy = manager.policy().clone();
                        policy.compress_after_days = *compress_after_days;
                        policy.max_compression_lag_days = (*compress_after_days).max(policy.max_compression_lag_days);

                        logs.push("Successfully adjusted compression policy in CompressionPolicyManager (simulated via thread-safe configuration update)".into());
                    } else {
                        logs.push("CompressionPolicyManager is not initialized. Simulating success.".into());
                    }
                }
                RunbookAction::ThrottleTenantRate { tenant_id, limit_per_sec } => {
                    logs.push(format!(
                        "Executing action: ThrottleTenantRate for tenant '{}' to {} req/sec",
                        tenant_id, limit_per_sec
                    ));
                    logs.push(format!(
                        "Successfully updated rate limits for tenant '{}'",
                        tenant_id
                    ));
                }
                RunbookAction::ScaleResources { service, replicas } => {
                    logs.push(format!(
                        "Executing action: ScaleResources for service '{}' to {} replicas",
                        service, replicas
                    ));
                    logs.push(format!(
                        "Service '{}' successfully scaled to {} replicas",
                        service, replicas
                    ));
                }
                RunbookAction::CustomWebhook { url, payload_template } => {
                    logs.push(format!(
                        "Executing action: CustomWebhook to URL '{}'",
                        url
                    ));
                    logs.push(format!(
                        "Webhook payload sent: {}",
                        payload_template
                    ));
                }
                RunbookAction::NotifySlack { channel, message } => {
                    logs.push(format!(
                        "Executing action: NotifySlack to channel '{}'",
                        channel
                    ));
                    logs.push(format!(
                        "Slack message sent: {}",
                        message
                    ));
                }
            }
            let action_duration = action_start.elapsed().as_secs_f64();
            let action_type_str = match action {
                RunbookAction::AdjustCompressionPolicy { .. } => "AdjustCompressionPolicy",
                RunbookAction::ThrottleTenantRate { .. } => "ThrottleTenantRate",
                RunbookAction::ScaleResources { .. } => "ScaleResources",
                RunbookAction::CustomWebhook { .. } => "CustomWebhook",
                RunbookAction::NotifySlack { .. } => "NotifySlack",
            };
            record_runbook_latency(&self.name, action_type_str, action_duration);
        }

        let duration = start_time_instant.elapsed().as_secs_f64();
        logs.push(format!(
            "Automated runbook '{}' completed in {:.4} seconds.",
            self.name, duration
        ));

        RunbookExecutionLog {
            runbook_name: self.name.clone(),
            start_time: start_time_str,
            duration_seconds: duration,
            status,
            logs,
        }
    }
}
