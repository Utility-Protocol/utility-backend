use backoff::{future::retry, ExponentialBackoff};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info, warn};

use super::metrics::record_pagerduty_api_request;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagerDutyPayload {
    pub summary: String,
    pub source: String,
    pub severity: String, // critical, error, warning, info
    pub component: Option<String>,
    pub group: Option<String>,
    pub class: Option<String>,
    pub custom_details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagerDutyEvent {
    pub routing_key: String,
    pub event_action: String, // trigger, acknowledge, resolve
    pub dedup_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<PagerDutyPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagerDutyResponse {
    pub status: String,
    pub message: String,
    pub dedup_key: String,
}

#[derive(Debug, Clone)]
pub struct PagerDutyClient {
    client: Client,
    routing_key: Option<String>,
    endpoint: String,
}

impl PagerDutyClient {
    pub fn new(routing_key: Option<String>, endpoint: Option<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();

        let endpoint =
            endpoint.unwrap_or_else(|| "https://events.pagerduty.com/v2/enqueue".to_string());

        Self {
            client,
            routing_key,
            endpoint,
        }
    }

    /// Creates a PagerDutyClient from environment variables.
    pub fn from_env() -> Self {
        let routing_key = std::env::var("PAGERDUTY_ROUTING_KEY")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let endpoint = std::env::var("PAGERDUTY_ENDPOINT")
            .ok()
            .filter(|s| !s.trim().is_empty());
        Self::new(routing_key, endpoint)
    }

    /// Asynchronously enqueues an event to PagerDuty with retry logic.
    pub async fn enqueue_event(
        &self,
        event_action: &str,
        dedup_key: &str,
        payload: Option<PagerDutyPayload>,
    ) -> Result<Option<PagerDutyResponse>, String> {
        let routing_key = match &self.routing_key {
            Some(key) => key.clone(),
            None => {
                info!(
                    action = %event_action,
                    dedup_key = %dedup_key,
                    "PagerDuty routing key not configured. Running in Mock/Sandbox Mode."
                );
                record_pagerduty_api_request("mock_success", event_action);
                return Ok(Some(PagerDutyResponse {
                    status: "success".to_string(),
                    message: "Mock success (sandbox mode)".to_string(),
                    dedup_key: dedup_key.to_string(),
                }));
            }
        };

        let event = PagerDutyEvent {
            routing_key,
            event_action: event_action.to_string(),
            dedup_key: dedup_key.to_string(),
            payload,
        };

        let backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_secs(15)),
            max_interval: Duration::from_secs(3),
            ..Default::default()
        };

        let endpoint = self.endpoint.clone();
        let client = self.client.clone();

        let op = || async {
            let res = client
                .post(&endpoint)
                .json(&event)
                .send()
                .await
                .map_err(|e| {
                    warn!("Network error sending alert to PagerDuty: {:?}", e);
                    backoff::Error::transient(anyhow::anyhow!(e))
                })?;

            let status = res.status();
            if status.is_success() {
                let pd_res = res.json::<PagerDutyResponse>().await.map_err(|e| {
                    error!("Failed to parse PagerDuty API response JSON: {:?}", e);
                    backoff::Error::permanent(anyhow::anyhow!(e))
                })?;
                Ok(pd_res)
            } else if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
                warn!(
                    "Transient error from PagerDuty (Status {}). Retrying...",
                    status
                );
                Err(backoff::Error::transient(anyhow::anyhow!(
                    "status: {}",
                    status
                )))
            } else {
                let err_text = res.text().await.unwrap_or_default();
                error!(
                    "Permanent error from PagerDuty API (Status {}): {}",
                    status, err_text
                );
                Err(backoff::Error::permanent(anyhow::anyhow!(
                    "status: {}, body: {}",
                    status,
                    err_text
                )))
            }
        };

        match retry(backoff, op).await {
            Ok(res) => {
                info!(
                    action = %event_action,
                    dedup_key = %dedup_key,
                    "Alert successfully processed by PagerDuty."
                );
                record_pagerduty_api_request("success", event_action);
                Ok(Some(res))
            }
            Err(e) => {
                error!(
                    action = %event_action,
                    dedup_key = %dedup_key,
                    error = %e,
                    "Failed to deliver alert to PagerDuty after multiple retries."
                );
                record_pagerduty_api_request("failed", event_action);
                Err(e.to_string())
            }
        }
    }
}
