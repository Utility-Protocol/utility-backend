use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
pub struct SorobanRpcResponse {
    pub id: String,
    pub jsonrpc: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

#[allow(dead_code)]
pub struct CircuitBreaker {
    failure_count: u64,
    max_failures: u64,
    is_open: bool,
}

impl CircuitBreaker {
    pub fn new(max_failures: u64) -> Self {
        Self {
            failure_count: 0,
            max_failures,
            is_open: false,
        }
    }

    pub async fn call_rpc(
        &mut self,
        rpc_url: &str,
        payload: serde_json::Value,
    ) -> Result<SorobanRpcResponse, &'static str> {
        if self.is_open {
            return Err("circuit breaker open: rpc calls suspended");
        }

        let client = reqwest::Client::new();
        let resp = client
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|_| {
                self.record_failure();
                "rpc request failed"
            })?;

        let body: SorobanRpcResponse = resp
            .json()
            .await
            .map_err(|_| {
                self.record_failure();
                "failed to parse rpc response"
            })?;

        self.failure_count = 0;
        self.is_open = false;
        info!("soroban rpc call succeeded");
        Ok(body)
    }

    fn record_failure(&mut self) {
        self.failure_count += 1;
        if self.failure_count >= self.max_failures {
            self.is_open = true;
        }
    }
}
