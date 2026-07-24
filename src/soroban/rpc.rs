use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const SLOW_REQUEST_THRESHOLD: Duration = Duration::from_secs(2);
const P99_OPEN_THRESHOLD_MS: u128 = 3_000;
const HALF_OPEN_PROBE_INTERVAL: Duration = Duration::from_secs(30);
const WINDOW_SIZE: usize = 100;
const MAX_QUEUED_REQUESTS: usize = 100;
const ERROR_RATE_OPEN_THRESHOLD: f64 = 0.20;

#[derive(Debug, Serialize, Deserialize)]
pub struct SorobanRpcResponse {
    pub id: String,
    pub jsonrpc: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CircuitState {
    Closed,
    HalfOpen,
    Open,
}

#[derive(Clone, Debug)]
struct RequestSample {
    duration: Duration,
    success: bool,
}

#[derive(Debug, Serialize)]
pub struct CircuitBreakerSnapshot {
    pub state: CircuitState,
    pub p50_latency_ms: u128,
    pub p95_latency_ms: u128,
    pub p99_latency_ms: u128,
    pub error_rate: f64,
    pub queued_request_count: usize,
    pub max_queued_requests: usize,
    pub window_size: usize,
}

pub struct CircuitBreaker {
    samples: VecDeque<RequestSample>,
    state: CircuitState,
    opened_at: Option<Instant>,
    last_probe_at: Option<Instant>,
    queued_requests: VecDeque<serde_json::Value>,
}

impl CircuitBreaker {
    pub fn new(_max_failures: u64) -> Self {
        Self {
            samples: VecDeque::with_capacity(WINDOW_SIZE),
            state: CircuitState::Closed,
            opened_at: None,
            last_probe_at: None,
            queued_requests: VecDeque::with_capacity(MAX_QUEUED_REQUESTS),
        }
    }

    pub fn snapshot(&self) -> CircuitBreakerSnapshot {
        let (p50, p95, p99) = self.latency_percentiles_ms();
        CircuitBreakerSnapshot {
            state: self.state,
            p50_latency_ms: p50,
            p95_latency_ms: p95,
            p99_latency_ms: p99,
            error_rate: self.error_rate(),
            queued_request_count: self.queued_requests.len(),
            max_queued_requests: MAX_QUEUED_REQUESTS,
            window_size: self.samples.len(),
        }
    }

    pub fn state(&self) -> CircuitState {
        self.state
    }

    pub fn force_open_for_test(&mut self) {
        self.open();
    }

    pub fn mark_opened_at_for_test(&mut self, opened_at: Instant) {
        self.opened_at = Some(opened_at);
    }

    pub fn admit_probe_for_test(&mut self) -> bool {
        matches!(
            self.admit_request(serde_json::json!({"probe": true})),
            Ok(Admission::Execute)
        )
    }

    pub fn record_success_for_test(&mut self, duration: Duration) {
        self.record_sample(duration, true);
    }

    #[tracing::instrument(skip(self), fields(otel.kind = "client", rpc.system = "soroban"))]
    pub async fn call_rpc(
        &mut self,
        rpc_url: &str,
        payload: serde_json::Value,
    ) -> Result<SorobanRpcResponse, &'static str> {
        match self.admit_request(payload.clone())? {
            Admission::Execute => {}
            Admission::Queued => return Err("circuit breaker open: rpc request queued"),
        }

        let client = reqwest::Client::builder()
            .timeout(RPC_TIMEOUT)
            .build()
            .map_err(|_| "failed to build rpc client")?;
        let started = Instant::now();
        let result = async {
            let resp = client
                .post(rpc_url)
                .json(&payload)
                .send()
                .await
                .map_err(|_| "rpc request failed")?;

            resp.json::<SorobanRpcResponse>()
                .await
                .map_err(|_| "failed to parse rpc response")
        }
        .await;

        let duration = started.elapsed();
        self.record_sample(duration, result.is_ok());

        if result.is_ok() && duration > SLOW_REQUEST_THRESHOLD {
            warn!(
                duration_ms = duration.as_millis(),
                threshold_ms = SLOW_REQUEST_THRESHOLD.as_millis(),
                "soroban rpc call succeeded slowly"
            );
        } else if result.is_ok() {
            info!(
                duration_ms = duration.as_millis(),
                "soroban rpc call succeeded"
            );
        } else {
            warn!(
                duration_ms = duration.as_millis(),
                "soroban rpc call failed"
            );
        }

        result
    }

    fn admit_request(&mut self, payload: serde_json::Value) -> Result<Admission, &'static str> {
        self.maybe_transition_to_half_open();

        match self.state {
            CircuitState::Closed => Ok(Admission::Execute),
            CircuitState::HalfOpen => {
                if self.probe_allowed() {
                    self.last_probe_at = Some(Instant::now());
                    Ok(Admission::Execute)
                } else {
                    self.queue_request(payload)
                }
            }
            CircuitState::Open => self.queue_request(payload),
        }
    }

    fn queue_request(&mut self, payload: serde_json::Value) -> Result<Admission, &'static str> {
        if self.queued_requests.len() >= MAX_QUEUED_REQUESTS {
            return Err("service unavailable: soroban rpc request queue full");
        }
        self.queued_requests.push_back(payload);
        Ok(Admission::Queued)
    }

    fn record_sample(&mut self, duration: Duration, success: bool) {
        if self.samples.len() == WINDOW_SIZE {
            self.samples.pop_front();
        }
        self.samples.push_back(RequestSample { duration, success });

        match self.state {
            CircuitState::HalfOpen if success => self.close(),
            CircuitState::HalfOpen => self.open(),
            CircuitState::Closed => {
                let (_, _, p99) = self.latency_percentiles_ms();
                if p99 > P99_OPEN_THRESHOLD_MS || self.error_rate() > ERROR_RATE_OPEN_THRESHOLD {
                    self.open();
                }
            }
            CircuitState::Open => {}
        }
    }

    fn maybe_transition_to_half_open(&mut self) {
        if self.state == CircuitState::Open
            && self
                .opened_at
                .map(|opened| opened.elapsed() >= HALF_OPEN_PROBE_INTERVAL)
                .unwrap_or(false)
        {
            self.state = CircuitState::HalfOpen;
        }
    }

    fn probe_allowed(&self) -> bool {
        self.last_probe_at
            .map(|probe| probe.elapsed() >= HALF_OPEN_PROBE_INTERVAL)
            .unwrap_or(true)
    }

    fn open(&mut self) {
        self.state = CircuitState::Open;
        self.opened_at = Some(Instant::now());
    }

    fn close(&mut self) {
        self.state = CircuitState::Closed;
        self.opened_at = None;
        self.last_probe_at = None;
        self.queued_requests.clear();
    }

    fn error_rate(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let errors = self.samples.iter().filter(|sample| !sample.success).count();
        errors as f64 / self.samples.len() as f64
    }

    fn latency_percentiles_ms(&self) -> (u128, u128, u128) {
        if self.samples.is_empty() {
            return (0, 0, 0);
        }
        let mut durations: Vec<u128> = self
            .samples
            .iter()
            .map(|sample| sample.duration.as_millis())
            .collect();
        durations.sort_unstable();
        (
            percentile(&durations, 50),
            percentile(&durations, 95),
            percentile(&durations, 99),
        )
    }
}

enum Admission {
    Execute,
    Queued,
}

fn percentile(sorted_values: &[u128], percentile: usize) -> u128 {
    let index = ((percentile as f64 / 100.0) * (sorted_values.len().saturating_sub(1) as f64))
        .ceil() as usize;
    sorted_values[index]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_on_sustained_degradation() {
        let mut breaker = CircuitBreaker::new(3);
        for _ in 0..10 {
            breaker.record_sample(Duration::from_millis(3_500), true);
        }
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(breaker.snapshot().p99_latency_ms > P99_OPEN_THRESHOLD_MS);
    }

    #[test]
    fn bounds_queue_while_open() {
        let mut breaker = CircuitBreaker::new(3);
        breaker.force_open_for_test();
        for _ in 0..MAX_QUEUED_REQUESTS {
            assert!(matches!(
                breaker.admit_request(serde_json::json!({"queued": true})),
                Ok(Admission::Queued)
            ));
        }
        assert!(breaker
            .admit_request(serde_json::json!({"queued": false}))
            .is_err());
    }

    #[test]
    fn half_open_probe_success_closes_circuit() {
        let mut breaker = CircuitBreaker::new(3);
        breaker.force_open_for_test();
        breaker.mark_opened_at_for_test(Instant::now() - HALF_OPEN_PROBE_INTERVAL);

        assert!(matches!(
            breaker.admit_request(serde_json::json!({"probe": true})),
            Ok(Admission::Execute)
        ));
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        breaker.record_sample(Duration::from_millis(50), true);
        assert_eq!(breaker.state(), CircuitState::Closed);
    }
}
