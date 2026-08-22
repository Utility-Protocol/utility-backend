use crate::api::AppState;
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::service_mesh::client::{ServiceMeshClient, ServiceMeshDiscovery};
use crate::service_mesh::{MeshIdentity, ServiceMeshMtlsConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Up,
    Down,
    Degraded,
}

#[derive(Debug, Clone, Serialize)]
pub struct DetailedHealthResponse {
    pub status: HealthStatus,
    pub timestamp: String,
    pub services: HashMap<String, ServiceHealth>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SummaryHealthResponse {
    pub status: HealthStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceHealth {
    pub status: HealthStatus,
    pub error: Option<String>,
}

pub struct HealthCircuitBreaker {
    failures: u32,
    state: CircuitState,
    last_failure: Option<Instant>,
}

enum CircuitState {
    Closed,
    Open,
}

impl HealthCircuitBreaker {
    pub fn new() -> Self {
        Self {
            failures: 0,
            state: CircuitState::Closed,
            last_failure: None,
        }
    }

    pub fn admit(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last) = self.last_failure {
                    if last.elapsed() > Duration::from_secs(30) {
                        self.state = CircuitState::Closed;
                        return true;
                    }
                }
                false
            }
        }
    }

    pub fn record_result(&mut self, success: bool) {
        if success {
            self.failures = 0;
            self.state = CircuitState::Closed;
        } else {
            self.failures += 1;
            if self.failures >= 3 {
                self.state = CircuitState::Open;
                self.last_failure = Some(Instant::now());
            }
        }
    }
}

pub struct HealthAggregator {
    cache: RwLock<Option<(Instant, DetailedHealthResponse)>>,
    circuit_breakers: RwLock<HashMap<String, HealthCircuitBreaker>>,
    discovery: Arc<ServiceMeshDiscovery>,
    mtls_config: ServiceMeshMtlsConfig,
    identity: MeshIdentity,
}

impl HealthAggregator {
    pub fn new(
        discovery: Arc<ServiceMeshDiscovery>,
        mtls_config: ServiceMeshMtlsConfig,
        identity: MeshIdentity,
    ) -> Self {
        Self {
            cache: RwLock::new(None),
            circuit_breakers: RwLock::new(HashMap::new()),
            discovery,
            mtls_config,
            identity,
        }
    }

    pub async fn get_health(&self) -> DetailedHealthResponse {
        {
            let cache = self.cache.read().await;
            if let Some((ts, response)) = &*cache {
                if ts.elapsed() < Duration::from_secs(10) {
                    return response.clone();
                }
            }
        }

        let endpoints = self.discovery.list().await;

        let client = match ServiceMeshClient::new(&self.mtls_config, &self.identity, endpoints.clone()).await {
            Ok(c) => Arc::new(c),
            Err(e) => {
                let mut services = HashMap::new();
                for ep in endpoints {
                    services.insert(
                        ep.name,
                        ServiceHealth {
                            status: HealthStatus::Down,
                            error: Some(format!("Client init error: {}", e)),
                        },
                    );
                }
                return DetailedHealthResponse {
                    status: HealthStatus::Down,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    services,
                };
            }
        };

        let mut futures = vec![];

        for endpoint in endpoints {
            let client = client.clone();
            let name = endpoint.name.clone();

            let mut admit = true;
            {
                let mut breakers = self.circuit_breakers.write().await;
                let breaker = breakers
                    .entry(name.clone())
                    .or_insert_with(HealthCircuitBreaker::new);
                admit = breaker.admit();
            }

            if !admit {
                futures.push(async move { (name, Err("Circuit breaker open".to_string())) });
                continue;
            }

            futures.push(async move {
                let res = client.check_health(&name).await;
                (name, res.map_err(|e| e.to_string()))
            });
        }

        let results = futures::future::join_all(futures).await;

        let mut services = HashMap::new();
        let mut all_up = true;
        let mut any_up = false;

        {
            let mut breakers = self.circuit_breakers.write().await;

            for (name, res) in results {
                let breaker = breakers
                    .entry(name.clone())
                    .or_insert_with(HealthCircuitBreaker::new);

                let health = match res {
                    Ok(_) => {
                        breaker.record_result(true);
                        any_up = true;
                        ServiceHealth {
                            status: HealthStatus::Up,
                            error: None,
                        }
                    }
                    Err(e) => {
                        breaker.record_result(false);
                        all_up = false;
                        ServiceHealth {
                            status: HealthStatus::Down,
                            error: Some(e),
                        }
                    }
                };

                let val = if matches!(health.status, HealthStatus::Up) {
                    1.0
                } else {
                    0.0
                };
                crate::api::metrics::set_health_status(&name, val);

                services.insert(name, health);
            }
        }

        let status = if services.is_empty() {
            HealthStatus::Up
        } else if all_up {
            HealthStatus::Up
        } else if any_up {
            HealthStatus::Degraded
        } else {
            HealthStatus::Down
        };

        let response = DetailedHealthResponse {
            status,
            timestamp: chrono::Utc::now().to_rfc3339(),
            services,
        };

        {
            let mut cache = self.cache.write().await;
            *cache = Some((Instant::now(), response.clone()));
        }

        response
    }
}

pub async fn summary_health(State(state): State<AppState>) -> Json<SummaryHealthResponse> {
    let detailed = state.health_aggregator.get_health().await;
    Json(SummaryHealthResponse {
        status: detailed.status,
    })
}

pub async fn detailed_health(State(state): State<AppState>) -> Json<DetailedHealthResponse> {
    let detailed = state.health_aggregator.get_health().await;
    Json(detailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn circuit_breaker_opens_and_half_opens() {
        let mut cb = HealthCircuitBreaker::new();
        assert!(cb.admit());
        cb.record_result(false);
        cb.record_result(false);
        assert!(cb.admit());
        cb.record_result(false); // 3rd failure
        assert!(!cb.admit()); // open

        cb.last_failure = Some(Instant::now() - Duration::from_secs(31));
        assert!(cb.admit()); // should allow probe
    }
}
