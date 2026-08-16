use std::sync::Arc;

use reqwest::Client as HttpClient;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{info, warn};
use url::Url;

use super::mtls::{build_client_tls_config, verify_peer_spiffe_id, MtlsConfig};
use super::{MeshIdentity, ServiceMeshMtlsConfig};

#[derive(Debug, Clone)]
pub struct ServiceEndpoint {
    pub name: String,
    pub url: Url,
    pub spiffe_id: String,
    pub health_check_path: String,
    pub timeout_secs: u64,
}

#[derive(Debug, Error)]
pub enum ServiceMeshClientError {
    #[error("HTTP request failed: {0}")]
    HttpError(String),
    #[error("mTLS configuration error: {0}")]
    MtlsError(String),
    #[error("Service not found: {0}")]
    ServiceNotFound(String),
    #[error("Health check failed for {service}: {reason}")]
    HealthCheckFailed { service: String, reason: String },
}

pub struct ServiceMeshClient {
    http_client: HttpClient,
    endpoints: Vec<ServiceEndpoint>,
    identity: MeshIdentity,
}

#[derive(Debug, Clone, Default)]
pub struct ServiceMeshDiscovery {
    services: Arc<RwLock<Vec<ServiceEndpoint>>>,
}

impl ServiceMeshDiscovery {
    pub fn new() -> Self {
        Self {
            services: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn register(&self, endpoint: ServiceEndpoint) {
        let mut services = self.services.write().await;
        if let Some(pos) = services.iter().position(|s| s.name == endpoint.name) {
            services[pos] = endpoint;
        } else {
            services.push(endpoint);
        }
    }

    pub async fn unregister(&self, name: &str) {
        let mut services = self.services.write().await;
        services.retain(|s| s.name != name);
    }

    pub async fn resolve(&self, name: &str) -> Option<ServiceEndpoint> {
        let services = self.services.read().await;
        services.iter().find(|s| s.name == name).cloned()
    }

    pub async fn list(&self) -> Vec<ServiceEndpoint> {
        let services = self.services.read().await;
        services.clone()
    }
}

impl ServiceMeshClient {
    pub async fn new(
        mtls_config: &ServiceMeshMtlsConfig,
        identity: &MeshIdentity,
        endpoints: Vec<ServiceEndpoint>,
    ) -> Result<Self, ServiceMeshClientError> {
        let tls_config = MtlsConfig {
            enabled: mtls_config.enabled,
            cert_path: mtls_config.cert_path.clone(),
            key_path: mtls_config.key_path.clone(),
            ca_cert_path: mtls_config.ca_cert_path.clone(),
            allowed_spiffe_ids: Vec::new(),
            require_client_cert: true,
        };

        let http_client = if mtls_config.enabled {
            let client_config = build_client_tls_config(&tls_config, "mesh.local")
                .map_err(|e| ServiceMeshClientError::MtlsError(e.to_string()))?;
            match client_config {
                Some(cfg) => {
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert(
                        reqwest::header::USER_AGENT,
                        reqwest::header::HeaderValue::from_str(&format!(
                            "utility-mesh-client/{}",
                            mtls_config.service_name
                        ))
                        .unwrap_or_default(),
                    );
                    HttpClient::builder()
                        .use_preconfigured_tls(cfg)
                        .default_headers(headers)
                        .build()
                        .map_err(|e| ServiceMeshClientError::HttpError(e.to_string()))?
                }
                None => HttpClient::new(),
            }
        } else {
            HttpClient::new()
        };

        Ok(Self {
            http_client,
            endpoints,
            identity: identity.clone(),
        })
    }

    pub async fn send_request(
        &self,
        service_name: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<String>,
    ) -> Result<reqwest::Response, ServiceMeshClientError> {
        let endpoint = self
            .endpoints
            .iter()
            .find(|e| e.name == service_name)
            .ok_or_else(|| ServiceMeshClientError::ServiceNotFound(service_name.to_string()))?;

        let url = endpoint
            .url
            .join(path.trim_start_matches('/'))
            .map_err(|e| ServiceMeshClientError::HttpError(e.to_string()))?;

        let mut req = self.http_client.request(method.clone(), url);

        if let Some(b) = body {
            req = req.header("content-type", "application/json").body(b);
        }

        req.header("x-service-name", &self.identity.service_account)
            .header("x-trust-domain", &self.identity.trust_domain)
            .timeout(std::time::Duration::from_secs(endpoint.timeout_secs))
            .send()
            .await
            .map_err(|e| ServiceMeshClientError::HttpError(e.to_string()))
    }

    pub async fn check_health(&self, service_name: &str) -> Result<bool, ServiceMeshClientError> {
        let endpoint = self
            .endpoints
            .iter()
            .find(|e| e.name == service_name)
            .ok_or_else(|| ServiceMeshClientError::ServiceNotFound(service_name.to_string()))?;

        let url = endpoint
            .url
            .join(endpoint.health_check_path.trim_start_matches('/'))
            .map_err(|e| ServiceMeshClientError::HttpError(e.to_string()))?;

        match self
            .http_client
            .get(url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => Ok(true),
            Ok(resp) => Err(ServiceMeshClientError::HealthCheckFailed {
                service: service_name.to_string(),
                reason: format!("HTTP {}", resp.status()),
            }),
            Err(e) => Err(ServiceMeshClientError::HealthCheckFailed {
                service: service_name.to_string(),
                reason: e.to_string(),
            }),
        }
    }

    pub async fn broadcast_request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<String>,
    ) -> Vec<(String, Result<reqwest::Response, ServiceMeshClientError>)> {
        let mut results = Vec::new();
        for endpoint in &self.endpoints {
            let result = self
                .send_request(&endpoint.name, method.clone(), path, body.clone())
                .await;
            results.push((endpoint.name.clone(), result));
        }
        results
    }
}
