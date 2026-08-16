use std::sync::Arc;
use std::{fs, path::Path};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use thiserror::Error;
use tokio_rustls::TlsAcceptor;
use tracing::info;

#[derive(Debug, Error)]
pub enum MtlsError {
    #[error("Failed to read certificate file: {0}")]
    CertReadError(String),
    #[error("Failed to read private key file: {0}")]
    KeyReadError(String),
    #[error("Failed to read CA certificate file: {0}")]
    CaReadError(String),
    #[error("Failed to parse certificate: {0}")]
    CertParseError(String),
    #[error("Failed to parse private key: {0}")]
    KeyParseError(String),
    #[error("No certificates found in file: {0}")]
    EmptyCertFile(String),
    #[error("Private key not found in file: {0}")]
    KeyNotFound(String),
    #[error("Failed to build TLS server config: {0}")]
    ServerConfigError(String),
    #[error("Failed to build TLS client config: {0}")]
    ClientConfigError(String),
    #[error("SPIFFE ID mismatch: expected {expected}, got {actual}")]
    SpiffeIdMismatch { expected: String, actual: String },
    #[error("Failed to extract SPIFFE ID from certificate")]
    SpiffeExtractionFailed,
    #[error("mTLS not enabled: {0}")]
    NotEnabled(String),
    #[error("Must be exactly one entity cert but found {0}")]
    EntityCertCount(usize),
}

pub struct MtlsConfig {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: String,
    pub allowed_spiffe_ids: Vec<String>,
    pub require_client_cert: bool,
}

impl Default for MtlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: String::new(),
            key_path: String::new(),
            ca_cert_path: String::new(),
            allowed_spiffe_ids: Vec::new(),
            require_client_cert: true,
        }
    }
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, MtlsError> {
    let data = fs::read(path).map_err(|e| MtlsError::CertReadError(format!("{}: {}", path, e)))?;
    let certs = rustls_pemfile::certs(&mut data.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| MtlsError::CertParseError(format!("{}: {}", path, e)))?;
    if certs.is_empty() {
        return Err(MtlsError::EmptyCertFile(path.to_string()));
    }
    Ok(certs)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, MtlsError> {
    let data = fs::read(path).map_err(|e| MtlsError::KeyReadError(format!("{}: {}", path, e)))?;
    rustls_pemfile::private_key(&mut data.as_slice())
        .map_err(|e| MtlsError::KeyParseError(format!("{}: {}", path, e)))?
        .ok_or_else(|| MtlsError::KeyNotFound(path.to_string()))
}

fn load_ca_certs(path: &str) -> Result<RootCertStore, MtlsError> {
    let data = fs::read(path).map_err(|e| MtlsError::CaReadError(format!("{}: {}", path, e)))?;
    let mut root_store = RootCertStore::empty();
    let certs = rustls_pemfile::certs(&mut data.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| MtlsError::CertParseError(format!("{}: {}", path, e)))?;
    for cert in certs {
        root_store
            .add(cert)
            .map_err(|e| MtlsError::CertParseError(format!("Failed to add CA cert: {}", e)))?;
    }
    Ok(root_store)
}

pub fn extract_spiffe_id(cert: &CertificateDer<'_>) -> Option<String> {
    use x509_parser::prelude::*;
    let (_rem, parsed) = X509Certificate::from_der(cert.as_ref()).ok()?;
    for ext in parsed.extensions() {
        if let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() {
            for name in &san.general_names {
                if let GeneralName::URI(uri) = name {
                    if uri.starts_with("spiffe://") {
                        return Some(uri.to_string());
                    }
                }
            }
        }
    }
    None
}

pub fn build_server_tls_config(config: &MtlsConfig) -> Result<Option<TlsAcceptor>, MtlsError> {
    if !config.enabled {
        return Ok(None);
    }
    let certs = load_certs(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;
    let root_store = load_ca_certs(&config.ca_cert_path)?;

    let server_config = if config.require_client_cert {
        ServerConfig::builder()
            .with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                    .build()
                    .map_err(|e| MtlsError::ServerConfigError(e.to_string()))?,
            )
            .with_single_cert(certs, key)
            .map_err(|e| MtlsError::ServerConfigError(e.to_string()))?
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| MtlsError::ServerConfigError(e.to_string()))?
    };

    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    info!("mTLS server configured with SPIFFE trust domain");
    Ok(Some(acceptor))
}

pub fn build_client_tls_config(
    config: &MtlsConfig,
    server_name: &str,
) -> Result<Option<ClientConfig>, MtlsError> {
    if !config.enabled {
        return Ok(None);
    }
    let root_store = load_ca_certs(&config.ca_cert_path)?;

    let builder = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let client_config =
        if Path::new(&config.cert_path).exists() && Path::new(&config.key_path).exists() {
            let certs = load_certs(&config.cert_path)?;
            let key = load_private_key(&config.key_path)?;
            ClientConfig::builder()
                .with_root_certificates(load_ca_certs(&config.ca_cert_path)?)
                .with_client_auth_cert(certs, key)
                .map_err(|e| MtlsError::ClientConfigError(e.to_string()))?
        } else {
            builder
        };

    info!("mTLS client configured for server: {}", server_name);
    Ok(Some(client_config))
}

pub fn verify_peer_spiffe_id(
    cert: &CertificateDer<'_>,
    allowed_ids: &[String],
) -> Result<String, MtlsError> {
    let spiffe_id = extract_spiffe_id(cert).ok_or(MtlsError::SpiffeExtractionFailed)?;
    if !allowed_ids.is_empty() && !allowed_ids.contains(&spiffe_id) {
        return Err(MtlsError::SpiffeIdMismatch {
            expected: allowed_ids.join(", "),
            actual: spiffe_id,
        });
    }
    Ok(spiffe_id)
}
