use crate::transport::tls::{SessionTicketConfig, SessionTicketStore};
use rustls::pki_types::CertificateDer;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tracing::info;

pub fn build_tls_acceptor_with_session_tickets(
    cert_path: &str,
    key_path: &str,
    ticket_config: &SessionTicketConfig,
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let cert_bytes = std::fs::read(cert_path)?;
    let key_bytes = std::fs::read(key_path)?;

    let certs: Vec<CertificateDer> =
        rustls_pemfile::certs(&mut cert_bytes.as_slice()).collect::<Result<Vec<_>, _>>()?;
    let key =
        rustls_pemfile::private_key(&mut key_bytes.as_slice())?.ok_or("no private key found")?;

    let ticket_store = Arc::new(SessionTicketStore::load_or_generate(ticket_config)?);
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    config.ticketer = ticket_store;

    info!("TLS acceptor configured with rotating session ticket keys");
    Ok(TlsAcceptor::from(Arc::new(config)))
}
