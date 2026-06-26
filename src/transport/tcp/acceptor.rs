//! TCP listener setup and the accept path that integrates the connection
//! manager and rate limiter.
//!
//! Listeners are created via `socket2` so `SO_REUSEADDR` / `SO_REUSEPORT` can be
//! set before `bind`, preventing `EADDRINUSE` during rapid restarts and
//! allowing kernel-level load balancing across worker listeners.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

use crate::identity::attestation::AttestationVerifier;
use crate::identity::cert_store::CertStore;

use super::connection_manager::{ConnectionManager, MeterId, Priority, TimedTcpStream};
use super::rate_limiter::ConnectionRateLimiter;

/// Bind a TCP listener with `SO_REUSEADDR` and `SO_REUSEPORT` set and the given
/// backlog.
pub fn bind_listener(addr: SocketAddr, backlog: i32) -> io::Result<TcpListener> {
    let domain = Domain::for_address(addr);
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    socket.set_reuse_address(true)?;
    // `SO_REUSEPORT` is Unix-only; skip it elsewhere rather than failing.
    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(backlog)?;

    TcpListener::from_std(socket.into())
}

/// Wire-format meter handshake: `[u16 BE meter_id_len][UTF-8 meter_id]`.
///
/// Mirrors the length-prefixed framing used by the gateway envelope parser so
/// the acceptor can identify the meter before registering the connection.
pub async fn read_meter_id(stream: &mut TcpStream) -> io::Result<MeterId> {
    let len = stream.read_u16().await? as usize;
    if len == 0 || len > 256 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "meter id length out of range",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    String::from_utf8(buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "meter id not UTF-8"))
}

/// Accept the next connection, applying rate limiting, identifying the meter and
/// registering it with the [`ConnectionManager`]. Any pre-existing connection
/// for the same meter is reset inside `register`.
///
/// Returns the meter ID and a [`TimedTcpStream`] handle for subsequent I/O.
pub async fn accept_and_register(
    listener: &TcpListener,
    cm: &Arc<ConnectionManager>,
    limiter: &Arc<ConnectionRateLimiter>,
    attestation_verifier: Option<&Arc<AttestationVerifier>>,
    cert_store: Option<&Arc<CertStore>>,
) -> io::Result<(MeterId, TimedTcpStream)> {
    // Smoothly throttle to the configured token-bucket rate during storms.
    limiter.acquire().await;

    let (mut stream, peer) = listener.accept().await?;
    stream.set_nodelay(true).ok();

    let meter_id = match read_meter_id(&mut stream).await {
        Ok(id) => id,
        Err(e) => {
            warn!(%peer, error = %e, "rejecting connection: bad meter handshake");
            // Reset the rejected connection via `SO_LINGER=0` (tokio's
            // `set_linger` is deprecated); frees the descriptor immediately.
            let _ = socket2::SockRef::from(&stream).set_linger(Some(std::time::Duration::ZERO));
            return Err(e);
        }
    };

    // --- Remote Attestation Handshake ---
    if let (Some(verifier), Some(store)) = (attestation_verifier, cert_store) {
        if verifier.config.attestation_enabled {
            debug!(%meter_id, %peer, "initiating remote attestation challenge");

            // 1. Generate and send 32-byte nonce
            let mut nonce = [0u8; 32];
            rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut nonce);
            stream.write_all(&nonce).await?;

            // 2. Receive quote length (u16 BE) and quote
            let quote_len = stream.read_u16().await? as usize;
            let mut quote = vec![0u8; quote_len];
            stream.read_exact(&mut quote).await?;

            // 3. Receive public key length (u16 BE) and public key (SEC1)
            let pk_len = stream.read_u16().await? as usize;
            let mut pk_bytes = vec![0u8; pk_len];
            stream.read_exact(&mut pk_bytes).await?;

            // 4. Receive enclave signature length (u16 BE) and signature
            let sig_len = stream.read_u16().await? as usize;
            let mut sig_bytes = vec![0u8; sig_len];
            stream.read_exact(&mut sig_bytes).await?;

            // 5. Verify quote
            match verifier.verify_quote(&quote, meter_id.clone(), &nonce).await {
                Ok(_) => {
                    // 6. Certify public key and store it
                    match verifier.certify_key(meter_id.clone(), &pk_bytes, &sig_bytes) {
                        Ok(cert) => {
                            if let Err(e) = store.store_certificate(&meter_id, &cert) {
                                warn!(%meter_id, error = %e, "failed to store meter certificate");
                                return Err(io::Error::new(io::ErrorKind::Other, "storage failure"));
                            }
                            // 7. Send certificate (our "attestation success" signal)
                            stream.write_u16(cert.len() as u16).await?;
                            stream.write_all(&cert).await?;
                        }
                        Err(e) => {
                            warn!(%meter_id, error = %e, "key certification failed");
                            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "certification failed"));
                        }
                    }
                }
                Err(e) => {
                    warn!(%meter_id, error = %e, "remote attestation failed");
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, "attestation failed"));
                }
            }
        }
    }

    debug!(meter_id = %meter_id, %peer, "accepted meter connection");
    let handle = cm
        .register(meter_id.clone(), stream, Priority::Normal)
        .await;
    Ok((meter_id, handle))
}
