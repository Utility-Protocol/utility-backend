use super::{connection::handle_connection, ReassemblyConfig};
use bytes::BytesMut;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{error, info, warn};

pub async fn accept_loop(
    listener: TcpListener,
    config: Arc<ReassemblyConfig>,
    frame_tx: mpsc::Sender<BytesMut>,
) -> std::io::Result<()> {
    loop {
        let (stream, remote_addr) = listener.accept().await?;
        info!(%remote_addr, "accepted tcp meter connection");

        let config = config.clone();
        let frame_tx = frame_tx.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, remote_addr, config, frame_tx).await {
                match error {
                    super::TransportError::IdleTimeout => {
                        warn!(%remote_addr, %error, "reset tcp meter connection")
                    }
                    _ => error!(%remote_addr, %error, "tcp meter connection failed"),
                }
            }
        });
    }
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
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

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

    debug!(meter_id = %meter_id, %peer, "accepted meter connection");
    let handle = cm
        .register(meter_id.clone(), stream, Priority::Normal)
        .await;
    Ok((meter_id, handle))
}
