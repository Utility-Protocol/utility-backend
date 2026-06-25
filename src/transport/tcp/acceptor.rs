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
}
