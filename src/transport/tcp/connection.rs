use super::{FrameReassembler, ReassemblyConfig, TransportError};
use bytes::BytesMut;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::mpsc,
    time::{timeout, Instant},
};
use tracing::{info, warn};

const READ_CHUNK_SIZE: usize = 8192;

pub async fn handle_connection(
    mut stream: TcpStream,
    remote_addr: SocketAddr,
    config: Arc<ReassemblyConfig>,
    frame_tx: mpsc::Sender<BytesMut>,
) -> Result<(), TransportError> {
    let mut reassembler = FrameReassembler::new(config.clone());
    let mut read_buf = vec![0_u8; READ_CHUNK_SIZE];
    let mut last_byte_at = Instant::now();

    loop {
        let read = match timeout(config.idle_timeout, stream.read(&mut read_buf)).await {
            Ok(read) => read?,
            Err(_) => {
                let buffered = reassembler.buffered_len();
                reassembler.clear();
                warn!(%remote_addr, buffered, since_last_byte_ms = last_byte_at.elapsed().as_millis(), "tcp reassembly idle timeout");
                return Err(TransportError::IdleTimeout);
            }
        };

        if read == 0 {
            info!(%remote_addr, "tcp meter connection closed");
            return Ok(());
        }

        last_byte_at = Instant::now();
        if let Some(frame) = reassembler.push_data(&read_buf[..read])? {
            forward_frame(&frame_tx, frame).await?;
        }

        while let Some(frame) = reassembler.try_parse_frame()? {
            forward_frame(&frame_tx, frame).await?;
        }
    }
}

async fn forward_frame(
    frame_tx: &mpsc::Sender<BytesMut>,
    frame: BytesMut,
) -> Result<(), TransportError> {
    frame_tx
        .send(frame)
        .await
        .map_err(|error| TransportError::Io {
            message: format!("parser frame channel closed: {error}"),
        })
}
