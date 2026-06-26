//! Length-prefixed binary frame reader with zero-copy CBOR decoding (issue #33).
//!
//! Wire format: `[u32 BE length][CBOR payload of `length` bytes]`. We read the
//! 4-byte prefix with `read_exact`, validate `length <= MAX_FRAME_SIZE`, then
//! read exactly `length` bytes into a pooled buffer — bounding per-frame memory
//! and eliminating the delimiter-scanning blow-up of `read_until`. CBOR is
//! decoded straight from the pooled slice (no intermediate `Vec` per frame).

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::buffer_pool::{BufferPool, PooledBuffer};

/// Bytes of the length prefix.
pub const LENGTH_PREFIX_LEN: usize = 4;

/// Errors from reading a frame.
#[derive(Debug)]
pub enum FrameError {
    /// The stream ended cleanly at a frame boundary.
    Closed,
    /// The advertised length exceeds the protocol maximum (possible attack).
    FrameTooLarge { length: usize, max: usize },
    /// An underlying I/O error.
    Io(std::io::Error),
    /// CBOR payload failed to decode.
    Decode(String),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Closed => write!(f, "stream closed at frame boundary"),
            FrameError::FrameTooLarge { length, max } => {
                write!(f, "frame length {length} exceeds maximum {max}")
            }
            FrameError::Io(e) => write!(f, "frame io error: {e}"),
            FrameError::Decode(e) => write!(f, "frame decode error: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

/// A decoded frame: a pooled buffer plus the valid payload length.
pub struct Frame {
    buffer: PooledBuffer,
    length: usize,
}

impl Frame {
    /// The frame payload (the first `length` bytes of the pooled buffer).
    pub fn payload(&self) -> &[u8] {
        &self.buffer.as_slice()[..self.length]
    }

    /// Payload length in bytes.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Decode the CBOR payload into `T` directly from the pooled slice.
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, FrameError> {
        ciborium::de::from_reader(self.payload()).map_err(|e| FrameError::Decode(e.to_string()))
    }
}

/// Read exactly one frame from `reader`, using a pooled buffer.
///
/// Validates the length against `pool.buf_size()` before reading the payload, so
/// a malicious/garbled prefix cannot cause an oversized read.
pub async fn read_frame<R>(reader: &mut R, pool: &BufferPool) -> Result<Frame, FrameError>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; LENGTH_PREFIX_LEN];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(FrameError::Closed),
        Err(e) => return Err(FrameError::Io(e)),
    }

    let length = u32::from_be_bytes(len_buf) as usize;
    let max = pool.buf_size();
    if length > max {
        return Err(FrameError::FrameTooLarge { length, max });
    }

    let mut buffer = pool.acquire().await;
    reader
        .read_exact(&mut buffer.as_mut_slice()[..length])
        .await
        .map_err(FrameError::Io)?;

    Ok(Frame { buffer, length })
}

/// Example telemetry payload schema (CBOR-encoded on the wire).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelemetryFrame {
    pub meter_id: String,
    pub timestamp: u64,
    pub value: f64,
}
