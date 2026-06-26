//! Backpressured telemetry stream handler (issue #33).
//!
//! Replaces the previous `read_until('\n')` loop — which buffered the entire
//! binary stream looking for a delimiter that never comes — with a bounded,
//! length-prefixed frame loop over a [`BufferPool`]. Each frame is read into a
//! pooled buffer, handed to a callback, then recycled; an oversized length is
//! treated as a security event and ends the connection.

use tokio::io::AsyncRead;
use tracing::warn;

use super::buffer_pool::BufferPool;
use super::frame_parser::{read_frame, Frame, FrameError};

/// Outcome of handling a stream to completion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamStats {
    /// Frames successfully read and dispatched.
    pub frames: u64,
    /// Frames rejected for exceeding `MAX_FRAME_SIZE`.
    pub oversized: u64,
}

/// Read length-prefixed frames from `reader` until EOF, invoking `on_frame` for
/// each. Returns the per-stream stats on clean close.
///
/// An oversized frame is logged as a security event and ends the stream (the
/// framing is unrecoverable once a bogus length is read, so the connection is
/// dropped by the caller). Other I/O errors propagate.
pub async fn handle_stream<R, F>(
    reader: &mut R,
    pool: &BufferPool,
    mut on_frame: F,
) -> Result<StreamStats, FrameError>
where
    R: AsyncRead + Unpin,
    F: FnMut(&Frame),
{
    let mut stats = StreamStats::default();
    loop {
        match read_frame(reader, pool).await {
            Ok(frame) => {
                on_frame(&frame);
                stats.frames += 1;
                // `frame` drops here, returning its buffer to the pool.
            }
            Err(FrameError::Closed) => return Ok(stats),
            Err(FrameError::FrameTooLarge { length, max }) => {
                stats.oversized += 1;
                warn!(
                    length,
                    max, "SECURITY: oversized telemetry frame; dropping connection"
                );
                return Err(FrameError::FrameTooLarge { length, max });
            }
            Err(other) => return Err(other),
        }
    }
}
