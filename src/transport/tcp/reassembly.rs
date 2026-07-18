use super::{config::ReassemblyConfig, errors::TransportError};
use crate::api::metrics;
use bytes::{Buf, BytesMut};
use std::sync::Arc;

const FRAME_HEADER_LEN: usize = 4;

#[derive(Debug, Clone)]
pub struct FrameReassembler {
    buf: BytesMut,
    config: Arc<ReassemblyConfig>,
}

impl FrameReassembler {
    pub fn new(config: Arc<ReassemblyConfig>) -> Self {
        Self {
            buf: BytesMut::with_capacity(FRAME_HEADER_LEN),
            config,
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(Arc::new(ReassemblyConfig::default()))
    }

    pub fn push_data(&mut self, data: &[u8]) -> Result<Option<BytesMut>, TransportError> {
        self.buf.extend_from_slice(data);
        if self.buf.len() > self.config.max_buffer_per_conn {
            let err = TransportError::BufferExceeded {
                current: self.buf.len(),
                max: self.config.max_buffer_per_conn,
            };
            self.clear();
            metrics::record_tcp_buffer_exceeded_reset();
            return Err(err);
        }

        if !self.buf.is_empty() {
            metrics::record_tcp_partial_frame_buffered();
        }

        self.try_parse_frame()
    }

    pub fn try_parse_frame(&mut self) -> Result<Option<BytesMut>, TransportError> {
        if self.buf.len() < FRAME_HEADER_LEN {
            return Ok(None);
        }

        let length =
            u32::from_le_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
        if length == 0 {
            self.clear();
            return Err(TransportError::InvalidHeader {
                reason: "zero-length payload",
            });
        }

        if length > self.config.max_frame_payload {
            let err = TransportError::FrameTooLarge {
                length,
                max: self.config.max_frame_payload,
            };
            self.clear();
            metrics::record_tcp_frame_too_large_error();
            return Err(err);
        }

        let frame_len = FRAME_HEADER_LEN + length;
        if self.buf.len() < frame_len {
            return Ok(None);
        }

        let mut frame = self.buf.split_to(frame_len);
        frame.advance(FRAME_HEADER_LEN);
        metrics::record_tcp_complete_frame_delivered();
        Ok(Some(frame))
    }

    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }
}
