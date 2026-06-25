use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransportError {
    #[error("frame payload length {length} exceeds maximum {max}")]
    FrameTooLarge { length: usize, max: usize },
    #[error("invalid frame header: {reason}")]
    InvalidHeader { reason: &'static str },
    #[error("reassembly buffer length {current} exceeds maximum {max}")]
    BufferExceeded { current: usize, max: usize },
    #[error("connection idle timeout while waiting for a complete frame")]
    IdleTimeout,
    #[error("tcp io error: {message}")]
    Io { message: String },
}

impl From<std::io::Error> for TransportError {
    fn from(error: std::io::Error) -> Self {
        Self::Io {
            message: error.to_string(),
        }
    }
}
