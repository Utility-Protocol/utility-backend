//! Network transport layer.
//!
//! Houses the adaptive TCP connection lifecycle manager (see [`tcp`]) and its
//! startup wiring (see [`lib`]), plus TLS session-ticket rotation (see [`tls`]).

pub mod lib;
pub mod tcp;
pub mod tls;

pub use tcp::{FrameReassembler, ReassemblyConfig, TransportError};
