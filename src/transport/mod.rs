pub mod tcp;

pub use tcp::{FrameReassembler, ReassemblyConfig, TransportError};
//! Network transport layer.
//!
//! Houses the adaptive TCP connection lifecycle manager (see [`tcp`]) and its
//! startup wiring (see [`lib`]), plus TLS session-ticket rotation (see [`tls`]).

pub mod lib;
pub mod tls;
