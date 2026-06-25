//! Network transport layer.
//!
//! Currently houses the adaptive TCP connection lifecycle manager (see
//! [`tcp`]) and its startup wiring (see [`lib`]).

pub mod lib;
pub mod tcp;
