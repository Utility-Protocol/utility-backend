//! Adaptive TCP connection lifecycle management for long-held meter sockets.
//!
//! See issue #53. The submodules cooperate to keep file-descriptor usage safely
//! below the process `rlimit` even under reconnection storms:
//!
//! * [`config`] — tunable bounds and ratios.
//! * [`connection_manager`] — per-meter registry, eviction primitives.
//! * [`fd_monitor`] — samples `/proc/self/fd` and drives reclamation.
//! * [`rate_limiter`] — sliding-window accept throttling and surge detection.
//! * [`acceptor`] — `SO_REUSEPORT` listeners and the accept→register path.

pub mod acceptor;
pub mod config;
pub mod connection;
pub mod connection_manager;
pub mod errors;
pub mod fd_monitor;
pub mod rate_limiter;
pub mod reassembly;

pub use config::ReassemblyConfig;
pub use errors::TransportError;
pub use reassembly::FrameReassembler;
