//! Custom allocators for the telemetry hot path.
//!
//! Houses the fixed-size lock-light [`arena`] allocator and its [`slab`]
//! backing. See [`arena`] for why it is not registered as `#[global_allocator]`.

pub mod arena;
pub mod slab;

pub use arena::{ArenaAllocator, ArenaConfig, ArenaMetrics, BLOCK_SIZES};
