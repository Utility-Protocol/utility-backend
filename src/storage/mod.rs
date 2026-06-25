//! Persistence backends for the settlement pipeline.
//!
//! Currently exposes the disk-backed spillover queue used by the cross-shard
//! credit-flow protocol (see [`durable_queue`]).

pub mod durable_queue;
pub mod reorg_log;
