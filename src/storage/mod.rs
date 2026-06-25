//! Storage backends.
//!
//! * [`durable_queue`] — disk-backed spillover queue for the cross-shard
//!   credit-flow protocol.
//! * [`timescaledb`] — workload-priority-partitioned connection pooling.

pub mod durable_queue;
pub mod reorg_log;
