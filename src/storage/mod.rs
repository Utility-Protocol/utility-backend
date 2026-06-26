//! Storage backends.
//!
//! * [`durable_queue`] — disk-backed spillover queue for the cross-shard
//!   credit-flow protocol.
//! * [`reorg_log`] — settled-batch state log for Soroban reorg rollback/replay.
//! * [`timescaledb`] — workload-priority-partitioned connection pooling.

pub mod durable_queue;
pub mod reorg_log;
pub mod timescaledb;
