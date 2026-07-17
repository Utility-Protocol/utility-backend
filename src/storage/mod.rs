//! Storage backends.
//!
//! * [`checkpoint`] — durable shutdown checkpoint for crash-safe resume.
//! * [`durable_queue`] — disk-backed spillover queue for the cross-shard
//!   credit-flow protocol.
//! * [`reorg_log`] — settled-batch state log for Soroban reorg rollback/replay.
//! * [`migrations`] — versioned database migrations with checksum validation and rollback planning.
//! * [`timescaledb`] — workload-priority-partitioned connection pooling.

pub mod checkpoint;
pub mod durable_queue;
pub mod migrations;
pub mod reorg_log;
pub mod timescaledb;
