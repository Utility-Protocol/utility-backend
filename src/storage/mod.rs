//! Storage backends.
//!
//! * [`checkpoint`] — durable shutdown checkpoint for crash-safe resume.
//! * [`durable_queue`] — disk-backed spillover queue for the cross-shard
//!   credit-flow protocol.
//! * [`reorg_log`] — settled-batch state log for Soroban reorg rollback/replay.
//! * [`job_scheduler`] — lease-based distributed worker claiming.
//! * [`timescaledb`] — workload-priority-partitioned connection pooling.

pub mod backup_verification;
pub mod checkpoint;
pub mod durable_queue;
pub mod job_scheduler;
pub mod reorg_log;
pub mod replication;
pub mod timescaledb;
