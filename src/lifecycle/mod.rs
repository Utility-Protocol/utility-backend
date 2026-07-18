//! Process lifecycle: structured-concurrency shutdown protocol (issue #49).
//!
//! * [`task_group`] — hierarchical cancellation tokens and per-stage task groups.
//! * [`shutdown`] — ordered, deadline-bounded graceful shutdown with checkpointing.

pub mod shutdown;
pub mod task_group;
