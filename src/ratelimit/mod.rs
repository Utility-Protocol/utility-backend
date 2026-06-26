//! Hierarchical token-bucket rate limiting for grid capacity (issue #48).
//!
//! * [`htb`] — the [`HtbTree`](htb::HtbTree) bucket hierarchy, refill, and
//!   conformance/borrowing logic.
//! * [`topology`] — grid topology definition, validation, and tree construction.

pub mod htb;
pub mod topology;

pub use htb::{HtbNode, HtbTree, NodeConfig};
pub use topology::{NodeDef, TopologyDef, TopologyError};
