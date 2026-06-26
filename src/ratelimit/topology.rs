//! Grid topology loader for the HTB scheduler (issue #48).
//!
//! The canonical schema is [`topology.proto`](./topology.proto). To keep the
//! build free of a `protoc` toolchain, topologies are described with the Rust
//! [`TopologyDef`] (mirroring the proto field-for-field) and validated before a
//! tree is built: unique ids, existing parents, a single root, no cycles, and
//! the HTB invariant that the sum of a parent's children's rates never exceeds
//! the parent's `ceil`.

use std::collections::{HashMap, HashSet};

use super::htb::{HtbTree, NodeConfig};

/// One node in a topology definition (mirrors the proto `Node`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeDef {
    pub id: u32,
    pub parent_id: Option<u32>,
    pub rate_tokens_per_s: u64,
    pub burst_tokens: u64,
    pub ceil_tokens_per_s: u64,
}

/// A full topology definition.
#[derive(Clone, Debug, Default)]
pub struct TopologyDef {
    pub nodes: Vec<NodeDef>,
}

/// Reasons a topology is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyError {
    Empty,
    DuplicateId(u32),
    MissingParent { node: u32, parent: u32 },
    MultipleRoots,
    NoRoot,
    Cycle(u32),
    ChildRatesExceedParentCeil { parent: u32, sum: u64, ceil: u64 },
}

impl std::fmt::Display for TopologyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopologyError::Empty => write!(f, "topology is empty"),
            TopologyError::DuplicateId(id) => write!(f, "duplicate node id {id}"),
            TopologyError::MissingParent { node, parent } => {
                write!(f, "node {node} references missing parent {parent}")
            }
            TopologyError::MultipleRoots => write!(f, "topology has more than one root"),
            TopologyError::NoRoot => write!(f, "topology has no root"),
            TopologyError::Cycle(id) => write!(f, "cycle detected at node {id}"),
            TopologyError::ChildRatesExceedParentCeil { parent, sum, ceil } => write!(
                f,
                "children of {parent} sum to rate {sum} exceeding ceil {ceil}"
            ),
        }
    }
}

impl std::error::Error for TopologyError {}

impl TopologyDef {
    /// Validate the definition without building a tree.
    pub fn validate(&self) -> Result<(), TopologyError> {
        if self.nodes.is_empty() {
            return Err(TopologyError::Empty);
        }

        let mut ids = HashSet::with_capacity(self.nodes.len());
        for node in &self.nodes {
            if !ids.insert(node.id) {
                return Err(TopologyError::DuplicateId(node.id));
            }
        }

        let mut root = None;
        for node in &self.nodes {
            match node.parent_id {
                None => {
                    if root.is_some() {
                        return Err(TopologyError::MultipleRoots);
                    }
                    root = Some(node.id);
                }
                Some(parent) if !ids.contains(&parent) => {
                    return Err(TopologyError::MissingParent {
                        node: node.id,
                        parent,
                    });
                }
                Some(_) => {}
            }
        }
        if root.is_none() {
            return Err(TopologyError::NoRoot);
        }

        // No cycles: every parent chain must reach the root within N steps.
        let parent_of: HashMap<u32, Option<u32>> =
            self.nodes.iter().map(|n| (n.id, n.parent_id)).collect();
        for node in &self.nodes {
            let mut cur = node.id;
            let mut steps = 0usize;
            while let Some(Some(parent)) = parent_of.get(&cur).copied() {
                cur = parent;
                steps += 1;
                if steps > self.nodes.len() {
                    return Err(TopologyError::Cycle(node.id));
                }
            }
        }

        // Sum of children rates must not exceed each parent's ceil.
        let mut child_rate_sum: HashMap<u32, u64> = HashMap::new();
        for node in &self.nodes {
            if let Some(parent) = node.parent_id {
                let entry = child_rate_sum.entry(parent).or_insert(0);
                *entry = entry.saturating_add(node.rate_tokens_per_s);
            }
        }
        for node in &self.nodes {
            if let Some(&sum) = child_rate_sum.get(&node.id) {
                if sum > node.ceil_tokens_per_s {
                    return Err(TopologyError::ChildRatesExceedParentCeil {
                        parent: node.id,
                        sum,
                        ceil: node.ceil_tokens_per_s,
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate and build an [`HtbTree`]. Nodes are inserted parents-first so
    /// child links resolve.
    pub fn build(&self, interest_apr: f64, start_ns: i64) -> Result<HtbTree, TopologyError> {
        self.validate()?;
        let tree = HtbTree::new(interest_apr);

        // Insert in depth order (root first) so a parent always exists when its
        // children are linked.
        let depth = self.depths();
        let mut ordered: Vec<&NodeDef> = self.nodes.iter().collect();
        ordered.sort_by_key(|n| depth.get(&n.id).copied().unwrap_or(0));

        for node in ordered {
            tree.add_node(
                node.id,
                NodeConfig {
                    rate: node.rate_tokens_per_s,
                    burst: node.burst_tokens,
                    ceil: node.ceil_tokens_per_s,
                    parent: node.parent_id,
                },
                start_ns,
            );
        }
        Ok(tree)
    }

    /// Depth of each node from the root (root = 0). Assumes a validated DAG.
    fn depths(&self) -> HashMap<u32, usize> {
        let parent_of: HashMap<u32, Option<u32>> =
            self.nodes.iter().map(|n| (n.id, n.parent_id)).collect();
        let mut depths = HashMap::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let mut d = 0usize;
            let mut cur = node.id;
            while let Some(Some(parent)) = parent_of.get(&cur).copied() {
                d += 1;
                cur = parent;
                if d > self.nodes.len() {
                    break;
                }
            }
            depths.insert(node.id, d);
        }
        depths
    }
}
