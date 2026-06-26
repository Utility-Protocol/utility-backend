//! Tests for the hierarchical token-bucket scheduler (issue #48).
//!
//! Covers topology validation, hierarchical debiting, burst/borrowing bounds,
//! rollback on rejection, refill/debt repayment, plus property-based checks that
//! no node is ever driven below its burst floor and that a rejected request is a
//! no-op.

use proptest::prelude::*;

use utility_backend::ratelimit::htb::HtbTree;
use utility_backend::ratelimit::topology::{NodeDef, TopologyDef, TopologyError};

/// root(1) ─ feeder(2) ─ meter(4), meter(5)
///        └ feeder(3)
fn test_topology() -> TopologyDef {
    TopologyDef {
        nodes: vec![
            NodeDef {
                id: 1,
                parent_id: None,
                rate_tokens_per_s: 1000,
                burst_tokens: 1000,
                ceil_tokens_per_s: 1000,
            },
            NodeDef {
                id: 2,
                parent_id: Some(1),
                rate_tokens_per_s: 600,
                burst_tokens: 600,
                ceil_tokens_per_s: 800,
            },
            NodeDef {
                id: 3,
                parent_id: Some(1),
                rate_tokens_per_s: 400,
                burst_tokens: 400,
                ceil_tokens_per_s: 800,
            },
            NodeDef {
                id: 4,
                parent_id: Some(2),
                rate_tokens_per_s: 200,
                burst_tokens: 200,
                ceil_tokens_per_s: 300,
            },
            NodeDef {
                id: 5,
                parent_id: Some(2),
                rate_tokens_per_s: 200,
                burst_tokens: 200,
                ceil_tokens_per_s: 300,
            },
        ],
    }
}

fn build() -> HtbTree {
    test_topology().build(0.10, 0).expect("valid topology")
}

#[test]
fn test_topology_validation_ok() {
    assert!(test_topology().validate().is_ok());
    let tree = build();
    assert_eq!(tree.len(), 5);
    assert_eq!(tree.root(), Some(1));
}

#[test]
fn test_topology_rejects_invalid() {
    // Duplicate id.
    let mut def = test_topology();
    def.nodes[1].id = 1;
    assert_eq!(def.validate(), Err(TopologyError::DuplicateId(1)));

    // Missing parent.
    let mut def = test_topology();
    def.nodes[3].parent_id = Some(99);
    assert_eq!(
        def.validate(),
        Err(TopologyError::MissingParent {
            node: 4,
            parent: 99
        })
    );

    // No root.
    let mut def = test_topology();
    def.nodes[0].parent_id = Some(2);
    assert!(matches!(
        def.validate(),
        Err(TopologyError::Cycle(_)) | Err(TopologyError::NoRoot)
    ));

    // Children rates exceed parent ceil: feeder 2 ceil below child sum.
    let mut def = test_topology();
    def.nodes[1].ceil_tokens_per_s = 100; // children 4+5 = 400 > 100
    assert!(matches!(
        def.validate(),
        Err(TopologyError::ChildRatesExceedParentCeil { parent: 2, .. })
    ));
}

#[test]
fn test_hierarchical_debit_and_borrow() {
    let tree = build();

    // Within capacity: debits leaf + ancestors.
    assert!(tree.conform_at(4, 100, 0));
    assert_eq!(tree.node(4).unwrap().tokens(), 100);
    assert_eq!(tree.node(2).unwrap().tokens(), 500);
    assert_eq!(tree.node(1).unwrap().tokens(), 900);

    // Borrow into burst: leaf goes negative but stays >= -burst.
    assert!(tree.conform_at(4, 200, 0));
    assert_eq!(tree.node(4).unwrap().tokens(), -100);
    assert_eq!(tree.node(4).unwrap().debt(), 100);
    assert_eq!(tree.node(2).unwrap().tokens(), 300);

    // Exceeds leaf burst floor (-200): rejected and rolled back.
    assert!(!tree.conform_at(4, 200, 0));
    assert_eq!(tree.node(4).unwrap().tokens(), -100, "leaf restored");
    assert_eq!(tree.node(2).unwrap().tokens(), 300, "parent untouched");
    assert_eq!(tree.node(1).unwrap().tokens(), 700, "root untouched");
}

#[test]
fn test_refill_repays_debt() {
    let tree = build();
    // Borrow the leaf down to its floor.
    assert!(tree.conform_at(4, 400, 0)); // 200 -> -200
    assert_eq!(tree.node(4).unwrap().tokens(), -200);
    assert_eq!(tree.node(4).unwrap().debt(), 200);

    // One second of refill at 200 tokens/s brings the leaf back to 0, debt clear.
    tree.refill_at(1_000_000_000);
    assert_eq!(tree.node(4).unwrap().tokens(), 0);
    assert_eq!(tree.node(4).unwrap().debt(), 0);

    // A further second refills toward burst (capped at 200).
    tree.refill_at(2_000_000_000);
    assert_eq!(tree.node(4).unwrap().tokens(), 200);
}

#[test]
fn test_unknown_meter_rejected() {
    let tree = build();
    assert!(!tree.conform_at(999, 1, 0));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// No request sequence ever drives any node below its burst floor.
    #[test]
    fn prop_never_below_burst_floor(
        reqs in prop::collection::vec((4u32..=5u32, 0u64..400u64), 0..40)
    ) {
        let tree = build();
        for (leaf, amount) in reqs {
            let _ = tree.conform_at(leaf, amount, 0);
            for id in [1u32, 2, 3, 4, 5] {
                let node = tree.node(id).unwrap();
                prop_assert!(
                    node.tokens() >= -(node.burst() as i64),
                    "node {id} tokens {} below floor -{}",
                    node.tokens(),
                    node.burst()
                );
            }
        }
    }

    /// A rejected request leaves the leaf→root path unchanged.
    #[test]
    fn prop_rejected_request_is_no_op(amount in 0u64..2000u64) {
        let tree = build();
        let _ = tree.conform_at(4, 150, 0);
        let before: Vec<i64> = [1u32, 2, 4].iter().map(|id| tree.node(*id).unwrap().tokens()).collect();
        let accepted = tree.conform_at(4, amount, 0);
        let after: Vec<i64> = [1u32, 2, 4].iter().map(|id| tree.node(*id).unwrap().tokens()).collect();
        if !accepted {
            prop_assert_eq!(before, after);
        }
    }
}
