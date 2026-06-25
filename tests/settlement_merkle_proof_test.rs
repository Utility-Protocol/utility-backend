use proptest::prelude::*;
use utility_backend::settlement::merkle::{verify, Leaf, MerkleTree, SiblingSide};

fn arb_leaf() -> impl Strategy<Value = Leaf> {
    (
        any::<u64>(),
        any::<i64>(),
        any::<u8>(),
        any::<i128>(),
        any::<u64>(),
    )
        .prop_map(
            |(meter_id, timestamp_ms, commodity_type, scaled_reading, nonce)| Leaf {
                meter_id,
                timestamp_ms,
                commodity_type,
                scaled_reading,
                nonce,
            },
        )
}

proptest! {
    #[test]
    fn verifies_valid_inclusion_proofs(leaves in prop::collection::vec(arb_leaf(), 1..256)) {
        let tree = MerkleTree::new(&leaves).expect("tree builds");
        for (i, leaf) in leaves.iter().enumerate() {
            let proof = tree.prove(i).expect("proof exists");
            prop_assert!(verify(tree.root(), leaf, &proof));
        }
    }

    #[test]
    fn rejects_wrong_leaf(mut leaves in prop::collection::vec(arb_leaf(), 2..128)) {
        let tree = MerkleTree::new(&leaves).expect("tree builds");
        let proof = tree.prove(0).expect("proof exists");
        leaves[0].nonce = leaves[0].nonce.wrapping_add(1);
        prop_assert!(!verify(tree.root(), &leaves[0], &proof));
    }

    #[test]
    fn rejects_reordered_sibling_side(leaves in prop::collection::vec(arb_leaf(), 2..128)) {
        let tree = MerkleTree::new(&leaves).expect("tree builds");
        let mut proof = tree.prove(0).expect("proof exists");
        if let Some(first) = proof.path.first_mut() {
            first.side = match first.side {
                SiblingSide::Left => SiblingSide::Right,
                SiblingSide::Right => SiblingSide::Left,
            };
        }
        prop_assert!(!verify(tree.root(), &leaves[0], &proof));
    }
}

#[test]
fn rejects_batches_over_limit() {
    let leaves = vec![
        Leaf {
            meter_id: 1,
            timestamp_ms: 1,
            commodity_type: 0,
            scaled_reading: 1,
            nonce: 1,
        };
        16_385
    ];

    assert!(MerkleTree::new(&leaves).is_err());
}
