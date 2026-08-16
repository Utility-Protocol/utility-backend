use criterion::{black_box, criterion_group, criterion_main, Criterion};
use utility_backend::settlement::merkle::{Leaf, MerkleTree};

fn leaves(n: usize) -> Vec<Leaf> {
    (0..n)
        .map(|i| Leaf {
            meter_id: i as u64,
            timestamp_ms: 1_700_000_000_000 + i as i64,
            commodity_type: (i % 4) as u8,
            scaled_reading: i as i128 * 1_000,
            nonce: i as u64 ^ 0xa5a5_a5a5,
            hlc_timestamp: i as u64,
        })
        .collect()
}

fn merkle_bench(c: &mut Criterion) {
    for n in [256usize, 1024, 4096, 16_384] {
        c.bench_function(&format!("merkle_tree_build_{n}"), |b| {
            let leaves = leaves(n);
            b.iter(|| MerkleTree::new(black_box(&leaves)).expect("tree builds"));
        });

        c.bench_function(&format!("merkle_proof_{n}"), |b| {
            let leaves = leaves(n);
            let tree = MerkleTree::new(&leaves).expect("tree builds");
            b.iter(|| tree.prove(black_box(n / 2)).expect("proof exists"));
        });
    }
}

criterion_group!(benches, merkle_bench);
criterion_main!(benches);
