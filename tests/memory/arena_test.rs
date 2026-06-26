//! Tests for the fixed-size arena allocator (issue #50).

use std::sync::Arc;
use std::thread;

use utility_backend::memory::arena::{ArenaAllocator, ArenaConfig};
use utility_backend::memory::slab::Slab;

fn small_arena() -> ArenaAllocator {
    ArenaAllocator::new(ArenaConfig {
        slab_size: 256 * 1024,
        max_slabs_per_class: 8,
    })
}

/// Write a recognizable pattern across the block and read it back.
///
/// # Safety
/// `ptr` must point to at least `size` writable bytes (an arena block).
unsafe fn fill_and_check(ptr: *mut u8, size: usize, seed: u8) {
    for i in 0..size {
        *ptr.add(i) = (i as u8).wrapping_add(seed);
    }
    for i in 0..size {
        assert_eq!(*ptr.add(i), (i as u8).wrapping_add(seed));
    }
}

#[test]
fn test_slab_reserve_distinct_blocks_and_exhaust() {
    let slab = Slab::new(1024, 128); // 8 blocks
    assert_eq!(slab.block_size(), 128);

    let batch = slab.reserve_batch(5);
    assert_eq!(batch.len(), 5);
    for w in batch.windows(2) {
        assert_eq!(w[1] - w[0], 128, "blocks must be one block_size apart");
    }

    let rest = slab.reserve_batch(10);
    assert_eq!(rest.len(), 3, "only 3 of 8 blocks remain");
    assert!(slab.is_exhausted());

    assert!(slab.contains(batch[0]));
    assert!(!slab.contains(batch[0] + 1024));
}

#[test]
fn test_size_class_matching() {
    let arena = small_arena();
    // Sizes map up to the next block class; > 512 has none.
    for size in [1usize, 128, 200, 256, 300, 512] {
        let ptr = arena.allocate(size);
        assert!(ptr.is_some(), "size {size} should map to a block class");
        arena.deallocate(size, ptr.unwrap());
    }
    assert!(
        arena.allocate(513).is_none(),
        "513 exceeds the largest block"
    );
    assert!(arena.allocate(4096).is_none());
}

#[test]
fn test_alloc_write_free_and_reuse() {
    let arena = small_arena();

    let p1 = arena.allocate(128).unwrap();
    assert!(arena.owns(p1));
    unsafe { fill_and_check(p1, 128, 0x5A) };
    assert!(arena.deallocate(128, p1));

    // Freed block returns to the thread-local list and is handed back next.
    let p2 = arena.allocate(128).unwrap();
    assert_eq!(p1, p2, "freed block should be recycled (LIFO)");
    arena.deallocate(128, p2);

    let m = arena.metrics();
    assert_eq!(m.alloc_count, 2);
    assert_eq!(m.free_count, 2);
    assert!(m.global_acquire_count >= 1);
}

#[test]
fn test_owns_rejects_foreign_pointers() {
    let arena = small_arena();
    let p = arena.allocate(256).unwrap();
    assert!(arena.owns(p));

    // A pointer from a different allocator must not be claimed.
    let foreign = Box::into_raw(Box::new(0u8));
    assert!(!arena.owns(foreign));
    unsafe { drop(Box::from_raw(foreign)) };

    arena.deallocate(256, p);
}

#[test]
fn test_concurrent_alloc_free_counts_and_no_corruption() {
    const THREADS: usize = 4;
    const ITERS: usize = 5_000;
    let arena = Arc::new(small_arena());

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let arena = arena.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ITERS {
                let size = [128usize, 256, 512][i % 3];
                let ptr = arena.allocate(size).expect("arena should not exhaust");
                // The block is exclusively owned between alloc and free.
                unsafe {
                    *ptr = t as u8;
                    assert_eq!(*ptr, t as u8, "block aliased across threads");
                }
                assert!(arena.deallocate(size, ptr));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let m = arena.metrics();
    assert_eq!(m.alloc_count, (THREADS * ITERS) as u64);
    assert_eq!(m.free_count, (THREADS * ITERS) as u64);
}
