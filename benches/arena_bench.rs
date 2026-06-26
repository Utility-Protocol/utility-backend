//! Criterion benchmarks for the fixed-size arena allocator (issue #50).
//!
//! Compares the arena against `std::alloc::System` for the hot-path pattern
//! (alloc -> use -> free in the same thread) and for cross-thread frees.
//! `mimalloc` is intentionally not included: it pulls a C/`libmimalloc-sys`
//! toolchain into the build, which the release image (`rust:slim`) does not
//! carry. Add it locally if you want the three-way comparison.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::thread;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use utility_backend::memory::arena::ArenaAllocator;

fn bench_same_thread(c: &mut Criterion) {
    let arena = ArenaAllocator::with_defaults();
    let mut group = c.benchmark_group("alloc_free_128_same_thread");

    group.bench_function("arena", |b| {
        b.iter(|| {
            let ptr = arena.allocate(black_box(128)).expect("arena block");
            arena.deallocate(128, ptr);
        });
    });

    let layout = Layout::from_size_align(128, 8).unwrap();
    group.bench_function("system", |b| {
        b.iter(|| unsafe {
            let ptr = System.alloc(black_box(layout));
            System.dealloc(ptr, layout);
        });
    });

    group.finish();
}

fn bench_cross_thread(c: &mut Criterion) {
    // One producer allocates, worker threads free: stresses the global
    // recycling free-list rather than the thread-local fast path.
    c.bench_function("alloc_then_cross_thread_free_128", |b| {
        let arena = Arc::new(ArenaAllocator::with_defaults());
        b.iter(|| {
            let mut ptrs: Vec<usize> = Vec::with_capacity(256);
            for _ in 0..256 {
                ptrs.push(arena.allocate(128).expect("arena block") as usize);
            }
            let mut handles = Vec::new();
            for chunk in ptrs.chunks(64) {
                let arena = arena.clone();
                let chunk: Vec<usize> = chunk.to_vec();
                handles.push(thread::spawn(move || {
                    for addr in chunk {
                        arena.deallocate(128, addr as *mut u8);
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_same_thread, bench_cross_thread);
criterion_main!(benches);
