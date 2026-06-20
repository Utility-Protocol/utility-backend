use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use utility_backend::gateway::parser::parse_envelope;

// Counting allocator — tracks heap allocations to enforce the zero-alloc contract.
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

fn make_envelope(meter_id: &str, payload: &[u8]) -> Vec<u8> {
    let id = meter_id.as_bytes();
    let mut buf = Vec::with_capacity(2 + id.len() + payload.len() + 32);
    buf.extend_from_slice(&(id.len() as u16).to_be_bytes());
    buf.extend_from_slice(id);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(&[0xABu8; 32]);
    buf
}

fn bench_zero_alloc(c: &mut Criterion) {
    let payload = vec![0u8; 256];
    let data = make_envelope("MTR-0001-SENSOR-A", &payload);

    c.bench_function("parse_envelope/zero_alloc_contract", |b| {
        b.iter(|| {
            let before = ALLOC_COUNT.load(Ordering::Relaxed);
            let result = parse_envelope(black_box(&data));
            let after = ALLOC_COUNT.load(Ordering::Relaxed);
            assert_eq!(
                after - before,
                0,
                "parse_envelope made {} unexpected heap allocation(s)",
                after - before
            );
            black_box(result.unwrap());
        })
    });
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_envelope/throughput");
    for &total_size in &[64usize, 256, 512, 1024, 2048] {
        let payload_len = total_size.saturating_sub(2 + 8 + 32); // header + "MTR-BNCH" + checksum
        let payload = vec![0u8; payload_len];
        let data = make_envelope("MTR-BNCH", &payload);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("envelope_bytes", total_size),
            &data,
            |b, d| {
                b.iter(|| black_box(parse_envelope(black_box(d)).unwrap()));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_zero_alloc, bench_throughput);
criterion_main!(benches);
