use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use utility_backend::ingestion::{MeterEvent, SharedRingBuffer};

fn bench_ringbuf_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("ringbuf_round_trip");
    let capacity = 262_144usize;

    for load_percent in [10usize, 50, 90, 99] {
        group.bench_with_input(
            BenchmarkId::from_parameter(load_percent),
            &load_percent,
            |b, load| {
                let path = format!("/tmp/utility-ringbuf-bench-{}-{}", std::process::id(), load);
                let ring = SharedRingBuffer::create(&path, capacity).expect("create ring buffer");
                let prefill = capacity * *load / 100;
                for i in 0..prefill {
                    ring.try_push(MeterEvent::new(i as u64, i as u64, 1, i as i128))
                        .unwrap();
                }

                let mut sequence = 0u64;
                b.iter(|| {
                    let _ = ring.try_pop();
                    let event = MeterEvent::new(sequence, sequence, 1, sequence as i128);
                    ring.try_push(black_box(event)).unwrap();
                    sequence = sequence.wrapping_add(1);
                });
                let _ = std::fs::remove_file(path);
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_ringbuf_round_trip);
criterion_main!(benches);
