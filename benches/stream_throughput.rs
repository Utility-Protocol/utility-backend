//! Throughput benchmark for telemetry frame ingestion (issue #33).
//!
//! Compares the pooled, length-prefixed reader against the previous pattern of
//! allocating a fresh `Vec<u8>` per frame. Run with `cargo bench --bench
//! stream_throughput`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;

use utility_backend::ingestion::buffer_pool::BufferPool;
use utility_backend::ingestion::frame_parser::{read_frame, FrameError, TelemetryFrame};

fn encode_frames(n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..n {
        let frame = TelemetryFrame {
            meter_id: format!("meter-{i}"),
            timestamp: i as u64,
            value: i as f64,
        };
        let mut payload = Vec::new();
        ciborium::into_writer(&frame, &mut payload).expect("encode");
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&payload);
    }
    out
}

fn bench_ingestion(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let frames = 10_000;
    let data = encode_frames(frames);

    let pool = BufferPool::with_defaults();
    c.bench_function("pooled_read_10k_frames", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut reader: &[u8] = &data;
                loop {
                    match read_frame(&mut reader, &pool).await {
                        Ok(frame) => {
                            black_box(frame.payload());
                        }
                        Err(FrameError::Closed) => break,
                        Err(e) => panic!("unexpected frame error: {e}"),
                    }
                }
            });
        });
    });

    c.bench_function("naive_alloc_read_10k_frames", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut reader: &[u8] = &data;
                loop {
                    let mut len_buf = [0u8; 4];
                    match reader.read_exact(&mut len_buf).await {
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => panic!("io error: {e}"),
                    }
                    let len = u32::from_be_bytes(len_buf) as usize;
                    let mut buf = vec![0u8; len]; // fresh allocation per frame
                    reader.read_exact(&mut buf).await.unwrap();
                    black_box(&buf);
                }
            });
        });
    });
}

criterion_group!(benches, bench_ingestion);
criterion_main!(benches);
