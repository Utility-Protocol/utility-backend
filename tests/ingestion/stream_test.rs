//! Tests for zero-copy framed ingestion with buffer-pool backpressure (#33).

use std::sync::Arc;

use parking_lot::Mutex;

use utility_backend::ingestion::buffer_pool::BufferPool;
use utility_backend::ingestion::frame_parser::{read_frame, FrameError, TelemetryFrame};
use utility_backend::ingestion::stream_handler::handle_stream;

fn encode(frame: &TelemetryFrame) -> Vec<u8> {
    let mut payload = Vec::new();
    ciborium::into_writer(frame, &mut payload).expect("encode");
    payload
}

fn framed(payload: &[u8]) -> Vec<u8> {
    let mut out = (payload.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(payload);
    out
}

fn sample(i: u64) -> TelemetryFrame {
    TelemetryFrame {
        meter_id: format!("meter-{i}"),
        timestamp: i,
        value: i as f64 + 0.5,
    }
}

#[tokio::test]
async fn test_pool_recycles_and_bounds_allocations() {
    let pool = BufferPool::new(2, 1024);
    assert_eq!(pool.available(), 2);

    {
        let _a = pool.acquire().await;
        let _b = pool.acquire().await;
        assert_eq!(pool.available(), 0);
        assert!(pool.try_acquire().is_none(), "exhausted pool backpressures");
        assert_eq!(pool.allocation_count(), 2);
    }

    // Both released; reacquiring reuses buffers without new allocations.
    assert_eq!(pool.available(), 2);
    let _c = pool.acquire().await;
    assert_eq!(pool.allocation_count(), 2, "buffer reused, no new alloc");
}

#[tokio::test]
async fn test_read_and_decode_frames() {
    let pool = BufferPool::new(4, 1024);
    let (f1, f2) = (sample(1), sample(2));
    let mut data = framed(&encode(&f1));
    data.extend(framed(&encode(&f2)));

    let mut reader: &[u8] = &data;

    let frame = read_frame(&mut reader, &pool).await.unwrap();
    assert_eq!(frame.decode::<TelemetryFrame>().unwrap(), f1);
    drop(frame);

    let frame = read_frame(&mut reader, &pool).await.unwrap();
    assert_eq!(frame.decode::<TelemetryFrame>().unwrap(), f2);
    drop(frame);

    assert!(matches!(
        read_frame(&mut reader, &pool).await,
        Err(FrameError::Closed)
    ));
}

#[tokio::test]
async fn test_oversized_frame_is_rejected_before_read() {
    let pool = BufferPool::new(2, 16); // 16-byte buffers
    let mut data = 100u32.to_be_bytes().to_vec(); // advertises 100 > 16
    data.extend_from_slice(&[0u8; 100]);

    let mut reader: &[u8] = &data;
    assert!(matches!(
        read_frame(&mut reader, &pool).await,
        Err(FrameError::FrameTooLarge {
            length: 100,
            max: 16
        })
    ));
}

#[tokio::test]
async fn test_held_frame_applies_backpressure() {
    let pool = BufferPool::new(1, 1024);
    let mut data = framed(&encode(&sample(1)));
    data.extend(framed(&encode(&sample(2))));

    let mut reader: &[u8] = &data;
    let held = read_frame(&mut reader, &pool).await.unwrap();
    assert_eq!(pool.available(), 0, "the only buffer is in use");
    assert!(
        pool.try_acquire().is_none(),
        "no buffer until the frame drops"
    );

    drop(held);
    assert_eq!(pool.available(), 1);
}

#[tokio::test]
async fn test_handle_stream_dispatches_all_frames() {
    let pool = BufferPool::new(8, 1024);
    let n: u64 = 50;
    let mut data = Vec::new();
    for i in 0..n {
        data.extend(framed(&encode(&sample(i))));
    }

    let received = Arc::new(Mutex::new(Vec::new()));
    let sink = received.clone();
    let mut reader: &[u8] = &data;

    let stats = handle_stream(&mut reader, &pool, |frame| {
        let decoded: TelemetryFrame = frame.decode().unwrap();
        sink.lock().push(decoded.timestamp);
    })
    .await
    .unwrap();

    assert_eq!(stats.frames, n);
    let got = received.lock();
    assert_eq!(got.len(), n as usize);
    assert_eq!(got[0], 0);
    assert_eq!(got[n as usize - 1], n - 1);
}

#[tokio::test]
async fn test_handle_stream_rejects_oversized() {
    let pool = BufferPool::new(2, 16);
    let mut data = 1000u32.to_be_bytes().to_vec();
    data.extend_from_slice(&[0u8; 10]);

    let mut reader: &[u8] = &data;
    let result = handle_stream(&mut reader, &pool, |_| {}).await;
    assert!(matches!(result, Err(FrameError::FrameTooLarge { .. })));
}
