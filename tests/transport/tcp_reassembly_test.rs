use bytes::BytesMut;
use proptest::prelude::*;
use std::{sync::Arc, time::Duration};
use utility_backend::transport::tcp::{FrameReassembler, ReassemblyConfig, TransportError};

fn config() -> Arc<ReassemblyConfig> {
    Arc::new(ReassemblyConfig::new(
        256 * 1024,
        u16::MAX as usize,
        Duration::from_secs(60),
    ))
}

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn feed_chunks(chunks: &[&[u8]]) -> Result<Vec<BytesMut>, TransportError> {
    let mut reassembler = FrameReassembler::new(config());
    let mut delivered = Vec::new();

    for chunk in chunks {
        if let Some(payload) = reassembler.push_data(chunk)? {
            delivered.push(payload);
        }
        while let Some(payload) = reassembler.try_parse_frame()? {
            delivered.push(payload);
        }
    }

    Ok(delivered)
}

#[test]
fn reassembles_one_byte_reads() {
    let payload = b"fragmented utility telemetry";
    let wire = frame(payload);
    let chunks = wire.iter().map(std::slice::from_ref).collect::<Vec<_>>();

    let delivered = feed_chunks(&chunks).unwrap();

    assert_eq!(delivered.len(), 1);
    assert_eq!(&delivered[0][..], payload);
}

#[test]
fn reassembles_mtu_boundary_split() {
    let payload = vec![0xAB; 4096];
    let wire = frame(&payload);
    let chunks = vec![&wire[..1500], &wire[1500..3000], &wire[3000..]];

    let delivered = feed_chunks(&chunks).unwrap();

    assert_eq!(delivered.len(), 1);
    assert_eq!(&delivered[0][..], payload.as_slice());
}

#[test]
fn delivers_multiple_nagle_coalesced_frames() {
    let payloads = [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()];
    let mut wire = Vec::new();
    for payload in payloads {
        wire.extend_from_slice(&frame(payload));
    }

    let delivered = feed_chunks(&[&wire]).unwrap();

    assert_eq!(delivered.len(), 3);
    assert_eq!(&delivered[0][..], b"one");
    assert_eq!(&delivered[1][..], b"two");
    assert_eq!(&delivered[2][..], b"three");
}

#[test]
fn rejects_adversarial_buffer_growth() {
    let cfg = Arc::new(ReassemblyConfig::new(
        8,
        u16::MAX as usize,
        Duration::from_secs(60),
    ));
    let mut reassembler = FrameReassembler::new(cfg);
    let mut partial = (128_u32).to_le_bytes().to_vec();
    partial.extend_from_slice(&[0_u8; 5]);

    let err = reassembler.push_data(&partial).unwrap_err();

    assert_eq!(err, TransportError::BufferExceeded { current: 9, max: 8 });
    assert_eq!(reassembler.buffered_len(), 0);
}

#[test]
fn rejects_zero_length_header() {
    let mut reassembler = FrameReassembler::new(config());

    let err = reassembler.push_data(&0_u32.to_le_bytes()).unwrap_err();

    assert_eq!(
        err,
        TransportError::InvalidHeader {
            reason: "zero-length payload"
        }
    );
}

#[test]
fn rejects_too_large_frame() {
    let mut reassembler = FrameReassembler::new(Arc::new(ReassemblyConfig::new(
        256 * 1024,
        8,
        Duration::from_secs(60),
    )));

    let err = reassembler.push_data(&9_u32.to_le_bytes()).unwrap_err();

    assert_eq!(err, TransportError::FrameTooLarge { length: 9, max: 8 });
}

proptest! {
    #[test]
    fn property_reassembles_random_fragmentation(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..512), 1..64),
        chunk_sizes in prop::collection::vec(1usize..8192, 1..256),
    ) {
        let mut wire = Vec::new();
        for payload in &payloads {
            wire.extend_from_slice(&frame(payload));
        }

        let mut reassembler = FrameReassembler::new(config());
        let mut delivered = Vec::new();
        let mut offset = 0;
        let mut chunk_index = 0;
        while offset < wire.len() {
            let chunk_size = chunk_sizes[chunk_index % chunk_sizes.len()];
            let end = (offset + chunk_size).min(wire.len());
            if let Some(payload) = reassembler.push_data(&wire[offset..end])? {
                delivered.push(payload.to_vec());
            }
            while let Some(payload) = reassembler.try_parse_frame()? {
                delivered.push(payload.to_vec());
            }
            offset = end;
            chunk_index += 1;
        }

        prop_assert_eq!(delivered, payloads);
        prop_assert_eq!(reassembler.buffered_len(), 0);
    }
}
