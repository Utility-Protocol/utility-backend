//! Cross-shard settlement message types and their wire codec.
//!
//! The canonical schema lives in [`messages.proto`](./messages.proto). To keep
//! the build free of a `protoc`/`prost` toolchain dependency (the release image
//! ships no protobuf compiler), the wire form is a small, explicit little-endian
//! binary encoding implemented here. The struct layout matches the `.proto`
//! definitions field-for-field so a protobuf backend can be swapped in later
//! without touching callers.

/// A single settlement payload routed from one shard to another.
///
/// `msg_id` is monotonically increasing **per (source shard, destination
/// shard)** stream and is the basis for deduplication and acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettlementMessage {
    /// Destination shard this message is addressed to.
    pub shard_id: u64,
    /// Monotonic per-stream sequence id.
    pub msg_id: u64,
    /// Opaque settlement payload (avg 256 B, max 4 KB).
    pub payload: Vec<u8>,
}

/// Credit replenishment sent by the receiver back to the sender after it has
/// processed a batch of messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreditGrant {
    /// Shard the grant is addressed to (the original sender).
    pub shard_id: u64,
    /// Number of additional credits granted.
    pub delta: u32,
}

/// Acknowledgement carrying the highest contiguous `msg_id` the receiver has
/// durably accepted. Everything at or below this id may be released by the
/// sender's retransmission tracker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AckMessage {
    /// Shard the ack is addressed to (the original sender).
    pub shard_id: u64,
    /// Highest contiguous accepted `msg_id`.
    pub acked_msg_id: u64,
}

impl SettlementMessage {
    /// Serialize to the durable-queue / wire byte form:
    /// `[u64 shard_id][u64 msg_id][u32 payload_len][payload]` (all little-endian).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20 + self.payload.len());
        buf.extend_from_slice(&self.shard_id.to_le_bytes());
        buf.extend_from_slice(&self.msg_id.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Parse a message produced by [`Self::to_bytes`]. Returns `None` on any
    /// truncation or length mismatch.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 20 {
            return None;
        }
        let shard_id = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let msg_id = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let payload_len = u32::from_le_bytes(bytes[16..20].try_into().ok()?) as usize;
        let payload = bytes.get(20..20 + payload_len)?.to_vec();
        Some(Self {
            shard_id,
            msg_id,
            payload,
        })
    }
}
