//! Durable shutdown checkpoint for crash-safe resume (issue #49).
//!
//! Records the last persisted per-source watermark and which shutdown stages
//! have drained, so a restart can resume from the exact point of interruption.
//! The blueprint stores this in RocksDB under `shutdown_checkpoint_v1` with a
//! synchronous write; to keep the build free of a `librocksdb-sys` C++ toolchain
//! this uses a single file written with `File::sync_all` (the durability
//! equivalent of `WriteOptions::set_sync(true)`).

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Identifier of a meter event source.
pub type MeterSourceId = String;

/// A point-in-time shutdown checkpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShutdownCheckpoint {
    /// When the checkpoint was written (Unix nanoseconds).
    pub timestamp_ns: i64,
    /// Last persisted offset (watermark) per meter source.
    pub watermark: HashMap<MeterSourceId, u64>,
    /// Bitset of shutdown stages that have fully drained (bit N = stage N).
    pub stages_drained: u8,
}

impl ShutdownCheckpoint {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark stage `bit` (0..8) as drained.
    pub fn mark_stage(&mut self, bit: u8) {
        if bit < 8 {
            self.stages_drained |= 1 << bit;
        }
    }

    /// Whether stage `bit` has drained.
    pub fn is_stage_drained(&self, bit: u8) -> bool {
        bit < 8 && ((self.stages_drained >> bit) & 1) == 1
    }

    /// Record the highest persisted offset for `source`.
    pub fn set_watermark(&mut self, source: impl Into<MeterSourceId>, offset: u64) {
        self.watermark.insert(source.into(), offset);
    }

    /// Serialize to the durable byte form.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.timestamp_ns.to_le_bytes());
        buf.push(self.stages_drained);
        buf.extend_from_slice(&(self.watermark.len() as u32).to_le_bytes());
        for (source, offset) in &self.watermark {
            buf.extend_from_slice(&(source.len() as u32).to_le_bytes());
            buf.extend_from_slice(source.as_bytes());
            buf.extend_from_slice(&offset.to_le_bytes());
        }
        buf
    }

    /// Parse a value produced by [`Self::to_bytes`]; `None` on truncation.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut pos = 0usize;
        let take = |bytes: &[u8], pos: &mut usize, n: usize| -> Option<Vec<u8>> {
            let slice = bytes.get(*pos..*pos + n)?.to_vec();
            *pos += n;
            Some(slice)
        };
        let timestamp_ns = i64::from_le_bytes(take(bytes, &mut pos, 8)?.try_into().ok()?);
        let stages_drained = *take(bytes, &mut pos, 1)?.first()?;
        let count = u32::from_le_bytes(take(bytes, &mut pos, 4)?.try_into().ok()?) as usize;
        let mut watermark = HashMap::with_capacity(count);
        for _ in 0..count {
            let key_len = u32::from_le_bytes(take(bytes, &mut pos, 4)?.try_into().ok()?) as usize;
            let key = String::from_utf8(take(bytes, &mut pos, key_len)?).ok()?;
            let offset = u64::from_le_bytes(take(bytes, &mut pos, 8)?.try_into().ok()?);
            watermark.insert(key, offset);
        }
        Some(Self {
            timestamp_ns,
            watermark,
            stages_drained,
        })
    }
}

/// File-backed durable store for a single [`ShutdownCheckpoint`].
pub struct CheckpointStore {
    path: PathBuf,
}

impl CheckpointStore {
    /// Open a store backed by the file at `path` (created on first save).
    pub fn open(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Persist `checkpoint` durably (data flushed to disk via `sync_all`).
    pub fn save(&self, checkpoint: &ShutdownCheckpoint) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = checkpoint.to_bytes();
        let mut file = std::fs::File::create(&self.path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    /// Load the stored checkpoint, or `None` if absent or unreadable.
    pub fn load(&self) -> io::Result<Option<ShutdownCheckpoint>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(ShutdownCheckpoint::from_bytes(&bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}
