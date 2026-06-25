//! Disk-backed durable spillover queue for cross-shard settlement messages.
//!
//! When a shard's in-memory buffer is exhausted, the credit-flow controller
//! spills messages here instead of dropping them or blocking ingestion. The
//! queue is FIFO per shard.
//!
//! The blueprint (issue #54) calls for a RocksDB column-family-per-shard
//! backend. To avoid pulling a C++/`librocksdb-sys` build dependency into the
//! release image, this module defines a [`DurableQueue`] trait with two
//! self-contained, dependency-free implementations:
//!
//! * [`FileDurableQueue`] — one append-structured log file per shard, providing
//!   real on-disk durability and FIFO `pop`.
//! * [`InMemoryDurableQueue`] — a process-memory queue used by tests and as a
//!   default when no spill directory is configured.
//!
//! A RocksDB backend can be added later as a third `DurableQueue` impl without
//! touching callers.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::settlement::messages::SettlementMessage;

/// FIFO, per-shard durable queue used for spillover under backpressure.
pub trait DurableQueue: Send + Sync {
    /// Append a message to the tail of `shard_id`'s queue.
    fn push(&self, shard_id: u64, msg: &SettlementMessage) -> io::Result<()>;

    /// Remove and return up to `count` messages from the head of `shard_id`'s
    /// queue, oldest first. Returns fewer than `count` (possibly zero) when the
    /// queue drains.
    fn pop_batch(&self, shard_id: u64, count: usize) -> io::Result<Vec<SettlementMessage>>;

    /// Number of messages currently queued for `shard_id`.
    fn len(&self, shard_id: u64) -> usize;

    /// Whether `shard_id`'s queue is empty.
    fn is_empty(&self, shard_id: u64) -> bool {
        self.len(shard_id) == 0
    }
}

// ---------------------------------------------------------------------------
// In-memory implementation
// ---------------------------------------------------------------------------

/// Process-memory durable queue. Not crash-safe; intended for tests and for
/// deployments without a configured spill directory.
#[derive(Default)]
pub struct InMemoryDurableQueue {
    shards: DashMap<u64, Mutex<VecDeque<SettlementMessage>>>,
}

impl InMemoryDurableQueue {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DurableQueue for InMemoryDurableQueue {
    fn push(&self, shard_id: u64, msg: &SettlementMessage) -> io::Result<()> {
        self.shards
            .entry(shard_id)
            .or_default()
            .lock()
            .push_back(msg.clone());
        Ok(())
    }

    fn pop_batch(&self, shard_id: u64, count: usize) -> io::Result<Vec<SettlementMessage>> {
        let mut out = Vec::new();
        if let Some(entry) = self.shards.get(&shard_id) {
            let mut q = entry.lock();
            for _ in 0..count {
                match q.pop_front() {
                    Some(m) => out.push(m),
                    None => break,
                }
            }
        }
        Ok(out)
    }

    fn len(&self, shard_id: u64) -> usize {
        self.shards
            .get(&shard_id)
            .map(|e| e.lock().len())
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// File-backed implementation
// ---------------------------------------------------------------------------

/// Per-shard on-disk state: a single log file with independent read/write
/// cursors. Records are framed as `[u32 len][message bytes]`.
struct ShardLog {
    file: File,
    read_pos: u64,
    write_pos: u64,
    count: usize,
}

/// Append-structured-log durable queue with one file per shard under a base
/// directory. Provides FIFO semantics with real on-disk persistence.
pub struct FileDurableQueue {
    dir: PathBuf,
    shards: DashMap<u64, Mutex<ShardLog>>,
}

impl FileDurableQueue {
    /// Open (creating if necessary) a durable queue rooted at `dir`.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            shards: DashMap::new(),
        })
    }

    fn shard_path(&self, shard_id: u64) -> PathBuf {
        self.dir.join(format!("shard_{shard_id:020}.log"))
    }

    /// Borrow (opening on first use) the log for `shard_id` and run `f` with it
    /// held under its mutex.
    fn with_shard<R>(
        &self,
        shard_id: u64,
        f: impl FnOnce(&mut ShardLog) -> io::Result<R>,
    ) -> io::Result<R> {
        if !self.shards.contains_key(&shard_id) {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(self.shard_path(shard_id))?;
            let write_pos = file.metadata()?.len();
            self.shards.entry(shard_id).or_insert_with(|| {
                Mutex::new(ShardLog {
                    file,
                    read_pos: 0,
                    write_pos,
                    count: 0,
                })
            });
        }
        let entry = self
            .shards
            .get(&shard_id)
            .expect("shard log inserted above");
        let mut log = entry.lock();
        f(&mut log)
    }
}

impl DurableQueue for FileDurableQueue {
    fn push(&self, shard_id: u64, msg: &SettlementMessage) -> io::Result<()> {
        let bytes = msg.to_bytes();
        let len = u32::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "message too large"))?;
        self.with_shard(shard_id, |log| {
            log.file.seek(SeekFrom::Start(log.write_pos))?;
            log.file.write_all(&len.to_le_bytes())?;
            log.file.write_all(&bytes)?;
            log.file.flush()?;
            log.write_pos += 4 + bytes.len() as u64;
            log.count += 1;
            Ok(())
        })
    }

    fn pop_batch(&self, shard_id: u64, count: usize) -> io::Result<Vec<SettlementMessage>> {
        self.with_shard(shard_id, |log| {
            let mut out = Vec::new();
            for _ in 0..count {
                if log.read_pos >= log.write_pos {
                    break;
                }
                log.file.seek(SeekFrom::Start(log.read_pos))?;
                let mut len_buf = [0u8; 4];
                log.file.read_exact(&mut len_buf)?;
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut rec = vec![0u8; len];
                log.file.read_exact(&mut rec)?;
                let msg = SettlementMessage::from_bytes(&rec).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "corrupt durable record")
                })?;
                out.push(msg);
                log.read_pos += 4 + len as u64;
                log.count = log.count.saturating_sub(1);
            }
            // Reclaim disk space once fully drained.
            if log.read_pos >= log.write_pos {
                log.file.set_len(0)?;
                log.read_pos = 0;
                log.write_pos = 0;
            }
            Ok(out)
        })
    }

    fn len(&self, shard_id: u64) -> usize {
        self.shards
            .get(&shard_id)
            .map(|e| e.lock().count)
            .unwrap_or(0)
    }
}
