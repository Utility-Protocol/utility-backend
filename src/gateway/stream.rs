use crate::ingestion::tai64n::Tai64N;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tracing::{info, warn};

pub struct MeterEvent {
    pub meter_id: String,
    pub timestamp_tai: Tai64N,
    pub correction_ns: i64,
    pub reading: f64,
    pub token_volume: u64,
}

#[allow(dead_code)]
pub struct BackpressureFilter {
    buffer_capacity: usize,
    tx: mpsc::Sender<MeterEvent>,
}

impl BackpressureFilter {
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<MeterEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                buffer_capacity: capacity,
                tx,
            },
            rx,
        )
    }

    pub async fn push(&self, event: MeterEvent) -> Result<(), &'static str> {
        self.tx
            .send(event)
            .await
            .map_err(|_| "backpressure buffer full: dropping event")
    }
}

pub async fn ingest_stream(
    filter: Arc<BackpressureFilter>,
    mut stream: impl tokio_stream::Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
) {
    use tokio_stream::StreamExt;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(data) => {
                info!(len = data.len(), "received meter datagram");
                let event = MeterEvent {
                    meter_id: String::from("unknown"),
                    timestamp_tai: Tai64N::now_with_correction(0),
                    correction_ns: 0,
                    reading: 0.0,
                    token_volume: 0,
                };
                if let Err(e) = filter.push(event).await {
                    warn!("{}", e);
                }
            }
            Err(e) => warn!(error = %e, "stream read error"),
        }
    }
}

// ===========================================================================
// Priority backpressure filter with adaptive budget and spill-to-disk (#4)
// ===========================================================================

/// Delivery priority of a meter event. `Critical` is highest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventPriority {
    /// Over-voltage / brownout alerts — never dropped, delivered first.
    Critical = 0,
    /// Watermark / control-plane events.
    High = 1,
    /// Routine meter readings.
    Normal = 2,
    /// Reporting / debug traffic.
    Low = 3,
}

impl EventPriority {
    pub const ALL: [EventPriority; 4] = [
        EventPriority::Critical,
        EventPriority::High,
        EventPriority::Normal,
        EventPriority::Low,
    ];

    pub fn index(self) -> usize {
        self as usize
    }

    /// `Critical`/`High` bypass the memory budget (never spilled or dropped).
    pub fn is_high_priority(self) -> bool {
        matches!(self, EventPriority::Critical | EventPriority::High)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            EventPriority::Critical => "critical",
            EventPriority::High => "high",
            EventPriority::Normal => "normal",
            EventPriority::Low => "low",
        }
    }

    fn from_u8(value: u8) -> Option<EventPriority> {
        match value {
            0 => Some(EventPriority::Critical),
            1 => Some(EventPriority::High),
            2 => Some(EventPriority::Normal),
            3 => Some(EventPriority::Low),
            _ => None,
        }
    }
}

/// What happened to a pushed event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// Buffered in memory.
    Enqueued,
    /// Over budget; written to the spill store.
    Spilled,
    /// Over budget and the spill store rejected it.
    Dropped,
}

/// An overflow event, retaining its priority and footprint for replay.
pub struct Spilled<E> {
    pub priority: EventPriority,
    pub size: usize,
    pub event: E,
}

/// Encoding used by the file-backed spill store.
pub trait SpillCodec: Sized {
    fn encode(&self, out: &mut Vec<u8>);
    fn decode(bytes: &[u8]) -> Option<Self>;
}

impl<E: SpillCodec> SpillCodec for Spilled<E> {
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.priority as u8);
        out.extend_from_slice(&(self.size as u64).to_le_bytes());
        self.event.encode(out);
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let priority = EventPriority::from_u8(*bytes.first()?)?;
        let size = u64::from_le_bytes(bytes.get(1..9)?.try_into().ok()?) as usize;
        let event = E::decode(bytes.get(9..)?)?;
        Some(Spilled {
            priority,
            size,
            event,
        })
    }
}

/// Overflow store for spilled events. FIFO.
pub trait SpillStore<T>: Send + Sync {
    fn push(&self, item: T) -> std::io::Result<()>;
    fn pop(&self) -> std::io::Result<Option<T>>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Bounded in-memory spill store (the default overflow buffer).
pub struct InMemorySpillStore<T> {
    items: Mutex<VecDeque<T>>,
    capacity: usize,
}

impl<T> InMemorySpillStore<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            capacity,
        }
    }
}

impl<T: Send> SpillStore<T> for InMemorySpillStore<T> {
    fn push(&self, item: T) -> std::io::Result<()> {
        let mut items = self.items.lock();
        if items.len() >= self.capacity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "spill store full",
            ));
        }
        items.push_back(item);
        Ok(())
    }

    fn pop(&self) -> std::io::Result<Option<T>> {
        Ok(self.items.lock().pop_front())
    }

    fn len(&self) -> usize {
        self.items.lock().len()
    }
}

struct SpillFile {
    file: File,
    read_pos: u64,
    write_pos: u64,
    count: usize,
}

/// Disk-backed spill store: an append log with read/write cursors, framed as
/// `[u32 len][record]`. Recovers all spilled events FIFO.
pub struct FileSpillStore<T> {
    inner: Mutex<SpillFile>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: SpillCodec + Send> FileSpillStore<T> {
    /// Open (creating if needed) a spill file at `path`.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        let write_pos = file.metadata()?.len();
        Ok(Self {
            inner: Mutex::new(SpillFile {
                file,
                read_pos: 0,
                write_pos,
                count: 0,
            }),
            _marker: PhantomData,
        })
    }
}

impl<T: SpillCodec + Send> SpillStore<T> for FileSpillStore<T> {
    fn push(&self, item: T) -> std::io::Result<()> {
        let mut bytes = Vec::new();
        item.encode(&mut bytes);
        let len = u32::try_from(bytes.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "record too large")
        })?;
        let mut log = self.inner.lock();
        let write_pos = log.write_pos;
        log.file.seek(SeekFrom::Start(write_pos))?;
        log.file.write_all(&len.to_le_bytes())?;
        log.file.write_all(&bytes)?;
        log.file.flush()?;
        log.write_pos += 4 + bytes.len() as u64;
        log.count += 1;
        Ok(())
    }

    fn pop(&self) -> std::io::Result<Option<T>> {
        let mut log = self.inner.lock();
        if log.read_pos >= log.write_pos {
            return Ok(None);
        }
        let read_pos = log.read_pos;
        log.file.seek(SeekFrom::Start(read_pos))?;
        let mut len_buf = [0u8; 4];
        log.file.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut record = vec![0u8; len];
        log.file.read_exact(&mut record)?;
        let item = T::decode(&record).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt record")
        })?;
        log.read_pos += 4 + len as u64;
        log.count = log.count.saturating_sub(1);
        if log.read_pos >= log.write_pos {
            log.file.set_len(0)?;
            log.read_pos = 0;
            log.write_pos = 0;
        }
        Ok(Some(item))
    }

    fn len(&self) -> usize {
        self.inner.lock().count
    }
}

struct Level<E> {
    queue: VecDeque<(E, usize)>,
    count: u64,
}

struct FilterInner<E> {
    levels: [Level<E>; 4],
    mem_bytes: usize,
}

/// A point-in-time snapshot of the filter's state (serialized by the admin API).
#[derive(Clone, Debug, Serialize)]
pub struct BackpressureStats {
    pub memory_bytes: usize,
    pub budget_bytes: usize,
    pub utilization: f64,
    pub critical: u64,
    pub high: u64,
    pub normal: u64,
    pub low: u64,
    pub spilled_total: u64,
    pub dropped_total: u64,
    pub spill_backlog: usize,
}

/// Multi-level priority backpressure filter with an adaptive memory budget and
/// spill-to-overflow. `Critical`/`High` events bypass the budget so alerts are
/// never dropped; `Normal`/`Low` events spill once the budget is exceeded and
/// are replayed by [`PriorityBackpressureFilter::drain_spill`] as memory frees.
pub struct PriorityBackpressureFilter<E> {
    inner: Mutex<FilterInner<E>>,
    notify: Notify,
    budget_bytes: usize,
    spill: Arc<dyn SpillStore<Spilled<E>>>,
    spilled_count: AtomicU64,
    dropped_count: AtomicU64,
}

impl<E: Send + 'static> PriorityBackpressureFilter<E> {
    /// Create a filter with the given memory budget and spill store.
    pub fn new(budget_bytes: usize, spill: Arc<dyn SpillStore<Spilled<E>>>) -> Self {
        Self {
            inner: Mutex::new(FilterInner {
                levels: std::array::from_fn(|_| Level {
                    queue: VecDeque::new(),
                    count: 0,
                }),
                mem_bytes: 0,
            }),
            notify: Notify::new(),
            budget_bytes,
            spill,
            spilled_count: AtomicU64::new(0),
            dropped_count: AtomicU64::new(0),
        }
    }

    /// Create a filter backed by a bounded in-memory spill store.
    pub fn with_in_memory_spill(budget_bytes: usize, spill_capacity: usize) -> Self {
        Self::new(
            budget_bytes,
            Arc::new(InMemorySpillStore::new(spill_capacity)),
        )
    }

    /// Offer an event of `priority` whose in-memory footprint is `size` bytes.
    pub fn push(&self, priority: EventPriority, event: E, size: usize) -> PushOutcome {
        let mut inner = self.inner.lock();
        if priority.is_high_priority() || inner.mem_bytes + size <= self.budget_bytes {
            let idx = priority.index();
            inner.levels[idx].queue.push_back((event, size));
            inner.levels[idx].count += 1;
            inner.mem_bytes += size;
            drop(inner);
            self.notify.notify_waiters();
            PushOutcome::Enqueued
        } else {
            drop(inner);
            match self.spill.push(Spilled {
                priority,
                size,
                event,
            }) {
                Ok(()) => {
                    self.spilled_count.fetch_add(1, Ordering::Relaxed);
                    PushOutcome::Spilled
                }
                Err(_) => {
                    self.dropped_count.fetch_add(1, Ordering::Relaxed);
                    PushOutcome::Dropped
                }
            }
        }
    }

    /// Pop the highest-priority buffered event, if any.
    pub fn pop(&self) -> Option<E> {
        let mut guard = self.inner.lock();
        let inner = &mut *guard;
        for level in inner.levels.iter_mut() {
            if let Some((event, size)) = level.queue.pop_front() {
                level.count -= 1;
                inner.mem_bytes -= size;
                return Some(event);
            }
        }
        None
    }

    /// Replay spilled events back into memory while the budget allows. Returns
    /// the number moved.
    pub fn drain_spill(&self) -> std::io::Result<usize> {
        let mut moved = 0;
        loop {
            let item = match self.spill.pop()? {
                Some(item) => item,
                None => break,
            };
            let mut inner = self.inner.lock();
            if inner.mem_bytes + item.size <= self.budget_bytes {
                let idx = item.priority.index();
                inner.levels[idx].queue.push_back((item.event, item.size));
                inner.levels[idx].count += 1;
                inner.mem_bytes += item.size;
                drop(inner);
                moved += 1;
            } else {
                drop(inner);
                // No room yet: return it to the spill store and stop.
                self.spill.push(item)?;
                break;
            }
        }
        if moved > 0 {
            self.notify.notify_waiters();
        }
        Ok(moved)
    }

    /// Current buffer/spill statistics.
    pub fn stats(&self) -> BackpressureStats {
        let inner = self.inner.lock();
        let utilization = if self.budget_bytes == 0 {
            0.0
        } else {
            inner.mem_bytes as f64 / self.budget_bytes as f64
        };
        BackpressureStats {
            memory_bytes: inner.mem_bytes,
            budget_bytes: self.budget_bytes,
            utilization,
            critical: inner.levels[0].count,
            high: inner.levels[1].count,
            normal: inner.levels[2].count,
            low: inner.levels[3].count,
            spilled_total: self.spilled_count.load(Ordering::Relaxed),
            dropped_total: self.dropped_count.load(Ordering::Relaxed),
            spill_backlog: self.spill.len(),
        }
    }

    /// Number of events buffered in memory across all priorities.
    pub fn len(&self) -> usize {
        let inner = self.inner.lock();
        inner.levels.iter().map(|l| l.count).sum::<u64>() as usize
    }

    /// Whether the in-memory buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Await and return the next event (highest priority first).
    pub async fn recv(&self) -> E {
        loop {
            if let Some(event) = self.pop() {
                return event;
            }
            let notified = self.notify.notified();
            if let Some(event) = self.pop() {
                return event;
            }
            let _ = tokio::time::timeout(Duration::from_millis(50), notified).await;
        }
    }
}

/// Spawn a background task that periodically replays spilled events.
pub fn spawn_drain_task<E: Send + 'static>(
    filter: Arc<PriorityBackpressureFilter<E>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(e) = filter.drain_spill() {
                warn!(error = %e, "backpressure spill drain failed");
            }
        }
    })
}
