//! Semaphore-gated recycling buffer pool for the telemetry hot path (issue #33).
//!
//! Reading every frame into a freshly allocated `Vec<u8>` produces ~10k
//! allocations/second and unbounded memory under load. This pool pre-sizes a
//! fixed set of `MAX_FRAME_SIZE` buffers and hands them out via a
//! [`tokio::sync::Semaphore`], so:
//!
//! * memory is bounded to `capacity * buf_size` (default 1024 × 64 KB = 64 MB);
//! * once all buffers are in flight, [`BufferPool::acquire`] awaits — applying
//!   backpressure to the reader instead of allocating without limit;
//! * released buffers are recycled, keeping the steady-state allocation rate at
//!   ~0 (tracked via [`BufferPool::allocation_count`]).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Maximum frame payload size per the wire protocol (64 KB).
pub const MAX_FRAME_SIZE: usize = 64 * 1024;

/// Default number of pooled buffers.
pub const DEFAULT_POOL_CAPACITY: usize = 1024;

struct PoolInner {
    free: Mutex<Vec<Vec<u8>>>,
    buf_size: usize,
    allocations: AtomicU64,
}

/// A fixed-capacity pool of reusable byte buffers.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<PoolInner>,
    permits: Arc<Semaphore>,
    capacity: usize,
}

impl BufferPool {
    /// Create a pool of `capacity` buffers, each `buf_size` bytes.
    pub fn new(capacity: usize, buf_size: usize) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                free: Mutex::new(Vec::with_capacity(capacity)),
                buf_size,
                allocations: AtomicU64::new(0),
            }),
            permits: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    /// Create a pool with the issue-#33 defaults (1024 × 64 KB).
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_POOL_CAPACITY, MAX_FRAME_SIZE)
    }

    /// Size of each buffer in bytes.
    pub fn buf_size(&self) -> usize {
        self.inner.buf_size
    }

    /// Total number of buffers the pool can hand out concurrently.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Buffers currently available to acquire without waiting.
    pub fn available(&self) -> usize {
        self.permits.available_permits()
    }

    /// Total fresh buffer allocations performed (reuses do not count). At steady
    /// state this plateaus near `capacity`.
    pub fn allocation_count(&self) -> u64 {
        self.inner.allocations.load(Ordering::Relaxed)
    }

    fn take_buffer(&self) -> Vec<u8> {
        if let Some(buf) = self.inner.free.lock().pop() {
            buf
        } else {
            self.inner.allocations.fetch_add(1, Ordering::Relaxed);
            vec![0u8; self.inner.buf_size]
        }
    }

    /// Acquire a buffer, awaiting if all are in flight (backpressure).
    pub async fn acquire(&self) -> PooledBuffer {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("buffer pool semaphore is never closed");
        PooledBuffer {
            buf: Some(self.take_buffer()),
            inner: self.inner.clone(),
            _permit: permit,
        }
    }

    /// Acquire a buffer without waiting; `None` if none are free.
    pub fn try_acquire(&self) -> Option<PooledBuffer> {
        let permit = self.permits.clone().try_acquire_owned().ok()?;
        Some(PooledBuffer {
            buf: Some(self.take_buffer()),
            inner: self.inner.clone(),
            _permit: permit,
        })
    }
}

/// A buffer checked out from a [`BufferPool`]; returns itself on drop.
pub struct PooledBuffer {
    buf: Option<Vec<u8>>,
    inner: Arc<PoolInner>,
    /// Held for RAII: releases the pool slot (backpressure) on drop.
    #[allow(dead_code)]
    _permit: OwnedSemaphorePermit,
}

impl PooledBuffer {
    /// The full buffer as a mutable slice (`buf_size` bytes).
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf.as_mut().expect("buffer present until drop")
    }

    /// The full buffer as a shared slice.
    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_ref().expect("buffer present until drop")
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            // Buffers keep their `buf_size` length, so they are reusable as-is.
            self.inner.free.lock().push(buf);
        }
        // `_permit` drops here, freeing a slot for a waiting acquirer.
    }
}
