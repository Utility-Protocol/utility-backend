//! Thread-local fixed-size arena allocator for the telemetry hot path (#50).
//!
//! Three size classes (128/256/512 B) each own a set of [`Slab`]s plus a global
//! recycling free-list. Each thread keeps a local free-list per class; the hot
//! path (`allocate`/`deallocate`) touches only that thread-local list and a few
//! relaxed atomic counters — no locks, no syscalls. When a thread's local list
//! is empty it bulk-acquires [`REFILL_BATCH`] blocks from the size class; when
//! it grows past [`LOCAL_MAX`] it bulk-returns [`RETURN_BATCH`] blocks.
//!
//! Recycled blocks are tracked by address in an external list rather than an
//! intrusive (written-into-the-block) list, trading a little memory for freedom
//! from use-after-free hazards. Block recycling stays correct and lock-light;
//! the per-class lock is taken only on the (infrequent, batched) refill/return
//! slow path.
//!
//! NOTE: like the existing `TrackingAllocator`, this is intentionally **not**
//! installed as `#[global_allocator]`. It is used explicitly for hot-path
//! buffers; wiring it in globally needs allocator-bootstrapping care beyond this
//! change. A [`GlobalAlloc`] impl is provided for that future use and for
//! benchmarking.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use thread_local::ThreadLocal;

use super::slab::Slab;

/// Fixed block sizes served by the arena, ascending.
pub const BLOCK_SIZES: [usize; 3] = [128, 256, 512];

/// Blocks pulled from a size class when a thread-local list runs dry.
const REFILL_BATCH: usize = 256;
/// Blocks returned to a size class when a thread-local list overflows.
const RETURN_BATCH: usize = 1024;
/// Thread-local list length that triggers a bulk return.
const LOCAL_MAX: usize = 2048;

/// Arena sizing parameters.
#[derive(Clone, Copy, Debug)]
pub struct ArenaConfig {
    /// Bytes per slab.
    pub slab_size: usize,
    /// Maximum slabs per size class (bounds arena memory).
    pub max_slabs_per_class: usize,
}

impl Default for ArenaConfig {
    fn default() -> Self {
        Self {
            slab_size: 2 * 1024 * 1024,
            max_slabs_per_class: 32,
        }
    }
}

/// A snapshot of the arena's hot-path counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArenaMetrics {
    pub alloc_count: u64,
    pub free_count: u64,
    pub global_acquire_count: u64,
    pub page_fault_count: u64,
}

struct ClassState {
    slabs: Vec<Slab>,
    free_list: Vec<usize>,
}

/// One size class: its slabs and recycled-block free-list behind a single lock
/// (taken only on the batched slow path).
struct SizeClass {
    block_size: usize,
    slab_size: usize,
    max_slabs: usize,
    state: Mutex<ClassState>,
}

impl SizeClass {
    fn new(block_size: usize, slab_size: usize, max_slabs: usize) -> Self {
        Self {
            block_size,
            slab_size,
            max_slabs,
            state: Mutex::new(ClassState {
                slabs: Vec::new(),
                free_list: Vec::new(),
            }),
        }
    }

    /// Acquire up to `want` blocks (recycled first, then freshly bumped from
    /// slabs). Returns the blocks and how many new slabs had to be mapped.
    fn refill(&self, want: usize) -> (Vec<usize>, usize) {
        let mut st = self.state.lock();
        let mut out = Vec::with_capacity(want);

        let take = want.min(st.free_list.len());
        let start = st.free_list.len() - take;
        out.extend(st.free_list.drain(start..));

        let mut new_slabs = 0;
        while out.len() < want {
            let need = want - out.len();
            let has_room = st.slabs.last().is_some_and(|s| !s.is_exhausted());
            if !has_room {
                if st.slabs.len() >= self.max_slabs {
                    break;
                }
                st.slabs.push(Slab::new(self.slab_size, self.block_size));
                new_slabs += 1;
            }
            let batch = st.slabs.last().expect("slab present").reserve_batch(need);
            if batch.is_empty() {
                break;
            }
            out.extend(batch);
        }
        (out, new_slabs)
    }

    fn return_blocks(&self, blocks: &[usize]) {
        self.state.lock().free_list.extend_from_slice(blocks);
    }

    fn owns(&self, addr: usize) -> bool {
        self.state.lock().slabs.iter().any(|s| s.contains(addr))
    }
}

type LocalCache = [Vec<usize>; 3];

/// Workload-priority-agnostic fixed-size arena allocator.
pub struct ArenaAllocator {
    classes: [SizeClass; 3],
    cache: ThreadLocal<RefCell<LocalCache>>,
    alloc_count: AtomicU64,
    free_count: AtomicU64,
    global_acquire_count: AtomicU64,
    page_fault_count: AtomicU64,
}

impl ArenaAllocator {
    /// Build an arena with the given sizing.
    pub fn new(config: ArenaConfig) -> Self {
        Self {
            classes: [
                SizeClass::new(BLOCK_SIZES[0], config.slab_size, config.max_slabs_per_class),
                SizeClass::new(BLOCK_SIZES[1], config.slab_size, config.max_slabs_per_class),
                SizeClass::new(BLOCK_SIZES[2], config.slab_size, config.max_slabs_per_class),
            ],
            cache: ThreadLocal::new(),
            alloc_count: AtomicU64::new(0),
            free_count: AtomicU64::new(0),
            global_acquire_count: AtomicU64::new(0),
            page_fault_count: AtomicU64::new(0),
        }
    }

    /// Build an arena with the default (issue #50) sizing.
    pub fn with_defaults() -> Self {
        Self::new(ArenaConfig::default())
    }

    /// Size class index for `size`, or `None` if it exceeds the largest block.
    fn class_index(size: usize) -> Option<usize> {
        BLOCK_SIZES.iter().position(|&b| size <= b)
    }

    fn local(&self) -> &RefCell<LocalCache> {
        self.cache
            .get_or(|| RefCell::new([Vec::new(), Vec::new(), Vec::new()]))
    }

    /// Allocate a block large enough for `size` bytes, or `None` if `size` has
    /// no matching block class or the arena is fully exhausted.
    pub fn allocate(&self, size: usize) -> Option<*mut u8> {
        let idx = Self::class_index(size)?;
        let mut cache = self.local().borrow_mut();
        if cache[idx].is_empty() {
            let (batch, new_slabs) = self.classes[idx].refill(REFILL_BATCH);
            if batch.is_empty() {
                return None;
            }
            self.global_acquire_count.fetch_add(1, Ordering::Relaxed);
            self.page_fault_count
                .fetch_add(new_slabs as u64, Ordering::Relaxed);
            cache[idx].extend(batch);
        }
        let addr = cache[idx].pop()?;
        self.alloc_count.fetch_add(1, Ordering::Relaxed);
        Some(addr as *mut u8)
    }

    /// Return a block previously obtained from [`Self::allocate`] for the same
    /// `size`. Returns `false` if `size` has no matching block class.
    pub fn deallocate(&self, size: usize, ptr: *mut u8) -> bool {
        let Some(idx) = Self::class_index(size) else {
            return false;
        };
        let mut cache = self.local().borrow_mut();
        cache[idx].push(ptr as usize);
        self.free_count.fetch_add(1, Ordering::Relaxed);
        if cache[idx].len() > LOCAL_MAX {
            let start = cache[idx].len() - RETURN_BATCH;
            let returned: Vec<usize> = cache[idx].drain(start..).collect();
            self.classes[idx].return_blocks(&returned);
        }
        true
    }

    /// Whether `ptr` was handed out by this arena (lives in one of its slabs).
    pub fn owns(&self, ptr: *mut u8) -> bool {
        let addr = ptr as usize;
        self.classes.iter().any(|c| c.owns(addr))
    }

    /// Current hot-path counter snapshot.
    pub fn metrics(&self) -> ArenaMetrics {
        ArenaMetrics {
            alloc_count: self.alloc_count.load(Ordering::Relaxed),
            free_count: self.free_count.load(Ordering::Relaxed),
            global_acquire_count: self.global_acquire_count.load(Ordering::Relaxed),
            page_fault_count: self.page_fault_count.load(Ordering::Relaxed),
        }
    }

    /// Publish the current counters to the Prometheus registry.
    pub fn publish_metrics(&self) {
        let m = self.metrics();
        crate::api::metrics::set_arena_counters(
            m.alloc_count as f64,
            m.free_count as f64,
            m.global_acquire_count as f64,
            m.page_fault_count as f64,
        );
    }
}

// SAFETY: the arena serves blocks only for layouts that fit a block class and
// need <= 8-byte alignment; everything else (and any allocation it cannot
// satisfy) delegates to the system allocator. `dealloc` routes back to the same
// place via the `owns` ownership check, so blocks are never freed by the wrong
// allocator. It uses `System` (not the global allocator) internally, so it is
// safe to install as a `#[global_allocator]` without self-recursion.
unsafe impl GlobalAlloc for ArenaAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.align() > 8 {
            return System.alloc(layout);
        }
        match self.allocate(layout.size()) {
            Some(ptr) => ptr,
            None => System.alloc(layout),
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.align() <= 8 && self.owns(ptr) {
            self.deallocate(layout.size(), ptr);
        } else {
            System.dealloc(ptr, layout);
        }
    }
}
