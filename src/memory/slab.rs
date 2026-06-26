//! Fixed-size slab with a CAS-based bump allocator (issue #50).
//!
//! A [`Slab`] owns one contiguous, page-aligned memory region carved into
//! equal-size blocks. Blocks are handed out by atomically advancing a monotonic
//! bump pointer — because the pointer only ever increases, the compare-and-swap
//! has no ABA hazard. Recycling of freed blocks is handled one level up by the
//! size class's free-list (see [`super::arena`]).
//!
//! The backing region is obtained from the system allocator (`std::alloc`)
//! rather than `mmap(MAP_HUGETLB)` so the allocator is portable and never
//! recurses through a global allocator. Huge-page backing can be layered in
//! later on Linux without changing this interface.

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A contiguous region divided into fixed-size blocks, served via a bump pointer.
pub struct Slab {
    base: usize,
    size: usize,
    block_size: usize,
    bump: AtomicUsize,
    layout: Layout,
}

impl Slab {
    /// Allocate a slab of `size` bytes divided into `block_size` blocks.
    ///
    /// `size` and `block_size` must be non-zero with `size >= block_size`;
    /// `block_size` must be a power of two so block addresses keep its alignment.
    pub fn new(size: usize, block_size: usize) -> Self {
        assert!(
            block_size.is_power_of_two(),
            "block size must be power of two"
        );
        assert!(size >= block_size, "slab smaller than one block");
        let layout = Layout::from_size_align(size, block_size).expect("valid slab layout");
        // SAFETY: layout has non-zero size and a power-of-two alignment.
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        Self {
            base: ptr as usize,
            size,
            block_size,
            bump: AtomicUsize::new(0),
            layout,
        }
    }

    /// Block size served by this slab.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Whether the slab has no room left for another block via the bump pointer.
    pub fn is_exhausted(&self) -> bool {
        self.size - self.bump.load(Ordering::Acquire) < self.block_size
    }

    /// Atomically reserve up to `count` fresh blocks in a single CAS, returning
    /// their base addresses. Returns an empty vec when the slab is exhausted.
    pub fn reserve_batch(&self, count: usize) -> Vec<usize> {
        loop {
            let cur = self.bump.load(Ordering::Acquire);
            let avail = (self.size - cur) / self.block_size;
            let take = count.min(avail);
            if take == 0 {
                return Vec::new();
            }
            let new = cur + take * self.block_size;
            if self
                .bump
                .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return (0..take)
                    .map(|i| self.base + cur + i * self.block_size)
                    .collect();
            }
        }
    }

    /// Whether `addr` falls within this slab's region.
    pub fn contains(&self, addr: usize) -> bool {
        addr >= self.base && addr < self.base + self.size
    }
}

impl Drop for Slab {
    fn drop(&mut self) {
        // SAFETY: `base`/`layout` are exactly what `alloc` returned in `new`, and
        // the slab owns the region for its whole lifetime.
        unsafe { dealloc(self.base as *mut u8, self.layout) }
    }
}
