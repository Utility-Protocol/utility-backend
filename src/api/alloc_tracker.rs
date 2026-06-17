use crate::api::metrics::GC_PAUSE_SECONDS;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

static ALLOC_ENABLED: AtomicBool = AtomicBool::new(true);

pub fn pause_alloc_tracking() {
    ALLOC_ENABLED.store(false, Ordering::SeqCst);
}

pub fn resume_alloc_tracking() {
    ALLOC_ENABLED.store(true, Ordering::SeqCst);
}

pub struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let start = Instant::now();
        let ptr = System.alloc(layout);
        let elapsed = start.elapsed().as_secs_f64();
        if ALLOC_ENABLED.load(Ordering::Relaxed) {
            GC_PAUSE_SECONDS.add(elapsed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let start = Instant::now();
        System.dealloc(ptr, layout);
        let elapsed = start.elapsed().as_secs_f64();
        if ALLOC_ENABLED.load(Ordering::Relaxed) {
            GC_PAUSE_SECONDS.add(elapsed);
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let start = Instant::now();
        let ptr = System.alloc_zeroed(layout);
        let elapsed = start.elapsed().as_secs_f64();
        if ALLOC_ENABLED.load(Ordering::Relaxed) {
            GC_PAUSE_SECONDS.add(elapsed);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let start = Instant::now();
        let new_ptr = System.realloc(ptr, layout, new_size);
        let elapsed = start.elapsed().as_secs_f64();
        if ALLOC_ENABLED.load(Ordering::Relaxed) {
            GC_PAUSE_SECONDS.add(elapsed);
        }
        new_ptr
    }
}
