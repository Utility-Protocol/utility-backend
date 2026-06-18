use crate::api::metrics::GC_PAUSE_SECONDS;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Instant;

thread_local! {
    static ALLOC_TRACKING: Cell<bool> = const { Cell::new(true) };
}

pub struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let start = Instant::now();
        let ptr = System.alloc(layout);
        let elapsed = start.elapsed().as_secs_f64();
        ALLOC_TRACKING.with(|tracking| {
            if tracking.get() {
                tracking.set(false);
                GC_PAUSE_SECONDS.add(elapsed);
                tracking.set(true);
            }
        });
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let start = Instant::now();
        System.dealloc(ptr, layout);
        let elapsed = start.elapsed().as_secs_f64();
        ALLOC_TRACKING.with(|tracking| {
            if tracking.get() {
                tracking.set(false);
                GC_PAUSE_SECONDS.add(elapsed);
                tracking.set(true);
            }
        });
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let start = Instant::now();
        let ptr = System.alloc_zeroed(layout);
        let elapsed = start.elapsed().as_secs_f64();
        ALLOC_TRACKING.with(|tracking| {
            if tracking.get() {
                tracking.set(false);
                GC_PAUSE_SECONDS.add(elapsed);
                tracking.set(true);
            }
        });
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let start = Instant::now();
        let new_ptr = System.realloc(ptr, layout, new_size);
        let elapsed = start.elapsed().as_secs_f64();
        ALLOC_TRACKING.with(|tracking| {
            if tracking.get() {
                tracking.set(false);
                GC_PAUSE_SECONDS.add(elapsed);
                tracking.set(true);
            }
        });
        new_ptr
    }
}
