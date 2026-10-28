//! Measurement-only global allocator (`--features count-alloc`). It counts the
//! process's live Rust heap bytes and their high-water across all threads. The
//! shipped binary does not enable it. Native C allocations made inside codec
//! libraries (for example zstd contexts) bypass the Rust allocator and are not
//! counted; neither is allocator retention of freed memory.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

struct Counting;
#[global_allocator]
static ALLOCATOR: Counting = Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static HIGH: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    HIGH.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every call forwards to System with the caller's layout; the counters
// never affect allocation results.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Count the transient old+new peak a moving realloc can have.
        grow(new_size);
        // SAFETY: forwarded unchanged.
        let result = unsafe { System.realloc(ptr, layout, new_size) };
        if result.is_null() {
            LIVE.fetch_sub(new_size, Ordering::Relaxed);
        } else {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        result
    }
}

/// Live-byte high-water since process start.
pub(crate) fn high_water() -> usize {
    HIGH.load(Ordering::Relaxed)
}
