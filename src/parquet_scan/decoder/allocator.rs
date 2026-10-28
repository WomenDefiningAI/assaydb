//! Test-only allocator instrumentation. One selected thread/generation is
//! measured; unrelated concurrent unit tests and pre-existing allocations are
//! excluded. Headers let deallocation follow the allocation's generation.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Counting;
#[global_allocator]
static ALLOCATOR: Counting = Counting;
static SERIAL: Mutex<()> = Mutex::new(());
static GENERATION: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static HIGH: AtomicUsize = AtomicUsize::new(0);
thread_local! { static ACTIVE: Cell<usize> = const { Cell::new(0) }; }

fn layout(layout: Layout) -> (Layout, usize) {
    Layout::new::<usize>()
        .extend(layout)
        .expect("test allocation layout")
}

// SAFETY: every allocation uses System with the same extended layout on
// deallocation. The header is aligned for usize, the payload for its layout.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, requested: Layout) -> *mut u8 {
        let (layout, offset) = layout(requested);
        // SAFETY: valid extended allocation layout.
        let base = unsafe { System.alloc(layout) };
        if base.is_null() {
            return base;
        }
        let generation = ACTIVE.try_with(Cell::get).unwrap_or(0);
        // SAFETY: the header and payload both lie within this allocation.
        unsafe {
            base.cast::<usize>().write(generation);
        }
        if generation != 0 && generation == GENERATION.load(Ordering::Relaxed) {
            let live = LIVE.fetch_add(requested.size(), Ordering::Relaxed) + requested.size();
            HIGH.fetch_max(live, Ordering::Relaxed);
        }
        unsafe { base.add(offset) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, requested: Layout) {
        let (layout, offset) = layout(requested);
        // SAFETY: ptr was returned by alloc with this requested layout.
        let base = unsafe { ptr.sub(offset) };
        let generation = unsafe { base.cast::<usize>().read() };
        if generation != 0 && generation == GENERATION.load(Ordering::Relaxed) {
            LIVE.fetch_sub(requested.size(), Ordering::Relaxed);
        }
        unsafe {
            System.dealloc(base, layout);
        }
    }
}

pub(super) fn measure<T>(run: impl FnOnce() -> T) -> (T, usize) {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let generation = GENERATION.fetch_add(1, Ordering::Relaxed) + 1;
    LIVE.store(0, Ordering::Relaxed);
    HIGH.store(0, Ordering::Relaxed);
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.set(0);
        }
    }
    let _reset = Reset;
    ACTIVE.set(generation);
    let result = run();
    (result, HIGH.load(Ordering::Relaxed))
}
