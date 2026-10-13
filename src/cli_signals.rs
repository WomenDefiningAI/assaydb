//! CLI-only signal handling: async-signal-safe notification, cooperative cleanup.
use std::sync::atomic::{AtomicBool, Ordering};

static CANCELLED: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_signal(_: libc::c_int) {
    CANCELLED.store(true, Ordering::Release);
}

pub struct Signals {
    previous: Vec<(libc::c_int, libc::sigaction)>,
}
impl Signals {
    pub fn install() -> Result<Self, String> {
        CANCELLED.store(false, Ordering::Release);
        let mut guard = Self {
            previous: Vec::with_capacity(2),
        };
        for signal in [libc::SIGINT, libc::SIGTERM] {
            // SAFETY: zero initializes the C action, sigemptyset initializes its
            // mask, and sigaction copies it before returning. The handler only
            // writes a lock-free AtomicBool and neither allocates nor takes locks.
            unsafe {
                let mut action: libc::sigaction = std::mem::zeroed();
                if libc::sigemptyset(&mut action.sa_mask) != 0 {
                    return Err(std::io::Error::last_os_error().to_string());
                }
                action.sa_sigaction = handle_signal as usize;
                action.sa_flags = libc::SA_RESTART;
                let mut previous = std::mem::MaybeUninit::uninit();
                if libc::sigaction(signal, &action, previous.as_mut_ptr()) != 0 {
                    return Err(std::io::Error::last_os_error().to_string());
                }
                guard.previous.push((signal, previous.assume_init()));
            }
        }
        Ok(guard)
    }
    pub fn cancellation(&self) -> assaydb::spill::Cancellation {
        assaydb::spill::Cancellation::with_signal_flag(&CANCELLED)
    }
}
impl Drop for Signals {
    fn drop(&mut self) {
        for (signal, previous) in self.previous.iter().rev() {
            // SAFETY: saved actions came from successful sigaction calls. The
            // standalone CLI installs once and restores them before returning.
            unsafe {
                libc::sigaction(*signal, previous, std::ptr::null_mut());
            }
        }
    }
}
