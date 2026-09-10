//! Linux syscall oracle, linked only into this integration test executable.

use std::cell::RefCell;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SyncEvent {
    pub path: PathBuf,
    pub succeeded: bool,
}

#[derive(Default)]
struct State {
    events: Vec<SyncEvent>,
    fail_sync: Option<PathBuf>,
    fallback: bool,
    fail_unlink: Option<PathBuf>,
    fallbacks: usize,
    unlink_failures: usize,
}

thread_local! {
    static TRACE: RefCell<Option<State>> = const { RefCell::new(None) };
}

pub struct Trace;

impl Trace {
    pub fn start(fail_sync: Option<&Path>, fallback: bool, fail_unlink: Option<&Path>) -> Self {
        TRACE.with(|slot| {
            assert!(slot.borrow().is_none(), "nested syscall trace");
            *slot.borrow_mut() = Some(State {
                fail_sync: fail_sync.map(Path::to_path_buf),
                fallback,
                fail_unlink: fail_unlink.map(Path::to_path_buf),
                ..State::default()
            });
        });
        Self
    }

    pub fn finish(self) -> (Vec<SyncEvent>, usize, usize) {
        let state = TRACE.with(|slot| slot.borrow_mut().take().unwrap());
        (state.events, state.fallbacks, state.unlink_failures)
    }
}

impl Drop for Trace {
    fn drop(&mut self) {
        TRACE.with(|slot| *slot.borrow_mut() = None);
    }
}

// Outside an armed test thread these symbols delegate directly to Linux.
// Explicit faults return EIO; every other fsync reaches the real filesystem.
#[unsafe(no_mangle)]
unsafe extern "C" fn fsync(fd: libc::c_int) -> libc::c_int {
    let armed = TRACE
        .try_with(|slot| slot.borrow().is_some())
        .unwrap_or(false);
    if !armed {
        return unsafe { libc::syscall(libc::SYS_fsync, fd) as libc::c_int };
    }
    let path = std::fs::read_link(format!("/proc/self/fd/{fd}")).unwrap_or_default();
    let fail = TRACE.with(|slot| {
        let mut slot = slot.borrow_mut();
        let state = slot.as_mut().unwrap();
        if state.fail_sync.as_ref() == Some(&path) {
            state.fail_sync.take();
            true
        } else {
            false
        }
    });
    let result = if fail {
        unsafe { *libc::__errno_location() = libc::EIO };
        -1
    } else {
        unsafe { libc::syscall(libc::SYS_fsync, fd) as libc::c_int }
    };
    let errno = unsafe { *libc::__errno_location() };
    TRACE.with(|slot| {
        slot.borrow_mut().as_mut().unwrap().events.push(SyncEvent {
            path,
            succeeded: result == 0,
        });
    });
    unsafe { *libc::__errno_location() = errno };
    result
}

#[unsafe(no_mangle)]
unsafe extern "C" fn renameat2(
    old_dir: libc::c_int,
    old: *const libc::c_char,
    new_dir: libc::c_int,
    new: *const libc::c_char,
    flags: libc::c_uint,
) -> libc::c_int {
    let fallback = TRACE
        .try_with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(state) = slot.as_mut().filter(|state| state.fallback) {
                state.fallbacks += 1;
                true
            } else {
                false
            }
        })
        .unwrap_or(false);
    if fallback {
        unsafe { *libc::__errno_location() = libc::ENOSYS };
        return -1;
    }
    unsafe { libc::syscall(libc::SYS_renameat2, old_dir, old, new_dir, new, flags) as libc::c_int }
}

#[unsafe(no_mangle)]
unsafe extern "C" fn unlink(path: *const libc::c_char) -> libc::c_int {
    use std::os::unix::ffi::OsStrExt;

    let fail = TRACE
        .try_with(|slot| {
            let mut slot = slot.borrow_mut();
            let Some(state) = slot.as_mut().filter(|state| state.fail_unlink.is_some()) else {
                return false;
            };
            let bytes = unsafe { std::ffi::CStr::from_ptr(path) }.to_bytes();
            if state.fail_unlink.as_ref().unwrap().as_os_str().as_bytes() == bytes {
                state.fail_unlink.take();
                state.unlink_failures += 1;
                true
            } else {
                false
            }
        })
        .unwrap_or(false);
    if fail {
        unsafe { *libc::__errno_location() = libc::EIO };
        return -1;
    }
    unsafe { libc::syscall(libc::SYS_unlinkat, libc::AT_FDCWD, path, 0) as libc::c_int }
}
