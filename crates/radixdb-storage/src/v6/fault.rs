//! Test-only lifecycle fault injection at stable durable boundaries.

use std::io;

use super::GenerationCrashPoint;

#[cfg(any(test, feature = "test-failpoints"))]
mod enabled {
    use std::cell::Cell;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard};

    use super::{io, GenerationCrashPoint};

    const POINT_ENV: &str = "RADIXDB_GENERATION_FAULT_POINT";
    const READY_ENV: &str = "RADIXDB_GENERATION_FAULT_READY";

    static OWNER: Mutex<()> = Mutex::new(());
    static ACTIVE: Mutex<Option<Arc<Registration>>> = Mutex::new(None);
    static PROCESS_STOPPED: AtomicBool = AtomicBool::new(false);
    static NEXT_THREAD_TOKEN: AtomicU64 = AtomicU64::new(1);

    thread_local! {
        static THREAD_TOKEN: Cell<u64> =
            Cell::new(NEXT_THREAD_TOKEN.fetch_add(1, Ordering::Relaxed));
    }

    fn current_thread_token() -> u64 {
        THREAD_TOKEN.with(Cell::get)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum GenerationFaultMode {
        ReturnIoError,
        AbortProcess,
    }

    #[derive(Debug)]
    struct Registration {
        point: GenerationCrashPoint,
        mode: GenerationFaultMode,
        owner: u64,
        hits: AtomicU64,
    }

    pub struct GenerationFaultGuard {
        registration: Arc<Registration>,
        _owner: MutexGuard<'static, ()>,
    }

    impl GenerationFaultGuard {
        pub fn arm(point: GenerationCrashPoint, mode: GenerationFaultMode) -> Self {
            let owner = OWNER
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let registration = Arc::new(Registration {
                point,
                mode,
                owner: current_thread_token(),
                hits: AtomicU64::new(0),
            });
            *ACTIVE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&registration));
            Self {
                registration,
                _owner: owner,
            }
        }

        pub fn hit_count(&self) -> u64 {
            self.registration.hits.load(Ordering::Acquire)
        }
    }

    impl Drop for GenerationFaultGuard {
        fn drop(&mut self) {
            *ACTIVE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        }
    }

    pub(super) fn reach(point: GenerationCrashPoint) -> io::Result<()> {
        reach_child_stop(point);
        let registration = ACTIVE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let Some(registration) = registration
            .filter(|active| active.point == point && active.owner == current_thread_token())
        else {
            return Ok(());
        };
        if registration
            .hits
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        match registration.mode {
            GenerationFaultMode::ReturnIoError => Err(io::Error::other(format!(
                "injected lifecycle boundary {}",
                point.name()
            ))),
            GenerationFaultMode::AbortProcess => std::process::abort(),
        }
    }

    fn reach_child_stop(point: GenerationCrashPoint) {
        let Ok(requested) = std::env::var(POINT_ENV) else {
            return;
        };
        if requested != point.name()
            || PROCESS_STOPPED
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        let ready = std::env::var_os(READY_ENV)
            .unwrap_or_else(|| panic!("{READY_ENV} is required when {POINT_ENV} is set"));
        let path = Path::new(&ready);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap_or_else(|error| {
                panic!(
                    "failed to publish lifecycle evidence '{}': {error}",
                    path.display()
                )
            });
        writeln!(file, "{}", point.name()).expect("write lifecycle boundary evidence");
        file.sync_all()
            .expect("sync lifecycle boundary evidence before process stop");
        std::process::abort();
    }
}

#[cfg(any(test, feature = "test-failpoints"))]
pub use enabled::{GenerationFaultGuard, GenerationFaultMode};

#[inline]
pub(crate) fn reach_generation_boundary(point: GenerationCrashPoint) -> io::Result<()> {
    #[cfg(any(test, feature = "test-failpoints"))]
    {
        enabled::reach(point)
    }
    #[cfg(not(any(test, feature = "test-failpoints")))]
    {
        let _ = point;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_registration_fires_once_at_its_exact_boundary() {
        let guard = GenerationFaultGuard::arm(
            GenerationCrashPoint::ControlDurable,
            GenerationFaultMode::ReturnIoError,
        );
        assert!(reach_generation_boundary(GenerationCrashPoint::ControlAfterPartialWrite).is_ok());
        assert!(reach_generation_boundary(GenerationCrashPoint::ControlDurable).is_err());
        assert!(reach_generation_boundary(GenerationCrashPoint::ControlDurable).is_ok());
        assert_eq!(guard.hit_count(), 1);
    }
}
