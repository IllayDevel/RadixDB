use std::sync::{Arc, Condvar, Mutex};

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct InFlightByteLimiter {
    inner: Arc<(Mutex<InFlightByteState>, Condvar)>,
    capacity: usize,
}

#[derive(Debug)]
struct InFlightByteState {
    available: usize,
}

#[doc(hidden)]
pub struct InFlightBytePermit {
    limiter: Option<InFlightByteLimiter>,
    bytes: usize,
}

impl std::fmt::Debug for InFlightBytePermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlightBytePermit")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl InFlightByteLimiter {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner: Arc::new((
                Mutex::new(InFlightByteState {
                    available: capacity,
                }),
                Condvar::new(),
            )),
            capacity,
        }
    }

    pub fn acquire(&self, requested: usize) -> InFlightBytePermit {
        let bytes = requested.min(self.capacity);
        if bytes == 0 {
            return InFlightBytePermit {
                limiter: None,
                bytes: 0,
            };
        }

        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("in-flight byte permit mutex poisoned");
        while state.available < bytes {
            state = condvar
                .wait(state)
                .expect("in-flight byte permit mutex poisoned while waiting");
        }
        state.available -= bytes;
        InFlightBytePermit {
            limiter: Some(self.clone()),
            bytes,
        }
    }

    pub fn try_acquire(&self, requested: usize) -> Option<InFlightBytePermit> {
        let bytes = requested.min(self.capacity);
        if bytes == 0 {
            return Some(InFlightBytePermit {
                limiter: None,
                bytes: 0,
            });
        }

        let (lock, _) = &*self.inner;
        let mut state = lock.lock().expect("in-flight byte permit mutex poisoned");
        if state.available < bytes {
            return None;
        }
        state.available -= bytes;
        Some(InFlightBytePermit {
            limiter: Some(self.clone()),
            bytes,
        })
    }

    /// Read the byte owner without joining its wait queue. Diagnostics reports
    /// `None` while the limiter is contended instead of delaying a workload.
    pub fn try_usage(&self) -> Option<(usize, usize)> {
        let (lock, _) = &*self.inner;
        let state = lock.try_lock().ok()?;
        Some((self.capacity, self.capacity.saturating_sub(state.available)))
    }

    #[doc(hidden)]
    pub fn available(&self) -> usize {
        self.inner
            .0
            .lock()
            .expect("in-flight byte permit mutex poisoned")
            .available
    }
}

impl InFlightBytePermit {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for InFlightBytePermit {
    fn drop(&mut self) {
        let Some(limiter) = self.limiter.take() else {
            return;
        };
        if self.bytes == 0 {
            return;
        }
        let (lock, condvar) = &*limiter.inner;
        let mut state = lock.lock().expect("in-flight byte permit mutex poisoned");
        state.available = state
            .available
            .saturating_add(self.bytes)
            .min(limiter.capacity);
        condvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permits_bound_and_restore_the_owned_budget() {
        let limiter = InFlightByteLimiter::new(10);
        let first = limiter.acquire(7);
        assert_eq!(first.bytes(), 7);
        assert_eq!(limiter.available(), 3);
        assert!(limiter.try_acquire(4).is_none());

        let second = limiter.try_acquire(3).expect("remaining budget");
        assert_eq!(limiter.available(), 0);
        drop(second);
        drop(first);
        assert_eq!(limiter.available(), 10);
    }

    #[test]
    fn oversized_request_owns_at_most_the_capacity() {
        let limiter = InFlightByteLimiter::new(4);
        let permit = limiter.acquire(usize::MAX);
        assert_eq!(permit.bytes(), 4);
        assert_eq!(limiter.try_usage(), Some((4, 4)));
    }
}
