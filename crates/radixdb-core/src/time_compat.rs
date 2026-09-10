// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Authoritative wall-clock and deadline types shared by RadixDB crates.

pub use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Wall clock used by durable metadata, SQL temporal functions and MVCC's
/// monotonic timestamp admission. Runtime deadlines deliberately continue to
/// use [`Instant`], so a wall-clock correction cannot extend or expire a
/// resource lease.
#[inline]
pub fn system_time_now() -> SystemTime {
    #[cfg(any(test, feature = "test-failpoints"))]
    if let Some(now) = *TEST_WALL_CLOCK
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        return now;
    }
    SystemTime::now()
}

#[cfg(any(test, feature = "test-failpoints"))]
static TEST_WALL_CLOCK: std::sync::RwLock<Option<SystemTime>> = std::sync::RwLock::new(None);

#[cfg(any(test, feature = "test-failpoints"))]
static TEST_WALL_CLOCK_GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Process-wide, test-only wall clock override. The guard serializes clock
/// chaos and clears the override on unwind. Production artifacts do not expose
/// or compile this control surface.
#[cfg(any(test, feature = "test-failpoints"))]
pub struct TestWallClockGuard {
    _gate: std::sync::MutexGuard<'static, ()>,
}

#[cfg(any(test, feature = "test-failpoints"))]
impl TestWallClockGuard {
    pub fn install(now: SystemTime) -> Self {
        let gate = TEST_WALL_CLOCK_GATE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *TEST_WALL_CLOCK
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(now);
        Self { _gate: gate }
    }

    pub fn set(&self, now: SystemTime) {
        *TEST_WALL_CLOCK
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(now);
    }
}

#[cfg(any(test, feature = "test-failpoints"))]
impl Drop for TestWallClockGuard {
    fn drop(&mut self) {
        *TEST_WALL_CLOCK
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}
