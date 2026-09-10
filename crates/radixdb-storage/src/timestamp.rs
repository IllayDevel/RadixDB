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

//! Fast timestamp generation for MVCC
//!
//! Provides monotonically increasing timestamps suitable for transaction
//! ordering and version tracking, even under heavy concurrent usage.
//!

use std::sync::atomic::{AtomicI64, Ordering};

use radixdb_core::time_compat::{system_time_now, UNIX_EPOCH};

/// Global state for timestamp generation - tracks last issued timestamp
static LAST_TIMESTAMP: AtomicI64 = AtomicI64::new(0);

/// Durable values above this boundary are rejected so recovery can never leave
/// the infallible process-local allocator one or two calls away from overflow.
/// The reserved 2^60-nanosecond tail is more than thirty years of allocations
/// at one timestamp per nanosecond.
const MAX_DURABLE_TIMESTAMP: i64 = i64::MAX - (1i64 << 60);

#[inline]
fn persisted_timestamp_is_admissible(timestamp: i64) -> bool {
    timestamp <= MAX_DURABLE_TIMESTAMP
}

/// Advances the process timestamp owner to a value observed in validated
/// durable state. Returns false when no strictly greater i64 timestamp can be
/// represented.
pub fn observe_persisted_timestamp(timestamp: i64) -> bool {
    if timestamp <= 0 {
        return true;
    }
    if !persisted_timestamp_is_admissible(timestamp) {
        return false;
    }
    LAST_TIMESTAMP.fetch_max(timestamp, Ordering::AcqRel);
    true
}

/// Returns the greatest timestamp admitted by this process. Checkpoint
/// metadata persists this value before retiring the WAL/RowVersion history
/// that originally carried it.
pub fn timestamp_high_water() -> i64 {
    LAST_TIMESTAMP.load(Ordering::Acquire)
}

/// Returns a monotonically increasing timestamp suitable for transaction
/// ordering and version tracking.
///
/// This function guarantees:
/// - Timestamps are always increasing (never go backwards)
/// - Unique timestamps even under heavy concurrent usage
/// - Nanosecond precision when system clock allows
///
/// # Implementation
///
/// Uses the system clock as the base, but handles:
/// - Clock skew (system time going backwards)
/// - Concurrent access (multiple threads getting timestamps simultaneously)
///
/// Uses atomic compare-and-swap to ensure each caller gets a unique,
/// monotonically increasing timestamp.
///
/// # Example
///
/// ```
/// use radixdb_storage::timestamp::get_fast_timestamp;
///
/// let ts1 = get_fast_timestamp();
/// let ts2 = get_fast_timestamp();
/// assert!(ts2 > ts1);
/// ```
pub fn get_fast_timestamp() -> i64 {
    // Get current time in nanoseconds
    let now_nano = system_time_now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(1); // Use 1 as minimum to ensure positive timestamps

    loop {
        // Load current last timestamp
        let last_ts = LAST_TIMESTAMP.load(Ordering::Acquire);

        // Calculate next timestamp: max(now, last + 1)
        // This ensures monotonicity even if clock goes backwards
        let next_ts = if now_nano > last_ts {
            now_nano
        } else {
            last_ts
                .checked_add(1)
                .expect("MVCC timestamp domain exhausted")
        };

        // Try to update using CAS
        if LAST_TIMESTAMP
            .compare_exchange(last_ts, next_ts, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return next_ts;
        }
        // If CAS failed, another thread updated the timestamp, so we loop and try again
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustc_hash::FxHashSet;
    use std::thread;

    #[test]
    fn r2_l05_a_persisted_timestamp_seeds_process_high_water() {
        let durable_timestamp = get_fast_timestamp() + 1_000_000_000;
        assert!(observe_persisted_timestamp(durable_timestamp));
        assert!(
            get_fast_timestamp() > durable_timestamp,
            "new process timestamps must continue the durable high-water"
        );
    }

    #[test]
    fn v2_r2_durable_timestamp_reserves_allocator_headroom() {
        assert!(persisted_timestamp_is_admissible(MAX_DURABLE_TIMESTAMP));
        assert!(!persisted_timestamp_is_admissible(
            MAX_DURABLE_TIMESTAMP + 1
        ));
        assert!(!persisted_timestamp_is_admissible(i64::MAX - 1));
        assert!(!persisted_timestamp_is_admissible(i64::MAX));
    }

    #[test]
    fn test_timestamp_monotonic() {
        // Test that timestamps are strictly increasing within a single thread
        let mut prev = get_fast_timestamp();
        for _ in 0..1000 {
            let ts = get_fast_timestamp();
            assert!(
                ts > prev,
                "Timestamp not strictly increasing: {} <= {}",
                ts,
                prev
            );
            prev = ts;
        }
    }

    #[test]
    fn test_timestamp_unique() {
        // Test that all timestamps in sequence are unique
        let mut timestamps = FxHashSet::default();
        for _ in 0..10000 {
            let ts = get_fast_timestamp();
            assert!(
                timestamps.insert(ts),
                "Duplicate timestamp detected: {}",
                ts
            );
        }
    }

    #[test]
    fn test_timestamp_concurrent() {
        // Test concurrent timestamp generation across multiple threads
        let handles: Vec<_> = (0..4)
            .map(|_| {
                thread::spawn(|| {
                    let mut timestamps = Vec::with_capacity(1000);
                    for _ in 0..1000 {
                        timestamps.push(get_fast_timestamp());
                    }
                    timestamps
                })
            })
            .collect();

        let mut all_timestamps: FxHashSet<i64> = FxHashSet::default();
        for handle in handles {
            let timestamps = handle.join().unwrap();
            for ts in timestamps {
                all_timestamps.insert(ts);
            }
        }

        // With the CAS-based algorithm, all timestamps should be unique
        // across all threads (4 threads * 1000 timestamps = 4000 total)
        assert_eq!(
            all_timestamps.len(),
            4000,
            "Expected all 4000 timestamps to be unique, got {}",
            all_timestamps.len()
        );
    }

    #[test]
    fn test_timestamp_positive() {
        for _ in 0..100 {
            let ts = get_fast_timestamp();
            assert!(ts > 0, "Timestamp should be positive: {}", ts);
        }
    }
}
