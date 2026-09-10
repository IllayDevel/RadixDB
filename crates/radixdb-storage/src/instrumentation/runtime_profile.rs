// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Stable value contracts crossing instrumentation owner boundaries.
//!
//! Counter storage remains beside the implementation that records it until
//! the corresponding execution/storage owner moves. These enums are pure
//! descriptions and therefore move before the counters without introducing a
//! callback, global state or an upper-layer dependency.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Release-workload telemetry for physical owner boundaries that are otherwise
/// invisible in ordinary result counters.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct RuntimeProfileSnapshot {
    pub wait_calls: u64,
    pub wait_nanos: u64,
    pub ddl_shared_waits: u64,
    pub ddl_exclusive_waits: u64,
    pub visibility_shared_waits: u64,
    pub visibility_exclusive_waits: u64,
    pub seal_shared_waits: u64,
    pub membership_exclusive_waits: u64,
    pub checkpoint_mutex_waits: u64,
    pub hash_build_calls: u64,
    pub hash_build_rows: u64,
    pub hash_build_nanos: u64,
    pub index_lookup_calls: u64,
    pub index_lookup_keys: u64,
    pub index_lookup_hits: u64,
    pub index_lookup_nanos: u64,
    pub protocol_round_trips: u64,
    pub protocol_round_trip_nanos: u64,
}

impl RuntimeProfileSnapshot {
    pub fn delta(self, before: Self) -> Self {
        macro_rules! delta {
            ($field:ident) => {
                self.$field.saturating_sub(before.$field)
            };
        }
        Self {
            wait_calls: delta!(wait_calls),
            wait_nanos: delta!(wait_nanos),
            ddl_shared_waits: delta!(ddl_shared_waits),
            ddl_exclusive_waits: delta!(ddl_exclusive_waits),
            visibility_shared_waits: delta!(visibility_shared_waits),
            visibility_exclusive_waits: delta!(visibility_exclusive_waits),
            seal_shared_waits: delta!(seal_shared_waits),
            membership_exclusive_waits: delta!(membership_exclusive_waits),
            checkpoint_mutex_waits: delta!(checkpoint_mutex_waits),
            hash_build_calls: delta!(hash_build_calls),
            hash_build_rows: delta!(hash_build_rows),
            hash_build_nanos: delta!(hash_build_nanos),
            index_lookup_calls: delta!(index_lookup_calls),
            index_lookup_keys: delta!(index_lookup_keys),
            index_lookup_hits: delta!(index_lookup_hits),
            index_lookup_nanos: delta!(index_lookup_nanos),
            protocol_round_trips: delta!(protocol_round_trips),
            protocol_round_trip_nanos: delta!(protocol_round_trip_nanos),
        }
    }
}

struct RuntimeProfileCounters {
    wait_calls: AtomicU64,
    wait_nanos: AtomicU64,
    ddl_shared_waits: AtomicU64,
    ddl_exclusive_waits: AtomicU64,
    visibility_shared_waits: AtomicU64,
    visibility_exclusive_waits: AtomicU64,
    seal_shared_waits: AtomicU64,
    membership_exclusive_waits: AtomicU64,
    checkpoint_mutex_waits: AtomicU64,
    hash_build_calls: AtomicU64,
    hash_build_rows: AtomicU64,
    hash_build_nanos: AtomicU64,
    index_lookup_calls: AtomicU64,
    index_lookup_keys: AtomicU64,
    index_lookup_hits: AtomicU64,
    index_lookup_nanos: AtomicU64,
    protocol_round_trips: AtomicU64,
    protocol_round_trip_nanos: AtomicU64,
}

impl RuntimeProfileCounters {
    const fn new() -> Self {
        Self {
            wait_calls: AtomicU64::new(0),
            wait_nanos: AtomicU64::new(0),
            ddl_shared_waits: AtomicU64::new(0),
            ddl_exclusive_waits: AtomicU64::new(0),
            visibility_shared_waits: AtomicU64::new(0),
            visibility_exclusive_waits: AtomicU64::new(0),
            seal_shared_waits: AtomicU64::new(0),
            membership_exclusive_waits: AtomicU64::new(0),
            checkpoint_mutex_waits: AtomicU64::new(0),
            hash_build_calls: AtomicU64::new(0),
            hash_build_rows: AtomicU64::new(0),
            hash_build_nanos: AtomicU64::new(0),
            index_lookup_calls: AtomicU64::new(0),
            index_lookup_keys: AtomicU64::new(0),
            index_lookup_hits: AtomicU64::new(0),
            index_lookup_nanos: AtomicU64::new(0),
            protocol_round_trips: AtomicU64::new(0),
            protocol_round_trip_nanos: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.wait_calls.store(0, Ordering::Relaxed);
        self.wait_nanos.store(0, Ordering::Relaxed);
        self.ddl_shared_waits.store(0, Ordering::Relaxed);
        self.ddl_exclusive_waits.store(0, Ordering::Relaxed);
        self.visibility_shared_waits.store(0, Ordering::Relaxed);
        self.visibility_exclusive_waits.store(0, Ordering::Relaxed);
        self.seal_shared_waits.store(0, Ordering::Relaxed);
        self.membership_exclusive_waits.store(0, Ordering::Relaxed);
        self.checkpoint_mutex_waits.store(0, Ordering::Relaxed);
        self.hash_build_calls.store(0, Ordering::Relaxed);
        self.hash_build_rows.store(0, Ordering::Relaxed);
        self.hash_build_nanos.store(0, Ordering::Relaxed);
        self.index_lookup_calls.store(0, Ordering::Relaxed);
        self.index_lookup_keys.store(0, Ordering::Relaxed);
        self.index_lookup_hits.store(0, Ordering::Relaxed);
        self.index_lookup_nanos.store(0, Ordering::Relaxed);
        self.protocol_round_trips.store(0, Ordering::Relaxed);
        self.protocol_round_trip_nanos.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> RuntimeProfileSnapshot {
        RuntimeProfileSnapshot {
            wait_calls: self.wait_calls.load(Ordering::Relaxed),
            wait_nanos: self.wait_nanos.load(Ordering::Relaxed),
            ddl_shared_waits: self.ddl_shared_waits.load(Ordering::Relaxed),
            ddl_exclusive_waits: self.ddl_exclusive_waits.load(Ordering::Relaxed),
            visibility_shared_waits: self.visibility_shared_waits.load(Ordering::Relaxed),
            visibility_exclusive_waits: self.visibility_exclusive_waits.load(Ordering::Relaxed),
            seal_shared_waits: self.seal_shared_waits.load(Ordering::Relaxed),
            membership_exclusive_waits: self.membership_exclusive_waits.load(Ordering::Relaxed),
            checkpoint_mutex_waits: self.checkpoint_mutex_waits.load(Ordering::Relaxed),
            hash_build_calls: self.hash_build_calls.load(Ordering::Relaxed),
            hash_build_rows: self.hash_build_rows.load(Ordering::Relaxed),
            hash_build_nanos: self.hash_build_nanos.load(Ordering::Relaxed),
            index_lookup_calls: self.index_lookup_calls.load(Ordering::Relaxed),
            index_lookup_keys: self.index_lookup_keys.load(Ordering::Relaxed),
            index_lookup_hits: self.index_lookup_hits.load(Ordering::Relaxed),
            index_lookup_nanos: self.index_lookup_nanos.load(Ordering::Relaxed),
            protocol_round_trips: self.protocol_round_trips.load(Ordering::Relaxed),
            protocol_round_trip_nanos: self.protocol_round_trip_nanos.load(Ordering::Relaxed),
        }
    }
}

static RUNTIME_PROFILE: RuntimeProfileCounters = RuntimeProfileCounters::new();

pub fn reset_runtime_profile() {
    RUNTIME_PROFILE.reset();
}

pub fn runtime_profile_snapshot() -> RuntimeProfileSnapshot {
    RUNTIME_PROFILE.snapshot()
}

/// Storage/runtime wait boundary observed by the current counter owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeWaitKind {
    DdlShared,
    DdlExclusive,
    VisibilityShared,
    VisibilityExclusive,
    SealShared,
    MembershipExclusive,
    CheckpointMutex,
}

/// Why the metadata-only INTEGER primary-key count declined to answer a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataPkCountFallback {
    Snapshot,
    SealOverlap,
    Unsupported,
    CandidateLimit,
}

/// Coarse reason bucket for a typed column batch falling back to row transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolColumnBatchFallback {
    RowState,
    QueryShape,
    StorageShape,
    Schema,
    Unknown,
}

#[inline]
pub fn record_runtime_wait(kind: RuntimeWaitKind, elapsed: Duration) {
    #[cfg(feature = "bench-harness")]
    {
        add(&RUNTIME_PROFILE.wait_calls, 1);
        add(&RUNTIME_PROFILE.wait_nanos, duration_nanos(elapsed));
        let counter = match kind {
            RuntimeWaitKind::DdlShared => &RUNTIME_PROFILE.ddl_shared_waits,
            RuntimeWaitKind::DdlExclusive => &RUNTIME_PROFILE.ddl_exclusive_waits,
            RuntimeWaitKind::VisibilityShared => &RUNTIME_PROFILE.visibility_shared_waits,
            RuntimeWaitKind::VisibilityExclusive => &RUNTIME_PROFILE.visibility_exclusive_waits,
            RuntimeWaitKind::SealShared => &RUNTIME_PROFILE.seal_shared_waits,
            RuntimeWaitKind::MembershipExclusive => &RUNTIME_PROFILE.membership_exclusive_waits,
            RuntimeWaitKind::CheckpointMutex => &RUNTIME_PROFILE.checkpoint_mutex_waits,
        };
        add(counter, 1);
    }
    #[cfg(not(feature = "bench-harness"))]
    let _ = (kind, elapsed);
}

#[inline]
pub fn record_hash_build(rows: usize, elapsed: Duration) {
    #[cfg(feature = "bench-harness")]
    {
        add(&RUNTIME_PROFILE.hash_build_calls, 1);
        add(&RUNTIME_PROFILE.hash_build_rows, rows as u64);
        add(&RUNTIME_PROFILE.hash_build_nanos, duration_nanos(elapsed));
    }
    #[cfg(not(feature = "bench-harness"))]
    let _ = (rows, elapsed);
}

#[inline]
pub fn record_index_lookup(keys: usize, hits: usize, elapsed: Duration) {
    #[cfg(feature = "bench-harness")]
    {
        add(&RUNTIME_PROFILE.index_lookup_calls, 1);
        add(&RUNTIME_PROFILE.index_lookup_keys, keys as u64);
        add(&RUNTIME_PROFILE.index_lookup_hits, hits as u64);
        add(&RUNTIME_PROFILE.index_lookup_nanos, duration_nanos(elapsed));
    }
    #[cfg(not(feature = "bench-harness"))]
    let _ = (keys, hits, elapsed);
}

#[inline]
pub fn record_protocol_round_trip(elapsed: Duration) {
    #[cfg(feature = "bench-harness")]
    {
        add(&RUNTIME_PROFILE.protocol_round_trips, 1);
        add(
            &RUNTIME_PROFILE.protocol_round_trip_nanos,
            duration_nanos(elapsed),
        );
    }
    #[cfg(not(feature = "bench-harness"))]
    let _ = elapsed;
}

#[inline]
#[cfg(feature = "bench-harness")]
fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

#[inline]
#[cfg(feature = "bench-harness")]
fn duration_nanos(elapsed: Duration) -> u64 {
    elapsed.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_profile_delta_saturates_each_counter() {
        let before = RuntimeProfileSnapshot {
            index_lookup_calls: 7,
            index_lookup_keys: 11,
            ..RuntimeProfileSnapshot::default()
        };
        let after = RuntimeProfileSnapshot {
            index_lookup_calls: 9,
            index_lookup_keys: 5,
            ..RuntimeProfileSnapshot::default()
        };
        let delta = after.delta(before);
        assert_eq!(delta.index_lookup_calls, 2);
        assert_eq!(delta.index_lookup_keys, 0);
    }
}
