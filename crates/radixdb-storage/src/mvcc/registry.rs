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

//! Transaction registry for MVCC visibility.
//!
//! Optimized design with minimal memory footprint:
//! - Active/Committing/Aborted transactions: tracked in single map
//! - Committed transactions: implicit (not in map = committed)
//! - Single lock acquisition in hot path
//!
//! Memory: O(active_transactions + aborted_transactions)

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use radixdb_core::{Error, I64Map, IsolationLevel, Result};

use super::version_store::VisibilityChecker;
use super::DDL_TXN_ID;
use crate::timestamp::get_fast_timestamp;

/// Invalid transaction ID returned when registry is not accepting new transactions.
pub const INVALID_TRANSACTION_ID: i64 = -999999999;

/// Special transaction ID for recovery transactions (always visible).
pub const RECOVERY_TRANSACTION_ID: i64 = -1;

/// Sentinel value for aborted transactions (negative begin_seq).
const ABORTED_SENTINEL: i64 = -1;

/// Durable engine-owned transaction identities occupy the negative domain
/// below the snapshot/recovery sentinel. The fixed DDL identity uses -2 and
/// current auto-DDL allocation continues downward from -3.
#[inline]
const fn is_internal_durable_transaction_id(txn_id: i64) -> bool {
    txn_id <= DDL_TXN_ID
}

/// Transaction status.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum TxnStatus {
    /// Transaction is in progress
    Active = 0,
    /// Two-phase commit in progress
    Committing = 1,
    /// Transaction was aborted
    Aborted = 2,
}

/// Packed transaction state plus its diagnostic start timestamp (24 bytes).
///
/// - For Active/Committing: begin_seq > 0, state_seq encodes status + commit_seq
/// - For Aborted: begin_seq = ABORTED_SENTINEL (-1)
#[derive(Clone, Copy, Debug)]
pub struct TxnState {
    /// Sequence when transaction began (negative = aborted sentinel)
    begin_seq: i64,
    /// Packed: lower 62 bits = commit_seq, upper 2 bits = status
    state_seq: i64,
    /// Process-monotonic, wall-anchored nanosecond timestamp used only for
    /// bounded transaction-age diagnostics.
    begin_timestamp_nanos: i64,
}

const STATUS_SHIFT: u32 = 62;
const SEQ_MASK: i64 = (1i64 << STATUS_SHIFT) - 1;

impl TxnState {
    /// Creates a new active transaction state.
    #[inline]
    fn new_active(begin_seq: i64) -> Self {
        Self {
            begin_seq,
            state_seq: 0, // Active=0, commit_seq=0
            begin_timestamp_nanos: get_fast_timestamp(),
        }
    }

    /// Creates an aborted transaction marker.
    #[inline]
    const fn new_aborted() -> Self {
        Self {
            begin_seq: ABORTED_SENTINEL,
            state_seq: (TxnStatus::Aborted as i64) << STATUS_SHIFT,
            begin_timestamp_nanos: 0,
        }
    }

    /// Returns true if this is an aborted transaction.
    #[inline(always)]
    pub const fn is_aborted(&self) -> bool {
        self.begin_seq == ABORTED_SENTINEL
    }

    /// Returns the begin sequence (0 if aborted).
    #[inline(always)]
    pub const fn begin_seq(&self) -> i64 {
        if self.begin_seq == ABORTED_SENTINEL {
            0
        } else {
            self.begin_seq
        }
    }

    /// Returns the transaction status.
    #[inline(always)]
    pub const fn status(&self) -> TxnStatus {
        if self.begin_seq == ABORTED_SENTINEL {
            return TxnStatus::Aborted;
        }
        match (self.state_seq >> STATUS_SHIFT) as u8 {
            0 => TxnStatus::Active,
            1 => TxnStatus::Committing,
            _ => TxnStatus::Aborted,
        }
    }

    /// Returns true if Active or Committing (not aborted).
    #[inline(always)]
    pub const fn is_active_or_committing(&self) -> bool {
        self.begin_seq != ABORTED_SENTINEL
    }

    /// Sets status to Committing with given commit_seq.
    #[inline(always)]
    fn set_committing(&mut self, commit_seq: i64) {
        self.state_seq = commit_seq | (1i64 << STATUS_SHIFT);
    }

    /// Returns the commit sequence (0 if not committing).
    #[inline(always)]
    pub const fn commit_seq(&self) -> i64 {
        self.state_seq & SEQ_MASK
    }

    #[inline(always)]
    pub const fn begin_timestamp_nanos(&self) -> i64 {
        self.begin_timestamp_nanos
    }
}

/// Thread-local direct-map cache size (32 KiB per thread).
const CACHE_SIZE: usize = 4096;

/// Cache index shift for XOR mixing (log2 of CACHE_SIZE).
/// XOR mixing prevents 0% hit rate when txn_ids are strided by CACHE_SIZE.
const CACHE_SHIFT: u32 = CACHE_SIZE.trailing_zeros();

/// Monotonic process-local identity used only to scope the TLS visibility
/// cache. Transaction IDs are registry-local, so caching an ID without this
/// owner identity can leak a committed outcome between databases.
static NEXT_REGISTRY_EPOCH: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
static R3_L01_SNAPSHOT_REGISTRY_LOCKS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static COMMITTED_CACHE: RefCell<CommittedCache> = const { RefCell::new(CommittedCache::new()) };
}

/// Direct-mapped cache for committed transaction IDs.
struct CommittedCache {
    registry_epoch: u64,
    entries: [i64; CACHE_SIZE],
    occupied_indices: Vec<usize>,
    snapshot_viewer: i64,
    snapshot_boundary: Option<SnapshotBoundary>,
}

impl CommittedCache {
    #[inline]
    const fn new() -> Self {
        Self {
            registry_epoch: 0,
            entries: [0; CACHE_SIZE],
            occupied_indices: Vec::new(),
            snapshot_viewer: 0,
            snapshot_boundary: None,
        }
    }

    /// Compute cache index with XOR mixing to avoid collisions on strided IDs.
    #[inline(always)]
    fn cache_index(txn_id: i64) -> usize {
        let x = txn_id as u64;
        ((x ^ (x >> CACHE_SHIFT)) as usize) & (CACHE_SIZE - 1)
    }

    #[inline(always)]
    fn contains(&self, registry_epoch: u64, txn_id: i64) -> bool {
        if self.registry_epoch != registry_epoch {
            return false;
        }
        let idx = Self::cache_index(txn_id);
        self.entries[idx] == txn_id
    }

    #[inline(always)]
    fn insert(&mut self, registry_epoch: u64, txn_id: i64) {
        if self.registry_epoch != registry_epoch {
            for idx in self.occupied_indices.drain(..) {
                self.entries[idx] = 0;
            }
            self.registry_epoch = registry_epoch;
            self.snapshot_viewer = 0;
            self.snapshot_boundary = None;
        }
        let idx = Self::cache_index(txn_id);
        if self.entries[idx] == 0 {
            self.occupied_indices.push(idx);
        }
        self.entries[idx] = txn_id;
    }

    #[inline]
    fn snapshot_visibility(
        &self,
        registry_epoch: u64,
        version_txn_id: i64,
        viewer_txn_id: i64,
    ) -> Option<bool> {
        if self.registry_epoch != registry_epoch || self.snapshot_viewer != viewer_txn_id {
            return None;
        }
        let boundary = self.snapshot_boundary.as_ref()?;
        if !boundary.active.load(Ordering::Acquire) {
            return None;
        }
        Some(boundary.is_visible(version_txn_id))
    }

    #[inline]
    fn install_snapshot(
        &mut self,
        registry_epoch: u64,
        viewer_txn_id: i64,
        boundary: SnapshotBoundary,
    ) {
        if self.registry_epoch != registry_epoch {
            for idx in self.occupied_indices.drain(..) {
                self.entries[idx] = 0;
            }
            self.registry_epoch = registry_epoch;
        }
        self.snapshot_viewer = viewer_txn_id;
        self.snapshot_boundary = Some(boundary);
    }
}

/// Immutable visibility boundary captured atomically at Snapshot begin.
#[derive(Clone)]
struct SnapshotBoundary {
    max_txn_id: i64,
    excluded: Arc<[i64]>,
    active: Arc<AtomicBool>,
}

impl SnapshotBoundary {
    #[inline]
    fn is_visible(&self, version_txn_id: i64) -> bool {
        version_txn_id > 0
            && version_txn_id <= self.max_txn_id
            && self.excluded.binary_search(&version_txn_id).is_err()
    }
}

/// Transaction registry with minimal memory footprint.
///
/// Design:
/// - `transactions`: Single map for Active/Committing/Aborted
/// - If txn_id not in map and valid → committed (implicit)
/// - `snapshot_seqs`: Commit sequences for snapshot isolation
///
/// Memory: O(active + aborted) instead of O(total)
pub struct TransactionRegistry {
    /// Exact owner identity for thread-local committed-cache entries.
    cache_epoch: u64,

    /// All tracked transactions (Active, Committing, Aborted).
    /// Committed transactions are REMOVED from this map.
    transactions: Mutex<I64Map<TxnState>>,

    /// Linearizes transaction admission with shutdown/restore drain.
    ///
    /// A begin holds this gate until its ID, snapshot boundary, registry entry
    /// and active-count publication are all visible. Stopping admission takes
    /// the same gate, so a subsequent zero count is a complete drain frontier.
    admission: Mutex<()>,

    /// For SNAPSHOT ISOLATION: txn_id -> commit_seq.
    /// GC removes old entries when commit_seq < min_active_begin_seq.
    snapshot_seqs: Mutex<I64Map<i64>>,

    /// Transactions that had already reserved a commit sequence but had not
    /// completed publication when a viewer began. A scalar sequence alone
    /// cannot represent this out-of-order completion hole.
    snapshot_exclusions: Mutex<I64Map<SnapshotBoundary>>,

    /// Completion timestamp for committed/aborted registry metadata.
    terminal_times: Mutex<I64Map<i64>>,

    /// Last assigned transaction ID (after begin_transaction, equals txn_id).
    next_txn_id: AtomicI64,

    /// Last assigned sequence number (after begin/commit, equals that seq).
    next_sequence: AtomicI64,

    /// Global isolation level (0 = ReadCommitted, 1 = SnapshotIsolation).
    global_isolation_level: AtomicU8,

    /// Per-transaction isolation level overrides.
    isolation_overrides: Mutex<I64Map<u8>>,

    /// Count of active isolation overrides (skip lookup when 0).
    override_count: AtomicUsize,

    /// Count of active transactions (O(1) lookup).
    active_txn_count: AtomicUsize,

    /// Whether new transactions are being accepted.
    accepting: AtomicBool,

    /// One outgoing wait-for edge per blocked transaction. Row ownership is
    /// stored by VersionStore; this graph only detects cycles across tables.
    waits_for: Mutex<I64Map<i64>>,
}

/// Non-blocking transaction ownership summary for runtime diagnostics.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct TransactionRuntimeSnapshot {
    pub active: u64,
    pub accepting: bool,
    pub oldest_begin_sequence: Option<i64>,
    pub oldest_begin_timestamp_nanos: Option<i64>,
    pub wait_edges: Option<u64>,
    pub scanned: u64,
    pub truncated: bool,
    pub registry_busy: bool,
}

impl TransactionRegistry {
    /// Creates a new transaction registry.
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    /// Creates a new transaction registry with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let cache_epoch = NEXT_REGISTRY_EPOCH.fetch_add(1, Ordering::AcqRel);
        assert!(cache_epoch != 0, "transaction registry epoch exhausted");
        Self {
            cache_epoch,
            transactions: Mutex::new(I64Map::with_capacity(capacity)),
            admission: Mutex::new(()),
            snapshot_seqs: Mutex::new(I64Map::new()),
            snapshot_exclusions: Mutex::new(I64Map::new()),
            terminal_times: Mutex::new(I64Map::new()),
            next_txn_id: AtomicI64::new(0),
            next_sequence: AtomicI64::new(0),
            global_isolation_level: AtomicU8::new(0),
            isolation_overrides: Mutex::new(I64Map::new()),
            override_count: AtomicUsize::new(0),
            active_txn_count: AtomicUsize::new(0),
            accepting: AtomicBool::new(true),
            waits_for: Mutex::new(I64Map::new()),
        }
    }

    #[inline(always)]
    const fn isolation_to_u8(level: IsolationLevel) -> u8 {
        match level {
            IsolationLevel::ReadCommitted => 0,
            IsolationLevel::SnapshotIsolation => 1,
        }
    }

    #[inline(always)]
    const fn u8_to_isolation(value: u8) -> IsolationLevel {
        match value {
            0 => IsolationLevel::ReadCommitted,
            _ => IsolationLevel::SnapshotIsolation,
        }
    }

    /// Sets the global isolation level.
    pub fn set_global_isolation_level(&self, level: IsolationLevel) {
        self.global_isolation_level
            .store(Self::isolation_to_u8(level), Ordering::Release);
    }

    /// Gets the current global isolation level.
    #[inline(always)]
    pub fn get_global_isolation_level(&self) -> IsolationLevel {
        Self::u8_to_isolation(self.global_isolation_level.load(Ordering::Acquire))
    }

    /// Confirms an active transaction's immutable isolation level.
    ///
    /// Isolation is selected by `begin_transaction_with_isolation`; changing
    /// it afterwards would move the snapshot linearization point.
    pub fn set_transaction_isolation_level(&self, txn_id: i64, level: IsolationLevel) -> bool {
        self.isolation_overrides
            .lock()
            .get(txn_id)
            .is_some_and(|stored| *stored == Self::isolation_to_u8(level))
    }

    /// Removes the isolation level override for a transaction.
    pub fn remove_transaction_isolation_level(&self, txn_id: i64) {
        if self.isolation_overrides.lock().remove(txn_id).is_some() {
            self.override_count.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(boundary) = self.snapshot_exclusions.lock().remove(txn_id) {
            boundary.active.store(false, Ordering::Release);
        }
    }

    /// Gets the isolation level for a specific transaction.
    #[inline(always)]
    pub fn get_isolation_level(&self, txn_id: i64) -> IsolationLevel {
        if self.override_count.load(Ordering::Relaxed) > 0 {
            if let Some(&level) = self.isolation_overrides.lock().get(txn_id) {
                return Self::u8_to_isolation(level);
            }
        }
        self.get_global_isolation_level()
    }

    /// Checks if snapshot isolation is needed for a transaction.
    #[inline(always)]
    fn needs_snapshot_isolation(&self, txn_id: i64) -> bool {
        if self.override_count.load(Ordering::Relaxed) > 0 {
            if let Some(&level) = self.isolation_overrides.lock().get(txn_id) {
                return level == 1;
            }
        }
        self.global_isolation_level.load(Ordering::Relaxed) == 1
    }

    /// Get the minimum begin_seq among active snapshot isolation transactions.
    /// Returns None if no snapshot transactions are active (seal/compaction can proceed freely).
    /// Returns Some(min_begin_seq) otherwise — only rows committed before this seq are safe to seal.
    pub fn get_min_snapshot_begin_seq(&self) -> Option<i64> {
        let transactions = self.transactions.lock();
        let levels = self.isolation_overrides.lock();
        transactions
            .iter()
            .filter(|(id, state)| state.is_active_or_committing() && levels.get(*id) == Some(&1))
            .map(|(_, state)| state.begin_seq())
            .min()
    }

    /// Begins a new transaction.
    #[doc(hidden)]
    pub fn begin_transaction(&self) -> (i64, i64) {
        self.begin_transaction_with_isolation(self.get_global_isolation_level())
    }

    /// Atomically begins a transaction with its immutable initial isolation.
    pub fn begin_transaction_with_isolation(&self, level: IsolationLevel) -> (i64, i64) {
        let _admission = self.admission.lock();
        if !self.accepting.load(Ordering::Acquire) {
            return (INVALID_TRANSACTION_ID, 0);
        }

        let mut transactions = self.transactions.lock();
        let Some(txn_id) = self
            .next_txn_id
            .load(Ordering::Acquire)
            .checked_add(1)
            .filter(|next| *next < i64::MAX)
        else {
            return (INVALID_TRANSACTION_ID, 0);
        };
        let Some(begin_seq) = self
            .next_sequence
            .load(Ordering::Acquire)
            .checked_add(1)
            .filter(|next| *next <= SEQ_MASK)
        else {
            return (INVALID_TRANSACTION_ID, 0);
        };

        // ID and sequence publication happen while holding the same lock used
        // by snapshot capture and commit publication. There is no allocated but
        // unregistered identity for a concurrent snapshot to miss.
        self.next_txn_id.store(txn_id, Ordering::Release);
        self.next_sequence.store(begin_seq, Ordering::Release);
        let excluded: Vec<i64> = if level == IsolationLevel::SnapshotIsolation {
            let mut ids: Vec<i64> = transactions.iter().map(|(id, _)| id).collect();
            ids.sort_unstable();
            ids
        } else {
            Vec::new()
        };
        transactions.insert(txn_id, TxnState::new_active(begin_seq));
        // Publish the immutable isolation and snapshot holes before releasing
        // the transaction lock. Readers of the snapshot frontier acquire the
        // same locks in this order, so they cannot observe a registered SI
        // transaction without its retention boundary.
        let mut levels = self.isolation_overrides.lock();
        levels.insert(txn_id, Self::isolation_to_u8(level));
        if level == IsolationLevel::SnapshotIsolation {
            self.snapshot_exclusions.lock().insert(
                txn_id,
                SnapshotBoundary {
                    max_txn_id: txn_id,
                    excluded: excluded.into(),
                    active: Arc::new(AtomicBool::new(true)),
                },
            );
        }
        self.override_count.fetch_add(1, Ordering::Relaxed);
        self.active_txn_count.fetch_add(1, Ordering::Release);
        drop(levels);
        drop(transactions);

        (txn_id, begin_seq)
    }

    /// Starts the commit process (two-phase commit).
    #[inline]
    pub fn start_commit(&self, txn_id: i64) -> Result<i64> {
        // Hold `transactions` while allocating and publishing the committing
        // state. A new snapshot therefore cannot observe the sequence advance
        // without also seeing this transaction as in-flight.
        let mut transactions = self.transactions.lock();
        let entry =
            transactions
                .get_mut(txn_id)
                .ok_or_else(|| Error::InvalidTransactionTransition {
                    txn_id,
                    expected: "Active".to_string(),
                    actual: "missing".to_string(),
                })?;
        if entry.status() != TxnStatus::Active {
            return Err(Error::InvalidTransactionTransition {
                txn_id,
                expected: "Active".to_string(),
                actual: format!("{:?}", entry.status()),
            });
        }
        let commit_seq = self
            .next_sequence
            .load(Ordering::Acquire)
            .checked_add(1)
            .filter(|next| *next <= SEQ_MASK)
            .ok_or_else(|| Error::internal("MVCC commit sequence domain exhausted"))?;
        self.next_sequence.store(commit_seq, Ordering::Release);
        entry.set_committing(commit_seq);
        Ok(commit_seq)
    }

    /// Completes the commit process (two-phase commit).
    #[inline]
    #[doc(hidden)]
    pub fn complete_commit(&self, txn_id: i64) -> Result<()> {
        self.complete_commit_inner(txn_id, false).map(|_| ())
    }

    /// Complete a commit and atomically reserve the visibility point used by
    /// storage state that can only be published after registry completion.
    ///
    /// Snapshot begin holds the same transaction lock. A snapshot therefore
    /// either excludes the committing transaction and precedes this storage
    /// publication, or includes both the committed transaction and the later
    /// storage state. It cannot land between those two logical outcomes.
    pub fn complete_commit_with_storage_publication(&self, txn_id: i64) -> Result<i64> {
        self.complete_commit_inner(txn_id, true)
    }

    fn complete_commit_inner(&self, txn_id: i64, reserve_storage_seq: bool) -> Result<i64> {
        let storage_seq;
        {
            let mut txns = self.transactions.lock();
            let state =
                txns.get(txn_id)
                    .copied()
                    .ok_or_else(|| Error::InvalidTransactionTransition {
                        txn_id,
                        expected: "Committing".to_string(),
                        actual: "missing".to_string(),
                    })?;
            if state.status() != TxnStatus::Committing || state.commit_seq() <= 0 {
                return Err(Error::InvalidTransactionTransition {
                    txn_id,
                    expected: "Committing".to_string(),
                    actual: format!("{:?}", state.status()),
                });
            }
            let seq = state.commit_seq();
            storage_seq = if reserve_storage_seq {
                let next = self
                    .next_sequence
                    .load(Ordering::Acquire)
                    .checked_add(1)
                    .filter(|next| *next <= SEQ_MASK)
                    .ok_or_else(|| {
                        Error::internal("MVCC storage publication sequence domain exhausted")
                    })?;
                self.next_sequence.store(next, Ordering::Release);
                next
            } else {
                seq
            };

            // Always retain the exact publication sequence. Whether a future
            // viewer needs snapshot semantics is decided at that viewer's
            // atomic begin, not by a mutable global mode at writer commit.
            self.snapshot_seqs.lock().insert(txn_id, seq);
            self.terminal_times
                .lock()
                .insert(txn_id, get_fast_timestamp());

            txns.remove(txn_id);
        };
        self.active_txn_count.fetch_sub(1, Ordering::AcqRel);
        self.remove_transaction_isolation_level(txn_id);
        Ok(storage_seq)
    }

    /// Aborts a transaction.
    #[inline]
    pub fn abort_transaction(&self, txn_id: i64) {
        // Replace with aborted marker (don't remove - need to track for visibility)
        // Only decrement counter if transaction was actually active/committing
        //
        // CRITICAL: We must check if the transaction exists before inserting the aborted marker.
        // If it's not in the map, it's already committed (or invalid), and we must NOT
        // resurrect it as an aborted transaction.
        let should_decrement = {
            let mut txns = self.transactions.lock();
            if let Some(state) = txns.get_mut(txn_id) {
                let was_active = state.is_active_or_committing();
                *state = TxnState::new_aborted();
                was_active
            } else {
                false
            }
        };

        if should_decrement {
            self.terminal_times
                .lock()
                .insert(txn_id, get_fast_timestamp());
            self.active_txn_count.fetch_sub(1, Ordering::AcqRel);
            self.remove_transaction_isolation_level(txn_id);
        }
    }

    /// Recovers a committed transaction during startup recovery.
    pub fn recover_committed_transaction(&self, txn_id: i64, commit_seq: i64) -> Result<()> {
        if !is_internal_durable_transaction_id(txn_id) && (txn_id <= 0 || txn_id == i64::MAX) {
            return Err(Error::internal(format!(
                "recovered transaction ID {txn_id} is outside the positive MVCC domain"
            )));
        }
        if !(1..=SEQ_MASK).contains(&commit_seq) {
            return Err(Error::internal(format!(
                "recovered commit sequence {commit_seq} exceeds the packed MVCC domain"
            )));
        }
        let _admission = self.admission.lock();
        let _transactions = self.transactions.lock();

        // Durable internal DDL units own real WAL ordering points but never
        // participate in user visibility or positive transaction-ID admission.
        if is_internal_durable_transaction_id(txn_id) {
            if commit_seq > self.next_sequence.load(Ordering::Acquire) {
                self.next_sequence.store(commit_seq, Ordering::Release);
            }
            return Ok(());
        }

        // Store in snapshot_seqs for visibility checks
        self.snapshot_seqs.lock().insert(txn_id, commit_seq);
        self.terminal_times
            .lock()
            .insert(txn_id, get_fast_timestamp());

        if txn_id > self.next_txn_id.load(Ordering::Acquire) {
            self.next_txn_id.store(txn_id, Ordering::Release);
        }

        if commit_seq > self.next_sequence.load(Ordering::Acquire) {
            self.next_sequence.store(commit_seq, Ordering::Release);
        }
        Ok(())
    }

    /// Records an aborted transaction during recovery.
    pub fn recover_aborted_transaction(&self, txn_id: i64) -> Result<()> {
        if is_internal_durable_transaction_id(txn_id) {
            return Ok(());
        }
        if txn_id <= 0 || txn_id == i64::MAX {
            return Err(Error::internal(format!(
                "recovered transaction ID {txn_id} is outside the positive MVCC domain"
            )));
        }
        let _admission = self.admission.lock();
        self.transactions
            .lock()
            .insert(txn_id, TxnState::new_aborted());
        self.terminal_times
            .lock()
            .insert(txn_id, get_fast_timestamp());

        self.recover_transaction_high_water_locked(txn_id);
        Ok(())
    }

    /// Advances the admission high-water to every positive transaction ID
    /// observed in retained durable history, including aborted and in-doubt
    /// units that are intentionally not applied.
    pub fn recover_transaction_high_water(&self, txn_id: i64) -> Result<()> {
        // Zero and negative IDs are reserved engine/recovery identities and do
        // not participate in the positive user-transaction high-water.
        if txn_id <= 0 {
            return Ok(());
        }
        if txn_id == i64::MAX {
            return Err(Error::internal(format!(
                "recovered transaction ID {txn_id} is outside the positive MVCC domain"
            )));
        }
        let _admission = self.admission.lock();
        self.recover_transaction_high_water_locked(txn_id);
        Ok(())
    }

    /// Advances the visibility-sequence high-water recovered from immutable
    /// physical state. A checkpoint can retire every WAL record that carried
    /// the old sequence frontier, while durable tombstones still compare their
    /// commit sequence with future snapshot boundaries.
    pub fn recover_visibility_high_water(&self, sequence: i64) -> Result<()> {
        if !(0..=SEQ_MASK).contains(&sequence) {
            return Err(Error::internal(format!(
                "recovered visibility sequence {sequence} exceeds the packed MVCC domain"
            )));
        }
        let _admission = self.admission.lock();
        if sequence > self.next_sequence.load(Ordering::Acquire) {
            self.next_sequence.store(sequence, Ordering::Release);
        }
        Ok(())
    }

    fn recover_transaction_high_water_locked(&self, txn_id: i64) {
        if txn_id > self.next_txn_id.load(Ordering::Acquire) {
            self.next_txn_id.store(txn_id, Ordering::Release);
        }
    }

    /// Checks if a version is visible (main entry point).
    ///
    /// Hot path - optimized with single lock acquisition.
    #[inline(always)]
    pub fn is_visible(&self, version_txn_id: i64, viewer_txn_id: i64) -> bool {
        // FAST PATH 1: Own writes always visible
        if version_txn_id == viewer_txn_id {
            return true;
        }

        // FAST PATH 2: Recovery transactions always visible
        if version_txn_id == RECOVERY_TRANSACTION_ID {
            return true;
        }

        // FAST PATH 3: Check isolation level
        let needs_snapshot = self.needs_snapshot_isolation(viewer_txn_id);

        if needs_snapshot {
            self.is_visible_snapshot(version_txn_id, viewer_txn_id)
        } else {
            self.check_committed(version_txn_id)
        }
    }

    /// Checks if a transaction is committed (READ COMMITTED visibility).
    ///
    /// Single lock acquisition for the hot path.
    #[inline(always)]
    fn check_committed(&self, txn_id: i64) -> bool {
        // Validate the identity before consulting the cache. In particular,
        // zero is the empty-slot sentinel and must never be visible.
        let next = self.next_txn_id.load(Ordering::Acquire);
        if txn_id <= 0 || txn_id > next {
            return false;
        }

        // SINGLE lock acquisition - check if in transactions map
        // If in map = Active, Committing, or Aborted = not committed
        if self.transactions.lock().contains_key(txn_id) {
            return false;
        }

        if COMMITTED_CACHE.with(|cache| cache.borrow().contains(self.cache_epoch, txn_id)) {
            return true;
        }

        COMMITTED_CACHE.with(|cache| cache.borrow_mut().insert(self.cache_epoch, txn_id));
        true
    }

    /// Checks if a version is directly visible (for READ COMMITTED).
    #[inline(always)]
    pub fn is_directly_visible(&self, version_txn_id: i64) -> bool {
        if version_txn_id == RECOVERY_TRANSACTION_ID {
            return true;
        }
        self.check_committed(version_txn_id)
    }

    /// Snapshot isolation visibility check using the immutable begin boundary.
    #[cold]
    #[inline(never)]
    fn is_visible_snapshot(&self, version_txn_id: i64, viewer_txn_id: i64) -> bool {
        if let Some(visible) = COMMITTED_CACHE.with(|cache| {
            cache
                .borrow()
                .snapshot_visibility(self.cache_epoch, version_txn_id, viewer_txn_id)
        }) {
            return visible;
        }

        #[cfg(test)]
        R3_L01_SNAPSHOT_REGISTRY_LOCKS.fetch_add(1, Ordering::Relaxed);
        let boundary = self.snapshot_exclusions.lock().get(viewer_txn_id).cloned();
        let Some(boundary) = boundary else {
            return self.check_committed(version_txn_id);
        };
        let visible = boundary.is_visible(version_txn_id);
        COMMITTED_CACHE.with(|cache| {
            cache
                .borrow_mut()
                .install_snapshot(self.cache_epoch, viewer_txn_id, boundary);
        });
        visible
    }

    /// Gets the commit sequence for a transaction.
    pub fn get_commit_sequence(&self, txn_id: i64) -> Option<i64> {
        // Check snapshot_seqs first
        if let Some(&seq) = self.snapshot_seqs.lock().get(txn_id) {
            return Some(seq);
        }

        // Check if still active/aborted
        if let Some(state) = self.transactions.lock().get(txn_id) {
            if state.is_aborted() || state.is_active_or_committing() {
                return None;
            }
        }

        // Valid committed transaction (GC'd from snapshot_seqs)
        let next = self.next_txn_id.load(Ordering::Acquire);
        if txn_id > 0 && txn_id <= next {
            return Some(0); // Committed but commit_seq unknown
        }

        None
    }

    /// Gets the begin sequence for an active transaction.
    pub fn get_transaction_begin_sequence(&self, txn_id: i64) -> i64 {
        self.transactions
            .lock()
            .get(txn_id)
            .map(|e| e.begin_seq())
            .unwrap_or(0)
    }

    /// Gets the commit sequence for a transaction that is currently committing.
    /// Returns 0 if the transaction is not found or not in committing state.
    pub fn get_committing_sequence(&self, txn_id: i64) -> i64 {
        self.transactions
            .lock()
            .get(txn_id)
            .map(|e| e.commit_seq())
            .unwrap_or(0)
    }

    /// Gets the current sequence number.
    pub fn get_current_sequence(&self) -> i64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    /// Reserve a visibility sequence for an internal storage publication.
    ///
    /// Snapshot begin uses the same transaction lock while advancing its
    /// frontier. This gives maintenance state such as seal-skip tombstones a
    /// real position in the visibility order instead of borrowing the sequence
    /// of a transaction that may still be excluded by an older snapshot.
    pub fn reserve_visibility_sequence(&self) -> Result<i64> {
        let _transactions = self.transactions.lock();
        let sequence = self
            .next_sequence
            .load(Ordering::Acquire)
            .checked_add(1)
            .filter(|next| *next <= SEQ_MASK)
            .ok_or_else(|| Error::internal("MVCC visibility sequence domain exhausted"))?;
        self.next_sequence.store(sequence, Ordering::Release);
        Ok(sequence)
    }

    /// Gets the current commit sequence number.
    pub fn current_commit_sequence(&self) -> i64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    /// Gets a snapshot-safe commit sequence cutoff.
    ///
    /// Returns the minimum commit_seq among all in-flight (committing)
    /// transactions minus 1, or `current_commit_sequence()` if none are
    /// in-flight. This guarantees all commits with seq ≤ the returned
    /// value have fully completed (versions applied, tombstones applied,
    /// removed from transactions map).
    pub fn safe_snapshot_cutoff(&self) -> i64 {
        let txns = self.transactions.lock();
        let mut min_committing = i64::MAX;
        for (_, entry) in txns.iter() {
            if entry.status() == TxnStatus::Committing {
                let seq = entry.commit_seq();
                if seq > 0 && seq < min_committing {
                    min_committing = seq;
                }
            }
        }
        // Read next_sequence WHILE holding the lock to prevent a concurrent
        // start_commit from bumping the sequence between the scan and this read.
        let current = self.next_sequence.load(Ordering::Acquire);
        drop(txns);
        if min_committing == i64::MAX {
            current
        } else {
            min_committing - 1
        }
    }

    /// Runs garbage collection.
    ///
    /// Removes:
    /// - Old aborted entries (txn_id < min_active_txn_id - buffer)
    /// - Old snapshot_seqs entries (commit_seq < min_active_begin_seq)
    /// - Isolation overrides for completed transactions
    pub fn run_gc(&self) -> usize {
        self.run_gc_with_retention(std::time::Duration::ZERO)
    }

    fn run_gc_with_retention(&self, max_age: std::time::Duration) -> usize {
        let mut removed = 0;
        let now = get_fast_timestamp();
        let retention_ns = i64::try_from(max_age.as_nanos()).unwrap_or(i64::MAX);
        let is_expired = |completed_at: i64| {
            retention_ns == 0
                || (retention_ns != i64::MAX && now.saturating_sub(completed_at) >= retention_ns)
        };
        let mut removed_terminal_ids = Vec::new();

        // Step 1: Collect override keys FIRST if any exist (O(Overrides))
        // This avoids building O(ActiveTxns) set just to filter O(Overrides) entries
        let override_keys: Vec<i64> = if self.override_count.load(Ordering::Relaxed) > 0 {
            self.isolation_overrides.lock().keys().collect()
        } else {
            Vec::new()
        };

        // Step 2: Single lock on transactions to:
        // - Calculate min values
        // - Remove old aborted entries
        // - Check which override keys are no longer active
        let (min_begin_seq, invalid_overrides) = {
            let mut txns = self.transactions.lock();
            let mut min_begin = i64::MAX;
            let mut min_id = i64::MAX;

            for (id, state) in txns.iter() {
                if state.is_active_or_committing() {
                    min_begin = min_begin.min(state.begin_seq());
                    min_id = min_id.min(id);
                }
            }

            if min_id == i64::MAX {
                min_id = self.next_txn_id.load(Ordering::Acquire);
            }

            // GC aborted entries while holding lock
            let aborted_cutoff = min_id.saturating_sub(10000);
            if aborted_cutoff > 0 {
                let terminal_times = self.terminal_times.lock();
                let len_before = txns.len();
                txns.retain(|id, state| {
                    let remove = state.is_aborted()
                        && id < aborted_cutoff
                        && terminal_times.get(id).is_some_and(|time| is_expired(*time));
                    if remove {
                        removed_terminal_ids.push(id);
                    }
                    !remove
                });
                removed += len_before - txns.len();
            }

            // Check which override keys are no longer active (O(Overrides) lookups)
            let invalid: Vec<i64> = override_keys
                .iter()
                .filter(|&&txn_id| {
                    // Invalid if not in transactions map (committed/unknown)
                    // or if aborted
                    match txns.get(txn_id) {
                        None => true, // Not active = committed or never existed
                        Some(state) => state.is_aborted(),
                    }
                })
                .copied()
                .collect();

            (min_begin, invalid)
        };

        // Step 3: GC snapshot_seqs
        {
            let mut seqs = self.snapshot_seqs.lock();
            let terminal_times = self.terminal_times.lock();
            let len_before = seqs.len();
            seqs.retain(|txn_id, &mut commit_seq| {
                let remove = commit_seq < min_begin_seq
                    && terminal_times
                        .get(txn_id)
                        .is_some_and(|time| is_expired(*time));
                if remove {
                    removed_terminal_ids.push(txn_id);
                }
                !remove
            });
            removed += len_before - seqs.len();
        }

        if !removed_terminal_ids.is_empty() {
            let mut terminal_times = self.terminal_times.lock();
            for txn_id in removed_terminal_ids {
                terminal_times.remove(txn_id);
            }
        }

        // Step 4: Remove invalid isolation overrides (O(InvalidOverrides))
        if !invalid_overrides.is_empty() {
            let mut overrides = self.isolation_overrides.lock();
            let mut actual_removals = 0usize;
            for txn_id in &invalid_overrides {
                if overrides.remove(*txn_id).is_some() {
                    removed += 1;
                    actual_removals += 1;
                }
            }
            if actual_removals > 0 {
                self.override_count
                    .fetch_sub(actual_removals, Ordering::Relaxed);
            }
        }

        removed
    }

    /// Cleans up old transactions (legacy API).
    pub fn cleanup_old_transactions(&self, max_age: std::time::Duration) -> i32 {
        self.run_gc_with_retention(max_age) as i32
    }

    /// Waits for active transactions to complete with timeout.
    pub fn wait_for_active_transactions(&self, timeout: std::time::Duration) -> i32 {
        let deadline = radixdb_core::time_compat::Instant::now() + timeout;

        loop {
            if radixdb_core::time_compat::Instant::now() > deadline {
                break;
            }

            let count = self.active_count();
            if count == 0 {
                return 0;
            }

            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        self.active_count() as i32
    }

    /// Stops accepting new transactions.
    pub fn stop_accepting_transactions(&self) {
        let _admission = self.admission.lock();
        self.accepting.store(false, Ordering::Release);
    }

    /// Starts accepting new transactions.
    pub fn start_accepting_transactions(&self) {
        let _admission = self.admission.lock();
        self.accepting.store(true, Ordering::Release);
    }

    /// Clear state populated by a failed engine startup/recovery attempt.
    ///
    /// The engine calls this only while admission is stopped and before ready
    /// publication, so there are no caller-owned active transactions to retain.
    pub fn reset_after_failed_startup(&self) {
        self.stop_accepting_transactions();
        self.transactions.lock().clear();
        self.snapshot_seqs.lock().clear();
        {
            let mut boundaries = self.snapshot_exclusions.lock();
            for (_, boundary) in boundaries.iter() {
                boundary.active.store(false, Ordering::Release);
            }
            boundaries.clear();
        }
        self.terminal_times.lock().clear();
        self.isolation_overrides.lock().clear();
        self.waits_for.lock().clear();
        self.next_txn_id.store(0, Ordering::Release);
        self.next_sequence.store(0, Ordering::Release);
        self.override_count.store(0, Ordering::Release);
        self.active_txn_count.store(0, Ordering::Release);
    }

    /// Checks if the registry is accepting new transactions.
    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    /// Gets the count of active transactions.
    #[inline]
    pub fn active_count(&self) -> usize {
        self.active_txn_count.load(Ordering::Acquire)
    }

    /// Read bounded transaction/wait ownership without joining a lock queue.
    /// A busy registry is reported explicitly instead of allowing diagnostics
    /// to become another source of backpressure.
    pub fn runtime_snapshot(&self, max_transactions: usize) -> TransactionRuntimeSnapshot {
        let mut snapshot = TransactionRuntimeSnapshot {
            active: self.active_count() as u64,
            accepting: self.is_accepting(),
            ..TransactionRuntimeSnapshot::default()
        };
        let Some(transactions) = self.transactions.try_lock() else {
            snapshot.registry_busy = true;
            return snapshot;
        };
        snapshot.truncated = transactions.len() > max_transactions;
        for (_, state) in transactions.iter().take(max_transactions) {
            let status = state.status();
            if status == TxnStatus::Active || status == TxnStatus::Committing {
                snapshot.scanned = snapshot.scanned.saturating_add(1);
                snapshot.oldest_begin_sequence = Some(
                    snapshot
                        .oldest_begin_sequence
                        .map_or(state.begin_seq, |oldest| oldest.min(state.begin_seq)),
                );
                snapshot.oldest_begin_timestamp_nanos = Some(
                    snapshot
                        .oldest_begin_timestamp_nanos
                        .map_or(state.begin_timestamp_nanos(), |oldest| {
                            oldest.min(state.begin_timestamp_nanos())
                        }),
                );
            }
        }
        snapshot.wait_edges = self.waits_for.try_lock().map(|waits| waits.len() as u64);
        snapshot
    }

    /// Returns all currently active transaction IDs.
    ///
    /// Used by cleanup routines to identify which transactions still hold
    /// resources (e.g., pending cold deletes in segment managers).
    pub fn active_transaction_ids(&self) -> Vec<i64> {
        self.transactions
            .lock()
            .iter()
            .filter(|(_, s)| {
                let status = s.status();
                status == TxnStatus::Active || status == TxnStatus::Committing
            })
            .map(|(id, _)| id)
            .collect()
    }

    /// Gets the count of entries in snapshot_seqs.
    pub fn committed_count(&self) -> usize {
        self.snapshot_seqs.lock().len()
    }

    /// Checks if a transaction is active.
    pub fn is_active(&self, txn_id: i64) -> bool {
        self.transactions
            .lock()
            .get(txn_id)
            .map(|e| e.status() == TxnStatus::Active)
            .unwrap_or(false)
    }

    /// Checks if a transaction is committed.
    pub fn is_committed(&self, txn_id: i64) -> bool {
        // Check if in transactions map
        if self.transactions.lock().contains_key(txn_id) {
            // If aborted or active/committing, not committed
            return false;
        }

        // Not in map - committed if valid ID
        let next = self.next_txn_id.load(Ordering::Acquire);
        txn_id > 0 && txn_id <= next
    }

    /// Checks if a transaction is in committing state.
    #[cfg(test)]
    pub fn is_committing(&self, txn_id: i64) -> bool {
        self.transactions
            .lock()
            .get(txn_id)
            .map(|e| e.status() == TxnStatus::Committing)
            .unwrap_or(false)
    }

    /// Checks if a transaction was committed before a given sequence.
    pub fn is_committed_before(&self, txn_id: i64, cutoff_commit_seq: i64) -> bool {
        // Special case: negative IDs are always "old"
        if txn_id < 0 {
            return true;
        }

        // Check if still in transactions map
        if self.transactions.lock().contains_key(txn_id) {
            // If aborted or still active/committing, not committed
            return false;
        }

        // Check snapshot_seqs for exact commit_seq
        if let Some(&commit_seq) = self.snapshot_seqs.lock().get(txn_id) {
            return commit_seq <= cutoff_commit_seq;
        }

        // Not in snapshot_seqs = old committed (GC'd)
        // GC'd means commit_seq was < min_active_begin_seq
        // Therefore definitely < cutoff_commit_seq
        let next = self.next_txn_id.load(Ordering::Acquire);
        txn_id > 0 && txn_id <= next
    }

    /// Returns true only when `txn_id` belongs to every currently active
    /// snapshot boundary.
    ///
    /// Snapshot begin records transactions that had reserved a commit sequence
    /// but had not completed publication. After such a transaction commits its
    /// sequence may still be older than the snapshot's scalar cutoff, while its
    /// ID remains deliberately excluded. Seal must preserve that exact hole:
    /// moving the row to cold storage would discard its MVCC provenance and
    /// make it visible to the older snapshot.
    pub fn capture_seal_visibility(&self) -> super::version_store::SealVisibilitySnapshot {
        // Lock in the same order as transaction begin. While `transactions` is
        // held, no transaction can begin or cross the complete-commit boundary,
        // so the scalar frontier and exclusion holes describe one instant.
        let transactions = self.transactions.lock();
        let boundaries = self.snapshot_exclusions.lock();

        let max_visible_txn_id = boundaries
            .values()
            .filter(|boundary| boundary.active.load(Ordering::Acquire))
            .map(|boundary| boundary.max_txn_id)
            .min();
        let mut excluded: Vec<i64> = transactions
            .iter()
            .filter(|(_, state)| state.is_active_or_committing())
            .map(|(txn_id, _)| txn_id)
            .collect();
        for boundary in boundaries
            .values()
            .filter(|boundary| boundary.active.load(Ordering::Acquire))
        {
            excluded.extend(
                boundary
                    .excluded
                    .iter()
                    .copied()
                    .filter(|txn_id| max_visible_txn_id.is_none_or(|max| *txn_id <= max)),
            );
        }

        super::version_store::SealVisibilitySnapshot::new(max_visible_txn_id, excluded)
    }
}

impl Default for TransactionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl VisibilityChecker for TransactionRegistry {
    fn is_visible(&self, version_txn_id: i64, viewing_txn_id: i64) -> bool {
        TransactionRegistry::is_visible(self, version_txn_id, viewing_txn_id)
    }

    fn get_current_sequence(&self) -> i64 {
        TransactionRegistry::get_current_sequence(self)
    }

    fn get_active_transaction_ids(&self) -> Vec<i64> {
        self.transactions
            .lock()
            .iter()
            .filter(|(_, s)| s.status() == TxnStatus::Active)
            .map(|(id, _)| id)
            .collect()
    }

    fn is_committed_before(&self, txn_id: i64, cutoff_commit_seq: i64) -> bool {
        TransactionRegistry::is_committed_before(self, txn_id, cutoff_commit_seq)
    }

    fn capture_seal_visibility(&self) -> super::version_store::SealVisibilitySnapshot {
        TransactionRegistry::capture_seal_visibility(self)
    }

    fn needs_snapshot_isolation(&self, txn_id: i64) -> bool {
        TransactionRegistry::needs_snapshot_isolation(self, txn_id)
    }

    fn register_row_wait(&self, waiter_txn_id: i64, owner_txn_id: i64) -> bool {
        if waiter_txn_id == owner_txn_id {
            return true;
        }
        let mut graph = self.waits_for.lock();
        graph.insert(waiter_txn_id, owner_txn_id);

        let mut current = owner_txn_id;
        let mut steps = 0usize;
        while let Some(next) = graph.get(current).copied() {
            if next == waiter_txn_id {
                graph.remove(waiter_txn_id);
                return false;
            }
            current = next;
            steps += 1;
            if steps > graph.len() {
                graph.remove(waiter_txn_id);
                return false;
            }
        }
        true
    }

    fn clear_row_wait(&self, waiter_txn_id: i64) {
        self.waits_for.lock().remove(waiter_txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    fn commit_for_test(registry: &TransactionRegistry, txn_id: i64) -> i64 {
        let commit_seq = registry.start_commit(txn_id).unwrap();
        registry.complete_commit(txn_id).unwrap();
        commit_seq
    }

    #[test]
    fn test_begin_transaction() {
        let registry = TransactionRegistry::new();

        let (txn_id1, seq1) = registry.begin_transaction();
        assert!(txn_id1 > 0);
        assert!(seq1 > 0);

        let (txn_id2, seq2) = registry.begin_transaction();
        assert!(txn_id2 > txn_id1);
        assert!(seq2 > seq1);
    }

    fn wait_until_admission_is_held(registry: &TransactionRegistry) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if registry.admission.try_lock().is_none() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "transaction did not reach the admission critical section"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn v2_r2_snapshot_begin_cannot_bypass_an_allocating_transaction() {
        let registry = Arc::new(TransactionRegistry::new());
        let transactions = registry.transactions.lock();

        let (first_tx, first_rx) = mpsc::channel();
        let first_registry = Arc::clone(&registry);
        let first = std::thread::spawn(move || {
            first_tx
                .send(
                    first_registry.begin_transaction_with_isolation(IsolationLevel::ReadCommitted),
                )
                .unwrap();
        });
        wait_until_admission_is_held(&registry);

        let (snapshot_tx, snapshot_rx) = mpsc::channel();
        let snapshot_registry = Arc::clone(&registry);
        let snapshot = std::thread::spawn(move || {
            snapshot_tx
                .send(
                    snapshot_registry
                        .begin_transaction_with_isolation(IsolationLevel::SnapshotIsolation),
                )
                .unwrap();
        });
        assert!(
            snapshot_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "snapshot begin bypassed an admission already inside registration"
        );

        drop(transactions);
        let (first_id, _) = first_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (snapshot_id, _) = snapshot_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        first.join().unwrap();
        snapshot.join().unwrap();

        let boundaries = registry.snapshot_exclusions.lock();
        let boundary = boundaries.get(snapshot_id).expect("snapshot boundary");
        assert!(boundary.excluded.contains(&first_id));
        assert!(first_id < snapshot_id);
    }

    #[test]
    fn v2_r2_shutdown_waits_for_inflight_admission_publication() {
        let registry = Arc::new(TransactionRegistry::new());
        let transactions = registry.transactions.lock();

        let (begin_tx, begin_rx) = mpsc::channel();
        let begin_registry = Arc::clone(&registry);
        let begin = std::thread::spawn(move || {
            begin_tx.send(begin_registry.begin_transaction()).unwrap();
        });
        wait_until_admission_is_held(&registry);

        let (stop_tx, stop_rx) = mpsc::channel();
        let stop_registry = Arc::clone(&registry);
        let stop = std::thread::spawn(move || {
            stop_registry.stop_accepting_transactions();
            stop_tx.send(stop_registry.active_count()).unwrap();
        });
        assert!(
            stop_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "shutdown crossed an admission whose active count was not published"
        );

        drop(transactions);
        let (txn_id, _) = begin_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let count_at_stop = stop_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        begin.join().unwrap();
        stop.join().unwrap();

        assert_eq!(count_at_stop, 1);
        assert!(!registry.is_accepting());
        registry.abort_transaction(txn_id);
    }

    #[test]
    fn v2_r2_sequence_domain_exhaustion_is_fallible_and_stable() {
        let registry = TransactionRegistry::new();
        registry
            .next_sequence
            .store(SEQ_MASK - 1, Ordering::Release);

        let (txn_id, begin_seq) = registry.begin_transaction();
        assert_eq!(begin_seq, SEQ_MASK);
        assert_eq!(
            registry.start_commit(txn_id).unwrap_err().to_string(),
            "MVCC commit sequence domain exhausted"
        );
        assert_eq!(registry.next_sequence.load(Ordering::Acquire), SEQ_MASK);
        assert_eq!(
            registry.begin_transaction(),
            (INVALID_TRANSACTION_ID, 0),
            "begin must fail without wrapping the packed sequence"
        );

        assert!(registry
            .recover_committed_transaction(100, SEQ_MASK + 1)
            .is_err());
        assert_eq!(registry.next_txn_id.load(Ordering::Acquire), txn_id);
    }

    #[test]
    fn row_wait_graph_rejects_cycle_and_allows_acyclic_chain() {
        let registry = TransactionRegistry::new();
        assert!(registry.register_row_wait(3, 2));
        assert!(registry.register_row_wait(2, 1));
        assert!(!registry.register_row_wait(1, 3));

        registry.clear_row_wait(2);
        assert!(registry.register_row_wait(1, 3));
        registry.clear_row_wait(1);
        registry.clear_row_wait(3);
    }

    #[test]
    fn test_two_phase_commit_marks_transaction_committed() {
        let registry = TransactionRegistry::new();

        let (txn_id, _) = registry.begin_transaction();
        assert!(registry.is_active(txn_id));
        assert!(!registry.is_committed(txn_id));

        commit_for_test(&registry, txn_id);
        assert!(!registry.is_active(txn_id));
        assert!(registry.is_committed(txn_id));
    }

    #[test]
    fn test_two_phase_commit() {
        let registry = TransactionRegistry::new();

        let (txn_id, _) = registry.begin_transaction();

        let commit_seq = registry.start_commit(txn_id).unwrap();
        assert!(commit_seq > 0);
        assert!(!registry.is_active(txn_id));
        assert!(registry.is_committing(txn_id));

        registry.complete_commit(txn_id).unwrap();
        assert!(!registry.is_committing(txn_id));
        assert!(registry.is_committed(txn_id));
    }

    #[test]
    fn r3_l01_batch_a_invalid_registry_transitions_are_side_effect_free() {
        let registry = TransactionRegistry::new();
        let before_sequence = registry.get_current_sequence();
        let before_active = registry.active_count();
        let before_committed = registry.committed_count();

        assert!(registry.start_commit(9_999).is_err());
        assert_eq!(
            registry.get_current_sequence(),
            before_sequence,
            "unknown start_commit must not consume a sequence"
        );
        assert!(registry.complete_commit(9_999).is_err());
        assert_eq!(registry.active_count(), before_active);
        assert_eq!(registry.committed_count(), before_committed);

        let (txn_id, _) = registry.begin_transaction();
        registry.start_commit(txn_id).unwrap();
        registry.complete_commit(txn_id).unwrap();
        let after_commit_sequence = registry.get_current_sequence();
        let after_commit_count = registry.committed_count();
        assert!(registry.complete_commit(txn_id).is_err());
        assert_eq!(registry.active_count(), before_active);
        assert_eq!(registry.get_current_sequence(), after_commit_sequence);
        assert_eq!(registry.committed_count(), after_commit_count);
    }

    #[test]
    fn r3_l01_batch_a_retention_duration_changes_aborted_metadata_cleanup() {
        fn registry_with_recent_aborts() -> TransactionRegistry {
            let registry = TransactionRegistry::new();
            for _ in 0..10_002 {
                let (txn_id, _) = registry.begin_transaction();
                registry.abort_transaction(txn_id);
            }
            registry
        }

        let immediate = registry_with_recent_aborts();
        let retained = registry_with_recent_aborts();
        let immediate_removed = immediate.cleanup_old_transactions(std::time::Duration::ZERO);
        let retained_removed = retained.cleanup_old_transactions(std::time::Duration::MAX);

        assert!(
            immediate_removed > 0,
            "zero retention must permit safe cleanup"
        );
        assert_eq!(
            retained_removed, 0,
            "recent transaction metadata must survive a maximal retention window"
        );
    }

    #[test]
    fn r3_l01_batch_a_snapshot_visibility_reuses_one_immutable_boundary() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let mut committed = Vec::new();
        for _ in 0..128 {
            let (txn_id, _) = registry.begin_transaction();
            commit_for_test(&registry, txn_id);
            committed.push(txn_id);
        }
        let (viewer, _) = registry.begin_transaction();
        R3_L01_SNAPSHOT_REGISTRY_LOCKS.store(0, Ordering::Relaxed);

        for txn_id in committed {
            assert!(registry.is_visible(txn_id, viewer));
        }

        let locks = R3_L01_SNAPSHOT_REGISTRY_LOCKS.load(Ordering::Relaxed);
        assert!(
            locks <= 2,
            "one immutable snapshot boundary must serve the row-scaled checks, got {locks} registry locks"
        );
    }

    #[test]
    fn r3_l01_batch_a_committed_cache_has_a_bounded_thread_footprint() {
        const { assert!(CACHE_SIZE <= 8_192) };
        assert!(
            std::mem::size_of::<CommittedCache>() <= 64 * 1024,
            "per-thread committed cache retains {} bytes",
            std::mem::size_of::<CommittedCache>()
        );
    }

    #[test]
    fn test_snapshot_started_during_commit_excludes_that_commit() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (writer, _) = registry.begin_transaction();
        let writer_commit_seq = registry.start_commit(writer).unwrap();

        // The reader begins after the writer reserved a commit sequence but
        // before publication. The scalar frontier therefore advances past the
        // reserved sequence; the exact exclusion owns the visibility hole.
        let (reader, reader_begin_seq) = registry.begin_transaction();
        assert!(
            reader_begin_seq > writer_commit_seq,
            "snapshot frontier {reader_begin_seq} must include the reserved sequence {writer_commit_seq}"
        );
        assert!(
            registry
                .snapshot_exclusions
                .lock()
                .get(reader)
                .is_some_and(|boundary| boundary.excluded.contains(&writer)),
            "snapshot must retain an exact exclusion for the in-flight writer"
        );
        assert!(
            !registry.is_visible(writer, reader),
            "an in-flight writer must not be visible before publication"
        );

        registry.complete_commit(writer).unwrap();
        assert!(
            !registry.is_visible(writer, reader),
            "a snapshot begun during an in-flight commit must not change after that commit completes"
        );
    }

    #[test]
    fn maintenance_visibility_sequence_follows_snapshot_started_during_commit() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (writer, _) = registry.begin_transaction();
        registry.start_commit(writer).unwrap();
        let (_reader, reader_begin_seq) = registry.begin_transaction();
        registry.complete_commit(writer).unwrap();

        let maintenance_seq = registry.reserve_visibility_sequence().unwrap();
        assert!(
            maintenance_seq > reader_begin_seq,
            "maintenance publication must remain invisible to the older snapshot"
        );
    }

    #[test]
    fn committed_storage_publication_separates_snapshots_around_commit_completion() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (writer, _) = registry.begin_transaction();
        registry.start_commit(writer).unwrap();
        let (_before, before_begin_seq) = registry.begin_transaction();
        let storage_seq = registry
            .complete_commit_with_storage_publication(writer)
            .unwrap();
        let (_after, after_begin_seq) = registry.begin_transaction();

        assert!(storage_seq > before_begin_seq);
        assert!(after_begin_seq >= storage_seq);
    }

    #[test]
    fn test_abort_transaction() {
        let registry = TransactionRegistry::new();

        let (txn_id, _) = registry.begin_transaction();
        assert!(registry.is_active(txn_id));

        registry.abort_transaction(txn_id);
        assert!(!registry.is_active(txn_id));
        assert!(!registry.is_committed(txn_id));

        // Verify aborted status
        let state = registry.transactions.lock().get(txn_id).copied();
        assert!(state.map(|s| s.is_aborted()).unwrap_or(false));
    }

    #[test]
    fn test_visibility_own_writes() {
        let registry = TransactionRegistry::new();

        let (txn_id, _) = registry.begin_transaction();
        assert!(registry.is_visible(txn_id, txn_id));
    }

    #[test]
    fn test_visibility_recovery_transaction() {
        let registry = TransactionRegistry::new();

        let (viewer_id, _) = registry.begin_transaction();
        assert!(registry.is_visible(RECOVERY_TRANSACTION_ID, viewer_id));
    }

    #[test]
    fn test_visibility_read_committed() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::ReadCommitted);

        let (txn1, _) = registry.begin_transaction();
        let (txn2, _) = registry.begin_transaction();

        // Active transaction not visible
        assert!(!registry.is_visible(txn1, txn2));

        // After commit, visible
        commit_for_test(&registry, txn1);
        assert!(registry.is_visible(txn1, txn2));
    }

    #[test]
    fn test_visibility_snapshot_isolation() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (txn1, _) = registry.begin_transaction();
        commit_for_test(&registry, txn1);

        let (txn2, _) = registry.begin_transaction();

        // txn1 committed before txn2 began - visible
        assert!(registry.is_visible(txn1, txn2));

        let (txn3, _) = registry.begin_transaction();
        commit_for_test(&registry, txn3);

        // txn3 committed after txn2 began - NOT visible
        assert!(!registry.is_visible(txn3, txn2));
    }

    #[test]
    fn test_stop_accepting() {
        let registry = TransactionRegistry::new();
        assert!(registry.is_accepting());

        registry.stop_accepting_transactions();
        assert!(!registry.is_accepting());

        let (txn_id, _) = registry.begin_transaction();
        assert_eq!(txn_id, INVALID_TRANSACTION_ID);
    }

    #[test]
    fn test_isolation_level_override() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::ReadCommitted);

        let (txn_id, _) = registry.begin_transaction();

        assert_eq!(
            registry.get_isolation_level(txn_id),
            IsolationLevel::ReadCommitted
        );

        assert!(
            !registry.set_transaction_isolation_level(txn_id, IsolationLevel::SnapshotIsolation)
        );
        assert_eq!(
            registry.get_isolation_level(txn_id),
            IsolationLevel::ReadCommitted
        );

        registry.remove_transaction_isolation_level(txn_id);
        assert_eq!(
            registry.get_isolation_level(txn_id),
            IsolationLevel::ReadCommitted
        );
    }

    #[test]
    fn test_get_commit_sequence() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (txn_id, _) = registry.begin_transaction();
        assert!(registry.get_commit_sequence(txn_id).is_none());

        let commit_seq = commit_for_test(&registry, txn_id);
        assert_eq!(registry.get_commit_sequence(txn_id), Some(commit_seq));
    }

    #[test]
    fn test_recover_committed_transaction() {
        let registry = TransactionRegistry::new();

        registry.recover_committed_transaction(1000, 500).unwrap();

        assert!(registry.is_committed(1000));
        assert_eq!(registry.get_commit_sequence(1000), Some(500));

        let (new_id, _) = registry.begin_transaction();
        assert!(new_id > 1000);
    }

    #[test]
    fn r2_l05_a_visibility_cache_is_registry_scoped_and_rejects_zero() {
        let registry_a = TransactionRegistry::new();
        let (committed, _) = registry_a.begin_transaction();
        commit_for_test(&registry_a, committed);
        let (viewer_a, _) = registry_a.begin_transaction();
        assert!(registry_a.is_visible(committed, viewer_a));

        let registry_b = TransactionRegistry::new();
        let (active_same_id, _) = registry_b.begin_transaction();
        let (viewer_b, _) = registry_b.begin_transaction();
        assert_eq!(active_same_id, committed);
        assert!(!registry_b.is_visible(0, viewer_b));
        assert!(!registry_b.is_visible(active_same_id, viewer_b));
    }

    #[test]
    fn r2_l05_a_snapshot_includes_completed_out_of_order_commit_only() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (slow, _) = registry.begin_transaction();
        let (fast, _) = registry.begin_transaction();
        let slow_commit_seq = registry.start_commit(slow).unwrap();
        let fast_commit_seq = registry.start_commit(fast).unwrap();
        assert!(fast_commit_seq > slow_commit_seq);
        registry.complete_commit(fast).unwrap();

        let (reader, _) = registry.begin_transaction();
        assert!(
            registry.is_visible(fast, reader),
            "a commit fully published before begin must be in the snapshot"
        );
        assert!(!registry.is_visible(slow, reader));

        registry.complete_commit(slow).unwrap();
        assert!(
            !registry.is_visible(slow, reader),
            "an in-flight commit excluded at begin must stay excluded"
        );
    }

    #[test]
    fn r2_l05_a_isolation_is_immutable_from_atomic_begin() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::ReadCommitted);
        let (reader, _) = registry.begin_transaction();

        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);
        assert_eq!(
            registry.get_isolation_level(reader),
            IsolationLevel::ReadCommitted,
            "changing the default must not change an active transaction"
        );

        registry.set_global_isolation_level(IsolationLevel::ReadCommitted);
        let (snapshot_reader, _) =
            registry.begin_transaction_with_isolation(IsolationLevel::SnapshotIsolation);
        let (writer, _) = registry.begin_transaction();
        commit_for_test(&registry, writer);
        assert!(!registry
            .set_transaction_isolation_level(snapshot_reader, IsolationLevel::ReadCommitted));
        assert!(
            !registry.is_visible(writer, snapshot_reader),
            "late isolation selection must not admit a post-begin commit"
        );
    }

    #[test]
    fn test_gc() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        for _ in 0..10 {
            let (txn_id, _) = registry.begin_transaction();
            commit_for_test(&registry, txn_id);
        }

        assert_eq!(registry.snapshot_seqs.lock().len(), 10);

        let (active_txn, _) = registry.begin_transaction();

        for _ in 0..5 {
            let (txn_id, _) = registry.begin_transaction();
            commit_for_test(&registry, txn_id);
        }

        let removed = registry.run_gc();
        assert!(removed > 0);

        commit_for_test(&registry, active_txn);
    }

    #[test]
    fn test_aborted_not_visible() {
        let registry = TransactionRegistry::new();

        let (txn1, _) = registry.begin_transaction();
        let (txn2, _) = registry.begin_transaction();

        registry.abort_transaction(txn1);

        assert!(!registry.is_visible(txn1, txn2));
        assert!(!registry.is_committed(txn1));
    }

    #[test]
    fn test_per_transaction_snapshot_isolation() {
        let registry = TransactionRegistry::new();
        // Global is READ COMMITTED
        registry.set_global_isolation_level(IsolationLevel::ReadCommitted);

        let (txn1, _) = registry.begin_transaction();
        commit_for_test(&registry, txn1);

        // txn2 uses SNAPSHOT ISOLATION override
        let (txn2, _) =
            registry.begin_transaction_with_isolation(IsolationLevel::SnapshotIsolation);

        // txn1 committed before txn2 began - visible
        assert!(registry.is_visible(txn1, txn2));

        let (txn3, _) = registry.begin_transaction();
        commit_for_test(&registry, txn3);

        // txn3 committed after txn2 began - NOT visible (snapshot isolation)
        assert!(!registry.is_visible(txn3, txn2));

        // But txn4 (READ COMMITTED) should see txn3
        let (txn4, _) = registry.begin_transaction();
        assert!(registry.is_visible(txn3, txn4));
    }

    // === complete_commit: exact sequence retention ===

    #[test]
    fn test_commit_read_committed_retains_sequence_for_future_snapshots() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::ReadCommitted);

        let (txn_id, _) = registry.begin_transaction();
        commit_for_test(&registry, txn_id);

        assert!(registry.snapshot_seqs.lock().get(txn_id).is_some());
    }

    // === recover_committed_transaction: lines 416, 432 ===

    #[test]
    fn test_recover_committed_advances_next_txn_id() {
        let registry = TransactionRegistry::new();

        // Recover txn 100 with commit_seq 50
        registry.recover_committed_transaction(100, 50).unwrap();
        // next_txn_id must advance past 100
        let (new_id, _) = registry.begin_transaction();
        assert!(new_id > 100);
    }

    #[test]
    fn r2_l05_a_recovered_transaction_high_water_fails_closed_at_domain_end() {
        let registry = TransactionRegistry::new();
        registry
            .recover_transaction_high_water(i64::MAX - 1)
            .unwrap();

        assert_eq!(
            registry.begin_transaction(),
            (INVALID_TRANSACTION_ID, 0),
            "transaction admission must not wrap the recovered ID high-water"
        );
        assert_eq!(registry.next_txn_id.load(Ordering::Acquire), i64::MAX - 1);
    }

    #[test]
    fn test_recover_committed_advances_next_sequence() {
        let registry = TransactionRegistry::new();

        registry.recover_committed_transaction(10, 200).unwrap();
        // next_sequence must advance past 200
        let (_, begin_seq) = registry.begin_transaction();
        assert!(begin_seq > 200);
    }

    #[test]
    fn physical_recovery_advances_independent_mvcc_high_waters() {
        let registry = TransactionRegistry::new();

        registry.recover_transaction_high_water(40).unwrap();
        registry.recover_visibility_high_water(90).unwrap();

        assert_eq!(registry.begin_transaction(), (41, 91));
        assert!(registry
            .recover_visibility_high_water(SEQ_MASK + 1)
            .is_err());
    }

    #[test]
    fn v2_r2_recovery_accepts_auto_ddl_identity_without_entering_user_domain() {
        let registry = TransactionRegistry::new();

        registry.recover_committed_transaction(-4, 200).unwrap();
        registry.recover_aborted_transaction(-5).unwrap();

        assert_eq!(registry.next_txn_id.load(Ordering::Acquire), 0);
        assert_eq!(registry.next_sequence.load(Ordering::Acquire), 200);
        assert!(registry.snapshot_seqs.lock().get(-4).is_none());
        assert!(registry.transactions.lock().get(-5).is_none());
        assert!(registry.recover_committed_transaction(-1, 201).is_err());
        assert!(registry.recover_aborted_transaction(0).is_err());
    }

    #[test]
    fn test_recover_committed_descending_order() {
        let registry = TransactionRegistry::new();

        // Recover in descending order — next_txn_id must still track the max
        registry.recover_committed_transaction(100, 50).unwrap();
        registry.recover_committed_transaction(50, 30).unwrap();

        let (new_id, _) = registry.begin_transaction();
        assert!(new_id > 100, "next_txn_id should be >= 100, got {}", new_id);
    }

    // === recover_aborted_transaction: lines 448, 455 ===

    #[test]
    fn test_recover_aborted_marks_aborted() {
        let registry = TransactionRegistry::new();

        registry.recover_aborted_transaction(42).unwrap();

        // Must be aborted, not committed, not active
        assert!(!registry.is_committed(42));
        assert!(!registry.is_active(42));
        let state = registry.transactions.lock().get(42).copied();
        assert!(state.is_some());
        assert!(state.unwrap().is_aborted());
    }

    #[test]
    fn test_recover_aborted_advances_next_txn_id() {
        let registry = TransactionRegistry::new();

        registry.recover_aborted_transaction(100).unwrap();
        // next_txn_id must advance past 100
        let (new_id, _) = registry.begin_transaction();
        assert!(new_id > 100);
    }

    #[test]
    fn test_recover_aborted_descending_order() {
        let registry = TransactionRegistry::new();

        registry.recover_aborted_transaction(200).unwrap();
        registry.recover_aborted_transaction(100).unwrap();

        // next_txn_id must still be >= 200
        let (new_id, _) = registry.begin_transaction();
        assert!(new_id > 200, "next_txn_id should be > 200, got {}", new_id);
    }

    #[test]
    fn test_recover_aborted_not_visible() {
        let registry = TransactionRegistry::new();

        registry.recover_aborted_transaction(5).unwrap();

        let (viewer, _) = registry.begin_transaction();
        // Aborted txn must never be visible
        assert!(!registry.is_visible(5, viewer));
    }

    // === check_committed: line 512 ===

    #[test]
    fn test_check_committed_negative_txn_id_not_committed() {
        let registry = TransactionRegistry::new();

        // Start a real transaction so next_txn_id > 0
        let (txn_id, _) = registry.begin_transaction();
        commit_for_test(&registry, txn_id);

        // Negative txn_id must NOT be committed.
        // Catches && -> || mutation: (-5 > 0 || -5 <= next) would wrongly be true
        assert!(!registry.check_committed(-5));
        assert!(!registry.check_committed(-100));
    }

    #[test]
    fn test_check_committed_future_txn_id() {
        let registry = TransactionRegistry::new();

        // No transactions started yet, so txn_id 1 is beyond next_txn_id
        assert!(!registry.check_committed(1));
    }

    #[test]
    fn test_check_committed_valid_committed() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (txn_id, _) = registry.begin_transaction();
        commit_for_test(&registry, txn_id);

        assert!(registry.check_committed(txn_id));
    }

    #[test]
    fn test_check_committed_boundary_next_txn_id() {
        let registry = TransactionRegistry::new();

        let (txn_id, _) = registry.begin_transaction();
        commit_for_test(&registry, txn_id);

        // txn_id == next_txn_id should be committed (boundary: <= next)
        assert!(registry.check_committed(txn_id));
        // txn_id + 1 > next_txn_id should NOT be committed
        assert!(!registry.check_committed(txn_id + 1));
    }

    // === is_directly_visible: line 523 ===

    #[test]
    fn test_is_directly_visible_recovery() {
        let registry = TransactionRegistry::new();

        // RECOVERY_TRANSACTION_ID is always directly visible
        assert!(registry.is_directly_visible(RECOVERY_TRANSACTION_ID));
    }

    #[test]
    fn test_is_directly_visible_normal_txn() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (txn_id, _) = registry.begin_transaction();
        // Active txn is NOT directly visible
        assert!(!registry.is_directly_visible(txn_id));

        commit_for_test(&registry, txn_id);
        // Committed txn IS directly visible
        assert!(registry.is_directly_visible(txn_id));
    }

    #[test]
    fn test_is_directly_visible_non_recovery_negative() {
        let registry = TransactionRegistry::new();

        // A negative txn_id that is NOT RECOVERY_TRANSACTION_ID should not be visible
        assert!(!registry.is_directly_visible(-99));
    }

    // === is_visible_snapshot: lines 541, 570 ===

    #[test]
    fn test_snapshot_committed_viewer_fallback() {
        // When the viewer has already committed, the match guard
        // `is_active_or_committing()` fails → falls back to check_committed.
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (txn1, _) = registry.begin_transaction();
        commit_for_test(&registry, txn1);

        let (txn2, _) = registry.begin_transaction();
        commit_for_test(&registry, txn2);

        // txn1 is committed → visible to committed viewer txn2
        assert!(registry.is_visible(txn1, txn2));
    }

    #[test]
    fn test_snapshot_invalid_version_txn_zero() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (viewer, _) = registry.begin_transaction();

        // version_txn_id 0 is invalid — not visible
        // Catches || → && mutation: (0 <= 0 && 0 > next) = (true && false) = false
        // but || → && would never return false for valid-looking IDs
        assert!(!registry.is_visible(0, viewer));
    }

    #[test]
    fn test_snapshot_invalid_version_txn_negative() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (viewer, _) = registry.begin_transaction();

        // Negative txn_id (not RECOVERY_TRANSACTION_ID) is invalid
        assert!(!registry.is_visible(-50, viewer));
    }

    #[test]
    fn test_snapshot_future_version_txn_not_visible() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (viewer, _) = registry.begin_transaction();

        // version_txn_id beyond next_txn_id is invalid
        // Catches > → == mutation: 9999 == next would be false (not caught),
        // but next+1 is immediately beyond
        let next = registry.next_txn_id.load(Ordering::Acquire);
        assert!(!registry.is_visible(next + 1, viewer));
        assert!(!registry.is_visible(next + 100, viewer));
    }

    #[test]
    fn test_snapshot_boundary_version_equals_next() {
        // version_txn_id == next_txn_id should be valid (it's an assigned ID)
        // Catches > → >= mutation at line 570
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (txn1, _) = registry.begin_transaction();
        commit_for_test(&registry, txn1);

        // txn1 == next_txn_id at this point
        let (viewer, _) = registry.begin_transaction();
        // txn1 committed before viewer — should be visible
        assert!(registry.is_visible(txn1, viewer));
    }

    // === get_commit_sequence: line 608 ===

    #[test]
    fn test_get_commit_sequence_invalid_txn_id() {
        let registry = TransactionRegistry::new();

        // txn_id 0 is invalid
        assert_eq!(registry.get_commit_sequence(0), None);
        // Negative is invalid
        assert_eq!(registry.get_commit_sequence(-1), None);
    }

    #[test]
    fn test_get_commit_sequence_future_txn_id() {
        let registry = TransactionRegistry::new();

        // No transactions started, txn_id 1 is beyond next_txn_id
        assert_eq!(registry.get_commit_sequence(1), None);
    }

    #[test]
    fn test_get_commit_sequence_active() {
        let registry = TransactionRegistry::new();

        let (txn_id, _) = registry.begin_transaction();
        // Active transaction has no commit_seq
        assert_eq!(registry.get_commit_sequence(txn_id), None);
    }

    #[test]
    fn test_get_commit_sequence_aborted() {
        let registry = TransactionRegistry::new();

        let (txn_id, _) = registry.begin_transaction();
        registry.abort_transaction(txn_id);
        // Aborted transaction has no commit_seq
        assert_eq!(registry.get_commit_sequence(txn_id), None);
    }

    #[test]
    fn test_get_commit_sequence_committed_with_snapshot() {
        let registry = TransactionRegistry::new();
        registry.set_global_isolation_level(IsolationLevel::SnapshotIsolation);

        let (txn_id, _) = registry.begin_transaction();
        let commit_seq = commit_for_test(&registry, txn_id);

        // With snapshot isolation, exact commit_seq is stored
        assert_eq!(registry.get_commit_sequence(txn_id), Some(commit_seq));
        assert!(commit_seq > 0);
    }
}
