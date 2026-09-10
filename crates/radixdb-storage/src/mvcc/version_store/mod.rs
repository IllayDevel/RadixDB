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

//! Version store for MVCC row versioning
//!
//! This module provides the core version storage for MVCC, including:
//! - [`RowVersion`] - Represents a specific version of a row
//! - [`VersionStore`] - Tracks latest committed versions for a table
//! - [`TransactionVersionStore`] - Transaction-local changes before commit
//!
//! # Performance
//!
//! The version store uses arena-based storage for zero-copy full table scans.
//! Row data is stored contiguously in memory, enabling 50x+ faster scans
//! compared to traditional per-row cloning.
//!

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::expression::CompiledFilter;
use crate::instrumentation;
use crate::mvcc::arena::RowArena;
use crate::mvcc::registry::TransactionRegistry;
use crate::timestamp::get_fast_timestamp;
use crate::Index;
use ahash::AHashMap;
use radixdb_core::{
    CompactArc, CowBTree, DataType, Error, I64Map, I64Set, IndexType, Row, RowVec, Schema,
    SmartString, Value,
};
#[cfg(feature = "parallel")]
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::{smallvec, SmallVec};

type CowBTreeMap<V> = RwLock<CowBTree<V>>;

#[inline]
fn new_i64_map<V>() -> I64Map<V> {
    I64Map::new()
}

#[inline]
fn new_i64_map_with_capacity<V>(capacity: usize) -> I64Map<V> {
    I64Map::with_capacity(capacity)
}

#[inline]
fn new_cow_btree_map<V: Clone>() -> CowBTreeMap<V> {
    RwLock::new(CowBTree::new())
}

/// Prepared runtime index definition consumed by the storage engine.
///
/// Durable WAL and snapshot codecs own their persisted representations in the
/// composition layer. This descriptor deliberately contains only the data
/// required to construct an index and therefore keeps MVCC independent from
/// a particular persistence format generation.
#[derive(Debug, Clone)]
pub struct IndexDefinition {
    pub name: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub column_ids: Vec<i32>,
    pub data_types: Vec<DataType>,
    pub is_unique: bool,
    pub index_type: IndexType,
    pub hnsw_m: Option<u16>,
    pub hnsw_ef_construction: Option<u16>,
    pub hnsw_ef_search: Option<u16>,
    pub hnsw_distance_metric: Option<u8>,
    pub partial_predicate: Option<crate::index::PartialIndexPredicateMetadata>,
    /// Prepared logical-to-physical key mapping supplied by the composition
    /// layer for a catalog-bound external operator class.
    pub key_encoder: Option<crate::index::PreparedIndexKeyEncoder>,
}

#[cfg(not(test))]
const ROW_CLAIM_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const ROW_CLAIM_WAIT_TIMEOUT: Duration = Duration::from_millis(100);

/// Bounds time spent behind one unchanged claim owner.
///
/// A hot row or UNIQUE key can legitimately pass through several short-lived
/// transactions while a later writer waits. Treating the waiter's total queue
/// residence as one owner timeout makes a progressing writer convoy fail at
/// its tail. The budget therefore restarts only when the observed blocker
/// changes; spurious wakeups and an unchanged owner remain strictly bounded.
struct ClaimOwnerWaitBudget<T> {
    owner: Option<T>,
    deadline: Instant,
}

impl<T: Copy + Eq> ClaimOwnerWaitBudget<T> {
    fn new() -> Self {
        Self {
            owner: None,
            deadline: Instant::now() + ROW_CLAIM_WAIT_TIMEOUT,
        }
    }

    fn remaining(&mut self, owner: T) -> Option<Duration> {
        let now = Instant::now();
        if self.owner != Some(owner) {
            self.owner = Some(owner);
            self.deadline = now + ROW_CLAIM_WAIT_TIMEOUT;
        }
        (now < self.deadline).then(|| self.deadline - now)
    }
}

/// Type alias for version lists - uses SmallVec to avoid heap allocation
/// for the common case of a single version per row within a transaction.
type VersionList = SmallVec<[RowVersion; 1]>;

type RecoveryIndexTransition = (Arc<dyn Index>, Option<Vec<Value>>, Option<Vec<Value>>);

struct ExternalIndexRemoval {
    index: Arc<dyn Index>,
    values: Vec<Value>,
    row_id: i64,
}

pub use crate::traits::index_values_for_row;

fn index_affecting_columns_changed(index: &dyn Index, old_row: &Row, new_row: &Row) -> bool {
    if index.column_ids().iter().any(|&col_id| {
        let col_idx = col_id as usize;
        old_row.get(col_idx) != new_row.get(col_idx)
    }) {
        return true;
    }

    index.partial_predicate().is_some_and(|predicate| {
        predicate.referenced_column_ids().iter().any(|&col_id| {
            let col_idx = col_id as usize;
            old_row.get(col_idx) != new_row.get(col_idx)
        })
    })
}

/// Group key using `CompactArc<Value>` to avoid cloning during aggregation.
/// Uses Arc::clone (O(1) atomic increment) instead of Value::clone (deep copy).
pub use crate::traits::GroupKey;

/// Hash map for GroupKey with randomized hashing (HashDoS resistant)
type GroupKeyMap<V> = AHashMap<GroupKey, V>;

/// Result of storage-level grouped aggregation
pub use crate::traits::GroupedAggregateResult;

/// Represents a specific version of a row with complete data
///

#[derive(Clone)]
pub struct RowVersion {
    /// Transaction that created this version
    pub txn_id: i64,
    /// Transaction that deleted this version (0 if not deleted)
    pub deleted_at_txn_id: i64,
    /// Complete row data
    pub data: Row,
    /// Timestamp when this version was created
    pub create_time: i64,
}

impl RowVersion {
    /// Creates a new row version
    pub fn new(txn_id: i64, data: Row) -> Self {
        Self {
            txn_id,
            deleted_at_txn_id: 0,
            data,
            create_time: get_fast_timestamp(),
        }
    }

    /// Creates a new row version with a pre-computed timestamp
    /// This avoids calling SystemTime::now() for each row in bulk operations
    #[inline]
    pub fn new_with_timestamp(txn_id: i64, data: Row, create_time: i64) -> Self {
        Self {
            txn_id,
            deleted_at_txn_id: 0,
            data,
            create_time,
        }
    }

    /// Creates a new deleted version
    pub fn new_deleted(txn_id: i64, data: Row) -> Self {
        Self {
            txn_id,
            deleted_at_txn_id: txn_id,
            data,
            create_time: get_fast_timestamp(),
        }
    }

    /// Creates a new deleted version with a pre-computed timestamp
    #[inline]
    pub fn new_deleted_with_timestamp(txn_id: i64, data: Row, create_time: i64) -> Self {
        Self {
            txn_id,
            deleted_at_txn_id: txn_id,
            data,
            create_time,
        }
    }

    /// Returns true if this version has been marked as deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at_txn_id != 0
    }
}

impl fmt::Debug for RowVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowVersion")
            .field("txn_id", &self.txn_id)
            .field("deleted_at_txn_id", &self.deleted_at_txn_id)
            .field("create_time", &self.create_time)
            .finish()
    }
}

impl fmt::Display for RowVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RowVersion{{TxnID: {}, DeletedAtTxnID: {}, CreateTime: {}}}",
            self.txn_id, self.deleted_at_txn_id, self.create_time
        )
    }
}

/// Entry in the version chain (linked list of versions)
/// Uses Arc for the prev pointer to enable O(1) cloning of the chain
///
/// # Memory Optimization
/// - `arena_idx`: Uses `Option<NonZeroU64>` (8 bytes) instead of `Option<usize>` (16 bytes)
///   Stored as `idx + 1` to enable niche optimization (0 = None)
struct VersionChainEntry {
    /// Current version
    version: RowVersion,
    /// Previous version in the chain (Arc allows cheap cloning)
    prev: Option<Arc<VersionChainEntry>>,
    /// Index into the arena for zero-copy access (None if data not in arena)
    /// Stored as `idx + 1` to enable niche optimization (NonZeroU64)
    arena_idx: Option<NonZeroU64>,
}

/// Count the depth of a version chain by traversing prev pointers.
/// O(k) where k is the chain length (typically <= max_version_history).
#[inline]
fn count_chain_depth(entry: &VersionChainEntry) -> usize {
    let mut depth = 1;
    let mut current = &entry.prev;
    while let Some(prev) = current {
        depth += 1;
        current = &prev.prev;
    }
    depth
}

/// Convert a public retention duration into a conservative timestamp cutoff.
/// Durations wider than the signed nanosecond domain retain all positive-time
/// history instead of truncating into a future cutoff.
#[inline]
fn retention_cutoff(now: i64, retention: std::time::Duration) -> i64 {
    let nanos = i64::try_from(retention.as_nanos()).unwrap_or(i64::MAX);
    now.saturating_sub(nanos)
}

/// Remove a physical column from every retained version in a row's history.
///
/// Historical versions share their tail through `Arc`; `make_mut` preserves
/// snapshots while giving the DDL owner a coherent rewritten chain.
fn remove_column_from_version_chain(entry: &mut VersionChainEntry, column_index: usize) {
    let mut current = entry;
    loop {
        current.version.data.remove_column(column_index);
        let Some(previous) = &mut current.prev else {
            break;
        };
        current = Arc::make_mut(previous);
    }
}

/// Estimate the live hot-memory footprint of one script/database value.
///
/// Fixed-width variants are already represented by the 16-byte `Value` enum.
/// Variable-width variants add their payload length only when the payload is
/// not stored inline. This is a governor signal, not a malloc profiler.
#[inline]
fn estimate_value_hot_bytes(value: &Value) -> usize {
    let base = std::mem::size_of::<Value>();
    match value {
        Value::Text(s) if s.is_heap() => base.saturating_add(s.len()),
        Value::Extension(bytes) => base.saturating_add(bytes.len()),
        _ => base,
    }
}

/// Estimate the live hot-memory footprint of one committed row.
#[inline]
pub fn estimate_row_hot_bytes(row: &Row) -> usize {
    row.as_slice()
        .iter()
        .fold(std::mem::size_of::<Row>(), |total, value| {
            total.saturating_add(estimate_value_hot_bytes(value))
        })
}

#[inline]
fn accumulate_hot_bytes_delta(
    old_bytes: usize,
    new_bytes: usize,
    add: &mut usize,
    sub: &mut usize,
) {
    if new_bytes > old_bytes {
        *add = add.saturating_add(new_bytes - old_bytes);
    } else if old_bytes > new_bytes {
        *sub = sub.saturating_add(old_bytes - new_bytes);
    }
}

/// Convert arena index (usize) to compact representation (Option<NonZeroU64>)
/// Stores `idx + 1` so that 0 can represent None via niche optimization
#[inline(always)]
fn pack_arena_idx(idx: usize) -> Option<NonZeroU64> {
    // idx + 1 is always > 0, supports full usize range on 64-bit systems
    NonZeroU64::new((idx as u64).saturating_add(1))
}

/// Convert compact arena index back to usize
/// Returns None if the stored value was None
#[inline(always)]
fn unpack_arena_idx(packed: Option<NonZeroU64>) -> Option<usize> {
    packed.map(|nz| (nz.get() - 1) as usize)
}

/// Convert Option<usize> to Option<NonZeroUsize> for RowIndex
/// Stores `idx + 1` so that 0 can represent None via niche optimization
#[inline(always)]
fn pack_row_arena_idx(idx: Option<usize>) -> Option<NonZeroUsize> {
    idx.and_then(|i| NonZeroUsize::new(i.wrapping_add(1)))
}

/// Convert Option<NonZeroUsize> back to Option<usize> for RowIndex
#[inline(always)]
fn unpack_row_arena_idx(packed: Option<NonZeroUsize>) -> Option<usize> {
    packed.map(|nz| nz.get().wrapping_sub(1))
}

/// What the transaction established about row existence before its first
/// mutation. An INSERT followed by UPDATE must retain `Absent` so concurrent
/// explicit-PK insertion is still detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteObservation {
    /// No committed row existed when the transaction first wrote this row ID.
    Absent,
    /// A concrete hot MVCC version was read and is available in read_version.
    HotVersion,
    /// A previously visible candidate was serialized by a row claim. This is
    /// used for cold rows and unchanged residual candidates that have no local
    /// version, but are not INSERTs.
    ClaimedExisting,
}

/// Tracks write provenance and the version read for conflict detection.
#[derive(Clone)]
pub struct WriteSetEntry {
    /// Existence observation made before the first mutation/claim.
    pub observation: WriteObservation,
    /// Concrete hot version when one was observed.
    pub read_version: Option<RowVersion>,
}

impl WriteSetEntry {
    #[inline]
    fn from_read(read_version: Option<RowVersion>) -> Self {
        let observation = if read_version.is_some() {
            WriteObservation::HotVersion
        } else {
            WriteObservation::Absent
        };
        Self {
            observation,
            read_version,
        }
    }

    #[inline]
    fn claimed() -> Self {
        Self {
            observation: WriteObservation::ClaimedExisting,
            read_version: None,
        }
    }
}

// ============================================================================
// HashMap Pool for TransactionVersionStore
// ============================================================================
//
// Pools recycled HashMaps to avoid allocation overhead when creating many
// short-lived transactions. Each auto-commit INSERT creates a transaction,
// and without pooling, this causes ~5.5KB allocation per INSERT.
//
// With pooling, maps are returned to the pool on Drop and reused by the next
// transaction, reducing allocation churn by ~99% for bulk insert workloads.

/// Maximum number of maps to keep in each pool.
/// Prevents unbounded memory growth while allowing reasonable reuse.
const MAP_POOL_MAX_SIZE: usize = 64;

/// Maps above this capacity represent an exceptional transaction peak and are
/// released instead of being retained process-wide by the reuse pool.
const MAP_POOL_MAX_RETAINED_CAPACITY: usize = 4096;

/// Global pool for VersionList maps (local_versions in TransactionVersionStore)
static VERSION_LIST_MAP_POOL: Mutex<Vec<I64Map<VersionList>>> = Mutex::new(Vec::new());

/// Global pool for WriteSetEntry maps (write_set in TransactionVersionStore)
static WRITE_SET_MAP_POOL: Mutex<Vec<I64Map<WriteSetEntry>>> = Mutex::new(Vec::new());

/// Get a VersionList map from pool or create a new one
#[inline]
fn get_version_list_map() -> I64Map<VersionList> {
    if let Some(map) = VERSION_LIST_MAP_POOL.lock().pop() {
        map
    } else {
        new_i64_map_with_capacity(TX_VERSION_MAP_INITIAL_CAPACITY)
    }
}

/// Get a WriteSetEntry map from pool or create a new one
#[inline]
fn get_write_set_map() -> I64Map<WriteSetEntry> {
    if let Some(map) = WRITE_SET_MAP_POOL.lock().pop() {
        map
    } else {
        new_i64_map_with_capacity(TX_VERSION_MAP_INITIAL_CAPACITY)
    }
}

/// Return a VersionList map to the pool for reuse
#[inline]
fn return_version_list_map(mut map: I64Map<VersionList>) {
    if map.capacity() > MAP_POOL_MAX_RETAINED_CAPACITY {
        return;
    }
    map.clear();
    let mut pool = VERSION_LIST_MAP_POOL.lock();
    if pool.len() < MAP_POOL_MAX_SIZE {
        pool.push(map);
    }
    // If pool is full, map is dropped (deallocated)
}

/// Return a WriteSetEntry map to the pool for reuse
#[inline]
fn return_write_set_map(mut map: I64Map<WriteSetEntry>) {
    if map.capacity() > MAP_POOL_MAX_RETAINED_CAPACITY {
        return;
    }
    map.clear();
    let mut pool = WRITE_SET_MAP_POOL.lock();
    if pool.len() < MAP_POOL_MAX_SIZE {
        pool.push(map);
    }
    // If pool is full, map is dropped (deallocated)
}

/// Clear the transaction version map pools.
/// Call this when dropping the database to release pooled memory.
pub fn clear_version_map_pools() {
    VERSION_LIST_MAP_POOL.lock().clear();
    WRITE_SET_MAP_POOL.lock().clear();
}

/// Capacity hint for transaction version maps - used by pool functions
const TX_VERSION_MAP_INITIAL_CAPACITY: usize = 16;

/// Lightweight row index for deferred materialization
///
/// Instead of cloning row data during scans, we return indices that can be
/// materialized later. This enables zero-copy filtering and limiting.
///
/// # Performance
/// For `SELECT * FROM t WHERE x > 100 LIMIT 10` on 100K rows:
/// - Old: Clone 100K rows, filter to 50K, limit to 10 (100K allocations)
/// - New: Get 100K indices, filter to 50K, limit to 10, clone 10 (10 allocations)
///
/// # Memory Optimization
/// Uses `Option<NonZeroUsize>` (8 bytes) instead of `Option<usize>` (16 bytes)
/// for arena_idx, reducing struct size from 24 to 16 bytes (33% smaller).
#[derive(Clone, Copy, Debug)]
pub struct RowIndex {
    /// Row ID
    pub row_id: i64,
    /// Arena index (None if row data is not in arena, must clone from version)
    /// Stored as `idx + 1` to enable niche optimization (0 = None)
    arena_idx: Option<NonZeroUsize>,
}

impl RowIndex {
    /// Create a new RowIndex with the given row_id and arena index
    #[inline(always)]
    pub fn new(row_id: i64, arena_idx: Option<usize>) -> Self {
        Self {
            row_id,
            arena_idx: pack_row_arena_idx(arena_idx),
        }
    }

    /// Get the arena index (unpacked to `Option<usize>`)
    #[inline(always)]
    pub fn arena_idx(&self) -> Option<usize> {
        unpack_row_arena_idx(self.arena_idx)
    }
}

/// Aggregate operation type for deferred aggregation
///
/// Used with `compute_aggregates()` to perform multiple aggregations in a single pass.
pub use crate::traits::AggregateOp;

/// Result of an aggregate operation
#[derive(Clone, Debug)]
pub enum AggregateResult {
    /// Count result
    Count(usize),
    /// Sum result (sum, count of non-null values)
    Sum(f64, usize),
    /// Min result
    Min(Option<Value>),
    /// Max result
    Max(Option<Value>),
    /// Avg result (sum, count of non-null values) - caller computes sum/count
    Avg(f64, usize),
}

/// Immutable seal-time view of every MVCC hole that cold storage cannot
/// represent on its own.
///
/// It is captured after the VersionStore's CowBTree snapshot. Transactions
/// that were still in flight at capture time and transactions excluded by any
/// active Snapshot reader remain in hot MVCC for a later seal cycle.
#[derive(Clone, Debug, Default)]
pub struct SealVisibilitySnapshot {
    max_visible_txn_id: Option<i64>,
    excluded_txn_ids: Arc<[i64]>,
}

impl SealVisibilitySnapshot {
    pub fn new(max_visible_txn_id: Option<i64>, mut excluded_txn_ids: Vec<i64>) -> Self {
        excluded_txn_ids.sort_unstable();
        excluded_txn_ids.dedup();
        Self {
            max_visible_txn_id,
            excluded_txn_ids: excluded_txn_ids.into(),
        }
    }

    #[inline]
    fn is_visible(&self, txn_id: i64) -> bool {
        // Recovery-owned rows predate every user snapshot.
        txn_id < 0
            || (txn_id > 0
                && self
                    .max_visible_txn_id
                    .is_none_or(|max_txn_id| txn_id <= max_txn_id)
                && self.excluded_txn_ids.binary_search(&txn_id).is_err())
    }
}

/// Internal accumulator for aggregations.
/// Sum/Avg use split i128 + f64 accumulators: i128 for integers (no overflow),
/// f64 for floats. Combined as `int as f64 + float` at finalization.
enum AggregateAccumulator {
    Count(usize),
    /// (int_sum, float_sum, count)
    Sum(i128, f64, usize),
    Min(Option<Value>),
    Max(Option<Value>),
    /// (int_sum, float_sum, count)
    Avg(i128, f64, usize),
}

/// Visibility checker trait - will be implemented by TransactionRegistry
///
/// This allows VersionStore to check visibility without circular dependencies
pub trait VisibilityChecker: Send + Sync {
    /// Check if a version created by `version_txn_id` is visible to `viewing_txn_id`
    fn is_visible(&self, version_txn_id: i64, viewing_txn_id: i64) -> bool;

    /// Get the current global sequence number
    fn get_current_sequence(&self) -> i64;

    /// Get all active transaction IDs (for cleanup operations)
    fn get_active_transaction_ids(&self) -> Vec<i64>;

    /// Check if a transaction was committed before a given commit sequence cutoff.
    ///
    /// Returns true if the transaction is committed AND its commit sequence
    /// is less than the cutoff. Used for consistent snapshot iteration to ensure
    /// only transactions committed before the snapshot point are included.
    ///
    /// Default implementation returns true for all committed transactions.
    fn is_committed_before(&self, _txn_id: i64, _cutoff_commit_seq: i64) -> bool {
        true // Default: no cutoff filtering
    }

    /// Capture whether committed versions are visible to every snapshot active
    /// at this seal boundary.
    ///
    /// A commit sequence cutoff alone is insufficient when a writer reserved
    /// its commit sequence before a snapshot began but completed publication
    /// afterwards. Such a writer is recorded in that snapshot's exclusion set
    /// and must stay in hot MVCC until the excluding snapshot has ended.
    fn capture_seal_visibility(&self) -> SealVisibilitySnapshot {
        SealVisibilitySnapshot::default()
    }

    /// Check if a transaction uses snapshot isolation.
    ///
    /// Under snapshot isolation, the arena-only fast path is unsafe because
    /// HEAD versions committed after the viewer's snapshot are not visible,
    /// and the correct behavior requires walking version chains to find older
    /// visible versions.
    fn needs_snapshot_isolation(&self, _txn_id: i64) -> bool {
        false // Default: ReadCommitted (arena fast path is safe)
    }

    /// Register one wait-for edge. Returns false when adding the edge would
    /// close a transaction deadlock cycle.
    fn register_row_wait(&self, _waiter_txn_id: i64, _owner_txn_id: i64) -> bool {
        true
    }

    /// Remove the current wait-for edge for a transaction.
    fn clear_row_wait(&self, _waiter_txn_id: i64) {}
}

enum VisibilityOwner {
    Registry(Arc<TransactionRegistry>),
    Custom(Arc<dyn VisibilityChecker>),
}

impl VisibilityChecker for VisibilityOwner {
    #[inline]
    fn is_visible(&self, version_txn_id: i64, viewing_txn_id: i64) -> bool {
        match self {
            Self::Registry(checker) => checker.is_visible(version_txn_id, viewing_txn_id),
            Self::Custom(checker) => checker.is_visible(version_txn_id, viewing_txn_id),
        }
    }

    #[inline]
    fn get_current_sequence(&self) -> i64 {
        match self {
            Self::Registry(checker) => checker.get_current_sequence(),
            Self::Custom(checker) => checker.get_current_sequence(),
        }
    }

    fn get_active_transaction_ids(&self) -> Vec<i64> {
        match self {
            Self::Registry(checker) => checker.get_active_transaction_ids(),
            Self::Custom(checker) => checker.get_active_transaction_ids(),
        }
    }

    fn is_committed_before(&self, txn_id: i64, cutoff_commit_seq: i64) -> bool {
        match self {
            Self::Registry(checker) => checker.is_committed_before(txn_id, cutoff_commit_seq),
            Self::Custom(checker) => checker.is_committed_before(txn_id, cutoff_commit_seq),
        }
    }

    fn capture_seal_visibility(&self) -> SealVisibilitySnapshot {
        match self {
            Self::Registry(checker) => checker.capture_seal_visibility(),
            Self::Custom(checker) => checker.capture_seal_visibility(),
        }
    }

    #[inline]
    fn needs_snapshot_isolation(&self, txn_id: i64) -> bool {
        match self {
            Self::Registry(checker) => checker.needs_snapshot_isolation(txn_id),
            Self::Custom(checker) => checker.needs_snapshot_isolation(txn_id),
        }
    }

    fn register_row_wait(&self, waiter_txn_id: i64, owner_txn_id: i64) -> bool {
        match self {
            Self::Registry(checker) => checker.register_row_wait(waiter_txn_id, owner_txn_id),
            Self::Custom(checker) => checker.register_row_wait(waiter_txn_id, owner_txn_id),
        }
    }

    fn clear_row_wait(&self, waiter_txn_id: i64) {
        match self {
            Self::Registry(checker) => checker.clear_row_wait(waiter_txn_id),
            Self::Custom(checker) => checker.clear_row_wait(waiter_txn_id),
        }
    }
}

/// Opaque snapshot of the version store at extraction time.
/// Used by `remove_sealed_rows` to detect concurrent commits.
pub struct ExtractionSnapshot {
    inner: radixdb_core::CowBTree<VersionChainEntry>,
}

/// Token holding pre-removal snapshot data needed for deferred index cleanup.
/// Created by `remove_sealed_rows`, consumed by `remove_sealed_index_entries`.
#[derive(Default)]
pub struct SealedIndexCleanup {
    /// Row IDs that were removed from the version store.
    pub removed_ids: Vec<i64>,
    /// CowBTree snapshot taken BEFORE version removal. Contains the row data
    /// needed to compute index values for removal.
    snapshot: Option<radixdb_core::CowBTree<VersionChainEntry>>,
}

/// VersionStore tracks the latest committed version of each row for a table
///
/// Uses `CowBTreeMap` (`RwLock<CowBTree>`) for the version store because:
/// - O(1) snapshot cloning for lock-free reads (critical for MVCC)
/// - Ordered iteration is free (B+ tree is sorted by key)
/// - MVCC has single-writer semantics per transaction, so concurrent map sharding is overhead
/// - Point lookups are O(log n) which is fast enough for typical row counts
/// - Eliminates the ~350μs sort overhead during full scans
///
/// Arena-based storage provides 50x+ faster full table scans by:
/// - Storing all row data contiguously in memory
/// - Returning slices instead of clones during iteration
/// - Eliminating per-row allocation overhead
pub struct VersionStore {
    /// Row versions indexed by row ID (CowBTree for O(1) snapshot cloning)
    versions: CowBTreeMap<VersionChainEntry>,
    /// The name of the table this store belongs to (SmartString for inline storage ≤24 bytes)
    table_name: SmartString,
    /// Table schema (Arc for zero-cost cloning on read)
    schema: RwLock<CompactArc<Schema>>,
    /// True once schema evolution can leave committed rows with a different
    /// physical width than the current logical schema.
    requires_row_normalization: AtomicBool,
    /// Indexes on this table (FxHashMap for fast string key lookups)
    indexes: RwLock<FxHashMap<String, Arc<dyn Index>>>,
    /// Whether this store has been closed
    closed: AtomicBool,
    /// Prevents close from crossing an in-flight row/index publication.
    mutation_gate: RwLock<()>,
    /// Auto-increment counter for tables without explicit PK
    auto_increment_counter: AtomicI64,
    /// Track which transaction has uncommitted changes to each row
    uncommitted_writes: RwLock<I64Map<i64>>,
    /// Transaction-local UNIQUE keys that are not yet represented by the
    /// committed index authority. Claims remain installed until the owning
    /// transaction reaches a terminal commit/rollback boundary.
    unique_key_claims: Mutex<unique_claims::SharedUniqueClaimMap>,
    /// Serializes the recheck/sleep boundary for row-claim waiters. Release
    /// paths take the same mutex before removing a claim and notifying, which
    /// prevents a notification from being lost between the failed claim check
    /// and `Condvar::wait_for`.
    claim_wait_mutex: Mutex<()>,
    claim_changed: parking_lot::Condvar,
    /// Registry-backed production checker or an explicitly installed test checker.
    visibility_checker: Option<VisibilityOwner>,
    /// Arena-based storage for zero-copy full table scans
    arena: RowArena,
    /// Zone maps for segment pruning (set by ANALYZE)
    /// Uses Arc to avoid cloning on every read - critical for high QPS workloads
    zone_maps: RwLock<Option<Arc<crate::volume::zonemap::TableZoneMap>>>,
    /// Monotonic owner for ANALYZE publication. Every committed data or schema
    /// mutation advances it before marking the installed map stale.
    zone_map_generation: AtomicU64,
    /// Maximum number of previous versions to keep per row (0 = unlimited)
    /// This limits memory growth during write-heavy operations.
    /// Default is 10 - enough for most concurrent transaction scenarios.
    max_version_history: usize,
    /// Count of committed non-deleted rows for O(1) COUNT(*) queries.
    /// Updated on commit: +1 for INSERT, -1 for DELETE.
    /// This is an optimization for the common case of autocommit queries.
    committed_row_count: AtomicUsize,
    /// Approximate byte budget of committed non-deleted rows still resident in hot MVCC memory.
    ///
    /// This intentionally tracks live hot row payload/order-of-magnitude memory pressure,
    /// not exact allocator ownership. It is updated next to `committed_row_count` and is
    /// used by higher-level governors to seal/prewarm/evict by bytes instead of rows only.
    committed_hot_bytes: AtomicUsize,
    /// Publication boundary for visibility-sensitive metadata reads.
    ///
    /// Commit code holds this lock exclusively from hot-version publication
    /// until the transaction becomes visible and matching cold tombstones have
    /// been published. Membership probes hold it shared across their complete
    /// hot+cold observation, preventing a mixed pre/post-commit answer.
    membership_fence: Arc<RwLock<()>>,
}

mod aggregate;
mod committed;
mod indexes;
mod lifecycle;
mod private;
mod read;
mod unique_claims;

pub use private::TransactionVersionStore;

impl Clone for VersionChainEntry {
    fn clone(&self) -> Self {
        Self {
            version: self.version.clone(),
            prev: self.prev.clone(), // Arc clone is O(1)
            arena_idx: self.arena_idx,
        }
    }
}

impl Drop for VersionChainEntry {
    fn drop(&mut self) {
        // Arc's default destruction follows `prev` recursively. One heavily
        // updated hot row can retain hundreds of thousands of visibility-safe
        // versions, so clearing it during checkpoint must not consume stack in
        // proportion to history depth. Detach uniquely owned nodes one by one;
        // a shared tail is left for its eventual last owner, whose same Drop
        // implementation will also dismantle it iteratively.
        let mut previous = self.prev.take();
        while let Some(shared) = previous {
            match Arc::try_unwrap(shared) {
                Ok(mut unique) => previous = unique.prev.take(),
                Err(shared) => {
                    drop(shared);
                    break;
                }
            }
        }
    }
}

impl fmt::Debug for VersionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VersionStore")
            .field("table_name", &self.table_name)
            .field("row_count", &self.row_count())
            .field("committed_hot_bytes", &self.committed_hot_bytes())
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish()
    }
}

#[cfg(test)]
mod tests;
