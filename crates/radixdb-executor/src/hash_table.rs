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

//! Optimized hash table for join operations.
//!
//! This module provides a specialized hash table designed for the build phase
//! of hash joins. Key optimizations:
//!
//! 1. **Pre-allocated**: Sized upfront based on build side cardinality
//! 2. **Cache-efficient**: Linear probing within cache lines
//! 3. **Zero-allocation probe**: Iterator returns indices without allocation
//! 4. **Full hash stored**: Quick rejection without row access
//!
//! # Memory Layout
//!
//! ```text
//! JoinHashTable
//! ├── bucket_heads: Vec<i32>    [bucket_count]     // First entry index per bucket
//! ├── entries: Vec<HashEntry>   [row_count]        // One per build row
//! └── bucket_mask: u64                             // For fast modulo
//!
//! HashEntry (16 bytes, cache-aligned)
//! ├── hash: u64     // Full hash for quick rejection
//! ├── row_idx: u32  // Index into build rows
//! └── next: u32     // Next in chain (EMPTY = end)
//! ```

use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};

use rustc_hash::FxHasher;

use radixdb_core::CompactArc;
use radixdb_core::{Row, Value};

/// Minimal sink used while a JOIN hash table and a probe accelerator are
/// populated in one pass. The optimizer owns the accelerator; the physical
/// hash foundation depends only on this lifecycle-neutral contract.
pub trait JoinHashObserver {
    /// Observe one canonical hash already computed for the hash table.
    fn insert_raw_hash(&mut self, hash: u64);

    /// Bytes retained by the observer and charged to the request budget.
    fn retained_bytes(&self) -> usize;
}

/// Immutable build-side state shared by every probe consumer of one physical
/// JOIN edge.
///
/// The row batch, key layout and hash table travel as one object. This makes
/// it impossible to attach a pre-built table to a different row ordering or a
/// different set of key columns, while cheap clones let a fused physical
/// pipeline reuse the same build state without rebuilding it.
#[derive(Clone)]
pub struct JoinHashState {
    build_rows: CompactArc<Vec<Row>>,
    key_indices: CompactArc<[usize]>,
    table: Arc<JoinHashTable>,
    /// One allocation is charged exactly once even when the immutable state is
    /// cloned by several physical consumers.
    _memory_reservation: Option<Arc<JoinMemoryReservation>>,
}

#[derive(Default)]
struct JoinMemoryUsage {
    retained_bytes: usize,
    peak_bytes: usize,
}

#[derive(Default)]
pub(crate) struct JoinMemoryOwner {
    usage: Mutex<JoinMemoryUsage>,
}

impl std::fmt::Debug for JoinMemoryOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JoinMemoryOwner")
            .field("retained_bytes", &self.retained_bytes())
            .field("peak_bytes", &self.peak_bytes())
            .finish()
    }
}

impl JoinMemoryOwner {
    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        bytes: usize,
        max_bytes: usize,
    ) -> Option<JoinMemoryReservation> {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next = usage.retained_bytes.checked_add(bytes)?;
        if next > max_bytes {
            return None;
        }
        usage.retained_bytes = next;
        usage.peak_bytes = usage.peak_bytes.max(next);
        Some(JoinMemoryReservation {
            owner: Arc::downgrade(self),
            bytes,
        })
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retained_bytes
    }

    pub(crate) fn peak_bytes(&self) -> usize {
        self.usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .peak_bytes
    }
}

#[doc(hidden)]
pub struct JoinMemoryReservation {
    owner: Weak<JoinMemoryOwner>,
    bytes: usize,
}

impl JoinMemoryReservation {
    /// Resize one live request-local reservation without opening a second
    /// accounting window. Blocking operators whose retained set grows one row
    /// at a time (Top-N, DISTINCT, sort runs) use this to share the same hard
    /// ceiling as JOIN hash/probe state.
    #[doc(hidden)]
    pub fn try_resize(&mut self, bytes: usize, max_bytes: usize) -> bool {
        let Some(owner) = self.owner.upgrade() else {
            // The request context has already gone away. No other owner can
            // compete for this budget, so only keep the local bookkeeping.
            self.bytes = bytes;
            return true;
        };
        let mut usage = owner
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let without_self = usage.retained_bytes.saturating_sub(self.bytes);
        let Some(next) = without_self.checked_add(bytes) else {
            return false;
        };
        if next > max_bytes {
            return false;
        }
        usage.retained_bytes = next;
        usage.peak_bytes = usage.peak_bytes.max(next);
        self.bytes = bytes;
        true
    }
}

impl std::fmt::Debug for JoinMemoryReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JoinMemoryReservation")
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Drop for JoinMemoryReservation {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let mut usage = owner
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        usage.retained_bytes = usage.retained_bytes.saturating_sub(self.bytes);
    }
}

impl JoinHashState {
    /// Build one immutable hash state for an already materialized physical
    /// batch. No row is copied: the state retains the exact batch identity.
    pub fn build(build_rows: CompactArc<Vec<Row>>, key_indices: &[usize]) -> Self {
        let table = JoinHashTable::build(&build_rows, key_indices);
        Self::from_table(build_rows, key_indices, table, None)
    }

    /// Build only when the immutable bucket/entry state fits its admission
    /// budget. `None` selects the bounded scan fallback rather than risking an
    /// allocator abort or process OOM.
    pub fn try_build(
        build_rows: CompactArc<Vec<Row>>,
        key_indices: &[usize],
        max_bytes: usize,
    ) -> Option<Self> {
        JoinHashTable::fits_retained_budget(build_rows.len(), max_bytes)
            .then(|| Self::build(build_rows, key_indices))
    }

    pub(crate) fn build_reserved(
        build_rows: CompactArc<Vec<Row>>,
        key_indices: &[usize],
        reservation: JoinMemoryReservation,
    ) -> Self {
        let table = JoinHashTable::build(&build_rows, key_indices);
        Self::from_table(build_rows, key_indices, table, Some(reservation))
    }

    /// Build one state while populating a bloom accelerator in the same pass.
    /// Keeping this constructor here prevents callers from pairing a table
    /// built for one key layout with metadata claiming another layout.
    pub fn build_with_bloom(
        build_rows: CompactArc<Vec<Row>>,
        key_indices: &[usize],
        observer: &mut impl JoinHashObserver,
    ) -> Self {
        let table = JoinHashTable::build_with_observer(&build_rows, key_indices, observer);
        Self::from_table(build_rows, key_indices, table, None)
    }

    pub(crate) fn build_with_bloom_reserved(
        build_rows: CompactArc<Vec<Row>>,
        key_indices: &[usize],
        observer: &mut impl JoinHashObserver,
        reservation: JoinMemoryReservation,
    ) -> Self {
        let table = JoinHashTable::build_with_observer(&build_rows, key_indices, observer);
        Self::from_table(build_rows, key_indices, table, Some(reservation))
    }

    fn from_table(
        build_rows: CompactArc<Vec<Row>>,
        key_indices: &[usize],
        table: JoinHashTable,
        reservation: Option<JoinMemoryReservation>,
    ) -> Self {
        assert_eq!(
            table.len(),
            build_rows.len(),
            "join hash state must contain one entry per build row"
        );
        Self {
            build_rows,
            key_indices: CompactArc::from(key_indices.to_vec()),
            table: Arc::new(table),
            _memory_reservation: reservation.map(Arc::new),
        }
    }

    /// Whether this state belongs to exactly this immutable batch and key
    /// layout. Pointer identity is intentional: equal values in a newly
    /// materialized batch do not prove equal physical row indices.
    pub fn matches(&self, build_rows: &CompactArc<Vec<Row>>, key_indices: &[usize]) -> bool {
        CompactArc::ptr_eq(&self.build_rows, build_rows) && self.key_indices.as_ref() == key_indices
    }

    #[inline]
    pub fn build_rows(&self) -> &CompactArc<Vec<Row>> {
        &self.build_rows
    }

    #[inline]
    pub fn table(&self) -> &Arc<JoinHashTable> {
        &self.table
    }

    #[cfg(test)]
    fn shares_allocations_with(&self, other: &Self) -> bool {
        CompactArc::ptr_eq(&self.build_rows, &other.build_rows)
            && Arc::ptr_eq(&self.table, &other.table)
    }
}

/// Sentinel value indicating end of chain or empty bucket.
const EMPTY: u32 = u32::MAX;

/// Minimum number of buckets (must be power of 2).
const MIN_BUCKETS: usize = 16;

/// Default admission boundary for the hash index itself. Build rows are owned
/// by the upstream relation; this limit prevents the JOIN from allocating an
/// additional unbounded bucket/entry structure over them.
#[doc(hidden)]
pub const DEFAULT_JOIN_HASH_STATE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// A hash entry in the join hash table.
///
/// Each entry represents one row from the build side.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct HashEntry {
    /// Full 64-bit hash for quick rejection during probe.
    /// Comparing hashes first avoids touching row data for non-matches.
    hash: u64,
    /// Index into the build rows vector.
    row_idx: u32,
    /// Index of next entry in the chain (EMPTY = end of chain).
    next: u32,
}

impl HashEntry {
    #[inline]
    fn new(hash: u64, row_idx: u32, next: u32) -> Self {
        Self {
            hash,
            row_idx,
            next,
        }
    }
}

/// Optimized hash table for join operations.
///
/// This hash table is specifically designed for the build phase of hash joins.
/// It uses chaining with linked entries stored in a flat vector for cache efficiency.
///
/// # Example
///
/// ```ignore
/// // Build phase
/// let mut table = JoinHashTable::with_capacity(build_rows.len());
/// for (idx, row) in build_rows.iter().enumerate() {
///     let hash = hash_row_keys(row, &key_indices);
///     table.insert(hash, idx as u32);
/// }
///
/// // Probe phase
/// for probe_row in probe_rows {
///     let hash = hash_row_keys(probe_row, &probe_key_indices);
///     for build_idx in table.probe(hash) {
///         // Verify actual key equality and produce output
///     }
/// }
/// ```
pub struct JoinHashTable {
    /// First entry index for each bucket (-1 if empty).
    /// Sized to power of 2 for fast modulo via bitwise AND.
    bucket_heads: Vec<i32>,

    /// Flat storage of all entries.
    /// One entry per build row.
    entries: Vec<HashEntry>,

    /// Mask for computing bucket index: bucket = hash & mask
    bucket_mask: u64,

    /// Number of entries inserted.
    len: usize,
}

impl JoinHashTable {
    fn bucket_count_for_rows(row_count: usize) -> Option<usize> {
        row_count
            .checked_mul(4)
            .map(|scaled| scaled / 3)
            .map(|scaled| scaled.max(MIN_BUCKETS))?
            .checked_next_power_of_two()
    }

    /// Exact retained size of bucket heads and compact entries before Vec
    /// allocator rounding. Overflow or row indices outside u32 are rejected.
    pub fn estimated_retained_bytes(row_count: usize) -> Option<usize> {
        if row_count > u32::MAX as usize {
            return None;
        }
        let buckets = Self::bucket_count_for_rows(row_count)?;
        buckets
            .checked_mul(std::mem::size_of::<i32>())?
            .checked_add(row_count.checked_mul(std::mem::size_of::<HashEntry>())?)
    }

    #[inline]
    pub fn fits_retained_budget(row_count: usize, max_bytes: usize) -> bool {
        Self::estimated_retained_bytes(row_count).is_some_and(|bytes| bytes <= max_bytes)
    }

    /// Create a new hash table with capacity for the given number of rows.
    ///
    /// The table is pre-allocated to avoid resizing during build.
    /// Bucket count is sized to achieve ~75% load factor.
    pub fn with_capacity(row_count: usize) -> Self {
        // Choose bucket count as next power of 2 >= row_count * 4/3.
        // Runtime callers apply `fits_retained_budget` first; this constructor
        // remains infallible for already-admitted sizes.
        let bucket_count = Self::bucket_count_for_rows(row_count)
            .expect("join hash table capacity exceeds addressable range");
        assert!(
            row_count <= u32::MAX as usize,
            "join hash table row index exceeds u32"
        );

        let bucket_mask = (bucket_count - 1) as u64;

        Self {
            bucket_heads: vec![-1; bucket_count],
            entries: Vec::with_capacity(row_count),
            bucket_mask,
            len: 0,
        }
    }

    /// Create an empty hash table (for cases where build side is empty).
    pub fn empty() -> Self {
        Self {
            bucket_heads: vec![-1; MIN_BUCKETS],
            entries: Vec::new(),
            bucket_mask: (MIN_BUCKETS - 1) as u64,
            len: 0,
        }
    }

    /// Build a hash table from rows using the specified key indices.
    ///
    /// This is the main entry point for creating a join hash table.
    pub fn build(rows: &[Row], key_indices: &[usize]) -> Self {
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        let mut table = Self::with_capacity(rows.len());

        for (idx, row) in rows.iter().enumerate() {
            let hash = hash_row_keys(row, key_indices);
            table.insert(hash, idx as u32);
        }

        #[cfg(feature = "bench-harness")]
        radixdb_storage::instrumentation::record_hash_build(rows.len(), started.elapsed());
        table
    }

    /// Build hash table and populate bloom filter in a single pass.
    ///
    /// This is more efficient than building separately because we only
    /// extract and hash key values once for both structures.
    ///
    /// # Arguments
    /// * `rows` - Build side rows
    /// * `key_indices` - Indices of join key columns
    /// * `bloom_builder` - Bloom filter builder to populate
    ///
    /// # Returns
    /// The built hash table (bloom filter is populated in-place)
    pub fn build_with_observer(
        rows: &[Row],
        key_indices: &[usize],
        observer: &mut impl JoinHashObserver,
    ) -> Self {
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        let mut table = Self::with_capacity(rows.len());

        for (idx, row) in rows.iter().enumerate() {
            // Extract keys and compute hash once
            let hash = hash_row_keys(row, key_indices);

            // Insert into hash table
            table.insert(hash, idx as u32);

            // Insert into bloom filter using the same pre-computed hash
            // This avoids re-hashing the same key values
            observer.insert_raw_hash(hash);
        }

        #[cfg(feature = "bench-harness")]
        radixdb_storage::instrumentation::record_hash_build(rows.len(), started.elapsed());
        table
    }

    /// Insert a row index with its pre-computed hash.
    ///
    /// # Arguments
    /// * `hash` - The hash of the row's key columns
    /// * `row_idx` - The index of the row in the build rows vector
    #[inline]
    pub fn insert(&mut self, hash: u64, row_idx: u32) {
        let bucket = (hash & self.bucket_mask) as usize;

        // Get current head of chain
        let old_head = self.bucket_heads[bucket];

        // Create new entry pointing to old head
        // Use cached len instead of Vec::len() to avoid repeated length checks
        let entry_idx = self.len as u32;
        let next = if old_head >= 0 {
            old_head as u32
        } else {
            EMPTY
        };
        self.entries.push(HashEntry::new(hash, row_idx, next));

        // Update bucket head to point to new entry
        self.bucket_heads[bucket] = entry_idx as i32;
        self.len += 1;
    }

    /// Probe the hash table for matching row indices.
    ///
    /// Returns an iterator that yields row indices for entries
    /// with matching hashes. The caller must verify actual key
    /// equality for each returned index (to handle hash collisions).
    ///
    /// This is a zero-allocation operation - the iterator only
    /// holds a reference to the table.
    #[inline]
    pub fn probe(&self, hash: u64) -> ProbeIter<'_> {
        let bucket = (hash & self.bucket_mask) as usize;
        let first = self.bucket_heads[bucket];

        ProbeIter {
            table: self,
            hash,
            current: first,
        }
    }

    /// Start a resumable zero-allocation probe. Unlike `ProbeIter`, the cursor
    /// owns no borrow and can therefore live inside a streaming operator while
    /// that operator mutates its other state between emitted matches.
    #[inline]
    pub fn probe_cursor(&self, hash: u64) -> ProbeCursor {
        let bucket = (hash & self.bucket_mask) as usize;
        ProbeCursor {
            hash,
            current: self.bucket_heads[bucket],
        }
    }

    /// Advance one resumable probe and return the next matching row index.
    #[inline]
    pub fn probe_next(&self, cursor: &mut ProbeCursor) -> Option<usize> {
        while cursor.current >= 0 {
            let entry = &self.entries[cursor.current as usize];
            cursor.current = if entry.next == EMPTY {
                -1
            } else {
                entry.next as i32
            };
            if entry.hash == cursor.hash {
                return Some(entry.row_idx as usize);
            }
        }
        None
    }

    /// Get the number of entries in the table.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the table is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the number of buckets.
    #[inline]
    pub fn bucket_count(&self) -> usize {
        self.bucket_heads.len()
    }

    /// Get the load factor (entries / buckets).
    #[inline]
    pub fn load_factor(&self) -> f64 {
        self.len as f64 / self.bucket_heads.len() as f64
    }
}

/// Zero-allocation iterator over probe results.
///
/// Yields row indices for entries whose hash matches the probe hash.
/// The caller must verify actual key equality for each returned index.
pub struct ProbeIter<'a> {
    table: &'a JoinHashTable,
    hash: u64,
    current: i32,
}

/// Borrow-free position in one hash bucket chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProbeCursor {
    hash: u64,
    current: i32,
}

impl Iterator for ProbeIter<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        while self.current >= 0 {
            let entry = &self.table.entries[self.current as usize];
            self.current = if entry.next == EMPTY {
                -1
            } else {
                entry.next as i32
            };

            // Only return if hash matches (quick rejection for non-matches)
            if entry.hash == self.hash {
                return Some(entry.row_idx as usize);
            }
        }
        None
    }
}

// ============================================================================
// Hashing Utilities
// ============================================================================

/// Hash values at given indices using a get function.
///
/// This is a generic version that works with any type that provides indexed access
/// to values (Row, RowRef, etc.). Uses the same FxHash algorithm as hash_row_keys.
#[inline]
pub fn hash_keys_with<'a, F>(key_indices: &[usize], get_value: F) -> u64
where
    F: Fn(usize) -> Option<&'a Value>,
{
    let mut hasher = FxHasher::default();

    for &idx in key_indices {
        if let Some(value) = get_value(idx) {
            hash_value(&mut hasher, value);
        } else {
            // NULL marker - use a sentinel that's unlikely to collide
            0xDEADBEEF_u64.hash(&mut hasher);
        }
    }

    hasher.finish()
}

/// Hash row key columns into a single u64.
///
/// Uses FxHasher which is optimized for trusted keys in embedded database context.
/// This is the same algorithm used in utils.rs but kept here to avoid circular deps.
#[inline]
pub fn hash_row_keys(row: &Row, key_indices: &[usize]) -> u64 {
    let mut hasher = FxHasher::default();

    for &idx in key_indices {
        if let Some(value) = row.get(idx) {
            hash_value(&mut hasher, value);
        } else {
            // NULL marker - use a sentinel that's unlikely to collide
            0xDEADBEEF_u64.hash(&mut hasher);
        }
    }

    hasher.finish()
}

/// Hash a single value into a hasher.
#[inline]
fn hash_value<H: Hasher>(hasher: &mut H, value: &Value) {
    value.hash(hasher);
}

/// Verify that two rows have equal key values.
///
/// Used after hash matching to confirm actual equality (handling hash collisions).
#[inline]
pub fn verify_key_equality(row1: &Row, row2: &Row, indices1: &[usize], indices2: &[usize]) -> bool {
    debug_assert_eq!(indices1.len(), indices2.len());

    for (&idx1, &idx2) in indices1.iter().zip(indices2.iter()) {
        let (Some(value1), Some(value2)) = (row1.get(idx1), row2.get(idx2)) else {
            return false;
        };

        if value1.is_null() || value2.is_null() || value1 != value2 {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_entry_retains_only_hash_and_compact_row_reference() {
        assert_eq!(std::mem::size_of::<HashEntry>(), 16);
    }

    #[test]
    fn immutable_hash_state_reuses_only_exact_batch_and_key_layout() {
        let rows = CompactArc::new(vec![make_row(vec![1, 10]), make_row(vec![2, 20])]);
        let state = JoinHashState::build(CompactArc::clone(&rows), &[0]);
        let reused = state.clone();

        assert!(state.matches(&rows, &[0]));
        assert!(!state.matches(&rows, &[1]));
        assert!(!state.matches(&CompactArc::new((*rows).clone()), &[0]));
        assert!(state.shares_allocations_with(&reused));
    }

    #[test]
    fn hash_state_admission_counts_buckets_and_entries_before_allocation() {
        let retained = JoinHashTable::estimated_retained_bytes(100).unwrap();
        assert_eq!(retained, 256 * std::mem::size_of::<i32>() + 100 * 16);
        assert!(JoinHashTable::fits_retained_budget(100, retained));
        assert!(!JoinHashTable::fits_retained_budget(100, retained - 1));
        assert!(JoinHashTable::estimated_retained_bytes(u32::MAX as usize + 1).is_none());
    }

    use radixdb_core::DataType;

    fn make_row(values: Vec<i64>) -> Row {
        Row::from_values(values.into_iter().map(Value::integer).collect())
    }

    #[test]
    fn test_basic_insert_and_probe() {
        let mut table = JoinHashTable::with_capacity(4);

        // Insert some entries
        table.insert(100, 0);
        table.insert(200, 1);
        table.insert(100, 2); // Same hash as first entry
        table.insert(300, 3);

        assert_eq!(table.len(), 4);

        // Probe for hash 100 should find entries 0 and 2
        let matches: Vec<_> = table.probe(100).collect();
        assert_eq!(matches.len(), 2);
        assert!(matches.contains(&0));
        assert!(matches.contains(&2));

        // Probe for hash 200 should find entry 1
        let matches: Vec<_> = table.probe(200).collect();
        assert_eq!(matches, vec![1]);

        // Probe for non-existent hash should find nothing
        let matches: Vec<_> = table.probe(999).collect();
        assert!(matches.is_empty());
    }

    #[test]
    fn test_build_from_rows() {
        let rows = vec![
            make_row(vec![1, 10]),
            make_row(vec![2, 20]),
            make_row(vec![1, 30]), // Same key as first row
            make_row(vec![3, 40]),
        ];

        let key_indices = vec![0]; // Key on first column
        let table = JoinHashTable::build(&rows, &key_indices);

        assert_eq!(table.len(), 4);

        // Probe for key=1
        let hash = hash_row_keys(&rows[0], &key_indices);
        let matches: Vec<_> = table.probe(hash).collect();
        assert_eq!(matches.len(), 2);
    }

    #[cfg(feature = "bench-harness")]
    #[test]
    fn runtime_profile_observes_bulk_hash_build_under_parallel_tests() {
        // Runtime instrumentation is process-wide by contract. Other executor
        // tests may build hash tables concurrently, so this test must prove
        // the contribution of its own build without claiming exclusive
        // ownership of the global counters.
        let before = radixdb_storage::instrumentation::snapshot().runtime_profile;
        let rows = vec![make_row(vec![1]), make_row(vec![2]), make_row(vec![3])];

        let _table = JoinHashTable::build(&rows, &[0]);

        let after = radixdb_storage::instrumentation::snapshot().runtime_profile;
        let delta = after.delta(before);
        assert!(delta.hash_build_calls >= 1);
        assert!(delta.hash_build_rows >= 3);
        assert!(delta.hash_build_nanos > 0);
    }

    #[test]
    fn test_empty_table() {
        let table = JoinHashTable::empty();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);

        let matches: Vec<_> = table.probe(100).collect();
        assert!(matches.is_empty());
    }

    #[test]
    fn test_load_factor() {
        let mut table = JoinHashTable::with_capacity(100);

        for i in 0..100 {
            table.insert(i as u64, i as u32);
        }

        // With 100 entries, load factor depends on bucket count
        // We target ~75% load factor, but it can vary
        let load = table.load_factor();
        assert!(
            load > 0.3 && load <= 1.0,
            "Load factor {} out of expected range",
            load
        );
        // Verify we have all entries
        assert_eq!(table.len(), 100);
    }

    #[test]
    fn test_verify_key_equality() {
        let row1 = Row::from_values(vec![Value::integer(1), Value::text("hello")]);
        let row2 = Row::from_values(vec![Value::integer(1), Value::text("hello")]);
        let row3 = Row::from_values(vec![Value::integer(2), Value::text("hello")]);

        assert!(verify_key_equality(&row1, &row2, &[0, 1], &[0, 1]));
        assert!(!verify_key_equality(&row1, &row3, &[0, 1], &[0, 1]));
    }

    #[test]
    fn test_hash_row_keys() {
        let row1 = make_row(vec![1, 2, 3]);
        let row2 = make_row(vec![1, 2, 3]);
        let row3 = make_row(vec![1, 2, 4]);

        let indices = vec![0, 1];

        // Same keys should produce same hash
        assert_eq!(
            hash_row_keys(&row1, &indices),
            hash_row_keys(&row2, &indices)
        );

        // Different values in non-key column shouldn't affect hash
        let row4 = make_row(vec![1, 2, 999]);
        assert_eq!(
            hash_row_keys(&row1, &indices),
            hash_row_keys(&row4, &indices)
        );

        // Different keys should (usually) produce different hash
        // This isn't guaranteed but is very likely
        assert_ne!(hash_row_keys(&row1, &[0, 2]), hash_row_keys(&row3, &[0, 2]));
    }

    fn assert_join_key_equal(left: Value, right: Value) {
        let left_row = Row::from_values(vec![left]);
        let right_row = Row::from_values(vec![right]);

        assert_eq!(
            hash_row_keys(&left_row, &[0]),
            hash_row_keys(&right_row, &[0])
        );
        assert!(verify_key_equality(&left_row, &right_row, &[0], &[0]));
    }

    #[test]
    fn test_canonical_numeric_join_keys() {
        const TWO_POW_53: i64 = 9_007_199_254_740_992;

        assert_join_key_equal(Value::Integer(TWO_POW_53), Value::Float(TWO_POW_53 as f64));
        assert_join_key_equal(
            Value::Integer(TWO_POW_53 + 2),
            Value::Float((TWO_POW_53 + 2) as f64),
        );
        assert_join_key_equal(Value::Float(0.0), Value::Float(-0.0));
        assert_join_key_equal(Value::Integer(0), Value::Float(-0.0));
    }

    #[test]
    fn test_rounded_integer_neighbor_does_not_join() {
        const TWO_POW_53: i64 = 9_007_199_254_740_992;

        let build_rows = [Row::from_values(vec![Value::Integer(TWO_POW_53 + 1)])];
        let probe_row = Row::from_values(vec![Value::Float((TWO_POW_53 + 1) as f64)]);
        let probe_hash = hash_row_keys(&probe_row, &[0]);
        let mut table = JoinHashTable::with_capacity(1);
        table.insert(probe_hash, 0); // Force the collision verifier to run.

        assert!(!verify_key_equality(&probe_row, &build_rows[0], &[0], &[0]));
        let verified_matches = table
            .probe(probe_hash)
            .filter(|&build_idx| {
                verify_key_equality(&probe_row, &build_rows[build_idx], &[0], &[0])
            })
            .count();
        assert_eq!(verified_matches, 0);
    }

    #[test]
    fn test_nan_payloads_share_join_key_contract() {
        let nan1 = f64::from_bits(0x7ff8_0000_0000_0001);
        let nan2 = f64::from_bits(0x7ff8_0000_0000_0002);

        assert_join_key_equal(Value::Float(nan1), Value::Float(nan2));
    }

    #[test]
    fn test_null_keys_never_join() {
        let left = Row::from_values(vec![Value::Null(DataType::Integer)]);
        let right = Row::from_values(vec![Value::Null(DataType::Float)]);

        assert_eq!(hash_row_keys(&left, &[0]), hash_row_keys(&right, &[0]));
        assert!(!verify_key_equality(&left, &right, &[0], &[0]));
    }

    #[test]
    fn test_composite_keys_use_canonical_value_contract() {
        const TWO_POW_53: i64 = 9_007_199_254_740_992;

        let integer_key =
            Row::from_values(vec![Value::Integer(TWO_POW_53), Value::Text("same".into())]);
        let float_key = Row::from_values(vec![
            Value::Float(TWO_POW_53 as f64),
            Value::Text("same".into()),
        ]);
        let different_tail = Row::from_values(vec![
            Value::Float(TWO_POW_53 as f64),
            Value::Text("different".into()),
        ]);
        let null_tail = Row::from_values(vec![
            Value::Float(TWO_POW_53 as f64),
            Value::Null(DataType::Text),
        ]);

        assert_eq!(
            hash_row_keys(&integer_key, &[0, 1]),
            hash_row_keys(&float_key, &[0, 1])
        );
        assert!(verify_key_equality(
            &integer_key,
            &float_key,
            &[0, 1],
            &[0, 1]
        ));
        assert!(!verify_key_equality(
            &integer_key,
            &different_tail,
            &[0, 1],
            &[0, 1]
        ));
        assert!(!verify_key_equality(
            &integer_key,
            &null_tail,
            &[0, 1],
            &[0, 1]
        ));
    }

    #[test]
    fn test_row_and_indexed_get_hash_paths_are_identical() {
        let row = Row::from_values(vec![
            Value::Integer(9_007_199_254_740_992),
            Value::Float(-0.0),
            Value::Text("key".into()),
        ]);
        let indices = [0, 1, 2];

        assert_eq!(
            hash_row_keys(&row, &indices),
            hash_keys_with(&indices, |idx| row.get(idx))
        );
    }

    #[test]
    fn test_chain_collision() {
        // Force collisions by using a small bucket count
        let mut table = JoinHashTable {
            bucket_heads: vec![-1; 4], // Only 4 buckets
            entries: Vec::new(),
            bucket_mask: 3,
            len: 0,
        };

        // All these will go to bucket 0 (hash & 3 == 0)
        table.insert(0, 0);
        table.insert(4, 1);
        table.insert(8, 2);
        table.insert(12, 3);

        // Probe for each should find exactly one match
        assert_eq!(table.probe(0).count(), 1);
        assert_eq!(table.probe(4).count(), 1);
        assert_eq!(table.probe(8).count(), 1);
        assert_eq!(table.probe(12).count(), 1);
    }
}
