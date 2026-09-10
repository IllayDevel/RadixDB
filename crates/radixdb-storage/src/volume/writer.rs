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

//! Volume writer: freezes in-memory rows into a column-major frozen volume.
//!
//! The freeze operation takes a set of rows (from the hot buffer or snapshot
//! recovery) and converts them to column-major storage with zone maps and
//! pre-computed aggregate stats. This is done by a background thread during
//! the seal operation.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use ahash::AHashMap;
use rustc_hash::FxHashSet;

use crate::instrumentation;
use radixdb_core::SmartString;
use radixdb_core::{DataType, Error, LogicalTypeRef, Result, Row, Schema, Value};

use super::column::{ColumnData, ZoneMap};
use super::stats::VolumeAggregateStats;

/// Compact immutable row-id metadata for one cold volume.
///
/// Sealed volumes overwhelmingly contain ascending contiguous IDs.  Keeping
/// those IDs as runs removes the permanent eight-byte-per-row startup tax while
/// preserving exact IDs for sparse/compacted volumes.
#[derive(Clone, Default)]
pub struct RowIds {
    runs: Arc<[RowIdRun]>,
    len: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowIdRun {
    first: i64,
    start_index: usize,
    len: usize,
}

impl RowIds {
    pub fn try_from_sorted_iter(
        values: impl IntoIterator<Item = i64>,
    ) -> std::result::Result<Self, &'static str> {
        let mut runs: Vec<RowIdRun> = Vec::new();
        let mut len = 0usize;
        let mut previous = None;
        for value in values {
            if previous.is_some_and(|previous| value <= previous) {
                return Err("row_ids must be strictly ascending");
            }
            if let Some(last) = runs.last_mut() {
                if previous.and_then(|previous| previous.checked_add(1)) == Some(value) {
                    last.len += 1;
                } else {
                    runs.push(RowIdRun {
                        first: value,
                        start_index: len,
                        len: 1,
                    });
                }
            } else {
                runs.push(RowIdRun {
                    first: value,
                    start_index: 0,
                    len: 1,
                });
            }
            previous = Some(value);
            len += 1;
        }
        Ok(Self {
            runs: Arc::from(runs),
            len,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn retained_bytes(&self) -> usize {
        self.runs.len() * std::mem::size_of::<RowIdRun>()
    }

    #[inline]
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub fn get(&self, index: usize) -> Option<i64> {
        if index >= self.len {
            return None;
        }
        let run_index = self
            .runs
            .partition_point(|run| run.start_index <= index)
            .saturating_sub(1);
        let run = self.runs.get(run_index)?;
        let offset = index - run.start_index;
        debug_assert!(offset < run.len);
        run.first.checked_add(offset as i64)
    }

    #[inline]
    pub fn at(&self, index: usize) -> i64 {
        self.get(index).expect("row-id index out of bounds")
    }

    #[inline]
    pub fn first(&self) -> Option<i64> {
        self.get(0)
    }

    #[inline]
    pub fn last(&self) -> Option<i64> {
        self.len.checked_sub(1).and_then(|index| self.get(index))
    }

    pub fn binary_search(&self, target: &i64) -> std::result::Result<usize, usize> {
        let run_index = self.runs.partition_point(|run| {
            let last = run.first + (run.len - 1) as i64;
            last < *target
        });
        let Some(run) = self.runs.get(run_index) else {
            return Err(self.len);
        };
        if *target < run.first {
            return Err(run.start_index);
        }
        let offset = (*target - run.first) as usize;
        if offset < run.len {
            Ok(run.start_index + offset)
        } else {
            Err(run.start_index + run.len)
        }
    }

    pub fn partition_point(&self, mut predicate: impl FnMut(i64) -> bool) -> usize {
        let mut left = 0usize;
        let mut right = self.len;
        while left < right {
            let middle = left + (right - left) / 2;
            if predicate(self.at(middle)) {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        left
    }

    #[inline]
    pub fn iter(&self) -> RowIdsIter<'_> {
        RowIdsIter {
            row_ids: self,
            front: 0,
            back: self.len,
        }
    }

    pub fn to_vec(&self) -> Vec<i64> {
        self.iter().collect()
    }
}

impl std::fmt::Debug for RowIds {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq<Vec<i64>> for RowIds {
    fn eq(&self, other: &Vec<i64>) -> bool {
        self.len == other.len() && self.iter().eq(other.iter().copied())
    }
}

pub struct RowIdsIter<'a> {
    row_ids: &'a RowIds,
    front: usize,
    back: usize,
}

impl Iterator for RowIdsIter<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front >= self.back {
            return None;
        }
        let value = self.row_ids.get(self.front);
        self.front += 1;
        value
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back - self.front;
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for RowIdsIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front >= self.back {
            return None;
        }
        self.back -= 1;
        self.row_ids.get(self.back)
    }
}

impl ExactSizeIterator for RowIdsIter<'_> {}

// LazyColumns: materialized columns or artifact-backed metadata-only columns
// =============================================================================

/// Column storage for eager builders and artifact-backed volumes.
///
/// Production cold reads never initialize these slots: they address row-group
/// blocks through `ArtifactDataSource`. Metadata-only slots deliberately panic
/// on direct access so a missing artifact path cannot silently fall back to an
/// eager representation.
pub struct LazyColumns {
    /// Materialized columns for builders/tests; empty for artifact-backed data.
    slots: Vec<OnceLock<ColumnData>>,
    /// Column data types, always available without loading blocks.
    col_data_types: Vec<DataType>,
    materialized: bool,
}

impl LazyColumns {
    /// Create from pre-loaded columns (VolumeBuilder::finish(), eager load).
    /// All OnceLock slots are pre-initialized. No compressed store.
    pub fn eager(columns: Vec<ColumnData>, col_data_types: Vec<DataType>) -> Self {
        let slots: Vec<OnceLock<ColumnData>> = columns
            .into_iter()
            .map(|col| {
                let cell = OnceLock::new();
                let _ = cell.set(col);
                cell
            })
            .collect();
        Self {
            slots,
            col_data_types,
            materialized: true,
        }
    }

    /// Create columns with only data types (for cold-tier volumes).
    /// No columns, no compressed store. Column access will panic;
    /// the volume must be reloaded from disk before scanning.
    pub fn metadata_only(col_data_types: Vec<DataType>) -> Self {
        let col_count = col_data_types.len();
        Self {
            slots: (0..col_count).map(|_| OnceLock::new()).collect(),
            col_data_types,
            materialized: false,
        }
    }

    /// Create empty LazyColumns (for Scanner::empty()).
    pub fn empty() -> Self {
        Self {
            slots: Vec::new(),
            col_data_types: Vec::new(),
            materialized: true,
        }
    }

    /// Whether all OnceLock slots are initialized (eager mode).
    pub fn is_eager(&self) -> bool {
        self.materialized
    }

    /// Number of columns.
    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether there are no columns.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Get the DataType for a column without decompressing it.
    #[inline]
    pub fn data_type(&self, idx: usize) -> DataType {
        self.col_data_types[idx]
    }

    /// Iterator over all materialized columns.
    pub fn iter(&self) -> LazyColumnsIter<'_> {
        LazyColumnsIter {
            columns: self,
            idx: 0,
        }
    }

    /// Estimate retained materialized column bytes.
    pub fn memory_size(&self) -> usize {
        self.loaded_columns_memory_size()
    }

    /// Bytes used by already materialized/decompressed column bodies.
    pub fn loaded_columns_memory_size(&self) -> usize {
        self.slots
            .iter()
            .filter_map(|slot| slot.get())
            .map(ColumnData::memory_size)
            .sum()
    }

    /// Return the dictionary for a materialized dictionary-encoded column.
    pub fn get_column_dictionary(&self, col_idx: usize) -> Option<Arc<[SmartString]>> {
        if let Some(ColumnData::Dictionary { dictionary, .. }) =
            self.slots.get(col_idx).and_then(|s| s.get())
        {
            return Some(Arc::clone(dictionary));
        }
        None
    }

    /// Take ownership of all loaded columns, consuming the LazyColumns.
    /// Used by compress_and_release to avoid cloning.
    pub fn take_columns(self) -> Vec<ColumnData> {
        let mut result = Vec::with_capacity(self.slots.len());
        for slot in self.slots {
            if let Some(col) = slot.into_inner() {
                result.push(col);
            }
        }
        result
    }
}

impl std::ops::Index<usize> for LazyColumns {
    type Output = ColumnData;

    #[inline]
    fn index(&self, idx: usize) -> &ColumnData {
        if let Some(col) = self.slots[idx].get() {
            return col;
        }
        panic!(
            "BUG: column {} accessed directly on artifact-backed volume; use ArtifactDataSource",
            idx
        )
    }
}

/// Iterator over materialized columns.
pub struct LazyColumnsIter<'a> {
    columns: &'a LazyColumns,
    idx: usize,
}

impl<'a> Iterator for LazyColumnsIter<'a> {
    type Item = &'a ColumnData;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.columns.len() {
            let col = &self.columns[self.idx];
            self.idx += 1;
            Some(col)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.columns.len() - self.idx;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for LazyColumnsIter<'_> {}

impl<'a> IntoIterator for &'a LazyColumns {
    type Item = &'a ColumnData;
    type IntoIter = LazyColumnsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Immutable metadata shared by materialized builders and artifact-backed volumes.
#[derive(Clone)]
pub struct VolumeMeta {
    /// Zone maps per column
    pub zone_maps: Vec<ZoneMap>,
    /// Bloom filters per column (for fast equality membership testing)
    pub bloom_filters: Vec<super::column::ColumnBloomFilter>,
    /// Pre-computed aggregate stats
    pub stats: VolumeAggregateStats,
    /// Number of live rows
    pub row_count: usize,
    /// Column names (from schema)
    pub column_names: Vec<String>,
    /// Column types (from schema)
    pub column_types: Vec<DataType>,
    /// Exact logical type identities, including catalog-bound external types.
    pub column_logical_types: Vec<LogicalTypeRef>,
    /// Row IDs for each row (preserves original IDs for index compatibility)
    pub row_ids: RowIds,
    /// Whether the time/integer columns are sorted (enables binary search)
    pub sorted_columns: Vec<bool>,
    /// Precomputed lowercase column name -> index map for O(1) lookup.
    /// Built once at construction; replaces O(C) linear scan in column_index().
    pub column_name_map: AHashMap<SmartString, usize>,
    /// Row group metadata for sub-volume zone map pruning.
    /// Empty for volumes with <= ROW_GROUP_SIZE rows (single implicit group).
    pub row_groups: Vec<super::column::RowGroupMeta>,
}

impl VolumeMeta {
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    pub fn column_types(&self) -> &[DataType] {
        &self.column_types
    }

    pub fn column_logical_types(&self) -> &[LogicalTypeRef] {
        &self.column_logical_types
    }

    pub fn row_ids(&self) -> &RowIds {
        &self.row_ids
    }

    /// Estimate retained allocations owned by this metadata.
    pub fn memory_size(&self) -> usize {
        use std::mem::size_of;

        let mut size = size_of::<Self>();
        size += self.row_ids.retained_bytes();
        size += self.zone_maps.capacity() * size_of::<ZoneMap>();
        for bf in &self.bloom_filters {
            size += bf.memory_size();
        }
        size += self.bloom_filters.capacity() * size_of::<super::column::ColumnBloomFilter>();
        size += self.stats.columns.capacity() * size_of::<super::stats::ColumnAggregateStats>();
        size += self.column_names.capacity() * size_of::<String>();
        for name in &self.column_names {
            size += name.capacity();
        }
        size += self.column_types.capacity() * size_of::<DataType>();
        size += self.sorted_columns.capacity().div_ceil(8);
        size += self.column_name_map.capacity() * size_of::<(SmartString, usize)>();
        size += self.row_groups.capacity() * size_of::<super::column::RowGroupMeta>();
        for rg in &self.row_groups {
            size += rg.zone_maps.capacity() * size_of::<ZoneMap>();
        }
        size
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VolumeResidentMemory {
    pub metadata: usize,
    pub row_ids: usize,
    pub column_payload: usize,
    pub exact_indices: usize,
    pub ordered_indices: usize,
    pub descriptor: usize,
    pub block_source: usize,
}

impl VolumeResidentMemory {
    pub fn total(self) -> usize {
        self.metadata
            .saturating_add(self.row_ids)
            .saturating_add(self.column_payload)
            .saturating_add(self.exact_indices)
            .saturating_add(self.ordered_indices)
            .saturating_add(self.descriptor)
            .saturating_add(self.block_source)
    }
}

fn posting_map_memory_size<T>(map: &rustc_hash::FxHashMap<Vec<usize>, Vec<T>>) -> usize {
    use std::mem::size_of;

    let mut size = map.capacity() * size_of::<(Vec<usize>, Vec<T>)>();
    for (key, postings) in map {
        size += key.capacity() * size_of::<usize>();
        size += postings.capacity() * size_of::<T>();
    }
    size
}

/// A frozen volume ready for queries.
///
/// Eager instances are transient builders; durable instances read through the
/// canonical DATA/INDEX artifact sources.
pub struct FrozenVolume {
    /// Column data stored as typed arrays with lazy decompression
    pub columns: LazyColumns,
    /// Shared metadata (zone maps, bloom filters, stats, row IDs, etc.)
    pub meta: Arc<VolumeMeta>,
    /// Canonical immutable DATA artifact reader. During the indivisible
    /// production cutover this replaces the descriptor-backed legacy source;
    /// it retains only checked directories and bounded row-group state.
    pub artifact_source: Option<Arc<crate::v6::ArtifactDataSource>>,
    /// Optional canonical INDEX artifact bound to `artifact_source`. Its
    /// checked directory is the only runtime owner of immutable V6 postings.
    pub artifact_index_source: Option<Arc<crate::v6::ArtifactIndexSource>>,
    /// Lazily built sorted dictionary-id accelerators, one slot per column.
    /// The IDs reference immutable dictionary strings, so no strings are
    /// duplicated and the accelerator can be shared by eager/cold views.
    pub dictionary_lookup_indices: Arc<[OnceLock<Box<[u32]>>]>,
    /// Per-volume exact-equality index, never invalidated (volume is immutable).
    /// Key: column indices in declared index order. The historical field name is
    /// retained inside the artifact-backed implementation because the optional disk section
    /// was first introduced for UNIQUE checks; entries also support duplicate
    /// hashes and are now used by persisted composite equality lookups.
    /// Value: sorted Vec of (hash, row_idx) pairs -- binary search for lookup.
    /// Uses 12 bytes per entry vs ~80 bytes for `FxHashMap<u64, Vec<u32>>`, and
    /// zero tiny heap allocations (single contiguous allocation).
    #[allow(clippy::type_complexity)]
    pub unique_indices:
        Arc<parking_lot::RwLock<rustc_hash::FxHashMap<Vec<usize>, Vec<(u64, u32)>>>>,
    /// Immutable equality-prefix plus INTEGER-range postings. The key lists the
    /// participating index columns, with the final column providing order.
    pub ordered_indices: Arc<parking_lot::RwLock<super::index_metadata::OrderedIndexMetadata>>,
    /// Access epoch counter. Bumped per scan for eviction tracking.
    pub last_access_epoch: std::sync::atomic::AtomicU64,
}

/// Builder that accumulates rows and produces a FrozenVolume.
pub struct VolumeBuilder {
    schema: Schema,
    num_cols: usize,
    // Per-column accumulators
    int_cols: Vec<Vec<i64>>,
    float_cols: Vec<Vec<f64>>,
    ts_cols: Vec<Vec<i64>>, // nanos since epoch
    bool_cols: Vec<Vec<bool>>,
    dict_cols: Vec<Vec<u32>>,
    #[allow(clippy::type_complexity)]
    bytes_cols: Vec<(Vec<u8>, Vec<(u64, u64)>)>, // (data, offsets)
    null_cols: Vec<Vec<bool>>,
    // Column type mapping
    col_storage: Vec<StorageKind>,
    // Dictionary maps for text columns
    dict_maps: Vec<AHashMap<SmartString, u32>>,
    dict_tables: Vec<Vec<SmartString>>,
    // Zone maps
    zone_maps: Vec<ZoneMap>,
    // Stats
    stats: VolumeAggregateStats,
    // Row IDs
    row_ids: Vec<i64>,
    // Sort tracking
    last_values: Vec<Option<i64>>,
    sorted: Vec<bool>,
    // Row count
    row_count: usize,
}

#[derive(Clone, Copy)]
enum StorageKind {
    Int64(usize),           // index into int_cols
    Float64(usize),         // index into float_cols
    Timestamp(usize),       // index into ts_cols
    Boolean(usize),         // index into bool_cols
    Dictionary(usize),      // index into dict_cols
    Bytes(usize, DataType), // index into bytes_cols + ext type
}

impl VolumeBuilder {
    /// Create a new builder from a table schema.
    pub fn new(schema: &Schema) -> Self {
        let num_cols = schema.columns.len();
        let mut int_cols = Vec::new();
        let mut float_cols = Vec::new();
        let mut ts_cols = Vec::new();
        let mut bool_cols = Vec::new();
        let mut dict_cols = Vec::new();
        let mut bytes_cols = Vec::new();
        let mut col_storage = Vec::with_capacity(num_cols);
        let mut last_values = Vec::with_capacity(num_cols);
        let mut sorted = Vec::with_capacity(num_cols);

        for col in &schema.columns {
            match col.data_type {
                DataType::Integer => {
                    let idx = int_cols.len();
                    int_cols.push(Vec::new());
                    col_storage.push(StorageKind::Int64(idx));
                    last_values.push(None);
                    sorted.push(true);
                }
                DataType::Float => {
                    let idx = float_cols.len();
                    float_cols.push(Vec::new());
                    col_storage.push(StorageKind::Float64(idx));
                    last_values.push(None);
                    sorted.push(false); // floats: don't track sort
                }
                DataType::Timestamp => {
                    let idx = ts_cols.len();
                    ts_cols.push(Vec::new());
                    col_storage.push(StorageKind::Timestamp(idx));
                    last_values.push(None);
                    sorted.push(true);
                }
                DataType::Boolean => {
                    let idx = bool_cols.len();
                    bool_cols.push(Vec::new());
                    col_storage.push(StorageKind::Boolean(idx));
                    last_values.push(None);
                    sorted.push(false);
                }
                DataType::Text => {
                    let idx = dict_cols.len();
                    dict_cols.push(Vec::new());
                    col_storage.push(StorageKind::Dictionary(idx));
                    last_values.push(None);
                    sorted.push(false);
                }
                dt => {
                    // JSON, Vector, etc. → raw bytes
                    let idx = bytes_cols.len();
                    bytes_cols.push((Vec::new(), Vec::new()));
                    col_storage.push(StorageKind::Bytes(idx, dt));
                    last_values.push(None);
                    sorted.push(false);
                }
            }
        }

        let num_dict_cols = dict_cols.len();
        Self {
            schema: schema.clone(),
            num_cols,
            int_cols,
            float_cols,
            ts_cols,
            bool_cols,
            dict_cols,
            bytes_cols,
            null_cols: vec![Vec::new(); num_cols],
            col_storage,
            dict_maps: vec![AHashMap::new(); num_dict_cols],
            dict_tables: vec![Vec::new(); num_dict_cols],
            zone_maps: (0..num_cols)
                .map(|_| ZoneMap {
                    min: Value::Null(DataType::Null),
                    max: Value::Null(DataType::Null),
                    null_count: 0,
                    row_count: 0,
                })
                .collect(),
            stats: VolumeAggregateStats::new(num_cols),
            row_ids: Vec::new(),
            last_values,
            sorted,
            row_count: 0,
        }
    }

    /// Create a builder with pre-allocated capacity.
    pub fn with_capacity(schema: &Schema, capacity: usize) -> Self {
        let mut builder = Self::new(schema);
        builder.row_ids.reserve(capacity);
        for nulls in &mut builder.null_cols {
            nulls.reserve(capacity);
        }
        for v in &mut builder.int_cols {
            v.reserve(capacity);
        }
        for v in &mut builder.float_cols {
            v.reserve(capacity);
        }
        for v in &mut builder.ts_cols {
            v.reserve(capacity);
        }
        for v in &mut builder.bool_cols {
            v.reserve(capacity);
        }
        for v in &mut builder.dict_cols {
            v.reserve(capacity);
        }
        builder
    }

    /// Validate and add a row to the volume without mutating any accumulator on
    /// failure. Physical rows must have the exact schema width/type and row IDs
    /// must already be strictly ordered and unique.
    pub fn try_add_row(&mut self, row_id: i64, row: &Row) -> Result<()> {
        validate_volume_row(&self.schema, self.row_ids.last().copied(), row_id, row)?;

        self.add_row_validated(row_id, row);
        Ok(())
    }

    /// Add a prevalidated row. This convenience boundary remains infallible for
    /// existing callers, but invalid input now fails closed instead of silently
    /// writing default values into a mismatched physical column.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn add_row(&mut self, row_id: i64, row: &Row) {
        self.try_add_row(row_id, row)
            .unwrap_or_else(|error| panic!("invalid volume row: {error}"));
    }

    fn add_row_validated(&mut self, row_id: i64, row: &Row) {
        self.row_ids.push(row_id);
        self.stats.total_rows += 1;
        self.stats.live_rows += 1;

        for col_idx in 0..self.num_cols {
            let value = row.get(col_idx).unwrap_or(&Value::Null(DataType::Null));

            self.zone_maps[col_idx].row_count += 1;
            let is_null = value.is_null();
            self.null_cols[col_idx].push(is_null);

            if is_null {
                self.zone_maps[col_idx].null_count += 1;
                // NULL placeholder (0) breaks sorted-order invariant that
                // binary search requires. Mark column unsorted.
                self.sorted[col_idx] = false;
                // Push placeholder for null
                match self.col_storage[col_idx] {
                    StorageKind::Int64(idx) => self.int_cols[idx].push(0),
                    StorageKind::Float64(idx) => self.float_cols[idx].push(0.0),
                    StorageKind::Timestamp(idx) => self.ts_cols[idx].push(0),
                    StorageKind::Boolean(idx) => self.bool_cols[idx].push(false),
                    StorageKind::Dictionary(idx) => self.dict_cols[idx].push(0),
                    StorageKind::Bytes(idx, _) => {
                        self.bytes_cols[idx].1.push((0, 0));
                    }
                }
                continue;
            }

            let is_nan = matches!(value, Value::Float(f) if f.is_nan());
            self.stats.columns[col_idx].accumulate(value);

            if !is_nan {
                let zm = &mut self.zone_maps[col_idx];
                if zm.min.is_null() {
                    zm.min = value.clone();
                    zm.max = value.clone();
                } else {
                    if let Ok(std::cmp::Ordering::Less) = value.compare(&zm.min) {
                        zm.min = value.clone();
                    }
                    if let Ok(std::cmp::Ordering::Greater) = value.compare(&zm.max) {
                        zm.max = value.clone();
                    }
                }
            }

            // Store in typed column
            match self.col_storage[col_idx] {
                StorageKind::Int64(idx) => {
                    let v = match value {
                        Value::Integer(i) => *i,
                        _ => 0,
                    };
                    // Track sortedness
                    if self.sorted[col_idx] {
                        if let Some(last) = self.last_values[col_idx] {
                            if v < last {
                                self.sorted[col_idx] = false;
                            }
                        }
                        self.last_values[col_idx] = Some(v);
                    }
                    self.int_cols[idx].push(v);
                }
                StorageKind::Float64(idx) => {
                    let v = match value {
                        Value::Float(f) => *f,
                        _ => 0.0,
                    };
                    self.float_cols[idx].push(v);
                }
                StorageKind::Timestamp(idx) => {
                    let nanos = match value {
                        Value::Timestamp(_) => value
                            .artifact_timestamp_nanos()
                            .expect("validated artifact-backed timestamp must fit nanoseconds"),
                        _ => 0,
                    };
                    if self.sorted[col_idx] {
                        if let Some(last) = self.last_values[col_idx] {
                            if nanos < last {
                                self.sorted[col_idx] = false;
                            }
                        }
                        self.last_values[col_idx] = Some(nanos);
                    }
                    self.ts_cols[idx].push(nanos);
                }
                StorageKind::Boolean(idx) => {
                    let v = match value {
                        Value::Boolean(b) => *b,
                        _ => false,
                    };
                    self.bool_cols[idx].push(v);
                }
                StorageKind::Dictionary(idx) => {
                    let s = match value {
                        Value::Text(s) => s.clone(),
                        _ => SmartString::from(""),
                    };
                    let dict_id = if let Some(&id) = self.dict_maps[idx].get(&s) {
                        id
                    } else {
                        let id = self.dict_tables[idx].len() as u32;
                        self.dict_tables[idx].push(s.clone());
                        self.dict_maps[idx].insert(s, id);
                        id
                    };
                    self.dict_cols[idx].push(dict_id);
                }
                StorageKind::Bytes(idx, _) => {
                    let bytes = match value {
                        Value::Extension(data) if data.len() > 1 => {
                            &data[1..] // skip type tag
                        }
                        _ => &[],
                    };
                    let offset = self.bytes_cols[idx].0.len() as u64;
                    let length = bytes.len() as u64;
                    self.bytes_cols[idx].0.extend_from_slice(bytes);
                    self.bytes_cols[idx].1.push((offset, length));
                }
            }
        }
        self.row_count += 1;
    }

    /// Freeze the builder into a FrozenVolume.
    pub fn finish(mut self) -> FrozenVolume {
        let (columns, sorted_columns) = self.take_columns();
        let meta = self.finish_meta(&columns, sorted_columns);
        let column_types = meta.column_types.clone();
        let column_count = column_types.len();

        FrozenVolume {
            columns: LazyColumns::eager(columns, column_types),
            meta: Arc::new(meta),
            artifact_source: None,
            artifact_index_source: None,
            dictionary_lookup_indices: new_dictionary_lookup_indices(column_count),
            unique_indices: Arc::new(parking_lot::RwLock::new(rustc_hash::FxHashMap::default())),
            ordered_indices: Arc::new(parking_lot::RwLock::new(rustc_hash::FxHashMap::default())),
            last_access_epoch: std::sync::atomic::AtomicU64::new(u64::MAX),
        }
    }

    fn take_columns(&mut self) -> (Vec<ColumnData>, Vec<bool>) {
        let mut columns = Vec::with_capacity(self.num_cols);
        let mut sorted_columns = Vec::with_capacity(self.num_cols);

        for col_idx in 0..self.num_cols {
            let nulls = std::mem::take(&mut self.null_cols[col_idx]);
            sorted_columns.push(self.sorted[col_idx]);

            let col_data = match self.col_storage[col_idx] {
                StorageKind::Int64(idx) => ColumnData::Int64 {
                    values: std::mem::take(&mut self.int_cols[idx]),
                    nulls,
                },
                StorageKind::Float64(idx) => ColumnData::Float64 {
                    values: std::mem::take(&mut self.float_cols[idx]),
                    nulls,
                },
                StorageKind::Timestamp(idx) => ColumnData::TimestampNanos {
                    values: std::mem::take(&mut self.ts_cols[idx]),
                    nulls,
                },
                StorageKind::Boolean(idx) => ColumnData::Boolean {
                    values: std::mem::take(&mut self.bool_cols[idx]),
                    nulls,
                },
                StorageKind::Dictionary(idx) => ColumnData::Dictionary {
                    ids: std::mem::take(&mut self.dict_cols[idx]),
                    dictionary: Arc::from(std::mem::take(&mut self.dict_tables[idx])),
                    nulls,
                },
                StorageKind::Bytes(idx, ext_type) => {
                    let (data, offsets) = std::mem::take(&mut self.bytes_cols[idx]);
                    ColumnData::Bytes {
                        data,
                        offsets,
                        ext_type,
                        nulls,
                    }
                }
            };
            columns.push(col_data);
        }

        (columns, sorted_columns)
    }

    fn finish_meta(mut self, columns: &[ColumnData], sorted_columns: Vec<bool>) -> VolumeMeta {
        let column_names: Vec<String> =
            self.schema.columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<DataType> = self.schema.columns.iter().map(|c| c.data_type).collect();
        let column_logical_types = self
            .schema
            .columns
            .iter()
            .map(|column| column.logical_type())
            .collect();

        // Build bloom filters from column data using typed methods
        // to avoid allocating a Value per cell (saves ~500K allocs for 100K rows).
        let bloom_filters: Vec<super::column::ColumnBloomFilter> = columns
            .iter()
            .enumerate()
            .map(|(col_idx, col)| {
                if !super::column::ColumnBloomFilter::supports_bloom_creation(
                    self.schema.columns[col_idx].data_type,
                ) {
                    return super::column::ColumnBloomFilter::always_maybe();
                }

                let mut bf = super::column::ColumnBloomFilter::new_capped(
                    self.row_count.max(1),
                    super::column::ColumnBloomFilter::DEFAULT_MAX_BITS,
                );
                for i in 0..self.row_count {
                    if col.is_null(i) {
                        continue;
                    }
                    match col {
                        super::column::ColumnData::Int64 { values, .. } => {
                            bf.add_i64(values[i]);
                        }
                        super::column::ColumnData::Float64 { values, .. } => {
                            bf.add_f64(values[i]);
                        }
                        super::column::ColumnData::TimestampNanos { values, .. } => {
                            bf.add_timestamp_nanos(values[i]);
                        }
                        super::column::ColumnData::Boolean { values, .. } => {
                            bf.add_bool(values[i]);
                        }
                        super::column::ColumnData::Dictionary {
                            ids, dictionary, ..
                        } => {
                            let dict_id = ids[i] as usize;
                            if dict_id < dictionary.len() {
                                bf.add_str(dictionary[dict_id].as_str());
                            }
                        }
                        super::column::ColumnData::Bytes { .. } => {}
                        super::column::ColumnData::External { .. } => {}
                    }
                }
                bf
            })
            .collect();

        // Ensure row_ids are sorted — binary_search in manifest/table depends on this.
        // All production paths (seal via BTree iter, compact via explicit sort, snapshot
        // recovery via BTreeMap iter) provide rows in ascending row_id order. This
        // check catches any future caller that violates this invariant.
        // Using a regular check (not debug_assert) because silent corruption in
        // release builds from unsorted row_ids would be catastrophic.
        if !self.row_ids.windows(2).all(|w| w[0] < w[1]) {
            // Sort as fallback instead of panicking
            self.row_ids.sort_unstable();
        }

        let column_name_map: AHashMap<SmartString, usize> = column_names
            .iter()
            .enumerate()
            .flat_map(|(i, name)| {
                let lower = SmartString::from(name.to_lowercase());
                let original = SmartString::from(name.as_str());
                if lower == original {
                    // Already lowercase — one entry
                    vec![(lower, i)]
                } else {
                    // Store both original and lowercase for zero-alloc lookup
                    vec![(original, i), (lower, i)]
                }
            })
            .collect();

        // Build row-group zone maps for sub-volume pruning.
        // Only worth it for volumes larger than one group.
        let row_groups = if self.row_count > super::column::ROW_GROUP_SIZE {
            let mut groups = Vec::new();
            let mut start = 0;
            while start < self.row_count {
                let end = (start + super::column::ROW_GROUP_SIZE).min(self.row_count);
                let group_zone_maps: Vec<super::column::ZoneMap> = columns
                    .iter()
                    .map(|col| col.zone_map_for_range(start, end))
                    .collect();
                groups.push(super::column::RowGroupMeta {
                    start_idx: start as u32,
                    end_idx: end as u32,
                    zone_maps: group_zone_maps,
                });
                start = end;
            }
            groups
        } else {
            Vec::new()
        };

        VolumeMeta {
            zone_maps: self.zone_maps,
            bloom_filters,
            stats: self.stats,
            row_count: self.row_count,
            column_names,
            column_types,
            column_logical_types,
            row_ids: RowIds::try_from_sorted_iter(self.row_ids)
                .expect("VolumeBuilder validates strictly ascending row IDs"),
            sorted_columns,
            column_name_map,
            row_groups,
        }
    }
}

/// Validate one physical volume row without mutating a builder or writer.
/// Eager and streaming seal paths share this exact admission contract.
pub fn validate_volume_row(
    schema: &Schema,
    previous_row_id: Option<i64>,
    row_id: i64,
    row: &Row,
) -> Result<()> {
    if row.len() != schema.columns.len() {
        return Err(Error::invalid_argument(format!(
            "volume row width {} does not match schema width {}",
            row.len(),
            schema.columns.len()
        )));
    }
    if previous_row_id.is_some_and(|previous| row_id <= previous) {
        return Err(Error::invalid_argument(format!(
            "volume row IDs must be strictly increasing: {} after {}",
            row_id,
            previous_row_id.unwrap()
        )));
    }
    for (index, column) in schema.columns.iter().enumerate() {
        let value = row
            .get(index)
            .ok_or_else(|| Error::invalid_argument("volume row is missing a column"))?;
        value.validate_shape()?;
        if !value.is_null() && value.data_type() != column.data_type {
            return Err(Error::invalid_argument(format!(
                "volume column '{}' expects {:?}, got {:?}",
                column.name,
                column.data_type,
                value.data_type()
            )));
        }
        column.validate_declared_value(value)?;
        if column.data_type == DataType::Timestamp
            && !value.is_null()
            && value.artifact_timestamp_nanos().is_none()
        {
            return Err(Error::invalid_argument(format!(
                "artifact-backed timestamp in column '{}' is outside the exact nanosecond range",
                column.name
            )));
        }
    }
    Ok(())
}

/// Source for a single schema column when reading from a frozen volume.
/// Precomputed once per volume per scan, then used for every row.
#[derive(Clone)]
pub enum ColSource {
    /// Schema column maps to this volume column index.
    Volume(usize),
    /// Schema column was added after this volume was sealed.
    /// Use this default value (NULL or DEFAULT from ALTER TABLE).
    Default(Value),
}

/// Precomputed mapping from current schema to a frozen volume's columns.
/// Computed once per volume per scan. Eliminates per-row name lookups.
#[derive(Clone)]
pub struct ColumnMapping {
    /// For each schema column position, how to get the value.
    pub sources: Vec<ColSource>,
    /// True when every schema column maps 1:1 to the same volume column
    /// in the same order. When true, callers can skip the mapping and
    /// use get_row()/get_row_projected() directly.
    pub is_identity: bool,
    logical_types: Arc<[LogicalTypeRef]>,
}

impl ColumnMapping {
    pub fn try_new(
        schema: &Schema,
        volume: &FrozenVolume,
        sources: Vec<ColSource>,
    ) -> Result<Self> {
        if sources.len() != schema.columns.len() {
            return Err(Error::invalid_argument(format!(
                "column mapping width {} does not match schema width {}",
                sources.len(),
                schema.columns.len()
            )));
        }
        let logical_types: Arc<[LogicalTypeRef]> = schema
            .columns
            .iter()
            .map(|column| column.logical_type())
            .collect::<Vec<_>>()
            .into();
        let is_identity = sources.len() == volume.columns.len()
            && sources.iter().enumerate().all(
                |(index, source)| matches!(source, ColSource::Volume(actual) if *actual == index),
            );
        let mapping = Self {
            sources,
            is_identity,
            logical_types,
        };
        mapping.validate_for_volume(volume)?;
        Ok(mapping)
    }

    pub fn identity(volume: &FrozenVolume) -> Self {
        Self {
            sources: (0..volume.columns.len()).map(ColSource::Volume).collect(),
            is_identity: true,
            logical_types: volume.meta.column_logical_types.clone().into(),
        }
    }

    pub fn empty() -> Self {
        Self {
            sources: Vec::new(),
            is_identity: true,
            logical_types: Arc::from([]),
        }
    }

    pub fn validate_for_volume(&self, volume: &FrozenVolume) -> Result<()> {
        if self.sources.len() != self.logical_types.len() {
            return Err(Error::invalid_argument(
                "column mapping metadata width mismatch",
            ));
        }
        let mut used = FxHashSet::default();
        for (logical_index, source) in self.sources.iter().enumerate() {
            let expected = self.logical_types[logical_index];
            match source {
                ColSource::Volume(physical_index) => {
                    if *physical_index >= volume.meta.column_types.len() {
                        return Err(Error::invalid_argument(format!(
                            "column mapping physical index {} is outside volume width {}",
                            physical_index,
                            volume.meta.column_types.len()
                        )));
                    }
                    let actual_logical = volume
                        .meta
                        .column_logical_types
                        .get(*physical_index)
                        .ok_or_else(|| {
                            Error::invalid_argument(format!(
                                "column mapping physical index {} has no logical type metadata",
                                physical_index
                            ))
                        })?;
                    if *actual_logical != expected {
                        return Err(Error::invalid_argument(format!(
                            "column mapping type mismatch at logical column {}: expected {:?}, physical {:?}",
                            logical_index, expected, actual_logical
                        )));
                    }
                    if !used.insert(*physical_index) {
                        return Err(Error::invalid_argument(format!(
                            "column mapping reuses physical column {}",
                            physical_index
                        )));
                    }
                }
                ColSource::Default(value) => {
                    value.validate_shape()?;
                    if value.logical_type() != expected {
                        return Err(Error::invalid_argument(format!(
                            "column mapping default type mismatch at logical column {}: expected {:?}, got {:?}",
                            logical_index,
                            expected,
                            value.logical_type()
                        )));
                    }
                }
            }
        }
        let derived_identity = self.sources.len() == volume.columns.len()
            && self.sources.iter().enumerate().all(
                |(index, source)| matches!(source, ColSource::Volume(actual) if *actual == index),
            );
        if self.is_identity != derived_identity {
            return Err(Error::invalid_argument(
                "column mapping identity flag disagrees with sources",
            ));
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.sources.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    pub fn is_identity(&self) -> bool {
        self.is_identity
    }
}

/// Operation-wide bounded cache used while compaction materializes rows from
/// immutable DATA artifacts.
///
/// The k-way compaction stream visits row indexes in ascending order for each
/// input volume. Keeping one row-group worth of decoded column blocks per
/// active input prevents a k-way merge from re-reading the same
/// `(segment, column, row_group)` block every time adjacent row IDs alternate
/// between segments. Each input advances monotonically, so replacing only that
/// segment's previous row group keeps memory bounded by the merge fan-in.
#[derive(Default)]
pub struct CompactionBlockCache {
    artifact_segments: AHashMap<[u8; 16], CompactionSegmentBlockCache>,
}

#[derive(Default)]
struct CompactionSegmentBlockCache {
    row_group_idx: Option<usize>,
    blocks: AHashMap<usize, ColumnData>,
}

impl CompactionBlockCache {
    pub fn clear(&mut self) {
        self.artifact_segments.clear();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn resident_block_count(&self) -> usize {
        self.artifact_segments
            .values()
            .map(|segment| segment.blocks.len())
            .sum()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn resident_segment_count(&self) -> usize {
        self.artifact_segments.len()
    }

    fn artifact_block(
        &mut self,
        source: &crate::v6::ArtifactDataSource,
        phys_idx: usize,
        row_group_idx: usize,
    ) -> Result<&ColumnData> {
        let segment_id = *source.layout().header().segment_id().as_bytes();
        let segment = self.artifact_segments.entry(segment_id).or_default();
        if segment.row_group_idx != Some(row_group_idx) {
            segment.row_group_idx = Some(row_group_idx);
            segment.blocks.clear();
        }
        if !segment.blocks.contains_key(&phys_idx) {
            let mut columns = source
                .read_columns(row_group_idx, &[phys_idx])?
                .into_columns();
            let (_, block) = columns.pop().ok_or_else(|| {
                Error::internal("cold compaction DATA artifact returned no requested column")
            })?;
            segment.blocks.insert(phys_idx, block);
        }
        segment.blocks.get(&phys_idx).ok_or_else(|| {
            Error::internal("cold compaction DATA artifact block missing after cache insert")
        })
    }
}

/// Compute column mapping from current schema to a frozen volume.
/// Handles renames (via column_renames fallback) and drops.
/// `volume_schema_version` is the schema epoch when the volume was created.
/// For dropped columns, only volumes created before or at the drop are masked.
/// Snapshot readers should use `SegmentManager::get_cold_segment_mapping()` so
/// the mapping and immutable volume come from the same topology generation.
pub fn compute_column_mapping_with_drops(
    schema: &Schema,
    volume: &FrozenVolume,
    dropped_columns: &[(radixdb_core::SmartString, u64)],
    volume_schema_version: u64,
    column_renames: &[(radixdb_core::SmartString, radixdb_core::SmartString)],
) -> ColumnMapping {
    let mut sources = Vec::with_capacity(schema.columns.len());
    let mut is_identity = schema.columns.len() == volume.columns.len();
    // Track which volume column indices are already claimed. Prevents two
    // schema columns from binding to the same physical column (e.g., after
    // RENAME a→b then ADD COLUMN a, both "b" via rename and "a" via direct
    // match would hit the same old physical column without this guard).
    let mut used_vol_indices = smallvec::SmallVec::<[usize; 16]>::new();

    for (pos, col) in schema.columns.iter().enumerate() {
        // Try rename fallback FIRST (higher priority: a renamed column's
        // old physical slot belongs to the renamed column, not a new column
        // that happens to reuse the old name).
        let mut physical_name = col.name_lower.as_str();
        let mut resolved = false;
        for _ in 0..=column_renames.len() {
            let Some((old, _)) = column_renames
                .iter()
                .rev()
                .find(|(_, new)| new.as_str() == physical_name)
            else {
                resolved = true;
                break;
            };
            physical_name = old.as_str();
        }
        let vol_idx = resolved
            .then(|| volume.column_index(physical_name))
            .flatten()
            .or_else(|| volume.column_index(&col.name_lower));

        if let Some(vol_idx) = vol_idx {
            let type_matches = volume
                .meta
                .column_logical_types
                .get(vol_idx)
                .is_some_and(|physical| *physical == col.logical_type());
            let was_dropped = dropped_columns.iter().any(|(d, drop_ver)| {
                d.as_str() == col.name_lower && volume_schema_version <= *drop_ver
            });
            let already_used = used_vol_indices.contains(&vol_idx);
            if type_matches && !was_dropped && !already_used {
                if is_identity && vol_idx != pos {
                    is_identity = false;
                }
                used_vol_indices.push(vol_idx);
                sources.push(ColSource::Volume(vol_idx));
            } else {
                is_identity = false;
                if let Some(ref default_val) = col.default_value {
                    sources.push(ColSource::Default(default_val.clone()));
                } else {
                    sources.push(ColSource::Default(Value::Null(col.data_type)));
                }
            }
        } else {
            // Column not in volume (added after seal, or dropped+re-added)
            is_identity = false;
            if let Some(ref default_val) = col.default_value {
                sources.push(ColSource::Default(default_val.clone()));
            } else {
                sources.push(ColSource::Default(Value::Null(col.data_type)));
            }
        }
    }

    let mapping = ColumnMapping::try_new(schema, volume, sources)
        .expect("derived schema-to-volume mapping must be valid");
    debug_assert_eq!(mapping.is_identity, is_identity);
    mapping
}

impl FrozenVolume {
    /// Build the runtime shell for one canonical DATA artifact.
    ///
    /// Row IDs remain compact run metadata because the surrounding MVCC
    /// topology still uses them for visibility and point routing. Column
    /// payloads are never materialized here: scans decode one bounded artifact
    /// row group through `ArtifactDataSource`.
    pub fn from_artifact_source(
        schema: &Schema,
        source: Arc<crate::v6::ArtifactDataSource>,
    ) -> Result<Self> {
        let layout = source.layout();
        if layout.columns().len() != schema.columns.len() {
            return Err(Error::internal(
                "DATA artifact column count differs from runtime schema",
            ));
        }
        for (physical, logical) in layout.columns().iter().zip(&schema.columns) {
            if physical.data_type().logical_type_ref() != logical.logical_type() {
                return Err(Error::internal(format!(
                    "DATA artifact column '{}' type differs from runtime schema",
                    logical.name
                )));
            }
        }

        let row_count = source.row_count()?;
        let mut row_ids = Vec::new();
        row_ids
            .try_reserve_exact(row_count)
            .map_err(|_| Error::internal("DATA row-ID metadata allocation failed"))?;
        for group in 0..source.row_group_count() {
            row_ids.extend_from_slice(source.read_row_ids(group)?.row_ids());
        }
        let row_ids = RowIds::try_from_sorted_iter(row_ids)
            .map_err(|detail| Error::internal(format!("invalid DATA row IDs: {detail}")))?;
        if row_ids.len() != row_count {
            return Err(Error::internal(
                "DATA row-ID metadata differs from artifact row count",
            ));
        }

        let column_types = schema
            .columns
            .iter()
            .map(|column| column.data_type)
            .collect::<Vec<_>>();
        let column_logical_types = schema
            .columns
            .iter()
            .map(|column| column.logical_type())
            .collect::<Vec<_>>();
        let column_names = schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let column_name_map = schema
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| (SmartString::from(column.name_lower.as_str()), index))
            .collect();

        let mut aggregate = VolumeAggregateStats::new(schema.columns.len());
        aggregate.total_rows = row_count as u64;
        aggregate.live_rows = row_count as u64;
        let mut zone_maps = Vec::with_capacity(schema.columns.len());
        for (column_ordinal, data_type) in column_types.iter().copied().enumerate() {
            let matching = layout
                .statistics()
                .iter()
                .filter(|entry| entry.column_ordinal() as usize == column_ordinal)
                .collect::<Vec<_>>();
            let zone = merge_artifact_zone_maps(data_type, row_count, &matching)?;
            let column_stats = &mut aggregate.columns[column_ordinal];
            column_stats.min = zone.min.clone();
            column_stats.max = zone.max.clone();
            column_stats.non_null_count = (row_count as u64).saturating_sub(zone.null_count as u64);
            for entry in &matching {
                column_stats.numeric_count = column_stats
                    .numeric_count
                    .checked_add(entry.numeric_count())
                    .ok_or_else(|| Error::internal("DATA numeric count overflows u64"))?;
                match data_type {
                    DataType::Integer => {
                        let sum = entry.integer_sum().ok_or_else(|| {
                            Error::internal("DATA INTEGER statistics are missing exact sum")
                        })?;
                        column_stats.sum_int = column_stats
                            .sum_int
                            .checked_add(sum)
                            .ok_or_else(|| Error::internal("DATA INTEGER sum overflows i128"))?;
                    }
                    DataType::Float => {
                        let sum = entry.float_sum().ok_or_else(|| {
                            Error::internal("DATA FLOAT statistics are missing exact sum")
                        })?;
                        column_stats.sum_float += sum;
                    }
                    _ => {
                        if entry.integer_sum().is_some()
                            || entry.float_sum().is_some()
                            || entry.numeric_count() != 0
                        {
                            return Err(Error::internal(
                                "non-numeric DATA statistics contain numeric aggregate",
                            ));
                        }
                    }
                }
            }
            zone_maps.push(zone);
        }

        let mut row_groups = Vec::with_capacity(layout.row_groups().len());
        for group in layout.row_groups() {
            let group_ordinal = group.group_ordinal();
            let group_row_count = group.row_count() as usize;
            let mut group_zones = Vec::with_capacity(schema.columns.len());
            for (column_ordinal, data_type) in column_types.iter().copied().enumerate() {
                let matching = layout
                    .statistics()
                    .iter()
                    .filter(|entry| {
                        entry.column_ordinal() as usize == column_ordinal
                            && entry.row_group_ordinal() == group_ordinal
                    })
                    .collect::<Vec<_>>();
                group_zones.push(merge_artifact_zone_maps(
                    data_type,
                    group_row_count,
                    &matching,
                )?);
            }
            let start_idx = u32::try_from(group.first_row_ordinal())
                .map_err(|_| Error::internal("DATA row-group start exceeds u32"))?;
            let end_idx = start_idx
                .checked_add(group.row_count())
                .ok_or_else(|| Error::internal("DATA row-group range overflows"))?;
            row_groups.push(super::column::RowGroupMeta {
                start_idx,
                end_idx,
                zone_maps: group_zones,
            });
        }

        Ok(Self {
            columns: LazyColumns::metadata_only(column_types.clone()),
            meta: Arc::new(VolumeMeta {
                zone_maps,
                bloom_filters: Vec::new(),
                stats: aggregate,
                row_count,
                column_names,
                column_types,
                column_logical_types,
                row_ids,
                sorted_columns: vec![false; schema.columns.len()],
                column_name_map,
                row_groups,
            }),
            artifact_source: Some(source),
            artifact_index_source: None,
            dictionary_lookup_indices: new_dictionary_lookup_indices(schema.columns.len()),
            unique_indices: Arc::new(parking_lot::RwLock::new(rustc_hash::FxHashMap::default())),
            ordered_indices: Arc::new(parking_lot::RwLock::new(rustc_hash::FxHashMap::default())),
            last_access_epoch: std::sync::atomic::AtomicU64::new(u64::MAX),
        })
    }

    pub fn meta(&self) -> &VolumeMeta {
        &self.meta
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn dictionary_for_column(&self, col_idx: usize) -> Option<Arc<[SmartString]>> {
        self.columns.get_column_dictionary(col_idx)
    }

    /// Resolve a Text literal to a dictionary ID through one lazy, immutable
    /// O(log U) accelerator per volume/column.
    pub fn dictionary_id_for_text(&self, col_idx: usize, value: &str) -> Option<u32> {
        let dictionary = self.dictionary_for_column(col_idx)?;
        let slot = self.dictionary_lookup_indices.get(col_idx)?;
        let sorted_ids = slot.get_or_init(|| {
            let mut ids: Vec<u32> = (0..dictionary.len())
                .map(|index| u32::try_from(index).expect("dictionary count fits u32"))
                .collect();
            ids.sort_unstable_by(|left, right| {
                dictionary[*left as usize].cmp(&dictionary[*right as usize])
            });
            ids.into_boxed_slice()
        });
        sorted_ids
            .binary_search_by(|id| dictionary[*id as usize].as_str().cmp(value))
            .ok()
            .map(|position| sorted_ids[position])
    }

    pub fn artifact_source(&self) -> Option<&Arc<crate::v6::ArtifactDataSource>> {
        self.artifact_source.as_ref()
    }

    pub fn with_artifact_source(
        mut self,
        source: Arc<crate::v6::ArtifactDataSource>,
    ) -> Result<Self> {
        if source.row_count()? != self.meta.row_count {
            return Err(Error::internal(
                "DATA artifact row count differs from frozen-volume metadata",
            ));
        }
        if source.column_count() != self.columns.len() {
            return Err(Error::internal(
                "DATA artifact column count differs from frozen-volume metadata",
            ));
        }
        self.artifact_source = Some(source);
        Ok(self)
    }

    pub fn artifact_index_source(&self) -> Option<&Arc<crate::v6::ArtifactIndexSource>> {
        self.artifact_index_source.as_ref()
    }

    pub fn with_artifact_index_source(
        mut self,
        source: Arc<crate::v6::ArtifactIndexSource>,
    ) -> Result<Self> {
        let data = self.artifact_source.as_ref().ok_or_else(|| {
            Error::internal("INDEX artifact cannot be attached without its DATA source")
        })?;
        let header = source.layout().header();
        if header.data_artifact_id() != data.layout().reference().id()
            || header.data_body_sha256() != data.layout().reference().body_sha256()
        {
            return Err(Error::internal(
                "INDEX artifact is not bound to the frozen DATA source",
            ));
        }
        self.artifact_index_source = Some(source);
        Ok(self)
    }

    pub fn row_id_at(&self, index: usize) -> Result<i64> {
        match self.artifact_source() {
            Some(source) => source.row_id_at(index),
            None => self
                .meta
                .row_ids
                .get(index)
                .ok_or_else(|| Error::internal("row-ID index is outside frozen volume")),
        }
    }

    pub fn find_row_id(&self, row_id: i64) -> Result<std::result::Result<usize, usize>> {
        match self.artifact_source() {
            Some(source) => source.find_row_id(row_id),
            None => Ok(self.meta.row_ids.binary_search(&row_id)),
        }
    }

    pub fn has_exact_postings(&self, columns: &[usize]) -> bool {
        self.artifact_index_source
            .as_ref()
            .is_some_and(|source| source.supports_equality(columns))
            || self.unique_indices.read().contains_key(columns)
    }

    pub fn visit_exact_posting_candidates(
        &self,
        columns: &[usize],
        hash: u64,
        mut visit: impl FnMut(u32) -> Result<bool>,
    ) -> Result<Option<()>> {
        if let Some(entries) = self.unique_indices.read().get(columns) {
            let start = entries.partition_point(|&(entry_hash, _)| entry_hash < hash);
            for &(entry_hash, row_idx) in &entries[start..] {
                if entry_hash != hash {
                    break;
                }
                if visit(row_idx)? {
                    return Ok(Some(()));
                }
            }
            return Ok(Some(()));
        }
        Ok(None)
    }

    pub fn has_ordered_postings(&self, columns: &[usize]) -> bool {
        self.artifact_index_source
            .as_ref()
            .is_some_and(|source| source.supports_ordered(columns))
            || self.ordered_indices.read().contains_key(columns)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn visit_ordered_posting_candidates(
        &self,
        columns: &[usize],
        prefix_hash: u64,
        min: Option<(i64, bool)>,
        max: Option<(i64, bool)>,
        ascending: bool,
        mut visit: impl FnMut(super::index_metadata::OrderedIndexEntry) -> Result<bool>,
    ) -> Result<Option<()>> {
        if let Some(entries) = self.ordered_indices.read().get(columns) {
            let prefix_start = entries.partition_point(|entry| entry.prefix_hash < prefix_hash);
            let prefix_end = entries.partition_point(|entry| entry.prefix_hash <= prefix_hash);
            let prefix_entries = &entries[prefix_start..prefix_end];
            let range_start = prefix_entries.partition_point(|entry| {
                min.is_some_and(|(bound, inclusive)| {
                    entry.order_key < bound || (!inclusive && entry.order_key == bound)
                })
            });
            let range_end = prefix_entries.partition_point(|entry| {
                max.is_none_or(|(bound, inclusive)| {
                    entry.order_key < bound || (inclusive && entry.order_key == bound)
                })
            });
            let range = &prefix_entries[range_start..range_end];
            if ascending {
                for &entry in range {
                    if visit(entry)? {
                        return Ok(Some(()));
                    }
                }
            } else {
                for &entry in range.iter().rev() {
                    if visit(entry)? {
                        return Ok(Some(()));
                    }
                }
            }
            return Ok(Some(()));
        }
        Ok(None)
    }

    /// Materialize a row through the current schema mapping while retaining at
    /// most one decoded DATA row-group per compaction input.
    pub fn get_row_for_compaction(&self, idx: usize, mapping: &ColumnMapping) -> Result<Row> {
        let mut cache = CompactionBlockCache::default();
        self.get_row_for_compaction_cached(idx, mapping, &mut cache)
    }

    /// Same as `get_row_for_compaction`, but reuses decoded DATA row-group
    /// blocks across adjacent row materializations from the same input volume.
    pub fn get_row_for_compaction_cached(
        &self,
        idx: usize,
        mapping: &ColumnMapping,
        cache: &mut CompactionBlockCache,
    ) -> Result<Row> {
        let mut values = Vec::with_capacity(mapping.sources.len());
        for schema_col_idx in 0..mapping.sources.len() {
            values.push(self.get_value_for_compaction_cached(
                idx,
                schema_col_idx,
                mapping,
                cache,
            )?);
        }

        let value_count = values.len();
        let row = Row::from_values(values);
        instrumentation::record_row_materialization_count(1, value_count as u64);
        Ok(row)
    }

    /// Materialize a single mapped schema value for compaction without forcing
    /// a full cold-volume reload or a full row allocation.
    pub fn get_value_for_compaction_cached(
        &self,
        idx: usize,
        schema_col_idx: usize,
        mapping: &ColumnMapping,
        cache: &mut CompactionBlockCache,
    ) -> Result<Value> {
        let src = mapping.sources.get(schema_col_idx).ok_or_else(|| {
            Error::internal(format!(
                "compaction schema column {} is outside mapping width {}",
                schema_col_idx,
                mapping.sources.len()
            ))
        })?;

        if let Some(source) = self.artifact_source() {
            return match src {
                ColSource::Default(value) => Ok(value.clone()),
                ColSource::Volume(phys_idx) => {
                    let group_idx = source.row_group_for_row(idx)?;
                    let range = source.row_group_range(group_idx)?;
                    let local_idx = idx.checked_sub(range.start).ok_or_else(|| {
                        Error::internal("DATA compaction row precedes its row group")
                    })?;
                    let block = cache.artifact_block(source, *phys_idx, group_idx)?;
                    Ok(block.get_value(local_idx))
                }
            };
        }

        if !self.is_cold() {
            return Ok(match src {
                ColSource::Volume(phys_idx) => self.columns[*phys_idx].get_value(idx),
                ColSource::Default(value) => value.clone(),
            });
        }

        Err(Error::internal(
            "metadata-only compaction volume has no DATA artifact source",
        ))
    }

    /// Get a row using a precomputed column mapping.
    /// Materializes all schema columns through the mapping.
    pub fn get_row_mapped(&self, idx: usize, mapping: &ColumnMapping) -> Row {
        let values: Vec<Value> = mapping
            .sources
            .iter()
            .map(|src| match src {
                ColSource::Volume(vol_idx) => self.columns[*vol_idx].get_value(idx),
                ColSource::Default(val) => val.clone(),
            })
            .collect();
        let value_count = values.len();
        let row = Row::from_values(values);
        instrumentation::record_row_materialization_count(1, value_count as u64);
        row
    }

    /// Get specific columns of a row using a precomputed column mapping.
    /// Only materializes the requested schema columns — skips the rest.
    pub fn get_row_mapped_projected(
        &self,
        idx: usize,
        mapping: &ColumnMapping,
        col_indices: &[usize],
    ) -> Row {
        let values: Vec<Value> = col_indices
            .iter()
            .map(|&ci| match &mapping.sources[ci] {
                ColSource::Volume(vol_idx) => self.columns[*vol_idx].get_value(idx),
                ColSource::Default(val) => val.clone(),
            })
            .collect();
        let value_count = values.len();
        let row = Row::from_values(values);
        instrumentation::record_row_materialization_count(1, value_count as u64);
        row
    }

    /// Get a row materializing only columns marked true in the mask.
    /// Other columns get typed Null (stack-only, zero allocation).
    /// The row has full schema width so filter column indices work.
    /// Uses LazyColumns::data_type() for unneeded columns to avoid decompression.
    #[inline]
    pub fn get_row_needed(&self, idx: usize, needed: &[bool]) -> Row {
        let values: Vec<Value> = (0..self.columns.len())
            .map(|ci| {
                if ci < needed.len() && needed[ci] {
                    self.columns[ci].get_value(idx)
                } else {
                    Value::Null(self.columns.data_type(ci))
                }
            })
            .collect();
        let value_count = values.len();
        let row = Row::from_values(values);
        instrumentation::record_row_materialization_count(1, value_count as u64);
        row
    }

    /// Get a row using a mapping, materializing only needed columns.
    /// Combines schema evolution (mapping) with column pruning (mask).
    /// Uses LazyColumns::data_type() for unneeded columns to avoid decompression.
    #[inline]
    pub fn get_row_mapped_needed(
        &self,
        idx: usize,
        mapping: &ColumnMapping,
        needed: &[bool],
    ) -> Row {
        let values: Vec<Value> = mapping
            .sources
            .iter()
            .enumerate()
            .map(|(ci, src)| {
                if ci < needed.len() && needed[ci] {
                    match src {
                        ColSource::Volume(vol_idx) => self.columns[*vol_idx].get_value(idx),
                        ColSource::Default(val) => val.clone(),
                    }
                } else {
                    match src {
                        ColSource::Volume(vol_idx) => Value::Null(self.columns.data_type(*vol_idx)),
                        ColSource::Default(val) => Value::Null(val.data_type()),
                    }
                }
            })
            .collect();
        let value_count = values.len();
        let row = Row::from_values(values);
        instrumentation::record_row_materialization_count(1, value_count as u64);
        row
    }

    /// Get a row as a Vec of Values (for executor compatibility).
    pub fn get_row(&self, idx: usize) -> Row {
        let values: Vec<Value> = self.columns.iter().map(|col| col.get_value(idx)).collect();
        let value_count = values.len();
        let row = Row::from_values(values);
        instrumentation::record_row_materialization_count(1, value_count as u64);
        row
    }

    /// Get specific columns of a row (projection pushdown).
    pub fn get_row_projected(&self, idx: usize, col_indices: &[usize]) -> Row {
        let values: Vec<Value> = col_indices
            .iter()
            .map(|&col| self.columns[col].get_value(idx))
            .collect();
        let value_count = values.len();
        let row = Row::from_values(values);
        instrumentation::record_row_materialization_count(1, value_count as u64);
        row
    }

    /// Check if a column is sorted (enables binary search).
    #[inline]
    pub fn is_sorted(&self, col_idx: usize) -> bool {
        self.meta.sorted_columns[col_idx]
    }

    /// Look up a composite unique key in this volume's per-volume hash index.
    /// Calls `f` for each matching row index. Supports volumes with duplicate values
    /// (pre-existing dupes not yet cleaned). The caller decides which match to accept
    /// (e.g., skip tombstoned rows, take first non-tombstoned).
    ///
    /// The index is built lazily on first call per column set and never invalidated
    /// (volume is immutable). Build cost: O(K) where K = this volume's row_count.
    /// Lookup cost: O(1) amortized.
    pub fn unique_lookup_all(
        &self,
        col_indices: &[usize],
        values: &[&Value],
        mut f: impl FnMut(u32) -> bool, // return true to stop early
    ) {
        if col_indices.iter().any(|&idx| idx >= self.columns.len()) {
            return;
        }

        let hash_compatible = col_indices
            .iter()
            .zip(values.iter())
            .all(|(&col_idx, &value)| {
                super::index_hash::persisted_index_hash_is_compatible(
                    self.meta.column_types[col_idx],
                    Some(value),
                )
            });
        if !hash_compatible {
            instrumentation::record_ram_accelerator_fallback();
            for row_idx in 0..self.meta.row_count {
                let matches = col_indices.iter().zip(values.iter()).all(|(&ci, &value)| {
                    let volume_value = self.columns[ci].get_value(row_idx);
                    !volume_value.is_null() && volume_value == *value
                });
                if matches && f(row_idx as u32) {
                    break;
                }
            }
            return;
        }

        // Compute hash of query values
        let mut hasher = super::index_hash::PersistedIndexHasher::new();
        for (&col_idx, &val) in col_indices.iter().zip(values.iter()) {
            hasher.add_value(self.meta.column_types[col_idx], val);
        }
        let hash = hasher.finish();

        // Fast path: check if index is already built
        {
            let indices = self.unique_indices.read();
            if let Some(sorted_idx) = indices.get(col_indices) {
                // Binary search for the hash, then scan all entries with same hash
                let pos = sorted_idx.partition_point(|&(h, _)| h < hash);
                let mut matched = false;
                for &(h, row_idx) in &sorted_idx[pos..] {
                    if h != hash {
                        break;
                    }
                    let matches = col_indices.iter().zip(values.iter()).all(|(&ci, &val)| {
                        let vol_val = self.columns[ci].get_value(row_idx as usize);
                        !vol_val.is_null() && vol_val == *val
                    });
                    if matches {
                        matched = true;
                        if f(row_idx) {
                            instrumentation::record_ram_accelerator_hit();
                            return;
                        }
                    }
                }
                if matched {
                    instrumentation::record_ram_accelerator_hit();
                } else {
                    instrumentation::record_ram_accelerator_miss();
                }
                return;
            }
        }

        // Build sorted index for this column set (first use)
        let build_start = Instant::now();
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(self.meta.row_count);
        for row_idx in 0..self.meta.row_count {
            let mut row_hasher = super::index_hash::PersistedIndexHasher::new();
            let mut has_null = false;
            for &ci in col_indices {
                if self.columns[ci].is_null(row_idx) {
                    has_null = true;
                    break;
                }
                row_hasher.add_value(
                    self.meta.column_types[ci],
                    &self.columns[ci].get_value(row_idx),
                );
            }
            if has_null {
                continue;
            }
            entries.push((row_hasher.finish(), row_idx as u32));
        }
        entries.sort_unstable_by_key(|&(h, _)| h);
        instrumentation::record_ram_accelerator_build(
            entries.len() as u64,
            (entries.len() * std::mem::size_of::<(u64, u32)>()) as u64,
            build_start.elapsed(),
        );

        // Look up before storing
        let pos = entries.partition_point(|&(h, _)| h < hash);
        let mut matched = false;
        for &(h, row_idx) in &entries[pos..] {
            if h != hash {
                break;
            }
            let matches = col_indices.iter().zip(values.iter()).all(|(&ci, &val)| {
                let vol_val = self.columns[ci].get_value(row_idx as usize);
                !vol_val.is_null() && vol_val == *val
            });
            if matches {
                matched = true;
                if f(row_idx) {
                    break;
                }
            }
        }
        if matched {
            instrumentation::record_ram_accelerator_hit();
        } else {
            instrumentation::record_ram_accelerator_miss();
        }

        // Store the built index
        self.unique_indices
            .write()
            .insert(col_indices.to_vec(), entries);
    }

    /// Pre-build the unique sorted index for a set of column indices.
    /// Called during seal/compaction so the first INSERT after seal doesn't
    /// pay a ~60ms stall scanning all rows to build the index.
    pub fn prebuild_unique_index(&self, col_indices: &[usize]) {
        if col_indices.iter().any(|&idx| idx >= self.columns.len()) {
            return;
        }
        if col_indices.iter().any(|&idx| {
            !super::index_hash::persisted_index_hash_is_compatible(
                self.meta.column_types[idx],
                None,
            )
        }) {
            return;
        }
        if self.unique_indices.read().contains_key(col_indices) {
            return;
        }
        let build_start = Instant::now();
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(self.meta.row_count);
        for row_idx in 0..self.meta.row_count {
            let mut row_hasher = super::index_hash::PersistedIndexHasher::new();
            let mut has_null = false;
            for &ci in col_indices {
                if self.columns[ci].is_null(row_idx) {
                    has_null = true;
                    break;
                }
                row_hasher.add_value(
                    self.meta.column_types[ci],
                    &self.columns[ci].get_value(row_idx),
                );
            }
            if has_null {
                continue;
            }
            entries.push((row_hasher.finish(), row_idx as u32));
        }
        entries.sort_unstable_by_key(|&(h, _)| h);
        instrumentation::record_ram_accelerator_build(
            entries.len() as u64,
            (entries.len() * std::mem::size_of::<(u64, u32)>()) as u64,
            build_start.elapsed(),
        );
        self.unique_indices
            .write()
            .insert(col_indices.to_vec(), entries);
    }

    /// Find the column index by name. O(1) via precomputed hashmap.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        if let Some(&idx) = self.meta.column_name_map.get(name) {
            return Some(idx);
        }
        let lower = name.to_lowercase();
        self.meta.column_name_map.get(lower.as_str()).copied()
    }

    /// Merge a column rename directly into column_name_map.
    /// Must be called BEFORE wrapping in Arc (takes &mut self).
    /// After this, column_index() finds both old and new names via the map.
    pub fn merge_column_rename(&mut self, new_name: &str, old_name: &str) {
        let meta = Arc::make_mut(&mut self.meta);
        let old_lower = SmartString::from(old_name.to_lowercase());
        let new_lower = SmartString::from(new_name.to_lowercase());
        if let Some(&idx) = meta.column_name_map.get(&old_lower) {
            meta.column_name_map.insert(new_lower, idx);
        } else if let Some(&idx) = meta.column_name_map.get(&new_lower) {
            // Already has the new name (chained rename handled)
            let _ = idx;
        }
    }

    /// Return the dictionary for a dictionary-encoded column.
    /// Avoids decompressing the full column — extracts from the shared
    /// dictionary stored in a materialized column.
    /// Returns None for non-dictionary or metadata-only columns.
    pub fn get_column_dictionary(&self, col_idx: usize) -> Option<Arc<[SmartString]>> {
        self.columns.get_column_dictionary(col_idx)
    }

    pub fn resident_memory(&self) -> VolumeResidentMemory {
        let dictionary_lookup_bytes = self
            .dictionary_lookup_indices
            .iter()
            .filter_map(OnceLock::get)
            .map(|ids| ids.len() * std::mem::size_of::<u32>())
            .sum::<usize>();
        let row_ids = self.meta.row_ids.retained_bytes();
        VolumeResidentMemory {
            metadata: self
                .meta
                .memory_size()
                .saturating_sub(row_ids)
                .saturating_add(dictionary_lookup_bytes),
            row_ids,
            column_payload: self.columns.memory_size(),
            exact_indices: posting_map_memory_size(&self.unique_indices.read()),
            ordered_indices: posting_map_memory_size(&self.ordered_indices.read()),
            descriptor: 0,
            block_source: 0,
        }
    }

    /// Estimate all retained memory owned by this volume. Engine-level shared
    /// caches and runtime configuration are reported by their own owners.
    pub fn memory_size(&self) -> usize {
        self.resident_memory().total()
    }

    /// Estimate evictable resident cache bytes for this volume.
    ///
    /// This deliberately excludes immutable metadata/descriptors because cold
    /// lookup, pruning and row routing depend on them staying resident. The
    /// returned number is the materialized payload controlled by eviction.
    pub fn cache_memory_size(&self) -> usize {
        self.columns.memory_size()
    }

    /// Artifact-backed volumes release all materialized payload in one
    /// demotion step.
    pub fn cache_memory_size_after_one_eviction_step(&self) -> usize {
        if self.columns.is_eager() {
            0
        } else {
            self.cache_memory_size()
        }
    }

    /// Mark this volume as recently accessed. Stores u64::MAX as a sentinel
    /// meaning "accessed since last eviction cycle." The eviction pass resets
    /// non-evicted volumes to current_epoch, so the idle counter only starts
    /// after the last access.
    #[inline]
    pub fn mark_accessed(&self) {
        self.last_access_epoch
            .store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether this volume is metadata-only and reads through its DATA source.
    pub fn is_cold(&self) -> bool {
        !self.columns.is_eager()
    }

    /// Create a metadata-only volume while retaining canonical artifact sources.
    pub fn to_cold(&self) -> FrozenVolume {
        FrozenVolume {
            columns: LazyColumns::metadata_only(self.meta.column_types.clone()),
            meta: Arc::clone(&self.meta),
            artifact_source: self.artifact_source.clone(),
            artifact_index_source: self.artifact_index_source.clone(),
            dictionary_lookup_indices: Arc::clone(&self.dictionary_lookup_indices),
            unique_indices: Arc::clone(&self.unique_indices),
            ordered_indices: Arc::clone(&self.ordered_indices),
            // Start recently accessed in the owning engine's epoch domain.
            last_access_epoch: std::sync::atomic::AtomicU64::new(u64::MAX),
        }
    }
}

fn merge_artifact_zone_maps(
    data_type: DataType,
    row_count: usize,
    statistics: &[&crate::v6::DataStatistics],
) -> Result<ZoneMap> {
    let row_count_u32 = u32::try_from(row_count)
        .map_err(|_| Error::internal("DATA zone-map row count exceeds u32"))?;
    let mut minimum: Option<Value> = None;
    let mut maximum: Option<Value> = None;
    let mut null_count = 0_u64;
    for entry in statistics {
        null_count = null_count
            .checked_add(entry.null_count())
            .ok_or_else(|| Error::internal("DATA zone-map NULL count overflows"))?;
        if let Some(candidate) = entry.minimum() {
            if minimum
                .as_ref()
                .is_none_or(|current| candidate.compare(current) == Ok(std::cmp::Ordering::Less))
            {
                minimum = Some(candidate.clone());
            }
        }
        if let Some(candidate) = entry.maximum() {
            if maximum
                .as_ref()
                .is_none_or(|current| candidate.compare(current) == Ok(std::cmp::Ordering::Greater))
            {
                maximum = Some(candidate.clone());
            }
        }
    }
    let null_count = u32::try_from(null_count)
        .map_err(|_| Error::internal("DATA zone-map NULL count exceeds u32"))?;
    if null_count > row_count_u32 {
        return Err(Error::internal(
            "DATA zone-map NULL count exceeds row count",
        ));
    }
    Ok(ZoneMap {
        min: minimum.unwrap_or_else(|| Value::null(data_type)),
        max: maximum.unwrap_or_else(|| Value::null(data_type)),
        null_count,
        row_count: row_count_u32,
    })
}

pub fn new_dictionary_lookup_indices(column_count: usize) -> Arc<[OnceLock<Box<[u32]>>]> {
    Arc::from(
        (0..column_count)
            .map(|_| OnceLock::new())
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::SchemaBuilder;

    fn test_schema() -> Schema {
        SchemaBuilder::new("test")
            .column("id", DataType::Integer, false, true)
            .column("time", DataType::Timestamp, false, false)
            .column("exchange", DataType::Text, false, false)
            .column("price", DataType::Float, false, false)
            .build()
    }

    #[test]
    fn r8_l01_batch_c_dense_row_ids_and_dictionary_lookup_are_compact_and_reused() {
        let dense = RowIds::try_from_sorted_iter(1..=1_000_000).unwrap();
        assert_eq!(dense.len(), 1_000_000);
        assert_eq!(dense.run_count(), 1);
        assert!(dense.retained_bytes() < 128);
        assert_eq!(dense.at(999_999), 1_000_000);
        assert_eq!(dense.binary_search(&500_000), Ok(499_999));

        let schema = SchemaBuilder::new("dictionary_lookup")
            .column("id", DataType::Integer, false, true)
            .column("name", DataType::Text, false, false)
            .build();
        let mut builder = VolumeBuilder::with_capacity(&schema, 3);
        for (id, name) in [(1, "zulu"), (2, "alpha"), (3, "middle")] {
            builder.add_row(
                id,
                &Row::from_values(vec![Value::Integer(id), Value::text(name)]),
            );
        }
        let volume = builder.finish();
        let before = volume.resident_memory().metadata;
        assert_eq!(volume.dictionary_id_for_text(1, "alpha"), Some(1));
        assert_eq!(volume.dictionary_id_for_text(1, "missing"), None);
        let after_first = volume.resident_memory().metadata;
        assert_eq!(after_first - before, 3 * std::mem::size_of::<u32>());
        assert_eq!(volume.dictionary_id_for_text(1, "zulu"), Some(0));
        assert_eq!(volume.resident_memory().metadata, after_first);
    }

    #[test]
    fn test_freeze_basic() {
        let schema = test_schema();
        let mut builder = VolumeBuilder::with_capacity(&schema, 3);

        let ts1 = chrono::Utc::now();
        let ts2 = ts1 + chrono::Duration::minutes(1);
        let ts3 = ts2 + chrono::Duration::minutes(1);

        builder.add_row(
            1,
            &Row::from_values(vec![
                Value::Integer(1),
                Value::Timestamp(ts1),
                Value::text("binance"),
                Value::Float(100.0),
            ]),
        );
        builder.add_row(
            2,
            &Row::from_values(vec![
                Value::Integer(2),
                Value::Timestamp(ts2),
                Value::text("coinbase"),
                Value::Float(101.5),
            ]),
        );
        builder.add_row(
            3,
            &Row::from_values(vec![
                Value::Integer(3),
                Value::Timestamp(ts3),
                Value::text("binance"),
                Value::Float(99.0),
            ]),
        );

        let volume = builder.finish();

        assert_eq!(volume.meta.row_count, 3);
        assert_eq!(volume.columns.len(), 4);
        assert_eq!(volume.meta.stats.count_star(), 3);

        // Check typed access
        assert_eq!(volume.columns[0].get_i64(0), 1);
        assert_eq!(volume.columns[0].get_i64(2), 3);
        assert_eq!(volume.columns[3].get_f64(1), 101.5);

        // Check dictionary encoding
        assert_eq!(volume.columns[2].get_str(0), "binance");
        assert_eq!(volume.columns[2].get_str(1), "coinbase");
        assert_eq!(volume.columns[2].get_str(2), "binance");
        // binance appears twice but uses same dict ID
        assert_eq!(
            volume.columns[2].get_dict_id(0),
            volume.columns[2].get_dict_id(2)
        );

        // Check zone maps
        assert_eq!(volume.meta.zone_maps[0].min, Value::Integer(1));
        assert_eq!(volume.meta.zone_maps[0].max, Value::Integer(3));
        assert_eq!(volume.meta.zone_maps[3].min, Value::Float(99.0));
        assert_eq!(volume.meta.zone_maps[3].max, Value::Float(101.5));

        // Check stats
        assert_eq!(volume.meta.stats.sum(3), 300.5); // 100.0 + 101.5 + 99.0

        // Check sortedness
        assert!(volume.is_sorted(0)); // id is sorted
        assert!(volume.is_sorted(1)); // time is sorted

        // Check row reconstruction
        let row = volume.get_row(0);
        assert_eq!(row.get(0), Some(&Value::Integer(1)));
        assert_eq!(row.get(2), Some(&Value::text("binance")));
    }

    #[test]
    fn r2_l06_batch_a_rejects_invalid_rows_before_builder_mutation() {
        let schema = SchemaBuilder::new("strict_builder")
            .column("id", DataType::Integer, false, true)
            .column("name", DataType::Text, false, false)
            .build();

        let mut wrong_width = VolumeBuilder::new(&schema);
        let width_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            wrong_width.add_row(1, &Row::from_values(vec![Value::Integer(1)]));
        }));
        assert!(
            width_result.is_err(),
            "r2_l06_batch_a: a short row must fail closed"
        );

        let mut wrong_type = VolumeBuilder::new(&schema);
        let type_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            wrong_type.add_row(
                1,
                &Row::from_values(vec![Value::Integer(1), Value::Integer(2)]),
            );
        }));
        assert!(
            type_result.is_err(),
            "r2_l06_batch_a: a physical type mismatch must fail closed"
        );

        let extension_schema = SchemaBuilder::new("strict_extension_builder")
            .column("payload", DataType::Json, false, false)
            .build();
        let mut malformed_extension = VolumeBuilder::new(&extension_schema);
        let malformed_json = Value::Extension(radixdb_core::CompactArc::from(
            [DataType::Json as u8, b'{'].as_slice(),
        ));
        assert!(malformed_extension
            .try_add_row(1, &Row::from_values(vec![malformed_json]))
            .is_err());
        assert_eq!(malformed_extension.finish().meta.row_count, 0);

        let mut duplicate_id = VolumeBuilder::new(&schema);
        duplicate_id.add_row(
            7,
            &Row::from_values(vec![Value::Integer(7), Value::text("first")]),
        );
        let duplicate_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            duplicate_id.add_row(
                7,
                &Row::from_values(vec![Value::Integer(8), Value::text("second")]),
            );
        }));
        assert!(
            duplicate_result.is_err(),
            "r2_l06_batch_a: duplicate row IDs must fail before publication"
        );
        let volume = duplicate_id.finish();
        assert_eq!(volume.meta.row_ids, vec![7]);
        assert_eq!(volume.meta.row_count, 1);
        assert_eq!(volume.get_row(0).len(), 2);
    }

    #[test]
    fn v2_r3_column_mapping_rejects_invalid_shape_and_derives_identity() {
        let schema = SchemaBuilder::new("mapping")
            .column("id", DataType::Integer, false, true)
            .column("name", DataType::Text, false, false)
            .build();
        let mut builder = VolumeBuilder::new(&schema);
        builder
            .try_add_row(
                1,
                &Row::from_values(vec![Value::Integer(1), Value::text("one")]),
            )
            .unwrap();
        let volume = builder.finish();

        let identity = ColumnMapping::try_new(
            &schema,
            &volume,
            vec![ColSource::Volume(0), ColSource::Volume(1)],
        )
        .unwrap();
        assert!(identity.is_identity());
        assert_eq!(identity.len(), 2);

        assert!(ColumnMapping::try_new(&schema, &volume, vec![ColSource::Volume(0)]).is_err());
        assert!(ColumnMapping::try_new(
            &schema,
            &volume,
            vec![ColSource::Volume(0), ColSource::Volume(9)],
        )
        .is_err());
        assert!(ColumnMapping::try_new(
            &schema,
            &volume,
            vec![ColSource::Volume(0), ColSource::Default(Value::Integer(7))],
        )
        .is_err());
        assert!(ColumnMapping::try_new(
            &schema,
            &volume,
            vec![ColSource::Volume(0), ColSource::Volume(0)],
        )
        .is_err());
    }

    #[test]
    fn eager_builder_rejects_timestamp_outside_artifact_nanoseconds() {
        let schema = SchemaBuilder::new("timestamp_bounds")
            .column("created_at", DataType::Timestamp, false, false)
            .build();
        let far_future = chrono::TimeZone::timestamp_opt(&chrono::Utc, 253_402_300_799, 0)
            .single()
            .unwrap();
        let mut builder = VolumeBuilder::new(&schema);

        let error = builder
            .try_add_row(1, &Row::from_values(vec![Value::Timestamp(far_future)]))
            .unwrap_err();
        assert!(error.to_string().contains("exact nanosecond range"));
        assert_eq!(builder.finish().meta.row_count, 0);
    }

    #[test]
    fn r3_l04_batch_b_chained_column_rename_resolves_original_physical_identity() {
        let physical_schema = SchemaBuilder::new("renamed")
            .column("id", DataType::Integer, false, true)
            .column("a", DataType::Text, false, false)
            .build();
        let mut builder = VolumeBuilder::with_capacity(&physical_schema, 1);
        builder.add_row(
            1,
            &Row::from_values(vec![Value::Integer(1), Value::text("kept")]),
        );
        let volume = builder.finish();
        let current_schema = SchemaBuilder::new("renamed")
            .column("id", DataType::Integer, false, true)
            .column("c", DataType::Text, false, false)
            .build();
        let renames = vec![("a".into(), "b".into()), ("b".into(), "c".into())];

        let mapping = compute_column_mapping_with_drops(&current_schema, &volume, &[], 0, &renames);
        match mapping.sources.get(1) {
            Some(ColSource::Volume(1)) => {}
            _ => panic!("logical c must resolve transitively to physical a"),
        }
        assert_eq!(
            volume.get_row_mapped(0, &mapping).get(1),
            Some(&Value::text("kept"))
        );
    }

    #[test]
    fn test_freeze_with_nulls() {
        let schema = test_schema();
        let mut builder = VolumeBuilder::new(&schema);

        builder.add_row(
            1,
            &Row::from_values(vec![
                Value::Integer(1),
                Value::Null(DataType::Timestamp),
                Value::text("binance"),
                Value::Null(DataType::Float),
            ]),
        );

        let volume = builder.finish();
        assert!(volume.columns[1].is_null(0));
        assert!(volume.columns[3].is_null(0));
        assert!(!volume.columns[0].is_null(0));

        let row = volume.get_row(0);
        assert_eq!(row.get(0), Some(&Value::Integer(1)));
        assert!(row.get(1).unwrap().is_null());
    }

    #[test]
    fn test_freeze_disables_bloom_for_extension_columns() {
        let schema = SchemaBuilder::new("test")
            .column("id", DataType::Integer, false, true)
            .column("payload", DataType::Json, true, false)
            .build();
        let mut builder = VolumeBuilder::new(&schema);
        builder.add_row(
            1,
            &Row::from_values(vec![Value::Integer(1), Value::json("{\"a\":1}")]),
        );
        builder.add_row(
            2,
            &Row::from_values(vec![Value::Integer(2), Value::json("{\"a\":2}")]),
        );

        let volume = builder.finish();
        assert!(!volume.meta.bloom_filters[0].is_disabled());
        assert!(volume.meta.bloom_filters[1].is_disabled());
        assert_eq!(volume.meta.bloom_filters[1].memory_size(), 0);
        assert!(volume.meta.bloom_filters[1].might_contain(&Value::json("{\"not_present\":true}")));
    }

    #[test]
    fn test_freeze_disables_float_bloom_for_current_writes() {
        let schema = SchemaBuilder::new("test")
            .column("id", DataType::Integer, false, true)
            .column("value", DataType::Float, false, false)
            .build();
        let mut builder = VolumeBuilder::new(&schema);
        builder.add_row(
            1,
            &Row::from_values(vec![Value::Integer(1), Value::Float(-0.0)]),
        );

        let volume = builder.finish();
        assert!(!volume.meta.bloom_filters[0].is_disabled());
        assert!(volume.meta.bloom_filters[1].is_disabled());
        assert_eq!(volume.meta.bloom_filters[1].memory_size(), 0);
        assert!(volume.meta.bloom_filters[1].might_contain(&Value::Float(0.0)));
        assert!(volume.meta.bloom_filters[1].might_contain(&Value::Integer(0)));
    }

    #[test]
    fn unique_lookup_falls_back_for_noncanonical_hash_domains() {
        let decimal_schema = SchemaBuilder::new("decimal_test")
            .column("amount", DataType::Decimal, false, false)
            .build();
        let mut builder = VolumeBuilder::new(&decimal_schema);
        builder.add_row(1, &Row::from_values(vec![Value::decimal(10, 2, 1)]));
        builder.add_row(2, &Row::from_values(vec![Value::decimal(20, 2, 1)]));
        let decimal_volume = builder.finish();

        decimal_volume.prebuild_unique_index(&[0]);
        assert!(decimal_volume.unique_indices.read().get(&vec![0]).is_none());

        let probe = Value::decimal(100, 3, 2);
        let mut matches = Vec::new();
        decimal_volume.unique_lookup_all(&[0], &[&probe], |row_idx| {
            matches.push(row_idx);
            false
        });
        assert_eq!(matches, vec![0]);

        let integer_schema = SchemaBuilder::new("integer_test")
            .column("value", DataType::Integer, false, false)
            .build();
        let mut builder = VolumeBuilder::new(&integer_schema);
        builder.add_row(1, &Row::from_values(vec![Value::Integer(42)]));
        let integer_volume = builder.finish();
        integer_volume.prebuild_unique_index(&[0]);
        assert!(integer_volume.unique_indices.read().get(&vec![0]).is_some());

        let float_probe = Value::Float(42.0);
        let mut matches = Vec::new();
        integer_volume.unique_lookup_all(&[0], &[&float_probe], |row_idx| {
            matches.push(row_idx);
            false
        });
        assert_eq!(matches, vec![0]);
    }

    #[test]
    fn test_binary_search_on_sorted() {
        let schema = SchemaBuilder::new("test")
            .column("time", DataType::Timestamp, false, false)
            .build();
        let mut builder = VolumeBuilder::new(&schema);

        let base = chrono::Utc::now();
        for i in 0..100 {
            let ts = base + chrono::Duration::minutes(i);
            builder.add_row(i, &Row::from_values(vec![Value::Timestamp(ts)]));
        }

        let volume = builder.finish();
        assert!(volume.is_sorted(0));

        // Binary search for row 50
        let target_nanos = {
            let ts = base + chrono::Duration::minutes(50);
            ts.timestamp_nanos_opt()
                .unwrap_or(ts.timestamp() * 1_000_000_000)
        };
        let idx = volume.columns[0].binary_search_ge(target_nanos);
        assert_eq!(idx, 50);
    }

    #[test]
    fn test_projection() {
        let schema = test_schema();
        let mut builder = VolumeBuilder::new(&schema);

        builder.add_row(
            1,
            &Row::from_values(vec![
                Value::Integer(1),
                Value::Timestamp(chrono::Utc::now()),
                Value::text("binance"),
                Value::Float(100.0),
            ]),
        );

        let volume = builder.finish();

        // Project only id and price (columns 0 and 3)
        let row = volume.get_row_projected(0, &[0, 3]);
        assert_eq!(row.len(), 2);
        assert_eq!(row.get(0), Some(&Value::Integer(1)));
        assert_eq!(row.get(1), Some(&Value::Float(100.0)));
    }

    #[test]
    fn unique_lookup_all_records_ram_accelerator_metrics() {
        let schema = SchemaBuilder::new("test")
            .column("id", DataType::Integer, false, true)
            .column("name", DataType::Text, false, false)
            .build();
        let mut builder = VolumeBuilder::with_capacity(&schema, 3);
        builder.add_row(
            10,
            &Row::from_values(vec![Value::Integer(1), Value::text("one")]),
        );
        builder.add_row(
            20,
            &Row::from_values(vec![Value::Integer(2), Value::text("two")]),
        );
        builder.add_row(
            30,
            &Row::from_values(vec![Value::Integer(3), Value::text("three")]),
        );
        let volume = builder.finish();

        let wanted = Value::Integer(2);
        let mut found = None;
        instrumentation::begin_ram_accelerator_probe();
        volume.unique_lookup_all(&[0], &[&wanted], |row_idx| {
            found = Some(row_idx);
            true
        });
        let first_lookup = instrumentation::end_ram_accelerator_probe();
        assert_eq!(found, Some(1));

        assert_eq!(first_lookup.builds, 1);
        assert_eq!(first_lookup.build_entries, 3);
        assert!(first_lookup.build_bytes > 0);
        assert_eq!(first_lookup.hits, 1);
        assert_eq!(first_lookup.misses, 0);

        let missing = Value::Integer(404);
        instrumentation::begin_ram_accelerator_probe();
        volume.unique_lookup_all(&[0], &[&missing], |_| {
            panic!("missing value must not call candidate callback")
        });
        let missing_lookup = instrumentation::end_ram_accelerator_probe();

        assert_eq!(missing_lookup.builds, 0);
        assert_eq!(missing_lookup.hits, 0);
        assert_eq!(missing_lookup.misses, 1);
    }
}
