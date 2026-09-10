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

//! Scanner implementation for frozen volumes.
//!
//! Implements the `Scanner` trait so frozen volumes can be used by the executor
//! through the same interface as live tables. The scanner supports:
//! - Column projection (only reconstruct Values for needed columns)
//! - Range filtering on sorted columns (binary search start position)
//! - Zone map pruning (skip entire volume if predicate doesn't match)
//!
//! This is the bridge between the column-major volume storage and the
//! row-major executor. Values are reconstructed lazily, one row at a time.

use std::sync::Arc;

use crate::traits::{EmptyScanner, Scanner, TypedBatchFallbackReason, TypedColumnBatch};
use radixdb_core::{DataType, Error, Result, Row, Schema, Value};

use super::writer::FrozenVolume;

mod merge;

pub use merge::MergingScanner;
pub(crate) use merge::RowTypedScanner;

#[cfg(test)]
mod tests;

// =============================================================================
// Columnar pre-filter: typed predicates evaluated directly on column data.
// Avoids full Value reconstruction for rows that don't match.
// =============================================================================

/// Typed target for columnar pre-filter comparison.
/// Operates on raw column data without constructing Value objects.
#[derive(Clone)]
enum TypedTarget {
    Int64(i64),
    Float64(f64),
    Text(String),
    Bool(bool),
}

#[derive(Clone)]
enum TypedInList {
    Int64(radixdb_core::I64Set),
    Float64(Vec<f64>),
    Bool { has_true: bool, has_false: bool },
    Text(rustc_hash::FxHashSet<String>),
}

#[derive(Clone)]
enum ColumnPredicateKind {
    Comparison {
        op: radixdb_core::Operator,
        target: TypedTarget,
    },
    InList {
        values: TypedInList,
        not: bool,
        has_null: bool,
    },
    NullCheck {
        is_null: bool,
    },
    LikePrefix {
        prefix: String,
    },
}

/// A predicate that can be evaluated directly on typed column data.
/// Extracted from the WHERE clause during `set_filter()`.
/// Safety invariant: must never reject a row that should match.
#[derive(Clone)]
struct ColumnPredicate {
    col_idx: usize,
    kind: ColumnPredicateKind,
}

#[derive(Clone)]
enum ColumnPredicateExpr {
    Single(ColumnPredicate),
    All(Vec<ColumnPredicateExpr>),
    Any(Vec<ColumnPredicateExpr>),
}

#[derive(Clone)]
enum TypedBatchSource {
    Volume(usize),
    Default(Value),
}

/// Cache holding decompressed columns for a single row group.
/// Avoids decompressing the entire column when only a few groups are needed.
struct GroupColumnCache {
    #[allow(dead_code)]
    group_idx: usize,
    /// Decompressed columns for this group (only needed columns populated)
    columns: Vec<Option<super::column::ColumnData>>,
    /// Global row index where this group starts
    group_start: usize,
}

impl GroupColumnCache {
    /// Get column data and local row index for a global row index.
    #[inline(always)]
    fn col_and_local(
        &self,
        col_idx: usize,
        global_idx: usize,
    ) -> Option<(&super::column::ColumnData, usize)> {
        self.columns[col_idx]
            .as_ref()
            .map(|col| (col, global_idx - self.group_start))
    }
}

/// Scanner over a frozen volume that implements the `Scanner` trait.
///
/// Reconstructs rows lazily from column-major data, projecting only
/// the requested columns. Skips rows marked as deleted in the segment-scoped
/// delete vector. Optionally evaluates a predicate to skip non-matching rows
/// without full Value construction.
pub struct VolumeScanner {
    /// Shared reference to the frozen volume
    volume: Arc<FrozenVolume>,
    /// Column indices to project (empty = all columns)
    project_cols: Vec<usize>,
    /// Pre-computed flag: true when project_cols is an identity mapping over
    /// all volume columns. Avoids recomputing this check on every row.
    is_full_projection: bool,
    /// Current scan position
    current_idx: usize,
    /// End position (exclusive) — may be less than row_count for filtered scans
    end_idx: usize,
    /// Current reconstructed row
    current_row: Row,
    /// Row ID of the current row (from segment's row_ids array)
    current_rid: i64,
    /// Whether we have a valid current row
    has_current: bool,
    /// True after this scanner has produced rows through the row-oriented
    /// `Scanner::next()` path. A typed batch may start from an arbitrary
    /// constructor range, but it must not resume after row iteration consumed
    /// part of the same cursor.
    row_iteration_started: bool,
    /// Precomputed column mapping (None = volume matches current schema).
    /// When set and not identity, replaces per-row name-based normalization
    /// with per-row index lookup through the mapping.
    column_mapping: Option<super::writer::ColumnMapping>,
    /// Any error that occurred
    error: Option<Error>,
    /// Optional predicate filter (from WHERE clause pushdown)
    filter: Option<Box<dyn crate::expression::Expression>>,
    /// Pre-resolved dictionary filters: (col_idx, dict_id) pairs.
    /// Enables O(1) u32 comparison per row instead of full Value reconstruction.
    dict_filters: Vec<(usize, u32)>,
    /// Pre-computed matching row indices (when dictionary filters narrow enough).
    /// When set, iteration skips the linear scan entirely.
    matching_indices: Option<Vec<usize>>,
    /// Current position in matching_indices.
    match_idx: usize,
    /// Pre-computed inter-volume visibility bitmap.
    /// Bit i is set (1) if row at index i is visible (not overridden by a newer volume).
    /// Stored as packed u64 words: word w covers rows [w*64 .. w*64+63].
    /// When None, all rows are assumed visible (no inter-volume dedup needed).
    visibility_bitmap: Option<Arc<Vec<u64>>>,
    /// Per-transaction pending cold deletes (deferred, not yet in shared DV).
    /// The owning transaction sees these as deleted; other transactions don't.
    pending_cold_deletes: Option<Arc<rustc_hash::FxHashSet<i64>>>,
    /// Committed tombstones (shared, immutable Arc reference — no clone).
    /// Kept separate from pending_cold_deletes to avoid cloning the tombstone set.
    /// Map: row_id → commit_seq (for snapshot isolation filtering).
    committed_tombstones: Option<Arc<rustc_hash::FxHashMap<i64, u64>>>,
    /// True only when at least one row-level visibility/skip overlay exists.
    /// The overwhelmingly common single-cold-segment case leaves it false, so
    /// the scan loop never loads a row id or probes empty hash tables.
    has_row_skip_overlay: bool,
    /// Snapshot sequence: if Some, only tombstones with commit_seq <= this are visible.
    /// None means auto-commit (all tombstones visible).
    pub snapshot_seq: Option<u64>,
    /// Typed pre-filter predicates extracted from the WHERE clause.
    /// Evaluated directly on column data without Value construction.
    typed_predicates: Vec<ColumnPredicate>,
    /// Exact columnar representation of the whole filter when it is built
    /// only from supported typed column predicates. Unlike `typed_predicates`,
    /// this preserves boolean shape (`AND`/`OR`) and can safely replace the
    /// row-level `filter.evaluate_fast()` pass.
    exact_typed_filter: Option<ColumnPredicateExpr>,
    /// True when the entire filter is represented by `exact_typed_filter`.
    /// In this mode accepted rows do not need a second full-row
    /// `filter.evaluate_fast()` pass.
    filter_covered_by_typed_predicates: bool,
    /// Precomputed set of columns needed for filter + projection.
    /// When set, the filter path materializes only these columns instead
    /// of all columns. Built in set_filter() from filter's referenced
    /// columns ∪ project_cols. None = materialize all (fallback).
    needed_cols: Option<Vec<bool>>,
    /// Pre-computed row group skip decisions. group_idx → can skip entirely.
    /// None = no row groups (small volume or no filter). Computed in set_filter().
    row_group_skips: Option<Vec<bool>>,
    /// Per-group column cache: decompresses only needed columns for the current
    /// group instead of the entire column. Active when the volume has an in-memory
    /// compressed store. Dramatically reduces decompression work for selective scans.
    group_cache: Option<GroupColumnCache>,
    /// Cached end of the current row group (exclusive index). Avoids per-row
    /// integer division in the slow-path scan loop. Recomputed only on group
    /// boundary crossings. 0 means "not yet initialized".
    next_group_boundary: usize,
    /// Terminal lifecycle state. Once closed, no row or typed-batch method may
    /// restart I/O or recreate released owners.
    closed: bool,
}

impl VolumeScanner {
    /// Compute whether `project_cols` is an identity mapping over all volume columns.
    /// Extracted as a helper so both constructors share the same logic.
    #[inline]
    fn compute_is_full_projection(project_cols: &[usize], num_cols: usize) -> bool {
        project_cols.len() == num_cols && project_cols.iter().enumerate().all(|(i, &c)| c == i)
    }

    fn normalize_projection(
        project_cols: Vec<usize>,
        num_cols: usize,
        empty_projection_means_all: bool,
    ) -> Vec<usize> {
        if empty_projection_means_all && project_cols.is_empty() {
            (0..num_cols).collect()
        } else {
            project_cols
        }
    }

    fn new_with_projection_mode(
        volume: Arc<FrozenVolume>,
        project_cols: Vec<usize>,
        start_idx: usize,
        end_idx: usize,
        empty_projection_means_all: bool,
    ) -> Self {
        let project = Self::normalize_projection(
            project_cols,
            volume.columns.len(),
            empty_projection_means_all,
        );
        let is_full_projection = Self::compute_is_full_projection(&project, volume.columns.len());
        // Mark access with an engine-independent sentinel. The owning
        // SegmentManager converts it to its own eviction epoch.
        volume.mark_accessed();
        let mut s = Self {
            volume,
            project_cols: project,
            is_full_projection,
            current_idx: start_idx,
            end_idx,
            current_row: Row::new(),
            current_rid: 0,
            has_current: false,
            row_iteration_started: false,
            error: None,
            filter: None,
            column_mapping: None,
            dict_filters: Vec::new(),
            matching_indices: None,
            match_idx: 0,
            visibility_bitmap: None,
            pending_cold_deletes: None,
            committed_tombstones: None,
            has_row_skip_overlay: false,
            snapshot_seq: None,
            typed_predicates: Vec::new(),
            exact_typed_filter: None,
            filter_covered_by_typed_predicates: false,
            needed_cols: None,
            row_group_skips: None,
            group_cache: None,
            next_group_boundary: 0,
            closed: false,
        };
        if !s.is_full_projection && s.should_use_group_cache() {
            let mut mask = vec![false; s.volume.columns.len()];
            for &ci in &s.project_cols {
                if ci < mask.len() {
                    mask[ci] = true;
                }
            }
            s.needed_cols = Some(mask);
        }
        s
    }

    fn validate_admission(
        volume: &FrozenVolume,
        project_cols: &[usize],
        start_idx: usize,
        end_idx: usize,
    ) -> Result<()> {
        if start_idx > end_idx || end_idx > volume.meta.row_count {
            return Err(Error::invalid_argument(format!(
                "volume scan range {}..{} is outside row count {}",
                start_idx, end_idx, volume.meta.row_count
            )));
        }
        if let Some(column) = project_cols
            .iter()
            .copied()
            .find(|column| *column >= volume.columns.len())
        {
            return Err(Error::invalid_argument(format!(
                "volume scan projection column {} is outside column count {}",
                column,
                volume.columns.len()
            )));
        }
        Ok(())
    }

    pub fn try_new(
        volume: Arc<FrozenVolume>,
        project_cols: Vec<usize>,
        _delete_vector: Option<()>,
    ) -> Result<Self> {
        let end_idx = volume.meta.row_count;
        Self::validate_admission(&volume, &project_cols, 0, end_idx)?;
        Ok(Self::new_with_projection_mode(
            volume,
            project_cols,
            0,
            end_idx,
            true,
        ))
    }

    pub fn try_new_exact_projection(
        volume: Arc<FrozenVolume>,
        project_cols: Vec<usize>,
    ) -> Result<Self> {
        let end_idx = volume.meta.row_count;
        Self::validate_admission(&volume, &project_cols, 0, end_idx)?;
        Ok(Self::new_with_projection_mode(
            volume,
            project_cols,
            0,
            end_idx,
            false,
        ))
    }

    pub fn try_with_range(
        volume: Arc<FrozenVolume>,
        project_cols: Vec<usize>,
        start_idx: usize,
        end_idx: usize,
        _delete_vector: Option<()>,
    ) -> Result<Self> {
        Self::validate_admission(&volume, &project_cols, start_idx, end_idx)?;
        Ok(Self::new_with_projection_mode(
            volume,
            project_cols,
            start_idx,
            end_idx,
            true,
        ))
    }

    pub fn try_with_range_exact_projection(
        volume: Arc<FrozenVolume>,
        project_cols: Vec<usize>,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<Self> {
        Self::validate_admission(&volume, &project_cols, start_idx, end_idx)?;
        Ok(Self::new_with_projection_mode(
            volume,
            project_cols,
            start_idx,
            end_idx,
            false,
        ))
    }

    /// Create a scanner over all rows in the volume.
    ///
    /// Backward-compatible behavior: an empty projection means all columns.
    #[doc(hidden)]
    pub fn new(
        volume: Arc<FrozenVolume>,
        project_cols: Vec<usize>,
        _delete_vector: Option<()>,
    ) -> Self {
        let end_idx = volume.meta.row_count;
        Self::new_with_projection_mode(volume, project_cols, 0, end_idx, true)
    }

    /// Create a scanner over all rows with an exact projection.
    ///
    /// Unlike `new`, an empty projection means zero output columns. This is
    /// used by storage-level operators such as COUNT(*) that need row
    /// boundaries but no materialized values.
    #[doc(hidden)]
    pub fn new_exact_projection(volume: Arc<FrozenVolume>, project_cols: Vec<usize>) -> Self {
        let end_idx = volume.meta.row_count;
        Self::new_with_projection_mode(volume, project_cols, 0, end_idx, false)
    }

    /// Create a scanner with a start/end range (for binary-search narrowing).
    #[doc(hidden)]
    pub fn with_range(
        volume: Arc<FrozenVolume>,
        project_cols: Vec<usize>,
        start_idx: usize,
        end_idx: usize,
        _delete_vector: Option<()>,
    ) -> Self {
        Self::new_with_projection_mode(volume, project_cols, start_idx, end_idx, true)
    }

    /// Create a ranged scanner with exact projection semantics.
    ///
    /// Unlike `with_range`, an empty projection means zero output columns.
    #[doc(hidden)]
    pub fn with_range_exact_projection(
        volume: Arc<FrozenVolume>,
        project_cols: Vec<usize>,
        start_idx: usize,
        end_idx: usize,
    ) -> Self {
        Self::new_with_projection_mode(volume, project_cols, start_idx, end_idx, false)
    }

    /// Set per-transaction pending cold deletes. The owning transaction
    /// sees these row_ids as deleted even though the shared DV hasn't
    /// been updated yet (deferred to commit).
    pub fn set_pending_cold_deletes(&mut self, pending: Arc<rustc_hash::FxHashSet<i64>>) {
        self.pending_cold_deletes = (!pending.is_empty()).then_some(pending);
        self.refresh_row_skip_overlay();
    }

    /// Set both committed tombstones (shared Arc, no clone) and dynamic
    /// skip set (hot row_ids + pending tombstones + per-volume dedup IDs).
    /// This avoids cloning the potentially large committed tombstone set.
    pub fn set_skip_sets(
        &mut self,
        committed: Arc<rustc_hash::FxHashMap<i64, u64>>,
        dynamic: Arc<rustc_hash::FxHashSet<i64>>,
    ) {
        self.committed_tombstones = (!committed.is_empty()).then_some(committed);
        self.pending_cold_deletes = (!dynamic.is_empty()).then_some(dynamic);
        self.refresh_row_skip_overlay();
    }

    /// Set a pre-computed inter-volume visibility bitmap.
    /// Bit i is 1 if the row at position i in this volume is visible (not overridden by
    /// a newer volume). Bit i being 0 means a newer volume has a row with the same row_id,
    /// so this row should be skipped without materialization.
    pub fn set_visibility_bitmap(&mut self, bitmap: Option<Arc<Vec<u64>>>) {
        self.visibility_bitmap = bitmap;
        self.refresh_row_skip_overlay();
    }

    #[inline]
    fn refresh_row_skip_overlay(&mut self) {
        self.has_row_skip_overlay = self.visibility_bitmap.is_some()
            || self.committed_tombstones.is_some()
            || self.pending_cold_deletes.is_some();
    }

    /// Create an empty scanner (for zone-map-pruned volumes that match nothing).
    pub fn empty() -> Self {
        Self {
            volume: Arc::new(FrozenVolume {
                columns: super::writer::LazyColumns::empty(),
                meta: Arc::new(super::writer::VolumeMeta {
                    zone_maps: Vec::new(),
                    bloom_filters: Vec::new(),
                    stats: super::stats::VolumeAggregateStats::new(0),
                    row_count: 0,
                    column_names: Vec::new(),
                    column_types: Vec::new(),
                    column_logical_types: Vec::new(),
                    row_ids: Default::default(),
                    sorted_columns: Vec::new(),
                    column_name_map: ahash::AHashMap::new(),
                    row_groups: Vec::new(),
                }),
                artifact_source: None,
                artifact_index_source: None,
                dictionary_lookup_indices: super::writer::new_dictionary_lookup_indices(0),
                unique_indices: std::sync::Arc::new(parking_lot::RwLock::new(
                    rustc_hash::FxHashMap::default(),
                )),
                ordered_indices: std::sync::Arc::new(parking_lot::RwLock::new(
                    rustc_hash::FxHashMap::default(),
                )),
                last_access_epoch: std::sync::atomic::AtomicU64::new(0),
            }),
            project_cols: Vec::new(),
            is_full_projection: true,
            current_idx: 0,
            end_idx: 0,
            current_row: Row::new(),
            current_rid: 0,
            has_current: false,
            row_iteration_started: false,
            error: None,
            filter: None,
            column_mapping: None,
            dict_filters: Vec::new(),
            matching_indices: None,
            match_idx: 0,
            visibility_bitmap: None,
            pending_cold_deletes: None,
            committed_tombstones: None,
            has_row_skip_overlay: false,
            snapshot_seq: None,
            typed_predicates: Vec::new(),
            exact_typed_filter: None,
            filter_covered_by_typed_predicates: false,
            needed_cols: None,
            row_group_skips: None,
            group_cache: None,
            next_group_boundary: 0,
            closed: false,
        }
    }

    /// Restrict this physical source to stable inclusive row-ID ranges. The
    /// volume row-ID vector is metadata, so candidate positions are prepared
    /// without decoding payload columns. Existing dictionary candidates are
    /// intersected rather than replaced.
    pub fn set_row_id_ranges(&mut self, ranges: Arc<Vec<(i64, i64)>>) {
        let candidates: Box<dyn Iterator<Item = usize>> =
            if let Some(existing) = self.matching_indices.take() {
                Box::new(existing.into_iter())
            } else {
                Box::new(self.current_idx..self.end_idx)
            };
        let mut indices = Vec::new();
        for index in candidates {
            match self.volume.row_id_at(index) {
                Ok(row_id)
                    if ranges
                        .iter()
                        .any(|(minimum, maximum)| row_id >= *minimum && row_id <= *maximum) =>
                {
                    indices.push(index);
                }
                Ok(_) => {}
                Err(error) => {
                    self.error = Some(error);
                    self.matching_indices = Some(Vec::new());
                    self.match_idx = 0;
                    return;
                }
            }
        }
        self.matching_indices = Some(indices);
        self.match_idx = 0;
    }

    /// Set a predicate filter on this scanner.
    /// Automatically extracts dictionary-based fast filters for text equality predicates.
    pub fn set_filter(&mut self, filter: Box<dyn crate::expression::Expression>) {
        self.group_cache = None;
        self.next_group_boundary = 0;
        // Extract dictionary filters for fast pre-filtering.
        let comparisons = filter.collect_comparisons();
        // Artifact dictionaries are row-group local. A missing value in one
        // dictionary says nothing about the rest of the segment, so the old
        // volume-wide dictionary shortcut is deliberately disabled there.
        if self.volume.artifact_source().is_none() {
            for &(col_name, op, value) in &comparisons {
                if op != radixdb_core::Operator::Eq {
                    continue;
                }
                if let Value::Text(s) = value {
                    if let Some(col_idx) = self.volume.column_index(col_name) {
                        let dict_id = self.volume.dictionary_id_for_text(col_idx, s.as_str());
                        if let Some(id) = dict_id {
                            self.dict_filters.push((col_idx, id));
                        } else {
                            self.current_idx = self.end_idx;
                            self.filter = Some(filter);
                            return;
                        }
                    }
                }
            }
        }
        // Pre-compute matching row indices from dictionary filters.
        // Skip pre-computation when match rate is too high (>10%) to avoid
        // large Vec allocation — use streaming dict filter in the slow path instead.
        if !self.dict_filters.is_empty() {
            let scan_range = self.end_idx - self.current_idx;
            let selectivity_cap = scan_range / 10; // 10% threshold
            let matches = if self.volume.artifact_source().is_some() {
                None
            } else {
                let mut m = Vec::new();
                for i in self.current_idx..self.end_idx {
                    let ok = self.dict_filters.iter().all(|&(ci, eid)| {
                        !self.volume.columns[ci].is_null(i)
                            && self.volume.columns[ci].get_dict_id(i) == eid
                    });
                    if ok {
                        m.push(i);
                    }
                    if m.len() > selectivity_cap {
                        break;
                    }
                }
                if m.len() > selectivity_cap {
                    None
                } else {
                    Some(m)
                }
            };
            self.matching_indices = matches;
        }
        // Extract typed pre-filter predicates using data_type() (no column decompression).
        //
        // The flat list is intentionally AND-only and remains a safe rejection
        // prefilter. Exact filter coverage is tracked separately by
        // `exact_typed_filter`, which preserves boolean shape (`AND`/`OR`).
        for &(col_name, op, value) in &comparisons {
            if !matches!(
                op,
                radixdb_core::Operator::Eq
                    | radixdb_core::Operator::Ne
                    | radixdb_core::Operator::Gt
                    | radixdb_core::Operator::Gte
                    | radixdb_core::Operator::Lt
                    | radixdb_core::Operator::Lte
            ) {
                continue;
            }
            let Some(predicate) = self.build_comparison_column_predicate(col_name, op, value)
            else {
                continue;
            };
            self.typed_predicates.push(predicate);
        }

        for (col_name, values, not, has_null) in filter.collect_in_list_infos() {
            let Some(col_idx) = self.volume.column_index(col_name) else {
                continue;
            };
            let Some(values) =
                Self::build_typed_in_list(values, self.volume.columns.data_type(col_idx))
            else {
                continue;
            };
            self.typed_predicates.push(ColumnPredicate {
                col_idx,
                kind: ColumnPredicateKind::InList {
                    values,
                    not,
                    has_null,
                },
            });
        }

        for (col_name, is_null) in filter.collect_null_check_infos() {
            let Some(col_idx) = self.volume.column_index(col_name) else {
                continue;
            };
            self.typed_predicates.push(ColumnPredicate {
                col_idx,
                kind: ColumnPredicateKind::NullCheck { is_null },
            });
        }

        for (col_name, prefix, negated) in filter.collect_like_prefix_infos() {
            if negated {
                continue;
            }
            let Some(col_idx) = self.volume.column_index(col_name) else {
                continue;
            };
            if self.volume.columns.data_type(col_idx) != radixdb_core::DataType::Text {
                continue;
            }
            self.typed_predicates.push(ColumnPredicate {
                col_idx,
                kind: ColumnPredicateKind::LikePrefix { prefix },
            });
        }

        let exact_typed_filter = self.build_exact_typed_filter(filter.as_ref());
        self.filter_covered_by_typed_predicates = exact_typed_filter.is_some();
        self.exact_typed_filter = exact_typed_filter;

        // Try to extract which columns the filter references.
        // If successful, combine with project_cols to build a bitmask
        // of columns needed during filter evaluation. This enables
        // column pruning: only those columns are materialized from the
        // column store, skipping expensive Text/JSON clones for
        // unreferenced columns.
        let mut filter_cols = Vec::new();
        if filter.collect_column_indices(&mut filter_cols) {
            let num_cols = self.volume.columns.len();
            // Use the larger of volume columns and mapping sources length
            // to handle schema-evolved volumes.
            let mask_len = if let Some(ref m) = self.column_mapping {
                m.sources.len().max(num_cols)
            } else {
                num_cols
            };
            let mut mask = vec![false; mask_len];
            for &ci in &filter_cols {
                if ci < mask_len {
                    mask[ci] = true;
                }
            }
            for &ci in &self.project_cols {
                if ci < mask_len {
                    mask[ci] = true;
                }
            }
            self.needed_cols = Some(mask);
        } else {
            // Cannot determine filter columns — materialize all columns
            // so the filter evaluates against real data, not Null.
            self.needed_cols = None;
        }

        // Pre-compute row group skip decisions from per-group zone maps.
        // For each group, if ANY comparison's zone map says "no match",
        // the entire group can be skipped.
        if !self.volume.meta.row_groups.is_empty() && !comparisons.is_empty() {
            let skips: Vec<bool> = self
                .volume
                .meta
                .row_groups
                .iter()
                .map(|rg| {
                    for &(col_name, op, value) in &comparisons {
                        let col_idx = match self.volume.column_index(col_name) {
                            Some(idx) if idx < rg.zone_maps.len() => idx,
                            _ => continue,
                        };
                        let zm = &rg.zone_maps[col_idx];
                        let dominated = match op {
                            radixdb_core::Operator::Eq => !zm.may_contain_eq(value),
                            radixdb_core::Operator::Gt => !zm.may_contain_gt(value),
                            radixdb_core::Operator::Gte => !zm.may_contain_gte(value),
                            radixdb_core::Operator::Lt => !zm.may_contain_lt(value),
                            radixdb_core::Operator::Lte => !zm.may_contain_lte(value),
                            _ => false,
                        };
                        if dominated {
                            return true; // skip this group
                        }
                    }
                    false
                })
                .collect();
            // Only store if at least one group can be skipped
            if skips.iter().any(|&s| s) {
                self.row_group_skips = Some(skips);
            }
        }

        self.filter = Some(filter);
    }

    fn build_comparison_column_predicate(
        &self,
        col_name: &str,
        op: radixdb_core::Operator,
        value: &Value,
    ) -> Option<ColumnPredicate> {
        if !matches!(
            op,
            radixdb_core::Operator::Eq
                | radixdb_core::Operator::Ne
                | radixdb_core::Operator::Gt
                | radixdb_core::Operator::Gte
                | radixdb_core::Operator::Lt
                | radixdb_core::Operator::Lte
        ) {
            return None;
        }

        let col_idx = self.volume.column_index(col_name)?;
        let col_dt = self.volume.columns.data_type(col_idx);
        let target = match (value, col_dt) {
            (Value::Integer(v), radixdb_core::DataType::Integer) => TypedTarget::Int64(*v),
            (Value::Float(v), radixdb_core::DataType::Float) => TypedTarget::Float64(*v),
            (Value::Text(v), radixdb_core::DataType::Text) => TypedTarget::Text(v.to_string()),
            (Value::Boolean(v), radixdb_core::DataType::Boolean)
                if matches!(op, radixdb_core::Operator::Eq | radixdb_core::Operator::Ne) =>
            {
                TypedTarget::Bool(*v)
            }
            (Value::Timestamp(dt), radixdb_core::DataType::Timestamp) => {
                TypedTarget::Int64(dt.timestamp_nanos_opt().unwrap_or_else(|| {
                    dt.timestamp()
                        .saturating_mul(1_000_000_000)
                        .saturating_add(dt.timestamp_subsec_nanos() as i64)
                }))
            }
            _ => return None,
        };

        Some(ColumnPredicate {
            col_idx,
            kind: ColumnPredicateKind::Comparison { op, target },
        })
    }

    /// Build an exact typed-column representation of the whole expression tree.
    ///
    /// Unlike the flat `typed_predicates` prefilter, this preserves boolean
    /// shape. That makes disjunctions safe: `(a = 1 OR b = 2)` remains `ANY`,
    /// not an accidental `AND` of two standalone predicates.
    fn build_exact_typed_filter(
        &self,
        expr: &dyn crate::expression::Expression,
    ) -> Option<ColumnPredicateExpr> {
        if let Some((col_name, op, value)) = expr.get_comparison_info() {
            return self
                .build_comparison_column_predicate(col_name, op, value)
                .map(ColumnPredicateExpr::Single);
        }

        if let Some((col_name, lower, upper, inclusive, not)) = expr.get_between_info() {
            let lower_op = if inclusive {
                radixdb_core::Operator::Gte
            } else {
                radixdb_core::Operator::Gt
            };
            let upper_op = if inclusive {
                radixdb_core::Operator::Lte
            } else {
                radixdb_core::Operator::Lt
            };
            if not {
                let below_op = if inclusive {
                    radixdb_core::Operator::Lt
                } else {
                    radixdb_core::Operator::Lte
                };
                let above_op = if inclusive {
                    radixdb_core::Operator::Gt
                } else {
                    radixdb_core::Operator::Gte
                };
                return Some(ColumnPredicateExpr::Any(vec![
                    ColumnPredicateExpr::Single(
                        self.build_comparison_column_predicate(col_name, below_op, lower)?,
                    ),
                    ColumnPredicateExpr::Single(
                        self.build_comparison_column_predicate(col_name, above_op, upper)?,
                    ),
                ]));
            }
            return Some(ColumnPredicateExpr::All(vec![
                ColumnPredicateExpr::Single(
                    self.build_comparison_column_predicate(col_name, lower_op, lower)?,
                ),
                ColumnPredicateExpr::Single(
                    self.build_comparison_column_predicate(col_name, upper_op, upper)?,
                ),
            ]));
        }

        if let Some((col_name, lower, upper, include_lower, include_upper)) = expr.get_range_info()
        {
            let lower_op = if include_lower {
                radixdb_core::Operator::Gte
            } else {
                radixdb_core::Operator::Gt
            };
            let upper_op = if include_upper {
                radixdb_core::Operator::Lte
            } else {
                radixdb_core::Operator::Lt
            };
            return Some(ColumnPredicateExpr::All(vec![
                ColumnPredicateExpr::Single(
                    self.build_comparison_column_predicate(col_name, lower_op, lower)?,
                ),
                ColumnPredicateExpr::Single(
                    self.build_comparison_column_predicate(col_name, upper_op, upper)?,
                ),
            ]));
        }

        if let Some((col_name, values, not, has_null)) = expr.get_in_list_info() {
            let col_idx = self.volume.column_index(col_name)?;
            let values = Self::build_typed_in_list(values, self.volume.columns.data_type(col_idx))?;
            return Some(ColumnPredicateExpr::Single(ColumnPredicate {
                col_idx,
                kind: ColumnPredicateKind::InList {
                    values,
                    not,
                    has_null,
                },
            }));
        }

        if let Some((col_name, is_null)) = expr.get_null_check_info() {
            let col_idx = self.volume.column_index(col_name)?;
            return Some(ColumnPredicateExpr::Single(ColumnPredicate {
                col_idx,
                kind: ColumnPredicateKind::NullCheck { is_null },
            }));
        }

        if let Some(children) = expr.get_and_operands() {
            return self.build_exact_typed_children(children, true);
        }

        if let Some(children) = expr.get_or_operands() {
            return self.build_exact_typed_children(children, false);
        }

        None
    }

    fn build_exact_typed_children(
        &self,
        children: &[Box<dyn crate::expression::Expression>],
        all: bool,
    ) -> Option<ColumnPredicateExpr> {
        if children.is_empty() {
            return None;
        }

        let mut typed_children = Vec::with_capacity(children.len());
        for child in children {
            typed_children.push(self.build_exact_typed_filter(child.as_ref())?);
        }

        Some(if all {
            ColumnPredicateExpr::All(typed_children)
        } else {
            ColumnPredicateExpr::Any(typed_children)
        })
    }

    fn build_typed_in_list(
        values: &[Value],
        data_type: radixdb_core::DataType,
    ) -> Option<TypedInList> {
        if crate::expression::in_list::has_cross_numeric_physical_variant(data_type, values) {
            return None;
        }
        match data_type {
            radixdb_core::DataType::Integer => {
                let mut set = radixdb_core::I64Set::new();
                for value in values {
                    if let Value::Integer(v) = value {
                        set.insert(*v);
                    }
                }
                Some(TypedInList::Int64(set))
            }
            radixdb_core::DataType::Float => {
                let mut typed_values = Vec::new();
                for value in values {
                    if let Value::Float(v) = value {
                        typed_values.push(*v);
                    }
                }
                Some(TypedInList::Float64(typed_values))
            }
            radixdb_core::DataType::Boolean => {
                let mut has_true = false;
                let mut has_false = false;
                for value in values {
                    match value {
                        Value::Boolean(true) => has_true = true,
                        Value::Boolean(false) => has_false = true,
                        _ => {}
                    }
                }
                Some(TypedInList::Bool {
                    has_true,
                    has_false,
                })
            }
            radixdb_core::DataType::Text => {
                let mut set = rustc_hash::FxHashSet::default();
                for value in values {
                    if let Value::Text(value) = value {
                        set.insert(value.to_string());
                    }
                }
                Some(TypedInList::Text(set))
            }
            radixdb_core::DataType::Timestamp => {
                let mut set = radixdb_core::I64Set::new();
                for value in values {
                    if let Value::Timestamp(dt) = value {
                        set.insert(dt.timestamp_nanos_opt().unwrap_or_else(|| {
                            dt.timestamp()
                                .saturating_mul(1_000_000_000)
                                .saturating_add(dt.timestamp_subsec_nanos() as i64)
                        }));
                    }
                }
                Some(TypedInList::Int64(set))
            }
            _ => None,
        }
    }

    #[inline]
    fn evaluate_column_predicate(&self, pred: &ColumnPredicate, idx: usize, exact: bool) -> bool {
        let (col, local) = self.col_and_idx(pred.col_idx, idx);
        if col.is_null(local) {
            return match pred.kind {
                ColumnPredicateKind::Comparison { .. } => {
                    // In exact mode, SQL NULL comparisons are UNKNOWN/false for
                    // WHERE. In prefilter mode, keep NULL rows for the row-level
                    // filter because a surrounding expression may still decide.
                    !exact
                }
                ColumnPredicateKind::InList { .. } => {
                    // NULL IN (...) and NULL NOT IN (...) both produce
                    // UNKNOWN, treated as false by WHERE. This is also a safe
                    // rejection when an IN predicate is collected from an AND tree.
                    false
                }
                ColumnPredicateKind::NullCheck { is_null } => is_null,
                ColumnPredicateKind::LikePrefix { .. } => {
                    // NULL LIKE pattern is UNKNOWN/false in WHERE, and LIKE
                    // prefix predicates are only used as non-negated rejection
                    // prefilters.
                    false
                }
            };
        }

        match &pred.kind {
            ColumnPredicateKind::Comparison { op, target } => match target {
                TypedTarget::Int64(target) => {
                    let val = col.get_i64(local);
                    match op {
                        radixdb_core::Operator::Eq => val == *target,
                        radixdb_core::Operator::Ne => val != *target,
                        radixdb_core::Operator::Gt => val > *target,
                        radixdb_core::Operator::Gte => val >= *target,
                        radixdb_core::Operator::Lt => val < *target,
                        radixdb_core::Operator::Lte => val <= *target,
                        _ => true,
                    }
                }
                TypedTarget::Float64(target) => {
                    let val = col.get_f64(local);
                    Value::Float(val)
                        .compare(&Value::Float(*target))
                        .is_ok_and(|ordering| match op {
                            radixdb_core::Operator::Eq => ordering == std::cmp::Ordering::Equal,
                            radixdb_core::Operator::Ne => ordering != std::cmp::Ordering::Equal,
                            radixdb_core::Operator::Gt => ordering == std::cmp::Ordering::Greater,
                            radixdb_core::Operator::Gte => ordering != std::cmp::Ordering::Less,
                            radixdb_core::Operator::Lt => ordering == std::cmp::Ordering::Less,
                            radixdb_core::Operator::Lte => ordering != std::cmp::Ordering::Greater,
                            _ => true,
                        })
                }
                TypedTarget::Text(target) => {
                    let val = col.get_str(local);
                    match op {
                        radixdb_core::Operator::Eq => val == target,
                        radixdb_core::Operator::Ne => val != target,
                        radixdb_core::Operator::Gt => val > target.as_str(),
                        radixdb_core::Operator::Gte => val >= target.as_str(),
                        radixdb_core::Operator::Lt => val < target.as_str(),
                        radixdb_core::Operator::Lte => val <= target.as_str(),
                        _ => true,
                    }
                }
                TypedTarget::Bool(target) => {
                    let val = col.get_bool(local);
                    match op {
                        radixdb_core::Operator::Eq => val == *target,
                        radixdb_core::Operator::Ne => val != *target,
                        _ => true,
                    }
                }
            },
            ColumnPredicateKind::InList {
                values,
                not,
                has_null,
            } => {
                let found = match values {
                    TypedInList::Int64(set) => set.contains(col.get_i64(local)),
                    TypedInList::Float64(values) => {
                        let val = col.get_f64(local);
                        values
                            .iter()
                            .any(|candidate| Value::Float(val) == Value::Float(*candidate))
                    }
                    TypedInList::Bool {
                        has_true,
                        has_false,
                    } => {
                        if col.get_bool(local) {
                            *has_true
                        } else {
                            *has_false
                        }
                    }
                    TypedInList::Text(set) => set.contains(col.get_str(local)),
                };
                if found {
                    !*not
                } else if *has_null {
                    false
                } else {
                    *not
                }
            }
            ColumnPredicateKind::NullCheck { is_null } => !*is_null,
            ColumnPredicateKind::LikePrefix { prefix } => col.get_str(local).starts_with(prefix),
        }
    }

    /// Evaluate typed pre-filter predicates directly on column data.
    /// Returns false only if the row definitely does not match (safe rejection).
    /// NULL comparisons conservatively pass through (the full/exact filter handles NULL logic).
    #[inline]
    fn evaluate_typed_predicates(&self, idx: usize) -> bool {
        for pred in &self.typed_predicates {
            if !self.evaluate_column_predicate(pred, idx, false) {
                return false;
            }
        }
        true
    }

    #[inline]
    fn evaluate_exact_typed_filter_expr(&self, expr: &ColumnPredicateExpr, idx: usize) -> bool {
        match expr {
            ColumnPredicateExpr::Single(predicate) => {
                self.evaluate_column_predicate(predicate, idx, true)
            }
            ColumnPredicateExpr::All(children) => children
                .iter()
                .all(|child| self.evaluate_exact_typed_filter_expr(child, idx)),
            ColumnPredicateExpr::Any(children) => children
                .iter()
                .any(|child| self.evaluate_exact_typed_filter_expr(child, idx)),
        }
    }

    #[inline]
    fn evaluate_exact_typed_filter(&self, idx: usize) -> bool {
        self.exact_typed_filter
            .as_ref()
            .is_none_or(|expr| self.evaluate_exact_typed_filter_expr(expr, idx))
    }

    /// Bulk row-id boundary for the most common selective DML predicate.
    ///
    /// A zero-width `INTEGER = constant` scan needs only row identity. Walking
    /// the decoded i64 slices directly avoids one generic Scanner state machine
    /// transition per source row while preserving row-group pruning, mapping,
    /// tombstones and inter-volume visibility. Unsupported shapes return false
    /// before advancing any state and keep the established row fallback.
    fn collect_integer_equality_row_ids(&mut self, output: &mut Vec<i64>) -> Result<bool> {
        if self.has_current
            || self.row_iteration_started
            || !self.project_cols.is_empty()
            || !self.filter_covered_by_typed_predicates
            || self.volume.artifact_source().is_none()
        {
            return Ok(false);
        }
        let Some(ColumnPredicateExpr::Single(predicate)) = self.exact_typed_filter.as_ref() else {
            return Ok(false);
        };
        let ColumnPredicateKind::Comparison {
            op: radixdb_core::Operator::Eq,
            target: TypedTarget::Int64(target),
        } = &predicate.kind
        else {
            return Ok(false);
        };
        let column_idx = predicate.col_idx;
        let target = *target;
        let group_size = self.scan_row_group_size();

        while self.current_idx < self.end_idx {
            let group_idx = self.current_idx / group_size;
            let group_end = ((group_idx + 1) * group_size).min(self.end_idx);
            if self
                .row_group_skips
                .as_ref()
                .is_some_and(|skips| group_idx < skips.len() && skips[group_idx])
            {
                self.current_idx = group_end;
                continue;
            }

            self.load_group_cache(group_idx);
            if let Some(error) = self.error.as_ref() {
                return Err(error.clone());
            }
            let cache = self.group_cache.as_ref().ok_or_else(|| {
                Error::internal(format!(
                    "cold integer equality row-id scan did not load row group {}",
                    group_idx
                ))
            })?;
            let Some(super::column::ColumnData::Int64 { values, nulls }) =
                cache.columns.get(column_idx).and_then(Option::as_ref)
            else {
                return Err(Error::internal(format!(
                    "cold integer equality row-id scan did not decode integer column {}",
                    column_idx
                )));
            };
            let local_start = self.current_idx.saturating_sub(cache.group_start);
            let local_end = group_end
                .saturating_sub(cache.group_start)
                .min(values.len());
            if !self.has_row_skip_overlay {
                for local_idx in local_start..local_end {
                    if !nulls[local_idx] && values[local_idx] == target {
                        output.push(self.volume.row_id_at(cache.group_start + local_idx)?);
                    }
                }
            } else {
                // Visibility overlays are the cold exceptional path. Materialize
                // only matching ordinals so their immutable cache borrow ends
                // before `should_skip_row` advances the overlay state.
                let matching = (local_start..local_end)
                    .filter(|&local_idx| !nulls[local_idx] && values[local_idx] == target)
                    .map(|local_idx| cache.group_start + local_idx)
                    .collect::<Vec<_>>();
                for global_idx in matching {
                    if !self.should_skip_row(global_idx) {
                        output.push(self.volume.row_id_at(global_idx)?);
                    }
                }
            }
            self.current_idx = group_end;
        }

        self.has_current = false;
        self.row_iteration_started = true;
        self.group_cache = None;
        Ok(true)
    }

    /// Set a precomputed column mapping for schema-evolved volumes.
    /// Only stores it if the mapping is non-identity (avoids overhead
    /// when the volume matches the current schema).
    #[doc(hidden)]
    pub fn set_column_mapping(&mut self, mapping: super::writer::ColumnMapping) -> Result<()> {
        mapping.validate_for_volume(&self.volume)?;
        self.group_cache = None;
        self.next_group_boundary = 0;
        if !mapping.is_identity {
            self.column_mapping = Some(mapping);
        }
        Ok(())
    }

    #[inline(always)]
    fn should_use_group_cache(&self) -> bool {
        self.volume.artifact_source().is_some()
    }

    fn scan_row_group_index(&self, row_index: usize) -> Result<usize> {
        match self.volume.artifact_source() {
            Some(source) => source.row_group_for_row(row_index),
            None => Ok(row_index / self.scan_row_group_size()),
        }
    }

    fn scan_row_group_range(&self, row_group_index: usize) -> Result<std::ops::Range<usize>> {
        match self.volume.artifact_source() {
            Some(source) => source.row_group_range(row_group_index),
            None => {
                let group_size = self.scan_row_group_size();
                let start = row_group_index.saturating_mul(group_size);
                Ok(start
                    ..start
                        .saturating_add(group_size)
                        .min(self.volume.meta.row_count))
            }
        }
    }

    #[inline(always)]
    fn scan_row_group_size(&self) -> usize {
        super::column::ROW_GROUP_SIZE
    }

    /// Get (column_data, local_index) for a global row index.
    /// Uses group cache when available, falls back to full volume columns.
    #[inline(always)]
    fn col_and_idx(
        &self,
        col_idx: usize,
        global_idx: usize,
    ) -> (&super::column::ColumnData, usize) {
        if let Some(ref cache) = self.group_cache {
            if let Some(pair) = cache.col_and_local(col_idx, global_idx) {
                return pair;
            }
        }
        (&self.volume.columns[col_idx], global_idx)
    }

    /// Return physical volume columns needed for the current row-group cache.
    ///
    /// Without schema mapping, `needed_cols` is already a physical-column mask.
    /// With mapping, `needed_cols` is a logical schema-column mask and must be
    /// translated through `ColumnMapping::sources`; otherwise projection/filter
    /// scans of schema-evolved DATA artifacts would load every physical column.
    fn needed_physical_columns_for_group_cache(&self) -> Vec<usize> {
        let col_count = self.volume.columns.len();
        let mut selected = vec![false; col_count];

        if let Some(mapping) = self.column_mapping.as_ref() {
            for (schema_idx, source) in mapping.sources.iter().enumerate() {
                let needed = self
                    .needed_cols
                    .as_ref()
                    .is_none_or(|needed| schema_idx < needed.len() && needed[schema_idx]);
                if !needed {
                    continue;
                }
                if let super::writer::ColSource::Volume(phys_idx) = source {
                    if *phys_idx < selected.len() {
                        selected[*phys_idx] = true;
                    }
                }
            }
        } else {
            for (phys_idx, slot) in selected.iter_mut().enumerate() {
                *slot = self
                    .needed_cols
                    .as_ref()
                    .is_none_or(|needed| phys_idx < needed.len() && needed[phys_idx]);
            }
        }

        selected
            .into_iter()
            .enumerate()
            .filter_map(|(phys_idx, needed)| needed.then_some(phys_idx))
            .collect()
    }

    /// Whether the remaining scan can be represented exactly as immutable DATA
    /// typed row groups. This is a semantic guard, not a performance hint:
    /// any predicate, schema mapping or previous row-oriented advance keeps
    /// the established `Row` fallback. Constructor ranges are exact: full row
    /// groups are emitted as-is, while partial DATA row groups are sliced after
    /// typed decode. Row-id visibility overlays are applied as columnar
    /// selections, so tombstones and inter-volume visibility do not require
    /// row/value materialization.
    fn can_emit_typed_batches(&self) -> bool {
        self.error.is_none()
            && !self.has_current
            && !self.row_iteration_started
            && self.volume.artifact_source().is_some()
            && self.filter.is_none()
            && self.dict_filters.is_empty()
            && self.matching_indices.is_none()
            && self.typed_predicates.is_empty()
            && self.exact_typed_filter.is_none()
            && !self.filter_covered_by_typed_predicates
            && self.row_group_skips.is_none()
            && !self.project_cols.is_empty()
            && self
                .project_cols
                .iter()
                .enumerate()
                .all(|(index, column)| !self.project_cols[index + 1..].contains(column))
            && self.typed_batch_sources().is_ok()
            && self.current_idx <= self.end_idx
    }

    fn data_type_supports_typed_batch(data_type: DataType) -> bool {
        matches!(
            data_type,
            DataType::Integer
                | DataType::Float
                | DataType::Text
                | DataType::Boolean
                | DataType::Timestamp
                | DataType::Bytes
                | DataType::Json
        )
    }

    fn typed_batch_sources(
        &self,
    ) -> std::result::Result<Vec<TypedBatchSource>, TypedBatchFallbackReason> {
        if self.project_cols.is_empty() {
            return Err(TypedBatchFallbackReason::EmptyProjection);
        }
        let mut sources = Vec::with_capacity(self.project_cols.len());
        if let Some(mapping) = self.column_mapping.as_ref() {
            for &schema_idx in &self.project_cols {
                match mapping
                    .sources
                    .get(schema_idx)
                    .ok_or(TypedBatchFallbackReason::SchemaMappingMissingColumn)?
                {
                    super::writer::ColSource::Volume(phys_idx) => {
                        if !Self::data_type_supports_typed_batch(
                            self.volume.columns.data_type(*phys_idx),
                        ) {
                            return Err(TypedBatchFallbackReason::UnsupportedStorageType);
                        }
                        sources.push(TypedBatchSource::Volume(*phys_idx));
                    }
                    super::writer::ColSource::Default(value) => {
                        super::column::ColumnData::constant(value, 0)
                            .ok_or(TypedBatchFallbackReason::UnsupportedSchemaDefault)?;
                        sources.push(TypedBatchSource::Default(value.clone()));
                    }
                }
            }
        } else {
            for &phys_idx in &self.project_cols {
                if !Self::data_type_supports_typed_batch(self.volume.columns.data_type(phys_idx)) {
                    return Err(TypedBatchFallbackReason::UnsupportedStorageType);
                }
                sources.push(TypedBatchSource::Volume(phys_idx));
            }
        }
        Ok(sources)
    }

    fn typed_batch_fallback_reason_internal(&self) -> TypedBatchFallbackReason {
        if self.error.is_some() {
            return TypedBatchFallbackReason::PendingError;
        }
        if self.has_current {
            return TypedBatchFallbackReason::RowAlreadyFetched;
        }
        if self.row_iteration_started {
            return TypedBatchFallbackReason::RowIterationStarted;
        }
        if self.volume.artifact_source().is_none() {
            return TypedBatchFallbackReason::NotArtifactBacked;
        }
        if self.filter.is_some() {
            return TypedBatchFallbackReason::RowFilter;
        }
        if !self.dict_filters.is_empty() {
            return TypedBatchFallbackReason::DictionaryFilter;
        }
        if self.matching_indices.is_some() {
            return TypedBatchFallbackReason::IndexSelection;
        }
        if !self.typed_predicates.is_empty() {
            return TypedBatchFallbackReason::TypedPredicate;
        }
        if self.exact_typed_filter.is_some() {
            return TypedBatchFallbackReason::ExactTypedFilter;
        }
        if self.filter_covered_by_typed_predicates {
            return TypedBatchFallbackReason::FilterCoveredByTypedPredicates;
        }
        if self.row_group_skips.is_some() {
            return TypedBatchFallbackReason::RowGroupSkips;
        }
        if self.project_cols.is_empty() {
            return TypedBatchFallbackReason::EmptyProjection;
        }
        if self
            .project_cols
            .iter()
            .enumerate()
            .any(|(index, column)| self.project_cols[index + 1..].contains(column))
        {
            return TypedBatchFallbackReason::DuplicateProjection;
        }
        if let Err(reason) = self.typed_batch_sources() {
            return reason;
        }
        if self.current_idx > self.end_idx {
            return TypedBatchFallbackReason::InvalidRange;
        }
        TypedBatchFallbackReason::UnsupportedResultShape
    }

    /// Read one complete immutable DATA row group in output projection order.
    ///
    /// No `Row` or `Value` is constructed here. The caller owns the decoded
    /// column buffers and must preserve the normal scanner order when it
    /// writes them to a transport or a vectorized operator.
    fn next_artifact_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        if self.current_idx == self.end_idx {
            self.has_current = false;
            return Ok(None);
        }
        if !self.can_emit_typed_batches() {
            return Err(Error::internal(
                "typed DATA batch requested for a scanner with row-level semantics",
            ));
        }
        let batch_sources = self.typed_batch_sources().map_err(|reason| {
            Error::internal(format!(
                "typed DATA batch has no supported column sources: {}",
                reason.as_str()
            ))
        })?;
        let physical_columns: Vec<usize> = batch_sources
            .iter()
            .filter_map(|source| match source {
                TypedBatchSource::Volume(phys_idx) => Some(*phys_idx),
                TypedBatchSource::Default(_) => None,
            })
            .collect();

        let source = self
            .volume
            .artifact_source()
            .cloned()
            .ok_or_else(|| Error::internal("typed DATA scanner has no artifact source"))?;
        loop {
            if self.current_idx == self.end_idx {
                self.has_current = false;
                return Ok(None);
            }
            let group_idx = source.row_group_for_row(self.current_idx)?;
            let expected_range = source.row_group_range(group_idx)?;
            let expected_group_start = expected_range.start;
            let expected_group_end = expected_range.end;
            let requested_start = self.current_idx;
            let requested_end = expected_group_end.min(self.end_idx);
            let batch = source.read_columns(group_idx, &physical_columns)?;
            let row_range = batch.row_range();
            if batch.columns().len() != physical_columns.len()
                || batch
                    .columns()
                    .iter()
                    .zip(&physical_columns)
                    .any(|((actual, _), expected)| actual != expected)
            {
                return Err(Error::internal(
                    "typed DATA batch columns do not match scanner projection",
                ));
            }
            let decoded_volume_columns = batch
                .into_columns()
                .into_iter()
                .map(|(_, column)| column)
                .collect::<Vec<_>>();
            let decoded_row_count = row_range.end - row_range.start;
            let mut decoded_volume_columns = decoded_volume_columns.into_iter();
            let mut columns = Vec::with_capacity(batch_sources.len());
            for batch_source in &batch_sources {
                match batch_source {
                    TypedBatchSource::Volume(_) => {
                        let column = decoded_volume_columns.next().ok_or_else(|| {
                            Error::internal("typed DATA batch missing decoded physical column")
                        })?;
                        columns.push(column);
                    }
                    TypedBatchSource::Default(value) => {
                        let column = super::column::ColumnData::constant(value, decoded_row_count)
                            .ok_or_else(|| {
                                Error::internal(format!(
                                    "typed DATA batch cannot synthesize default column of type {}",
                                    value.data_type()
                                ))
                            })?;
                        columns.push(column);
                    }
                }
            }
            if decoded_volume_columns.next().is_some() {
                return Err(Error::internal(
                    "typed DATA batch decoded more physical columns than requested",
                ));
            }
            if row_range.start != expected_group_start || row_range.end != expected_group_end {
                return Err(Error::internal(format!(
                    "typed DATA batch range {}..{} does not match scanner row group {}..{}",
                    row_range.start, row_range.end, expected_group_start, expected_group_end
                )));
            }
            if requested_start < row_range.start || requested_end > row_range.end {
                return Err(Error::internal(format!(
                    "typed DATA requested range {}..{} is outside decoded row group {}..{}",
                    requested_start, requested_end, row_range.start, row_range.end
                )));
            }

            let selected_indices = if self.has_row_skip_overlay {
                Some(
                    (requested_start..requested_end)
                        .filter(|&idx| !self.should_skip_row(idx))
                        .map(|idx| idx - row_range.start)
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            };
            let (row_count, columns) = if let Some(indices) = selected_indices {
                let row_count = indices.len();
                (
                    row_count,
                    columns
                        .into_iter()
                        .map(|column| column.select_indices(&indices))
                        .collect(),
                )
            } else if requested_start == row_range.start && requested_end == row_range.end {
                (requested_end - requested_start, columns)
            } else {
                let local_start = requested_start - row_range.start;
                let local_end = requested_end - row_range.start;
                (
                    requested_end - requested_start,
                    columns
                        .into_iter()
                        .map(|column| column.slice_range(local_start, local_end))
                        .collect(),
                )
            };

            self.current_idx = requested_end;
            self.next_group_boundary = self.current_idx;
            self.group_cache = None;
            self.has_current = false;
            if row_count == 0 {
                continue;
            }
            return Ok(Some(TypedColumnBatch::new(row_count, columns)));
        }
    }

    /// Load group cache for a new row group. Decompresses only the columns
    /// needed for filtering and projection from the DATA artifact.
    fn load_group_cache(&mut self, group_idx: usize) {
        let col_count = self.volume.columns.len();
        self.group_cache = None;

        let mut columns: Vec<Option<super::column::ColumnData>> = vec![None; col_count];
        let needed_columns = self.needed_physical_columns_for_group_cache();

        if let Some(source) = self.volume.artifact_source().cloned() {
            match source.read_columns(group_idx, &needed_columns) {
                Ok(batch) => {
                    debug_assert_eq!(batch.row_group_index(), group_idx);
                    debug_assert_eq!(batch.columns().len(), needed_columns.len());
                    let batch_start = batch.row_range().start;
                    for (column_index, column) in batch.into_columns() {
                        if column_index < columns.len() {
                            columns[column_index] = Some(column);
                        }
                    }
                    self.group_cache = Some(GroupColumnCache {
                        group_idx,
                        columns,
                        group_start: batch_start,
                    });
                }
                Err(error) => self.error = Some(error),
            }
            return;
        }

        self.group_cache = None;
    }

    // =========================================================================
    // Shared helpers for both fast path (matching_indices) and slow path
    // (linear scan). Extracted to eliminate code duplication — a single source
    // of truth for skip checks and row materialization.
    // =========================================================================

    /// Check tombstones and pending deletes for a row index. Returns true if
    /// the row should be skipped.
    #[inline(always)]
    fn row_id_at(&mut self, idx: usize) -> Option<i64> {
        match self.volume.row_id_at(idx) {
            Ok(row_id) => Some(row_id),
            Err(error) => {
                self.error = Some(error);
                None
            }
        }
    }

    fn should_skip_row(&mut self, idx: usize) -> bool {
        if !self.has_row_skip_overlay {
            return false;
        }
        self.should_skip_row_with_overlay(idx)
    }

    /// Slow visibility path, reached only when this scanner has a real
    /// tombstone, hot/pending skip set, or inter-volume bitmap.
    #[cold]
    #[inline(never)]
    fn should_skip_row_with_overlay(&mut self, idx: usize) -> bool {
        // Check pre-computed inter-volume visibility bitmap first (O(1) bit check).
        // A clear bit means a newer volume owns this row_id — skip without materialization.
        if let Some(ref bm) = self.visibility_bitmap {
            let word_idx = idx >> 6;
            if word_idx < bm.len() && (bm[word_idx] >> (idx & 63)) & 1 == 0 {
                return true;
            }
        }
        let Some(rid) = self.row_id_at(idx) else {
            return true;
        };
        if let Some(ref ts) = self.committed_tombstones {
            if let Some(&commit_seq) = ts.get(&rid) {
                if self.snapshot_seq.is_none_or(|ss| commit_seq <= ss) {
                    return true;
                }
            }
        }
        if let Some(ref pending) = self.pending_cold_deletes {
            if pending.contains(&rid) {
                return true;
            }
        }
        false
    }

    /// Check dictionary pre-filters for a row index. Returns true if the row
    /// does NOT match (should be skipped). Only called when dict_filters is
    /// non-empty.
    #[inline(always)]
    fn dict_filters_reject(&self, idx: usize) -> bool {
        for &(col_idx, expected_id) in &self.dict_filters {
            let (col, local) = self.col_and_idx(col_idx, idx);
            if col.is_null(local) || col.get_dict_id(local) != expected_id {
                return true;
            }
        }
        false
    }

    /// Materialize a row at `idx`, evaluate the filter (if any), and write
    /// the result into `self.current_row`. Returns false if the filter
    /// rejects the row.
    #[inline(always)]
    fn materialize_row(&mut self, idx: usize) -> bool {
        if self.group_cache.is_some() {
            return self.materialize_row_from_cache(idx);
        }

        if let Some(ref filter) = self.filter {
            if self.filter_covered_by_typed_predicates {
                if let Some(ref mapping) = self.column_mapping {
                    if self.is_full_projection {
                        self.current_row = self.volume.get_row_mapped(idx, mapping);
                    } else {
                        self.current_row =
                            self.volume
                                .get_row_mapped_projected(idx, mapping, &self.project_cols);
                    }
                } else if self.is_full_projection {
                    self.current_row = self.volume.get_row(idx);
                } else {
                    self.current_row = self.volume.get_row_projected(idx, &self.project_cols);
                }
                return true;
            }

            let full_row = match (&self.needed_cols, &self.column_mapping) {
                (Some(mask), Some(mapping)) => {
                    self.volume.get_row_mapped_needed(idx, mapping, mask)
                }
                (Some(mask), None) => self.volume.get_row_needed(idx, mask),
                (None, Some(mapping)) => self.volume.get_row_mapped(idx, mapping),
                (None, None) => self.volume.get_row(idx),
            };
            match filter.evaluate(&full_row) {
                Ok(true) => {}
                Ok(false) => return false,
                Err(error) => {
                    self.error = Some(error);
                    return false;
                }
            }
            if self.is_full_projection {
                self.current_row = full_row;
            } else {
                self.current_row = Row::from_values(
                    self.project_cols
                        .iter()
                        .map(|&col| {
                            full_row
                                .get(col)
                                .cloned()
                                .unwrap_or(Value::Null(radixdb_core::DataType::Null))
                        })
                        .collect(),
                );
            }
        } else if let Some(ref mapping) = self.column_mapping {
            if self.is_full_projection {
                self.current_row = self.volume.get_row_mapped(idx, mapping);
            } else {
                self.current_row =
                    self.volume
                        .get_row_mapped_projected(idx, mapping, &self.project_cols);
            }
        } else if self.is_full_projection {
            self.current_row = self.volume.get_row(idx);
        } else {
            self.current_row = self.volume.get_row_projected(idx, &self.project_cols);
        }
        true
    }

    /// Build a row from the per-group column cache (avoids full-column decompression).
    fn materialize_row_from_cache(&mut self, idx: usize) -> bool {
        if self.filter.is_none() || self.filter_covered_by_typed_predicates {
            if let Some(ref mapping) = self.column_mapping {
                let values: Vec<Value> = if self.is_full_projection {
                    mapping
                        .sources
                        .iter()
                        .map(|src| match src {
                            super::writer::ColSource::Volume(vol_idx) => {
                                let (col, local) = self.col_and_idx(*vol_idx, idx);
                                col.get_value(local)
                            }
                            super::writer::ColSource::Default(value) => value.clone(),
                        })
                        .collect()
                } else {
                    self.project_cols
                        .iter()
                        .map(|&schema_idx| match &mapping.sources[schema_idx] {
                            super::writer::ColSource::Volume(vol_idx) => {
                                let (col, local) = self.col_and_idx(*vol_idx, idx);
                                col.get_value(local)
                            }
                            super::writer::ColSource::Default(value) => value.clone(),
                        })
                        .collect()
                };
                self.current_row = Row::from_values(values);
            } else if self.is_full_projection {
                let col_count = self.volume.columns.len();
                self.current_row = Row::from_values(
                    (0..col_count)
                        .map(|ci| {
                            let (col, local) = self.col_and_idx(ci, idx);
                            col.get_value(local)
                        })
                        .collect(),
                );
            } else {
                self.current_row = Row::from_values(
                    self.project_cols
                        .iter()
                        .map(|&ci| {
                            let (col, local) = self.col_and_idx(ci, idx);
                            col.get_value(local)
                        })
                        .collect(),
                );
            }
            crate::instrumentation::record_row_materialization_count(
                1,
                self.current_row.len() as u64,
            );
            return true;
        }

        let full_row = if let Some(ref mapping) = self.column_mapping {
            let values: Vec<Value> = mapping
                .sources
                .iter()
                .enumerate()
                .map(|(schema_idx, src)| {
                    let is_needed = self
                        .needed_cols
                        .as_ref()
                        .is_none_or(|needed| schema_idx < needed.len() && needed[schema_idx]);
                    match src {
                        super::writer::ColSource::Volume(vol_idx) if is_needed => {
                            let (col, local) = self.col_and_idx(*vol_idx, idx);
                            col.get_value(local)
                        }
                        super::writer::ColSource::Volume(vol_idx) => {
                            Value::Null(self.volume.columns.data_type(*vol_idx))
                        }
                        super::writer::ColSource::Default(value) if is_needed => value.clone(),
                        super::writer::ColSource::Default(value) => Value::Null(value.data_type()),
                    }
                })
                .collect();
            Row::from_values(values)
        } else {
            let col_count = self.volume.columns.len();
            let values: Vec<Value> = (0..col_count)
                .map(|ci| {
                    let is_needed = self
                        .needed_cols
                        .as_ref()
                        .is_none_or(|needed| ci < needed.len() && needed[ci]);
                    if is_needed {
                        let (col, local) = self.col_and_idx(ci, idx);
                        col.get_value(local)
                    } else {
                        Value::Null(self.volume.columns.data_type(ci))
                    }
                })
                .collect();
            Row::from_values(values)
        };
        crate::instrumentation::record_row_materialization_count(1, full_row.len() as u64);

        // Apply filter if present
        if let Some(ref filter) = self.filter {
            match filter.evaluate(&full_row) {
                Ok(true) => {}
                Ok(false) => return false,
                Err(error) => {
                    self.error = Some(error);
                    return false;
                }
            }
        }

        // Project
        if self.is_full_projection {
            self.current_row = full_row;
        } else {
            self.current_row = Row::from_values(
                self.project_cols
                    .iter()
                    .map(|&col| {
                        full_row
                            .get(col)
                            .cloned()
                            .unwrap_or(Value::Null(radixdb_core::DataType::Null))
                    })
                    .collect(),
            );
        }
        true
    }
}

impl Scanner for VolumeScanner {
    fn next(&mut self) -> bool {
        if self.closed {
            self.has_current = false;
            return false;
        }
        if self.error.is_some() {
            self.has_current = false;
            return false;
        }

        // Fast path: use pre-computed matching indices (from dictionary filters).
        let use_group_cache_fast = self.should_use_group_cache();
        if self.matching_indices.is_some() {
            loop {
                let idx = match self.matching_indices.as_ref() {
                    Some(indices) if self.match_idx < indices.len() => {
                        let i = indices[self.match_idx];
                        self.match_idx += 1;
                        i
                    }
                    _ => {
                        self.has_current = false;
                        return false;
                    }
                };

                if self.should_skip_row(idx) {
                    continue;
                }

                // Load group cache on group transition (matching_indices are sorted)
                if use_group_cache_fast {
                    let gi = match self.scan_row_group_index(idx) {
                        Ok(group) => group,
                        Err(error) => {
                            self.error = Some(error);
                            self.has_current = false;
                            return false;
                        }
                    };
                    let need_load = self.group_cache.as_ref().is_none_or(|c| c.group_idx != gi);
                    if need_load {
                        self.load_group_cache(gi);
                        if self.error.is_some() {
                            self.has_current = false;
                            return false;
                        }
                    }
                }

                if !self.typed_predicates.is_empty() && !self.evaluate_typed_predicates(idx) {
                    continue;
                }
                if self.filter_covered_by_typed_predicates && !self.evaluate_exact_typed_filter(idx)
                {
                    continue;
                }
                if !self.materialize_row(idx) {
                    if self.error.is_some() {
                        self.has_current = false;
                        return false;
                    }
                    continue;
                }

                let Some(row_id) = self.row_id_at(idx) else {
                    self.has_current = false;
                    return false;
                };
                self.current_rid = row_id;
                self.has_current = true;
                self.row_iteration_started = true;
                return true;
            }
        }

        // Slow path: linear scan with row-group skipping + per-group decompression
        let use_group_cache = self.should_use_group_cache();
        while self.current_idx < self.end_idx {
            // Row-group boundary: skip pruned groups + load group cache
            if self.current_idx >= self.next_group_boundary {
                let group_idx = match self.scan_row_group_index(self.current_idx) {
                    Ok(group) => group,
                    Err(error) => {
                        self.error = Some(error);
                        self.has_current = false;
                        return false;
                    }
                };
                let group_range = match self.scan_row_group_range(group_idx) {
                    Ok(range) => range,
                    Err(error) => {
                        self.error = Some(error);
                        self.has_current = false;
                        return false;
                    }
                };
                self.next_group_boundary = group_range.end.min(self.end_idx);

                // Zone map skip
                if let Some(ref skips) = self.row_group_skips {
                    if group_idx < skips.len() && skips[group_idx] {
                        self.current_idx = self.next_group_boundary;
                        continue;
                    }
                }

                // A restored index may already cover an entire cold group.
                // Check metadata visibility before touching projected blocks so
                // recovery can reuse persisted HNSW graphs without vector I/O.
                if use_group_cache
                    && self.has_row_skip_overlay
                    && (self.current_idx..self.next_group_boundary)
                        .all(|index| self.should_skip_row(index))
                {
                    self.current_idx = self.next_group_boundary;
                    continue;
                }

                // Load per-group cache (compressed-store only)
                if use_group_cache {
                    self.load_group_cache(group_idx);
                    if self.error.is_some() {
                        self.has_current = false;
                        return false;
                    }
                }
            }

            if self.should_skip_row(self.current_idx) {
                self.current_idx += 1;
                continue;
            }

            let idx = self.current_idx;

            if !self.dict_filters.is_empty() && self.dict_filters_reject(idx) {
                self.current_idx += 1;
                continue;
            }
            if !self.typed_predicates.is_empty() && !self.evaluate_typed_predicates(idx) {
                self.current_idx += 1;
                continue;
            }
            if self.filter_covered_by_typed_predicates && !self.evaluate_exact_typed_filter(idx) {
                self.current_idx += 1;
                continue;
            }
            if !self.materialize_row(idx) {
                if self.error.is_some() {
                    self.has_current = false;
                    return false;
                }
                self.current_idx += 1;
                continue;
            }

            let Some(row_id) = self.row_id_at(idx) else {
                self.has_current = false;
                return false;
            };
            self.current_rid = row_id;
            self.has_current = true;
            self.row_iteration_started = true;
            self.current_idx += 1;
            return true;
        }

        self.has_current = false;
        false
    }

    fn row(&self) -> &Row {
        &self.current_row
    }

    fn err(&self) -> Option<&Error> {
        self.error.as_ref()
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.has_current = false;
        self.current_row = Row::new();
        self.group_cache = None;
        self.matching_indices = None;
        self.dict_filters.clear();
        self.typed_predicates.clear();
        self.exact_typed_filter = None;
        self.filter = None;
        Ok(())
    }

    fn take_row(&mut self) -> Row {
        self.has_current = false;
        std::mem::take(&mut self.current_row)
    }

    fn take_row_with_id(&mut self) -> Result<(i64, Row)> {
        if !self.has_current {
            return Err(Error::internal(
                "row identity requested without a current row",
            ));
        }
        let rid = self.current_rid;
        self.has_current = false;
        Ok((rid, std::mem::take(&mut self.current_row)))
    }

    fn current_row_id(&self) -> Result<i64> {
        if !self.has_current {
            return Err(Error::internal(
                "row identity requested without a current row",
            ));
        }
        Ok(self.current_rid)
    }

    fn collect_remaining_row_ids(&mut self, output: &mut Vec<i64>) -> Result<bool> {
        if self.closed {
            return Ok(true);
        }
        self.collect_integer_equality_row_ids(output)
    }

    fn warmup(&mut self) {}

    fn supports_typed_batches(&self) -> bool {
        self.can_emit_typed_batches()
    }

    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        if self.can_emit_typed_batches() {
            None
        } else {
            Some(self.typed_batch_fallback_reason_internal())
        }
    }

    fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        if self.closed {
            return Ok(None);
        }
        self.next_artifact_typed_batch()
    }

    #[cfg(test)]
    fn is_warmed_for_test(&self) -> bool {
        false
    }

    fn estimated_count(&self) -> Option<usize> {
        Some(if self.closed {
            0
        } else {
            self.end_idx.saturating_sub(self.current_idx)
        })
    }
}
