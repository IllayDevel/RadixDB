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

//! Streaming hash join operator.
//!
//! This operator implements hash join with the following key optimizations:
//!
//! 1. **Streaming Probe Side**: Only the build side is materialized.
//!    The probe side streams through without full materialization.
//!
//! 2. **Pre-allocated Hash Table**: The hash table is sized upfront
//!    based on build side cardinality, avoiding resizing.
//!
//! 3. **Zero-Copy Output**: Uses CompositeRow to combine rows without
//!    cloning values until final materialization is needed.
//!
//! # Join Types
//!
//! - INNER: Only matching rows
//! - LEFT OUTER: All left rows, matched right or NULLs
//! - RIGHT OUTER: All right rows, matched left or NULLs
//! - FULL OUTER: All rows from both sides

use crate::context::check_current_query_cancelled;
use crate::expression::JoinFilter;
use crate::hash_table::{
    hash_keys_with, JoinHashState, JoinHashTable, ProbeCursor, DEFAULT_JOIN_HASH_STATE_MAX_BYTES,
};
use crate::operator::{ColumnInfo, ColumnSource, JoinProjection, Operator, RowRef};
use radixdb_core::value::NULL_VALUE;
use radixdb_core::CompactArc;
use radixdb_core::{Result, Row};

/// Pre-computed column names to avoid format! allocations in hot paths.
/// Covers most common cases (up to 32 columns).
const BUILD_COLUMN_NAMES: [&str; 32] = [
    "build_0", "build_1", "build_2", "build_3", "build_4", "build_5", "build_6", "build_7",
    "build_8", "build_9", "build_10", "build_11", "build_12", "build_13", "build_14", "build_15",
    "build_16", "build_17", "build_18", "build_19", "build_20", "build_21", "build_22", "build_23",
    "build_24", "build_25", "build_26", "build_27", "build_28", "build_29", "build_30", "build_31",
];

/// Get a build column name efficiently, using pre-computed names when possible.
#[inline]
fn get_build_column_name(i: usize) -> String {
    if i < BUILD_COLUMN_NAMES.len() {
        BUILD_COLUMN_NAMES[i].to_string()
    } else {
        format!("build_{}", i)
    }
}

#[inline]
fn verify_probe_build_key_equality(
    probe: &RowRef,
    build: &Row,
    probe_indices: &[usize],
    build_indices: &[usize],
) -> bool {
    debug_assert_eq!(probe_indices.len(), build_indices.len());

    probe_indices
        .iter()
        .zip(build_indices.iter())
        .all(|(&probe_idx, &build_idx)| {
            let (Some(probe_value), Some(build_value)) =
                (probe.get(probe_idx), build.get(build_idx))
            else {
                return false;
            };
            !probe_value.is_null() && !build_value.is_null() && probe_value == build_value
        })
}

/// Which side of the join to use as the build side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    /// Use left side as build (right as probe)
    Left,
    /// Use right side as build (left as probe)
    Right,
}

/// Type of join to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// INNER JOIN - only matching rows
    Inner,
    /// LEFT OUTER JOIN - all left rows
    Left,
    /// RIGHT OUTER JOIN - all right rows
    Right,
    /// FULL OUTER JOIN - all rows from both sides
    Full,
    /// CROSS JOIN - cartesian product
    Cross,
    /// SEMI JOIN - return left rows that have at least one match (for EXISTS)
    Semi,
    /// ANTI JOIN - return left rows that have NO matches (for NOT EXISTS)
    Anti,
}

impl JoinType {
    /// Parse join type from string (as used in parser AST).
    /// Optimized to avoid allocation - uses byte-level case-insensitive matching.
    pub fn parse(s: &str) -> Self {
        // Fast path: check first byte to avoid scanning entire string
        let bytes = s.as_bytes();
        for (i, &b) in bytes.iter().enumerate() {
            // Case-insensitive byte matching: ASCII letters | 32 lowercases.
            // Each arm is gated by a guard so clippy's collapsible_match is satisfied.
            match b | 32 {
                b'l' if i + 4 <= bytes.len()
                    && (bytes[i + 1] | 32) == b'e'
                    && (bytes[i + 2] | 32) == b'f'
                    && (bytes[i + 3] | 32) == b't' =>
                {
                    return JoinType::Left;
                }
                b'r' if i + 5 <= bytes.len()
                    && (bytes[i + 1] | 32) == b'i'
                    && (bytes[i + 2] | 32) == b'g'
                    && (bytes[i + 3] | 32) == b'h'
                    && (bytes[i + 4] | 32) == b't' =>
                {
                    return JoinType::Right;
                }
                b'f' if i + 4 <= bytes.len()
                    && (bytes[i + 1] | 32) == b'u'
                    && (bytes[i + 2] | 32) == b'l'
                    && (bytes[i + 3] | 32) == b'l' =>
                {
                    return JoinType::Full;
                }
                b'c' if i + 5 <= bytes.len()
                    && (bytes[i + 1] | 32) == b'r'
                    && (bytes[i + 2] | 32) == b'o'
                    && (bytes[i + 3] | 32) == b's'
                    && (bytes[i + 4] | 32) == b's' =>
                {
                    return JoinType::Cross;
                }
                b's' if i + 4 <= bytes.len()
                    && (bytes[i + 1] | 32) == b'e'
                    && (bytes[i + 2] | 32) == b'm'
                    && (bytes[i + 3] | 32) == b'i' =>
                {
                    return JoinType::Semi;
                }
                b'a' if i + 4 <= bytes.len()
                    && (bytes[i + 1] | 32) == b'n'
                    && (bytes[i + 2] | 32) == b't'
                    && (bytes[i + 3] | 32) == b'i' =>
                {
                    return JoinType::Anti;
                }
                _ => {}
            }
        }
        JoinType::Inner
    }

    /// Alias for parse() - used by parallel.rs for compatibility.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        Self::parse(s)
    }

    /// Check if this join needs unmatched probe rows (NULL-extended).
    /// Used by parallel hash join.
    pub fn needs_unmatched_probe(&self, swapped: bool) -> bool {
        match self {
            JoinType::Inner | JoinType::Cross | JoinType::Semi => false,
            JoinType::Anti => !swapped, // ANTI: unmatched probe rows (when not swapped)
            JoinType::Left => !swapped, // LEFT JOIN: unmatched left (probe when not swapped)
            JoinType::Right => swapped, // RIGHT JOIN: unmatched right (probe when swapped)
            JoinType::Full => true,     // FULL JOIN: always needs unmatched rows
        }
    }

    /// Check if this join needs unmatched build rows (NULL-extended).
    /// Used by parallel hash join.
    pub fn needs_unmatched_build(&self, swapped: bool) -> bool {
        match self {
            JoinType::Inner | JoinType::Cross | JoinType::Semi | JoinType::Anti => false,
            JoinType::Left => swapped, // LEFT JOIN: unmatched left (build when swapped)
            JoinType::Right => !swapped, // RIGHT JOIN: unmatched right (build when not swapped)
            JoinType::Full => true,    // FULL JOIN: always needs unmatched rows
        }
    }

    /// Check if this is a semi-join (EXISTS semantics).
    pub fn is_semi(&self) -> bool {
        matches!(self, JoinType::Semi)
    }

    /// Check if this is an anti-join (NOT EXISTS semantics).
    pub fn is_anti(&self) -> bool {
        matches!(self, JoinType::Anti)
    }
}

/// Streaming hash join operator.
///
/// The join proceeds in two phases:
///
/// 1. **Build Phase** (in `open()`):
///    - Materialize the build side (smaller side)
///    - Build hash table on join keys
///
/// 2. **Probe Phase** (in `next()`):
///    - Stream through probe side one row at a time
///    - Lookup matches in hash table
///    - Return combined rows
///
/// For OUTER joins, additional tracking is used to ensure unmatched
/// rows are returned with NULL padding.
pub struct HashJoinOperator {
    // Input operators
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,

    // Join configuration
    join_type: JoinType,
    build_side: JoinSide,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,
    residual_filters: Vec<JoinFilter>,

    // Build phase state (populated in open())
    // Uses CompactArc<Vec<Row>> to enable zero-copy sharing with CTE results.
    // When dropped, only decrements refcount (O(1)) instead of deallocating rows.
    build_rows: CompactArc<Vec<Row>>,
    hash_table: Option<std::sync::Arc<JoinHashTable>>,
    hash_state_max_bytes: usize,
    scan_fallback: bool,

    // Output schema
    schema: Vec<ColumnInfo>,
    left_col_count: usize,
    right_col_count: usize,
    projection: Option<JoinProjection>,
    projection_columns: Option<CompactArc<[ColumnSource]>>,

    // Probe phase state
    // Stores probe row directly - clones only when needed for 1:N scenarios
    current_probe_row: Option<RowRef>,
    current_probe_cursor: ProbeCursor,
    current_scan_idx: usize,
    pending_build_idx: Option<usize>,
    probe_had_match: bool,

    // For OUTER joins: track which build rows were matched
    build_matched: Vec<bool>,
    returning_unmatched_build: bool,
    unmatched_build_idx: usize,

    // For self-join optimization
    is_self_join: bool,
    self_join_probe_idx: usize,

    // Cached NULL rows for OUTER joins (avoid per-row allocation)
    cached_null_build: Option<Row>,
    cached_null_probe: Option<Row>,

    // State tracking
    opened: bool,
    probe_exhausted: bool,

    // Operator-local counters used by the bounded JOIN instrumentation owner.
    // They are read once after execution; no global atomic is touched per row.
    observed_probe_rows: u64,
    observed_candidate_rows: u64,
    observed_deferred_probe_rows: u64,
    observed_deferred_output_rows: u64,
    deferred_metrics_recorded: bool,
}

impl HashJoinOperator {
    /// Create a new hash join operator.
    ///
    /// # Arguments
    /// * `left` - Left input operator
    /// * `right` - Right input operator
    /// * `join_type` - Type of join (INNER, LEFT, RIGHT, FULL)
    /// * `left_key_indices` - Column indices for left join keys
    /// * `right_key_indices` - Column indices for right join keys
    /// * `build_side` - Which side to use as build (typically smaller)
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        join_type: JoinType,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
        build_side: JoinSide,
    ) -> Self {
        // Build schema
        // For Semi/Anti joins, only return left (probe) columns
        let mut schema = Vec::new();
        if join_type.is_semi() || join_type.is_anti() {
            schema.extend(left.schema().iter().cloned());
        } else {
            schema.extend(left.schema().iter().cloned());
            schema.extend(right.schema().iter().cloned());
        }

        let left_col_count = left.schema().len();
        let right_col_count = right.schema().len();

        Self {
            left,
            right,
            join_type,
            build_side,
            left_key_indices,
            right_key_indices,
            residual_filters: Vec::new(),
            build_rows: CompactArc::new(Vec::new()),
            hash_table: None,
            hash_state_max_bytes: DEFAULT_JOIN_HASH_STATE_MAX_BYTES,
            scan_fallback: false,
            schema,
            left_col_count,
            right_col_count,
            projection: None,
            projection_columns: None,
            current_probe_row: None,
            current_probe_cursor: ProbeCursor::default(),
            current_scan_idx: 0,
            pending_build_idx: None,
            probe_had_match: false,
            build_matched: Vec::new(),
            returning_unmatched_build: false,
            unmatched_build_idx: 0,
            is_self_join: false,
            self_join_probe_idx: 0,
            cached_null_build: None,
            cached_null_probe: None,
            opened: false,
            probe_exhausted: false,
            observed_probe_rows: 0,
            observed_candidate_rows: 0,
            observed_deferred_probe_rows: 0,
            observed_deferred_output_rows: 0,
            deferred_metrics_recorded: false,
        }
    }

    /// Create a hash join operator with pre-built hash table and rows.
    ///
    /// This avoids the build phase in `open()` since the hash table is already
    /// constructed. Used by streaming joins where hash table and bloom filter
    /// are built together in a single pass for efficiency.
    ///
    /// # Arguments
    /// * `probe` - Probe side operator (will be iterated during join)
    /// * `hash_state` - Exact build rows, key layout and their pre-built table
    /// * `join_type` - Type of join
    /// * `left_key_indices` - Key indices for the logical left side
    /// * `right_key_indices` - Key indices for the logical right side
    /// * `build_is_left` - Whether build side is left (for schema ordering)
    pub fn with_prebuilt(
        probe: Box<dyn Operator>,
        hash_state: JoinHashState,
        join_type: JoinType,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
        build_is_left: bool,
        build_col_count: usize,
    ) -> Result<Self> {
        let build_key_indices = if build_is_left {
            &left_key_indices
        } else {
            &right_key_indices
        };
        if !hash_state.matches(hash_state.build_rows(), build_key_indices) {
            return Err(radixdb_core::Error::internal(
                "pre-built join hash state key layout mismatch",
            ));
        }
        let build_rows = CompactArc::clone(hash_state.build_rows());
        let hash_table = std::sync::Arc::clone(hash_state.table());
        let probe_col_count = probe.schema().len();

        // Build schema based on build side position
        // Pre-allocate schema with known capacity
        let total_cols = build_col_count + probe_col_count;
        let mut schema = Vec::with_capacity(total_cols);
        let (left_col_count, right_col_count) = if build_is_left {
            // Build is left: [build_cols, probe_cols]
            for i in 0..build_col_count {
                schema.push(ColumnInfo::new(get_build_column_name(i)));
            }
            schema.extend(probe.schema().iter().cloned());
            (build_col_count, probe_col_count)
        } else {
            // Build is right: [probe_cols, build_cols]
            schema.extend(probe.schema().iter().cloned());
            for i in 0..build_col_count {
                schema.push(ColumnInfo::new(get_build_column_name(i)));
            }
            (probe_col_count, build_col_count)
        };

        let build_side = if build_is_left {
            JoinSide::Left
        } else {
            JoinSide::Right
        };

        // Track matched builds for OUTER joins
        let build_matched = if matches!(join_type, JoinType::Full)
            || (matches!(join_type, JoinType::Left) && build_is_left)
            || (matches!(join_type, JoinType::Right) && !build_is_left)
        {
            vec![false; build_rows.len()]
        } else {
            Vec::new()
        };

        // Store probe operator in the non-build side slot
        let (left, right) = if build_is_left {
            // Build is left, probe is right
            (
                Box::new(crate::operator::EmptyOperator::new()) as Box<dyn Operator>,
                probe,
            )
        } else {
            // Build is right, probe is left
            (
                probe,
                Box::new(crate::operator::EmptyOperator::new()) as Box<dyn Operator>,
            )
        };

        Ok(Self {
            left,
            right,
            join_type,
            build_side,
            left_key_indices,
            right_key_indices,
            residual_filters: Vec::new(),
            build_rows,
            hash_table: Some(hash_table),
            hash_state_max_bytes: DEFAULT_JOIN_HASH_STATE_MAX_BYTES,
            scan_fallback: false,
            schema,
            left_col_count,
            right_col_count,
            projection: None,
            projection_columns: None,
            current_probe_row: None,
            current_probe_cursor: ProbeCursor::default(),
            current_scan_idx: 0,
            pending_build_idx: None,
            probe_had_match: false,
            build_matched,
            returning_unmatched_build: false,
            unmatched_build_idx: 0,
            is_self_join: false,
            self_join_probe_idx: 0,
            cached_null_build: None,
            cached_null_probe: None,
            opened: false,
            probe_exhausted: false,
            observed_probe_rows: 0,
            observed_candidate_rows: 0,
            observed_deferred_probe_rows: 0,
            observed_deferred_output_rows: 0,
            deferred_metrics_recorded: false,
        })
    }

    /// Create an optimized self-join operator.
    ///
    /// For self-joins (t1 JOIN t1), this avoids scanning the table twice
    /// by reusing the same materialized data for both build and probe.
    pub fn self_join(
        input: Box<dyn Operator>,
        join_type: JoinType,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
    ) -> Self {
        // For self-join, schema is input schema duplicated
        let mut schema = Vec::new();
        schema.extend(input.schema().iter().cloned());
        schema.extend(input.schema().iter().cloned());

        let col_count = input.schema().len();

        // We'll use left as the input, right will be unused
        Self {
            left: input,
            right: Box::new(crate::operator::EmptyOperator::new()),
            join_type,
            build_side: JoinSide::Left, // Build from the single input
            left_key_indices,
            right_key_indices,
            residual_filters: Vec::new(),
            build_rows: CompactArc::new(Vec::new()),
            hash_table: None,
            hash_state_max_bytes: DEFAULT_JOIN_HASH_STATE_MAX_BYTES,
            scan_fallback: false,
            schema,
            left_col_count: col_count,
            right_col_count: col_count,
            projection: None,
            projection_columns: None,
            current_probe_row: None,
            current_probe_cursor: ProbeCursor::default(),
            current_scan_idx: 0,
            pending_build_idx: None,
            probe_had_match: false,
            build_matched: Vec::new(),
            returning_unmatched_build: false,
            unmatched_build_idx: 0,
            is_self_join: true,
            self_join_probe_idx: 0,
            cached_null_build: None,
            cached_null_probe: None,
            opened: false,
            probe_exhausted: false,
            observed_probe_rows: 0,
            observed_candidate_rows: 0,
            observed_deferred_probe_rows: 0,
            observed_deferred_output_rows: 0,
            deferred_metrics_recorded: false,
        }
    }

    pub(crate) fn observed_probe_rows(&self) -> u64 {
        self.observed_probe_rows
    }

    pub(crate) fn observed_candidate_rows(&self) -> u64 {
        self.observed_candidate_rows
    }

    pub(crate) fn used_scan_fallback(&self) -> bool {
        self.scan_fallback
    }

    pub(crate) fn with_hash_state_max_bytes(mut self, max_bytes: usize) -> Self {
        self.hash_state_max_bytes = max_bytes;
        self
    }

    fn publish_deferred_metrics(&mut self) {
        if self.deferred_metrics_recorded {
            return;
        }
        radixdb_storage::instrumentation::record_join_deferred_rows(
            self.observed_deferred_output_rows,
            self.observed_deferred_probe_rows,
        );
        self.deferred_metrics_recorded = true;
    }

    #[inline]
    fn observe_output(&mut self, row: RowRef) -> RowRef {
        if row.is_deferred() {
            self.observed_deferred_output_rows =
                self.observed_deferred_output_rows.saturating_add(1);
        }
        row
    }

    /// Set projection pushdown configuration.
    ///
    /// When set, the operator creates projected rows directly from the left/right
    /// sources instead of materializing a full combined join row and projecting it
    /// later. `ColumnSource::Outer` means the logical left side and
    /// `ColumnSource::Inner` means the logical right side.
    pub fn with_projection(
        mut self,
        columns: Vec<ColumnSource>,
        projected_schema: Vec<ColumnInfo>,
    ) -> Self {
        self.projection_columns = Some(CompactArc::from(columns.clone()));
        self.projection = Some(JoinProjection { columns });
        self.schema = projected_schema;
        self
    }

    /// Attach the non-equality part of `ON` to the hash match-state owner.
    /// Equality hash hits do not count as matches until every residual accepts
    /// the same virtual left/right pair.
    pub fn with_residual_filters(mut self, filters: Vec<JoinFilter>) -> Self {
        self.residual_filters = filters;
        self
    }

    #[inline]
    fn candidate_passes_residual(&self, probe_row: &RowRef, build_idx: usize) -> Result<bool> {
        if self.residual_filters.is_empty() {
            return Ok(true);
        }
        let build_row = RowRef::shared(CompactArc::clone(&self.build_rows), build_idx);
        let (left, right) = match self.build_side {
            JoinSide::Left => (&build_row, probe_row),
            JoinSide::Right => (probe_row, &build_row),
        };
        for filter in &self.residual_filters {
            if !filter.matches_row_refs_checked(left, right)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Get the key indices based on which side is build vs probe.
    fn build_key_indices(&self) -> &[usize] {
        match self.build_side {
            JoinSide::Left => &self.left_key_indices,
            JoinSide::Right => &self.right_key_indices,
        }
    }

    fn probe_key_indices(&self) -> &[usize] {
        match self.build_side {
            JoinSide::Left => &self.right_key_indices,
            JoinSide::Right => &self.left_key_indices,
        }
    }

    #[inline]
    fn project_left_right_refs(&self, left: RowRef, right: RowRef) -> RowRef {
        RowRef::projected(
            left,
            right,
            CompactArc::clone(
                self.projection_columns
                    .as_ref()
                    .expect("projection columns must exist when projection is enabled"),
            ),
        )
    }

    #[inline]
    fn project_probe_build_match(&self, probe_row: RowRef, build_idx: usize) -> RowRef {
        let build_row = RowRef::shared(CompactArc::clone(&self.build_rows), build_idx);
        match self.build_side {
            JoinSide::Left => self.project_left_right_refs(build_row, probe_row),
            JoinSide::Right => self.project_left_right_refs(probe_row, build_row),
        }
    }

    #[inline]
    fn project_probe_without_build(&self, probe_row: RowRef, null_build: Row) -> RowRef {
        let null_build = RowRef::owned(null_build);
        match self.build_side {
            JoinSide::Left => self.project_left_right_refs(null_build, probe_row),
            JoinSide::Right => self.project_left_right_refs(probe_row, null_build),
        }
    }

    #[inline]
    fn project_build_without_probe(&self, build_idx: usize, null_probe: Row) -> RowRef {
        let build_row = RowRef::shared(CompactArc::clone(&self.build_rows), build_idx);
        let null_probe = RowRef::owned(null_probe);
        match self.build_side {
            JoinSide::Left => self.project_left_right_refs(build_row, null_probe),
            JoinSide::Right => self.project_left_right_refs(null_probe, build_row),
        }
    }

    /// Get cached NULL row for the build side (used in OUTER joins).
    /// Caches the row on first call to avoid per-row allocation.
    #[inline]
    fn null_build_row(&mut self) -> Row {
        if let Some(ref row) = self.cached_null_build {
            return row.clone();
        }
        let count = match self.build_side {
            JoinSide::Left => self.left_col_count,
            JoinSide::Right => self.right_col_count,
        };
        let row = Row::from_values(vec![NULL_VALUE; count]);
        self.cached_null_build = Some(row.clone());
        row
    }

    /// Get cached NULL row for the probe side (used in OUTER joins).
    /// Caches the row on first call to avoid per-row allocation.
    #[inline]
    fn null_probe_row(&mut self) -> Row {
        if let Some(ref row) = self.cached_null_probe {
            return row.clone();
        }
        let count = match self.build_side {
            JoinSide::Left => self.right_col_count,
            JoinSide::Right => self.left_col_count,
        };
        let row = Row::from_values(vec![NULL_VALUE; count]);
        self.cached_null_probe = Some(row.clone());
        row
    }

    /// Combine probe row with a build row by index.
    /// Uses DirectBuildComposite to avoid cloning build rows - stores Arc reference instead.
    /// OPTIMIZATION: Uses DirectBuildComposite (no Arc allocation for probe row).
    #[inline]
    fn combine_rows_direct(&self, probe_row: RowRef, build_idx: usize) -> RowRef {
        if self.projection.is_some() {
            return self.project_probe_build_match(probe_row, build_idx);
        }

        // probe_is_left determines output column order:
        // - true: output = [probe, build]
        // - false: output = [build, probe]
        let probe_is_left = matches!(self.build_side, JoinSide::Right);
        RowRef::direct_build_composite(
            probe_row.into_owned(),
            CompactArc::clone(&self.build_rows),
            build_idx,
            probe_is_left,
        )
    }

    /// Combine probe and build rows into a RowRef without allocation.
    /// Uses CompositeRow to defer materialization until needed.
    /// Used for OUTER join unmatched rows where we need an actual null row.
    #[inline]
    fn combine_rows_ref(&self, probe_row: RowRef, build_row: Row) -> RowRef {
        if self.projection.is_some() {
            let build_row = RowRef::owned(build_row);
            return match self.build_side {
                JoinSide::Left => self.project_left_right_refs(build_row, probe_row),
                JoinSide::Right => self.project_left_right_refs(probe_row, build_row),
            };
        }

        let probe_row = probe_row.into_owned();

        match self.build_side {
            JoinSide::Left => {
                // Build is left, probe is right
                // Output: [build_row, probe_row] = [left, right]
                RowRef::Composite(crate::operator::CompositeRow::new(build_row, probe_row))
            }
            JoinSide::Right => {
                // Build is right, probe is left
                // Output: [probe_row, build_row] = [left, right]
                RowRef::Composite(crate::operator::CompositeRow::new(probe_row, build_row))
            }
        }
    }

    /// Get the next probe row (from probe operator or self-join iteration).
    fn next_probe_row(&mut self) -> Result<Option<RowRef>> {
        if self.is_self_join {
            // For self-join, iterate over the materialized build rows
            if self.self_join_probe_idx >= self.build_rows.len() {
                return Ok(None);
            }
            let row = self.build_rows[self.self_join_probe_idx].clone();
            self.self_join_probe_idx += 1;
            Ok(Some(RowRef::owned(row)))
        } else {
            // Normal case: get from probe operator
            let probe_op = match self.build_side {
                JoinSide::Left => &mut self.right,
                JoinSide::Right => &mut self.left,
            };

            probe_op.next()
        }
    }

    #[inline]
    fn next_build_candidate(&mut self) -> Option<usize> {
        if let Some(pending) = self.pending_build_idx.take() {
            return Some(pending);
        }
        if self.scan_fallback {
            if self.current_scan_idx >= self.build_rows.len() {
                return None;
            }
            let candidate = self.current_scan_idx;
            self.current_scan_idx += 1;
            Some(candidate)
        } else {
            self.hash_table
                .as_ref()
                .expect("opened hash join must own a table")
                .probe_next(&mut self.current_probe_cursor)
        }
    }

    #[inline]
    fn prefetch_build_candidate(&mut self) {
        self.pending_build_idx = if self.scan_fallback {
            if self.current_scan_idx < self.build_rows.len() {
                let candidate = self.current_scan_idx;
                self.current_scan_idx += 1;
                Some(candidate)
            } else {
                None
            }
        } else {
            self.hash_table
                .as_ref()
                .expect("opened hash join must own a table")
                .probe_next(&mut self.current_probe_cursor)
        };
    }
}

impl Operator for HashJoinOperator {
    fn open(&mut self) -> Result<()> {
        self.observed_probe_rows = 0;
        self.observed_candidate_rows = 0;
        self.observed_deferred_probe_rows = 0;
        self.observed_deferred_output_rows = 0;
        self.deferred_metrics_recorded = false;
        if let Some(projection) = &self.projection {
            projection.validate(self.left_col_count, self.right_col_count, self.schema.len())?;
        }
        // Check if hash table was pre-built (via with_prebuilt constructor)
        if self.hash_table.is_some() {
            // Pre-built case: only need to open the probe side
            // Build side is already materialized
            let probe_op = match self.build_side {
                JoinSide::Left => &mut self.right,
                JoinSide::Right => &mut self.left,
            };
            if let Err(error) = probe_op.open() {
                let _ = probe_op.close();
                return Err(error);
            }
            self.opened = true;
            return Ok(());
        }

        // Standard case: open both inputs and build hash table
        if let Err(error) = self.left.open() {
            let _ = self.left.close();
            return Err(error);
        }
        if !self.is_self_join {
            if let Err(error) = self.right.open() {
                let _ = self.right.close();
                let _ = self.left.close();
                return Err(error);
            }
        }

        let open_result = (|| {
            check_current_query_cancelled()?;

            // Materialize build side
            let build_op = match self.build_side {
                JoinSide::Left => &mut self.left,
                JoinSide::Right => &mut self.right,
            };

            // Collect all build rows
            let mut build_rows = Vec::new();
            while let Some(row_ref) = build_op.next()? {
                if build_rows.len() & 0xff == 0 {
                    check_current_query_cancelled()?;
                }
                build_rows.push(row_ref.into_owned());
            }

            // Admit the additional hash index before allocating it. The build
            // batch is still a valid bounded-scan relation when the index does
            // not fit, so correctness does not depend on allocator success.
            let build_key_indices = self.build_key_indices().to_vec();
            let hash_table =
                if JoinHashTable::fits_retained_budget(build_rows.len(), self.hash_state_max_bytes)
                {
                    Some(std::sync::Arc::new(JoinHashTable::build(
                        &build_rows,
                        &build_key_indices,
                    )))
                } else {
                    self.scan_fallback = true;
                    None
                };

            // Track which build rows match for OUTER joins that need unmatched BUILD rows:
            // - FULL: always need unmatched rows from both sides
            // - LEFT with build_side=Left: unmatched LEFT (build) rows need NULLs
            // - RIGHT with build_side=Right: unmatched RIGHT (build) rows need NULLs
            let needs_build_tracking = matches!(self.join_type, JoinType::Full)
                || (matches!(self.join_type, JoinType::Left) && self.build_side == JoinSide::Left)
                || (matches!(self.join_type, JoinType::Right)
                    && self.build_side == JoinSide::Right)
                || (self.is_self_join && !matches!(self.join_type, JoinType::Inner));
            if needs_build_tracking {
                self.build_matched = vec![false; build_rows.len()];
            }

            // Wrap in CompactArc for zero-copy drop (only refcount decrement, not deallocation)
            self.build_rows = CompactArc::new(build_rows);
            self.hash_table = hash_table;
            self.opened = true;

            Ok(())
        })();
        if let Err(error) = open_result {
            let _ = self.close();
            return Err(error);
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        check_current_query_cancelled()?;
        if !self.opened {
            return Err(radixdb_core::Error::internal(
                "HashJoinOperator::next called before open",
            ));
        }

        // If we're returning unmatched build rows (for FULL/RIGHT OUTER)
        if self.returning_unmatched_build {
            while self.unmatched_build_idx < self.build_rows.len() {
                if self.unmatched_build_idx & 0xff == 0 {
                    check_current_query_cancelled()?;
                }
                let idx = self.unmatched_build_idx;
                self.unmatched_build_idx += 1;

                if !self.build_matched[idx] {
                    if self.projection.is_some() {
                        let null_probe = self.null_probe_row();
                        let row = self.project_build_without_probe(idx, null_probe);
                        return Ok(Some(self.observe_output(row)));
                    }
                    let build_row = self.build_rows[idx].clone();
                    let null_probe = self.null_probe_row();
                    let row = self.combine_rows_ref(RowRef::owned(null_probe), build_row);
                    return Ok(Some(self.observe_output(row)));
                }
            }
            return Ok(None);
        }

        let mut scanned_probe_rows = 0_usize;
        loop {
            // Try to return the next match for the current probe row. The
            // cursor walks the bucket in-place; no per-probe Vec of candidate
            // row indices is allocated or retained.
            while self.current_probe_row.is_some() {
                if self.observed_candidate_rows & 0xff == 0 {
                    check_current_query_cancelled()?;
                }
                let Some(build_idx) = self.next_build_candidate() else {
                    break;
                };
                self.observed_candidate_rows = self.observed_candidate_rows.saturating_add(1);

                let build_row = &self.build_rows[build_idx];

                // Verify actual key equality (handle hash collisions)
                if verify_probe_build_key_equality(
                    self.current_probe_row.as_ref().unwrap(),
                    build_row,
                    self.probe_key_indices(),
                    self.build_key_indices(),
                ) {
                    if !self.candidate_passes_residual(
                        self.current_probe_row.as_ref().unwrap(),
                        build_idx,
                    )? {
                        continue;
                    }
                    self.probe_had_match = true;

                    // SEMI JOIN: Return probe row only (no build columns), then skip remaining matches
                    if self.join_type.is_semi() {
                        let probe_row = self.current_probe_row.take().unwrap();
                        self.pending_build_idx = None;
                        self.current_probe_cursor = ProbeCursor::default();
                        if self.projection.is_some() {
                            let row = self.project_probe_build_match(probe_row, build_idx);
                            return Ok(Some(self.observe_output(row)));
                        }
                        return Ok(Some(self.observe_output(probe_row)));
                    }

                    // ANTI JOIN: Found a match, so this probe row should NOT be returned
                    // Just skip remaining matches and move to next probe row
                    if self.join_type.is_anti() {
                        self.current_probe_row = None;
                        self.pending_build_idx = None;
                        self.current_probe_cursor = ProbeCursor::default();
                        continue;
                    }

                    // Mark build row as matched (for OUTER joins)
                    if !self.build_matched.is_empty() {
                        self.build_matched[build_idx] = true;
                    }

                    // Peek one hash candidate without allocating. Take probe
                    // ownership only when the bucket is exhausted; otherwise
                    // retain the candidate for the next call and clone the
                    // probe row for this 1:N output.
                    self.prefetch_build_candidate();
                    let probe_row = if self.pending_build_idx.is_none() {
                        // No more matches - take ownership (zero-copy)
                        self.current_probe_row.take().unwrap()
                    } else {
                        // More potential matches - clone probe row (rare 1:N case)
                        self.current_probe_row.as_ref().unwrap().clone()
                    };
                    let row = self.combine_rows_direct(probe_row, build_idx);
                    return Ok(Some(self.observe_output(row)));
                }
            }

            // Handle unmatched probe row
            // - ANTI JOIN: Return probe row when NO match found
            // - OUTER JOINs: Return probe row with NULL build columns
            if self.join_type.is_anti() && !self.probe_had_match {
                if let Some(probe_row) = self.current_probe_row.take() {
                    if self.projection.is_some() {
                        let null_build = self.null_build_row();
                        let row = self.project_probe_without_build(probe_row, null_build);
                        return Ok(Some(self.observe_output(row)));
                    }
                    return Ok(Some(self.observe_output(probe_row)));
                }
            }

            // Handle unmatched probe row for OUTER joins
            // Output unmatched probe rows only when probe side needs "all rows":
            // - FULL: all rows from both sides
            // - RIGHT with build_side=Left: probe=right, need all right rows
            // - LEFT with build_side=Right: probe=left, need all left rows
            let needs_unmatched_probe = matches!(self.join_type, JoinType::Full)
                || (matches!(self.join_type, JoinType::Right) && self.build_side == JoinSide::Left)
                || (matches!(self.join_type, JoinType::Left) && self.build_side == JoinSide::Right);

            if needs_unmatched_probe && !self.probe_had_match {
                if let Some(probe_row) = self.current_probe_row.take() {
                    if self.projection.is_some() {
                        let null_build = self.null_build_row();
                        let row = self.project_probe_without_build(probe_row, null_build);
                        return Ok(Some(self.observe_output(row)));
                    }
                    let null_build = self.null_build_row();
                    let row = self.combine_rows_ref(probe_row, null_build);
                    return Ok(Some(self.observe_output(row)));
                }
            }

            // Get next probe row (must be done before borrowing hash_table)
            if scanned_probe_rows & 0xff == 0 {
                check_current_query_cancelled()?;
            }
            let next_probe = self.next_probe_row()?;
            scanned_probe_rows = scanned_probe_rows.saturating_add(1);
            match next_probe {
                Some(probe_row) => {
                    self.observed_probe_rows = self.observed_probe_rows.saturating_add(1);
                    if probe_row.is_deferred() {
                        self.observed_deferred_probe_rows =
                            self.observed_deferred_probe_rows.saturating_add(1);
                    }
                    if self.scan_fallback {
                        self.current_scan_idx = 0;
                        self.current_probe_cursor = ProbeCursor::default();
                    } else {
                        // Compute the hash only for the admitted table path.
                        let probe_key_indices = self.probe_key_indices();
                        let hash = hash_keys_with(probe_key_indices, |idx| probe_row.get(idx));
                        self.current_probe_cursor = self
                            .hash_table
                            .as_ref()
                            .expect("opened hash join must own a table")
                            .probe_cursor(hash);
                    }
                    self.pending_build_idx = None;

                    // Store probe row directly (no Arc wrapping needed)
                    self.current_probe_row = Some(probe_row);
                    self.probe_had_match = false;
                }
                None => {
                    // Probe side exhausted
                    self.probe_exhausted = true;

                    // Return unmatched build rows for OUTER joins where build side
                    // corresponds to the "all rows" side of the join:
                    // - FULL: all rows from both sides
                    // - LEFT with build_side=Left: all left (build) rows
                    // - RIGHT with build_side=Right: all right (build) rows
                    if !self.build_matched.is_empty() {
                        self.returning_unmatched_build = true;
                        self.unmatched_build_idx = 0;
                        // Recursive call to handle unmatched build rows
                        return self.next();
                    }

                    return Ok(None);
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.publish_deferred_metrics();
        let left = self.left.close();
        let right = if self.is_self_join {
            Ok(())
        } else {
            self.right.close()
        };
        left.and(right)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Rough estimate: min of both sides (for INNER)
        // Could be refined with statistics
        let left_est = self.left.estimated_rows()?;
        let right_est = self.right.estimated_rows()?;

        Some(match self.join_type {
            JoinType::Inner => left_est.min(right_est),
            JoinType::Left => left_est,
            JoinType::Right => right_est,
            JoinType::Full => left_est + right_est,
            JoinType::Cross => left_est * right_est,
            JoinType::Semi => left_est.min(right_est), // At most all left rows
            JoinType::Anti => left_est,                // At most all left rows
        })
    }

    fn name(&self) -> &str {
        match self.join_type {
            JoinType::Inner => "HashJoin (INNER)",
            JoinType::Left => "HashJoin (LEFT)",
            JoinType::Right => "HashJoin (RIGHT)",
            JoinType::Full => "HashJoin (FULL)",
            JoinType::Cross => "HashJoin (CROSS)",
            JoinType::Semi => "HashJoin (SEMI)",
            JoinType::Anti => "HashJoin (ANTI)",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::MaterializedOperator;
    use radixdb_core::Value;
    use radixdb_storage::instrumentation;

    fn make_rows(data: Vec<Vec<i64>>) -> Vec<Row> {
        data.into_iter()
            .map(|vals| Row::from_values(vals.into_iter().map(Value::integer).collect()))
            .collect()
    }

    fn make_operator(data: Vec<Vec<i64>>, cols: Vec<&str>) -> Box<dyn Operator> {
        let rows = make_rows(data);
        let schema = cols.into_iter().map(ColumnInfo::new).collect();
        Box::new(MaterializedOperator::new(rows, schema))
    }

    fn collect_results(op: &mut dyn Operator) -> Result<Vec<Row>> {
        let mut results = Vec::new();
        op.open()?;
        while let Some(row_ref) = op.next()? {
            results.push(row_ref.into_owned());
        }
        op.close()?;
        Ok(results)
    }

    fn canonical_rows(rows: Vec<Row>) -> Vec<String> {
        let mut rows = rows
            .into_iter()
            .map(|row| format!("{row:?}"))
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    }

    fn execute_budget_case(
        join_type: JoinType,
        build_side: JoinSide,
        max_bytes: usize,
    ) -> (Vec<String>, bool) {
        let left = make_operator(
            vec![vec![1, 10], vec![1, 11], vec![2, 20], vec![4, 40]],
            vec!["id", "left_value"],
        );
        let right = make_operator(
            vec![vec![1, 100], vec![1, 101], vec![3, 300]],
            vec!["id", "right_value"],
        );
        let mut join = HashJoinOperator::new(left, right, join_type, vec![0], vec![0], build_side)
            .with_hash_state_max_bytes(max_bytes);
        let rows = collect_results(&mut join).unwrap();
        (canonical_rows(rows), join.used_scan_fallback())
    }

    #[test]
    fn bounded_scan_fallback_matches_hash_semantics_for_all_join_types() {
        let cases = [
            (JoinType::Inner, JoinSide::Right),
            (JoinType::Left, JoinSide::Right),
            (JoinType::Right, JoinSide::Left),
            (JoinType::Full, JoinSide::Right),
            (JoinType::Semi, JoinSide::Right),
            (JoinType::Anti, JoinSide::Right),
        ];

        for (join_type, build_side) in cases {
            let (hashed, hash_fallback) = execute_budget_case(join_type, build_side, usize::MAX);
            let (scanned, scan_fallback) = execute_budget_case(join_type, build_side, 0);
            assert!(!hash_fallback, "{join_type:?} unexpectedly used fallback");
            assert!(scan_fallback, "{join_type:?} did not use fallback");
            assert_eq!(scanned, hashed, "{join_type:?} fallback changed results");
        }
    }

    #[test]
    fn test_inner_join() {
        let left = make_operator(
            vec![vec![1, 10], vec![2, 20], vec![3, 30]],
            vec!["id", "value"],
        );
        let right = make_operator(vec![vec![1, 100], vec![3, 300]], vec!["id", "data"]);

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Inner,
            vec![0], // left key: id
            vec![0], // right key: id
            JoinSide::Right,
        );

        let results = collect_results(&mut join).unwrap();

        // Should have 2 matches: id=1 and id=3
        assert_eq!(results.len(), 2);

        // Verify first match (id=1)
        let row1 = &results[0];
        assert_eq!(row1.get(0), Some(&Value::integer(1)));
        assert_eq!(row1.get(1), Some(&Value::integer(10)));
        assert_eq!(row1.get(2), Some(&Value::integer(1)));
        assert_eq!(row1.get(3), Some(&Value::integer(100)));
    }

    #[test]
    fn test_inner_join_projection_materializes_selected_columns_only() {
        let left = make_operator(
            vec![vec![1, 10, 1000], vec![2, 20, 2000], vec![3, 30, 3000]],
            vec!["id", "value", "unused_left"],
        );
        let right = make_operator(
            vec![vec![1, 100, 9000], vec![3, 300, 7000]],
            vec!["id", "data", "unused_right"],
        );

        let projected_schema = vec![ColumnInfo::new("value"), ColumnInfo::new("data")];
        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Inner,
            vec![0],
            vec![0],
            JoinSide::Right,
        )
        .with_projection(
            vec![ColumnSource::Outer(1), ColumnSource::Inner(1)],
            projected_schema,
        );

        assert_eq!(join.schema().len(), 2);
        let results = collect_results(&mut join).unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|row| row.len() == 2));
        assert_eq!(results[0].get(0), Some(&Value::integer(10)));
        assert_eq!(results[0].get(1), Some(&Value::integer(100)));
        assert_eq!(results[1].get(0), Some(&Value::integer(30)));
        assert_eq!(results[1].get(1), Some(&Value::integer(300)));
    }

    #[test]
    fn projected_hash_chain_keeps_probe_rows_deferred_between_edges() {
        let first_left = make_operator(vec![vec![1, 10], vec![2, 20]], vec!["id", "payload"]);
        let first_right = make_operator(vec![vec![1, 100], vec![2, 200]], vec!["id", "dictionary"]);
        let first = HashJoinOperator::new(
            first_left,
            first_right,
            JoinType::Inner,
            vec![0],
            vec![0],
            JoinSide::Right,
        )
        .with_projection(
            vec![
                ColumnSource::Outer(0),
                ColumnSource::Outer(1),
                ColumnSource::Inner(1),
            ],
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("payload"),
                ColumnInfo::new("dictionary"),
            ],
        );

        let second_right = make_operator(vec![vec![1, 1000], vec![2, 2000]], vec!["id", "leaf"]);
        let mut second = HashJoinOperator::new(
            Box::new(first),
            second_right,
            JoinType::Inner,
            vec![0],
            vec![0],
            JoinSide::Right,
        )
        .with_projection(
            vec![
                ColumnSource::Outer(1),
                ColumnSource::Outer(2),
                ColumnSource::Inner(1),
            ],
            vec![
                ColumnInfo::new("payload"),
                ColumnInfo::new("dictionary"),
                ColumnInfo::new("leaf"),
            ],
        );

        instrumentation::begin_join_execution_probe();
        second.open().unwrap();
        let first_output = second.next().unwrap().unwrap();
        assert!(first_output.is_deferred());
        assert_eq!(first_output.get(0), Some(&Value::integer(10)));
        assert_eq!(first_output.get(1), Some(&Value::integer(100)));
        assert_eq!(first_output.get(2), Some(&Value::integer(1000)));
        assert_eq!(first_output.into_owned().len(), 3);

        let second_output = second.next().unwrap().unwrap();
        assert!(second_output.is_deferred());
        assert_eq!(second_output.get(0), Some(&Value::integer(20)));
        assert_eq!(second_output.get(1), Some(&Value::integer(200)));
        assert_eq!(second_output.get(2), Some(&Value::integer(2000)));
        assert!(second.next().unwrap().is_none());
        second.close().unwrap();

        let probe = instrumentation::end_join_execution_probe();
        assert_eq!(probe.deferred_rows, 4);
        assert_eq!(probe.deferred_rows_consumed, 2);
    }

    #[test]
    fn public_hash_join_rejects_invalid_projection_before_reading_rows() {
        let left = make_operator(vec![vec![1]], vec!["left_id"]);
        let right = make_operator(vec![vec![1]], vec!["right_id"]);
        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Inner,
            vec![0],
            vec![0],
            JoinSide::Right,
        )
        .with_projection(
            vec![ColumnSource::Inner(1)],
            vec![ColumnInfo::new("invalid")],
        );

        assert!(join.open().is_err());
    }

    #[test]
    fn test_left_join() {
        let left = make_operator(
            vec![vec![1, 10], vec![2, 20], vec![3, 30]],
            vec!["id", "value"],
        );
        let right = make_operator(vec![vec![1, 100]], vec!["id", "data"]);

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Left,
            vec![0],
            vec![0],
            JoinSide::Right,
        );

        let results = collect_results(&mut join).unwrap();

        // Should have 3 rows: id=1 matched, id=2 and id=3 with NULLs
        assert_eq!(results.len(), 3);

        // Check that id=2 has NULLs on right side
        let row2 = results
            .iter()
            .find(|r| r.get(0) == Some(&Value::integer(2)))
            .unwrap();
        assert!(row2.get(2).unwrap().is_null());
        assert!(row2.get(3).unwrap().is_null());
    }

    #[test]
    fn test_left_join_projection_uses_sparse_null_build_side() {
        let left = make_operator(
            vec![vec![1, 10], vec![2, 20], vec![3, 30]],
            vec!["id", "value"],
        );
        let right = make_operator(vec![vec![1, 100]], vec!["id", "data"]);

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Left,
            vec![0],
            vec![0],
            JoinSide::Right,
        )
        .with_projection(
            vec![ColumnSource::Outer(1), ColumnSource::Inner(1)],
            vec![ColumnInfo::new("value"), ColumnInfo::new("data")],
        );

        let results = collect_results(&mut join).unwrap();

        assert_eq!(results.len(), 3);
        let row2 = results
            .iter()
            .find(|row| row.get(0) == Some(&Value::integer(20)))
            .unwrap();
        assert_eq!(row2.len(), 2);
        assert!(row2.get(1).unwrap().is_null());
    }

    #[test]
    fn test_self_join() {
        let input = make_operator(
            vec![vec![1, 10], vec![2, 10], vec![3, 20]],
            vec!["id", "age"],
        );

        // Self-join on age (find pairs with same age)
        let mut join = HashJoinOperator::self_join(
            input,
            JoinType::Inner,
            vec![1], // left key: age
            vec![1], // right key: age
        );

        let results = collect_results(&mut join).unwrap();

        // id=1 and id=2 both have age=10, so we get:
        // (1,10) x (1,10), (1,10) x (2,10), (2,10) x (1,10), (2,10) x (2,10)
        // = 4 matches for age=10
        // id=3 has age=20, matches only itself = 1 match
        // Total = 5
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_empty_build() {
        let left = make_operator(vec![vec![1, 10], vec![2, 20]], vec!["id", "value"]);
        let right = make_operator(vec![], vec!["id", "data"]);

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Inner,
            vec![0],
            vec![0],
            JoinSide::Right,
        );

        let results = collect_results(&mut join).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_multi_key_join() {
        let left = make_operator(
            vec![vec![1, 10, 100], vec![1, 20, 200], vec![2, 10, 300]],
            vec!["a", "b", "val"],
        );
        let right = make_operator(
            vec![vec![1, 10, 1000], vec![1, 20, 2000]],
            vec!["a", "b", "data"],
        );

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Inner,
            vec![0, 1], // left keys: a, b
            vec![0, 1], // right keys: a, b
            JoinSide::Right,
        );

        let results = collect_results(&mut join).unwrap();

        // Should match (1,10) and (1,20)
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_semi_join() {
        // Left: users with id 1, 2, 3
        let left = make_operator(
            vec![vec![1, 100], vec![2, 200], vec![3, 300]],
            vec!["id", "value"],
        );
        // Right: orders for users 1 and 3 (user 1 has 2 orders)
        let right = make_operator(
            vec![vec![1, 10], vec![1, 20], vec![3, 30]],
            vec!["user_id", "order_id"],
        );

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Semi,
            vec![0], // left key: id
            vec![0], // right key: user_id
            JoinSide::Right,
        );

        let results = collect_results(&mut join).unwrap();

        // Semi join: return users who have at least one order
        // User 1 has 2 orders but should only appear once
        // User 2 has no orders - should NOT appear
        // User 3 has 1 order - should appear
        assert_eq!(results.len(), 2);

        // Schema should only have left columns
        assert_eq!(join.schema().len(), 2);

        // Verify we got users 1 and 3
        let ids: Vec<i64> = results
            .iter()
            .map(|r| r.get(0).unwrap().as_int64().unwrap())
            .collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
        assert!(!ids.contains(&2));
    }

    #[test]
    fn test_semi_join_projection_returns_requested_probe_columns_only() {
        let left = make_operator(
            vec![vec![1, 100, 1000], vec![2, 200, 2000], vec![3, 300, 3000]],
            vec!["id", "value", "unused_left"],
        );
        let right = make_operator(
            vec![vec![1, 10], vec![1, 20], vec![3, 30]],
            vec!["user_id", "order_id"],
        );

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Semi,
            vec![0],
            vec![0],
            JoinSide::Right,
        )
        .with_projection(vec![ColumnSource::Outer(1)], vec![ColumnInfo::new("value")]);

        let results = collect_results(&mut join).unwrap();

        assert_eq!(join.schema().len(), 1);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|row| row.len() == 1));
        let values: Vec<i64> = results
            .iter()
            .map(|row| row.get(0).unwrap().as_int64().unwrap())
            .collect();
        assert!(values.contains(&100));
        assert!(values.contains(&300));
        assert!(!values.contains(&200));
    }

    #[test]
    fn test_anti_join() {
        // Left: users with id 1, 2, 3
        let left = make_operator(
            vec![vec![1, 100], vec![2, 200], vec![3, 300]],
            vec!["id", "value"],
        );
        // Right: orders for users 1 and 3
        let right = make_operator(vec![vec![1, 10], vec![3, 30]], vec!["user_id", "order_id"]);

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Anti,
            vec![0], // left key: id
            vec![0], // right key: user_id
            JoinSide::Right,
        );

        let results = collect_results(&mut join).unwrap();

        // Anti join: return users who have NO orders
        // User 1 has orders - should NOT appear
        // User 2 has no orders - should appear
        // User 3 has orders - should NOT appear
        assert_eq!(results.len(), 1);

        // Schema should only have left columns
        assert_eq!(join.schema().len(), 2);

        // Verify we only got user 2
        let row = &results[0];
        assert_eq!(row.get(0), Some(&Value::integer(2)));
        assert_eq!(row.get(1), Some(&Value::integer(200)));
    }

    #[test]
    fn test_anti_join_projection_returns_requested_probe_columns_only() {
        let left = make_operator(
            vec![vec![1, 100, 1000], vec![2, 200, 2000], vec![3, 300, 3000]],
            vec!["id", "value", "unused_left"],
        );
        let right = make_operator(vec![vec![1, 10], vec![3, 30]], vec!["user_id", "order_id"]);

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Anti,
            vec![0],
            vec![0],
            JoinSide::Right,
        )
        .with_projection(vec![ColumnSource::Outer(1)], vec![ColumnInfo::new("value")]);

        let results = collect_results(&mut join).unwrap();

        assert_eq!(join.schema().len(), 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 1);
        assert_eq!(results[0].get(0), Some(&Value::integer(200)));
    }

    #[test]
    fn test_anti_join_empty_right() {
        // Left: users with id 1, 2, 3
        let left = make_operator(
            vec![vec![1, 100], vec![2, 200], vec![3, 300]],
            vec!["id", "value"],
        );
        // Right: no orders
        let right = make_operator(vec![], vec!["user_id", "order_id"]);

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Anti,
            vec![0],
            vec![0],
            JoinSide::Right,
        );

        let results = collect_results(&mut join).unwrap();

        // Anti join with empty right: all left rows should be returned
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_semi_join_empty_right() {
        // Left: users with id 1, 2, 3
        let left = make_operator(
            vec![vec![1, 100], vec![2, 200], vec![3, 300]],
            vec!["id", "value"],
        );
        // Right: no orders
        let right = make_operator(vec![], vec!["user_id", "order_id"]);

        let mut join = HashJoinOperator::new(
            left,
            right,
            JoinType::Semi,
            vec![0],
            vec![0],
            JoinSide::Right,
        );

        let results = collect_results(&mut join).unwrap();

        // Semi join with empty right: no left rows should be returned
        assert_eq!(results.len(), 0);
    }
}
