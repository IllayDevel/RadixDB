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

//! Volcano-style operator interface for streaming query execution.
//!
//! This module provides the foundation for a streaming execution model where
//! operators pull rows on-demand rather than materializing everything upfront.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐
//! │ Consumer     │ ← Pulls rows via next()
//! └──────┬───────┘
//!        │
//! ┌──────▼───────┐
//! │ Join Op      │ ← Build side materialized, probe side streamed
//! └──────┬───────┘
//!        │
//! ┌──────┴──────┐
//! │             │
//! ▼             ▼
//! ┌─────┐   ┌─────┐
//! │Scan │   │Scan │ ← Stream rows from storage
//! └─────┘   └─────┘
//! ```
//!
//! # Key Benefits
//!
//! 1. **Reduced Memory**: Only materialize what's needed (e.g., hash join build side)
//! 2. **Early Termination**: LIMIT can stop execution without processing all rows
//! 3. **Pipelining**: Multiple operators can work on the same row in sequence
//! 4. **Zero-Copy**: RowRef allows referencing rows without cloning

use std::fmt;

use radixdb_core::value::NULL_VALUE;
use radixdb_core::{CompactArc, CompactVec};
use radixdb_core::{Error, Result, Row, Value};
use radixdb_storage::{DeferredColumnSource, DeferredRow};

/// Column information for operator schema.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Original table alias (if from a table)
    pub table_alias: Option<String>,
}

/// Physical ordering guaranteed by an operator.
///
/// This is a certificate produced by the physical plan, not a runtime guess
/// from inspecting materialized rows.  Merge consumers may use only the
/// leading key prefix and NULL ordering they explicitly require.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OrderingProperty {
    /// The operator makes no ordering guarantee.
    #[default]
    Unknown,
    /// Rows are ascending on the listed key prefix, with NULL values last.
    AscendingNullsLast(Vec<usize>),
}

impl OrderingProperty {
    /// Create an ascending, NULLS LAST ordering certificate.
    pub fn ascending_nulls_last(key_indices: Vec<usize>) -> Self {
        if key_indices.is_empty() {
            Self::Unknown
        } else {
            Self::AscendingNullsLast(key_indices)
        }
    }

    /// Return whether this certificate proves the ordering required by merge.
    pub fn proves_ascending_nulls_last(&self, required_keys: &[usize]) -> bool {
        if required_keys.is_empty() {
            return false;
        }
        match self {
            Self::AscendingNullsLast(keys) => keys.starts_with(required_keys),
            Self::Unknown => false,
        }
    }

    /// Preserve this certificate through a fused JOIN projection.
    ///
    /// Every certified outer key must remain in the projected row. Missing,
    /// reordered, or inner-sourced keys fail closed to Unknown.
    pub fn remap_outer_projection(&self, columns: &[ColumnSource]) -> Self {
        let Self::AscendingNullsLast(keys) = self else {
            return Self::Unknown;
        };
        let mut remapped = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(output) = columns
                .iter()
                .position(|source| matches!(source, ColumnSource::Outer(index) if index == key))
            else {
                return Self::Unknown;
            };
            remapped.push(output);
        }
        Self::ascending_nulls_last(remapped)
    }
}

impl ColumnInfo {
    /// Create a new column info with just a name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_alias: None,
        }
    }
}

/// Specifies which side of a binary join a projected column comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnSource {
    /// Column from the left/outer row at the given index.
    Outer(usize),
    /// Column from the right/inner row at the given index.
    Inner(usize),
}

/// Projection configuration for fused projection during join row creation.
#[derive(Debug, Clone)]
pub struct JoinProjection {
    /// Columns to extract, in SELECT order.
    pub columns: Vec<ColumnSource>,
}

impl JoinProjection {
    /// Validate a public projection against both logical input schemas before
    /// any row is read. Execution may then use indexed access without trusting
    /// an external caller to have run the internal planner.
    pub fn validate(
        &self,
        left_columns: usize,
        right_columns: usize,
        output_columns: usize,
    ) -> Result<()> {
        if self.columns.len() != output_columns {
            return Err(Error::invalid_argument(format!(
                "join projection has {} values but output schema has {} columns",
                self.columns.len(),
                output_columns
            )));
        }
        for source in &self.columns {
            let (side, index, width) = match source {
                ColumnSource::Outer(index) => ("left", *index, left_columns),
                ColumnSource::Inner(index) => ("right", *index, right_columns),
            };
            if index >= width {
                return Err(Error::invalid_argument(format!(
                    "join projection {side} column index {index} is outside width {width}"
                )));
            }
        }
        Ok(())
    }
}

/// Volcano-style iterator interface for query operators.
///
/// Each operator implements this trait to participate in the streaming
/// execution pipeline. The execution follows the open-next-close pattern:
///
/// 1. `open()` - Initialize the operator (called once)
/// 2. `next()` - Get the next row (called repeatedly until None)
/// 3. `close()` - Release resources (called once at end)
///
/// # Thread Safety
///
/// Operators are `Send` to allow execution on different threads,
/// but individual operators are not `Sync` - they maintain mutable state.
pub trait Operator: Send {
    /// Initialize the operator.
    ///
    /// Called once before the first `next()` call.
    /// This is where child operators should be opened and
    /// any one-time initialization should occur.
    fn open(&mut self) -> Result<()>;

    /// Get the next row from this operator.
    ///
    /// Returns:
    /// - `Ok(Some(row))` - A row is available
    /// - `Ok(None)` - No more rows (exhausted)
    /// - `Err(e)` - An error occurred
    ///
    /// After returning `None`, subsequent calls should continue to return `None`.
    fn next(&mut self) -> Result<Option<RowRef>>;

    /// Close the operator and release resources.
    ///
    /// Called once after all rows have been consumed or when
    /// execution is terminated early. Child operators should
    /// also be closed.
    fn close(&mut self) -> Result<()>;

    /// Get the schema (column information) for this operator's output.
    fn schema(&self) -> &[ColumnInfo];

    /// Get an estimate of the number of rows this operator will produce.
    ///
    /// Returns `None` if the estimate is not available.
    /// Used by the query planner for cost estimation.
    fn estimated_rows(&self) -> Option<usize> {
        None
    }

    /// Physical ordering guaranteed by this operator's output.
    ///
    /// Unknown is deliberately fail-closed: an executor must never discover
    /// ordering by rescanning the complete output merely to choose an
    /// algorithm.
    fn ordering(&self) -> OrderingProperty {
        OrderingProperty::Unknown
    }

    /// Get a descriptive name for this operator (for EXPLAIN).
    fn name(&self) -> &str;
}

/// A row reference that can be borrowed, owned, or composite.
///
/// This enum allows operators to return rows without always cloning:
/// - `Borrowed`: Reference to an existing row (zero-copy)
/// - `Owned`: An owned row (when materialization is needed)
/// - `Composite`: Virtual row combining left and right join sides
///
/// # Performance
///
/// The key optimization is that `Composite` allows hash joins to
/// return combined rows without actually copying values from both sides.
/// Values are only copied when the final result is materialized.
#[derive(Debug, Clone)]
pub enum RowRef {
    /// Owned row - the row data is owned by this RowRef.
    Owned(Row),

    /// Composite row - combines two rows without copying.
    /// Used by join operators to avoid materializing combined rows.
    Composite(CompositeRow),

    /// Direct build composite - combines owned probe row with Arc-referenced build rows.
    /// OPTIMIZATION: Avoids Arc allocation for probe row (saves 1 allocation per match).
    /// Used for hash joins where build rows are shared via Arc.
    DirectBuildComposite(DirectBuildCompositeRow),

    /// One row in an immutable shared build batch.
    ///
    /// This lets a projected JOIN output retain the build row by Arc + index
    /// instead of cloning every value from that row.
    Shared(SharedRow),

    /// Projected virtual row over an existing outer row and one joined row.
    ///
    /// Unlike an owned projected `Row`, this keeps the selected slots virtual
    /// across subsequent JOIN edges. Values are copied only when the final
    /// result consumer requests an owned row.
    Projected(ProjectedRow),

    /// Deferred row carried through an internal QueryResult boundary.
    Deferred(DeferredRow),
}

impl RowRef {
    /// Create an owned RowRef from a Row.
    #[inline]
    pub fn owned(row: Row) -> Self {
        RowRef::Owned(row)
    }

    /// Create a composite RowRef from left and right rows.
    #[inline]
    pub fn composite(left: Row, right: Row) -> Self {
        RowRef::Composite(CompositeRow::new(left, right))
    }

    /// Get the number of columns in this row.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            RowRef::Owned(row) => row.len(),
            RowRef::Composite(comp) => comp.len(),
            RowRef::DirectBuildComposite(direct) => direct.len(),
            RowRef::Shared(shared) => shared.len(),
            RowRef::Projected(projected) => projected.len(),
            RowRef::Deferred(deferred) => deferred.len(),
        }
    }

    /// Check if this row is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a value by index without cloning.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&Value> {
        match self {
            RowRef::Owned(row) => row.get(idx),
            RowRef::Composite(comp) => comp.get(idx),
            RowRef::DirectBuildComposite(direct) => direct.get(idx),
            RowRef::Shared(shared) => shared.get(idx),
            RowRef::Projected(projected) => projected.get(idx),
            RowRef::Deferred(deferred) => deferred.get(idx),
        }
    }

    /// Convert to an owned Row.
    ///
    /// For `Owned`, this is a no-op move.
    /// For `Composite` and `DirectBuildComposite`, this materializes the combined row.
    #[inline]
    pub fn into_owned(self) -> Row {
        match self {
            RowRef::Owned(row) => row,
            // Use materialize_owned to move values instead of cloning
            RowRef::Composite(comp) => comp.materialize_owned(),
            RowRef::DirectBuildComposite(direct) => direct.materialize_owned(),
            RowRef::Shared(shared) => shared.materialize_owned(),
            RowRef::Projected(projected) => projected.materialize_owned(),
            RowRef::Deferred(deferred) => deferred.into_owned(),
        }
    }

    /// Clone to an owned Row.
    ///
    /// Use `into_owned()` when possible to avoid cloning.
    pub fn to_owned(&self) -> Row {
        match self {
            RowRef::Owned(row) => row.clone(),
            RowRef::Composite(comp) => comp.materialize(),
            RowRef::DirectBuildComposite(direct) => direct.materialize(),
            RowRef::Shared(shared) => shared.materialize(),
            RowRef::Projected(projected) => projected.materialize(),
            RowRef::Deferred(deferred) => deferred.to_owned(),
        }
    }

    /// Get a reference to the underlying Row if this is Owned.
    #[inline]
    pub fn as_row(&self) -> Option<&Row> {
        match self {
            RowRef::Owned(row) => Some(row),
            RowRef::Shared(shared) => Some(shared.row()),
            RowRef::Composite(_)
            | RowRef::DirectBuildComposite(_)
            | RowRef::Projected(_)
            | RowRef::Deferred(_) => None,
        }
    }

    /// Create a direct-build composite RowRef.
    ///
    /// OPTIMIZATION: Avoids Arc allocation for probe row.
    /// Use this for 1:1 joins where probe row is owned and not shared.
    #[inline]
    pub fn direct_build_composite(
        probe: Row,
        build_rows: CompactArc<Vec<Row>>,
        build_idx: usize,
        probe_is_left: bool,
    ) -> Self {
        RowRef::DirectBuildComposite(DirectBuildCompositeRow::new(
            probe,
            build_rows,
            build_idx,
            probe_is_left,
        ))
    }

    /// Reference one row in an immutable shared batch.
    #[inline]
    pub fn shared(rows: CompactArc<Vec<Row>>, row_idx: usize) -> Self {
        RowRef::Shared(SharedRow::new(rows, row_idx))
    }

    /// Create a deferred projected row for a JOIN output.
    #[inline]
    pub fn projected(left: RowRef, right: RowRef, columns: CompactArc<[ColumnSource]>) -> Self {
        RowRef::Projected(ProjectedRow::new(left, right, columns))
    }

    /// Restore a compact row received from an internal QueryResult boundary.
    #[inline]
    pub fn deferred(row: DeferredRow) -> Self {
        RowRef::Deferred(row)
    }

    /// Convert into the portable representation used between recursive JOINs.
    pub fn into_deferred(self) -> DeferredRow {
        match self {
            RowRef::Owned(row) => DeferredRow::owned(row),
            RowRef::Shared(SharedRow { rows, row_idx }) => DeferredRow::shared(rows, row_idx),
            RowRef::Projected(ProjectedRow {
                left,
                right,
                columns,
            }) => {
                let columns = columns
                    .iter()
                    .map(|source| match source {
                        ColumnSource::Outer(index) => DeferredColumnSource::Left(*index),
                        ColumnSource::Inner(index) => DeferredColumnSource::Right(*index),
                    })
                    .collect::<Vec<_>>();
                DeferredRow::projected(
                    left.into_deferred(),
                    right.into_deferred(),
                    CompactArc::from(columns),
                )
            }
            // These legacy unprojected shapes move complete rows already. Keep
            // their established materialization behavior until JR-09 replaces
            // the binary JoinResult<RowVec> boundary altogether.
            RowRef::Composite(composite) => DeferredRow::owned(composite.materialize_owned()),
            RowRef::DirectBuildComposite(composite) => {
                DeferredRow::owned(composite.materialize_owned())
            }
            RowRef::Deferred(row) => row,
        }
    }

    /// Whether this row still carries a deferred JOIN representation.
    #[inline]
    pub fn is_deferred(&self) -> bool {
        match self {
            RowRef::Owned(_) => false,
            RowRef::Deferred(row) => row.is_deferred(),
            _ => true,
        }
    }

    /// Conservative size of the request-local row graph retained by this
    /// handle. Arc-backed immutable batches are owned and charged elsewhere;
    /// this method accounts only for the handle/graph added by a pull batch.
    pub fn estimated_retained_bytes(&self) -> usize {
        fn row_bytes(row: &Row) -> usize {
            row.iter().fold(std::mem::size_of::<Row>(), |total, value| {
                let payload = match value {
                    Value::Text(text) => text.len(),
                    Value::Extension(bytes) => bytes.len(),
                    _ => 0,
                };
                total
                    .saturating_add(std::mem::size_of::<Value>())
                    .saturating_add(payload)
            })
        }

        match self {
            Self::Owned(row) => row_bytes(row),
            Self::Shared(_) => std::mem::size_of::<Self>(),
            Self::Composite(row) => std::mem::size_of::<Self>()
                .saturating_add(row_bytes(&row.left))
                .saturating_add(row_bytes(&row.right)),
            Self::DirectBuildComposite(row) => {
                std::mem::size_of::<Self>().saturating_add(row_bytes(&row.probe))
            }
            Self::Projected(row) => std::mem::size_of::<Self>()
                .saturating_add(row.left.estimated_retained_bytes())
                .saturating_add(row.right.estimated_retained_bytes())
                .saturating_add(
                    row.columns
                        .len()
                        .saturating_mul(std::mem::size_of::<ColumnSource>()),
                ),
            Self::Deferred(row) => row.estimated_retained_bytes(),
        }
    }
}

/// Arc-backed reference to one row in a materialized batch.
#[derive(Debug, Clone)]
pub struct SharedRow {
    rows: CompactArc<Vec<Row>>,
    row_idx: usize,
}

impl SharedRow {
    #[inline]
    pub fn new(rows: CompactArc<Vec<Row>>, row_idx: usize) -> Self {
        assert!(
            row_idx < rows.len(),
            "shared row index outside immutable batch"
        );
        Self { rows, row_idx }
    }

    #[inline]
    fn row(&self) -> &Row {
        &self.rows[self.row_idx]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.row().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.row().is_empty()
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.row().get(idx)
    }

    #[inline]
    pub fn materialize(&self) -> Row {
        self.row().clone()
    }

    #[inline]
    pub fn materialize_owned(self) -> Row {
        self.row().clone()
    }
}

/// A projected row whose slots still reference the preceding JOIN edge.
///
/// The outer side may itself be projected, so a long selective JOIN chain is
/// represented as a shallow sequence of slot maps instead of repeatedly
/// copying every retained payload value into a new `Row` at each edge.
#[derive(Debug, Clone)]
pub struct ProjectedRow {
    left: Box<RowRef>,
    right: Box<RowRef>,
    columns: CompactArc<[ColumnSource]>,
}

impl ProjectedRow {
    #[inline]
    pub fn new(left: RowRef, right: RowRef, columns: CompactArc<[ColumnSource]>) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            columns,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<&Value> {
        match self.columns.get(idx)? {
            ColumnSource::Outer(index) => self.left.get(*index),
            ColumnSource::Inner(index) => self.right.get(*index),
        }
    }

    pub fn materialize(&self) -> Row {
        let mut values = CompactVec::with_capacity(self.columns.len());
        for index in 0..self.columns.len() {
            values.push(self.get(index).cloned().unwrap_or(NULL_VALUE));
        }
        Row::from_compact_vec(values)
    }

    #[inline]
    pub fn materialize_owned(self) -> Row {
        // Projection may reorder or repeat slots, so consuming the source rows
        // cannot generally move their values. Materialize once at the final
        // ownership boundary instead of once per JOIN edge.
        self.materialize()
    }
}

impl fmt::Display for ProjectedRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for index in 0..self.len() {
            if index > 0 {
                write!(f, ", ")?;
            }
            match self.get(index) {
                Some(value) => write!(f, "{value}")?,
                None => write!(f, "NULL")?,
            }
        }
        write!(f, ")")
    }
}

/// A composite row that references values from two source rows.
///
/// This is the key optimization for joins - instead of cloning all values
/// from both the left and right rows into a new row, we keep references
/// to both and provide a unified view.
///
/// # Memory Layout
///
/// ```text
/// CompositeRow
/// ├── left: Row (owned)
/// ├── right: Row (owned)
/// └── left_cols: usize
///
/// Logical columns: [left_col_0, left_col_1, ..., right_col_0, right_col_1, ...]
///                  |<--- left_cols --->|<--- right cols --->|
/// ```
#[derive(Debug, Clone)]
pub struct CompositeRow {
    /// Left side of the join (probe row in hash join)
    left: Row,
    /// Right side of the join (build row in hash join)
    right: Row,
    /// Number of columns from the left side
    left_cols: usize,
}

impl CompositeRow {
    /// Create a new composite row from left and right parts.
    #[inline]
    pub fn new(left: Row, right: Row) -> Self {
        let left_cols = left.len();
        Self {
            left,
            right,
            left_cols,
        }
    }

    /// Get the total number of columns.
    #[inline]
    pub fn len(&self) -> usize {
        self.left_cols + self.right.len()
    }

    /// Check if this composite row is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty()
    }

    /// Get a value by index without cloning.
    ///
    /// Indexes 0..left_cols return from the left row.
    /// Indexes left_cols..total return from the right row.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&Value> {
        if idx < self.left_cols {
            self.left.get(idx)
        } else {
            self.right.get(idx - self.left_cols)
        }
    }

    /// Get a reference to the left row.
    #[inline]
    pub fn left(&self) -> &Row {
        &self.left
    }

    /// Get a reference to the right row.
    #[inline]
    pub fn right(&self) -> &Row {
        &self.right
    }

    /// Materialize into an owned Row (cloning version).
    ///
    /// This creates a single Row by copying all values from both sides.
    /// Only call this when the final result needs to be stored.
    /// Prefer `materialize_owned()` when you can consume the CompositeRow.
    pub fn materialize(&self) -> Row {
        let total = self.len();
        let mut values: CompactVec<Value> = CompactVec::with_capacity(total);

        // Copy left values using extend_clone for efficiency
        values.extend_clone(self.left.as_slice());

        // Copy right values using extend_clone for efficiency
        values.extend_clone(self.right.as_slice());

        Row::from_compact_vec(values)
    }

    /// Materialize into an owned Row by moving values (zero-copy).
    ///
    /// This consumes the CompositeRow and moves all values without cloning.
    /// Use this instead of `materialize()` when you no longer need the CompositeRow.
    #[inline]
    pub fn materialize_owned(self) -> Row {
        Row::from_combined_owned(self.left, self.right)
    }

    /// Decompose into the left and right rows.
    #[inline]
    pub fn into_parts(self) -> (Row, Row) {
        (self.left, self.right)
    }
}

impl fmt::Display for CompositeRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for i in 0..self.len() {
            if i > 0 {
                write!(f, ", ")?;
            }
            if let Some(v) = self.get(i) {
                write!(f, "{}", v)?;
            } else {
                write!(f, "NULL")?;
            }
        }
        write!(f, ")")
    }
}

/// A direct-build composite row that owns the probe row directly.
///
/// This stores the probe Row directly without Arc wrapping, saving one Arc
/// allocation per matched row. The build rows are shared via Arc for
/// efficient access.
#[derive(Debug)]
pub struct DirectBuildCompositeRow {
    /// Probe row (owned directly, no Arc overhead)
    probe: Row,
    /// Shared reference to build rows
    build_rows: CompactArc<Vec<Row>>,
    /// Index into build_rows
    build_idx: usize,
    /// Number of columns from the probe side
    probe_cols: usize,
    /// Whether probe is left side (true) or right side (false)
    probe_is_left: bool,
}

impl DirectBuildCompositeRow {
    /// Create a new direct-build composite row.
    #[inline]
    pub fn new(
        probe: Row,
        build_rows: CompactArc<Vec<Row>>,
        build_idx: usize,
        probe_is_left: bool,
    ) -> Self {
        debug_assert!(
            build_idx < build_rows.len(),
            "build_idx {} out of bounds (len={})",
            build_idx,
            build_rows.len()
        );
        let probe_cols = probe.len();
        Self {
            probe,
            build_rows,
            build_idx,
            probe_cols,
            probe_is_left,
        }
    }

    /// Get the total number of columns.
    #[inline]
    pub fn len(&self) -> usize {
        self.probe_cols + self.build_rows[self.build_idx].len()
    }

    /// Check if this row is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.probe.is_empty() && self.build_rows[self.build_idx].is_empty()
    }

    /// Get a value by index without cloning.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&Value> {
        let build_row = &self.build_rows[self.build_idx];
        if self.probe_is_left {
            // Output: [probe, build]
            if idx < self.probe_cols {
                self.probe.get(idx)
            } else {
                build_row.get(idx - self.probe_cols)
            }
        } else {
            // Output: [build, probe]
            let build_cols = build_row.len();
            if idx < build_cols {
                build_row.get(idx)
            } else {
                self.probe.get(idx - build_cols)
            }
        }
    }

    /// Materialize into an owned Row (cloning version).
    pub fn materialize(&self) -> Row {
        let build_row = &self.build_rows[self.build_idx];
        let total = self.probe_cols + build_row.len();
        let mut values: CompactVec<Value> = CompactVec::with_capacity(total);

        if self.probe_is_left {
            values.extend_clone(self.probe.as_slice());
            values.extend_clone(build_row.as_slice());
        } else {
            values.extend_clone(build_row.as_slice());
            values.extend_clone(self.probe.as_slice());
        }

        Row::from_compact_vec(values)
    }

    /// Materialize into an owned Row by moving probe values.
    ///
    /// Combines probe and build rows efficiently:
    /// - Moves probe values (owned) - uses extend_into_compact_vec to avoid Vec allocation
    /// - Clones build values (shared reference)
    #[inline]
    pub fn materialize_owned(self) -> Row {
        let build_row = &self.build_rows[self.build_idx];
        let total = self.probe_cols + build_row.len();

        let mut values: CompactVec<Value> = CompactVec::with_capacity(total);
        if self.probe_is_left {
            // Use extend_into_compact_vec to avoid intermediate Vec allocation
            self.probe.extend_into_compact_vec(&mut values);
            values.extend_clone(build_row.as_slice());
        } else {
            values.extend_clone(build_row.as_slice());
            // Use extend_into_compact_vec to avoid intermediate Vec allocation
            self.probe.extend_into_compact_vec(&mut values);
        }
        Row::from_compact_vec(values)
    }
}

impl Clone for DirectBuildCompositeRow {
    fn clone(&self) -> Self {
        Self {
            probe: self.probe.clone(),
            build_rows: CompactArc::clone(&self.build_rows),
            build_idx: self.build_idx,
            probe_cols: self.probe_cols,
            probe_is_left: self.probe_is_left,
        }
    }
}

impl fmt::Display for DirectBuildCompositeRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for i in 0..self.len() {
            if i > 0 {
                write!(f, ", ")?;
            }
            if let Some(v) = self.get(i) {
                write!(f, "{}", v)?;
            } else {
                write!(f, "NULL")?;
            }
        }
        write!(f, ")")
    }
}

// ============================================================================
// Helper Operators
// ============================================================================

/// An empty operator that produces no rows.
///
/// Useful as a placeholder or for empty result sets.
pub struct EmptyOperator {
    schema: Vec<ColumnInfo>,
    opened: bool,
}

impl EmptyOperator {
    /// Create an empty operator with no schema.
    pub fn new() -> Self {
        Self {
            schema: Vec::new(),
            opened: false,
        }
    }
}

impl Default for EmptyOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl Operator for EmptyOperator {
    fn open(&mut self) -> Result<()> {
        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn name(&self) -> &str {
        "Empty"
    }
}

/// An operator that yields rows from a pre-materialized vector.
///
/// This is useful for:
/// - Converting existing `Vec<Row>` results to the operator model
/// - CTEs that have been pre-computed
/// - Subquery results
pub struct MaterializedOperator {
    rows: Vec<Row>,
    schema: Vec<ColumnInfo>,
    ordering: OrderingProperty,
    current_idx: usize,
    opened: bool,
}

impl MaterializedOperator {
    /// Create an operator from a vector of rows.
    pub fn new(rows: Vec<Row>, schema: Vec<ColumnInfo>) -> Self {
        Self {
            rows,
            schema,
            ordering: OrderingProperty::Unknown,
            current_idx: 0,
            opened: false,
        }
    }

    /// Attach ordering already proven by the producing physical operator.
    ///
    /// This method does not inspect rows.  Callers must propagate a genuine
    /// physical certificate rather than infer one from current contents.
    pub fn with_ordering(mut self, ordering: OrderingProperty) -> Self {
        self.ordering = ordering;
        self
    }

    /// Create from a `CompactArc<Vec<Row>>`, unwrapping if sole owner or cloning if shared.
    /// This is optimal for CTE results which may have multiple references.
    pub fn from_arc(arc_rows: CompactArc<Vec<Row>>, schema: Vec<ColumnInfo>) -> Self {
        let rows = CompactArc::try_unwrap(arc_rows).unwrap_or_else(|arc| (*arc).clone());
        Self::new(rows, schema)
    }
}

impl Operator for MaterializedOperator {
    fn open(&mut self) -> Result<()> {
        self.current_idx = 0;
        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        if self.current_idx >= self.rows.len() {
            return Ok(None);
        }

        // Take ownership of the row, leaving an empty Row in its place.
        // This is O(1) instead of clone() which is O(n) for row width.
        // Safe because we only iterate forward and never revisit rows.
        let row = std::mem::take(&mut self.rows[self.current_idx]);
        self.current_idx += 1;
        Ok(Some(RowRef::Owned(row)))
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.rows.len())
    }

    fn ordering(&self) -> OrderingProperty {
        self.ordering.clone()
    }

    fn name(&self) -> &str {
        "Materialized"
    }
}

// ============================================================================
// QueryResult to Operator Adapter
// ============================================================================

use radixdb_storage::QueryResult as StorageQueryResult;

/// Operator that streams rows from a QueryResult.
///
/// This adapter allows existing QueryResult (from table scans, etc.)
/// to be used in the streaming operator pipeline. Unlike MaterializedOperator,
/// this does NOT load all rows upfront - it streams them on demand.
///
/// # Benefits
///
/// - **Memory efficient**: Only one row in memory at a time
/// - **Early termination**: LIMIT stops reading immediately
/// - **Streaming pipeline**: Fits into Volcano execution model
pub struct QueryResultOperator {
    result: Box<dyn StorageQueryResult>,
    schema: Vec<ColumnInfo>,
    ordering: OrderingProperty,
    opened: bool,
}

impl QueryResultOperator {
    /// Create a new streaming operator from a QueryResult.
    pub fn new(result: Box<dyn StorageQueryResult>, columns: Vec<String>) -> Self {
        let ordering = result.ascending_nulls_last_ordering().map_or(
            OrderingProperty::Unknown,
            OrderingProperty::ascending_nulls_last,
        );
        let schema = columns.into_iter().map(ColumnInfo::new).collect();
        Self {
            result,
            schema,
            ordering,
            opened: false,
        }
    }
}

impl Operator for QueryResultOperator {
    fn open(&mut self) -> Result<()> {
        radixdb_storage::instrumentation::record_join_source_open(self.opened);
        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        if !self.opened {
            return Ok(None);
        }

        if self.result.next() {
            // Preserve an internal deferred JOIN row when available. Ordinary
            // QueryResult implementations return an owned row through the
            // trait's compatibility default.
            Ok(Some(RowRef::deferred(self.result.take_deferred_row())))
        } else if let Some(error) = self.result.last_error() {
            // A scanner result signals I/O/filter failures after `next()` has
            // returned false. Operators must not translate that into a clean
            // end-of-stream (especially count-only paths, where it would yield
            // a plausible but wrong scalar result).
            Err(error)
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        self.result.close()
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        // QueryResult doesn't expose count, return None
        None
    }

    fn ordering(&self) -> OrderingProperty {
        self.ordering.clone()
    }

    fn name(&self) -> &str {
        "QueryResultScan"
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn join_projection_rejects_shape_and_side_indices_before_execution() {
        let wrong_width = JoinProjection {
            columns: vec![ColumnSource::Outer(0)],
        };
        assert!(wrong_width.validate(1, 1, 2).is_err());

        let bad_left = JoinProjection {
            columns: vec![ColumnSource::Outer(1)],
        };
        assert!(bad_left.validate(1, 1, 1).is_err());

        let bad_right = JoinProjection {
            columns: vec![ColumnSource::Inner(1)],
        };
        assert!(bad_right.validate(1, 1, 1).is_err());

        let valid = JoinProjection {
            columns: vec![ColumnSource::Inner(0), ColumnSource::Outer(0)],
        };
        valid.validate(1, 1, 2).unwrap();
    }

    #[test]
    fn test_composite_row_basic() {
        let left = Row::from_values(vec![Value::integer(1), Value::text("hello")]);
        let right = Row::from_values(vec![Value::float(3.14), Value::boolean(true)]);

        let comp = CompositeRow::new(left, right);

        assert_eq!(comp.len(), 4);
        assert_eq!(comp.get(0), Some(&Value::integer(1)));
        assert_eq!(comp.get(1), Some(&Value::text("hello")));
        assert_eq!(comp.get(2), Some(&Value::float(3.14)));
        assert_eq!(comp.get(3), Some(&Value::boolean(true)));
        assert_eq!(comp.get(4), None);
    }

    #[test]
    fn test_composite_row_materialize() {
        let left = Row::from_values(vec![Value::integer(1)]);
        let right = Row::from_values(vec![Value::integer(2)]);

        let comp = CompositeRow::new(left, right);
        let materialized = comp.materialize();

        assert_eq!(materialized.len(), 2);
        assert_eq!(materialized.get(0), Some(&Value::integer(1)));
        assert_eq!(materialized.get(1), Some(&Value::integer(2)));
    }

    #[test]
    fn test_row_ref_owned() {
        let row = Row::from_values(vec![Value::integer(42)]);
        let row_ref = RowRef::owned(row);

        assert_eq!(row_ref.len(), 1);
        assert_eq!(row_ref.get(0), Some(&Value::integer(42)));

        let owned = row_ref.into_owned();
        assert_eq!(owned.get(0), Some(&Value::integer(42)));
    }

    #[test]
    fn test_row_ref_composite() {
        let left = Row::from_values(vec![Value::integer(1)]);
        let right = Row::from_values(vec![Value::integer(2)]);
        let row_ref = RowRef::composite(left, right);

        assert_eq!(row_ref.len(), 2);
        assert_eq!(row_ref.get(0), Some(&Value::integer(1)));
        assert_eq!(row_ref.get(1), Some(&Value::integer(2)));
    }

    #[test]
    fn projected_row_ref_keeps_transitive_join_slots_deferred() {
        let first = RowRef::projected(
            RowRef::owned(Row::from_values(vec![
                Value::integer(1),
                Value::text("payload"),
            ])),
            RowRef::owned(Row::from_values(vec![
                Value::integer(10),
                Value::text("dictionary"),
            ])),
            CompactArc::from(vec![
                ColumnSource::Outer(0),
                ColumnSource::Outer(1),
                ColumnSource::Inner(1),
            ]),
        );
        let second = RowRef::projected(
            first,
            RowRef::owned(Row::from_values(vec![
                Value::integer(20),
                Value::text("leaf"),
            ])),
            CompactArc::from(vec![
                ColumnSource::Outer(1),
                ColumnSource::Inner(1),
                ColumnSource::Outer(2),
            ]),
        );

        assert!(second.is_deferred());
        assert_eq!(second.len(), 3);
        assert_eq!(second.get(0), Some(&Value::text("payload")));
        assert_eq!(second.get(1), Some(&Value::text("leaf")));
        assert_eq!(second.get(2), Some(&Value::text("dictionary")));

        let materialized = second.into_owned();
        assert_eq!(
            materialized,
            Row::from_values(vec![
                Value::text("payload"),
                Value::text("leaf"),
                Value::text("dictionary"),
            ])
        );
    }

    #[test]
    fn test_empty_operator() {
        let mut op = EmptyOperator::new();
        op.open().unwrap();

        assert!(op.next().unwrap().is_none());
        assert!(op.next().unwrap().is_none());

        op.close().unwrap();
    }

    #[test]
    fn test_materialized_operator() {
        let rows = vec![
            Row::from_values(vec![Value::integer(1)]),
            Row::from_values(vec![Value::integer(2)]),
            Row::from_values(vec![Value::integer(3)]),
        ];
        let schema = vec![ColumnInfo::new("id")];

        let mut op = MaterializedOperator::new(rows, schema);
        op.open().unwrap();

        let row1 = op.next().unwrap().unwrap();
        assert_eq!(row1.get(0), Some(&Value::integer(1)));

        let row2 = op.next().unwrap().unwrap();
        assert_eq!(row2.get(0), Some(&Value::integer(2)));

        let row3 = op.next().unwrap().unwrap();
        assert_eq!(row3.get(0), Some(&Value::integer(3)));

        assert!(op.next().unwrap().is_none());

        op.close().unwrap();
    }
}
