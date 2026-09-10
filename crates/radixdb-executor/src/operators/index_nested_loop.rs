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

//! Index Nested Loop Join Operator.
//!
//! This operator implements index nested loop join with O(N * log M) complexity
//! by using indexes on the inner table for lookups. It's optimal when:
//! - The inner (right) table has an index on the join key column
//! - The outer (left) table is small or has good selectivity
//!
//! For each row in the outer table, we use the index/PK to find matching rows
//! in the inner table, avoiding a full scan of the inner table.

use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::context::{current_query_is_cancelled, CancellationHandle};
use crate::expression::JoinFilter;
use crate::lookup_key::exact_integer_pk_value;
use crate::operator::{
    ColumnInfo, ColumnSource, JoinProjection, Operator, OrderingProperty, RowRef,
};
use radixdb_core::value::NULL_VALUE;
use radixdb_core::{CompactArc, CompactVec};
use radixdb_core::{Result, Row, RowVec, Value, ValueMap, ValueSet};
use radixdb_storage::expression::ConstBoolExpr;
use radixdb_storage::instrumentation::{self, JoinExecutionKind, JoinExecutionRecord};
use radixdb_storage::traits::{Index, Table};

use super::hash_join::JoinType;
use super::reference_unique_lookup::{
    execute_unique_lookup_join_batch, lookup_edge_batch, LookupEdgeCardinality, LookupEdgeFallback,
    SharedUniqueLookupRows, UniqueLookupIntegrity,
};

#[cfg(test)]
pub(crate) static INDEX_NL_CANCELLATION_OBSERVED: AtomicUsize = AtomicUsize::new(0);

#[inline]
fn check_query_cancelled(cancellation: Option<&CancellationHandle>) -> Result<()> {
    if cancellation.map_or_else(current_query_is_cancelled, CancellationHandle::is_cancelled) {
        #[cfg(test)]
        INDEX_NL_CANCELLATION_OBSERVED.fetch_add(1, Ordering::Relaxed);
        Err(radixdb_core::Error::QueryCancelled)
    } else {
        Ok(())
    }
}

fn remap_inner_projection_columns(columns: &[ColumnSource]) -> (Vec<ColumnSource>, Vec<usize>) {
    let mut inner_indices = Vec::new();
    let mut remapped = Vec::with_capacity(columns.len());

    for col in columns {
        match col {
            ColumnSource::Outer(idx) => remapped.push(ColumnSource::Outer(*idx)),
            ColumnSource::Inner(idx) => {
                let position = if let Some(pos) = inner_indices.iter().position(|seen| seen == idx)
                {
                    pos
                } else {
                    inner_indices.push(*idx);
                    inner_indices.len() - 1
                };
                remapped.push(ColumnSource::Inner(position));
            }
        }
    }

    (remapped, inner_indices)
}

/// Index Nested Loop Join lookup strategy.
/// Determines how to find matching rows in the inner (right) table.
#[derive(Clone)]
pub enum IndexLookupStrategy {
    /// Use a secondary index for lookups (index.get_row_ids_equal)
    SecondaryIndex(Arc<dyn Index>),
    /// Use the table's complete hot+cold exact candidate contract. The stored
    /// column name lets a segmented table resolve immutable postings without
    /// exposing them as a mutable hot Index handle.
    SegmentedSecondaryIndex {
        column_name: String,
        index_name: String,
    },
    /// Use primary key lookup (direct row_id = value)
    /// In radixdb, PRIMARY KEY INTEGER values ARE the row_ids
    PrimaryKey,
}

/// Index Nested Loop Join Operator.
///
/// For each row in the outer input, looks up matching rows in the inner table
/// using an index. This avoids full table scans of the inner table.
pub struct IndexNestedLoopJoinOperator {
    // Outer input operator
    outer: Box<dyn Operator>,

    // Inner table (accessed via index)
    inner_table: Box<dyn Table>,

    // Join configuration
    join_type: JoinType,
    outer_key_idx: usize,
    lookup_strategy: IndexLookupStrategy,
    residual_filter: Option<JoinFilter>,
    cancellation: Option<CancellationHandle>,

    // Output schema
    schema: Vec<ColumnInfo>,
    inner_col_count: usize,

    // Optional projection pushdown: create projected rows during combine
    projection: Option<JoinProjection>,
    // Inner table columns fetched when projection is safe to push below row-id lookup.
    // Projection ColumnSource::Inner indices are remapped to these positions.
    inner_projection_indices: Option<Vec<usize>>,
    // Active-row position of the INTEGER PK used by direct row-id lookup. The
    // position is remapped when inner projection pushdown is enabled.
    inner_lookup_key_idx: Option<usize>,
    projection_error: Option<String>,
    output_ordering: OrderingProperty,

    // Current state
    current_outer_row: Option<Row>,
    // Optimization: Store (id, row) to verify specific inner rows if needed
    current_inner_rows: RowVec,
    current_inner_idx: usize,
    outer_had_match: bool,

    // Optimization: Reusable buffer for row IDs to avoid allocation per outer row
    row_id_buffer: Vec<i64>,

    // Optimization: Reusable row buffer to avoid allocation per join output
    row_buffer: Row,

    // Expression for fetching rows (always true - we apply residual separately)
    true_expr: ConstBoolExpr,

    // State tracking
    opened: bool,
    outer_exhausted: bool,

    // Bounded operator-local observability, published once by close().
    execution_kind: JoinExecutionKind,
    metrics_started: Option<Instant>,
    metrics_recorded: bool,
    observed_outer_rows: u64,
    observed_key_rows: u64,
    observed_lookup_calls: u64,
    observed_lookup_candidates: u64,
    observed_output_rows: u64,
    outer_width: u64,
    inner_width: u64,
}

impl IndexNestedLoopJoinOperator {
    /// Create a new index nested loop join operator.
    ///
    /// # Arguments
    /// * `outer` - Outer input operator
    /// * `inner_table` - Inner table to lookup from
    /// * `inner_schema` - Schema of the inner table
    /// * `join_type` - Type of join (INNER or LEFT)
    /// * `outer_key_idx` - Column index of the join key in outer rows
    /// * `lookup_strategy` - How to find matching inner rows
    /// * `residual_filter` - Optional additional filter after key match
    pub fn new(
        outer: Box<dyn Operator>,
        inner_table: Box<dyn Table>,
        inner_schema: Vec<ColumnInfo>,
        join_type: JoinType,
        outer_key_idx: usize,
        lookup_strategy: IndexLookupStrategy,
        residual_filter: Option<JoinFilter>,
    ) -> Self {
        let lookup_is_unique = match &lookup_strategy {
            IndexLookupStrategy::PrimaryKey => true,
            IndexLookupStrategy::SecondaryIndex(index) => index.is_unique(),
            IndexLookupStrategy::SegmentedSecondaryIndex { column_name, .. } => inner_table
                .get_index_on_column(column_name)
                .is_some_and(|index| index.is_unique()),
        };
        let output_ordering = if lookup_is_unique {
            outer.ordering()
        } else {
            OrderingProperty::Unknown
        };
        let inner_table_schema = inner_table.schema();
        let inner_lookup_key_idx = match &lookup_strategy {
            IndexLookupStrategy::PrimaryKey => inner_table_schema.pk_column_index(),
            IndexLookupStrategy::SecondaryIndex(index) => index
                .column_names()
                .first()
                .and_then(|column_name| inner_table_schema.find_column(column_name))
                .map(|(column_index, _)| column_index),
            IndexLookupStrategy::SegmentedSecondaryIndex { column_name, .. } => inner_table_schema
                .find_column(column_name)
                .map(|(column_index, _)| column_index),
        };
        // Build combined schema
        let mut schema = Vec::new();
        schema.extend(outer.schema().iter().cloned());
        schema.extend(inner_schema.iter().cloned());

        let inner_col_count = inner_schema.len();

        // Pre-allocate row buffer for typical join output size
        let outer_col_count = outer.schema().len();
        let total_cols = outer_col_count + inner_col_count;

        Self {
            outer,
            inner_table,
            join_type,
            outer_key_idx,
            lookup_strategy,
            residual_filter,
            cancellation: None,
            schema,
            inner_col_count,
            projection: None,
            inner_projection_indices: None,
            inner_lookup_key_idx,
            projection_error: None,
            output_ordering,
            current_outer_row: None,
            current_inner_rows: RowVec::new(),
            current_inner_idx: 0,
            outer_had_match: false,
            // Pre-allocate buffer for typical number of matches (small)
            row_id_buffer: Vec::with_capacity(16),
            // Pre-allocate row buffer to avoid per-row allocation
            row_buffer: Row::with_capacity(total_cols),
            true_expr: ConstBoolExpr::true_expr(),
            opened: false,
            outer_exhausted: false,
            execution_kind: JoinExecutionKind::IndexNestedLoop,
            metrics_started: None,
            metrics_recorded: false,
            observed_outer_rows: 0,
            observed_key_rows: 0,
            observed_lookup_calls: 0,
            observed_lookup_candidates: 0,
            observed_output_rows: 0,
            outer_width: outer_col_count as u64,
            inner_width: inner_col_count as u64,
        }
    }

    fn reset_metrics(&mut self) {
        self.metrics_started = Some(Instant::now());
        self.metrics_recorded = false;
        self.observed_outer_rows = 0;
        self.observed_key_rows = 0;
        self.observed_lookup_calls = 0;
        self.observed_lookup_candidates = 0;
        self.observed_output_rows = 0;
    }

    fn publish_metrics(&mut self) {
        if self.metrics_recorded {
            return;
        }
        let Some(started) = self.metrics_started.take() else {
            return;
        };
        instrumentation::record_join_outer_rows(self.observed_outer_rows, self.observed_key_rows);
        instrumentation::record_join_rows_constructed(self.observed_output_rows);
        instrumentation::record_join_execution(
            self.execution_kind,
            JoinExecutionRecord {
                left_rows: self.observed_outer_rows,
                right_rows: self.observed_lookup_candidates,
                output_rows: self.observed_output_rows,
                left_width: self.outer_width,
                right_width: self.inner_width,
                output_width: self.schema.len() as u64,
                candidate_pairs: self.observed_lookup_candidates,
                lookup_calls: self.observed_lookup_calls,
                lookup_candidate_rows: self.observed_lookup_candidates,
                wall_nanos: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                outer_pull_nanos: 0,
                key_prepare_nanos: 0,
                lookup_nanos: 0,
                candidate_map_nanos: 0,
            },
        );
        self.metrics_recorded = true;
    }

    pub fn with_cancellation(mut self, cancellation: CancellationHandle) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    /// Set projection pushdown configuration.
    ///
    /// When set, the operator creates projected rows directly during the combine step
    /// instead of creating full combined rows. This avoids creating large intermediate
    /// rows that would be immediately projected down to fewer columns.
    ///
    /// # Arguments
    /// * `columns` - Column sources in SELECT order (preserves original column ordering)
    /// * `projected_schema` - Schema for the projected output
    pub fn with_projection(
        mut self,
        columns: Vec<ColumnSource>,
        projected_schema: Vec<ColumnInfo>,
    ) -> Self {
        self.output_ordering = self.output_ordering.remap_outer_projection(&columns);
        self.projection_error = JoinProjection {
            columns: columns.clone(),
        }
        .validate(
            self.outer.schema().len(),
            self.inner_col_count,
            projected_schema.len(),
        )
        .err()
        .map(|error| error.to_string());
        let (columns, inner_projection_indices) = if self.residual_filter.is_none() {
            let (remapped, mut inner_indices) = remap_inner_projection_columns(&columns);
            if let Some(key_idx) = self.inner_lookup_key_idx {
                let active_idx = inner_indices
                    .iter()
                    .position(|index| *index == key_idx)
                    .unwrap_or_else(|| {
                        inner_indices.push(key_idx);
                        inner_indices.len() - 1
                    });
                self.inner_lookup_key_idx = Some(active_idx);
            }
            (remapped, Some(inner_indices))
        } else {
            (columns, None)
        };
        self.projection = Some(JoinProjection { columns });
        self.inner_projection_indices = inner_projection_indices;
        if let Some(indices) = self.inner_projection_indices.as_ref() {
            self.inner_width = indices.len() as u64;
        }
        self.schema = projected_schema;
        self
    }

    /// Create a NULL row for the inner side.
    /// Creates the active inner row shape: projected when safe, full-width otherwise.
    #[inline]
    fn null_inner_row(&self) -> Row {
        let inner_col_count = self
            .inner_projection_indices
            .as_ref()
            .map_or(self.inner_col_count, Vec::len);
        let null_values: Vec<Value> = (0..inner_col_count).map(|_| NULL_VALUE).collect();
        Row::from_values(null_values)
    }

    /// Combine outer and inner rows into reusable buffer (owns both).
    /// OPTIMIZATION: Moves both outer and inner values without cloning.
    /// Use when outer row is no longer needed (last match for this outer).
    ///
    /// Projection indices are validated at query planning time against the
    /// table schemas. Checked indexing keeps a violated contract fail-closed.
    #[inline]
    fn combine_owned_into_buffer(&mut self, outer: Row, inner: Row) {
        match &self.projection {
            Some(proj) => {
                // Fused projection into buffer - columns are in SELECT order
                self.row_buffer.clear();
                self.row_buffer.reserve(proj.columns.len());

                // OPTIMIZATION: Move values instead of cloning (we own both rows)
                let mut outer_values = outer.into_values();
                let mut inner_values = inner.into_values();

                for col_source in &proj.columns {
                    match col_source {
                        ColumnSource::Outer(idx) => {
                            self.row_buffer
                                .push(std::mem::take(&mut outer_values[*idx]));
                        }
                        ColumnSource::Inner(idx) => {
                            self.row_buffer
                                .push(std::mem::take(&mut inner_values[*idx]));
                        }
                    }
                }
            }
            None => {
                // Combine both rows into buffer
                self.row_buffer.combine_into_owned(outer, inner);
            }
        }
    }

    /// Take the combined row from the buffer, preserving buffer capacity.
    /// OPTIMIZATION: Uses take_and_clear to keep buffer capacity for next iteration,
    /// avoiding reallocation in combine_into_owned.
    #[inline]
    fn take_from_buffer(&mut self) -> Row {
        self.row_buffer.take_and_clear()
    }

    /// Create combined row directly (for cases where buffer can't be used).
    /// When projection is set, creates projected row directly.
    ///
    /// Projection indices are validated at query planning time against the
    /// table schemas. Checked indexing keeps a violated contract fail-closed.
    #[inline]
    fn create_combined_row(&self, outer: &Row, inner: Row) -> Row {
        match &self.projection {
            Some(proj) => {
                // Fused projection: create only the columns we need, in SELECT order
                let mut values: CompactVec<Value> = CompactVec::with_capacity(proj.columns.len());

                // We need to move from inner but clone from outer (we don't own outer)
                let outer_slice = outer.as_slice();
                let mut inner_values = inner.into_values();

                for col_source in &proj.columns {
                    match col_source {
                        ColumnSource::Outer(idx) => {
                            values.push(outer_slice[*idx].clone());
                        }
                        ColumnSource::Inner(idx) => {
                            values.push(std::mem::take(&mut inner_values[*idx]));
                        }
                    }
                }

                Row::from_compact_vec(values)
            }
            None => Row::from_combined_clone_move(outer, inner),
        }
    }

    /// Look up matching inner rows for the current outer row.
    /// Uses internal buffers to avoid allocations.
    fn lookup_inner_rows(&mut self, key_value: &Value) -> Result<()> {
        check_query_cancelled(self.cancellation.as_ref())?;
        self.observed_lookup_calls = self.observed_lookup_calls.saturating_add(1);
        // Clear buffers for reuse
        self.row_id_buffer.clear();
        self.current_inner_rows.clear();

        let column_name = match &self.lookup_strategy {
            IndexLookupStrategy::PrimaryKey => self
                .inner_table
                .schema()
                .pk_column_index()
                .and_then(|column_index| self.inner_table.schema().columns.get(column_index))
                .map(|column| column.name.as_str()),
            IndexLookupStrategy::SecondaryIndex(index) => {
                index.column_names().first().map(String::as_str)
            }
            IndexLookupStrategy::SegmentedSecondaryIndex { column_name, .. } => {
                Some(column_name.as_str())
            }
        }
        .ok_or_else(|| {
            radixdb_core::Error::internal("index nested-loop lookup has no key column")
        })?;
        // Keep the direct physical lookup for the common committed-only path.
        // A table-level lookup is required only when the current transaction
        // owns an unpublished delta, for segmented cold postings, or when a
        // non-INTEGER primary key cannot be mapped directly to row_id.
        let requires_table_lookup = self.inner_table.has_local_changes()
            || matches!(
                &self.lookup_strategy,
                IndexLookupStrategy::SegmentedSecondaryIndex { .. }
            )
            || matches!(&self.lookup_strategy, IndexLookupStrategy::PrimaryKey)
                && exact_integer_pk_value(key_value).is_none();
        if requires_table_lookup {
            let row_ids = self
                .inner_table
                .collect_row_ids_by_index_values(column_name, std::slice::from_ref(key_value))
                .ok_or_else(|| {
                    radixdb_core::Error::internal(format!(
                        "transactional join index coverage disappeared for column {column_name}",
                    ))
                })??;
            self.row_id_buffer.extend(row_ids);
        } else {
            match &self.lookup_strategy {
                IndexLookupStrategy::SecondaryIndex(index) => {
                    index.get_row_ids_equal_into(
                        std::slice::from_ref(key_value),
                        &mut self.row_id_buffer,
                    )?;
                }
                IndexLookupStrategy::SegmentedSecondaryIndex { .. } => unreachable!(
                    "segmented join lookup must use the table-level exact-index contract"
                ),
                IndexLookupStrategy::PrimaryKey => {
                    if let Some(id) = exact_integer_pk_value(key_value) {
                        self.row_id_buffer.push(id);
                    }
                }
            }
        }

        if self.row_id_buffer.is_empty() {
            return Ok(());
        }

        // Fetch matching rows directly into inner_rows buffer. When the join has
        // no residual ON predicate, projection can cross the row-id lookup
        // boundary safely: the index lookup already proved the key match and the
        // output projection is remapped to the projected inner row shape.
        if let Some(indices) = self.inner_projection_indices.as_ref() {
            self.current_inner_rows = self
                .inner_table
                .collect_rows_by_ids_projected(&self.row_id_buffer, indices)?;
        } else {
            self.inner_table.fetch_rows_by_ids_into(
                &self.row_id_buffer,
                &self.true_expr,
                &mut self.current_inner_rows,
            )?;
        }
        // Transaction-local secondary-index changes are not published to the
        // shared index before commit. The table lookup therefore admits the
        // complete local delta as candidates. Recheck the authoritative row
        // value here to remove unrelated inserts, deletes and old-key entries.
        // The projected lookup shape always retains this key column.
        let key_idx = self.inner_lookup_key_idx.ok_or_else(|| {
            radixdb_core::Error::invalid_argument(
                "index nested-loop lookup cannot resolve its inner key column",
            )
        })?;
        self.current_inner_rows.retain(|(_, row)| {
            row.get(key_idx)
                .is_some_and(|inner_key| !inner_key.is_null() && inner_key == key_value)
        });
        self.observed_lookup_candidates = self
            .observed_lookup_candidates
            .saturating_add(self.current_inner_rows.len() as u64);
        check_query_cancelled(self.cancellation.as_ref())?;
        Ok(())
    }

    /// Advance to the next outer row and lookup matching inner rows.
    fn advance_outer(&mut self) -> Result<bool> {
        check_query_cancelled(self.cancellation.as_ref())?;
        match self.outer.next()? {
            Some(row_ref) => {
                self.observed_outer_rows = self.observed_outer_rows.saturating_add(1);
                let outer_row = row_ref.into_owned();

                // Get the join key value from the outer row
                let key_value = match outer_row.get(self.outer_key_idx) {
                    Some(v) if !v.is_null() => v.clone(),
                    _ => {
                        // NULL key - no match possible (NULL != NULL in SQL)
                        self.current_outer_row = Some(outer_row);
                        self.current_inner_rows.clear();
                        self.current_inner_idx = 0;
                        self.outer_had_match = false;
                        return Ok(true);
                    }
                };
                self.observed_key_rows = self.observed_key_rows.saturating_add(1);

                // Lookup matching inner rows
                self.lookup_inner_rows(&key_value)?;

                self.current_outer_row = Some(outer_row);
                // self.current_inner_rows is already populated by lookup_inner_rows
                self.current_inner_idx = 0;
                self.outer_had_match = false;
                Ok(true)
            }
            None => {
                self.outer_exhausted = true;
                Ok(false)
            }
        }
    }
}

impl Operator for IndexNestedLoopJoinOperator {
    fn open(&mut self) -> Result<()> {
        self.reset_metrics();
        check_query_cancelled(self.cancellation.as_ref())?;
        if let Some(message) = self.projection_error.take() {
            return Err(radixdb_core::Error::invalid_argument(message));
        }
        if let Some(projection) = &self.projection {
            JoinProjection {
                columns: projection.columns.clone(),
            }
            .validate(
                self.outer.schema().len(),
                self.inner_projection_indices
                    .as_ref()
                    .map_or(self.inner_col_count, Vec::len),
                self.schema.len(),
            )?;
        }
        if let Err(error) = self.outer.open() {
            let _ = self.outer.close();
            return Err(error);
        }

        // Get first outer row
        if let Err(error) = self.advance_outer() {
            let _ = self.outer.close();
            return Err(error);
        }

        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        check_query_cancelled(self.cancellation.as_ref())?;
        if !self.opened {
            return Err(radixdb_core::Error::internal(
                "IndexNestedLoopJoinOperator::next called before open",
            ));
        }

        let is_left_outer = matches!(self.join_type, JoinType::Left | JoinType::Full);

        loop {
            // Check if outer is exhausted
            if self.outer_exhausted {
                return Ok(None);
            }

            // Ensure we have an outer row
            if self.current_outer_row.is_none() && !self.advance_outer()? {
                return Ok(None);
            }

            // Try to find a match in current inner rows
            let inner_len = self.current_inner_rows.len();
            while self.current_inner_idx < inner_len {
                if self.current_inner_idx & 0xff == 0 {
                    check_query_cancelled(self.cancellation.as_ref())?;
                }
                let inner_idx = self.current_inner_idx;
                self.current_inner_idx += 1;

                // Check filter with borrowed references first
                let passes_filter = {
                    let outer_row = self.current_outer_row.as_ref().unwrap();
                    let inner_entry = &self.current_inner_rows[inner_idx];
                    if let Some(ref filter) = self.residual_filter {
                        filter.matches_checked(outer_row, &inner_entry.1)?
                    } else {
                        true
                    }
                };

                if passes_filter {
                    self.outer_had_match = true;
                    let inner_row = std::mem::take(&mut self.current_inner_rows[inner_idx].1);

                    // OPTIMIZATION: Check if this is the last inner row to check
                    // If so, we can take ownership of outer_row and move its values
                    let is_last_inner = self.current_inner_idx >= inner_len;
                    if is_last_inner {
                        // No more inner rows - take ownership of outer and move values
                        let outer_row = self.current_outer_row.take().unwrap();
                        // Pre-advance to next outer for next call
                        self.advance_outer()?;
                        self.combine_owned_into_buffer(outer_row, inner_row);
                        self.observed_output_rows = self.observed_output_rows.saturating_add(1);
                        return Ok(Some(RowRef::Owned(self.take_from_buffer())));
                    } else {
                        // More inner rows to check - create combined row directly
                        // (Can't use buffer here due to borrow of current_outer_row)
                        let outer_row = self.current_outer_row.as_ref().unwrap();
                        let combined = self.create_combined_row(outer_row, inner_row);
                        self.observed_output_rows = self.observed_output_rows.saturating_add(1);
                        return Ok(Some(RowRef::Owned(combined)));
                    }
                }
            }

            // Exhausted inner rows for current outer row
            // Handle LEFT OUTER: emit outer row with NULLs if no match
            if is_left_outer && !self.outer_had_match {
                let outer_row = self.current_outer_row.take().unwrap();
                self.advance_outer()?;
                let null_inner = self.null_inner_row();
                // Use buffer-based combine since we own outer_row
                self.combine_owned_into_buffer(outer_row, null_inner);
                self.observed_output_rows = self.observed_output_rows.saturating_add(1);
                return Ok(Some(RowRef::Owned(self.take_from_buffer())));
            }

            // Move to next outer row
            if !self.advance_outer()? {
                return Ok(None);
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        let result = self.outer.close();
        self.publish_metrics();
        self.opened = false;
        result
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Rough estimate based on outer side
        let outer_est = self.outer.estimated_rows()?;
        Some(match self.join_type {
            JoinType::Inner => outer_est, // Assume most outer rows match
            JoinType::Left | JoinType::Full => outer_est,
            _ => outer_est,
        })
    }

    fn ordering(&self) -> OrderingProperty {
        self.output_ordering.clone()
    }

    fn name(&self) -> &str {
        match self.join_type {
            JoinType::Inner => "IndexNL (INNER)",
            JoinType::Left => "IndexNL (LEFT)",
            _ => "IndexNL",
        }
    }
}

// Align the logical lookup window with one artifact-backed row group. A 256-row outer
// window made a 10k/100k fan-out reopen the table fetch cache and decode the
// same immutable column blocks tens or hundreds of times. The byte ceiling
// below keeps wide rows bounded independently of this cardinality ceiling.
// Permit two physical row groups when the retained graph remains inside the
// explicit byte budget. A 64K row-only ceiling split a common 100K fan-out
// into two batches even though its projected/deferred graph was small enough;
// the next unique edge then fetched the same dictionary keys twice. The byte
// guard remains authoritative for wide values, so this does not turn the
// operator into an unbounded collector.
const BATCH_INDEX_NL_OUTER_ROWS: usize = 131_072;
const BATCH_INDEX_NL_OUTER_BYTES: usize = 64 * 1024 * 1024;

/// Bounded batch Index-NL operator.
///
/// A fixed-size outer chunk is collected, lookup keys are deduplicated, and
/// matching inner rows are fetched once for the whole chunk. Results remain
/// streamed in outer-row order; the operator never retains the complete outer
/// input or complete join output.
pub struct BatchIndexNestedLoopJoinOperator {
    outer: Box<dyn Operator>,
    inner_table: Box<dyn Table>,
    join_type: JoinType,
    outer_key_idx: usize,
    lookup_strategy: IndexLookupStrategy,
    residual_filter: Option<JoinFilter>,
    cancellation: Option<CancellationHandle>,
    schema: Vec<ColumnInfo>,
    inner_col_count: usize,
    inner_lookup_column: Option<String>,
    inner_lookup_key_idx: Option<usize>,
    lookup_is_unique: bool,
    output_ordering: OrderingProperty,
    active_inner_key_idx: Option<usize>,
    projection: Option<JoinProjection>,
    projection_columns: Option<CompactArc<[ColumnSource]>>,
    inner_projection_indices: Option<Vec<usize>>,
    projection_error: Option<String>,
    outer_batch: Vec<RowRef>,
    outer_index: usize,
    current_inner_index: usize,
    current_outer_had_match: bool,
    current_non_unique_indices: Option<CompactArc<[usize]>>,
    unique_inner_rows_by_key: Option<SharedUniqueLookupRows>,
    inner_rows: Option<CompactArc<Vec<Row>>>,
    inner_row_indices_by_key: ValueMap<CompactArc<[usize]>>,
    opened: bool,
    outer_exhausted: bool,
    metrics_started: Option<Instant>,
    metrics_recorded: bool,
    observed_outer_rows: u64,
    observed_key_rows: u64,
    observed_lookup_calls: u64,
    observed_lookup_candidates: u64,
    observed_lookup_key_rows: u64,
    observed_lookup_distinct_keys: u64,
    observed_output_rows: u64,
    observed_deferred_outer_rows: u64,
    observed_deferred_output_rows: u64,
    observed_outer_pull_nanos: u64,
    observed_key_prepare_nanos: u64,
    observed_lookup_nanos: u64,
    observed_candidate_map_nanos: u64,
    outer_width: u64,
    inner_width: u64,
}

impl BatchIndexNestedLoopJoinOperator {
    /// Create a new batch index nested loop join operator.
    pub fn new(
        outer: Box<dyn Operator>,
        inner_table: Box<dyn Table>,
        inner_schema: Vec<ColumnInfo>,
        join_type: JoinType,
        outer_key_idx: usize,
        lookup_strategy: IndexLookupStrategy,
        residual_filter: Option<JoinFilter>,
    ) -> Self {
        let inner_table_schema = inner_table.schema();
        let inner_lookup_column = match &lookup_strategy {
            IndexLookupStrategy::PrimaryKey => inner_table_schema
                .pk_column_index()
                .and_then(|index| inner_table_schema.columns.get(index))
                .map(|column| column.name.clone()),
            IndexLookupStrategy::SecondaryIndex(index) => index.column_names().first().cloned(),
            IndexLookupStrategy::SegmentedSecondaryIndex { column_name, .. } => {
                Some(column_name.clone())
            }
        };
        let inner_lookup_key_idx = inner_lookup_column.as_ref().and_then(|column_name| {
            inner_table_schema
                .find_column(column_name)
                .map(|(index, _)| index)
        });
        let lookup_is_unique = match &lookup_strategy {
            IndexLookupStrategy::PrimaryKey => true,
            IndexLookupStrategy::SecondaryIndex(index) => index.is_unique(),
            IndexLookupStrategy::SegmentedSecondaryIndex { column_name, .. } => inner_table
                .get_index_on_column(column_name)
                .is_some_and(|index| index.is_unique()),
        };
        let output_ordering = if lookup_is_unique {
            outer.ordering()
        } else {
            OrderingProperty::Unknown
        };
        let mut schema = outer.schema().to_vec();
        schema.extend(inner_schema.iter().cloned());
        let outer_width = outer.schema().len() as u64;
        let inner_width = inner_schema.len() as u64;
        Self {
            outer,
            inner_table,
            join_type,
            outer_key_idx,
            lookup_strategy,
            residual_filter,
            cancellation: None,
            schema,
            inner_col_count: inner_schema.len(),
            inner_lookup_column,
            inner_lookup_key_idx,
            lookup_is_unique,
            output_ordering,
            active_inner_key_idx: inner_lookup_key_idx,
            projection: None,
            projection_columns: None,
            inner_projection_indices: None,
            projection_error: None,
            outer_batch: Vec::with_capacity(4_096),
            outer_index: 0,
            current_inner_index: 0,
            current_outer_had_match: false,
            current_non_unique_indices: None,
            unique_inner_rows_by_key: None,
            inner_rows: None,
            inner_row_indices_by_key: ValueMap::default(),
            opened: false,
            outer_exhausted: false,
            metrics_started: None,
            metrics_recorded: false,
            observed_outer_rows: 0,
            observed_key_rows: 0,
            observed_lookup_calls: 0,
            observed_lookup_candidates: 0,
            observed_lookup_key_rows: 0,
            observed_lookup_distinct_keys: 0,
            observed_output_rows: 0,
            observed_deferred_outer_rows: 0,
            observed_deferred_output_rows: 0,
            observed_outer_pull_nanos: 0,
            observed_key_prepare_nanos: 0,
            observed_lookup_nanos: 0,
            observed_candidate_map_nanos: 0,
            outer_width,
            inner_width,
        }
    }

    pub fn with_cancellation(mut self, cancellation: CancellationHandle) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    /// Set projection pushdown configuration.
    ///
    /// When set, the operator creates projected rows directly during the combine step
    /// instead of creating full combined rows. This avoids creating large intermediate
    /// rows that would be immediately projected down to fewer columns.
    ///
    /// # Arguments
    /// * `columns` - Column sources in SELECT order (preserves original column ordering)
    /// * `projected_schema` - Schema for the projected output
    pub fn with_projection(
        mut self,
        columns: Vec<ColumnSource>,
        projected_schema: Vec<ColumnInfo>,
    ) -> Self {
        self.output_ordering = self.output_ordering.remap_outer_projection(&columns);
        self.projection_error = JoinProjection {
            columns: columns.clone(),
        }
        .validate(
            self.outer.schema().len(),
            self.inner_col_count,
            projected_schema.len(),
        )
        .err()
        .map(|error| error.to_string());
        let (columns, inner_projection_indices) = if self.residual_filter.is_none() {
            let (remapped, mut inner_indices) = remap_inner_projection_columns(&columns);
            if let Some(key_idx) = self.inner_lookup_key_idx {
                let active_idx = inner_indices
                    .iter()
                    .position(|index| *index == key_idx)
                    .unwrap_or_else(|| {
                        inner_indices.push(key_idx);
                        inner_indices.len() - 1
                    });
                self.active_inner_key_idx = Some(active_idx);
            }
            (remapped, Some(inner_indices))
        } else {
            (columns, None)
        };
        self.projection_columns = Some(CompactArc::from(columns.clone()));
        self.projection = Some(JoinProjection { columns });
        self.inner_projection_indices = inner_projection_indices;
        if let Some(indices) = self.inner_projection_indices.as_ref() {
            self.inner_width = indices.len() as u64;
        }
        self.schema = projected_schema;
        self
    }

    fn reset_metrics(&mut self) {
        self.metrics_started = Some(Instant::now());
        self.metrics_recorded = false;
        self.observed_outer_rows = 0;
        self.observed_key_rows = 0;
        self.observed_lookup_calls = 0;
        self.observed_lookup_candidates = 0;
        self.observed_lookup_key_rows = 0;
        self.observed_lookup_distinct_keys = 0;
        self.observed_output_rows = 0;
        self.observed_deferred_outer_rows = 0;
        self.observed_deferred_output_rows = 0;
        self.observed_outer_pull_nanos = 0;
        self.observed_key_prepare_nanos = 0;
        self.observed_lookup_nanos = 0;
        self.observed_candidate_map_nanos = 0;
    }

    fn publish_metrics(&mut self) {
        if self.metrics_recorded {
            return;
        }
        let Some(started) = self.metrics_started.take() else {
            return;
        };
        instrumentation::record_join_outer_rows(self.observed_outer_rows, self.observed_key_rows);
        instrumentation::record_join_lookup_key_batch(
            self.observed_lookup_key_rows,
            self.observed_lookup_distinct_keys,
        );
        instrumentation::record_join_rows_constructed(
            self.observed_output_rows
                .saturating_sub(self.observed_deferred_output_rows),
        );
        instrumentation::record_join_deferred_rows(
            self.observed_deferred_output_rows,
            self.observed_deferred_outer_rows,
        );
        instrumentation::record_join_execution(
            JoinExecutionKind::BatchIndexNestedLoop,
            JoinExecutionRecord {
                left_rows: self.observed_outer_rows,
                right_rows: self.observed_lookup_candidates,
                output_rows: self.observed_output_rows,
                left_width: self.outer_width,
                right_width: self.inner_width,
                output_width: self.schema.len() as u64,
                candidate_pairs: self.observed_lookup_candidates,
                lookup_calls: self.observed_lookup_calls,
                lookup_candidate_rows: self.observed_lookup_candidates,
                wall_nanos: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                outer_pull_nanos: self.observed_outer_pull_nanos,
                key_prepare_nanos: self.observed_key_prepare_nanos,
                lookup_nanos: self.observed_lookup_nanos,
                candidate_map_nanos: self.observed_candidate_map_nanos,
            },
        );
        self.metrics_recorded = true;
    }

    fn load_outer_batch(&mut self) -> Result<bool> {
        check_query_cancelled(self.cancellation.as_ref())?;
        self.outer_batch.clear();
        self.unique_inner_rows_by_key = None;
        self.inner_rows = None;
        self.inner_row_indices_by_key.clear();
        self.outer_index = 0;
        self.current_inner_index = 0;
        self.current_outer_had_match = false;
        self.current_non_unique_indices = None;

        let outer_pull_started = Instant::now();
        let mut retained_bytes = 0usize;
        while self.outer_batch.len() < BATCH_INDEX_NL_OUTER_ROWS {
            let Some(row_ref) = self.outer.next()? else {
                self.outer_exhausted = true;
                break;
            };
            retained_bytes = retained_bytes.saturating_add(row_ref.estimated_retained_bytes());
            self.observed_deferred_outer_rows = self
                .observed_deferred_outer_rows
                .saturating_add(u64::from(row_ref.is_deferred()));
            self.outer_batch.push(row_ref);
            if retained_bytes >= BATCH_INDEX_NL_OUTER_BYTES {
                break;
            }
        }
        self.observed_outer_pull_nanos = self.observed_outer_pull_nanos.saturating_add(
            outer_pull_started
                .elapsed()
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64,
        );
        if self.outer_batch.is_empty() {
            return Ok(false);
        }

        self.observed_outer_rows = self
            .observed_outer_rows
            .saturating_add(self.outer_batch.len() as u64);
        let key_prepare_started = Instant::now();
        let mut seen = ValueSet::with_capacity(self.outer_batch.len());
        let mut keys = Vec::with_capacity(self.outer_batch.len());
        let mut key_rows = 0u64;
        for row in &self.outer_batch {
            let Some(value) = row.get(self.outer_key_idx).filter(|value| !value.is_null()) else {
                continue;
            };
            self.observed_key_rows = self.observed_key_rows.saturating_add(1);
            key_rows = key_rows.saturating_add(1);
            match self.lookup_strategy {
                IndexLookupStrategy::PrimaryKey => {
                    let Some(key) = exact_integer_pk_value(value).map(Value::Integer) else {
                        continue;
                    };
                    if seen.insert(key.clone()) {
                        keys.push(key);
                    }
                }
                _ => {
                    // Probe the set by reference first. Repeated UUID/reference
                    // keys dominate fan-out joins; cloning before dedup paid an
                    // Arc operation for every outer row instead of every
                    // distinct lookup key.
                    if !seen.contains(value) {
                        let key = value.clone();
                        seen.insert(key.clone());
                        keys.push(key);
                    }
                }
            }
        }
        self.observed_lookup_key_rows = self.observed_lookup_key_rows.saturating_add(key_rows);
        self.observed_lookup_distinct_keys = self
            .observed_lookup_distinct_keys
            .saturating_add(keys.len() as u64);
        self.observed_key_prepare_nanos = self.observed_key_prepare_nanos.saturating_add(
            key_prepare_started
                .elapsed()
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64,
        );
        if keys.is_empty() {
            return Ok(true);
        }

        let column_name = self.inner_lookup_column.as_deref().ok_or_else(|| {
            radixdb_core::Error::invalid_argument(
                "batch index nested-loop lookup has no inner key column",
            )
        })?;
        let fallback = match &self.lookup_strategy {
            IndexLookupStrategy::PrimaryKey => LookupEdgeFallback::IntegerPrimaryKey,
            IndexLookupStrategy::SecondaryIndex(index) => {
                LookupEdgeFallback::SecondaryIndex(index.as_ref())
            }
            IndexLookupStrategy::SegmentedSecondaryIndex { .. } => LookupEdgeFallback::None,
        };
        let cancellation = self.cancellation.clone();
        let key_idx = self.active_inner_key_idx.ok_or_else(|| {
            radixdb_core::Error::invalid_argument(
                "batch index nested-loop lookup cannot resolve active inner key",
            )
        })?;

        if self.lookup_is_unique {
            let lookup_started = Instant::now();
            let lookup = execute_unique_lookup_join_batch(
                self.inner_table.as_ref(),
                column_name,
                &keys,
                self.inner_projection_indices.as_deref(),
                key_idx,
                LookupEdgeCardinality::AtMostOne,
                fallback,
                |_| Ok(()),
                || check_query_cancelled(cancellation.as_ref()),
            )?
            .ok_or_else(|| lookup_disappeared_error(&self.lookup_strategy))?;
            if lookup.integrity != UniqueLookupIntegrity::Complete {
                return Err(radixdb_core::Error::internal(
                    "proven unique batch join lookup returned duplicate visible rows",
                ));
            }
            self.observed_lookup_calls = self
                .observed_lookup_calls
                .saturating_add(lookup.lookup_calls);
            self.observed_lookup_candidates = self
                .observed_lookup_candidates
                .saturating_add(lookup.candidate_rows as u64);
            self.unique_inner_rows_by_key = Some(lookup.rows_by_key.into_shared());
            self.observed_lookup_nanos = self.observed_lookup_nanos.saturating_add(
                lookup_started
                    .elapsed()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64,
            );
        } else {
            let lookup_started = Instant::now();
            let lookup = lookup_edge_batch(
                self.inner_table.as_ref(),
                column_name,
                &keys,
                self.inner_projection_indices.as_deref(),
                LookupEdgeCardinality::Unbounded,
                fallback,
                || check_query_cancelled(cancellation.as_ref()),
            )?
            .ok_or_else(|| lookup_disappeared_error(&self.lookup_strategy))?;
            self.observed_lookup_nanos = self.observed_lookup_nanos.saturating_add(
                lookup_started
                    .elapsed()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64,
            );
            self.observed_lookup_calls = self
                .observed_lookup_calls
                .saturating_add(lookup.lookup_calls);
            self.observed_lookup_candidates = self
                .observed_lookup_candidates
                .saturating_add(lookup.rows.len() as u64);
            let candidate_map_started = Instant::now();
            let mut inner_rows = Vec::with_capacity(lookup.rows.len());
            let mut row_indices_by_key: ValueMap<Vec<usize>> = ValueMap::default();
            for (_, row) in lookup.rows {
                if let Some(key) = row.get(key_idx).cloned() {
                    row_indices_by_key
                        .entry(key)
                        .or_default()
                        .push(inner_rows.len());
                }
                inner_rows.push(row);
            }
            for (key, indices) in row_indices_by_key {
                self.inner_row_indices_by_key
                    .insert(key, CompactArc::from(indices));
            }
            self.inner_rows = Some(CompactArc::new(inner_rows));
            self.observed_candidate_map_nanos = self.observed_candidate_map_nanos.saturating_add(
                candidate_map_started
                    .elapsed()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        check_query_cancelled(self.cancellation.as_ref())?;
        Ok(true)
    }

    fn combine_rows(&self, outer: RowRef, inner: RowRef) -> RowRef {
        if let Some(columns) = &self.projection_columns {
            RowRef::projected(outer, inner, CompactArc::clone(columns))
        } else {
            RowRef::Owned(Row::from_combined_owned(
                outer.into_owned(),
                inner.into_owned(),
            ))
        }
    }

    #[inline]
    fn take_current_outer(&mut self) -> RowRef {
        std::mem::replace(
            &mut self.outer_batch[self.outer_index],
            RowRef::Owned(Row::new()),
        )
    }

    fn null_inner_row(&self) -> Row {
        let width = self
            .inner_projection_indices
            .as_ref()
            .map_or(self.inner_col_count, Vec::len);
        Row::from_values((0..width).map(|_| NULL_VALUE).collect())
    }

    fn advance_outer(&mut self) {
        self.outer_index += 1;
        self.current_inner_index = 0;
        self.current_outer_had_match = false;
        self.current_non_unique_indices = None;
    }

    fn lookup_candidate(&mut self) -> (usize, Option<RowRef>) {
        if let Some(rows) = self.unique_inner_rows_by_key.as_ref() {
            let value = self.outer_batch[self.outer_index].get(self.outer_key_idx);
            let row = match self.lookup_strategy {
                IndexLookupStrategy::PrimaryKey => value
                    .and_then(exact_integer_pk_value)
                    .map(Value::Integer)
                    .as_ref()
                    .and_then(|key| rows.get(key)),
                _ => value
                    .filter(|value| !value.is_null())
                    .and_then(|key| rows.get(key)),
            };
            return (
                usize::from(row.is_some()),
                if self.current_inner_index == 0 {
                    row.map(|(rows, row_idx)| RowRef::shared(rows, row_idx))
                } else {
                    None
                },
            );
        }
        if self.current_non_unique_indices.is_none() {
            let value = self.outer_batch[self.outer_index].get(self.outer_key_idx);
            self.current_non_unique_indices = match self.lookup_strategy {
                IndexLookupStrategy::PrimaryKey => value
                    .and_then(exact_integer_pk_value)
                    .map(Value::Integer)
                    .as_ref()
                    .and_then(|key| self.inner_row_indices_by_key.get(key)),
                _ => value
                    .filter(|value| !value.is_null())
                    .and_then(|key| self.inner_row_indices_by_key.get(key)),
            }
            .cloned();
        }
        let candidates = self.current_non_unique_indices.as_ref();
        (
            candidates.map_or(0, |indices| indices.len()),
            candidates.and_then(|indices| {
                let row_idx = *indices.get(self.current_inner_index)?;
                Some(RowRef::shared(
                    CompactArc::clone(self.inner_rows.as_ref()?),
                    row_idx,
                ))
            }),
        )
    }
}

fn lookup_disappeared_error(lookup_strategy: &IndexLookupStrategy) -> radixdb_core::Error {
    match lookup_strategy {
        IndexLookupStrategy::SegmentedSecondaryIndex { column_name, .. } => {
            radixdb_core::Error::internal(format!(
                "segmented batch join index coverage disappeared for column {column_name}"
            ))
        }
        _ => radixdb_core::Error::internal(
            "batch index nested-loop selected lookup disappeared during execution",
        ),
    }
}

impl Operator for BatchIndexNestedLoopJoinOperator {
    fn open(&mut self) -> Result<()> {
        self.reset_metrics();
        self.outer_exhausted = false;
        check_query_cancelled(self.cancellation.as_ref())?;
        if let Some(message) = self.projection_error.take() {
            return Err(radixdb_core::Error::invalid_argument(message));
        }
        if let Err(error) = self.outer.open() {
            let _ = self.outer.close();
            return Err(error);
        }
        if let Err(error) = self.load_outer_batch() {
            let _ = self.outer.close();
            return Err(error);
        }
        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        check_query_cancelled(self.cancellation.as_ref())?;
        if !self.opened {
            return Err(radixdb_core::Error::internal(
                "BatchIndexNestedLoopJoinOperator::next called before open",
            ));
        }
        let is_left_outer = matches!(self.join_type, JoinType::Left | JoinType::Full);

        loop {
            if self.outer_index >= self.outer_batch.len()
                && (self.outer_exhausted || !self.load_outer_batch()?)
            {
                return Ok(None);
            }

            let (candidate_count, candidate) = self.lookup_candidate();

            if let Some(inner) = candidate {
                self.current_inner_index += 1;
                let passes = self.residual_filter.as_ref().map_or(Ok(true), |filter| {
                    filter.matches_row_refs_checked(&self.outer_batch[self.outer_index], &inner)
                })?;
                if passes {
                    self.current_outer_had_match = true;
                    let outer = if self.current_inner_index >= candidate_count {
                        let outer = self.take_current_outer();
                        self.advance_outer();
                        outer
                    } else {
                        self.outer_batch[self.outer_index].clone()
                    };
                    let output = self.combine_rows(outer, inner);
                    self.observed_output_rows = self.observed_output_rows.saturating_add(1);
                    self.observed_deferred_output_rows = self
                        .observed_deferred_output_rows
                        .saturating_add(u64::from(output.is_deferred()));
                    return Ok(Some(output));
                }
                continue;
            }

            if is_left_outer && !self.current_outer_had_match {
                let outer = self.take_current_outer();
                let output = self.combine_rows(outer, RowRef::Owned(self.null_inner_row()));
                self.advance_outer();
                self.observed_output_rows = self.observed_output_rows.saturating_add(1);
                self.observed_deferred_output_rows = self
                    .observed_deferred_output_rows
                    .saturating_add(u64::from(output.is_deferred()));
                return Ok(Some(output));
            }
            self.advance_outer();
        }
    }

    fn close(&mut self) -> Result<()> {
        let result = self.outer.close();
        self.publish_metrics();
        self.outer_batch.clear();
        self.current_non_unique_indices = None;
        self.unique_inner_rows_by_key = None;
        self.inner_rows = None;
        self.inner_row_indices_by_key.clear();
        self.opened = false;
        result
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.outer.estimated_rows()
    }

    fn ordering(&self) -> OrderingProperty {
        self.output_ordering.clone()
    }

    fn name(&self) -> &str {
        "BatchIndexNL"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::MaterializedOperator;
    use radixdb_storage::mvcc::engine::MVCCEngine;
    use radixdb_storage::traits::Engine;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    struct CountingOuter {
        rows: Vec<Row>,
        index: usize,
        consumed: Arc<AtomicUsize>,
        schema: Vec<ColumnInfo>,
    }

    impl Operator for CountingOuter {
        fn open(&mut self) -> Result<()> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<RowRef>> {
            let Some(row) = self.rows.get(self.index).cloned() else {
                return Ok(None);
            };
            self.index += 1;
            self.consumed.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(Some(RowRef::Owned(row)))
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn schema(&self) -> &[ColumnInfo] {
            &self.schema
        }

        fn name(&self) -> &str {
            "CountingOuter"
        }
    }

    #[test]
    fn remap_inner_projection_columns_keeps_outer_and_compacts_inner_columns() {
        let columns = vec![
            ColumnSource::Outer(1),
            ColumnSource::Inner(3),
            ColumnSource::Inner(1),
            ColumnSource::Outer(0),
            ColumnSource::Inner(3),
        ];

        let (remapped, inner_indices) = remap_inner_projection_columns(&columns);

        assert_eq!(
            remapped,
            vec![
                ColumnSource::Outer(1),
                ColumnSource::Inner(0),
                ColumnSource::Inner(1),
                ColumnSource::Outer(0),
                ColumnSource::Inner(0),
            ]
        );
        assert_eq!(inner_indices, vec![3, 1]);
    }

    fn make_outer_operator() -> Box<dyn Operator> {
        Box::new(MaterializedOperator::new(
            vec![
                Row::from_values(vec![Value::integer(1), Value::integer(10)]),
                Row::from_values(vec![Value::integer(3), Value::integer(30)]),
            ],
            vec![ColumnInfo::new("outer_id"), ColumnInfo::new("outer_value")],
        ))
    }

    fn make_inner_table() -> (Arc<MVCCEngine>, Box<dyn Table>) {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let executor = crate::Executor::new(Arc::clone(&engine));
        executor
            .execute(
                "CREATE TABLE inner_join_target (id INTEGER PRIMARY KEY, data INTEGER NOT NULL, unused INTEGER NOT NULL)",
            )
            .unwrap();
        drop(executor);

        {
            let mut tx = engine.begin_transaction().unwrap();
            let mut table = tx.get_table("inner_join_target").unwrap();
            table
                .insert(Row::from_values(vec![
                    Value::integer(1),
                    Value::integer(100),
                    Value::integer(1000),
                ]))
                .unwrap();
            table
                .insert(Row::from_values(vec![
                    Value::integer(2),
                    Value::integer(200),
                    Value::integer(2000),
                ]))
                .unwrap();
            table
                .insert(Row::from_values(vec![
                    Value::integer(3),
                    Value::integer(300),
                    Value::integer(3000),
                ]))
                .unwrap();
            tx.commit().unwrap();
        }

        let tx = engine.begin_transaction().unwrap();
        let table = tx.get_table("inner_join_target").unwrap();
        (engine, table)
    }

    fn projected_schema() -> Vec<ColumnInfo> {
        vec![ColumnInfo::new("outer_value"), ColumnInfo::new("data")]
    }

    fn collect_operator_rows(op: &mut dyn Operator) -> Vec<Row> {
        let mut rows = Vec::new();
        op.open().unwrap();
        while let Some(row_ref) = op.next().unwrap() {
            rows.push(row_ref.into_owned());
        }
        op.close().unwrap();
        rows
    }

    #[test]
    fn streaming_index_nested_loop_projection_returns_requested_columns_only() {
        let (_engine, inner_table) = make_inner_table();
        let inner_schema = vec![
            ColumnInfo::new("id"),
            ColumnInfo::new("data"),
            ColumnInfo::new("unused"),
        ];
        let mut join = IndexNestedLoopJoinOperator::new(
            make_outer_operator(),
            inner_table,
            inner_schema,
            JoinType::Inner,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        )
        .with_projection(
            vec![ColumnSource::Outer(1), ColumnSource::Inner(1)],
            projected_schema(),
        );

        let before = instrumentation::snapshot();
        let rows = collect_operator_rows(&mut join);
        let after = instrumentation::snapshot();

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.len() == 2));
        assert_eq!(rows[0].get(0), Some(&Value::integer(10)));
        assert_eq!(rows[0].get(1), Some(&Value::integer(100)));
        assert_eq!(rows[1].get(0), Some(&Value::integer(30)));
        assert_eq!(rows[1].get(1), Some(&Value::integer(300)));
        assert!(
            after.join_index_nested_loop_calls
                >= before.join_index_nested_loop_calls.saturating_add(1)
        );
        assert!(after.join_lookup_calls >= before.join_lookup_calls.saturating_add(2));
        assert!(
            after.join_lookup_candidate_rows >= before.join_lookup_candidate_rows.saturating_add(2)
        );
        assert!(after.join_output_rows >= before.join_output_rows.saturating_add(2));
    }

    #[test]
    fn streaming_index_nested_loop_observes_a_pre_cancelled_request() {
        let (_engine, inner_table) = make_inner_table();
        let context = crate::context::ExecutionContext::new();
        let cancellation = context.cancellation_handle();
        cancellation.cancel();
        let mut join = IndexNestedLoopJoinOperator::new(
            make_outer_operator(),
            inner_table,
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
            JoinType::Inner,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        )
        .with_cancellation(cancellation);

        let before = INDEX_NL_CANCELLATION_OBSERVED.load(AtomicOrdering::Relaxed);
        assert!(matches!(
            join.open(),
            Err(radixdb_core::Error::QueryCancelled)
        ));
        assert!(
            INDEX_NL_CANCELLATION_OBSERVED.load(AtomicOrdering::Relaxed) > before,
            "index nested-loop operator did not observe its cancellation handle"
        );
    }

    #[test]
    fn primary_key_lookup_admits_only_exact_integer_domain_keys() {
        let (_engine, inner_table) = make_inner_table();
        let outer = Box::new(MaterializedOperator::new(
            vec![
                Row::from_values(vec![Value::Float(0.5)]),
                Row::from_values(vec![Value::Float(1.0)]),
                Row::from_values(vec![Value::Integer(3)]),
            ],
            vec![ColumnInfo::new("outer_key")],
        ));
        let mut join = IndexNestedLoopJoinOperator::new(
            outer,
            inner_table,
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
            JoinType::Inner,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        )
        .with_projection(
            vec![ColumnSource::Outer(0), ColumnSource::Inner(1)],
            vec![ColumnInfo::new("outer_key"), ColumnInfo::new("data")],
        );

        let rows = collect_operator_rows(&mut join);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0), Some(&Value::Float(1.0)));
        assert_eq!(rows[0].get(1), Some(&Value::Integer(100)));
        assert_eq!(rows[1].get(0), Some(&Value::Integer(3)));
        assert_eq!(rows[1].get(1), Some(&Value::Integer(300)));
    }

    #[test]
    fn public_index_nested_loop_rejects_invalid_projection_before_lookup() {
        let (_engine, inner_table) = make_inner_table();
        let mut join = IndexNestedLoopJoinOperator::new(
            make_outer_operator(),
            inner_table,
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
            JoinType::Inner,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        )
        .with_projection(
            vec![ColumnSource::Inner(3)],
            vec![ColumnInfo::new("invalid")],
        );

        assert!(join.open().is_err());
    }

    #[test]
    fn batch_index_nested_loop_fetches_multiple_terminal_columns_once() {
        let (_engine, inner_table) = make_inner_table();
        let inner_schema = vec![
            ColumnInfo::new("id"),
            ColumnInfo::new("data"),
            ColumnInfo::new("unused"),
        ];
        let mut join = BatchIndexNestedLoopJoinOperator::new(
            make_outer_operator(),
            inner_table,
            inner_schema,
            JoinType::Inner,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        )
        .with_projection(
            vec![
                ColumnSource::Outer(1),
                ColumnSource::Inner(1),
                ColumnSource::Inner(2),
            ],
            vec![
                ColumnInfo::new("outer_value"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
        );

        assert_eq!(join.inner_projection_indices, Some(vec![1, 2, 0]));
        instrumentation::begin_join_execution_probe();
        let mut rows = collect_operator_rows(&mut join);
        let probe = instrumentation::end_join_execution_probe();
        rows.sort_by_key(|row| row.get(0).and_then(Value::as_int64).unwrap_or_default());

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.len() == 3));
        assert_eq!(rows[0].get(0), Some(&Value::integer(10)));
        assert_eq!(rows[0].get(1), Some(&Value::integer(100)));
        assert_eq!(rows[0].get(2), Some(&Value::integer(1000)));
        assert_eq!(rows[1].get(0), Some(&Value::integer(30)));
        assert_eq!(rows[1].get(1), Some(&Value::integer(300)));
        assert_eq!(rows[1].get(2), Some(&Value::integer(3000)));
        assert_eq!(probe.lookup_calls, 1);
        assert_eq!(probe.lookup_candidate_rows, 2);
    }

    #[test]
    fn batch_index_nested_loop_keeps_projection_deferred_across_edges() {
        let (engine, first_inner) = make_inner_table();
        let first = BatchIndexNestedLoopJoinOperator::new(
            make_outer_operator(),
            first_inner,
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
            JoinType::Inner,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        )
        .with_projection(
            vec![
                ColumnSource::Outer(0),
                ColumnSource::Outer(1),
                ColumnSource::Inner(1),
            ],
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("outer_value"),
                ColumnInfo::new("first_data"),
            ],
        );

        let tx = engine.begin_transaction().unwrap();
        let second_inner = tx.get_table("inner_join_target").unwrap();
        let mut second = BatchIndexNestedLoopJoinOperator::new(
            Box::new(first),
            second_inner,
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
            JoinType::Inner,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        )
        .with_projection(
            vec![
                ColumnSource::Outer(1),
                ColumnSource::Outer(2),
                ColumnSource::Inner(2),
            ],
            vec![
                ColumnInfo::new("outer_value"),
                ColumnInfo::new("first_data"),
                ColumnInfo::new("second_unused"),
            ],
        );

        second.open().unwrap();
        let first_output = second.next().unwrap().unwrap();
        assert!(first_output.is_deferred());
        assert_eq!(first_output.get(0), Some(&Value::integer(10)));
        assert_eq!(first_output.get(1), Some(&Value::integer(100)));
        assert_eq!(first_output.get(2), Some(&Value::integer(1000)));
        assert_eq!(first_output.into_owned().len(), 3);

        let second_output = second.next().unwrap().unwrap();
        assert!(second_output.is_deferred());
        assert_eq!(second_output.get(0), Some(&Value::integer(30)));
        assert_eq!(second_output.get(1), Some(&Value::integer(300)));
        assert_eq!(second_output.get(2), Some(&Value::integer(3000)));
        assert!(second.next().unwrap().is_none());
        second.close().unwrap();
        assert_eq!(
            second.observed_deferred_outer_rows, 2,
            "the second edge must consume both deferred rows from the first edge"
        );
        assert_eq!(
            second.observed_deferred_output_rows, 2,
            "the second edge must preserve both outputs as deferred rows"
        );
        assert_eq!(
            second
                .observed_output_rows
                .saturating_sub(second.observed_deferred_output_rows),
            0,
            "the second edge must not construct an owned output row"
        );
    }

    #[test]
    fn r8_l01_batch_h_batch_index_nl_does_not_drain_outer_in_open() {
        let (_engine, inner_table) = make_inner_table();
        let consumed = Arc::new(AtomicUsize::new(0));
        let outer = CountingOuter {
            rows: (0..200_000)
                .map(|index| Row::from_values(vec![Value::Integer(index)]))
                .collect(),
            index: 0,
            consumed: Arc::clone(&consumed),
            schema: vec![ColumnInfo::new("id")],
        };
        let mut join = BatchIndexNestedLoopJoinOperator::new(
            Box::new(outer),
            inner_table,
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
            JoinType::Left,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        );

        join.open().unwrap();
        assert_eq!(
            consumed.load(AtomicOrdering::Relaxed),
            BATCH_INDEX_NL_OUTER_ROWS
        );
        assert!(join.next().unwrap().is_some());
        assert!(consumed.load(AtomicOrdering::Relaxed) < 200_000);
        join.close().unwrap();
    }

    #[test]
    fn batch_index_nested_loop_bounds_wide_outer_rows_by_bytes() {
        let (_engine, inner_table) = make_inner_table();
        let consumed = Arc::new(AtomicUsize::new(0));
        let payload = "x".repeat(1024 * 1024);
        let outer = CountingOuter {
            rows: (0..128)
                .map(|index| {
                    Row::from_values(vec![Value::Integer(index), Value::text(payload.clone())])
                })
                .collect(),
            index: 0,
            consumed: Arc::clone(&consumed),
            schema: vec![ColumnInfo::new("id"), ColumnInfo::new("payload")],
        };
        let mut join = BatchIndexNestedLoopJoinOperator::new(
            Box::new(outer),
            inner_table,
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
            JoinType::Left,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        );

        join.open().unwrap();
        let retained: usize = join
            .outer_batch
            .iter()
            .map(RowRef::estimated_retained_bytes)
            .sum();
        assert!(retained >= BATCH_INDEX_NL_OUTER_BYTES);
        assert!(join.outer_batch.len() < 128);
        assert_eq!(
            consumed.load(AtomicOrdering::Relaxed),
            join.outer_batch.len()
        );
        join.close().unwrap();
    }

    #[test]
    fn batch_index_nested_loop_deduplicates_keys_inside_bounded_chunks() {
        let (_engine, inner_table) = make_inner_table();
        let outer = Box::new(MaterializedOperator::new(
            (0..1_000)
                .map(|_| Row::from_values(vec![Value::Integer(1)]))
                .collect(),
            vec![ColumnInfo::new("id")],
        ));
        let mut join = BatchIndexNestedLoopJoinOperator::new(
            outer,
            inner_table,
            vec![
                ColumnInfo::new("id"),
                ColumnInfo::new("data"),
                ColumnInfo::new("unused"),
            ],
            JoinType::Inner,
            0,
            IndexLookupStrategy::PrimaryKey,
            None,
        );

        instrumentation::begin_join_execution_probe();
        let rows = collect_operator_rows(&mut join);
        let probe = instrumentation::end_join_execution_probe();

        assert_eq!(rows.len(), 1_000);
        assert_eq!(probe.operator_calls, 1);
        assert_eq!(probe.batch_index_nested_loop_calls, 1);
        assert_eq!(probe.lookup_calls, 1);
        assert_eq!(probe.lookup_candidate_rows, 1);
        assert_eq!(probe.output_rows, 1_000);
    }
}
