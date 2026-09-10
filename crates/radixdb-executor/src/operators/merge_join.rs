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

//! Merge Join Operator for pre-sorted inputs.
//!
//! This operator implements merge join with O(N+M) complexity when both
//! inputs are already sorted on the join keys. It's optimal for:
//! - Joining tables that are physically sorted (e.g., clustered index)
//! - Joining results of ORDER BY queries
//! - Self-joins on sorted columns

use std::cmp::Ordering;

use crate::context::check_current_query_cancelled;
use crate::operator::{ColumnInfo, Operator, OrderingProperty, RowRef};
use crate::utils::compare_values;
use radixdb_core::value::NULL_VALUE;
use radixdb_core::{Result, Row, Value};

use super::hash_join::JoinType;

fn compare_rows_on_keys(
    left: &Row,
    right: &Row,
    left_key_indices: &[usize],
    right_key_indices: &[usize],
) -> Ordering {
    for (li, ri) in left_key_indices.iter().zip(right_key_indices.iter()) {
        let lv = left.get(*li).cloned().unwrap_or(NULL_VALUE);
        let rv = right.get(*ri).cloned().unwrap_or(NULL_VALUE);

        // SQL equality join keys containing NULL never match. Returning a
        // stable ordering still lets the merge cursors make progress.
        if lv.is_null() && rv.is_null() {
            return Ordering::Less;
        }
        if lv.is_null() {
            return Ordering::Greater;
        }
        if rv.is_null() {
            return Ordering::Less;
        }

        let cmp = compare_values(&lv, &rv);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn compare_same_side_keys(row1: &Row, row2: &Row, key_indices: &[usize]) -> Ordering {
    for &idx in key_indices {
        let v1 = row1.get(idx).cloned().unwrap_or(NULL_VALUE);
        let v2 = row2.get(idx).cloned().unwrap_or(NULL_VALUE);

        if v1.is_null() && v2.is_null() {
            continue;
        }
        if v1.is_null() {
            return Ordering::Greater;
        }
        if v2.is_null() {
            return Ordering::Less;
        }

        let cmp = compare_values(&v1, &v2);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

/// Conservative upper bound for the two duplicate-group Vec allocations.
/// Row payloads are moved from the already materialized inputs, not cloned;
/// the blocking state adds only Row slots. The factor of two covers geometric
/// Vec capacity growth.
fn merge_group_slot_bytes(rows: usize) -> Option<usize> {
    rows.checked_mul(std::mem::size_of::<Row>())?.checked_mul(2)
}

/// Merge Join Operator for pre-sorted inputs.
///
/// Both inputs must be sorted on their respective join keys.
/// The operator performs a single pass through both inputs,
/// producing matches as they're found.
pub struct MergeJoinOperator {
    // Input operators
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,

    // Join configuration
    join_type: JoinType,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,

    // Output schema
    schema: Vec<ColumnInfo>,
    left_col_count: usize,
    right_col_count: usize,

    // Ordered cursors. Only the current row and one duplicate-key group are
    // retained; complete inputs are never materialized by open().
    left_current: Option<Row>,
    right_current: Option<Row>,
    left_group: Vec<Row>,
    right_group: Vec<Row>,
    group_left_idx: usize,
    group_right_idx: usize,
    group_retained_rows: usize,
    max_group_rows: usize,
    max_group_bytes: usize,

    // Cached null rows for OUTER joins (avoid repeated allocation)
    cached_null_left: Vec<Value>,
    cached_null_right: Vec<Value>,

    // Physical property contract. Merge is invalid without both certificates.
    input_ordering_certified: bool,
    output_ordering: OrderingProperty,

    // State
    opened: bool,
}

impl MergeJoinOperator {
    /// Create a new merge join operator.
    ///
    /// # Arguments
    /// * `left` - Left input operator (must be sorted on left_key_indices)
    /// * `right` - Right input operator (must be sorted on right_key_indices)
    /// * `join_type` - Type of join (INNER, LEFT, RIGHT, FULL - NOT Cross)
    /// * `left_key_indices` - Column indices for left join keys
    /// * `right_key_indices` - Column indices for right join keys
    ///
    /// # Panics
    /// Debug builds will panic if `join_type` is `JoinType::Cross`.
    /// Cross joins should use NestedLoopJoinOperator instead.
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        join_type: JoinType,
        left_key_indices: Vec<usize>,
        right_key_indices: Vec<usize>,
    ) -> Self {
        // Cross joins should use NestedLoop, not MergeJoin (no keys to merge on)
        debug_assert!(
            !matches!(join_type, JoinType::Cross),
            "MergeJoin cannot be used for CROSS JOIN - use NestedLoopJoin instead"
        );

        let left_ordering = left.ordering();
        let right_ordering = right.ordering();
        let input_ordering_certified = left_ordering.proves_ascending_nulls_last(&left_key_indices)
            && right_ordering.proves_ascending_nulls_last(&right_key_indices);
        let output_ordering =
            if input_ordering_certified && matches!(join_type, JoinType::Inner | JoinType::Left) {
                left_ordering
            } else {
                OrderingProperty::Unknown
            };

        // Build combined schema
        let mut schema = Vec::new();
        schema.extend(left.schema().iter().cloned());
        schema.extend(right.schema().iter().cloned());

        let left_col_count = left.schema().len();
        let right_col_count = right.schema().len();

        Self {
            left,
            right,
            join_type,
            left_key_indices,
            right_key_indices,
            schema,
            left_col_count,
            right_col_count,
            left_current: None,
            right_current: None,
            left_group: Vec::new(),
            right_group: Vec::new(),
            group_left_idx: 0,
            group_right_idx: 0,
            group_retained_rows: 0,
            max_group_rows: crate::utils::RetainedRowsBudget::DEFAULT_MAX_ROWS,
            max_group_bytes: crate::utils::RetainedRowsBudget::DEFAULT_MAX_BYTES,
            cached_null_left: Vec::new(),  // Initialized in open()
            cached_null_right: Vec::new(), // Initialized in open()
            input_ordering_certified,
            output_ordering,
            opened: false,
        }
    }

    /// Bound the only blocking state retained by streaming merge: the two
    /// matching duplicate-key groups. Production planning preflights the same
    /// limits and falls back to hash before execution when they cannot fit.
    pub(crate) fn with_group_budget(mut self, max_rows: usize, max_bytes: usize) -> Self {
        self.max_group_rows = max_rows;
        self.max_group_bytes = max_bytes;
        self
    }

    /// Prove that every *matching* duplicate-key group fits the merge owner.
    /// Small relations are admitted from cardinality alone in O(1). Only an
    /// oversized merge candidate pays the linear key-only safety scan.
    pub(crate) fn matching_groups_retained_bytes(
        left_rows: &[Row],
        right_rows: &[Row],
        left_key_indices: &[usize],
        right_key_indices: &[usize],
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Option<usize>> {
        if left_key_indices.is_empty() || left_key_indices.len() != right_key_indices.len() {
            return Ok(None);
        }

        let total_rows = left_rows.len().checked_add(right_rows.len());
        if let Some(total_rows) = total_rows {
            if total_rows <= max_rows {
                if let Some(bytes) = merge_group_slot_bytes(total_rows) {
                    if bytes <= max_bytes {
                        return Ok(Some(bytes));
                    }
                }
            }
        }

        let mut left_idx = 0;
        let mut right_idx = 0;
        let mut peak_group_bytes = 0;
        while left_idx < left_rows.len() && right_idx < right_rows.len() {
            if (left_idx.wrapping_add(right_idx)) & 0xff == 0 {
                check_current_query_cancelled()?;
            }
            match compare_rows_on_keys(
                &left_rows[left_idx],
                &right_rows[right_idx],
                left_key_indices,
                right_key_indices,
            ) {
                Ordering::Less => left_idx += 1,
                Ordering::Greater => right_idx += 1,
                Ordering::Equal => {
                    let left_start = left_idx;
                    let right_start = right_idx;
                    left_idx += 1;
                    while left_idx < left_rows.len()
                        && compare_same_side_keys(
                            &left_rows[left_start],
                            &left_rows[left_idx],
                            left_key_indices,
                        ) == Ordering::Equal
                    {
                        left_idx += 1;
                    }
                    right_idx += 1;
                    while right_idx < right_rows.len()
                        && compare_same_side_keys(
                            &right_rows[right_start],
                            &right_rows[right_idx],
                            right_key_indices,
                        ) == Ordering::Equal
                    {
                        right_idx += 1;
                    }

                    let group_rows =
                        (left_idx - left_start).saturating_add(right_idx - right_start);
                    if group_rows > max_rows
                        || merge_group_slot_bytes(group_rows).is_none_or(|bytes| bytes > max_bytes)
                    {
                        return Ok(None);
                    }
                    peak_group_bytes = peak_group_bytes.max(
                        merge_group_slot_bytes(group_rows)
                            .expect("admitted merge group must have addressable slot bytes"),
                    );
                }
            }
        }
        Ok(Some(peak_group_bytes))
    }

    #[cfg(test)]
    pub(crate) fn matching_groups_fit_budget(
        left_rows: &[Row],
        right_rows: &[Row],
        left_key_indices: &[usize],
        right_key_indices: &[usize],
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<bool> {
        Self::matching_groups_retained_bytes(
            left_rows,
            right_rows,
            left_key_indices,
            right_key_indices,
            max_rows,
            max_bytes,
        )
        .map(|bytes| bytes.is_some())
    }

    /// Compare two rows on their respective join keys.
    fn compare_on_keys(&self, left: &Row, right: &Row) -> Ordering {
        compare_rows_on_keys(left, right, &self.left_key_indices, &self.right_key_indices)
    }

    /// Compare two rows from the same side on their keys.
    fn compare_same_side(&self, row1: &Row, row2: &Row, key_indices: &[usize]) -> Ordering {
        compare_same_side_keys(row1, row2, key_indices)
    }

    fn admit_group_row(&mut self) -> Result<()> {
        let next_rows = self.group_retained_rows.saturating_add(1);
        let next_bytes = merge_group_slot_bytes(next_rows).unwrap_or(usize::MAX);
        if next_rows > self.max_group_rows || next_bytes > self.max_group_bytes {
            return Err(radixdb_core::Error::invalid_argument(format!(
                "merge join duplicate group exceeds memory budget (rows {next_rows}/{}, slot bytes {next_bytes}/{})",
                self.max_group_rows, self.max_group_bytes
            )));
        }
        self.group_retained_rows = next_rows;
        Ok(())
    }

    /// Create a NULL row for the left side (uses cached values).
    #[inline]
    fn null_left_row(&self) -> Row {
        Row::from_values(self.cached_null_left.clone())
    }

    /// Create a NULL row for the right side (uses cached values).
    #[inline]
    fn null_right_row(&self) -> Row {
        Row::from_values(self.cached_null_right.clone())
    }

    /// Combine left and right rows into output row.
    #[inline]
    fn combine(&self, left: &Row, right: &Row) -> Row {
        Row::from_combined(left, right)
    }

    fn pull_left(&mut self) -> Result<Option<Row>> {
        self.left.next().map(|row| row.map(RowRef::into_owned))
    }

    fn pull_right(&mut self) -> Result<Option<Row>> {
        self.right.next().map(|row| row.map(RowRef::into_owned))
    }

    /// Consume the complete duplicate-key groups starting at both current
    /// cursors. The first non-matching row remains as the next cursor value.
    fn collect_equal_groups(&mut self) -> Result<()> {
        self.left_group.clear();
        self.right_group.clear();
        self.group_left_idx = 0;
        self.group_right_idx = 0;
        self.group_retained_rows = 0;

        self.admit_group_row()?;
        self.left_group.push(
            self.left_current
                .take()
                .expect("equal merge key without left cursor"),
        );
        self.admit_group_row()?;
        self.right_group.push(
            self.right_current
                .take()
                .expect("equal merge key without right cursor"),
        );

        loop {
            if self.left_group.len() & 0xff == 0 {
                check_current_query_cancelled()?;
            }
            match self.pull_left()? {
                Some(row)
                    if self.compare_same_side(
                        &row,
                        &self.left_group[0],
                        &self.left_key_indices,
                    ) == Ordering::Equal =>
                {
                    self.admit_group_row()?;
                    self.left_group.push(row);
                }
                next => {
                    self.left_current = next;
                    break;
                }
            }
        }

        loop {
            if self.right_group.len() & 0xff == 0 {
                check_current_query_cancelled()?;
            }
            match self.pull_right()? {
                Some(row)
                    if self.compare_same_side(
                        &row,
                        &self.right_group[0],
                        &self.right_key_indices,
                    ) == Ordering::Equal =>
                {
                    self.admit_group_row()?;
                    self.right_group.push(row);
                }
                next => {
                    self.right_current = next;
                    break;
                }
            }
        }
        Ok(())
    }

    fn emit_group_match(&mut self) -> Option<RowRef> {
        if self.group_left_idx >= self.left_group.len()
            || self.group_right_idx >= self.right_group.len()
        {
            return None;
        }

        let row = self.combine(
            &self.left_group[self.group_left_idx],
            &self.right_group[self.group_right_idx],
        );
        self.group_right_idx += 1;
        if self.group_right_idx == self.right_group.len() {
            self.group_right_idx = 0;
            self.group_left_idx += 1;
            if self.group_left_idx == self.left_group.len() {
                self.left_group.clear();
                self.right_group.clear();
                self.group_left_idx = 0;
                self.group_retained_rows = 0;
            }
        }
        Some(RowRef::Owned(row))
    }
}

impl Operator for MergeJoinOperator {
    fn open(&mut self) -> Result<()> {
        if !self.input_ordering_certified {
            return Err(radixdb_core::Error::invalid_argument(
                "merge join requires explicit ascending NULLS LAST ordering certificates",
            ));
        }
        if matches!(
            self.join_type,
            JoinType::Cross | JoinType::Semi | JoinType::Anti
        ) {
            return Err(radixdb_core::Error::invalid_argument(
                "merge join supports only INNER, LEFT, RIGHT and FULL joins",
            ));
        }

        if let Err(error) = self.left.open() {
            let _ = self.left.close();
            return Err(error);
        }
        if let Err(error) = self.right.open() {
            let _ = self.right.close();
            let _ = self.left.close();
            return Err(error);
        }

        let open_result = (|| {
            check_current_query_cancelled()?;

            // Pre-cache null rows for OUTER joins (avoids repeated allocation)
            // NULL_VALUE is a static constant, cloning Vec is just memcpy
            if matches!(
                self.join_type,
                JoinType::Left | JoinType::Right | JoinType::Full
            ) {
                self.cached_null_left = vec![NULL_VALUE; self.left_col_count];
                self.cached_null_right = vec![NULL_VALUE; self.right_col_count];
            }

            self.left_group.clear();
            self.right_group.clear();
            self.group_left_idx = 0;
            self.group_right_idx = 0;
            self.group_retained_rows = 0;
            self.left_current = self.pull_left()?;
            self.right_current = self.pull_right()?;

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
                "MergeJoinOperator::next called before open",
            ));
        }

        let is_left_outer = matches!(self.join_type, JoinType::Left | JoinType::Full);
        let is_right_outer = matches!(self.join_type, JoinType::Right | JoinType::Full);

        if let Some(row) = self.emit_group_match() {
            return Ok(Some(row));
        }

        loop {
            check_current_query_cancelled()?;

            match (&self.left_current, &self.right_current) {
                (None, None) => return Ok(None),
                (None, Some(_)) => {
                    if !is_right_outer {
                        return Ok(None);
                    }
                    let right = self
                        .right_current
                        .take()
                        .expect("right cursor disappeared during outer emission");
                    self.right_current = self.pull_right()?;
                    let null_left = self.null_left_row();
                    return Ok(Some(RowRef::Owned(self.combine(&null_left, &right))));
                }
                (Some(_), None) => {
                    if !is_left_outer {
                        return Ok(None);
                    }
                    let left = self
                        .left_current
                        .take()
                        .expect("left cursor disappeared during outer emission");
                    self.left_current = self.pull_left()?;
                    let null_right = self.null_right_row();
                    return Ok(Some(RowRef::Owned(self.combine(&left, &null_right))));
                }
                (Some(_), Some(_)) => {}
            }

            let ordering = self.compare_on_keys(
                self.left_current.as_ref().expect("left cursor missing"),
                self.right_current.as_ref().expect("right cursor missing"),
            );
            match ordering {
                Ordering::Less => {
                    let left = self.left_current.take().expect("left cursor missing");
                    self.left_current = self.pull_left()?;
                    if is_left_outer {
                        let null_right = self.null_right_row();
                        return Ok(Some(RowRef::Owned(self.combine(&left, &null_right))));
                    }
                }
                Ordering::Greater => {
                    let right = self.right_current.take().expect("right cursor missing");
                    self.right_current = self.pull_right()?;
                    if is_right_outer {
                        let null_left = self.null_left_row();
                        return Ok(Some(RowRef::Owned(self.combine(&null_left, &right))));
                    }
                }
                Ordering::Equal => {
                    self.collect_equal_groups()?;
                    return Ok(self.emit_group_match());
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.left_current = None;
        self.right_current = None;
        self.left_group.clear();
        self.right_group.clear();
        self.group_retained_rows = 0;
        self.opened = false;
        let left = self.left.close();
        let right = self.right.close();
        left.and(right)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        let left_est = self.left.estimated_rows()?;
        let right_est = self.right.estimated_rows()?;

        Some(match self.join_type {
            JoinType::Inner => left_est.min(right_est),
            JoinType::Left => left_est,
            JoinType::Right => right_est,
            JoinType::Full => left_est + right_est,
            JoinType::Cross => left_est * right_est,
            JoinType::Semi => left_est.min(right_est),
            JoinType::Anti => left_est,
        })
    }

    fn ordering(&self) -> OrderingProperty {
        self.output_ordering.clone()
    }

    fn name(&self) -> &str {
        match self.join_type {
            JoinType::Inner => "MergeJoin (INNER)",
            JoinType::Left => "MergeJoin (LEFT)",
            JoinType::Right => "MergeJoin (RIGHT)",
            JoinType::Full => "MergeJoin (FULL)",
            JoinType::Cross => "MergeJoin (CROSS)",
            JoinType::Semi => "MergeJoin (SEMI)",
            JoinType::Anti => "MergeJoin (ANTI)",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::MaterializedOperator;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    struct CountingOrderedOperator {
        rows: Vec<Option<Row>>,
        schema: Vec<ColumnInfo>,
        next_index: usize,
        next_calls: Arc<AtomicUsize>,
    }

    impl CountingOrderedOperator {
        fn new(rows: Vec<Row>, next_calls: Arc<AtomicUsize>) -> Self {
            Self {
                rows: rows.into_iter().map(Some).collect(),
                schema: vec![ColumnInfo::new("id")],
                next_index: 0,
                next_calls,
            }
        }
    }

    impl Operator for CountingOrderedOperator {
        fn open(&mut self) -> Result<()> {
            self.next_index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<RowRef>> {
            self.next_calls.fetch_add(1, AtomicOrdering::Relaxed);
            let Some(row) = self.rows.get_mut(self.next_index) else {
                return Ok(None);
            };
            self.next_index += 1;
            Ok(row.take().map(RowRef::Owned))
        }

        fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn schema(&self) -> &[ColumnInfo] {
            &self.schema
        }

        fn ordering(&self) -> OrderingProperty {
            OrderingProperty::ascending_nulls_last(vec![0])
        }

        fn name(&self) -> &str {
            "CountingOrdered"
        }
    }

    fn make_rows(data: Vec<Vec<i64>>) -> Vec<Row> {
        data.into_iter()
            .map(|vals| Row::from_values(vals.into_iter().map(Value::integer).collect()))
            .collect()
    }

    fn make_operator(data: Vec<Vec<i64>>, cols: Vec<&str>) -> Box<dyn Operator> {
        let rows = make_rows(data);
        let schema = cols.into_iter().map(ColumnInfo::new).collect();
        Box::new(
            MaterializedOperator::new(rows, schema)
                .with_ordering(OrderingProperty::ascending_nulls_last(vec![0])),
        )
    }

    fn make_value_operator(rows: Vec<Row>, cols: Vec<&str>) -> Box<dyn Operator> {
        let schema = cols.into_iter().map(ColumnInfo::new).collect();
        Box::new(
            MaterializedOperator::new(rows, schema)
                .with_ordering(OrderingProperty::ascending_nulls_last(vec![0])),
        )
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

    #[test]
    fn test_inner_merge_join() {
        // Both sides sorted on id
        let left = make_operator(
            vec![vec![1, 10], vec![2, 20], vec![3, 30]],
            vec!["id", "value"],
        );
        let right = make_operator(vec![vec![1, 100], vec![3, 300]], vec!["id", "data"]);

        let mut join = MergeJoinOperator::new(
            left,
            right,
            JoinType::Inner,
            vec![0], // left key: id
            vec![0], // right key: id
        );
        assert_eq!(
            join.ordering(),
            OrderingProperty::ascending_nulls_last(vec![0])
        );

        let results = collect_results(&mut join).unwrap();

        // Should have 2 matches: id=1 and id=3
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn open_prefetches_only_one_row_per_ordered_input() {
        let left_calls = Arc::new(AtomicUsize::new(0));
        let right_calls = Arc::new(AtomicUsize::new(0));
        let rows = (0..10_000)
            .map(|value| Row::from_values(vec![Value::Integer(value)]))
            .collect::<Vec<_>>();
        let left = Box::new(CountingOrderedOperator::new(
            rows.clone(),
            Arc::clone(&left_calls),
        ));
        let right = Box::new(CountingOrderedOperator::new(rows, Arc::clone(&right_calls)));
        let mut join = MergeJoinOperator::new(left, right, JoinType::Inner, vec![0], vec![0]);

        join.open().unwrap();
        assert_eq!(left_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(right_calls.load(AtomicOrdering::Relaxed), 1);
        assert!(join.next().unwrap().is_some());
        assert_eq!(left_calls.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(right_calls.load(AtomicOrdering::Relaxed), 2);
        join.close().unwrap();
    }

    #[test]
    fn merge_join_rejects_inputs_without_physical_ordering_certificate() {
        let schema = vec![ColumnInfo::new("id")];
        let left = Box::new(MaterializedOperator::new(
            make_rows(vec![vec![1], vec![2]]),
            schema.clone(),
        ));
        let right = Box::new(MaterializedOperator::new(
            make_rows(vec![vec![1], vec![2]]),
            schema,
        ));
        let mut join = MergeJoinOperator::new(left, right, JoinType::Inner, vec![0], vec![0]);

        let error = join.open().unwrap_err();
        assert!(error.to_string().contains("ordering certificates"));
    }

    #[test]
    fn test_left_merge_join() {
        let left = make_operator(
            vec![vec![1, 10], vec![2, 20], vec![3, 30]],
            vec!["id", "value"],
        );
        let right = make_operator(vec![vec![1, 100]], vec!["id", "data"]);

        let mut join = MergeJoinOperator::new(left, right, JoinType::Left, vec![0], vec![0]);

        let results = collect_results(&mut join).unwrap();

        // All 3 left rows should be preserved
        assert_eq!(results.len(), 3);

        // Check that id=2 and id=3 have NULLs on right side
        let row2 = results
            .iter()
            .find(|r| r.get(0) == Some(&Value::integer(2)))
            .unwrap();
        assert!(row2.get(2).unwrap().is_null());
    }

    #[test]
    fn streaming_right_and_full_merge_emit_unmatched_rows_once() {
        let make_left = || make_operator(vec![vec![1, 10], vec![3, 30]], vec!["id", "value"]);
        let make_right = || {
            make_operator(
                vec![vec![2, 200], vec![3, 300], vec![4, 400]],
                vec!["id", "data"],
            )
        };

        let mut right =
            MergeJoinOperator::new(make_left(), make_right(), JoinType::Right, vec![0], vec![0]);
        let right_rows = collect_results(&mut right).unwrap();
        assert_eq!(right_rows.len(), 3);
        assert_eq!(
            right_rows
                .iter()
                .filter(|row| row.get(0).is_some_and(Value::is_null))
                .count(),
            2
        );

        let mut full =
            MergeJoinOperator::new(make_left(), make_right(), JoinType::Full, vec![0], vec![0]);
        let full_rows = collect_results(&mut full).unwrap();
        assert_eq!(full_rows.len(), 4);
        assert_eq!(
            full_rows
                .iter()
                .filter(|row| row.get(0).is_some_and(Value::is_null))
                .count(),
            2
        );
        assert_eq!(
            full_rows
                .iter()
                .filter(|row| row.get(2).is_some_and(Value::is_null))
                .count(),
            1
        );
    }

    #[test]
    fn streaming_merge_never_matches_null_key_groups() {
        let left = make_value_operator(
            vec![
                Row::from_values(vec![Value::Integer(1)]),
                Row::from_values(vec![Value::Null(radixdb_core::DataType::Integer)]),
            ],
            vec!["key"],
        );
        let right = make_value_operator(
            vec![
                Row::from_values(vec![Value::Integer(1)]),
                Row::from_values(vec![Value::Null(radixdb_core::DataType::Integer)]),
            ],
            vec!["key"],
        );
        let mut join = MergeJoinOperator::new(left, right, JoinType::Inner, vec![0], vec![0]);

        let rows = collect_results(&mut join).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_merge_join_with_duplicates() {
        // Both sides have duplicate keys
        let left = make_operator(
            vec![vec![1, 10], vec![1, 11], vec![2, 20]],
            vec!["id", "value"],
        );
        let right = make_operator(
            vec![vec![1, 100], vec![1, 101], vec![2, 200]],
            vec!["id", "data"],
        );

        let mut join = MergeJoinOperator::new(left, right, JoinType::Inner, vec![0], vec![0]);

        let results = collect_results(&mut join).unwrap();

        // id=1: 2 left x 2 right = 4 matches
        // id=2: 1 left x 1 right = 1 match
        // Total = 5
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn matching_group_preflight_rejects_only_oversized_matching_groups() {
        let matching_left = make_rows((0..10).map(|_| vec![1]).collect());
        let matching_right = make_rows((0..10).map(|_| vec![1]).collect());
        let bytes = merge_group_slot_bytes(20).unwrap();
        assert!(!MergeJoinOperator::matching_groups_fit_budget(
            &matching_left,
            &matching_right,
            &[0],
            &[0],
            19,
            bytes,
        )
        .unwrap());
        assert!(MergeJoinOperator::matching_groups_fit_budget(
            &matching_left,
            &matching_right,
            &[0],
            &[0],
            20,
            bytes,
        )
        .unwrap());

        let unmatched_left = make_rows((0..100).map(|_| vec![1]).collect());
        let unmatched_right = make_rows((0..100).map(|_| vec![2]).collect());
        assert!(MergeJoinOperator::matching_groups_fit_budget(
            &unmatched_left,
            &unmatched_right,
            &[0],
            &[0],
            1,
            merge_group_slot_bytes(1).unwrap(),
        )
        .unwrap());
    }

    #[test]
    fn direct_merge_owner_fails_before_duplicate_group_can_exceed_budget() {
        let left = make_operator(vec![vec![1], vec![1]], vec!["id"]);
        let right = make_operator(vec![vec![1], vec![1]], vec!["id"]);
        let mut join = MergeJoinOperator::new(left, right, JoinType::Inner, vec![0], vec![0])
            .with_group_budget(3, usize::MAX);

        join.open().unwrap();
        let error = join.next().unwrap_err();
        assert!(error
            .to_string()
            .contains("duplicate group exceeds memory budget"));
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_uses_exact_numeric_and_nan_ordering() {
        const EXACT: i64 = 1_i64 << 53;
        let left = make_value_operator(
            vec![
                Row::from_values(vec![Value::Float(-0.0), Value::text("left-zero")]),
                Row::from_values(vec![Value::Integer(EXACT), Value::text("left-exact")]),
                Row::from_values(vec![
                    Value::Integer(EXACT + 1),
                    Value::text("left-neighbor"),
                ]),
                Row::from_values(vec![Value::Float(f64::NAN), Value::text("left-nan")]),
            ],
            vec!["key", "value"],
        );
        let right = make_value_operator(
            vec![
                Row::from_values(vec![Value::Integer(0), Value::text("right-zero")]),
                Row::from_values(vec![Value::Float(EXACT as f64), Value::text("right-exact")]),
                Row::from_values(vec![
                    Value::Float(f64::from_bits(0x7ff8_0000_0000_0042)),
                    Value::text("right-nan"),
                ]),
            ],
            vec!["key", "value"],
        );

        let mut join = MergeJoinOperator::new(left, right, JoinType::Inner, vec![0], vec![0]);
        let results = collect_results(&mut join).unwrap();

        assert_eq!(results.len(), 3);
        assert!(results
            .iter()
            .any(|row| row.get(1) == Some(&Value::text("left-zero"))));
        assert!(results
            .iter()
            .any(|row| row.get(1) == Some(&Value::text("left-exact"))));
        assert!(results
            .iter()
            .any(|row| row.get(1) == Some(&Value::text("left-nan"))));
        assert!(!results
            .iter()
            .any(|row| row.get(1) == Some(&Value::text("left-neighbor"))));
    }
}
