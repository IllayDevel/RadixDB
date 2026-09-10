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

//! Set operations execution (UNION, INTERSECT, EXCEPT)
//!
//! This module handles SQL set operations that combine results from multiple queries:
//! - UNION / UNION ALL
//! - INTERSECT / INTERSECT ALL
//! - EXCEPT / EXCEPT ALL

use radixdb_core::row_vec::RowVec;
use radixdb_core::{DataType, Error, Result};
use radixdb_sql::ast::{SelectStatement, SetOperation, SetOperationType};
use radixdb_storage::traits::QueryResult;
use rustc_hash::FxHashMap;

use crate::context::ExecutionContext;
use crate::result::ExecutorResult;
use crate::utils::{hash_row, rows_equal, RetainedRowsBudget};

fn materialize_set_operand(
    mut result: Box<dyn QueryResult>,
    ctx: &ExecutionContext,
    retained: &mut RetainedRowsBudget,
) -> Result<RowVec> {
    let mut rows = RowVec::with_capacity(result.estimated_count().unwrap_or(0).min(16_384));
    let mut row_id = 0_i64;
    while result.next() {
        if row_id & 0xff == 0 {
            ctx.check_cancelled()?;
        }
        let row = result.take_row();
        retained.admit(&row)?;
        rows.push((row_id, row));
        row_id += 1;
    }
    if let Some(error) = result.last_error() {
        return Err(error);
    }
    Ok(rows)
}

pub fn merge_set_type(left: DataType, right: DataType) -> Result<DataType> {
    if left == DataType::Null {
        return Ok(right);
    }
    if right == DataType::Null || left == right {
        return Ok(left);
    }
    let numeric = |data_type| {
        matches!(
            data_type,
            DataType::Integer | DataType::Float | DataType::Decimal
        )
    };
    if numeric(left) && numeric(right) {
        return Ok(if left == DataType::Decimal || right == DataType::Decimal {
            DataType::Decimal
        } else if left == DataType::Float || right == DataType::Float {
            DataType::Float
        } else {
            DataType::Integer
        });
    }
    Err(Error::Type(format!(
        "set-operation column has incompatible types {left} and {right}"
    )))
}

fn checkpoint_set_work(ctx: &ExecutionContext, work: &mut usize) -> Result<()> {
    *work = work.saturating_add(1);
    if *work & 0xff == 0 {
        ctx.check_cancelled()?;
    }
    Ok(())
}

fn bind_set_row_types(
    left: &mut RowVec,
    right: &mut RowVec,
    width: usize,
    ctx: &ExecutionContext,
    work: &mut usize,
) -> Result<()> {
    let mut target_types = vec![DataType::Null; width];
    for (_, row) in left.iter().chain(right.iter()) {
        checkpoint_set_work(ctx, work)?;
        for (column, value) in row.iter().enumerate() {
            target_types[column] = merge_set_type(target_types[column], value.data_type())?;
        }
    }
    for (_, row) in left.iter_mut().chain(right.iter_mut()) {
        checkpoint_set_work(ctx, work)?;
        for (column, target_type) in target_types.iter().copied().enumerate() {
            let Some(value) = row.get_mut(column) else {
                return Err(Error::internal(
                    "set-operation row width changed after binding",
                ));
            };
            if value.data_type() != target_type {
                *value = value.try_coerce_to_type(target_type)?;
            }
        }
    }
    Ok(())
}

/// Execute set operations (UNION, INTERSECT, EXCEPT).
///
/// The callback is the only upward port: each right operand re-enters the
/// statement owner without coupling the relational algebra to that owner.
pub fn execute_set_operations<F>(
    left_result: Box<dyn QueryResult>,
    set_ops: &[SetOperation],
    ctx: &ExecutionContext,
    _limit: Option<usize>,
    mut execute_right: F,
) -> Result<Box<dyn QueryResult>>
where
    F: FnMut(&SelectStatement, &ExecutionContext) -> Result<Box<dyn QueryResult>>,
{
    // Materialize before binding: all operands must share one output type
    // before DISTINCT/hash semantics or LIMIT can observe them.
    let columns = left_result.columns().to_vec();
    let mut retained = RetainedRowsBudget::new("set operation");
    let mut work = 0_usize;
    let mut result_rows = materialize_set_operand(left_result, ctx, &mut retained)?;

    // Process each set operation in sequence
    for set_op in set_ops {
        ctx.check_cancelled()?;
        // Execute the right side query with incremented depth (part of same logical query)
        let set_ctx = ctx.with_incremented_query_depth();
        let right_result = execute_right(&set_op.right, &set_ctx)?;

        // Validate column count matches (SQL standard requirement)
        let right_col_count = right_result.columns().len();
        let left_col_count = columns.len();
        if left_col_count != right_col_count {
            return Err(radixdb_core::Error::internal(format!(
                "each {} query must have the same number of columns: left has {}, right has {}",
                match &set_op.operation {
                    SetOperationType::Union | SetOperationType::UnionAll => "UNION",
                    SetOperationType::Intersect | SetOperationType::IntersectAll => "INTERSECT",
                    SetOperationType::Except | SetOperationType::ExceptAll => "EXCEPT",
                },
                left_col_count,
                right_col_count
            )));
        }
        let mut right_rows = materialize_set_operand(right_result, ctx, &mut retained)?;
        bind_set_row_types(
            &mut result_rows,
            &mut right_rows,
            left_col_count,
            ctx,
            &mut work,
        )?;

        // Apply the set operation
        match &set_op.operation {
            SetOperationType::Union => {
                // UNION: combine rows and remove duplicates with proper collision handling
                // Use hash map: hash -> list of indices to detect duplicates with collision handling
                let mut hash_to_indices: FxHashMap<u64, Vec<usize>> = FxHashMap::default();
                let mut unique_rows = RowVec::new();
                let mut row_id = 0i64;

                // Add left rows (dedup)
                for (_, row) in result_rows {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(&row);
                    let indices = hash_to_indices.entry(hash).or_default();

                    // Check if this exact row already exists (handle hash collisions)
                    let is_duplicate = indices
                        .iter()
                        .any(|&idx| rows_equal(&unique_rows[idx].1, &row));

                    if !is_duplicate {
                        indices.push(unique_rows.len());
                        unique_rows.push((row_id, row));
                        row_id += 1;
                    }
                }

                // Add right rows (dedup)
                for (_, row) in right_rows {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(&row);
                    let indices = hash_to_indices.entry(hash).or_default();

                    // Check if this exact row already exists (handle hash collisions)
                    let is_duplicate = indices
                        .iter()
                        .any(|&idx| rows_equal(&unique_rows[idx].1, &row));

                    if !is_duplicate {
                        indices.push(unique_rows.len());
                        unique_rows.push((row_id, row));
                        row_id += 1;
                    }
                }

                result_rows = unique_rows;
            }
            SetOperationType::UnionAll => {
                // UNION ALL: concatenate after common-type binding. Paging
                // remains owned by the outer logical query.
                for (row_id, (_, row)) in (result_rows.len() as i64..).zip(right_rows) {
                    checkpoint_set_work(ctx, &mut work)?;
                    result_rows.push((row_id, row));
                }
            }
            SetOperationType::Intersect => {
                // INTERSECT: keep only rows that exist in both (dedup) with proper collision handling

                // Build hash map: hash -> list of right rows with that hash
                let mut right_hash_map: FxHashMap<u64, Vec<usize>> = FxHashMap::default();
                for (idx, (_, row)) in right_rows.iter().enumerate() {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(row);
                    right_hash_map.entry(hash).or_default().push(idx);
                }

                // Track which left rows we've already added (for deduplication)
                let mut left_seen: FxHashMap<u64, Vec<usize>> = FxHashMap::default();
                let mut intersected_rows = RowVec::new();
                let mut row_id = 0i64;

                for (_, left_row) in result_rows {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(&left_row);

                    // Check if this hash exists in right side
                    if let Some(right_indices) = right_hash_map.get(&hash) {
                        // Check if any right row with this hash actually equals this left row
                        let has_match = right_indices
                            .iter()
                            .any(|&idx| rows_equal(&left_row, &right_rows[idx].1));

                        if has_match {
                            // Check if we've already added this left row (dedup)
                            let left_indices = left_seen.entry(hash).or_default();
                            let is_duplicate = left_indices
                                .iter()
                                .any(|&idx| rows_equal(&intersected_rows[idx].1, &left_row));

                            if !is_duplicate {
                                left_indices.push(intersected_rows.len());
                                intersected_rows.push((row_id, left_row));
                                row_id += 1;
                            }
                        }
                    }
                }

                result_rows = intersected_rows;
            }
            SetOperationType::IntersectAll => {
                // INTERSECT ALL: keep matching rows with multiplicity with proper collision handling

                // Build hash map: hash -> list of (row_index, remaining_count)
                // Each unique row in right side gets its own counter
                let mut right_hash_map: FxHashMap<u64, Vec<usize>> = FxHashMap::default();
                for (idx, (_, row)) in right_rows.iter().enumerate() {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(row);
                    right_hash_map.entry(hash).or_default().push(idx);
                }

                // For each unique right row, count how many times it appears
                let mut right_row_counts: FxHashMap<u64, Vec<(usize, usize)>> =
                    FxHashMap::default();
                for (hash, indices) in right_hash_map {
                    checkpoint_set_work(ctx, &mut work)?;
                    let mut unique_rows_in_bucket: Vec<(usize, usize)> = Vec::new();
                    for &idx in &indices {
                        // Find if this row already exists in unique_rows_in_bucket
                        if let Some(entry) =
                            unique_rows_in_bucket.iter_mut().find(|(rep_idx, _)| {
                                rows_equal(&right_rows[*rep_idx].1, &right_rows[idx].1)
                            })
                        {
                            entry.1 += 1; // Increment count
                        } else {
                            unique_rows_in_bucket.push((idx, 1)); // New unique row
                        }
                    }
                    right_row_counts.insert(hash, unique_rows_in_bucket);
                }

                let mut intersected_rows = RowVec::new();
                let mut row_id = 0i64;
                for (_, left_row) in result_rows {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(&left_row);

                    // Find matching right row and decrement its count
                    if let Some(bucket) = right_row_counts.get_mut(&hash) {
                        // Find the first matching row with count > 0
                        if let Some(entry) = bucket.iter_mut().find(|(rep_idx, count)| {
                            *count > 0 && rows_equal(&left_row, &right_rows[*rep_idx].1)
                        }) {
                            entry.1 -= 1; // Decrement count
                            intersected_rows.push((row_id, left_row));
                            row_id += 1;
                        }
                    }
                }

                result_rows = intersected_rows;
            }
            SetOperationType::Except => {
                // EXCEPT: keep left rows not in right (dedup) with proper collision handling

                // Build hash map: hash -> list of right row indices
                let mut right_hash_map: FxHashMap<u64, Vec<usize>> = FxHashMap::default();
                for (idx, (_, row)) in right_rows.iter().enumerate() {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(row);
                    right_hash_map.entry(hash).or_default().push(idx);
                }

                // Track which left rows we've already added (for deduplication)
                let mut left_seen: FxHashMap<u64, Vec<usize>> = FxHashMap::default();
                let mut excepted_rows = RowVec::new();
                let mut row_id = 0i64;

                for (_, left_row) in result_rows {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(&left_row);

                    // Check if this row exists in right side
                    let exists_in_right = if let Some(right_indices) = right_hash_map.get(&hash) {
                        right_indices
                            .iter()
                            .any(|&idx| rows_equal(&left_row, &right_rows[idx].1))
                    } else {
                        false
                    };

                    if !exists_in_right {
                        // Check if we've already added this left row (dedup)
                        let left_indices = left_seen.entry(hash).or_default();
                        let is_duplicate = left_indices
                            .iter()
                            .any(|&idx| rows_equal(&excepted_rows[idx].1, &left_row));

                        if !is_duplicate {
                            left_indices.push(excepted_rows.len());
                            excepted_rows.push((row_id, left_row));
                            row_id += 1;
                        }
                    }
                }

                result_rows = excepted_rows;
            }
            SetOperationType::ExceptAll => {
                // EXCEPT ALL: remove matching rows with multiplicity with proper collision handling

                // Build hash map: hash -> list of row indices
                let mut right_hash_map: FxHashMap<u64, Vec<usize>> = FxHashMap::default();
                for (idx, (_, row)) in right_rows.iter().enumerate() {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(row);
                    right_hash_map.entry(hash).or_default().push(idx);
                }

                // For each unique right row, count how many times it appears
                let mut right_row_counts: FxHashMap<u64, Vec<(usize, usize)>> =
                    FxHashMap::default();
                for (hash, indices) in right_hash_map {
                    checkpoint_set_work(ctx, &mut work)?;
                    let mut unique_rows_in_bucket: Vec<(usize, usize)> = Vec::new();
                    for &idx in &indices {
                        // Find if this row already exists in unique_rows_in_bucket
                        if let Some(entry) =
                            unique_rows_in_bucket.iter_mut().find(|(rep_idx, _)| {
                                rows_equal(&right_rows[*rep_idx].1, &right_rows[idx].1)
                            })
                        {
                            entry.1 += 1; // Increment count
                        } else {
                            unique_rows_in_bucket.push((idx, 1)); // New unique row
                        }
                    }
                    right_row_counts.insert(hash, unique_rows_in_bucket);
                }

                let mut excepted_rows = RowVec::new();
                let mut row_id = 0i64;
                for (_, left_row) in result_rows {
                    checkpoint_set_work(ctx, &mut work)?;
                    let hash = hash_row(&left_row);

                    // Check if this row should be removed (exists in right with count > 0)
                    let mut should_remove = false;
                    if let Some(bucket) = right_row_counts.get_mut(&hash) {
                        // Find the first matching row with count > 0
                        if let Some(entry) = bucket.iter_mut().find(|(rep_idx, count)| {
                            *count > 0 && rows_equal(&left_row, &right_rows[*rep_idx].1)
                        }) {
                            entry.1 -= 1; // Decrement count
                            should_remove = true;
                        }
                    }

                    if !should_remove {
                        excepted_rows.push((row_id, left_row));
                        row_id += 1;
                    }
                }

                result_rows = excepted_rows;
            }
        }
    }

    Ok(Box::new(ExecutorResult::new(columns, result_rows)))
}
