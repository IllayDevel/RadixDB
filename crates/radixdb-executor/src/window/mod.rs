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

//! Window Function Execution
//!
//! This module implements window function execution for SQL queries.
//!
//! Supports:
//! - ROW_NUMBER() - Sequential row numbering
//! - RANK() - Ranking with gaps
//! - DENSE_RANK() - Ranking without gaps
//! - NTILE(n) - Divides rows into n groups
//! - LEAD(col, offset, default) - Access next row's value
//! - LAG(col, offset, default) - Access previous row's value
//!
//! Window clauses:
//! - OVER () - Entire result set as one partition
//! - OVER (PARTITION BY col) - Partition by column values
//! - OVER (ORDER BY col) - Order within partition

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::cmp::Ordering;

use radixdb_core::row_vec::RowVec;
use radixdb_core::value::NULL_VALUE;
use radixdb_core::{CompactVec, StringMap};
use radixdb_core::{Error, Result, Row, Value};

/// Type alias for partition keys - stack-allocated for common case (up to 4 columns)
type PartitionKey = SmallVec<[Value; 4]>;

use radixdb_functions::{FunctionRegistry, WindowFunction};
use radixdb_sql::ast::*;
use radixdb_storage::traits::{QueryResult, Table};

use super::context::ExecutionContext;
use super::expression::{ExpressionEval, MultiExpressionEval};
use super::result::{ColumnarResult, ExecutorResult};
use super::utils::build_column_index_map;

mod aggregate;
mod execute;
mod partition;
mod planning;
#[cfg(test)]
mod tests;

/// Narrow composition contract required by window planning and execution.
pub trait WindowHost: Sync {
    fn window_function_registry(&self) -> &FunctionRegistry;
}

/// Single owner for window planning, partition state, execution and result
/// finalization. The host supplies only the immutable function registry.
pub struct WindowExecutor<'a, H: WindowHost + ?Sized> {
    host: &'a H,
}

impl<'a, H: WindowHost + ?Sized> WindowExecutor<'a, H> {
    fn new(host: &'a H) -> Self {
        Self { host }
    }
}

/// Compatibility surface used by the composition root while the surrounding
/// SELECT orchestration is migrated.
pub trait WindowExecutorExt: WindowHost {
    fn execute_select_with_window_functions(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
    ) -> Result<Box<dyn QueryResult>> {
        WindowExecutor::new(self).execute_select_with_window_functions(
            stmt,
            ctx,
            base_rows,
            base_columns,
        )
    }

    fn execute_select_with_window_functions_presorted(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
        pre_sorted: Option<WindowPreSortedState>,
    ) -> Result<Box<dyn QueryResult>> {
        WindowExecutor::new(self).execute_select_with_window_functions_presorted(
            stmt,
            ctx,
            base_rows,
            base_columns,
            pre_sorted,
        )
    }

    fn execute_select_with_window_functions_pregrouped(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
        pre_grouped: WindowPreGroupedState,
    ) -> Result<Box<dyn QueryResult>> {
        WindowExecutor::new(self).execute_select_with_window_functions_pregrouped(
            stmt,
            ctx,
            base_rows,
            base_columns,
            pre_grouped,
        )
    }

    fn execute_select_with_window_functions_lazy_partition(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        table: &dyn Table,
        base_columns: &[String],
        partition_col: &str,
        limit: usize,
    ) -> Result<Box<dyn QueryResult>> {
        WindowExecutor::new(self).execute_select_with_window_functions_lazy_partition(
            stmt,
            ctx,
            table,
            base_columns,
            partition_col,
            limit,
        )
    }
}

impl<T: WindowHost + ?Sized> WindowExecutorExt for T {}

/// Information about a window function call in a SELECT list
#[derive(Clone, Debug)]
pub struct WindowFunctionInfo {
    /// The window function name (ROW_NUMBER, RANK, etc.)
    pub name: String,
    /// Arguments to the function (for LEAD, LAG, NTILE)
    pub arguments: Vec<Expression>,
    /// Partition by column names (simple identifiers only, for fast-path lookups)
    pub partition_by: Vec<String>,
    /// Original PARTITION BY expressions (includes function calls, complex exprs)
    pub partition_by_exprs: Vec<Expression>,
    /// Order by expressions
    pub order_by: Vec<OrderByExpression>,
    /// Window frame specification (e.g., ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    pub frame: Option<WindowFrame>,
    /// Result column name (may include alias)
    pub column_name: String,
    /// Whether DISTINCT was specified (for COUNT(DISTINCT col) OVER())
    pub is_distinct: bool,
}

/// Information about a SELECT list item for window function processing
pub struct SelectItem {
    pub output_name: String,
    pub source: SelectItemSource,
}

/// Source of a SELECT item value
#[allow(clippy::large_enum_variant)]
pub enum SelectItemSource {
    BaseColumn(usize),
    /// Window function name - stored in lowercase for O(1) lookup in window_value_map
    WindowFunction(String),
    Expression(Expression),
    /// Expression containing one or more window functions.
    /// Stores (expression, list_of_window_function_names_lowercase).
    /// Each Window node in the expression is replaced with a placeholder identifier
    /// referencing the corresponding name at the same index.
    ExpressionWithWindow(Expression, Vec<String>),
}

/// Pre-sorted state for window function optimization
/// When rows are pre-sorted by an indexed column, we can skip sorting in window functions
#[derive(Clone, Debug)]
pub struct WindowPreSortedState {
    /// Column name that rows are sorted by (lowercase)
    pub column: String,
    /// Whether sorted in ascending order
    pub ascending: bool,
}

/// Columnar layout for ORDER BY values - optimized for sorting performance
///
/// Instead of `Vec<Vec<(Value, bool)>>` (row-oriented, N allocations for N rows),
/// this uses `Vec<Vec<Value>>` (column-oriented, K allocations for K ORDER BY columns).
///
/// Benefits:
/// - Reduces allocations from O(N) to O(K) where K = number of ORDER BY columns
/// - Stores ascending flags once per column instead of once per value
/// - Better cache locality when accessing sort keys across rows
#[derive(Clone, Debug)]
pub struct ColumnarOrderByValues {
    /// Column values: columns[col_idx][row_idx] = value
    columns: Vec<Vec<Value>>,
    /// Ascending flags: one per ORDER BY column
    ascending: Vec<bool>,
    /// Resolved NULL placement: one per ORDER BY column.
    nulls_first: Vec<bool>,
    /// Number of rows
    num_rows: usize,
}

impl ColumnarOrderByValues {
    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0 || self.columns.is_empty()
    }

    /// Get number of columns
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Get value at (row, column)
    #[inline]
    pub fn get(&self, row_idx: usize, col_idx: usize) -> Option<&Value> {
        self.columns.get(col_idx).and_then(|col| col.get(row_idx))
    }

    /// Get first ORDER BY value for a row
    #[inline]
    pub fn get_first(&self, row_idx: usize) -> Option<&Value> {
        self.get(row_idx, 0)
    }

    /// Get ascending flag for column
    #[inline]
    pub fn is_ascending(&self, col_idx: usize) -> bool {
        self.ascending.get(col_idx).copied().unwrap_or(true)
    }

    /// Get resolved NULL placement for column.
    #[inline]
    pub fn nulls_first(&self, col_idx: usize) -> bool {
        self.nulls_first.get(col_idx).copied().unwrap_or(false)
    }

    /// Compare ORDER BY values of two rows for equality (without cloning)
    /// Returns true if all ORDER BY column values are equal
    #[inline]
    pub fn rows_equal(&self, row_a: usize, row_b: usize) -> bool {
        for col in &self.columns {
            let val_a = col.get(row_a);
            let val_b = col.get(row_b);
            match (val_a, val_b) {
                (Some(a), Some(b)) if a == b => continue,
                (None, None) => continue,
                _ => return false,
            }
        }
        true
    }
}

/// Pre-grouped state for window function PARTITION BY optimization
/// When rows are fetched grouped by an indexed partition column, we can skip hash-based grouping
#[derive(Clone)]
pub struct WindowPreGroupedState {
    /// Pre-built partition map: partition key -> row indices
    pub partition_map: FxHashMap<PartitionKey, Vec<usize>>,
    /// The column name (lowercase) that the partition map was built from
    pub partition_column: String,
}
