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

//! Subquery Execution
//!
//! This module handles execution of subqueries including:
//! - EXISTS subqueries
//! - Scalar subqueries
//! - IN subqueries

use std::sync::Arc;

use radixdb_core::CompactArc;
use radixdb_core::SmartString;

use radixdb_core::{Error, Result, Value, ValueMap, ValueSet};
use radixdb_sql::ast::*;
use radixdb_sql::token::TokenType;
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::{Engine, QueryResult};

use super::context::{
    cache_batch_aggregate, cache_batch_aggregate_info, cache_count_counter,
    cache_exists_correlation, cache_exists_fetcher, cache_exists_index, cache_exists_pred_key,
    cache_exists_predicate, cache_exists_schema, cache_in_subquery, cache_scalar_subquery,
    cache_semi_join_arc, compute_semi_join_cache_key, extract_table_names_for_cache,
    get_cached_batch_aggregate, get_cached_batch_aggregate_info, get_cached_count_counter,
    get_cached_exists_correlation, get_cached_exists_fetcher, get_cached_exists_index,
    get_cached_exists_pred_key, get_cached_exists_predicate, get_cached_exists_schema,
    get_cached_in_subquery, get_cached_scalar_subquery, get_cached_semi_join,
    BatchAggregateLookupInfo, ExecutionContext, ExistsCorrelationInfo,
};
use super::expr_converter::convert_ast_to_storage_expr;
use super::expression::compute_expression_hash;
use super::operator::{ColumnInfo, MaterializedOperator, Operator};
use super::operators::hash_join::{HashJoinOperator, JoinSide, JoinType};
use super::utils::{dummy_token, dummy_token_clone, value_to_expression};
use crate::access::handle::QueryTableHandle;

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of row IDs to check when verifying visibility for EXISTS/COUNT.
/// We batch up to this many to balance between:
/// - Wasted work if first row is visible (checking extra rows)
/// - Overhead if most row IDs point to deleted rows (need multiple round trips)
///
/// A value of 10 provides reasonable tradeoff for typical workloads.
const VISIBILITY_CHECK_BATCH_SIZE: usize = 10;

// ============================================================================
// Semi-Join Optimization for EXISTS Subqueries
// ============================================================================

// Result type for correlation extraction: (outer_col, outer_table, inner_col, remaining_predicate)
type CorrelationExtraction = (String, Option<String>, String, Option<Arc<Expression>>);

/// Information extracted from an EXISTS subquery for semi-join optimization.
/// Example: EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500)
/// - outer_column: "u.id" (or "id" with outer table "u")
/// - inner_column: "o.user_id" (or "user_id")
/// - inner_table: "orders"
/// - inner_alias: Some("o")
/// - non_correlated_where: Some("o.amount > 500")
#[derive(Debug)]
pub struct SemiJoinInfo {
    /// The outer column referenced in the correlation (e.g., "id" from "u.id")
    pub outer_column: String,
    /// The outer table alias if qualified (e.g., "u" from "u.id")
    pub outer_table: Option<String>,
    /// The inner column used in the correlation (e.g., "user_id" from "o.user_id")
    pub inner_column: String,
    /// The inner table name
    pub inner_table: String,
    /// The inner table alias if present
    pub inner_alias: Option<String>,
    /// Non-correlated part of the WHERE clause (filters only on inner table)
    /// Uses Arc to avoid cloning expression trees during semi-join optimization
    pub non_correlated_where: Option<Arc<Expression>>,
    /// Whether this is NOT EXISTS
    pub is_negated: bool,
}

/// Information needed for index-nested-loop EXISTS execution.
///
/// This is used for direct index probing instead of running a full subquery.
#[derive(Debug, Clone)]
struct IndexNestedLoopInfo {
    outer_column: String,
    outer_table: Option<String>,
    inner_column: String,
    inner_table: String,
    #[allow(dead_code)]
    additional_predicate: Option<Expression>,
}

mod correlation;
mod rewrite;
mod semi_join;
#[cfg(test)]
mod tests;

/// Narrow composition contract for recursive SELECT execution. Query-local
/// caches remain owned by [`ExecutionContext`]; the host provides only the
/// engine and the surrounding SELECT entrypoint.
pub trait SubqueryHost: Sync {
    fn subquery_engine(&self) -> &Arc<MVCCEngine>;
    fn subquery_open_table(&self, table_name: &str) -> Result<QueryTableHandle>;
    fn subquery_execute_select(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
}

/// Single owner for subquery rewriting, correlation analysis, cache use and
/// semi/anti-join execution.
pub struct SubqueryExecutor<'a, H: SubqueryHost + ?Sized> {
    host: &'a H,
}

impl<'a, H: SubqueryHost + ?Sized> SubqueryExecutor<'a, H> {
    fn new(host: &'a H) -> Self {
        Self { host }
    }
}

/// Internal call surface between subquery traversal and the SELECT owner.
pub trait SubqueryExecutorExt: SubqueryHost {
    fn process_where_subqueries(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
    ) -> Result<Expression> {
        SubqueryExecutor::new(self).process_where_subqueries(expression, context)
    }

    fn execute_exists_subquery(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
    ) -> Result<bool> {
        SubqueryExecutor::new(self).execute_exists_subquery(statement, context)
    }

    fn try_process_select_subqueries(
        &self,
        columns: &[Expression],
        context: &ExecutionContext,
    ) -> Result<Option<Vec<Expression>>> {
        SubqueryExecutor::new(self).try_process_select_subqueries(columns, context)
    }

    fn process_correlated_expression(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
    ) -> Result<Expression> {
        SubqueryExecutor::new(self).process_correlated_expression(expression, context)
    }

    fn process_correlated_where(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
    ) -> Result<Expression> {
        SubqueryExecutor::new(self).process_correlated_where(expression, context)
    }

    fn should_use_index_nested_loop_for_anti_join(
        &self,
        info: &SemiJoinInfo,
        outer_limit: Option<i64>,
    ) -> bool {
        SubqueryExecutor::new(self).should_use_index_nested_loop_for_anti_join(info, outer_limit)
    }

    fn execute_semi_join_optimization(
        &self,
        info: &SemiJoinInfo,
        context: &ExecutionContext,
    ) -> Result<CompactArc<ValueSet>> {
        SubqueryExecutor::new(self).execute_semi_join_optimization(info, context)
    }

    fn execute_anti_join(
        &self,
        info: &SemiJoinInfo,
        outer_rows: CompactArc<Vec<radixdb_core::Row>>,
        outer_columns: &[String],
        context: &ExecutionContext,
    ) -> Result<radixdb_core::RowVec> {
        SubqueryExecutor::new(self).execute_anti_join(info, outer_rows, outer_columns, context)
    }

    fn try_optimize_exists_to_semi_join(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
        outer_tables: &[String],
        outer_limit: Option<i64>,
    ) -> Result<Option<Expression>> {
        SubqueryExecutor::new(self).try_optimize_exists_to_semi_join(
            expression,
            context,
            outer_tables,
            outer_limit,
        )
    }

    fn try_optimize_in_to_semi_join(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
        outer_tables: &[String],
    ) -> Result<Option<Expression>> {
        SubqueryExecutor::new(self).try_optimize_in_to_semi_join(expression, context, outer_tables)
    }

    fn has_subqueries(expression: &Expression) -> bool
    where
        Self: Sized,
    {
        SubqueryExecutor::<Self>::has_subqueries(expression)
    }

    fn has_correlated_subqueries(expression: &Expression) -> bool
    where
        Self: Sized,
    {
        SubqueryExecutor::<Self>::has_correlated_subqueries(expression)
    }

    fn has_correlated_select_subqueries(columns: &[Expression]) -> bool
    where
        Self: Sized,
    {
        SubqueryExecutor::<Self>::has_correlated_select_subqueries(columns)
    }

    fn is_subquery_correlated(statement: &SelectStatement) -> bool
    where
        Self: Sized,
    {
        SubqueryExecutor::<Self>::is_subquery_correlated(statement)
    }

    fn try_extract_not_exists_info(
        expression: &Expression,
        outer_tables: &[String],
    ) -> Option<SemiJoinInfo>
    where
        Self: Sized,
    {
        SubqueryExecutor::<Self>::try_extract_not_exists_info(expression, outer_tables)
    }

    fn collect_outer_table_names(table: &Option<Box<Expression>>) -> Vec<String>
    where
        Self: Sized,
    {
        SubqueryExecutor::<Self>::collect_outer_table_names(table)
    }
}

impl<T: SubqueryHost + ?Sized> SubqueryExecutorExt for T {}
