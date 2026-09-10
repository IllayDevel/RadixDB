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

//! Aggregation and GROUP BY Execution
//!
//! This module implements aggregation with GROUP BY and HAVING clauses:
//!
//! - Global aggregation (without GROUP BY): `SELECT COUNT(*) FROM table`
//! - Grouped aggregation: `SELECT category, SUM(amount) FROM sales GROUP BY category`
//! - HAVING clause: `SELECT category, SUM(amount) FROM sales GROUP BY category HAVING SUM(amount) > 100`

use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};

use ahash::AHasher;
use hashbrown::hash_map::RawEntryMut;
#[cfg(feature = "parallel")]
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
// SmallVec removed - Vec is faster due to spilled() check overhead in hot loops

use radixdb_core::{CompactArc, CompactVec, I64Map, StringMap};
use radixdb_core::{Error, Result, Row, RowVec, Value, ValueMap, ValueSet};
use radixdb_functions::aggregate::{numeric::NumericAccumulator, CompiledAggregate};
use radixdb_functions::{AggregateFunction, AggregateOrderBySpec, FunctionRegistry};
use radixdb_sql::ast::*;
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::{Engine, QueryResult};

use super::compiled_plan::{CompiledCountDistinct, CompiledExecution};
use super::context::ExecutionContext;
#[allow(deprecated)]
use super::expression::CompiledEvaluator;
use super::expression::{ExpressionEval, RowFilter};
use super::mutation::host::ActiveTransaction;
use super::query_classification::QueryClassification;
use super::result::ExecutorResult;
use super::utils::build_column_index_map;

// Re-export for backward compatibility
pub use super::utils::{expression_contains_aggregate, is_aggregate_function};

mod execute;
mod finalize;
mod global;
mod grouped;
mod planning;
mod rollup;
mod storage;
mod streaming;
#[cfg(test)]
mod tests;

/// Narrow composition contract for the remaining correlated-subquery hooks.
/// Storage and function dependencies stay explicit and downward-only.
pub trait AggregationHost: Sync {
    fn aggregation_engine(&self) -> &Arc<MVCCEngine>;
    fn aggregation_function_registry(&self) -> &FunctionRegistry;
    fn aggregation_active_transaction(&self) -> &Mutex<Option<ActiveTransaction>>;
    fn aggregation_process_where_subqueries(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
    ) -> Result<Expression>;
    fn aggregation_try_process_select_subqueries(
        &self,
        columns: &[Expression],
        context: &ExecutionContext,
    ) -> Result<Option<Vec<Expression>>>;
    fn aggregation_has_correlated_subqueries(&self, expression: &Expression) -> bool;
    fn aggregation_process_correlated_expression(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
    ) -> Result<Expression>;
    fn aggregation_output_column_names(
        &self,
        select_expressions: &[Expression],
        source_columns: &[String],
        table_alias: Option<&str>,
    ) -> Vec<String>;
}

/// Single owner for aggregate planning, state, execution and finalization.
pub struct AggregationExecutor<'a, H: AggregationHost + ?Sized> {
    host: &'a H,
}

impl<'a, H: AggregationHost + ?Sized> AggregationExecutor<'a, H> {
    fn new(host: &'a H) -> Self {
        Self { host }
    }
}

/// Internal call surface between aggregation phases and the SELECT owner.
pub trait AggregationExecutorExt: AggregationHost {
    fn execute_select_with_aggregation(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
        rows: RowVec,
        columns: &[String],
    ) -> Result<Box<dyn QueryResult>> {
        AggregationExecutor::new(self)
            .execute_select_with_aggregation(statement, context, rows, columns)
    }

    fn execute_aggregation_for_window(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
        rows: &[(i64, Row)],
        columns: &[String],
    ) -> Result<(Vec<String>, RowVec)> {
        AggregationExecutor::new(self)
            .execute_aggregation_for_window(statement, context, rows, columns)
    }

    fn try_aggregation_pushdown(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        statement: &SelectStatement,
        context: &ExecutionContext,
        classification: &Arc<QueryClassification>,
    ) -> Result<Option<Box<dyn QueryResult>>> {
        AggregationExecutor::new(self).try_aggregation_pushdown(
            table,
            statement,
            context,
            classification,
        )
    }

    fn try_filtered_aggregation_pushdown(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        statement: &SelectStatement,
        context: &ExecutionContext,
        classification: &Arc<QueryClassification>,
        columns: &[String],
    ) -> Result<Option<Box<dyn QueryResult>>> {
        AggregationExecutor::new(self).try_filtered_aggregation_pushdown(
            table,
            statement,
            context,
            classification,
            columns,
        )
    }

    fn try_streaming_global_aggregation(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        statement: &SelectStatement,
        context: &ExecutionContext,
        classification: &Arc<QueryClassification>,
    ) -> Result<Option<Box<dyn QueryResult>>> {
        AggregationExecutor::new(self).try_streaming_global_aggregation(
            table,
            statement,
            context,
            classification,
        )
    }

    fn try_streaming_derived_table_aggregation(
        &self,
        source: Box<dyn QueryResult>,
        statement: &SelectStatement,
        classification: &Arc<QueryClassification>,
        context: &ExecutionContext,
    ) -> Result<DerivedAggregationAttempt> {
        AggregationExecutor::new(self).try_streaming_derived_table_aggregation(
            source,
            statement,
            classification,
            context,
        )
    }

    fn try_storage_aggregation(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        statement: &SelectStatement,
        context: &ExecutionContext,
        columns: &[String],
        classification: &QueryClassification,
    ) -> Option<Box<dyn QueryResult>> {
        AggregationExecutor::new(self).try_storage_aggregation(
            table,
            statement,
            context,
            columns,
            classification,
        )
    }

    fn try_fast_count_distinct_compiled(
        &self,
        statement: &SelectStatement,
        compiled: &RwLock<CompiledExecution>,
    ) -> Option<Result<Box<dyn QueryResult>>> {
        AggregationExecutor::new(self).try_fast_count_distinct_compiled(statement, compiled)
    }

    fn try_fast_count_star_compiled(
        &self,
        statement: &SelectStatement,
        compiled: &RwLock<CompiledExecution>,
    ) -> Option<Result<Box<dyn QueryResult>>> {
        AggregationExecutor::new(self).try_fast_count_star_compiled(statement, compiled)
    }
}

impl<T: AggregationHost + ?Sized> AggregationExecutorExt for T {}

/// Single condition in a HAVING clause
#[derive(Clone, Debug)]
struct HavingCondition {
    /// Index of the aggregate in the aggregations array
    agg_index: usize,
    /// Comparison operator
    op: ComparisonOp,
    /// Threshold value
    threshold: f64,
}

/// Simple HAVING filter for inline application during fast aggregation
/// Supports: SUM(col) op value, COUNT(*) op value, COUNT(col) op value
/// Also supports AND combinations: COUNT(*) > 10 AND SUM(x) > 100
#[derive(Clone, Debug)]
struct SimpleHavingFilter {
    /// All conditions that must pass (AND semantics)
    conditions: Vec<HavingCondition>,
}

#[derive(Clone, Copy, Debug)]
enum ComparisonOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Neq,
}

impl HavingCondition {
    /// Check if a value passes this condition
    fn matches(&self, value: f64) -> bool {
        match self.op {
            ComparisonOp::Gt => value > self.threshold,
            ComparisonOp::Gte => value >= self.threshold,
            ComparisonOp::Lt => value < self.threshold,
            ComparisonOp::Lte => value <= self.threshold,
            ComparisonOp::Eq => (value - self.threshold).abs() < f64::EPSILON,
            ComparisonOp::Neq => (value - self.threshold).abs() >= f64::EPSILON,
        }
    }
}

impl SimpleHavingFilter {
    /// Create a filter with a single condition
    fn single(agg_index: usize, op: ComparisonOp, threshold: f64) -> Self {
        Self {
            conditions: vec![HavingCondition {
                agg_index,
                op,
                threshold,
            }],
        }
    }

    /// Combine two filters with AND semantics
    fn and(mut self, other: Self) -> Self {
        self.conditions.extend(other.conditions);
        self
    }
}

/// Simple aggregate type for fast aggregation path
/// Supports COUNT, SUM, AVG, MIN, MAX (no DISTINCT, FILTER, ORDER BY, or expressions)
#[derive(Clone)]
enum SimpleAgg {
    Count(Option<usize>), // COUNT(*) or COUNT(col) - stores column index for COUNT(col)
    Sum(usize),           // SUM(col) - stores column index
    Avg(usize),           // AVG(col) - stores column index
    Min(usize),           // MIN(col) - stores column index
    Max(usize),           // MAX(col) - stores column index
}

impl SimpleAgg {
    #[inline]
    fn count_includes_row(&self, row: &Row) -> bool {
        match self {
            Self::Count(None) => true,
            Self::Count(Some(column_index)) => {
                row.get(*column_index).is_some_and(|value| !value.is_null())
            }
            _ => false,
        }
    }
}

/// Outcome of trying the streaming aggregate path for a derived source.
///
/// A rejected optimization returns the original result source. This is
/// essential: the caller must materialize that source once, rather than issue
/// the same subquery a second time after an optimizer probe consumed a row.
pub enum DerivedAggregationAttempt {
    Applied(Box<dyn QueryResult>),
    Rejected(Box<dyn QueryResult>),
}

struct DerivedAggregationPlan {
    group_col_name: String,
    group_col_idx: usize,
    aggregations: Vec<SqlAggregateFunction>,
    simple_aggs: Vec<SimpleAgg>,
}

/// Try to parse a simple HAVING clause for inline filtering
/// Returns None if the HAVING is too complex for inline optimization
/// Supports: single conditions and AND combinations
fn try_parse_simple_having(
    having: &Expression,
    aggregations: &[SqlAggregateFunction],
) -> Option<SimpleHavingFilter> {
    // Handle AND expressions: parse both sides and combine
    if let Expression::Infix(binop) = having {
        if binop.operator.eq_ignore_ascii_case("AND") {
            let left = try_parse_simple_having(&binop.left, aggregations)?;
            let right = try_parse_simple_having(&binop.right, aggregations)?;
            return Some(left.and(right));
        }
    }

    // Handle single comparison: AGG(col) op value
    try_parse_single_having_condition(having, aggregations)
        .map(|(agg_index, op, threshold)| SimpleHavingFilter::single(agg_index, op, threshold))
}

/// Parse a single HAVING condition (not AND/OR)
fn try_parse_single_having_condition(
    having: &Expression,
    aggregations: &[SqlAggregateFunction],
) -> Option<(usize, ComparisonOp, f64)> {
    // Handle comparison: AGG(col) op value
    if let Expression::Infix(binop) = having {
        let (op, threshold) = match binop.operator.as_str() {
            ">" => (ComparisonOp::Gt, extract_numeric_value(&binop.right)?),
            ">=" => (ComparisonOp::Gte, extract_numeric_value(&binop.right)?),
            "<" => (ComparisonOp::Lt, extract_numeric_value(&binop.right)?),
            "<=" => (ComparisonOp::Lte, extract_numeric_value(&binop.right)?),
            "=" => (ComparisonOp::Eq, extract_numeric_value(&binop.right)?),
            "!=" | "<>" => (ComparisonOp::Neq, extract_numeric_value(&binop.right)?),
            _ => return None,
        };

        // Left side should be an aggregate function
        if let Expression::FunctionCall(func) = &*binop.left {
            let func_upper = func.function.to_uppercase();
            if matches!(func_upper.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX") {
                // Find matching aggregate
                for (i, agg) in aggregations.iter().enumerate() {
                    if agg.name.to_uppercase() == func_upper && !agg.distinct {
                        // Check if column matches (for non-COUNT(*))
                        let col_matches = if func_upper == "COUNT" {
                            // COUNT(*) or COUNT(col)
                            func.arguments.first().is_none_or(|arg| {
                                matches!(arg, Expression::Star(_))
                                    || match arg {
                                        Expression::Identifier(id) => {
                                            id.value_lower == agg.column_lower
                                        }
                                        _ => false,
                                    }
                            })
                        } else {
                            // SUM, AVG, etc. - check column
                            func.arguments.first().is_some_and(|arg| match arg {
                                Expression::Identifier(id) => id.value_lower == agg.column_lower,
                                _ => false,
                            })
                        };

                        if col_matches {
                            return Some((i, op, threshold));
                        }
                    }
                }
            }
        }
    }

    None
}

/// Extract numeric value from expression
fn extract_numeric_value(expr: &Expression) -> Option<f64> {
    match expr {
        Expression::IntegerLiteral(lit) => Some(lit.value as f64),
        Expression::FloatLiteral(lit) => Some(lit.value),
        Expression::Prefix(unary) if unary.operator == "-" => {
            extract_numeric_value(&unary.right).map(|v| -v)
        }
        _ => None,
    }
}

/// Represents a grouping set for ROLLUP/CUBE operations
/// Each grouping set specifies which columns are active (included in grouping)
/// For ROLLUP(a, b), we get: [true, true], [true, false], [false, false]
#[derive(Clone, Debug)]
struct GroupingSet {
    /// For each GROUP BY column, whether it's included in this grouping level
    /// If false, the column value will be NULL in the output (rolled up)
    active_columns: Vec<bool>,
}

/// Generate a canonical key for an expression for semantic matching.
/// This ensures consistent matching regardless of token positions or formatting.
/// All string-based keys are lowercased for case-insensitive matching.
fn expression_canonical_key(expr: &Expression) -> String {
    match expr {
        Expression::Identifier(id) => id.value_lower.to_string(),
        Expression::QualifiedIdentifier(qid) => {
            format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower)
        }
        Expression::IntegerLiteral(lit) => format!("$pos:{}", lit.value),
        Expression::FloatLiteral(lit) => format!("$float:{}", lit.value),
        Expression::StringLiteral(lit) => format!("$str:{}", lit.value.to_lowercase()),
        Expression::BooleanLiteral(lit) => format!("$bool:{}", lit.value),
        Expression::FunctionCall(func) => {
            // For function calls, build a canonical form
            let args: Vec<String> = func
                .arguments
                .iter()
                .map(expression_canonical_key)
                .collect();
            format!("{}({})", func.function.to_lowercase(), args.join(","))
        }
        Expression::Infix(bin) => {
            // For infix/binary operations, build a canonical form
            format!(
                "({} {} {})",
                expression_canonical_key(&bin.left),
                bin.operator.to_lowercase(),
                expression_canonical_key(&bin.right)
            )
        }
        Expression::Prefix(un) => {
            // For prefix/unary operations
            format!(
                "({}{})",
                un.operator.to_lowercase(),
                expression_canonical_key(&un.right)
            )
        }
        Expression::Aliased(aliased) => {
            // For aliased expressions, use the underlying expression
            expression_canonical_key(&aliased.expression)
        }
        // For other complex expressions, use Display but lowercase for consistency
        _ => format!("{}", expr).to_lowercase(),
    }
}

/// Generate a canonical key for a GroupByItem for semantic matching.
fn group_by_item_canonical_key(item: &GroupByItem) -> String {
    match item {
        GroupByItem::Column(name) => name.to_lowercase(),
        GroupByItem::Position(pos) => format!("$pos:{}", pos),
        GroupByItem::Expression { expr, .. } => expression_canonical_key(expr),
    }
}

/// Represents a GROUP BY item - either a column reference or an expression
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum GroupByItem {
    /// Simple column reference by name
    Column(String),
    /// Positional reference like GROUP BY 1
    Position(usize),
    /// Complex expression that needs to be evaluated
    Expression {
        /// The expression to evaluate
        expr: Expression,
        /// Display name for the result column (from alias if available)
        display_name: String,
    },
}

/// Represents the source of a column in post-aggregation processing
#[derive(Clone, Debug)]
enum ColumnSource {
    /// Column comes directly from aggregation result
    AggColumn(String),
    /// Column needs to be evaluated from an expression (boxed to reduce enum size)
    Expression(Box<Expression>),
    /// Correlated subquery expression that needs per-row evaluation with outer row context
    CorrelatedExpression(Box<Expression>),
    /// GROUPING() function - index is the GROUP BY column position (0-based)
    GroupingFlag(usize),
}

/// Compute a hash for a group key (slice of Values)
/// This avoids allocating Vec<Value> for each row
/// OPTIMIZATION: Use AHasher for optimal hashing of Value types (strings, floats, JSON, etc.)
/// Empirically tested to perform better than FxHasher for GROUP BY workloads
/// Called on every row in GROUP BY, so performance is critical
#[inline]
fn hash_group_key(values: &[Value]) -> u64 {
    let mut hasher = AHasher::default();
    for v in values {
        v.hash(&mut hasher);
    }
    hasher.finish()
}

#[inline]
fn track_distinct_value(seen: &mut ValueSet, value: &Value) -> bool {
    seen.insert(value.clone())
}

/// Group entry storing the key values and row indices
struct GroupEntry {
    /// The actual key values (stored once per group)
    key_values: Vec<Value>,
    /// Indices of rows belonging to this group
    row_indices: Vec<usize>,
}

/// Represents an aggregate function call in a SELECT list
#[derive(Clone, Debug)]
pub struct SqlAggregateFunction {
    /// Function name (COUNT, SUM, AVG, MIN, MAX, etc.)
    pub name: String,
    /// Column name the function operates on (* for COUNT(*))
    pub column: String,
    /// Pre-computed lowercase column name for index lookups
    pub column_lower: String,
    /// Alias for the result column
    pub alias: Option<String>,
    /// Whether DISTINCT is specified
    pub distinct: bool,
    /// Extra arguments (e.g., separator for STRING_AGG)
    pub extra_args: Vec<Value>,
    /// The expression to evaluate for each row (for SUM(val * 2), AVG(a + b), etc.)
    /// If None, use column directly; if Some, evaluate expression first
    pub expression: Option<Expression>,
    /// ORDER BY clause for ordered-set aggregates like STRING_AGG
    pub order_by: Vec<radixdb_sql::ast::OrderByExpression>,
    /// FILTER clause condition - only accumulate rows where this is true
    pub filter: Option<Expression>,
    /// Whether this aggregate is hidden (only used for ORDER BY, not in SELECT)
    pub hidden: bool,
}

impl SqlAggregateFunction {
    /// Get the result column name
    pub fn get_column_name(&self) -> String {
        if let Some(ref alias) = self.alias {
            alias.clone()
        } else if self.column == "*" {
            format!("{}(*)", self.name)
        } else if self.extra_args.is_empty() {
            format!("{}({})", self.name, self.column)
        } else {
            // Include extra arguments in column name (e.g., STRING_AGG(name, ' | '))
            let args_str: Vec<String> = std::iter::once(self.column.clone())
                .chain(self.extra_args.iter().map(|v| match v {
                    Value::Text(s) => format!("'{}'", s),
                    other => other.to_string(),
                }))
                .collect();
            format!("{}({})", self.name, args_str.join(", "))
        }
    }

    /// Get the expression name (without alias) for HAVING clause matching
    /// This returns `SUM(price)` even if there's an alias like `AS total`
    pub fn get_expression_name(&self) -> String {
        if self.column == "*" {
            format!("{}(*)", self.name)
        } else {
            format!("{}({})", self.name, self.column)
        }
    }
}
