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

//! EXPLAIN statement execution
//!
//! This module handles EXPLAIN and EXPLAIN ANALYZE query plan output,
//! showing the execution strategy and cost estimates for SQL statements.

use crate::cte::CteExecutorExt;
use crate::navigation::NavigationExecutorExt;
use ahash::AHashSet;
use radixdb_core::SmartString;
use radixdb_core::{Result, Row, RowVec, Value};
use radixdb_functions::registry::global_registry;
use radixdb_sql::ast::*;
use radixdb_storage::traits::{Engine, QueryResult, ScanPlan};

use super::context::ExecutionContext;
use super::operators::index_nested_loop::IndexLookupStrategy;
use super::planner::RuntimeJoinAlgorithm;
use super::pushdown;
use super::result::ExecutorResult;
use super::utils::{
    collect_table_qualifiers, combine_predicates_with_and, expression_contains_aggregate,
    filter_references_column, flatten_and_predicates, is_aggregate_function,
    substitute_filter_column,
};
use super::Executor;

#[derive(Debug, Clone, Copy, Default)]
struct ArtifactCacheExplainStats {
    descriptor_open_calls: u64,
    file_open_calls: u64,
    file_stat_calls: u64,
    file_identity_checks: u64,
    fadvise_calls: u64,
    fadvise_errors: u64,
    pread_calls: u64,
    pread_bytes: u64,
    singleflight_leaders: u64,
    singleflight_followers: u64,
    payload_decompress_calls: u64,
    payload_decompress_nanos: u64,
    column_deserialize_calls: u64,
    column_deserialize_nanos: u64,
    cache_hits: u64,
    cache_misses: u64,
    cache_insert_bytes: u64,
}

impl ArtifactCacheExplainStats {
    fn has_artifact_activity(self) -> bool {
        self.descriptor_open_calls != 0
            || self.file_open_calls != 0
            || self.file_stat_calls != 0
            || self.file_identity_checks != 0
            || self.fadvise_calls != 0
            || self.pread_calls != 0
            || self.pread_bytes != 0
            || self.singleflight_leaders != 0
            || self.singleflight_followers != 0
            || self.payload_decompress_calls != 0
            || self.column_deserialize_calls != 0
            || self.cache_hits != 0
            || self.cache_misses != 0
            || self.cache_insert_bytes != 0
    }
}

#[derive(Debug, Clone, Copy)]
struct ExplainAnalyzeStats<'a> {
    row_count: usize,
    time_str: &'a str,
    query_wall_nanos: u64,
    join_peak_memory_bytes: usize,
    join_retained_memory_bytes: usize,
    metadata_pk_count: radixdb_storage::instrumentation::MetadataPkCountProbeSnapshot,
    artifact_columnar_group: radixdb_storage::instrumentation::ArtifactColumnarGroupProbeSnapshot,
    join_execution: radixdb_storage::instrumentation::JoinExecutionProbeSnapshot,
    join_execution_trace: &'a radixdb_storage::instrumentation::JoinExecutionTraceSnapshot,
    join_planning: &'a radixdb_storage::instrumentation::JoinPlanningProbeSnapshot,
    plugin_planner: crate::query::PluginPlannerProbeSnapshot,
    artifact_cache: ArtifactCacheExplainStats,
}

include!("analyze.rs");
include!("plan.rs");

/// Extract table alias from a table expression
fn extract_table_alias(expr: &Expression) -> Option<String> {
    match expr {
        Expression::TableSource(simple) => simple
            .alias
            .as_ref()
            .map(|a| a.value.to_string())
            .or_else(|| Some(simple.name.value.to_string())),
        Expression::Aliased(aliased) => Some(aliased.alias.value.to_string()),
        _ => None,
    }
}

/// Collect all table aliases (lowercase) from a table expression tree.
/// Handles nested JoinSource by recursing into both sides.
fn collect_table_aliases(expr: &Expression, aliases: &mut rustc_hash::FxHashSet<String>) {
    match expr {
        Expression::TableSource(simple) => {
            let alias = simple
                .alias
                .as_ref()
                .map(|a| a.value.to_string().to_lowercase())
                .unwrap_or_else(|| simple.name.value.to_string().to_lowercase());
            aliases.insert(alias);
        }
        Expression::JoinSource(join) => {
            collect_table_aliases(&join.left, aliases);
            collect_table_aliases(&join.right, aliases);
        }
        Expression::SubquerySource(ss) => {
            if let Some(ref alias) = ss.alias {
                aliases.insert(alias.value.to_string().to_lowercase());
            }
        }
        Expression::CteReference(cr) => {
            let alias = cr
                .alias
                .as_ref()
                .map(|a| a.value.to_string().to_lowercase())
                .unwrap_or_else(|| cr.name.value.to_string().to_lowercase());
            aliases.insert(alias);
        }
        Expression::FunctionTableSource(fts) => {
            let alias = fts
                .alias
                .as_ref()
                .map(|a| a.value.to_string().to_lowercase())
                .unwrap_or_else(|| fts.function.value.to_string().to_lowercase());
            aliases.insert(alias);
        }
        Expression::ValuesSource(vs) => {
            if let Some(ref alias) = vs.alias {
                aliases.insert(alias.value.to_string().to_lowercase());
            }
        }
        _ => {}
    }
}

fn explain_join_barrier(join: &radixdb_sql::ast::JoinTableSource) -> &'static str {
    if !join.using_columns.is_empty() || join.join_type.to_uppercase().contains("NATURAL") {
        return "natural_or_using";
    }
    match join.join_type.trim().to_uppercase().as_str() {
        "INNER" if join.condition.is_some() => "reorderable_inner",
        "CROSS" => "cross",
        "LEFT" | "LEFT OUTER" => "left",
        "RIGHT" | "RIGHT OUTER" => "right",
        "FULL" | "FULL OUTER" => "full",
        _ => "other",
    }
}

/// Collect all output column names (lowercase) from a table expression tree.
/// Handles real tables (via engine schema lookup), subqueries (via SELECT list),
/// and nested joins (via recursion).
fn collect_side_columns(
    expr: &Expression,
    engine: &dyn Engine,
    columns: &mut rustc_hash::FxHashSet<String>,
) {
    match expr {
        Expression::TableSource(simple) => {
            let table_name = simple.name.value.to_string();
            if let Ok(schema) = engine.get_table_schema(&table_name) {
                for col in schema.column_names() {
                    columns.insert(col.to_lowercase());
                }
            }
        }
        Expression::SubquerySource(ss) => {
            let has_star = ss
                .subquery
                .columns
                .iter()
                .any(|c| matches!(c, Expression::Star(_) | Expression::QualifiedStar(_)));
            if has_star {
                // SELECT * inherits all columns from the subquery's source
                if let Some(ref table_expr) = ss.subquery.table_expr {
                    collect_side_columns(table_expr, engine, columns);
                }
            } else {
                extract_select_column_names(&ss.subquery.columns, columns);
            }
        }
        Expression::CteReference(cr) => {
            // CTE output columns match the CTE's SELECT list, but the WITH
            // clause is not available here. Fall back to the CTE name as a
            // table lookup — works when the CTE shares a name with a real table
            // or when the engine has materialized CTE metadata.
            let table_name = cr.name.value.to_string();
            if let Ok(schema) = engine.get_table_schema(&table_name) {
                for col in schema.column_names() {
                    columns.insert(col.to_lowercase());
                }
            }
        }
        Expression::FunctionTableSource(fts) => {
            if !fts.column_aliases.is_empty() {
                for ca in &fts.column_aliases {
                    columns.insert(ca.value_lower.to_string());
                }
            } else {
                // Fall back to TVF's default column names from the registry
                let fn_name = fts.function.value.to_uppercase();
                if let Some(tvf) = global_registry().get_tvf(&fn_name) {
                    for col in tvf.column_names() {
                        columns.insert(col.to_lowercase());
                    }
                }
            }
        }
        Expression::ValuesSource(vs) => {
            if !vs.column_aliases.is_empty() {
                for ca in &vs.column_aliases {
                    columns.insert(ca.value_lower.to_string());
                }
            } else if let Some(first_row) = vs.rows.first() {
                // Auto-generate column1, column2, ... based on row width
                for i in 0..first_row.len() {
                    columns.insert(format!("column{}", i + 1));
                }
            }
        }
        Expression::JoinSource(join) => {
            collect_side_columns(&join.left, engine, columns);
            collect_side_columns(&join.right, engine, columns);
        }
        _ => {}
    }
}

/// Extract output column names from a SELECT column list.
/// Handles aliases, qualified identifiers, and bare identifiers.
fn extract_select_column_names(
    select_columns: &[Expression],
    out: &mut rustc_hash::FxHashSet<String>,
) {
    for col in select_columns {
        match col {
            Expression::Aliased(a) => {
                out.insert(a.alias.value_lower.to_string());
            }
            Expression::Identifier(id) => {
                out.insert(id.value_lower.to_string());
            }
            Expression::QualifiedIdentifier(qi) => {
                out.insert(qi.name.value_lower.to_string());
            }
            // Star or complex expressions — can't determine column names
            _ => {}
        }
    }
}

/// Collect unqualified (bare) column names from a predicate expression.
fn collect_unqualified_columns(expr: &Expression, columns: &mut rustc_hash::FxHashSet<String>) {
    match expr {
        Expression::Identifier(id) => {
            columns.insert(id.value_lower.to_string());
        }
        Expression::Infix(infix) => {
            collect_unqualified_columns(&infix.left, columns);
            collect_unqualified_columns(&infix.right, columns);
        }
        Expression::Prefix(prefix) => {
            collect_unqualified_columns(&prefix.right, columns);
        }
        Expression::In(in_expr) => {
            collect_unqualified_columns(&in_expr.left, columns);
            // Recurse into the right side (ExpressionList, List, or subquery)
            match in_expr.right.as_ref() {
                Expression::ExpressionList(el) => {
                    for elem in &el.expressions {
                        collect_unqualified_columns(elem, columns);
                    }
                }
                Expression::List(list) => {
                    for elem in &list.elements {
                        collect_unqualified_columns(elem, columns);
                    }
                }
                other => {
                    collect_unqualified_columns(other, columns);
                }
            }
        }
        Expression::Between(between) => {
            collect_unqualified_columns(&between.expr, columns);
            collect_unqualified_columns(&between.lower, columns);
            collect_unqualified_columns(&between.upper, columns);
        }
        Expression::Like(like) => {
            collect_unqualified_columns(&like.left, columns);
            collect_unqualified_columns(&like.pattern, columns);
        }
        Expression::FunctionCall(func) => {
            for arg in &func.arguments {
                collect_unqualified_columns(arg, columns);
            }
        }
        Expression::Cast(cast) => {
            collect_unqualified_columns(&cast.expr, columns);
        }
        Expression::Case(case) => {
            if let Some(ref val) = case.value {
                collect_unqualified_columns(val, columns);
            }
            for when in &case.when_clauses {
                collect_unqualified_columns(&when.condition, columns);
                collect_unqualified_columns(&when.then_result, columns);
            }
            if let Some(ref else_val) = case.else_value {
                collect_unqualified_columns(else_val, columns);
            }
        }
        Expression::Aliased(aliased) => {
            collect_unqualified_columns(&aliased.expression, columns);
        }
        _ => {}
    }
}

/// Partition a WHERE clause into predicates belonging to the left side,
/// right side, or the join level (cross-table / outer-join nullable-side predicates).
/// When a predicate uses unqualified column names, resolves them against
/// table schemas via the engine to determine the correct side.
///
/// For outer joins, predicates on the nullable side are post-join filters
/// (they test NULL-padded rows), so they are shown at join level, not under
/// the child scan. LEFT JOIN: right side is nullable. RIGHT JOIN: left side.
/// FULL JOIN: both sides.
fn partition_where_for_explain(
    where_clause: &Expression,
    left_expr: &Expression,
    right_expr: &Expression,
    join_type: &str,
    engine: &dyn Engine,
) -> (Option<Expression>, Option<Expression>, Option<Expression>) {
    let jt = join_type.to_uppercase();
    let left_nullable = jt == "RIGHT" || jt == "FULL" || jt == "NATURAL RIGHT";
    let right_nullable = jt == "LEFT" || jt == "FULL" || jt == "NATURAL LEFT";
    let mut left_aliases = rustc_hash::FxHashSet::default();
    let mut right_aliases = rustc_hash::FxHashSet::default();
    collect_table_aliases(left_expr, &mut left_aliases);
    collect_table_aliases(right_expr, &mut right_aliases);

    // Collect column names for each side: real tables via schema lookup,
    // subqueries via their SELECT list, nested joins via recursion.
    let mut left_columns = rustc_hash::FxHashSet::default();
    let mut right_columns = rustc_hash::FxHashSet::default();
    collect_side_columns(left_expr, engine, &mut left_columns);
    collect_side_columns(right_expr, engine, &mut right_columns);

    let predicates = flatten_and_predicates(where_clause);
    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut join_preds = Vec::new();

    for pred in predicates {
        let qualifiers = collect_table_qualifiers(&pred);
        // Also collect unqualified columns — a predicate can mix qualified and
        // unqualified refs (e.g., `o.status = name` has qualifier "o" AND
        // unqualified "name"). Both must be checked to determine side ownership.
        let mut unqual_cols = rustc_hash::FxHashSet::default();
        collect_unqualified_columns(&pred, &mut unqual_cols);
        let unqual_in_left = unqual_cols
            .iter()
            .any(|c| left_columns.contains(c.as_str()));
        let unqual_in_right = unqual_cols
            .iter()
            .any(|c| right_columns.contains(c.as_str()));

        let (targets_left, targets_right) = if !qualifiers.is_empty() {
            let qual_targets_left = qualifiers.iter().any(|q| left_aliases.contains(q));
            let qual_targets_right = qualifiers.iter().any(|q| right_aliases.contains(q));
            (
                qual_targets_left || unqual_in_left,
                qual_targets_right || unqual_in_right,
            )
        } else {
            // Fully unqualified predicate: resolve column names against schemas
            (
                unqual_in_left && !unqual_in_right,
                unqual_in_right && !unqual_in_left,
            )
        };

        if targets_left && !targets_right {
            if left_nullable {
                // Left side is nullable (RIGHT/FULL JOIN): post-join filter
                join_preds.push(pred);
            } else {
                left_preds.push(pred);
            }
        } else if targets_right && !targets_left {
            if right_nullable {
                // Right side is nullable (LEFT/FULL JOIN): post-join filter
                join_preds.push(pred);
            } else {
                right_preds.push(pred);
            }
        } else {
            // Cross-table, ambiguous, or no schema info: join level
            join_preds.push(pred);
        }
    }

    (
        combine_predicates_with_and(left_preds),
        combine_predicates_with_and(right_preds),
        combine_predicates_with_and(join_preds),
    )
}

// ============================================================================
// Helper Functions
// ============================================================================

fn base_table_source_for_explain(expr: &Expression) -> Option<&SimpleTableSource> {
    match expr {
        Expression::TableSource(table) => Some(table),
        Expression::Aliased(aliased) => match aliased.expression.as_ref() {
            Expression::TableSource(table) => Some(table),
            _ => None,
        },
        _ => None,
    }
}

fn is_single_equality_join_condition_for_explain(condition: Option<&Expression>) -> bool {
    let Some(Expression::Infix(infix)) = condition else {
        return false;
    };
    infix.operator == "="
        && matches!(
            (infix.left.as_ref(), infix.right.as_ref()),
            (
                Expression::Identifier(_) | Expression::QualifiedIdentifier(_),
                Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
            )
        )
}

fn is_count_star_select_for_explain(columns: Option<&[Expression]>) -> bool {
    let Some([expression]) = columns else {
        return false;
    };
    let function = match expression {
        Expression::FunctionCall(function) => function.as_ref(),
        Expression::Aliased(aliased) => match aliased.expression.as_ref() {
            Expression::FunctionCall(function) => function.as_ref(),
            _ => return false,
        },
        _ => return false,
    };
    function.function.eq_ignore_ascii_case("COUNT")
        && !function.is_distinct
        && function.filter.is_none()
        && function.order_by.is_empty()
        && matches!(function.arguments.as_slice(), [Expression::Star(_)])
}

fn count_qualified_column_for_explain(
    columns: Option<&[Expression]>,
) -> Option<&QualifiedIdentifier> {
    let Some([expression]) = columns else {
        return None;
    };
    let function = match expression {
        Expression::FunctionCall(function) => function.as_ref(),
        Expression::Aliased(aliased) => match aliased.expression.as_ref() {
            Expression::FunctionCall(function) => function.as_ref(),
            _ => return None,
        },
        _ => return None,
    };
    if !function.function.eq_ignore_ascii_case("COUNT")
        || function.is_distinct
        || function.filter.is_some()
        || !function.order_by.is_empty()
    {
        return None;
    }
    let [Expression::QualifiedIdentifier(identifier)] = function.arguments.as_slice() else {
        return None;
    };
    (!identifier.is_multi_part_path()).then_some(identifier)
}

/// Check if an expression is an equality condition (for EXPLAIN join algorithm display)
pub(crate) fn is_equality_condition(expr: &Expression) -> bool {
    match expr {
        Expression::Infix(infix) => {
            // Check for equality operator
            if infix.operator == "=" {
                // Check that both sides are column references (not literals)
                let left_is_col = matches!(
                    infix.left.as_ref(),
                    Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
                );
                let right_is_col = matches!(
                    infix.right.as_ref(),
                    Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
                );
                left_is_col && right_is_col
            } else if infix.operator.eq_ignore_ascii_case("AND") {
                // AND condition - check if any part is an equality join
                is_equality_condition(&infix.left) || is_equality_condition(&infix.right)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Extract table name from a table expression (for statistics lookup)
pub(crate) fn extract_table_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::TableSource(simple) => Some(simple.name.value.to_string()),
        Expression::JoinSource(join) => extract_table_name(&join.left),
        Expression::SubquerySource(_) => None, // Can't get stats for subquery
        _ => None,
    }
}
