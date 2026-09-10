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

//! SELECT Query Execution
//!
//! This module implements SELECT query execution including:
//! - Simple table scans
//! - WHERE clause filtering
//! - Column projection
//! - ORDER BY sorting
//! - LIMIT/OFFSET
//! - DISTINCT
//! - Aggregate functions and GROUP BY
//! - JOIN operations

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::access::handle::{
    open_query_source, open_query_table_pair, open_query_table_raw, query_table_has_cold_segments,
    QuerySourceHandle,
};
use crate::access::{index as access_index, predicate as access_predicate};
use crate::access::{projection as access_projection, scan as access_scan};
use crate::aggregation::AggregationExecutorExt;
use crate::binding::source::{
    collect_unqualified_join_columns, join_column_is_coalesced, SourceBindingExt,
};
use crate::cte::CteExecutorExt;
use crate::navigation::NavigationExecutorExt;
use crate::pipeline::ordering as pipeline_ordering;
use crate::pipeline::{distinct as pipeline_distinct, filter as pipeline_filter};
use crate::pipeline::{paging::PageWindow, projection as pipeline_projection};
use crate::pipeline::{set as pipeline_set, shape::RowShape};
use crate::subquery::SubqueryExecutorExt;
use crate::window::WindowExecutorExt;
use lru::LruCache;
use radixdb_core::SmartString;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use radixdb_core::{CompactArc, CompactVec, StringMap};
use radixdb_core::{Error, NavigationErrorCode, Result, Row, RowVec, Schema, Value, ValueSet};
use radixdb_sql::ast::*;
use radixdb_sql::token::{Position, Token, TokenType};
use radixdb_storage::mvcc::engine::ViewDefinition;
use radixdb_storage::traits::{Engine, QueryResult};

/// Maximum depth for nested views to prevent stack overflow
const MAX_VIEW_DEPTH: usize = 32;

/// Threshold below which streaming NOT EXISTS with early termination outperforms
/// bulk anti-join materialization. Queries with LIMIT below this value use the
/// streaming InHashSet path which can terminate early, while queries without LIMIT
/// (or with larger LIMIT) use bulk anti-join which is faster for full scans.
/// Based on benchmarking where streaming wins when result set is small enough
/// to benefit from early termination.
const ANTI_JOIN_LIMIT_THRESHOLD: i64 = 10000;

/// Deferred projection info: (column_indices, output_column_names)
/// Used when projection can be deferred until after ORDER BY + LIMIT
type DeferredProjection = (Vec<usize>, Vec<String>);

fn estimated_projected_inner_width(
    schema: &Schema,
    projection: Option<&super::utils::JoinProjectionIndices>,
) -> u64 {
    let Some(projection) = projection else {
        return estimated_schema_row_width(schema);
    };
    projection
        .columns
        .iter()
        .filter_map(|source| match source {
            super::operator::ColumnSource::Inner(index) => schema.columns.get(*index),
            super::operator::ColumnSource::Outer(_) => None,
        })
        .map(|column| {
            super::planner::estimated_schema_column_width(
                column.data_type,
                column.vector_dimensions,
            )
            .saturating_add(u64::from(column.nullable))
        })
        .sum::<u64>()
        .max(1)
}

struct ComplexTopNRow {
    row: Row,
    keys: Vec<Value>,
    ordinal: u64,
    specs: CompactArc<Vec<(bool, Option<bool>)>>,
}

struct ComplexOrderKeyContext<'a> {
    stmt: &'a SelectStatement,
    columns: &'a CompactArc<Vec<String>>,
    execution: &'a ExecutionContext,
    columns_lower: &'a [CompactArc<str>],
    qualified_names: Option<&'a [CompactArc<str>]>,
    correlated: bool,
}

fn compare_complex_order_keys(
    a_keys: &[Value],
    a_ordinal: u64,
    b_keys: &[Value],
    b_ordinal: u64,
    specs: &[(bool, Option<bool>)],
) -> Ordering {
    for (index, (ascending, nulls_first)) in specs.iter().copied().enumerate() {
        let a = a_keys.get(index);
        let b = b_keys.get(index);
        let a_null = a.is_none_or(Value::is_null);
        let b_null = b.is_none_or(Value::is_null);
        if a_null || b_null {
            if a_null && b_null {
                continue;
            }
            let nulls_first = nulls_first.unwrap_or(!ascending);
            return if a_null == nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        let mut ordering = a.unwrap().cmp(b.unwrap());
        if !ascending {
            ordering = ordering.reverse();
        }
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    a_ordinal.cmp(&b_ordinal)
}

impl PartialEq for ComplexTopNRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ComplexTopNRow {}

impl PartialOrd for ComplexTopNRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ComplexTopNRow {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_complex_order_keys(
            &self.keys,
            self.ordinal,
            &other.keys,
            other.ordinal,
            &self.specs,
        )
    }
}

/// Optional physical count semi-join rewrite result:
/// `(result, output columns, rows already ordered, deferred projection)`.
type CountPkSemijoinAttempt = Option<(
    Box<dyn QueryResult>,
    CompactArc<Vec<String>>,
    bool,
    Option<DeferredProjection>,
)>;

fn base_table_source(expr: &Expression) -> Option<&SimpleTableSource> {
    match expr {
        Expression::TableSource(table) => Some(table),
        Expression::Aliased(aliased) => match aliased.expression.as_ref() {
            Expression::TableSource(table) => Some(table),
            _ => None,
        },
        _ => None,
    }
}

fn is_count_star_select(stmt: &SelectStatement) -> bool {
    if stmt.columns.len() != 1 || stmt.distinct || !stmt.distinct_on.is_empty() {
        return false;
    }
    let expression = match &stmt.columns[0] {
        Expression::FunctionCall(function) => function.as_ref(),
        Expression::Aliased(aliased) => match aliased.expression.as_ref() {
            Expression::FunctionCall(function) => function.as_ref(),
            _ => return false,
        },
        _ => return false,
    };
    expression.function.eq_ignore_ascii_case("COUNT")
        && !expression.is_distinct
        && expression.filter.is_none()
        && expression.order_by.is_empty()
        && matches!(expression.arguments.as_slice(), [Expression::Star(_)])
}

/// Return the single direct `COUNT(alias.column)` argument. Expressions,
/// unqualified identifiers and navigation paths deliberately stay on the
/// general aggregate path because their NULL/multiplicity semantics require
/// full binding and evaluation.
fn count_qualified_column_select(stmt: &SelectStatement) -> Option<&QualifiedIdentifier> {
    if stmt.columns.len() != 1 || stmt.distinct || !stmt.distinct_on.is_empty() {
        return None;
    }
    let function = match &stmt.columns[0] {
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

fn is_single_equality_join_condition(condition: Option<&Expression>) -> bool {
    let Some(Expression::Infix(infix)) = condition else {
        return false;
    };
    if infix.op_type != InfixOperator::Equal {
        return false;
    }
    matches!(
        (infix.left.as_ref(), infix.right.as_ref()),
        (
            Expression::Identifier(_) | Expression::QualifiedIdentifier(_),
            Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
        )
    )
}

fn qualified_column_for_alias(expr: &Expression, alias: &str) -> Option<String> {
    let Expression::QualifiedIdentifier(identifier) = expr else {
        return None;
    };
    if identifier.is_multi_part_path() {
        return None;
    }
    identifier
        .qualifier
        .value
        .eq_ignore_ascii_case(alias)
        .then(|| identifier.name.value_lower.to_string())
}

/// Recognize the null-rejection predicate that turns a LEFT JOIN into an
/// anti-join. Requiring an explicitly qualified right-side column keeps the
/// rewrite out of ambiguous SQL; schema validation later proves that real
/// inner rows can never satisfy the predicate themselves.
fn extract_right_is_null_column(expr: &Expression, right_alias: &str) -> Option<String> {
    let Expression::Infix(infix) = expr else {
        return None;
    };
    if infix.op_type != InfixOperator::Is
        || !matches!(infix.right.as_ref(), Expression::NullLiteral(_))
    {
        return None;
    }
    qualified_column_for_alias(infix.left.as_ref(), right_alias)
}

fn extract_qualified_join_columns(
    condition: &Expression,
    left_alias: &str,
    right_alias: &str,
) -> Option<(String, String)> {
    let Expression::Infix(infix) = condition else {
        return None;
    };
    if infix.op_type != InfixOperator::Equal {
        return None;
    }
    if let (Some(left), Some(right)) = (
        qualified_column_for_alias(infix.left.as_ref(), left_alias),
        qualified_column_for_alias(infix.right.as_ref(), right_alias),
    ) {
        return Some((left, right));
    }
    Some((
        qualified_column_for_alias(infix.right.as_ref(), left_alias)?,
        qualified_column_for_alias(infix.left.as_ref(), right_alias)?,
    ))
}

struct CollectedTableRows {
    rows: RowVec,
    window_presorted_state: Option<WindowPreSortedState>,
    window_pregrouped_state: Option<WindowPreGroupedState>,
    source_columns: Option<Vec<String>>,
}

impl CollectedTableRows {
    fn full(rows: RowVec) -> Self {
        Self {
            rows,
            window_presorted_state: None,
            window_pregrouped_state: None,
            source_columns: None,
        }
    }

    fn full_presorted(rows: RowVec, state: WindowPreSortedState) -> Self {
        Self {
            rows,
            window_presorted_state: Some(state),
            window_pregrouped_state: None,
            source_columns: None,
        }
    }

    fn full_pregrouped(rows: RowVec, state: WindowPreGroupedState) -> Self {
        Self {
            rows,
            window_presorted_state: None,
            window_pregrouped_state: Some(state),
            source_columns: None,
        }
    }

    fn projected(rows: RowVec, source_columns: Vec<String>) -> Self {
        Self {
            rows,
            window_presorted_state: None,
            window_pregrouped_state: None,
            source_columns: Some(source_columns),
        }
    }
}

/// Type alias for select execution results: (result, column_names, limit_offset_applied, deferred_projection)
/// Using CompactArc<Vec<String>> for column names enables zero-copy sharing across query execution.
/// The deferred_projection field is Some when projection should be applied after ORDER BY + LIMIT.
type SelectResult = Result<(
    Box<dyn QueryResult>,
    CompactArc<Vec<String>>,
    bool,
    Option<DeferredProjection>,
)>;

type CertifiedJoinLeaf = (Box<dyn QueryResult>, Vec<String>);

use super::context::{
    clear_batch_aggregate_cache, clear_batch_aggregate_info_cache, clear_count_counter_cache,
    clear_exists_correlation_cache, clear_exists_fetcher_cache, clear_exists_index_cache,
    clear_exists_pred_key_cache, clear_exists_predicate_cache, clear_exists_schema_cache,
    ExecutionContext,
};
use super::expression::{
    compile_expression_with_context, CompiledEvaluator, ExecuteContext, ExprVM, ExpressionEval,
    JoinFilter, RowFilter, SharedProgram,
};
use super::index_optimizer::IndexOptimizerExt;
use super::join_executor::{JoinExecutor, JoinInputOrderings, StreamingJoinRequest};
use super::join_graph::{JoinReorderBarrier, LogicalJoinGraph};
use super::operator::{ColumnInfo, Operator, QueryResultOperator};
use super::operators::hash_join::JoinType as OperatorJoinType;
use super::operators::index_nested_loop::{
    BatchIndexNestedLoopJoinOperator, IndexLookupStrategy, IndexNestedLoopJoinOperator,
};
use super::operators::{
    BloomFilterOperator, CountIntegerAntiJoinOperator, CountPkSemiJoinOperator,
    IntegerAntiJoinLookup,
};
use super::parallel::{self, ParallelConfig};
use super::planner::{estimated_schema_row_width, IndexedJoinCostInput};
use super::query_classification::{get_classification, QueryClassification};
use super::result::{
    CertifiedOrderedResult, DeferredExecutorResult, DeferredFilteredResult, ExecResult,
    ExecutorResult, ExprMappedResult, FilteredResult, LimitedResult, OperatorExecutorResult,
    OrderedResult, ProjectedResult, RadixOrderSpec, ScannerResult, StreamingProjectionResult,
    StreamingRowsResult, TopNResult,
};
use super::utils::{
    add_table_qualifier, build_column_index_map, collect_table_qualifiers,
    combine_predicates_with_and, compare_values, dummy_token, expression_contains_aggregate,
    extract_base_column_name, extract_join_keys_and_residual, filter_references_column,
    flatten_and_predicates, get_table_alias_from_expr, strip_table_qualifier,
    substitute_filter_column,
};
use super::utils::{compute_join_projection, JoinProjectionIndices};
use super::window::{WindowPreGroupedState, WindowPreSortedState};
use super::Executor;
use crate::optimizer::bloom::BloomFilterBuilder;

/// Pre-computed column name mappings for correlated subqueries.
/// Uses CompactArc<str> for zero-cost cloning in the per-row inner loop.
struct ColumnKeyMapping {
    /// Column index in the row
    index: usize,
    /// Lowercase column name (e.g., "id")
    col_lower: CompactArc<str>,
    /// Qualified name with table alias (e.g., "c.id")
    qualified_name: Option<CompactArc<str>>,
    /// Unqualified part if original had a dot (e.g., "id" from "table.id")
    unqualified_part: Option<CompactArc<str>>,
}

impl ColumnKeyMapping {
    /// Build column key mappings from column names and optional table alias.
    /// This pre-computes all the string transformations needed for correlated subquery
    /// outer row context, avoiding per-row allocations.
    fn build_mappings(columns: &[String], table_alias: Option<&str>) -> Vec<ColumnKeyMapping> {
        columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| {
                let col_lower: CompactArc<str> = CompactArc::from(col_name.to_lowercase().as_str());
                let qualified_name = table_alias
                    .map(|alias| CompactArc::from(format!("{}.{}", alias, col_lower).as_str()));
                let unqualified_part = col_name.rfind('.').map(|dot_idx| {
                    CompactArc::from(col_name[dot_idx + 1..].to_lowercase().as_str())
                });
                ColumnKeyMapping {
                    index: i,
                    col_lower,
                    qualified_name,
                    unqualified_part,
                }
            })
            .collect()
    }
}

/// Collect the visible relation names owned by one side of a JOIN node.
///
/// A left-deep `JoinSource` has no single alias, so predicate ownership must be
/// decided against the complete relation set rather than one `left_alias`.
fn collect_join_relation_aliases(expr: &Expression, aliases: &mut FxHashSet<String>) {
    match expr {
        Expression::TableSource(source) => {
            aliases.insert(
                source
                    .alias
                    .as_ref()
                    .map(|alias| alias.value_lower.to_string())
                    .unwrap_or_else(|| source.name.value_lower.to_string()),
            );
        }
        Expression::JoinSource(join) => {
            collect_join_relation_aliases(&join.left, aliases);
            collect_join_relation_aliases(&join.right, aliases);
        }
        // An explicit alias is a relation boundary and hides names inside the
        // wrapped source from its parent scope.
        Expression::Aliased(aliased) => {
            aliases.insert(aliased.alias.value_lower.to_string());
        }
        Expression::SubquerySource(source) => {
            if let Some(alias) = &source.alias {
                aliases.insert(alias.value_lower.to_string());
            }
        }
        Expression::FunctionTableSource(source) => {
            aliases.insert(
                source
                    .alias
                    .as_ref()
                    .map(|alias| alias.value_lower.to_string())
                    .unwrap_or_else(|| source.function.value_lower.to_string()),
            );
        }
        Expression::ValuesSource(source) => {
            if let Some(alias) = &source.alias {
                aliases.insert(alias.value_lower.to_string());
            }
        }
        Expression::CteReference(source) => {
            aliases.insert(
                source
                    .alias
                    .as_ref()
                    .map(|alias| alias.value_lower.to_string())
                    .unwrap_or_else(|| source.name.value_lower.to_string()),
            );
        }
        _ => {}
    }
}

/// Return true only for a complete INNER component whose leaves are ordinary
/// table sources and whose edges have explicit ON predicates. Everything that
/// can change visibility or NULL-extension remains a hard reorder boundary.
fn is_reorderable_inner_component(expression: &Expression) -> bool {
    match expression {
        Expression::TableSource(_) => true,
        Expression::JoinSource(join) => {
            join.join_type.eq_ignore_ascii_case("INNER")
                && join.condition.is_some()
                && join.using_columns.is_empty()
                && is_reorderable_inner_component(&join.left)
                && is_reorderable_inner_component(&join.right)
        }
        _ => false,
    }
}

/// Verify the structural candidate against the already-bound logical graph.
/// This prevents the physical planner from independently inventing a relation
/// component after the binder has classified an edge as a barrier.
fn graph_certifies_inner_component(expression: &Expression, graph: &LogicalJoinGraph) -> bool {
    let mut aliases = FxHashSet::default();
    collect_join_relation_aliases(expression, &mut aliases);
    if aliases.len() < 2 {
        return false;
    }
    let ordinals = graph
        .relations
        .iter()
        .filter_map(|relation| {
            relation
                .visible_name
                .as_ref()
                .filter(|name| aliases.contains(name.as_str()))
                .map(|_| relation.ordinal)
        })
        .collect::<FxHashSet<_>>();
    if ordinals.len() != aliases.len() {
        return false;
    }

    let mut internal_edges = 0usize;
    for edge in graph.edges.iter() {
        let inside = edge
            .left_relations
            .iter()
            .chain(edge.right_relations.iter())
            .all(|ordinal| ordinals.contains(ordinal));
        if !inside {
            continue;
        }
        if edge.barrier != JoinReorderBarrier::ReorderableInner {
            return false;
        }
        internal_edges += 1;
    }
    internal_edges == ordinals.len().saturating_sub(1)
}

/// Flatten one already-proven INNER component without splitting its ON
/// predicates. Preserving each complete predicate avoids changing residual
/// evaluation or match semantics when a multi-relation predicate is attached
/// to the first edge where all of its inputs are available.
fn flatten_reorderable_inner_component(
    expression: &Expression,
    leaves: &mut Vec<Expression>,
    conditions: &mut Vec<Expression>,
    join_token: &mut Option<Token>,
) {
    match expression {
        Expression::JoinSource(join) => {
            if join_token.is_none() {
                *join_token = Some(join.token.clone());
            }
            flatten_reorderable_inner_component(&join.left, leaves, conditions, join_token);
            flatten_reorderable_inner_component(&join.right, leaves, conditions, join_token);
            conditions.push(
                join.condition
                    .as_deref()
                    .expect("reorderable INNER edge has an ON predicate")
                    .clone(),
            );
        }
        _ => leaves.push(expression.clone()),
    }
}

/// Extract table-local WHERE conjuncts for cardinality estimation only. The
/// original WHERE remains on the statement and is evaluated normally; this
/// function never moves predicates across an operator boundary.
fn local_reorder_filter(where_clause: Option<&Expression>, alias: &str) -> Option<Expression> {
    let alias = alias.to_lowercase();
    let predicates = where_clause
        .map(flatten_and_predicates)
        .unwrap_or_default()
        .into_iter()
        .filter(|predicate| {
            let qualifiers = collect_table_qualifiers(predicate);
            qualifiers.len() == 1 && qualifiers.contains(&alias)
        })
        .map(|predicate| strip_table_qualifier(&predicate, &alias))
        .collect::<Vec<_>>();
    combine_predicates_with_and(predicates)
}

fn collect_qualified_columns_for_alias(
    expression: &Expression,
    alias: &str,
    columns: &mut FxHashSet<String>,
) {
    radixdb_sql::ast::walk_expression_tree(expression, &mut |node| {
        if let Expression::QualifiedIdentifier(identifier) = node {
            if identifier.qualifier.value.eq_ignore_ascii_case(alias) {
                columns.insert(identifier.name.value_lower.to_string());
            }
        }
    });
}

fn equality_columns_for_alias(
    condition: &Expression,
    alias: &str,
    joined: &FxHashSet<String>,
) -> Vec<String> {
    let mut columns = Vec::new();
    for predicate in flatten_and_predicates(condition) {
        let Expression::Infix(infix) = predicate else {
            continue;
        };
        if infix.op_type != InfixOperator::Equal {
            continue;
        }
        let pair = match (infix.left.as_ref(), infix.right.as_ref()) {
            (Expression::QualifiedIdentifier(left), Expression::QualifiedIdentifier(right)) => {
                Some((left, right))
            }
            _ => None,
        };
        let Some((left, right)) = pair else {
            continue;
        };
        if left.qualifier.value.eq_ignore_ascii_case(alias)
            && joined.contains(right.qualifier.value_lower.as_str())
        {
            columns.push(left.name.value_lower.to_string());
        } else if right.qualifier.value.eq_ignore_ascii_case(alias)
            && joined.contains(left.qualifier.value_lower.as_str())
        {
            columns.push(right.name.value_lower.to_string());
        }
    }
    columns.sort_unstable();
    columns.dedup();
    columns
}

fn local_equality_filter_columns(filter: &Expression) -> Vec<String> {
    let mut columns = Vec::new();
    for predicate in flatten_and_predicates(filter) {
        let Expression::Infix(infix) = predicate else {
            continue;
        };
        if infix.op_type != InfixOperator::Equal {
            continue;
        }
        let column = match (infix.left.as_ref(), infix.right.as_ref()) {
            (Expression::Identifier(identifier), right)
                if !matches!(
                    right,
                    Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
                ) =>
            {
                Some(identifier.value_lower.to_string())
            }
            (left, Expression::Identifier(identifier))
                if !matches!(
                    left,
                    Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
                ) =>
            {
                Some(identifier.value_lower.to_string())
            }
            _ => None,
        };
        if let Some(column) = column {
            columns.push(column);
        }
    }
    columns.sort_unstable();
    columns.dedup();
    columns
}

#[derive(Debug, Clone)]
struct ReorderLeafEstimate {
    table_name: String,
    rows: u64,
    base_rows: u64,
    pages: u64,
    row_width: u64,
    projected_width: u64,
    filter_indexed: bool,
}

impl ReorderLeafEstimate {
    fn root_cost(&self) -> u64 {
        let read_cost = if self.filter_indexed {
            4096u64.saturating_add(self.rows.saturating_mul(self.row_width))
        } else {
            self.pages.saturating_mul(4096)
        };
        read_cost.saturating_add(self.rows.saturating_mul(self.projected_width))
    }
}

#[derive(Debug, Clone, Copy)]
struct ReorderEdgeEstimate {
    cost: u64,
    output_rows: u64,
}

thread_local! {
    /// JOIN subtrees covered by the current whole-component planning pass.
    /// The scope is request/thread local and prevents recursive left-deep
    /// execution from re-planning every already-ordered prefix.
    static PLANNED_JOIN_SUBTREES: RefCell<Vec<Expression>> = const { RefCell::new(Vec::new()) };
    /// One top-level ORDER BY requirement inherited by recursive JOIN prefixes.
    /// It is installed only for a simple qualified ascending non-NULL key and
    /// consumed fail-closed by a matching ordered-index leaf.
    static JOIN_INDEX_ORDER_REQUIREMENTS: RefCell<Vec<JoinIndexOrderRequirement>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone)]
struct JoinIndexOrderRequirement {
    qualifier: String,
    column: String,
}

struct JoinIndexOrderScope;

impl JoinIndexOrderScope {
    fn install(stmt: &SelectStatement, classification: &QueryClassification) -> Option<Self> {
        if JOIN_INDEX_ORDER_REQUIREMENTS.with(|requirements| !requirements.borrow().is_empty())
            || stmt.limit.is_none()
            || stmt.order_by.len() != 1
            || classification.has_group_by
            || classification.has_aggregation
            || classification.has_window_functions
            || classification.has_distinct
        {
            return None;
        }
        let order = &stmt.order_by[0];
        if !order.ascending || order.nulls_first == Some(true) {
            return None;
        }
        let Expression::QualifiedIdentifier(identifier) = &order.expression else {
            return None;
        };
        JOIN_INDEX_ORDER_REQUIREMENTS.with(|requirements| {
            requirements.borrow_mut().push(JoinIndexOrderRequirement {
                qualifier: identifier.qualifier.value_lower.to_string(),
                column: identifier.name.value_lower.to_string(),
            });
        });
        Some(Self)
    }
}

impl Drop for JoinIndexOrderScope {
    fn drop(&mut self) {
        JOIN_INDEX_ORDER_REQUIREMENTS.with(|requirements| {
            requirements.borrow_mut().pop();
        });
    }
}

fn active_join_index_order_requirement() -> Option<JoinIndexOrderRequirement> {
    JOIN_INDEX_ORDER_REQUIREMENTS.with(|requirements| requirements.borrow().last().cloned())
}

struct PlannedJoinExecutionScope {
    previous_len: usize,
}

impl PlannedJoinExecutionScope {
    fn install(expression: &Expression) -> Self {
        fn collect(expression: &Expression, output: &mut Vec<Expression>) {
            if let Expression::JoinSource(join) = expression {
                output.push(expression.clone());
                collect(&join.left, output);
                collect(&join.right, output);
            }
        }

        PLANNED_JOIN_SUBTREES.with(|subtrees| {
            let mut subtrees = subtrees.borrow_mut();
            let previous_len = subtrees.len();
            collect(expression, &mut subtrees);
            Self { previous_len }
        })
    }
}

impl Drop for PlannedJoinExecutionScope {
    fn drop(&mut self) {
        PLANNED_JOIN_SUBTREES.with(|subtrees| {
            subtrees.borrow_mut().truncate(self.previous_len);
        });
    }
}

fn join_subtree_has_physical_plan(join_source: &JoinTableSource) -> bool {
    let expression = Expression::JoinSource(Box::new(join_source.clone()));
    PLANNED_JOIN_SUBTREES.with(|subtrees| {
        subtrees
            .borrow()
            .iter()
            .any(|planned| planned == &expression)
    })
}

fn localize_join_side_filter(
    filter: Option<&Expression>,
    relation_aliases: &FxHashSet<String>,
) -> Option<Expression> {
    let filter = filter?;
    if relation_aliases.len() == 1 {
        let alias = relation_aliases.iter().next().expect("one relation alias");
        Some(strip_table_qualifier(filter, alias))
    } else {
        // Keep qualifiers while the filter descends through a nested JOIN. Its
        // child node will partition the predicate again and strip the qualifier
        // only at the owning leaf relation.
        Some(filter.clone())
    }
}

struct JoinDependencyCollector<'a> {
    left_aliases: &'a FxHashSet<String>,
    right_aliases: &'a FxHashSet<String>,
    left_columns: &'a FxHashMap<String, usize>,
    right_columns: &'a FxHashMap<String, usize>,
    left: Vec<Expression>,
    right: Vec<Expression>,
    seen_left: FxHashSet<String>,
    seen_right: FxHashSet<String>,
    blocked: bool,
}

impl JoinDependencyCollector<'_> {
    fn collect(&mut self, expression: &Expression) {
        radixdb_sql::ast::walk_expression_tree(expression, &mut |node| {
            self.collect_reference(node)
        });
    }

    fn collect_reference(&mut self, node: &Expression) {
        match node {
            Expression::Star(_) | Expression::QualifiedStar(_) => self.blocked = true,
            Expression::Exists(_) | Expression::AllAny(_) | Expression::ScalarSubquery(_) => {
                // A subquery owns another lexical scope. Until dependencies are
                // represented by the logical binder, retaining the complete row is
                // safer than projecting an inner-scope identifier by accident.
                self.blocked = true;
            }
            Expression::QualifiedIdentifier(identifier) => {
                let qualifier = identifier.qualifier.value_lower.as_str();
                let key = identifier.to_string().to_lowercase();
                if self.left_aliases.contains(qualifier) {
                    if self.seen_left.insert(key) {
                        self.left.push(node.clone());
                    }
                } else if self.right_aliases.contains(qualifier) && self.seen_right.insert(key) {
                    self.right.push(node.clone());
                }
            }
            Expression::Identifier(identifier) => {
                let name = identifier.value_lower.to_string();
                let left_count = self.left_columns.get(&name).copied().unwrap_or(0);
                let right_count = self.right_columns.get(&name).copied().unwrap_or(0);
                match (left_count, right_count) {
                    (1, 0) if self.seen_left.insert(name.clone()) => self.left.push(node.clone()),
                    (0, 1) if self.seen_right.insert(name) => self.right.push(node.clone()),
                    (0, 0) => {
                        // This may be a SELECT alias used by ORDER BY. The SELECT
                        // expression itself contributes the real dependencies.
                    }
                    _ => self.blocked = true,
                }
            }
            _ => {}
        }
    }
}

struct JoinDependencyProjections {
    input_left: Option<Vec<Expression>>,
    input_right: Option<Vec<Expression>>,
    output_left: Option<Vec<Expression>>,
    output_right: Option<Vec<Expression>>,
}

fn join_dependency_projections(
    dependencies: Option<&[Expression]>,
    join_source: &JoinTableSource,
    left_aliases: &FxHashSet<String>,
    right_aliases: &FxHashSet<String>,
    left_columns: &FxHashMap<String, usize>,
    right_columns: &FxHashMap<String, usize>,
) -> JoinDependencyProjections {
    if !join_source.using_columns.is_empty()
        || join_source
            .join_type
            .to_ascii_uppercase()
            .contains("NATURAL")
    {
        return JoinDependencyProjections {
            input_left: None,
            input_right: None,
            output_left: None,
            output_right: None,
        };
    }
    let Some(dependencies) = dependencies else {
        return JoinDependencyProjections {
            input_left: None,
            input_right: None,
            output_left: None,
            output_right: None,
        };
    };

    let mut collector = JoinDependencyCollector {
        left_aliases,
        right_aliases,
        left_columns,
        right_columns,
        left: Vec::new(),
        right: Vec::new(),
        seen_left: FxHashSet::default(),
        seen_right: FxHashSet::default(),
        blocked: false,
    };

    for expression in dependencies {
        collector.collect_reference(expression);
    }
    if collector.blocked {
        return JoinDependencyProjections {
            input_left: None,
            input_right: None,
            output_left: None,
            output_right: None,
        };
    }

    let output_left = (!collector.left.is_empty()).then(|| collector.left.clone());
    let output_right = (!collector.right.is_empty()).then(|| collector.right.clone());
    if let Some(condition) = join_source.condition.as_deref() {
        collector.collect(condition);
    }
    if collector.blocked {
        return JoinDependencyProjections {
            input_left: None,
            input_right: None,
            output_left,
            output_right,
        };
    }

    JoinDependencyProjections {
        input_left: (!collector.left.is_empty()).then_some(collector.left),
        input_right: (!collector.right.is_empty()).then_some(collector.right),
        output_left,
        output_right,
    }
}

fn compute_join_dependency_projection(
    left: Option<&[Expression]>,
    right: Option<&[Expression]>,
    outer_columns: &[String],
    inner_columns: &[String],
) -> Option<JoinProjectionIndices> {
    let expressions: Vec<Expression> = left
        .into_iter()
        .flatten()
        .chain(right.into_iter().flatten())
        .cloned()
        .collect();
    if expressions.is_empty() {
        return None;
    }
    let mut projection = compute_join_projection(&expressions, outer_columns, inner_columns)?;
    projection.output_columns = expressions.iter().map(expression_binding_name).collect();
    Some(projection)
}

fn expression_binding_name(expression: &Expression) -> String {
    match expression {
        Expression::Identifier(identifier) => identifier.value.to_string(),
        Expression::QualifiedIdentifier(identifier) => {
            format!("{}.{}", identifier.qualifier.value, identifier.name.value)
        }
        Expression::Aliased(aliased) => aliased.alias.value.to_string(),
        _ => expression.to_string(),
    }
}

struct CachedJoinDependencyProjection {
    classification: Arc<QueryClassification>,
    columns: Vec<String>,
    projection: Option<Arc<JoinProjectionIndices>>,
}

thread_local! {
    static JOIN_DEPENDENCY_PROJECTION_CACHE: RefCell<LruCache<u64, Vec<CachedJoinDependencyProjection>>> =
        RefCell::new(LruCache::new(NonZeroUsize::new(512).unwrap()));
}

pub(crate) fn clear_join_dependency_projection_cache() {
    JOIN_DEPENDENCY_PROJECTION_CACHE.with(|cache| cache.borrow_mut().clear());
}

fn cached_join_dependency_projection(
    classification: &Arc<QueryClassification>,
    left: Option<&[Expression]>,
    right: Option<&[Expression]>,
    outer_columns: &[String],
    inner_columns: &[String],
) -> Option<Arc<JoinProjectionIndices>> {
    let mut hasher = FxHasher::default();
    Arc::as_ptr(classification).hash(&mut hasher);
    outer_columns.len().hash(&mut hasher);
    inner_columns.len().hash(&mut hasher);
    for column in outer_columns.iter().chain(inner_columns) {
        column.hash(&mut hasher);
    }
    let key = hasher.finish();

    JOIN_DEPENDENCY_PROJECTION_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(bucket) = cache.get(&key) {
            if let Some(entry) = bucket.iter().find(|entry| {
                Arc::ptr_eq(&entry.classification, classification)
                    && entry.columns.len() == outer_columns.len() + inner_columns.len()
                    && entry
                        .columns
                        .iter()
                        .zip(outer_columns.iter().chain(inner_columns))
                        .all(|(cached, actual)| cached == actual)
            }) {
                return entry.projection.clone();
            }
        }

        let projection =
            compute_join_dependency_projection(left, right, outer_columns, inner_columns)
                .map(Arc::new);
        let entry = CachedJoinDependencyProjection {
            classification: Arc::clone(classification),
            columns: outer_columns.iter().chain(inner_columns).cloned().collect(),
            projection: projection.clone(),
        };
        if let Some(bucket) = cache.get_mut(&key) {
            bucket.push(entry);
        } else {
            cache.put(key, vec![entry]);
        }
        projection
    })
}

/// Partition WHERE predicates by complete JOIN-side relation sets.
/// Returns (left_filter, right_filter, cross_table_filter).
///
/// Unqualified predicates stay at the current JOIN boundary until the binder
/// can prove one owner. Assigning them to the left side arbitrarily is both an
/// unsafe semantic guess and a source of unstable plans.
fn partition_where_for_join(
    where_clause: &Expression,
    join_source: &JoinTableSource,
    left_expr: &Expression,
    right_expr: &Expression,
    left_columns: &FxHashMap<String, usize>,
    right_columns: &FxHashMap<String, usize>,
) -> Result<(Option<Expression>, Option<Expression>, Option<Expression>)> {
    let mut left_aliases = FxHashSet::default();
    let mut right_aliases = FxHashSet::default();
    collect_join_relation_aliases(left_expr, &mut left_aliases);
    collect_join_relation_aliases(right_expr, &mut right_aliases);

    // Collect AND-ed predicates
    let predicates = flatten_and_predicates(where_clause);

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut cross_preds = Vec::new();

    for pred in predicates {
        // A nested SELECT owns a separate lexical scope. Qualifier collection
        // for the current JOIN cannot classify its outer references safely, so
        // the predicate must remain post-join where the complete row is present.
        if Executor::has_subqueries(&pred) {
            cross_preds.push(pred);
            continue;
        }
        let qualifiers = collect_table_qualifiers(&pred);
        let mut unqualified_columns = FxHashSet::default();
        collect_unqualified_join_columns(&pred, &mut unqualified_columns);

        let mut refs_left = qualifiers.iter().any(|alias| left_aliases.contains(alias));
        let mut refs_right = qualifiers.iter().any(|alias| right_aliases.contains(alias));
        let has_unknown_qualifier = qualifiers
            .iter()
            .any(|alias| !left_aliases.contains(alias) && !right_aliases.contains(alias));

        for column in &unqualified_columns {
            let left_count = left_columns.get(column).copied().unwrap_or(0);
            let right_count = right_columns.get(column).copied().unwrap_or(0);
            let total_count = left_count.saturating_add(right_count);
            if total_count > 1 {
                if join_column_is_coalesced(join_source, column, left_columns, right_columns) {
                    // NATURAL/USING publishes one coalesced output column. Keep
                    // its predicate at the JOIN boundary; it no longer belongs
                    // to either physical input independently.
                    refs_left = true;
                    refs_right = true;
                    continue;
                }
                return Err(Error::AmbiguousColumn(column.clone()));
            }
            refs_left |= left_count == 1;
            refs_right |= right_count == 1;
        }

        if has_unknown_qualifier || (refs_left && refs_right) {
            // References both tables - must be applied post-join
            cross_preds.push(pred);
        } else if refs_left {
            // Only references left table - push to left
            left_preds.push(pred);
        } else if refs_right {
            // Only references right table - push to right
            right_preds.push(pred);
        } else {
            // Constant or unresolved column reference: keep it at the JOIN
            // boundary so normal expression binding reports an unknown column.
            cross_preds.push(pred);
        }
    }

    Ok((
        combine_predicates_with_and(left_preds),
        combine_predicates_with_and(right_preds),
        combine_predicates_with_and(cross_preds),
    ))
}

fn is_internal_reference_navigation_join(join: &JoinTableSource) -> bool {
    matches!(
        join.right.as_ref(),
        Expression::TableSource(table)
            if table
                .alias
                .as_ref()
                .is_some_and(|alias| alias.value_lower().starts_with("__radix_nav_"))
    )
}

fn validate_reference_target_uniqueness(
    rows: &[Row],
    key_index: usize,
    target_column: &str,
) -> Result<()> {
    let mut visible_keys = ValueSet::default();
    for row in rows {
        let Some(key) = row.get(key_index) else {
            return Err(Error::internal(
                "generated reference join target row omitted its key",
            ));
        };
        if !key.is_null() && !visible_keys.insert(key.clone()) {
            return Err(Error::navigation(
                NavigationErrorCode::TargetNotUnique,
                format!("reference target {target_column} returned more than one visible row"),
            ));
        }
    }
    Ok(())
}

fn validate_reference_join_matches(
    rows: &RowVec,
    source_key_index: usize,
    target_key_index: usize,
    source_column: &str,
    target_column: &str,
) -> Result<()> {
    for (_, row) in rows.iter() {
        let source = row.get(source_key_index).ok_or_else(|| {
            Error::internal("generated reference join source row omitted its key")
        })?;
        let target = row.get(target_key_index).ok_or_else(|| {
            Error::internal("generated reference join result omitted its target key")
        })?;
        if !source.is_null() && target.is_null() {
            return Err(Error::navigation(
                NavigationErrorCode::TargetMissing,
                format!("non-null reference {source_column} has no target in {target_column}"),
            ));
        }
    }
    Ok(())
}

include!("select_entry.rs");
include!("plugin_planner.rs");
include!("table_scan.rs");
include!("join_execute.rs");
include!("join_plan.rs");
include!("materialize_project.rs");
include!("utility.rs");

#[cfg(test)]
mod tests;
