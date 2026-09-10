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

//! Query Classification Cache
//!
//! This module caches pre-computed characteristics of SELECT statements to avoid
//! repeated AST traversals. For example, determining if a query has aggregation
//! requires walking the entire SELECT column list - we cache this result.
//!
//! # Performance Impact
//!
//! Before: has_aggregation() called 3-5 times per query, each traversing all columns
//! After: Single traversal on first access, O(1) lookup thereafter

use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;
use rustc_hash::FxHasher;

use radixdb_sql::ast::{Expression, GroupByModifier, SelectStatement};

use super::join_graph::LogicalJoinGraph;

/// Maximum number of cached query classifications (LRU eviction)
const CLASSIFICATION_CACHE_SIZE: usize = 512;

/// Global cache for query classifications
type ClassificationBucket = Vec<(SelectStatement, Arc<QueryClassification>)>;
static CLASSIFICATION_CACHE: Mutex<Option<LruCache<u64, ClassificationBucket>>> = Mutex::new(None);

/// Clear the classification cache. Call on database drop to release memory.
pub fn clear_classification_cache() {
    let mut guard = CLASSIFICATION_CACHE.lock();
    *guard = None;
}

/// Pre-computed characteristics of a SELECT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryClassification {
    // === Basic query structure ===
    /// Whether the query has aggregate functions (COUNT, SUM, etc.)
    pub has_aggregation: bool,
    /// Whether the query has window functions (ROW_NUMBER, etc.)
    pub has_window_functions: bool,
    /// Whether the query has GROUP BY clause
    pub has_group_by: bool,
    /// Whether the query has ORDER BY clause
    pub has_order_by: bool,
    /// Whether the query has LIMIT clause
    pub has_limit: bool,
    /// Whether the query has OFFSET clause
    pub has_offset: bool,
    /// Whether the query has DISTINCT
    pub has_distinct: bool,
    /// Whether the query has DISTINCT ON (expr, ...)
    pub has_distinct_on: bool,
    /// Whether the query has set operations (UNION, etc.)
    pub has_set_operations: bool,
    /// Whether the query has HAVING clause
    pub has_having: bool,

    // === SELECT clause analysis ===
    /// Whether the SELECT is `*` (all columns)
    pub is_select_star: bool,
    /// Whether SELECT has scalar subqueries
    pub select_has_scalar_subqueries: bool,
    /// Current-scope column references required after the JOIN tree. `None`
    /// means that a star or nested query makes dependency projection unsafe.
    pub join_projection_dependencies: Option<Arc<Vec<Expression>>>,

    // === JOIN analysis ===
    /// Whether the query has any joins
    pub has_joins: bool,
    /// Number of JOIN edges in the complete table-expression tree.
    pub join_count: usize,
    /// Whether at least one JOIN edge is an outer-join reorder barrier.
    pub has_outer_joins: bool,
    /// Whether the table expression contains a derived/subquery boundary.
    pub has_derived_tables: bool,
    /// INNER equality edges that may participate in a reorder component.
    pub reorderable_join_count: usize,
    /// Stable relation identities, complete edge relation sets and explicit
    /// reorder barriers for the current table-expression scope.
    #[doc(hidden)]
    pub logical_join_graph: Option<Arc<LogicalJoinGraph>>,

    // === WHERE clause analysis ===
    /// Whether query has a WHERE clause
    pub has_where: bool,
    /// Whether WHERE clause has parameters ($1, $2, etc.)
    pub where_has_parameters: bool,
    /// Whether WHERE clause has any subqueries
    pub where_has_subqueries: bool,
    /// Whether WHERE has correlated subqueries (references outer columns)
    pub where_has_correlated_subqueries: bool,
    /// Whether WHERE or SELECT has non-deterministic functions (NOW, RANDOM, UUID, etc.)
    /// that return different values per execution and must not be semantically cached
    pub has_nondeterministic_functions: bool,

    // === SELECT clause correlated subqueries ===
    /// Whether any SELECT column has correlated subqueries
    pub select_has_correlated_subqueries: bool,

    // === ORDER BY analysis ===
    /// Whether ORDER BY has correlated subqueries
    pub order_by_has_correlated_subqueries: bool,
}

impl QueryClassification {
    /// Classify a SELECT statement, computing all characteristics in a single pass
    pub fn classify(stmt: &SelectStatement) -> Self {
        // Basic query structure
        let has_aggregation = Self::check_has_aggregation(stmt);
        let has_window_functions = Self::check_has_window_functions(stmt);
        let has_group_by = !stmt.group_by.columns.is_empty();
        let has_order_by = !stmt.order_by.is_empty();
        let has_limit = stmt.limit.is_some();
        let has_offset = stmt.offset.is_some();
        let has_distinct = stmt.distinct;
        let has_distinct_on = !stmt.distinct_on.is_empty();
        let has_set_operations = !stmt.set_operations.is_empty();
        let has_having = stmt.having.is_some();

        // SELECT clause analysis
        let is_select_star =
            stmt.columns.len() == 1 && matches!(stmt.columns.first(), Some(Expression::Star(_)));
        let select_has_scalar_subqueries = stmt
            .columns
            .iter()
            .any(Self::expression_has_scalar_subquery);
        let join_projection_dependencies = Self::collect_join_projection_dependencies(stmt);

        // JOIN analysis
        let (has_joins, has_outer_joins, join_count, has_derived_tables) =
            Self::analyze_table_source(&stmt.table_expr);
        let logical_join_graph = stmt
            .table_expr
            .as_deref()
            .and_then(LogicalJoinGraph::bind)
            .map(Arc::new);
        let reorderable_join_count = logical_join_graph
            .as_deref()
            .map_or(0, LogicalJoinGraph::reorderable_edge_count);

        // WHERE clause analysis
        let has_where = stmt.where_clause.is_some();
        let (
            where_has_parameters,
            where_has_subqueries,
            _,
            _,
            _,
            _,
            where_has_correlated_subqueries,
        ) = if let Some(ref where_clause) = stmt.where_clause {
            Self::analyze_where_clause(where_clause)
        } else {
            (false, false, false, false, false, false, false)
        };

        // Non-deterministic function detection (NOW, RANDOM, UUID, etc.)
        // Check both WHERE and SELECT columns — semantic cache must not serve stale
        // results when any part of the query uses non-deterministic functions.
        let has_nondeterministic_functions = stmt
            .where_clause
            .as_ref()
            .is_some_and(|wc| Self::expression_has_nondeterministic_functions(wc))
            || stmt
                .columns
                .iter()
                .any(Self::expression_has_nondeterministic_functions);

        // SELECT column correlated subquery analysis
        let select_has_correlated_subqueries = stmt
            .columns
            .iter()
            .any(Self::expression_has_correlated_subqueries);

        // ORDER BY correlated subquery analysis
        let order_by_has_correlated_subqueries = stmt
            .order_by
            .iter()
            .any(|ob| Self::expression_has_correlated_subqueries(&ob.expression));

        QueryClassification {
            has_aggregation,
            has_window_functions,
            has_group_by,
            has_order_by,
            has_limit,
            has_offset,
            has_distinct,
            has_distinct_on,
            has_set_operations,
            has_having,
            is_select_star,
            select_has_scalar_subqueries,
            join_projection_dependencies,
            has_joins,
            join_count,
            has_outer_joins,
            has_derived_tables,
            reorderable_join_count,
            logical_join_graph,
            has_where,
            where_has_parameters,
            where_has_subqueries,
            where_has_correlated_subqueries,
            has_nondeterministic_functions,
            select_has_correlated_subqueries,
            order_by_has_correlated_subqueries,
        }
    }

    fn collect_join_projection_dependencies(
        stmt: &SelectStatement,
    ) -> Option<Arc<Vec<Expression>>> {
        let mut dependencies = Vec::new();
        let mut blocked = false;
        let mut collect = |expression: &Expression| {
            radixdb_sql::ast::walk_expression_tree(expression, &mut |node| match node {
                Expression::Identifier(_) | Expression::QualifiedIdentifier(_) => {
                    dependencies.push(node.clone());
                }
                Expression::Star(_)
                | Expression::QualifiedStar(_)
                | Expression::Exists(_)
                | Expression::AllAny(_)
                | Expression::ScalarSubquery(_) => blocked = true,
                _ => {}
            });
        };

        for expression in stmt.distinct_on.iter().chain(&stmt.columns) {
            collect(expression);
        }
        if let Some(expression) = stmt.where_clause.as_deref() {
            collect(expression);
        }
        for expression in &stmt.group_by.columns {
            collect(expression);
        }
        if let GroupByModifier::GroupingSets(sets) = &stmt.group_by.modifier {
            for set in sets {
                for expression in set {
                    collect(expression);
                }
            }
        }
        if let Some(expression) = stmt.having.as_deref() {
            collect(expression);
        }
        for window in &stmt.window_defs {
            for expression in &window.partition_by {
                collect(expression);
            }
            for order in &window.order_by {
                collect(&order.expression);
            }
        }
        for order in &stmt.order_by {
            collect(&order.expression);
        }

        (!blocked).then(|| Arc::new(dependencies))
    }

    /// Analyze table source for joins and derived tables
    fn analyze_table_source(table_expr: &Option<Box<Expression>>) -> (bool, bool, usize, bool) {
        let mut has_joins = false;
        let mut has_outer_joins = false;
        let mut join_count = 0;
        let mut has_derived_tables = false;

        if let Some(ref expr) = table_expr {
            Self::analyze_table_expr_recursive(
                expr,
                &mut has_joins,
                &mut has_outer_joins,
                &mut join_count,
                &mut has_derived_tables,
            );
        }

        (has_joins, has_outer_joins, join_count, has_derived_tables)
    }

    /// Recursively analyze table expression for joins and derived tables
    fn analyze_table_expr_recursive(
        expr: &Expression,
        has_joins: &mut bool,
        has_outer_joins: &mut bool,
        join_count: &mut usize,
        has_derived_tables: &mut bool,
    ) {
        match expr {
            Expression::JoinSource(join) => {
                *has_joins = true;
                *join_count += 1;

                // Check for outer join types
                let join_type = join.join_type.to_uppercase();
                if join_type.contains("LEFT")
                    || join_type.contains("RIGHT")
                    || join_type.contains("FULL")
                {
                    *has_outer_joins = true;
                }

                // Recurse into left and right
                Self::analyze_table_expr_recursive(
                    &join.left,
                    has_joins,
                    has_outer_joins,
                    join_count,
                    has_derived_tables,
                );
                Self::analyze_table_expr_recursive(
                    &join.right,
                    has_joins,
                    has_outer_joins,
                    join_count,
                    has_derived_tables,
                );
            }
            Expression::SubquerySource(_) | Expression::ScalarSubquery(_) => {
                *has_derived_tables = true;
            }
            Expression::Aliased(aliased) => {
                Self::analyze_table_expr_recursive(
                    &aliased.expression,
                    has_joins,
                    has_outer_joins,
                    join_count,
                    has_derived_tables,
                );
            }
            _ => {}
        }
    }

    /// Analyze WHERE clause for various subquery types
    fn analyze_where_clause(expr: &Expression) -> (bool, bool, bool, bool, bool, bool, bool) {
        let has_parameters = Self::expression_has_parameters(expr);
        let mut has_exists = false;
        let mut has_in_subquery = false;
        let mut has_scalar_subquery = false;
        let mut has_all_any = false;

        Self::analyze_where_expr_recursive(
            expr,
            &mut has_exists,
            &mut has_in_subquery,
            &mut has_scalar_subquery,
            &mut has_all_any,
        );

        let has_subqueries = has_exists || has_in_subquery || has_scalar_subquery || has_all_any;

        // Check for correlated subqueries (expensive AST traversal - cached here)
        let has_correlated = Self::expression_has_correlated_subqueries(expr);

        (
            has_parameters,
            has_subqueries,
            has_exists,
            has_in_subquery,
            has_scalar_subquery,
            has_all_any,
            has_correlated,
        )
    }

    /// Recursively analyze WHERE expression for subquery types
    fn analyze_where_expr_recursive(
        expr: &Expression,
        has_exists: &mut bool,
        has_in_subquery: &mut bool,
        has_scalar_subquery: &mut bool,
        has_all_any: &mut bool,
    ) {
        radixdb_sql::ast::walk_expression_tree(expr, &mut |expression| match expression {
            Expression::Exists(_) => *has_exists = true,
            Expression::AllAny(_) => *has_all_any = true,
            Expression::ScalarSubquery(_) => *has_scalar_subquery = true,
            Expression::In(in_expr)
                if matches!(
                    in_expr.right.as_ref(),
                    Expression::ScalarSubquery(_) | Expression::SubquerySource(_)
                ) =>
            {
                *has_in_subquery = true;
            }
            _ => {}
        });
    }

    /// Check if expression contains scalar subqueries
    fn expression_has_scalar_subquery(expr: &Expression) -> bool {
        let mut found = false;
        radixdb_sql::ast::walk_expression_tree(expr, &mut |expression| {
            found |= matches!(expression, Expression::ScalarSubquery(_));
        });
        found
    }

    /// Check if any column expression contains aggregate functions
    fn check_has_aggregation(stmt: &SelectStatement) -> bool {
        !stmt.group_by.columns.is_empty()
            || stmt
                .columns
                .iter()
                .chain(&stmt.distinct_on)
                .any(Self::expression_has_aggregation)
            || stmt
                .having
                .as_deref()
                .is_some_and(Self::expression_has_aggregation)
            || stmt
                .order_by
                .iter()
                .any(|order| Self::expression_has_aggregation(&order.expression))
    }

    /// Check if an expression contains aggregate functions
    fn expression_has_aggregation(expr: &Expression) -> bool {
        match expr {
            Expression::FunctionCall(func) => {
                if is_aggregate_function(&func.function) {
                    return true;
                }
                func.arguments.iter().any(Self::expression_has_aggregation)
                    || func
                        .order_by
                        .iter()
                        .any(|order| Self::expression_has_aggregation(&order.expression))
                    || func
                        .filter
                        .as_deref()
                        .is_some_and(Self::expression_has_aggregation)
            }
            Expression::Aliased(aliased) => Self::expression_has_aggregation(&aliased.expression),
            Expression::Infix(infix) => {
                Self::expression_has_aggregation(&infix.left)
                    || Self::expression_has_aggregation(&infix.right)
            }
            Expression::Prefix(prefix) => Self::expression_has_aggregation(&prefix.right),
            Expression::Cast(cast) => Self::expression_has_aggregation(&cast.expr),
            Expression::Case(case) => {
                case.value
                    .as_deref()
                    .is_some_and(Self::expression_has_aggregation)
                    || case.when_clauses.iter().any(|w| {
                        Self::expression_has_aggregation(&w.condition)
                            || Self::expression_has_aggregation(&w.then_result)
                    })
                    || case
                        .else_value
                        .as_deref()
                        .is_some_and(Self::expression_has_aggregation)
            }
            Expression::Distinct(distinct) => Self::expression_has_aggregation(&distinct.expr),
            Expression::Between(between) => {
                Self::expression_has_aggregation(&between.expr)
                    || Self::expression_has_aggregation(&between.lower)
                    || Self::expression_has_aggregation(&between.upper)
            }
            Expression::In(in_expr) => {
                Self::expression_has_aggregation(&in_expr.left)
                    || Self::expression_has_aggregation(&in_expr.right)
            }
            Expression::Like(like) => {
                Self::expression_has_aggregation(&like.left)
                    || Self::expression_has_aggregation(&like.pattern)
                    || like
                        .escape
                        .as_deref()
                        .is_some_and(Self::expression_has_aggregation)
            }
            Expression::List(list) => list.elements.iter().any(Self::expression_has_aggregation),
            Expression::ExpressionList(list) => list
                .expressions
                .iter()
                .any(Self::expression_has_aggregation),
            Expression::ScalarSubquery(_) | Expression::SubquerySource(_) => false, // Subquery aggregates are handled separately
            _ => false,
        }
    }

    /// Check if any column expression contains window functions
    fn check_has_window_functions(stmt: &SelectStatement) -> bool {
        stmt.columns
            .iter()
            .chain(&stmt.distinct_on)
            .any(Self::expression_has_window_function)
            || stmt
                .having
                .as_deref()
                .is_some_and(Self::expression_has_window_function)
            || stmt
                .order_by
                .iter()
                .any(|order| Self::expression_has_window_function(&order.expression))
    }

    /// Check if an expression contains window functions
    fn expression_has_window_function(expr: &Expression) -> bool {
        match expr {
            Expression::Window(_) => true,
            Expression::Aliased(aliased) => {
                Self::expression_has_window_function(&aliased.expression)
            }
            Expression::Infix(infix) => {
                Self::expression_has_window_function(&infix.left)
                    || Self::expression_has_window_function(&infix.right)
            }
            Expression::Prefix(prefix) => Self::expression_has_window_function(&prefix.right),
            Expression::Cast(cast) => Self::expression_has_window_function(&cast.expr),
            Expression::Distinct(d) => Self::expression_has_window_function(&d.expr),
            Expression::FunctionCall(func) => {
                func.arguments
                    .iter()
                    .any(Self::expression_has_window_function)
                    || func
                        .order_by
                        .iter()
                        .any(|order| Self::expression_has_window_function(&order.expression))
                    || func
                        .filter
                        .as_deref()
                        .is_some_and(Self::expression_has_window_function)
            }
            Expression::Case(case) => {
                case.value
                    .as_ref()
                    .is_some_and(|v| Self::expression_has_window_function(v))
                    || case.when_clauses.iter().any(|w| {
                        Self::expression_has_window_function(&w.condition)
                            || Self::expression_has_window_function(&w.then_result)
                    })
                    || case
                        .else_value
                        .as_ref()
                        .is_some_and(|e| Self::expression_has_window_function(e))
            }
            Expression::Between(b) => {
                Self::expression_has_window_function(&b.expr)
                    || Self::expression_has_window_function(&b.lower)
                    || Self::expression_has_window_function(&b.upper)
            }
            Expression::In(i) => {
                Self::expression_has_window_function(&i.left)
                    || Self::expression_has_window_function(&i.right)
            }
            Expression::Like(l) => {
                Self::expression_has_window_function(&l.left)
                    || Self::expression_has_window_function(&l.pattern)
                    || l.escape
                        .as_ref()
                        .is_some_and(|e| Self::expression_has_window_function(e))
            }
            Expression::List(l) => l.elements.iter().any(Self::expression_has_window_function),
            Expression::ExpressionList(l) => l
                .expressions
                .iter()
                .any(Self::expression_has_window_function),
            _ => false,
        }
    }

    /// Check if an expression contains parameter placeholders ($1, $2, etc.).
    fn expression_has_parameters(expr: &Expression) -> bool {
        let mut found = false;
        radixdb_sql::ast::walk_expression_tree(expr, &mut |expression| {
            found |= matches!(expression, Expression::Parameter(_));
        });
        found
    }
    /// Check if an expression contains non-deterministic functions whose return
    /// value changes between executions (NOW, CURRENT_DATE, RANDOM, UUID, etc.).
    /// Queries with these functions must not be served from the semantic cache.
    fn expression_has_nondeterministic_functions(expr: &Expression) -> bool {
        match expr {
            Expression::FunctionCall(func) => {
                super::expression::is_non_foldable_function(&func.function)
                    || func
                        .arguments
                        .iter()
                        .any(Self::expression_has_nondeterministic_functions)
            }
            Expression::Prefix(prefix) => {
                Self::expression_has_nondeterministic_functions(&prefix.right)
            }
            Expression::Infix(infix) => {
                Self::expression_has_nondeterministic_functions(&infix.left)
                    || Self::expression_has_nondeterministic_functions(&infix.right)
            }
            Expression::In(in_expr) => {
                Self::expression_has_nondeterministic_functions(&in_expr.left)
                    || Self::expression_has_nondeterministic_functions(&in_expr.right)
            }
            Expression::List(list) => list
                .elements
                .iter()
                .any(Self::expression_has_nondeterministic_functions),
            Expression::ExpressionList(list) => list
                .expressions
                .iter()
                .any(Self::expression_has_nondeterministic_functions),
            Expression::Between(between) => {
                Self::expression_has_nondeterministic_functions(&between.expr)
                    || Self::expression_has_nondeterministic_functions(&between.lower)
                    || Self::expression_has_nondeterministic_functions(&between.upper)
            }
            Expression::Like(like) => {
                Self::expression_has_nondeterministic_functions(&like.left)
                    || Self::expression_has_nondeterministic_functions(&like.pattern)
                    || like
                        .escape
                        .as_ref()
                        .is_some_and(|e| Self::expression_has_nondeterministic_functions(e))
            }
            Expression::Case(case) => {
                case.value
                    .as_ref()
                    .is_some_and(|e| Self::expression_has_nondeterministic_functions(e))
                    || case.when_clauses.iter().any(|w| {
                        Self::expression_has_nondeterministic_functions(&w.condition)
                            || Self::expression_has_nondeterministic_functions(&w.then_result)
                    })
                    || case
                        .else_value
                        .as_ref()
                        .is_some_and(|e| Self::expression_has_nondeterministic_functions(e))
            }
            Expression::Aliased(aliased) => {
                Self::expression_has_nondeterministic_functions(&aliased.expression)
            }
            Expression::Cast(cast) => Self::expression_has_nondeterministic_functions(&cast.expr),
            Expression::Distinct(distinct) => {
                Self::expression_has_nondeterministic_functions(&distinct.expr)
            }
            Expression::ScalarSubquery(subquery) => {
                subquery
                    .subquery
                    .columns
                    .iter()
                    .any(Self::expression_has_nondeterministic_functions)
                    || subquery
                        .subquery
                        .where_clause
                        .as_ref()
                        .is_some_and(|w| Self::expression_has_nondeterministic_functions(w))
            }
            Expression::AllAny(all_any) => {
                Self::expression_has_nondeterministic_functions(&all_any.left)
            }
            Expression::InHashSet(in_hash) => {
                Self::expression_has_nondeterministic_functions(&in_hash.column)
            }
            _ => false,
        }
    }

    /// Check if an expression contains correlated subqueries (references outer columns)
    /// This is an expensive check as it must examine each subquery's WHERE clause
    fn expression_has_correlated_subqueries(expr: &Expression) -> bool {
        let mut correlated = false;
        radixdb_sql::ast::walk_expression_tree(expr, &mut |expression| {
            if correlated {
                return;
            }
            correlated = match expression {
                Expression::Exists(exists) => Self::is_subquery_correlated(&exists.subquery),
                Expression::ScalarSubquery(subquery) => {
                    Self::is_subquery_correlated(&subquery.subquery)
                }
                Expression::AllAny(all_any) => Self::is_subquery_correlated(&all_any.subquery),
                _ => false,
            };
        });
        correlated
    }

    /// Check if a subquery is correlated (references outer table columns)
    fn is_subquery_correlated(subquery: &SelectStatement) -> bool {
        // Collect table names/aliases from the subquery's FROM clause
        let subquery_tables = Self::collect_subquery_tables(&subquery.table_expr);
        let mut correlated = false;
        radixdb_sql::ast::walk_select_tree(subquery, &mut |expression| {
            if correlated {
                return;
            }
            match expression {
                Expression::QualifiedIdentifier(qid) => {
                    correlated = !subquery_tables
                        .iter()
                        .any(|table| table.eq_ignore_ascii_case(&qid.qualifier.value_lower));
                }
                Expression::Identifier(_) => correlated = true,
                _ => {}
            }
        });
        correlated
    }

    /// Collect table names and aliases from a subquery's FROM clause
    fn collect_subquery_tables(table_expr: &Option<Box<Expression>>) -> Vec<String> {
        let mut tables = Vec::new();
        if let Some(ref expr) = table_expr {
            Self::collect_tables_recursive(expr, &mut tables);
        }
        tables
    }

    /// Recursively collect table names and aliases
    fn collect_tables_recursive(expr: &Expression, tables: &mut Vec<String>) {
        match expr {
            Expression::Identifier(ident) => {
                tables.push(ident.value_lower.to_string());
            }
            Expression::Aliased(aliased) => {
                // Add alias (use value_lower for case-insensitive matching)
                tables.push(aliased.alias.value_lower.to_string());
                // Also collect from inner expression
                Self::collect_tables_recursive(&aliased.expression, tables);
            }
            Expression::JoinSource(join) => {
                Self::collect_tables_recursive(&join.left, tables);
                Self::collect_tables_recursive(&join.right, tables);
            }
            Expression::SubquerySource(subquery) => {
                // Subquery has an alias, collect it
                if let Some(ref alias) = subquery.alias {
                    tables.push(alias.value_lower.to_string());
                }
            }
            Expression::TableSource(table) => {
                // Add table name and alias if present
                tables.push(table.name.value_lower.to_string());
                if let Some(ref alias) = table.alias {
                    tables.push(alias.value_lower.to_string());
                }
            }
            Expression::FunctionTableSource(fs) => {
                if let Some(ref alias) = fs.alias {
                    tables.push(alias.value_lower.to_string());
                } else {
                    tables.push(fs.function.value_lower.to_string());
                }
            }
            _ => {}
        }
    }

    /// Check if an expression references columns from tables NOT in the given list
    #[allow(dead_code)]
    fn has_outer_column_reference(expr: &Expression, inner_tables: &[String]) -> bool {
        match expr {
            Expression::QualifiedIdentifier(qi) => {
                // Has table qualifier - check if it's NOT in inner tables
                let table_ref = &qi.qualifier.value_lower;
                // If table reference is NOT in inner tables, it's an outer reference
                !inner_tables.iter().any(|t| t == table_ref)
            }
            Expression::Infix(infix) => {
                Self::has_outer_column_reference(&infix.left, inner_tables)
                    || Self::has_outer_column_reference(&infix.right, inner_tables)
            }
            Expression::Prefix(prefix) => {
                Self::has_outer_column_reference(&prefix.right, inner_tables)
            }
            Expression::FunctionCall(func) => func
                .arguments
                .iter()
                .any(|a| Self::has_outer_column_reference(a, inner_tables)),
            Expression::Case(case) => {
                case.when_clauses.iter().any(|w| {
                    Self::has_outer_column_reference(&w.condition, inner_tables)
                        || Self::has_outer_column_reference(&w.then_result, inner_tables)
                }) || case
                    .else_value
                    .as_ref()
                    .is_some_and(|e| Self::has_outer_column_reference(e, inner_tables))
            }
            Expression::In(in_expr) => {
                Self::has_outer_column_reference(&in_expr.left, inner_tables)
                    || Self::has_outer_column_reference(&in_expr.right, inner_tables)
            }
            Expression::Between(between) => {
                Self::has_outer_column_reference(&between.expr, inner_tables)
                    || Self::has_outer_column_reference(&between.lower, inner_tables)
                    || Self::has_outer_column_reference(&between.upper, inner_tables)
            }
            Expression::Aliased(aliased) => {
                Self::has_outer_column_reference(&aliased.expression, inner_tables)
            }
            Expression::List(list) => list
                .elements
                .iter()
                .any(|e| Self::has_outer_column_reference(e, inner_tables)),
            Expression::ExpressionList(list) => list
                .expressions
                .iter()
                .any(|e| Self::has_outer_column_reference(e, inner_tables)),
            _ => false,
        }
    }
}

/// Check if a function name is an aggregate function
fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "GROUP_CONCAT"
            | "STRING_AGG"
            | "ARRAY_AGG"
            | "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "VARIANCE"
            | "VAR_POP"
            | "VAR_SAMP"
            | "PERCENTILE"
            | "PERCENTILE_CONT"
            | "PERCENTILE_DISC"
            | "MEDIAN"
            | "MODE"
            | "BOOL_AND"
            | "BOOL_OR"
            | "BIT_AND"
            | "BIT_OR"
            | "BIT_XOR"
            | "FIRST"
            | "LAST"
            | "ANY_VALUE"
    )
}

/// Compute a cache key for a SELECT statement
/// Only hashes structural elements that affect classification (not literal values)
/// Uses FxHasher which is 2-5x faster than SipHash for small keys.
fn compute_classification_key(stmt: &SelectStatement) -> u64 {
    let mut hasher = FxHasher::default();

    // Hash structural properties
    stmt.distinct.hash(&mut hasher);
    stmt.distinct_on.len().hash(&mut hasher);
    stmt.columns.len().hash(&mut hasher);
    stmt.group_by.columns.len().hash(&mut hasher);
    stmt.order_by.len().hash(&mut hasher);
    stmt.limit.is_some().hash(&mut hasher);
    stmt.offset.is_some().hash(&mut hasher);
    stmt.having.is_some().hash(&mut hasher);
    stmt.with.is_some().hash(&mut hasher);
    stmt.set_operations.len().hash(&mut hasher);

    // Hash DISTINCT ON expression structures
    for expr in &stmt.distinct_on {
        hash_expression_structure(expr, &mut hasher);
    }

    // Hash column expression types (not values)
    for col in &stmt.columns {
        hash_expression_structure(col, &mut hasher);
    }

    // Hash WHERE clause structure if present
    if let Some(ref where_clause) = stmt.where_clause {
        hash_expression_structure(where_clause, &mut hasher);
    }

    // The classification cache is not a physical-plan cache, but its key must
    // still keep distinct FROM/JOIN shapes apart. In particular an eligible
    // `COUNT(*) ... ON child.fk = parent.pk` and a residual-ON fallback must
    // never reuse one another's cached structural classification.
    if let Some(table_expr) = &stmt.table_expr {
        hash_expression_structure(table_expr, &mut hasher);
    }

    // Hash ORDER BY expressions (critical for correlated subquery detection)
    // Without this, queries with same ORDER BY count but different expressions
    // would incorrectly share classification (e.g., "ORDER BY 1" vs "ORDER BY (SELECT ...)")
    for ob in &stmt.order_by {
        hash_expression_structure(&ob.expression, &mut hasher);
        ob.ascending.hash(&mut hasher);
        ob.nulls_first.hash(&mut hasher);
    }

    // Hash GROUP BY expressions
    for gb in &stmt.group_by.columns {
        hash_expression_structure(gb, &mut hasher);
    }

    // Hash HAVING clause if present
    if let Some(ref having) = stmt.having {
        hash_expression_structure(having, &mut hasher);
    }

    hasher.finish()
}

/// Hash the structural elements of an expression (discriminants, not values)
fn hash_expression_structure(expr: &Expression, hasher: &mut FxHasher) {
    std::mem::discriminant(expr).hash(hasher);

    match expr {
        Expression::FunctionCall(func) => {
            // Hash function name case-insensitively without allocating
            for c in func.function.bytes() {
                c.to_ascii_uppercase().hash(hasher);
            }
            func.arguments.len().hash(hasher);
            for arg in &func.arguments {
                hash_expression_structure(arg, hasher);
            }
        }
        Expression::Window(wf) => {
            // Hash window function name case-insensitively without allocating
            for c in wf.function.function.bytes() {
                c.to_ascii_uppercase().hash(hasher);
            }
        }
        Expression::Aliased(aliased) => {
            hash_expression_structure(&aliased.expression, hasher);
        }
        Expression::Infix(infix) => {
            infix.operator.hash(hasher);
            hash_expression_structure(&infix.left, hasher);
            hash_expression_structure(&infix.right, hasher);
        }
        Expression::Prefix(prefix) => {
            hash_expression_structure(&prefix.right, hasher);
        }
        Expression::Cast(cast) => {
            cast.type_name.hash(hasher);
            hash_expression_structure(&cast.expr, hasher);
        }
        Expression::Case(case) => {
            case.when_clauses.len().hash(hasher);
            for wc in &case.when_clauses {
                hash_expression_structure(&wc.condition, hasher);
                hash_expression_structure(&wc.then_result, hasher);
            }
            if let Some(ref else_val) = case.else_value {
                hash_expression_structure(else_val, hasher);
            }
        }
        Expression::In(in_expr) => {
            in_expr.not.hash(hasher);
            hash_expression_structure(&in_expr.left, hasher);
            hash_expression_structure(&in_expr.right, hasher);
        }
        Expression::Between(between) => {
            between.not.hash(hasher);
            hash_expression_structure(&between.expr, hasher);
            hash_expression_structure(&between.lower, hasher);
            hash_expression_structure(&between.upper, hasher);
        }
        Expression::List(list) => {
            list.elements.len().hash(hasher);
        }
        Expression::ScalarSubquery(subquery) => {
            compute_classification_key(&subquery.subquery).hash(hasher);
        }
        Expression::Exists(exists) => {
            compute_classification_key(&exists.subquery).hash(hasher);
        }
        Expression::AllAny(all_any) => {
            hash_expression_structure(&all_any.left, hasher);
            compute_classification_key(&all_any.subquery).hash(hasher);
        }
        Expression::QualifiedIdentifier(qi) => {
            // CRITICAL: Hash the qualifier (table name/alias) to distinguish correlated references
            // e.g., "c.id" vs "o.id" - the qualifier determines if it's an outer reference
            for c in qi.qualifier.value_lower.bytes() {
                c.hash(hasher);
            }
        }
        Expression::Parameter(param) => {
            param.index.hash(hasher);
        }
        Expression::TableSource(table) => {
            // Names do not affect classification, while alias/temporal shape
            // does affect join routing and correlated-reference analysis.
            table.alias.is_some().hash(hasher);
            table.as_of.is_some().hash(hasher);
            if let Some(as_of) = &table.as_of {
                for byte in as_of.as_of_type.bytes() {
                    byte.to_ascii_uppercase().hash(hasher);
                }
                hash_expression_structure(&as_of.value, hasher);
            }
        }
        Expression::JoinSource(join) => {
            for byte in join.join_type.bytes() {
                byte.to_ascii_uppercase().hash(hasher);
            }
            hash_expression_structure(&join.left, hasher);
            hash_expression_structure(&join.right, hasher);
            join.using_columns.len().hash(hasher);
            for column in &join.using_columns {
                for byte in column.value_lower.bytes() {
                    byte.hash(hasher);
                }
            }
            if let Some(condition) = &join.condition {
                hash_expression_structure(condition, hasher);
            }
        }
        Expression::SubquerySource(subquery) => {
            // Include the nested SELECT's complete structural key; a derived
            // table must not collide with a same-shaped base table.
            compute_classification_key(&subquery.subquery).hash(hasher);
            subquery.alias.is_some().hash(hasher);
        }
        _ => {
            // For literals and unqualified identifiers, just use discriminant
        }
    }
}

/// Get or compute the classification for a SELECT statement
pub fn get_classification(stmt: &SelectStatement) -> Arc<QueryClassification> {
    let cache_key = compute_classification_key(stmt);
    {
        let mut guard = CLASSIFICATION_CACHE.lock();
        let cache = guard.get_or_insert_with(|| {
            LruCache::new(NonZeroUsize::new(CLASSIFICATION_CACHE_SIZE).unwrap())
        });
        if let Some(bucket) = cache.get(&cache_key) {
            if let Some((_, classification)) = bucket.iter().find(|(cached, _)| cached == stmt) {
                return classification.clone();
            }
        }
    }

    // Classification is pure. Traverse the AST without holding the
    // process-wide LRU lock, then reconcile a possible concurrent winner.
    let classification = Arc::new(QueryClassification::classify(stmt));
    let mut guard = CLASSIFICATION_CACHE.lock();
    let cache = guard.get_or_insert_with(|| {
        LruCache::new(NonZeroUsize::new(CLASSIFICATION_CACHE_SIZE).unwrap())
    });
    if let Some(bucket) = cache.get(&cache_key) {
        if let Some((_, winner)) = bucket.iter().find(|(cached, _)| cached == stmt) {
            return winner.clone();
        }
    }
    if let Some(bucket) = cache.get_mut(&cache_key) {
        bucket.push((stmt.clone(), classification.clone()));
    } else {
        cache.put(cache_key, vec![(stmt.clone(), classification.clone())]);
    }
    classification
}

/// Clear the classification cache (for testing)
#[cfg(test)]
pub fn clear_cache() {
    let mut guard = CLASSIFICATION_CACHE.lock();
    if let Some(cache) = guard.as_mut() {
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_sql::ast::{GroupByClause, StarExpression};
    use radixdb_sql::token::{Position, Token, TokenType};

    fn parsed_select(sql: &str) -> SelectStatement {
        let statements = radixdb_sql::parse_sql(sql).unwrap();
        let radixdb_sql::Statement::Select(select) = &statements[0] else {
            panic!("expected SELECT")
        };
        select.clone()
    }

    #[test]
    fn r5_l03_semantic_and_classification_identity_preserve_complete_ast_classification() {
        clear_cache();
        let uncorrelated = parsed_select("SELECT (SELECT inner_t.id FROM inner_t) FROM outer_t");
        let correlated = parsed_select("SELECT (SELECT outer_t.id FROM inner_t) FROM outer_t");
        let first = get_classification(&uncorrelated);
        let second = get_classification(&correlated);
        assert!(!first.select_has_correlated_subqueries);
        assert!(second.select_has_correlated_subqueries);

        let aggregate_in_order = parsed_select("SELECT x FROM t ORDER BY COUNT(*)");
        assert!(get_classification(&aggregate_in_order).has_aggregation);

        let window_in_order = parsed_select("SELECT x FROM t ORDER BY ROW_NUMBER() OVER ()");
        assert!(get_classification(&window_in_order).has_window_functions);
    }

    fn dummy_token() -> Token {
        Token::new(TokenType::Keyword, "SELECT", Position::new(0, 1, 1))
    }

    fn create_select_star() -> SelectStatement {
        SelectStatement {
            token: dummy_token(),
            with: None,
            distinct: false,
            distinct_on: vec![],
            columns: vec![Expression::Star(StarExpression {
                token: dummy_token(),
            })],
            table_expr: None,
            where_clause: None,
            group_by: GroupByClause::default(),
            having: None,
            window_defs: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
            set_operations: vec![],
        }
    }

    #[test]
    fn test_select_star_classification() {
        let stmt = create_select_star();
        let classification = QueryClassification::classify(&stmt);

        // Basic flags
        assert!(classification.is_select_star);
        assert!(!classification.has_aggregation);
        assert!(!classification.has_window_functions);
        assert!(!classification.has_group_by);
        assert!(!classification.has_order_by);
        assert!(!classification.has_limit);
        assert!(!classification.has_distinct);

        assert!(!classification.has_joins);
        assert_eq!(classification.join_count, 0);
        assert!(!classification.has_outer_joins);
        assert!(!classification.has_derived_tables);
        assert!(!classification.has_where);
        assert!(!classification.where_has_subqueries);
    }

    #[test]
    fn test_classification_cache_lifecycle_preserves_result() {
        clear_cache();

        let stmt = create_select_star();

        // The cache is process-wide and may be cleared concurrently when another
        // database/test is dropped. Pointer identity is therefore not part of the
        // contract; recomputation after eviction must remain semantically exact.
        let class1 = get_classification(&stmt);
        assert!(class1.is_select_star);

        clear_cache();
        let class2 = get_classification(&stmt);
        assert_eq!(*class1, *class2);
    }

    #[test]
    fn classification_cache_distinguishes_eligible_and_residual_join_shapes() {
        clear_cache();
        let eligible = radixdb_sql::parse_sql(
            "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id",
        )
        .unwrap();
        let residual = radixdb_sql::parse_sql(
            "SELECT COUNT(*) FROM child c INNER JOIN parent p ON c.parent_id = p.id AND c.tag = p.tag",
        )
        .unwrap();
        let eligible = match &eligible[0] {
            radixdb_sql::Statement::Select(statement) => statement,
            _ => panic!("expected SELECT"),
        };
        let residual = match &residual[0] {
            radixdb_sql::Statement::Select(statement) => statement,
            _ => panic!("expected SELECT"),
        };

        let eligible_classification = get_classification(eligible);
        let residual_classification = get_classification(residual);
        assert!(eligible_classification.has_joins);
        assert!(residual_classification.has_joins);
        assert!(!Arc::ptr_eq(
            &eligible_classification,
            &residual_classification
        ));
    }

    #[test]
    fn classification_preserves_join_depth_outer_barrier_and_derived_boundary() {
        let statement = parsed_select(
            "SELECT * FROM a \
             JOIN b ON b.a_id = a.id \
             LEFT JOIN (SELECT id FROM c) c1 ON c1.id = b.c_id \
             JOIN d ON d.id = a.d_id",
        );
        let classification = QueryClassification::classify(&statement);
        assert!(classification.has_joins);
        assert_eq!(classification.join_count, 3);
        assert!(classification.has_outer_joins);
        assert!(classification.has_derived_tables);
        assert_eq!(classification.reorderable_join_count, 2);
        let graph = classification.logical_join_graph.as_ref().unwrap();
        assert_eq!(graph.edges.len(), classification.join_count);
    }
}
