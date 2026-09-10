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

//! Common Table Expression (CTE) Execution
//!
//! This module implements WITH clause execution for SQL queries.
//!
//! Supports:
//! - Basic CTEs: `WITH x AS (SELECT ...) SELECT * FROM x`
//! - Multiple CTEs: `WITH a AS (...), b AS (...) SELECT ...`
//! - CTE with column aliases: `WITH x(col1, col2) AS (SELECT ...) SELECT ...`
//! - CTEs referencing other CTEs: `WITH a AS (...), b AS (SELECT * FROM a) ...`
//!
//! Optimizations:
//! - CTE Inlining: Single-use, non-recursive CTEs are converted to subqueries
//!   to preserve index access and benefit from LIMIT pushdown
//!
//! Note: Recursive CTEs are parsed but not yet executed.

use ahash::AHashSet;
use std::sync::{Arc, OnceLock};

use radixdb_core::{CompactArc, CompactVec, StringMap};
use radixdb_core::{DataType, Error, Result, Row, RowVec, Value};
use radixdb_functions::FunctionRegistry;
use radixdb_sql::ast::*;
use radixdb_sql::token::{Position, Token, TokenType};
use radixdb_storage::traits::QueryResult;

use super::aggregation::{AggregationExecutorExt, AggregationHost};
use super::context::ExecutionContext;
use super::expression::{compile_expression_with_context, ExpressionEval};
use super::pipeline::paging::evaluate_page_expression;
use super::pipeline::set::merge_set_type;
use super::query_classification::{get_classification, QueryClassification};
use super::subquery::{SubqueryExecutorExt, SubqueryHost};
use super::utils::build_column_index_map;
use super::utils::RetainedRowsBudget;
use super::window::{WindowExecutorExt, WindowHost};

/// Type alias for CTE data: (columns, rows) with Arc for zero-copy sharing
/// Uses `Vec<(i64, Row)>` for rows - same structure as `RowVec` but Arc-shareable
pub type CteData = (
    CompactArc<Vec<String>>,
    CompactArc<Vec<(i64, Row)>>,
    Arc<OnceLock<CompactArc<Vec<Row>>>>,
);

/// Type alias for CTE data map
/// Uses `CompactArc<Vec<String>>` for columns and `CompactArc<Vec<(i64, Row)>>` for rows
/// to enable zero-copy sharing of CTE results with joins
pub type CteDataMap = StringMap<CteData>;

/// Registry for CTE results during query execution
///
/// Uses Arc with COW (copy-on-write) semantics to avoid cloning:
/// - During building: `Arc::make_mut` gives mutable access without cloning (single owner)
/// - During sharing: `data()` returns cheap Arc clone (O(1), no data copy)
#[derive(Clone)]
pub struct CteRegistry {
    /// Materialized CTE results (name -> (columns, rows))
    /// Arc provides cheap sharing; make_mut provides COW for modifications
    data: Arc<CteDataMap>,
}

impl Default for CteRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CteRegistry {
    /// Create a new CTE registry
    pub fn new() -> Self {
        Self {
            data: Arc::new(StringMap::new()),
        }
    }

    /// Store a materialized CTE result
    ///
    /// Uses Arc::make_mut for COW semantics:
    /// - If we're the only owner, mutates in place (no clone)
    /// - If shared, clones first then mutates (preserves other references)
    ///
    /// Both columns and rows are wrapped in Arc to enable zero-copy sharing.
    /// Accepts RowVec and converts to CompactArc<Vec<(i64, Row)>> for sharing.
    pub fn store(&mut self, name: &str, columns: Vec<String>, rows: RowVec) {
        let name_lower = name.to_lowercase();
        // Convert RowVec to Vec<(i64, Row)> for Arc sharing
        let rows_vec: Vec<(i64, Row)> = rows.into_iter().collect();
        Arc::make_mut(&mut self.data).insert(
            name_lower,
            (
                CompactArc::new(columns),
                CompactArc::new(rows_vec),
                Arc::new(OnceLock::new()),
            ),
        );
    }

    /// Store a materialized CTE result with pre-wrapped Arcs
    ///
    /// Use this when you already have Arc-wrapped data to avoid cloning.
    /// This enables zero-copy sharing of CTE results between queries.
    pub fn store_arc(
        &mut self,
        name: &str,
        columns: CompactArc<Vec<String>>,
        rows: CompactArc<Vec<(i64, Row)>>,
        materialized_rows: Arc<OnceLock<CompactArc<Vec<Row>>>>,
    ) {
        let name_lower = name.to_lowercase();
        Arc::make_mut(&mut self.data).insert(name_lower, (columns, rows, materialized_rows));
    }

    /// Look up a materialized CTE by case-insensitive name.
    pub fn get(&self, name: &str) -> Option<&CteData> {
        self.data.get(&name.to_lowercase())
    }

    /// Get a shared Arc reference to the internal data map for context transfer
    ///
    /// This is always O(1) - just an Arc reference count increment.
    /// No data cloning ever happens here.
    pub fn data(&self) -> Arc<CteDataMap> {
        self.data.clone()
    }

    /// Iterate over all stored CTEs (for copying to temp registries)
    pub fn iter(&self) -> impl Iterator<Item = (&String, &CteData)> {
        self.data.iter()
    }
}

/// Narrow composition contract for CTE execution. CTE materialization and
/// inlining stay executor-owned; the host only provides the recursive SELECT
/// entrypoint and immutable function registry.
pub trait CteHost: AggregationHost + SubqueryHost + WindowHost {
    fn cte_function_registry(&self) -> &FunctionRegistry;
    fn cte_execute_select(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
}

/// Single owner for CTE registries, materialization, recursive fixpoints and
/// safe inlining decisions.
pub struct CteExecutor<'a, H: CteHost + ?Sized> {
    host: &'a H,
}

impl<'a, H: CteHost + ?Sized> CteExecutor<'a, H> {
    fn new(host: &'a H) -> Self {
        Self { host }
    }
}

/// Internal call surface between CTE expansion and the SELECT owner.
pub trait CteExecutorExt: CteHost {
    fn execute_select_with_ctes(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        CteExecutor::new(self).execute_select_with_ctes(statement, context)
    }

    fn execute_query_on_cte_result(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
        columns: Vec<String>,
        rows: RowVec,
    ) -> Result<(Vec<String>, RowVec)> {
        CteExecutor::new(self).execute_query_on_cte_result(statement, context, columns, rows)
    }

    fn execute_query_on_cte_result_inner(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
        columns: Vec<String>,
        rows: RowVec,
        skip_order_limit: bool,
    ) -> Result<(Vec<String>, RowVec, bool)> {
        CteExecutor::new(self).execute_query_on_cte_result_inner(
            statement,
            context,
            columns,
            rows,
            skip_order_limit,
        )
    }

    fn has_cte(&self, statement: &SelectStatement) -> bool {
        CteExecutor::new(self).has_cte(statement)
    }

    fn try_inline_ctes(
        &self,
        statement: &SelectStatement,
        with_clause: &WithClause,
    ) -> Option<SelectStatement> {
        CteExecutor::new(self).try_inline_ctes(statement, with_clause)
    }
}

impl<T: CteHost + ?Sized> CteExecutorExt for T {}

fn materialize_result(mut result: Box<dyn QueryResult>) -> Result<RowVec> {
    let mut rows = result
        .estimated_count()
        .map_or_else(RowVec::new, RowVec::with_capacity);
    let mut row_id = 0i64;
    while result.next() {
        rows.push((row_id, result.take_row()));
        row_id += 1;
    }
    if let Some(error) = result.last_error() {
        return Err(error);
    }
    Ok(rows)
}

impl<H: CteHost + ?Sized> CteExecutor<'_, H> {
    /// Execute a SELECT statement with WITH clause (CTEs)
    pub(crate) fn execute_select_with_ctes(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        // Get the WITH clause
        let with_clause = match &stmt.with {
            Some(with) => with,
            None => return self.host.cte_execute_select(stmt, ctx),
        };

        // CTE INLINING OPTIMIZATION:
        // For single-use, non-recursive CTEs, convert to subqueries to:
        // 1. Preserve index access (CTEs lose indexes when materialized)
        // 2. Enable LIMIT pushdown through subqueries
        // This is similar to PostgreSQL 12+'s CTE inlining behavior
        if let Some(inlined_stmt) = self.try_inline_ctes(stmt, with_clause) {
            // Execute the rewritten query (without WITH clause or with fewer CTEs)
            return self.host.cte_execute_select(&inlined_stmt, ctx);
        }

        // Create CTE registry
        let mut cte_registry = CteRegistry::new();

        // Execute each CTE in order
        for cte in &with_clause.ctes {
            // Execute the CTE query (handles recursive CTEs)
            let (columns, rows) = if cte.is_recursive {
                // Pass column aliases to recursive CTE execution so they're available during iteration
                let aliases = if cte.column_names.is_empty() {
                    None
                } else {
                    Some(cte.column_names.as_slice())
                };
                self.execute_recursive_cte_with_columns(
                    &cte.name.value,
                    &cte.query,
                    ctx,
                    &mut cte_registry,
                    aliases,
                )?
            } else {
                self.execute_cte_query(&cte.query, ctx, &mut cte_registry)?
            };

            // Apply column aliases if specified
            let columns = if !cte.column_names.is_empty() {
                cte.column_names
                    .iter()
                    .enumerate()
                    .map(|(i, alias)| {
                        if i < columns.len() {
                            alias.value.to_string()
                        } else {
                            columns
                                .get(i)
                                .cloned()
                                .unwrap_or_else(|| format!("col{}", i))
                        }
                    })
                    .collect()
            } else {
                columns
            };

            // Store the materialized result
            cte_registry.store(&cte.name.value, columns, rows);
        }

        // Execute the main query with CTE registry
        self.execute_main_query_with_ctes(stmt, ctx, &mut cte_registry)
    }

    /// Execute a single CTE query
    fn execute_cte_query(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        cte_registry: &mut CteRegistry,
    ) -> Result<(Vec<String>, RowVec)> {
        let ctx_with_ctes = ctx.with_cte_data(cte_registry.data());
        let mut statement = stmt.clone();
        statement.with = None;
        let result = self.host.cte_execute_select(&statement, &ctx_with_ctes)?;
        let columns = result.columns().to_vec();
        let rows = materialize_result(result)?;

        Ok((columns, rows))
    }

    /// Execute a recursive CTE
    fn execute_recursive_cte_with_columns(
        &self,
        cte_name: &str,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        cte_registry: &mut CteRegistry,
        column_aliases: Option<&[Identifier]>,
    ) -> Result<(Vec<String>, RowVec)> {
        use radixdb_sql::ast::SetOperationType;

        // Maximum iterations to prevent infinite loops
        const MAX_ITERATIONS: usize = 10000;

        // The recursive CTE query should have UNION ALL structure
        if stmt.set_operations.is_empty() {
            return Err(Error::InvalidArgument(
                "Recursive CTE must have UNION ALL between anchor and recursive members"
                    .to_string(),
            ));
        }

        // Check that all set operations are UNION ALL
        for set_op in &stmt.set_operations {
            if !matches!(set_op.operation, SetOperationType::UnionAll) {
                return Err(Error::InvalidArgument(
                    "Recursive CTE only supports UNION ALL (not UNION)".to_string(),
                ));
            }
        }

        // Execute the anchor member (the first SELECT before UNION ALL)
        let anchor_stmt = SelectStatement {
            token: stmt.token.clone(),
            distinct: stmt.distinct,
            distinct_on: stmt.distinct_on.clone(),
            columns: stmt.columns.clone(),
            with: None,
            table_expr: stmt.table_expr.clone(),
            where_clause: stmt.where_clause.clone(),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            window_defs: stmt.window_defs.clone(),
            order_by: vec![], // No ORDER BY for anchor
            limit: None,
            offset: None,
            set_operations: vec![],
        };

        let result = self.host.cte_execute_select(&anchor_stmt, ctx)?;
        let anchor_columns = result.columns().to_vec();

        if let Some(aliases) = column_aliases {
            if aliases.len() != anchor_columns.len() {
                return Err(Error::InvalidArgument(format!(
                    "recursive CTE {cte_name} declares {} columns but anchor returns {}",
                    aliases.len(),
                    anchor_columns.len()
                )));
            }
        }

        // Apply column aliases if provided (for recursive CTE column naming)
        let columns: Vec<String> = if let Some(aliases) = column_aliases {
            aliases
                .iter()
                .enumerate()
                .map(|(i, alias)| {
                    if i < anchor_columns.len() {
                        alias.value.to_string()
                    } else {
                        anchor_columns
                            .get(i)
                            .cloned()
                            .unwrap_or_else(|| format!("col{}", i))
                    }
                })
                .collect()
        } else {
            anchor_columns
        };

        let mut all_rows = materialize_result(result)?;
        if all_rows.iter().any(|(_, row)| row.len() != columns.len()) {
            return Err(Error::InvalidArgument(format!(
                "recursive CTE {cte_name} anchor row width does not match its {} columns",
                columns.len()
            )));
        }

        // If no anchor rows, return empty result
        if all_rows.is_empty() {
            return Ok((columns, all_rows));
        }

        let mut target_types = vec![DataType::Null; columns.len()];
        for (_, row) in all_rows.iter() {
            for (column, value) in row.iter().enumerate() {
                target_types[column] = merge_set_type(target_types[column], value.data_type())
                    .map_err(|error| {
                        Error::InvalidArgument(format!(
                            "recursive CTE {cte_name} anchor types are incompatible: {error}"
                        ))
                    })?;
            }
        }
        for (_, row) in all_rows.iter_mut() {
            for (value, target_type) in row.iter_mut().zip(&target_types) {
                if value.data_type() != *target_type {
                    *value = value.try_coerce_to_type(*target_type).map_err(|error| {
                        Error::InvalidArgument(format!(
                            "recursive CTE {cte_name} anchor type is incompatible: {error}"
                        ))
                    })?;
                }
            }
        }

        // Current working set (rows from previous iteration)
        let mut retained = RetainedRowsBudget::new("recursive CTE");
        for (_, row) in all_rows.iter() {
            retained.admit(row)?; // accumulated result
            retained.admit(row)?; // first working set clone
        }
        let mut working_rows = all_rows.clone();

        // Iterate until no new rows or max iterations
        let mut converged = false;
        for _iteration in 0..MAX_ITERATIONS {
            ctx.check_cancelled()?;
            if working_rows.is_empty() {
                converged = true;
                break;
            }

            // Create a temporary CTE registry with current working set
            let mut temp_registry = CteRegistry::new();
            // Copy existing CTEs - share Arc to avoid cloning row data
            for (name, (cols, rows, materialized_rows)) in cte_registry.iter() {
                temp_registry.store_arc(
                    name,
                    cols.clone(),
                    CompactArc::clone(rows),
                    Arc::clone(materialized_rows),
                );
            }
            // Move the working set behind a shared owner. The recursive query
            // borrows the Arc and no third full row clone is created.
            let working_rows_arc = CompactArc::new(working_rows.into_iter().collect());
            temp_registry.store_arc(
                cte_name,
                CompactArc::new(columns.clone()),
                CompactArc::clone(&working_rows_arc),
                Arc::new(OnceLock::new()),
            );

            // Execute each recursive member
            let mut new_rows = RowVec::new();
            for set_op in &stmt.set_operations {
                // The recursive member is in set_op.right
                let mut recursive_result =
                    self.execute_cte_query(&set_op.right, ctx, &mut temp_registry)?;

                if recursive_result.0.len() != columns.len() {
                    return Err(Error::InvalidArgument(format!(
                        "recursive CTE {cte_name} member returns {} columns but anchor returns {}",
                        recursive_result.0.len(),
                        columns.len()
                    )));
                }

                let mut merged_types = target_types.clone();
                for (_, row) in recursive_result.1.iter() {
                    if row.len() != columns.len() {
                        return Err(Error::InvalidArgument(format!(
                            "recursive CTE {cte_name} member row width {} does not match {}",
                            row.len(),
                            columns.len()
                        )));
                    }
                    for (column, value) in row.iter().enumerate() {
                        merged_types[column] =
                            merge_set_type(merged_types[column], value.data_type()).map_err(
                                |error| {
                                    Error::InvalidArgument(format!(
                                "recursive CTE {cte_name} member type is incompatible: {error}"
                            ))
                                },
                            )?;
                    }
                }

                if merged_types != target_types {
                    for (_, row) in all_rows.iter_mut().chain(new_rows.iter_mut()) {
                        for (value, target_type) in row.iter_mut().zip(&merged_types) {
                            if value.data_type() != *target_type {
                                *value =
                                    value.try_coerce_to_type(*target_type).map_err(|error| {
                                        Error::InvalidArgument(format!(
                                        "recursive CTE {cte_name} type migration failed: {error}"
                                    ))
                                    })?;
                            }
                        }
                    }
                    target_types = merged_types;
                }

                // Extend with rows from recursive result, renumbering row IDs
                let base_id = new_rows.len() as i64;
                for (i, (_, mut row)) in recursive_result.1.drain(..).enumerate() {
                    if i & 0xff == 0 {
                        ctx.check_cancelled()?;
                    }
                    if row.len() != columns.len() {
                        return Err(Error::InvalidArgument(format!(
                            "recursive CTE {cte_name} member row width {} does not match {}",
                            row.len(),
                            columns.len()
                        )));
                    }
                    for (value, target_type) in row.iter_mut().zip(&target_types) {
                        if value.data_type() != *target_type {
                            *value = value.try_coerce_to_type(*target_type).map_err(|error| {
                                Error::InvalidArgument(format!(
                                    "recursive CTE {cte_name} member type is incompatible with anchor: {error}"
                                ))
                            })?;
                        }
                    }
                    retained.admit(&row)?;
                    new_rows.push((base_id + i as i64, row));
                }
            }

            // The previous working set is no longer retained separately from
            // `all_rows` after the recursive members finish.
            drop(temp_registry);
            for (_, row) in working_rows_arc.iter() {
                retained.release(row);
            }

            if new_rows.is_empty() {
                converged = true;
                break;
            }

            // Add new rows to total result, renumbering row IDs
            let base_id = all_rows.len() as i64;
            for (i, (_, row)) in new_rows.iter().enumerate() {
                retained.admit(row)?;
                all_rows.push((base_id + i as i64, row.clone()));
            }

            // New rows become the working set for next iteration
            working_rows = new_rows;
        }

        if !converged {
            return Err(Error::InvalidArgument(format!(
                "recursive CTE {cte_name} exceeded {MAX_ITERATIONS} iterations"
            )));
        }

        let classification = get_classification(stmt);
        all_rows =
            self.apply_order_by_limit_offset(stmt, ctx, &classification, all_rows, &columns)?;

        Ok((columns, all_rows))
    }

    /// Execute the main query with CTEs available
    fn execute_main_query_with_ctes(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        cte_registry: &mut CteRegistry,
    ) -> Result<Box<dyn QueryResult>> {
        let ctx_with_ctes = ctx.with_cte_data(cte_registry.data());
        let mut statement = stmt.clone();
        statement.with = None;
        // CTE rows are regular in-memory table sources in the execution context.
        // Running the ordinary SELECT pipeline keeps JOIN projection, set ops,
        // DISTINCT, complex ORDER BY and final paging under the same owners.
        self.host.cte_execute_select(&statement, &ctx_with_ctes)
    }

    /// Execute a query on CTE result data
    #[allow(dead_code)]
    pub(crate) fn execute_query_on_cte_result(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        cte_columns: Vec<String>,
        cte_rows: RowVec,
    ) -> Result<(Vec<String>, RowVec)> {
        let (cols, rows, _applied) =
            self.execute_query_on_cte_result_inner(stmt, ctx, cte_columns, cte_rows, false)?;
        Ok((cols, rows))
    }

    /// Inner implementation that optionally skips ORDER BY/LIMIT processing.
    /// Returns (columns, rows, order_limit_applied).
    /// When `skip_order_limit` is true, ORDER BY and LIMIT/OFFSET are NOT applied,
    /// allowing the caller to delegate to a more capable ORDER BY handler.
    pub(crate) fn execute_query_on_cte_result_inner(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        cte_columns: Vec<String>,
        cte_rows: RowVec,
        skip_order_limit: bool,
    ) -> Result<(Vec<String>, RowVec, bool)> {
        // OPTIMIZATION: Get cached query classification to avoid repeated AST traversals
        let classification = get_classification(stmt);

        // Apply WHERE clause filter
        let filtered_rows = if let Some(ref where_clause) = stmt.where_clause {
            // Process subqueries in WHERE clause (e.g., IN subqueries on CTEs)
            // Use cached classification to avoid AST traversal
            let processed_where = if classification.where_has_subqueries {
                self.host.process_where_subqueries(where_clause, ctx)?
            } else {
                (**where_clause).clone()
            };

            // Compile filter once and reuse for all rows
            let mut eval = ExpressionEval::compile_with_options(
                &processed_where,
                &cte_columns,
                None,
                ctx.outer_columns(),
                None,
                self.host.cte_function_registry(),
            )?
            .with_context(ctx);

            let mut result = RowVec::new();
            let mut row_id = 0i64;
            for (_, row) in cte_rows {
                if eval.eval_bool_checked(&row)? {
                    result.push((row_id, row));
                    row_id += 1;
                }
            }
            result
        } else {
            cte_rows
        };

        // Check for aggregation
        if classification.has_aggregation {
            let result = self.host.execute_select_with_aggregation(
                stmt,
                ctx,
                filtered_rows,
                &cte_columns,
            )?;
            let columns = result.columns().to_vec();
            let mut rows = materialize_result(result)?;

            if !skip_order_limit {
                rows =
                    self.apply_order_by_limit_offset(stmt, ctx, &classification, rows, &columns)?;
            }
            return Ok((columns, rows, !skip_order_limit));
        }

        // Check for window functions
        if classification.has_window_functions {
            let result = self.host.execute_select_with_window_functions(
                stmt,
                ctx,
                &filtered_rows,
                &cte_columns,
            )?;
            let columns = result.columns().to_vec();
            let mut rows = materialize_result(result)?;

            if !skip_order_limit {
                rows =
                    self.apply_order_by_limit_offset(stmt, ctx, &classification, rows, &columns)?;
            }
            return Ok((columns, rows, !skip_order_limit));
        }

        // Process scalar subqueries in SELECT columns before projection
        let processed_columns = self
            .host
            .try_process_select_subqueries(&stmt.columns, ctx)?;
        let columns_to_use = processed_columns.as_ref().unwrap_or(&stmt.columns);

        // Determine output columns
        let output_columns =
            self.resolve_cte_output_columns_from_exprs(columns_to_use, &cte_columns)?;

        let needs_projection = self.needs_projection_for_columns(columns_to_use);

        if skip_order_limit {
            // Caller will handle ORDER BY + LIMIT/OFFSET via expression-based sort.
            // If ORDER BY references source columns not in the projected output,
            // return unprojected rows so the caller can evaluate ORDER BY expressions.
            // The caller's truncation logic (expected_columns) will trim afterwards.
            let needs_source_for_order = classification.has_order_by
                && self.order_by_needs_source_columns(
                    &stmt.order_by,
                    &output_columns,
                    &cte_columns,
                );

            if needs_source_for_order {
                // Return source columns + projected columns so caller can sort on source columns
                // then trim to projected columns via expected_columns mechanism
                let mut combined_columns = output_columns.clone();
                for src_col in &cte_columns {
                    if !combined_columns
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(src_col))
                    {
                        combined_columns.push(src_col.clone());
                    }
                }

                // Build rows with projected columns first, then extra source columns
                let result_rows = if needs_projection {
                    let projected = self.project_cte_rows_from_columns(
                        columns_to_use,
                        &filtered_rows,
                        &cte_columns,
                        ctx,
                    )?;
                    // Append source columns that aren't in output
                    let extra_src_indices: Vec<usize> = cte_columns
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| {
                            !output_columns.iter().any(|oc| oc.eq_ignore_ascii_case(c))
                        })
                        .map(|(i, _)| i)
                        .collect();

                    projected
                        .into_iter()
                        .zip(filtered_rows.iter())
                        .map(|((id, proj_row), (_, src_row))| {
                            let mut vals = proj_row.into_values();
                            for &idx in &extra_src_indices {
                                vals.push(
                                    src_row.get(idx).cloned().unwrap_or(Value::null_unknown()),
                                );
                            }
                            (id, Row::from_values(vals))
                        })
                        .collect()
                } else {
                    filtered_rows
                };
                return Ok((combined_columns, result_rows, false));
            }

            let result_rows = if needs_projection {
                self.project_cte_rows_from_columns(
                    columns_to_use,
                    &filtered_rows,
                    &cte_columns,
                    ctx,
                )?
            } else {
                filtered_rows
            };
            return Ok((output_columns, result_rows, false));
        }

        // Check if ORDER BY references source columns not in the projected output.
        // If so, sort BEFORE projection using source columns, then project after.
        let needs_pre_sort = classification.has_order_by
            && self.order_by_needs_source_columns(&stmt.order_by, &output_columns, &cte_columns);

        // Sort before projection if ORDER BY references non-projected source columns
        let mut result_rows = if needs_pre_sort {
            if needs_projection {
                // Sort on source columns first, then project
                let sorted =
                    self.apply_order_by_to_rows(filtered_rows, &stmt.order_by, &cte_columns)?;
                self.project_cte_rows_from_columns(columns_to_use, &sorted, &cte_columns, ctx)?
            } else {
                self.apply_order_by_to_rows(filtered_rows, &stmt.order_by, &cte_columns)?
            }
        } else {
            // Normal path: project first, then sort on output columns
            let mut rows = if needs_projection {
                self.project_cte_rows_from_columns(
                    columns_to_use,
                    &filtered_rows,
                    &cte_columns,
                    ctx,
                )?
            } else {
                filtered_rows
            };

            if classification.has_order_by {
                rows = self.apply_order_by_to_rows(rows, &stmt.order_by, &output_columns)?;
            }
            rows
        };

        // Apply LIMIT and OFFSET (using classification for quick check)
        if classification.has_offset {
            if let Some(ref offset_expr) = stmt.offset {
                let offset = evaluate_page_expression(offset_expr, ctx, "OFFSET")?;
                // Use drain to avoid extra allocation from skip().collect()
                if offset > 0 && offset < result_rows.len() {
                    result_rows.drain(..offset);
                } else if offset >= result_rows.len() {
                    result_rows.clear();
                }
            }
        }

        if classification.has_limit {
            if let Some(ref limit_expr) = stmt.limit {
                let limit = evaluate_page_expression(limit_expr, ctx, "LIMIT")?;
                if limit < result_rows.len() {
                    result_rows.truncate(limit);
                }
            }
        }

        Ok((output_columns, result_rows, true))
    }

    /// Extract the base CTE/table name for registry lookup (ignores aliases)
    fn extract_cte_name_for_lookup(&self, expr: &Expression) -> Option<String> {
        match expr {
            Expression::CteReference(cte_ref) => Some(cte_ref.name.value.to_string()),
            Expression::TableSource(simple_table_source) => {
                Some(simple_table_source.name.value.to_string())
            }
            Expression::Identifier(id) => Some(id.value.to_string()),
            _ => None,
        }
    }

    /// Resolve output column names from expression list (for processed subqueries)
    fn resolve_cte_output_columns_from_exprs(
        &self,
        columns: &[Expression],
        cte_columns: &[String],
    ) -> Result<Vec<String>> {
        let mut output_columns = Vec::new();

        for (i, col_expr) in columns.iter().enumerate() {
            match col_expr {
                Expression::Star(_) | Expression::QualifiedStar(_) => {
                    output_columns.extend(cte_columns.iter().cloned());
                }
                Expression::Identifier(id) => {
                    output_columns.push(id.value.to_string());
                }
                Expression::Aliased(aliased) => {
                    output_columns.push(aliased.alias.value.to_string());
                }
                _ => {
                    output_columns.push(format!("expr{}", i + 1));
                }
            }
        }

        if output_columns.is_empty() {
            output_columns = cte_columns.to_vec();
        }

        Ok(output_columns)
    }

    /// Check if columns need projection
    fn needs_projection_for_columns(&self, columns: &[Expression]) -> bool {
        if columns.is_empty() {
            return false;
        }

        // Check if it's just SELECT *
        if columns.len() == 1 {
            if let Expression::Star(_) = &columns[0] {
                return false;
            }
        }

        true
    }

    /// Project rows based on provided column expressions (for processed subqueries)
    fn project_cte_rows_from_columns(
        &self,
        columns: &[Expression],
        rows: &RowVec,
        cte_columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<RowVec> {
        use super::expression::{ExecuteContext, ExprVM, SharedProgram};

        let col_index_map = build_column_index_map(cte_columns);

        // Pre-compile expressions that need evaluation
        // Store: Star -> None, Identifier -> column index, Complex -> compiled program
        enum CompiledColumn {
            Star,
            Identifier(usize),
            Compiled(SharedProgram),
        }

        let compiled_columns: Vec<CompiledColumn> = columns
            .iter()
            .map(|col_expr| match col_expr {
                Expression::Star(_) => Ok(CompiledColumn::Star),
                Expression::Identifier(id) => {
                    let idx = col_index_map
                        .get(id.value_lower.as_str())
                        .copied()
                        .ok_or_else(|| Error::ColumnNotFound(id.value.to_string()))?;
                    Ok(CompiledColumn::Identifier(idx))
                }
                Expression::Aliased(aliased) => {
                    let program = compile_expression_with_context(
                        &aliased.expression,
                        cte_columns,
                        ctx.outer_columns(),
                        self.host.cte_function_registry(),
                    )?;
                    Ok(CompiledColumn::Compiled(program))
                }
                _ => {
                    let program = compile_expression_with_context(
                        col_expr,
                        cte_columns,
                        ctx.outer_columns(),
                        self.host.cte_function_registry(),
                    )?;
                    Ok(CompiledColumn::Compiled(program))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // Create VM for expression execution (reused for all rows)
        let mut vm = ExprVM::new();
        let mut result_rows = RowVec::with_capacity(rows.len());

        for (row_id, (_, row)) in rows.iter().enumerate() {
            // OPTIMIZATION: Pre-allocate CompactVec with estimated capacity
            let mut values: CompactVec<Value> =
                CompactVec::with_capacity(columns.len().max(row.len()));
            // CRITICAL: Include params from context for parameterized queries
            let mut exec_ctx = ExecuteContext::new(row)
                .with_params(ctx.params())
                .with_named_params(ctx.named_params())
                .with_transaction_id(ctx.transaction_id())
                .with_stored_function_invoker(ctx.stored_function_invoker());
            if let Some(outer_row) = ctx.outer_row() {
                exec_ctx = exec_ctx.with_outer_row(outer_row);
            }

            for compiled in &compiled_columns {
                match compiled {
                    CompiledColumn::Star => {
                        // OPTIMIZATION: Extend with row values
                        values.extend(row.iter().cloned());
                    }
                    CompiledColumn::Identifier(idx) => {
                        values.push(row.get(*idx).cloned().unwrap_or_else(Value::null_unknown));
                    }
                    CompiledColumn::Compiled(program) => {
                        values.push(vm.execute_cow(program, &exec_ctx)?);
                    }
                }
            }

            result_rows.push((row_id as i64, Row::from_compact_vec(values)));
        }

        Ok(result_rows)
    }

    /// Check if a SELECT statement has a WITH clause
    pub(crate) fn has_cte(&self, stmt: &SelectStatement) -> bool {
        stmt.with.is_some()
    }

    /// Apply ORDER BY to rows
    fn apply_order_by_to_rows(
        &self,
        mut rows: RowVec,
        order_by: &[radixdb_sql::ast::OrderByExpression],
        columns: &[String],
    ) -> Result<RowVec> {
        if order_by.is_empty() || rows.is_empty() {
            return Ok(rows);
        }

        // Build column index map
        let col_index_map = build_column_index_map(columns);

        // Build order specs: (column_index, ascending, nulls_first)
        let order_specs: Vec<(Option<usize>, bool, Option<bool>)> = order_by
            .iter()
            .map(|ob| {
                let col_idx = match &ob.expression {
                    Expression::Identifier(id) => {
                        col_index_map.get(id.value_lower.as_str()).copied()
                    }
                    Expression::QualifiedIdentifier(qi) => {
                        // Try both qualified and unqualified names
                        let full_name =
                            format!("{}.{}", qi.qualifier, qi.name.value).to_lowercase();
                        col_index_map
                            .get(&full_name)
                            .or_else(|| col_index_map.get(qi.name.value_lower.as_str()))
                            .copied()
                    }
                    Expression::IntegerLiteral(lit) => {
                        // ORDER BY 1, 2, etc. - 1-based column position
                        let pos = lit.value as usize;
                        if pos > 0 && pos <= columns.len() {
                            Some(pos - 1)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                (col_idx, ob.ascending, ob.nulls_first)
            })
            .collect();

        // Sort using the same comparison function as the main query executor
        // RowVec derefs to Vec<(i64, Row)>, so we sort by the Row part
        rows.sort_by(|(_, a), (_, b)| {
            for (col_idx, ascending, nulls_first) in &order_specs {
                if let Some(idx) = col_idx {
                    let a_val = a.get(*idx);
                    let b_val = b.get(*idx);

                    // Check if either value is NULL
                    let a_is_null = a_val.is_none() || a_val.map(|v| v.is_null()).unwrap_or(true);
                    let b_is_null = b_val.is_none() || b_val.map(|v| v.is_null()).unwrap_or(true);

                    // Handle NULL comparison
                    if a_is_null || b_is_null {
                        if a_is_null && b_is_null {
                            continue; // Both NULL, move to next column
                        }
                        // Default: NULLS LAST for ASC, NULLS FIRST for DESC
                        let nulls_come_first = nulls_first.unwrap_or(!*ascending);
                        let cmp = if a_is_null {
                            if nulls_come_first {
                                std::cmp::Ordering::Less
                            } else {
                                std::cmp::Ordering::Greater
                            }
                        } else if nulls_come_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        };
                        return cmp;
                    }

                    // Both non-NULL - normal comparison
                    let cmp = match (a_val, b_val) {
                        (Some(av), Some(bv)) => {
                            av.partial_cmp(bv).unwrap_or(std::cmp::Ordering::Equal)
                        }
                        _ => std::cmp::Ordering::Equal,
                    };

                    let cmp = if !*ascending { cmp.reverse() } else { cmp };

                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Check if ORDER BY references source columns not present in the projected output.
    /// Returns true if sorting must happen before projection.
    /// Recursively inspects expressions (e.g., -value, value*2) for column references.
    fn order_by_needs_source_columns(
        &self,
        order_by: &[OrderByExpression],
        output_columns: &[String],
        source_columns: &[String],
    ) -> bool {
        let output_lower: AHashSet<String> =
            output_columns.iter().map(|c| c.to_lowercase()).collect();

        for ob in order_by {
            if self.expr_references_source_not_output(&ob.expression, &output_lower, source_columns)
            {
                return true;
            }
        }
        false
    }

    /// Recursively check if an expression references source columns not in the output.
    /// Conservative: returns true for any unknown composite expression to avoid
    /// silently dropping columns that ORDER BY needs.
    fn expr_references_source_not_output(
        &self,
        expr: &Expression,
        output_lower: &AHashSet<String>,
        source_columns: &[String],
    ) -> bool {
        let check = |e: &Expression| {
            self.expr_references_source_not_output(e, output_lower, source_columns)
        };
        let is_source_not_output = |name: &str| {
            !output_lower.contains(name)
                && source_columns.iter().any(|c| c.eq_ignore_ascii_case(name))
        };

        match expr {
            // Leaf column references — the core check
            Expression::Identifier(id) => is_source_not_output(id.value_lower.as_str()),
            Expression::QualifiedIdentifier(qi) => {
                is_source_not_output(qi.name.value_lower.as_str())
            }

            // Literals and constants — never reference columns
            Expression::IntegerLiteral(_)
            | Expression::FloatLiteral(_)
            | Expression::StringLiteral(_)
            | Expression::BooleanLiteral(_)
            | Expression::NullLiteral(_)
            | Expression::IntervalLiteral(_)
            | Expression::Parameter(_)
            | Expression::Star(_)
            | Expression::QualifiedStar(_)
            | Expression::Default(_) => false,

            // Composite expressions — recurse into children
            Expression::Prefix(p) => check(&p.right),
            Expression::Infix(inf) => check(&inf.left) || check(&inf.right),
            Expression::FunctionCall(fc) => fc.arguments.iter().any(&check),
            Expression::Cast(c) => check(&c.expr),
            Expression::Aliased(a) => check(&a.expression),
            Expression::Case(case) => {
                case.value.as_ref().is_some_and(|v| check(v))
                    || case
                        .when_clauses
                        .iter()
                        .any(|w| check(&w.condition) || check(&w.then_result))
                    || case.else_value.as_ref().is_some_and(|e| check(e))
            }
            Expression::Between(b) => check(&b.expr) || check(&b.lower) || check(&b.upper),
            Expression::In(i) => check(&i.left),
            Expression::Like(l) => check(&l.left) || check(&l.pattern),
            Expression::Distinct(d) => check(&d.expr),
            Expression::Window(w) => w.function.arguments.iter().any(check),

            // Unknown composite — conservatively assume it may reference source columns
            _ => true,
        }
    }

    /// Apply ORDER BY, OFFSET, and LIMIT to in-memory rows.
    /// Used by aggregation and window function paths in execute_query_on_cte_result
    /// which otherwise early-return without these post-processing steps.
    fn apply_order_by_limit_offset(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &QueryClassification,
        mut rows: RowVec,
        columns: &[String],
    ) -> Result<RowVec> {
        if classification.has_order_by {
            rows = self.apply_order_by_to_rows(rows, &stmt.order_by, columns)?;
        }

        if classification.has_offset {
            if let Some(ref offset_expr) = stmt.offset {
                let offset = evaluate_page_expression(offset_expr, ctx, "OFFSET")?;
                if offset > 0 && offset < rows.len() {
                    rows.drain(..offset);
                } else if offset >= rows.len() {
                    rows.clear();
                }
            }
        }

        if classification.has_limit {
            if let Some(ref limit_expr) = stmt.limit {
                let limit = evaluate_page_expression(limit_expr, ctx, "LIMIT")?;
                if limit < rows.len() {
                    rows.truncate(limit);
                }
            }
        }

        Ok(rows)
    }

    // =========================================================================
    // CTE INLINING OPTIMIZATION
    // =========================================================================

    /// Check if LIMIT pushdown would be more beneficial than CTE inlining.
    ///
    /// Returns true when streaming aggregation with limit pushdown will be faster
    /// than inlining as a subquery.
    fn should_use_limit_pushdown_instead(
        &self,
        stmt: &SelectStatement,
        with_clause: &WithClause,
    ) -> bool {
        // Must have LIMIT without ORDER BY
        if stmt.limit.is_none() || !stmt.order_by.is_empty() {
            return false;
        }

        // Must have a JOIN
        let join_source = match &stmt.table_expr {
            Some(expr) => match expr.as_ref() {
                Expression::JoinSource(js) => js,
                _ => return false,
            },
            None => return false,
        };

        let join_type = join_source.join_type.to_uppercase();
        let is_inner_join = join_type == "INNER" || join_type.is_empty() || join_type == "JOIN";
        let is_left_join = join_type == "LEFT" || join_type == "LEFT OUTER";
        let is_right_join = join_type == "RIGHT" || join_type == "RIGHT OUTER";

        if !is_inner_join && !is_left_join && !is_right_join {
            return false;
        }

        // Check if exactly one side is a CTE with GROUP BY
        let cte_names: AHashSet<String> = with_clause
            .ctes
            .iter()
            .filter(|c| !c.is_recursive && !c.query.group_by.columns.is_empty())
            .map(|c| c.name.value_lower.to_string())
            .collect();

        if cte_names.is_empty() {
            return false;
        }

        let left_cte = self
            .extract_cte_name_for_lookup(&join_source.left)
            .filter(|n| cte_names.contains(&n.to_lowercase()));
        let right_cte = self
            .extract_cte_name_for_lookup(&join_source.right)
            .filter(|n| cte_names.contains(&n.to_lowercase()));

        // For INNER JOIN: either side can be CTE
        // For LEFT JOIN: CTE must be on the RIGHT (each CTE row produces at most one result)
        // For RIGHT JOIN: CTE must be on the LEFT (each CTE row produces at most one result)
        match (&left_cte, &right_cte) {
            (Some(_), None) if is_inner_join || is_right_join => true,
            (None, Some(_)) if is_inner_join || is_left_join => true,
            _ => false,
        }
    }

    /// Try to inline single-use, non-recursive CTEs as subqueries.
    /// Returns Some(rewritten_stmt) if all CTEs can be inlined, None otherwise.
    ///
    /// Benefits of inlining:
    /// - Preserves index access (materialized CTEs lose all indexes)
    /// - Enables LIMIT pushdown through subqueries
    /// - Avoids memory overhead of full CTE materialization
    pub(crate) fn try_inline_ctes(
        &self,
        stmt: &SelectStatement,
        with_clause: &WithClause,
    ) -> Option<SelectStatement> {
        // Inlining removes WITH from the rewritten statement, so it is only
        // admitted for the one shape whose complete reference graph is the
        // FROM item itself. Rich expressions and compound branches stay
        // materialized until their full AST dependency graph is proven.
        let simple_projection = stmt.columns.len() == 1
            && matches!(
                stmt.columns[0],
                Expression::Star(_) | Expression::QualifiedStar(_)
            );
        if !simple_projection
            || stmt.where_clause.is_some()
            || stmt.having.is_some()
            || !stmt.group_by.columns.is_empty()
            || !stmt.window_defs.is_empty()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
            || stmt.offset.is_some()
            || !stmt.set_operations.is_empty()
            || stmt.distinct
            || !stmt.distinct_on.is_empty()
        {
            return None;
        }

        // Early exit: no table expression means nothing to inline
        let table_expr = stmt.table_expr.as_ref()?;

        // Skip inlining if LIMIT pushdown with streaming would be more beneficial.
        // This happens when:
        // 1. Main query has LIMIT (no ORDER BY)
        // 2. CTE has GROUP BY (can use streaming aggregation)
        // 3. CTE is in INNER JOIN (limit pushdown is safe)
        if self.should_use_limit_pushdown_instead(stmt, with_clause) {
            return None;
        }

        // Pre-compute lowercase CTE names once to avoid repeated to_lowercase() calls
        // Store (lowercase_name, original_cte) pairs
        let cte_names_lower: Vec<(String, &CommonTableExpression)> = with_clause
            .ctes
            .iter()
            .map(|cte| {
                // Early exit for conditions that prevent inlining
                if cte.is_recursive || !cte.column_names.is_empty() {
                    return Err(());
                }
                Ok((cte.name.value_lower.to_string(), cte))
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .ok()?;

        // Build map from pre-computed lowercase names
        let cte_defs: StringMap<&CommonTableExpression> = cte_names_lower.iter().cloned().collect();
        let cte_name_set: AHashSet<&str> = cte_defs.keys().map(|s| s.as_str()).collect();

        // Check if any CTE references another CTE (CTE chaining)
        // These cannot be simply inlined as they have data dependencies
        for (_, cte) in &cte_names_lower {
            for other_cte_name in &cte_name_set {
                if self.query_references_cte(&cte.query, other_cte_name) {
                    // CTE references another CTE - can't inline
                    return None;
                }
            }
        }

        // Count CTE references in the main query using pre-computed names
        // Separate counts for table expressions (JOIN targets) vs WHERE clause subqueries
        let mut table_ref_counts: StringMap<usize> =
            cte_defs.keys().map(|name| (name.clone(), 0)).collect();
        let mut where_ref_counts: StringMap<usize> = table_ref_counts.clone();

        // Count references in table expression (FROM/JOIN)
        self.count_cte_references_in_expr(table_expr, &mut table_ref_counts);

        // Count references in WHERE clause (including IN/EXISTS subqueries)
        if let Some(ref where_clause) = stmt.where_clause {
            self.count_cte_references_in_expr(where_clause, &mut where_ref_counts);
        }

        // Only inline CTEs that:
        // 1. Are used exactly once in table expressions (FROM/JOIN)
        // 2. Are NOT used in WHERE clause subqueries (these need special handling)
        for name in cte_defs.keys() {
            let table_refs = table_ref_counts.get(name).copied().unwrap_or(0);
            let where_refs = where_ref_counts.get(name).copied().unwrap_or(0);

            // Skip if used in WHERE clause - subquery handling is different
            if where_refs > 0 {
                return None;
            }

            // Skip if used more than once - multi-use benefits from materialization
            if table_refs > 1 {
                return None;
            }
            // table_refs == 0 (unused) or table_refs == 1 (single-use) can be inlined
        }

        // All CTEs are single-use - perform inlining
        // OPTIMIZATION: Only clone if something actually changes
        // First check if any CTE references exist that we can inline
        let any_refs = table_ref_counts.values().any(|&count| count > 0);
        if !any_refs {
            // No CTE references in table expression - no inlining needed
            return None;
        }

        // Try to inline - if nothing changes, skip the expensive cloning
        let inlined_expr = self.try_inline_cte_references(table_expr, &cte_defs)?;

        Some(SelectStatement {
            token: stmt.token.clone(),
            distinct: stmt.distinct,
            distinct_on: stmt.distinct_on.clone(),
            columns: stmt.columns.clone(),
            with: None, // Remove WITH clause
            table_expr: Some(Box::new(inlined_expr)),
            where_clause: stmt.where_clause.clone(),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            window_defs: stmt.window_defs.clone(),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit.clone(),
            offset: stmt.offset.clone(),
            set_operations: stmt.set_operations.clone(),
        })
    }

    /// Check if a query references a specific CTE by name
    fn query_references_cte(&self, stmt: &SelectStatement, cte_name: &str) -> bool {
        // Check table expression
        if let Some(ref table_expr) = stmt.table_expr {
            if self.expr_references_cte(table_expr, cte_name) {
                return true;
            }
        }

        // Check WHERE clause
        if let Some(ref where_clause) = stmt.where_clause {
            if self.expr_references_cte(where_clause, cte_name) {
                return true;
            }
        }

        false
    }

    /// Check if an expression references a CTE
    fn expr_references_cte(&self, expr: &Expression, cte_name: &str) -> bool {
        match expr {
            Expression::CteReference(cte_ref) => cte_ref.name.value.eq_ignore_ascii_case(cte_name),
            Expression::TableSource(ts) => ts.name.value.eq_ignore_ascii_case(cte_name),
            Expression::Identifier(id) => id.value.eq_ignore_ascii_case(cte_name),
            Expression::JoinSource(js) => {
                self.expr_references_cte(&js.left, cte_name)
                    || self.expr_references_cte(&js.right, cte_name)
            }
            Expression::SubquerySource(sq) => self.query_references_cte(&sq.subquery, cte_name),
            Expression::ScalarSubquery(sq) => self.query_references_cte(&sq.subquery, cte_name),
            Expression::In(in_expr) => {
                // Check if right side is a ScalarSubquery
                if let Expression::ScalarSubquery(sq) = &*in_expr.right {
                    self.query_references_cte(&sq.subquery, cte_name)
                } else {
                    false
                }
            }
            Expression::Exists(ex) => self.query_references_cte(&ex.subquery, cte_name),
            Expression::Infix(infix) => {
                self.expr_references_cte(&infix.left, cte_name)
                    || self.expr_references_cte(&infix.right, cte_name)
            }
            _ => false,
        }
    }

    /// Count CTE references in a SELECT statement
    fn count_cte_references_in_stmt(
        &self,
        stmt: &SelectStatement,
        ref_counts: &mut StringMap<usize>,
    ) {
        // Check table expression
        if let Some(ref table_expr) = stmt.table_expr {
            self.count_cte_references_in_expr(table_expr, ref_counts);
        }

        // Check WHERE clause
        if let Some(ref where_clause) = stmt.where_clause {
            self.count_cte_references_in_expr(where_clause, ref_counts);
        }

        // Check SELECT columns for subqueries
        for col in &stmt.columns {
            self.count_cte_references_in_expr(col, ref_counts);
        }
    }

    /// Count CTE references in an expression
    fn count_cte_references_in_expr(&self, expr: &Expression, ref_counts: &mut StringMap<usize>) {
        match expr {
            Expression::CteReference(cte_ref) => {
                let name: &str = cte_ref.name.value_lower.as_str();
                if let Some(count) = ref_counts.get_mut(name) {
                    *count += 1;
                }
            }
            Expression::TableSource(ts) => {
                let name: &str = ts.name.value_lower.as_str();
                if let Some(count) = ref_counts.get_mut(name) {
                    *count += 1;
                }
            }
            Expression::Identifier(id) => {
                let name: &str = id.value_lower.as_str();
                if let Some(count) = ref_counts.get_mut(name) {
                    *count += 1;
                }
            }
            Expression::JoinSource(js) => {
                self.count_cte_references_in_expr(&js.left, ref_counts);
                self.count_cte_references_in_expr(&js.right, ref_counts);
            }
            Expression::SubquerySource(sq) => {
                self.count_cte_references_in_stmt(&sq.subquery, ref_counts);
            }
            Expression::ScalarSubquery(sq) => {
                self.count_cte_references_in_stmt(&sq.subquery, ref_counts);
            }
            Expression::In(in_expr) => {
                self.count_cte_references_in_expr(&in_expr.left, ref_counts);
                // Check if right side is a ScalarSubquery
                if let Expression::ScalarSubquery(sq) = &*in_expr.right {
                    self.count_cte_references_in_stmt(&sq.subquery, ref_counts);
                }
            }
            Expression::Exists(ex) => {
                self.count_cte_references_in_stmt(&ex.subquery, ref_counts);
            }
            Expression::Infix(infix) => {
                self.count_cte_references_in_expr(&infix.left, ref_counts);
                self.count_cte_references_in_expr(&infix.right, ref_counts);
            }
            Expression::Aliased(aliased) => {
                self.count_cte_references_in_expr(&aliased.expression, ref_counts);
            }
            _ => {}
        }
    }

    /// Replace CTE references with subqueries in an expression.
    /// Returns Some(new_expr) if any replacement was made, None if no changes needed.
    fn try_inline_cte_references(
        &self,
        expr: &Expression,
        cte_defs: &StringMap<&CommonTableExpression>,
    ) -> Option<Expression> {
        match expr {
            Expression::CteReference(cte_ref) => {
                // Use pre-computed lowercase from value_lower if available
                let name = &cte_ref.name.value_lower;
                cte_defs.get(name.as_str()).map(|cte| {
                    // Convert CTE to SubquerySource
                    let alias = cte_ref
                        .alias
                        .clone()
                        .unwrap_or_else(|| cte_ref.name.clone());
                    Expression::SubquerySource(Box::new(SubqueryTableSource {
                        token: Token::new(TokenType::Punctuator, "(", Position::new(0, 0, 0)),
                        subquery: cte.query.clone(),
                        alias: Some(alias),
                    }))
                })
            }
            Expression::TableSource(ts) => {
                // Use pre-computed lowercase from value_lower
                let name = &ts.name.value_lower;
                cte_defs.get(name.as_str()).map(|cte| {
                    // Convert to SubquerySource preserving alias
                    let alias = ts.alias.clone().unwrap_or_else(|| ts.name.clone());
                    Expression::SubquerySource(Box::new(SubqueryTableSource {
                        token: Token::new(TokenType::Punctuator, "(", Position::new(0, 0, 0)),
                        subquery: cte.query.clone(),
                        alias: Some(alias),
                    }))
                })
            }
            Expression::JoinSource(js) => {
                let left_changed = self.try_inline_cte_references(&js.left, cte_defs);
                let right_changed = self.try_inline_cte_references(&js.right, cte_defs);

                // Only create new JoinSource if something changed
                if left_changed.is_some() || right_changed.is_some() {
                    let left = left_changed.unwrap_or_else(|| (*js.left).clone());
                    let right = right_changed.unwrap_or_else(|| (*js.right).clone());
                    Some(Expression::JoinSource(Box::new(JoinTableSource {
                        token: js.token.clone(),
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type: js.join_type.clone(),
                        condition: js.condition.clone(),
                        using_columns: js.using_columns.clone(),
                    })))
                } else {
                    None
                }
            }
            Expression::SubquerySource(sq) => {
                // Only recurse if there's a table_expr
                if let Some(ref table_expr) = sq.subquery.table_expr {
                    if let Some(inlined) = self.try_inline_cte_references(table_expr, cte_defs) {
                        let mut new_subquery = (*sq.subquery).clone();
                        new_subquery.table_expr = Some(Box::new(inlined));
                        return Some(Expression::SubquerySource(Box::new(SubqueryTableSource {
                            token: sq.token.clone(),
                            subquery: Box::new(new_subquery),
                            alias: sq.alias.clone(),
                        })));
                    }
                }
                None
            }
            _ => None, // No change needed
        }
    }
}
