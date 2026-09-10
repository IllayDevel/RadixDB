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

//! DML Statement Execution
//!
//! This module implements execution of Data Manipulation Language (DML) statements:
//! - INSERT
//! - UPDATE
//! - DELETE

use radixdb_catalog::TriggerTiming;
use radixdb_core::CompactArc;
use radixdb_core::I64Set;
use radixdb_core::SmartString;
use radixdb_core::{DataType, Error, Result, Row, Schema, Value};
use radixdb_sql::ast::*;
use radixdb_storage::expression::Expression as StorageExpr;
use radixdb_storage::traits::{Engine, QueryResult, Table};
use rustc_hash::FxHashMap;
use std::sync::Arc;

use super::dml_support::*;
use super::returning::build_returning_result;
use super::upsert::{apply_on_duplicate_update, compile_upsert};
use crate::compiled_plan::{CompiledExecution, CompiledInsert};
use crate::context::{
    invalidate_in_subquery_cache_for_table, invalidate_scalar_subquery_cache_for_table,
    invalidate_semi_join_cache_for_table, ExecutionContext,
};
use crate::expression::CompiledEvaluator;
use crate::mutation::host::MutationHost;
use crate::mutation::validation::{
    compile_table_check_constraints, prepare_insert_row_constraints,
    validate_resulting_row_constraints,
};
use crate::procedural::{DmlTriggerEvent, DmlTriggerPlan};
use crate::pushdown;
use crate::result::ExecResult;
use crate::utils::dummy_token_clone;
use std::sync::RwLock;

#[doc(hidden)]
pub trait DmlExecutorExt: MutationHost {
    fn execute_with_trigger_statement<T>(
        &self,
        plan: &DmlTriggerPlan,
        ctx: &ExecutionContext,
        execute: impl FnOnce(&DmlTriggerPlan) -> Result<T>,
    ) -> Result<T> {
        let boundary = self.mutation_begin_trigger_boundary()?;
        let outcome = (|| {
            self.mutation_fire_statement_triggers(plan, TriggerTiming::Before, ctx)?;
            let value = execute(plan)?;
            self.mutation_fire_statement_triggers(plan, TriggerTiming::After, ctx)?;
            Ok(value)
        })();
        match outcome {
            Ok(value) => match self.mutation_complete_trigger_boundary(&boundary) {
                Ok(()) => Ok(value),
                Err(primary) => {
                    match self.mutation_abort_trigger_boundary(&boundary) {
                        Ok(()) => Err(primary),
                        Err(cleanup) => Err(Error::internal(format!(
                            "trigger statement commit failed: {primary}; rollback also failed: {cleanup}"
                        ))),
                    }
                }
            },
            Err(primary) => match self.mutation_abort_trigger_boundary(&boundary) {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(Error::internal(format!(
                    "trigger statement failed: {primary}; rollback also failed: {cleanup}"
                ))),
            },
        }
    }

    fn accept_inserted_row(
        &self,
        triggers: Option<&DmlTriggerPlan>,
        inserted: Option<Row>,
        has_returning: bool,
        returning_rows: &mut Vec<Row>,
        ctx: &ExecutionContext,
    ) -> Result<()> {
        if let Some(inserted) = inserted {
            if let Some(plan) = triggers {
                self.mutation_fire_after_row_triggers(plan, None, Some(&inserted), None, ctx)?;
            }
            if has_returning {
                returning_rows.push(inserted);
            }
        }
        Ok(())
    }

    /// Select row_ids for DML operations using the full SELECT executor.
    /// This reuses all SELECT optimizations (indexes, semi-joins, parallel execution, etc.)
    /// for UPDATE and DELETE operations.
    ///
    /// Returns Some(row_ids) if:
    /// - Table has a single-column INTEGER PRIMARY KEY
    /// - WHERE clause exists
    ///
    /// Returns None to fall back to storage layer's scan-based approach.
    fn select_row_ids_for_dml(
        &self,
        table_name: &str,
        where_clause: &Expression,
        schema: &Schema,
        table: &dyn Table,
        ctx: &ExecutionContext,
    ) -> Result<Option<Vec<i64>>> {
        // Check if this is a single-column INTEGER PRIMARY KEY
        let pk_indices = schema.primary_key_indices();
        if pk_indices.len() != 1 {
            return Ok(None);
        }

        let pk_idx = pk_indices[0];
        let pk_col = &schema.columns[pk_idx];

        // Must be INTEGER type (where value = row_id)
        if pk_col.data_type != DataType::Integer {
            return Ok(None);
        }

        let pk_column_name = &pk_col.name;
        let pk_column_lower = pk_column_name.to_lowercase();

        // FAST PATH: If WHERE is InHashSet on the PK column, extract row_ids directly
        // This avoids building and executing a full SELECT statement
        if let Expression::InHashSet(in_expr) = where_clause {
            if let Expression::Identifier(id) = in_expr.column.as_ref() {
                if id.value_lower == pk_column_lower {
                    if in_expr.not {
                        // NOT IN: get all active row_ids and exclude the ones in the set
                        let excluded: I64Set = in_expr
                            .values
                            .iter()
                            .filter_map(|v| match v {
                                Value::Integer(i) => Some(*i),
                                _ => None,
                            })
                            .collect();

                        let mut row_ids: Vec<i64> = table
                            .get_active_row_ids()
                            .into_iter()
                            .filter(|id| !excluded.contains(*id))
                            .collect();
                        row_ids.sort_unstable();
                        return Ok(Some(row_ids));
                    } else {
                        // IN: extract integer values directly from the HashSet
                        let mut row_ids: Vec<i64> = in_expr
                            .values
                            .iter()
                            .filter_map(|v| match v {
                                Value::Integer(i) => Some(*i),
                                _ => None,
                            })
                            .collect();
                        row_ids.sort_unstable();
                        return Ok(Some(row_ids));
                    }
                }
            }
        }

        // GENERAL PATH: Build SELECT query and use full executor
        let select_stmt = SelectStatement {
            token: dummy_token_clone(),
            distinct: false,
            distinct_on: vec![],
            columns: vec![Expression::Identifier(Identifier::new(
                dummy_token_clone(),
                pk_column_name.clone(),
            ))],
            with: None,
            table_expr: Some(Box::new(Expression::TableSource(Box::new(
                SimpleTableSource {
                    token: dummy_token_clone(),
                    name: Identifier::new(dummy_token_clone(), table_name.to_string()),
                    alias: None,
                    as_of: None,
                },
            )))),
            where_clause: Some(Box::new(where_clause.clone())),
            group_by: GroupByClause::default(),
            having: None,
            window_defs: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operations: Vec::new(),
        };

        // Execute using full SELECT executor (gets all optimizations)
        let mut result = self.mutation_execute_select(&select_stmt, ctx)?;

        // Collect row_ids from result
        let mut row_ids = Vec::new();
        while result.next() {
            let row = result.row();
            if let Some(Value::Integer(id)) = row.get(0) {
                row_ids.push(*id);
            }
        }
        if let Some(err) = result.last_error() {
            return Err(err);
        }

        // Sort for cache locality
        row_ids.sort_unstable();

        Ok(Some(row_ids))
    }

    /// Execute an INSERT statement
    fn execute_insert(
        &self,
        stmt: &InsertStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let plan = self.mutation_prepare_dml_triggers(
            stmt.table_name.value_lower.as_str(),
            DmlTriggerEvent::Insert,
            &[],
            ctx,
        )?;
        if plan.is_empty() {
            return self.execute_insert_body(stmt, ctx, None);
        }
        self.execute_with_trigger_statement(&plan, ctx, |plan| {
            self.execute_insert_body(stmt, ctx, Some(plan))
        })
    }

    fn execute_insert_body(
        &self,
        stmt: &InsertStatement,
        ctx: &ExecutionContext,
        triggers: Option<&DmlTriggerPlan>,
    ) -> Result<Box<dyn QueryResult>> {
        // OPTIMIZATION: Use pre-computed lowercase name to avoid allocation per query
        let table_name = &stmt.table_name.value_lower;

        // Check if there's an active explicit transaction
        let mut active_tx = self.mutation_active_transaction().lock().unwrap();

        let (mut table, should_auto_commit, standalone_tx) =
            if let Some(ref mut tx_state) = *active_tx {
                // Use the active transaction
                // NOTE: table_name is already lowercase (value_lower from AST)
                let table = tx_state.transaction.get_table(table_name)?;

                // Store a reference to this table for commit/rollback
                if !tx_state.tables.contains_key(table_name.as_str()) {
                    tx_state.tables.insert(
                        table_name.to_string(),
                        tx_state.transaction.get_table(table_name)?,
                    );
                }

                (table, false, None)
            } else {
                // No active transaction - create a standalone transaction with auto-commit
                let tx = self.mutation_engine().begin_transaction()?;
                let table = tx.get_table(table_name)?;
                (table, true, Some(tx))
            };

        // Drop the lock before doing work
        drop(active_tx);

        // ON CONFLICT uses the same key/row ownership as ordinary writes:
        // committed unique indexes serialize absent-key publication and the
        // MVCC row-claim path serializes updates of an existing conflict row.
        // A table-wide mutex here would make unrelated keys wait behind a long
        // INSERT SELECT without adding a stronger correctness boundary.
        let resulting_schema = table.schema().clone();

        // Pre-compute schema information to avoid repeated borrows during insert
        let schema_column_count: usize;
        let column_indices: Vec<usize>;
        // Pre-compute column types for type coercion
        let column_types: Vec<radixdb_core::DataType>;
        // Pre-compute vector dimensions for vector columns (0 for non-vector)
        let column_vector_dims: Vec<u16>;
        // Pre-compute column names for error messages
        let column_names: Vec<String>;
        // Pre-compute ALL column types for default values and check constraints
        let all_column_types: Vec<radixdb_core::DataType>;
        // Pre-compute default values and check expressions for all columns
        let default_exprs: Vec<Option<String>>;
        let auto_increment_pk_idx: Option<usize>;
        {
            let schema = table.schema();
            schema_column_count = schema.columns.len();
            auto_increment_pk_idx = auto_increment_pk_index(schema);

            // Extract default and check expressions from schema
            default_exprs = schema
                .columns
                .iter()
                .map(|c| c.default_expr.clone())
                .collect();
            all_column_types = schema.columns.iter().map(|c| c.data_type).collect();

            // OPTIMIZATION: When no columns specified, insert into all columns in order
            // Skip all column name lookups - just use sequential indices
            if stmt.columns.is_empty() {
                column_indices = (0..schema_column_count).collect();
                column_types = all_column_types.clone();
                column_vector_dims = schema.columns.iter().map(|c| c.vector_dimensions).collect();
                // Use schema's cached column names - avoids re-collecting on every INSERT
                column_names = schema.column_names_owned().to_vec();
            } else {
                // Validate columns exist and pre-compute their indices
                // OPTIMIZATION: Use cached column_index_map for O(1) lookups instead of O(n) linear scan
                let col_map = schema.column_index_map();
                column_indices = stmt
                    .columns
                    .iter()
                    .map(|id| {
                        // Use pre-computed lowercase value from AST
                        col_map
                            .get(id.value_lower.as_str())
                            .copied()
                            .ok_or_else(|| Error::ColumnNotFound(id.value.to_string()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                // Get column types for the specified columns
                column_types = column_indices
                    .iter()
                    .map(|&idx| schema.columns[idx].data_type)
                    .collect();
                column_vector_dims = column_indices
                    .iter()
                    .map(|&idx| schema.columns[idx].vector_dimensions)
                    .collect();
                // Get column names for error messages
                column_names = column_indices
                    .iter()
                    .map(|&idx| schema.columns[idx].name.clone())
                    .collect();
            }
        }

        let mut seen_insert_targets = rustc_hash::FxHashSet::default();
        for &column_index in &column_indices {
            if !seen_insert_targets.insert(column_index) {
                return Err(Error::InvalidArgument(
                    "INSERT target column is specified more than once".to_string(),
                ));
            }
        }

        // Pre-compute FK info for parent validation (CompactArc ref-count bump, not deep clone)
        let fk_schema = if !table.schema().foreign_keys.is_empty() {
            Some(table.schema().clone())
        } else {
            None
        };
        let compiled_table_checks = compile_table_check_constraints(table.schema())?;
        validate_conflict_target(&stmt.conflict_target, table.schema(), &*table)?;
        // Create VM for constant expression evaluation (reused for all INSERT values)
        use crate::expression::{compile_expression, ExecuteContext, ExprVM};
        let mut vm = ExprVM::new();
        let params = ctx.params();
        let named_params = ctx.named_params();
        let empty_row = Row::new();

        // OPTIMIZATION: Pre-build ExecuteContext once (reused for all expressions)
        let mut base_exec_ctx = ExecuteContext::new(&empty_row);
        if !params.is_empty() {
            base_exec_ctx = base_exec_ctx.with_params(params);
        }
        if !named_params.is_empty() {
            base_exec_ctx = base_exec_ctx.with_named_params(named_params);
        }
        base_exec_ctx = base_exec_ctx
            .with_transaction_id(ctx.transaction_id())
            .with_stored_function_invoker(ctx.stored_function_invoker());

        let mut rows_affected = 0i64;
        let mut last_insert_id = 0i64;

        // RETURNING clause support - collect inserted rows if RETURNING is specified
        let has_returning = !stmt.returning.is_empty();
        let needs_inserted_row =
            has_returning || triggers.is_some_and(DmlTriggerPlan::has_after_row_triggers);
        let mut returning_rows: Vec<Row> = Vec::new();
        // OPTIMIZATION: Only get column names Arc when RETURNING is used (avoids 7ms clone)
        let schema_column_names_arc = if has_returning {
            Some(table.schema().column_names_arc())
        } else {
            None
        };

        // Check if this is INSERT ... SELECT
        if let Some(ref select_stmt) = stmt.select {
            // Get schema for conflict handling (needed for duplicate row lookup and target matching)
            let select_schema = if stmt.on_duplicate || stmt.do_nothing {
                Some(self.mutation_engine().get_table_schema(table_name)?)
            } else {
                None
            };

            // Pre-compile upsert expressions once for all conflicting rows in this batch.
            // Without this, compile_upsert work repeats for every conflict (O(n) cost).
            let compiled_upsert = if stmt.on_duplicate {
                if let Some(ref s) = select_schema {
                    Some(compile_upsert(self, s, stmt)?)
                } else {
                    None
                }
            } else {
                None
            };

            // For explicit transactions, materialize the SELECT BEFORE any inserts
            // to ensure statement atomicity: a late runtime error (e.g., invalid REGEXP)
            // won't leave partial inserts pending for commit. Auto-commit transactions
            // stream rows directly since the standalone transaction rolls back on drop.
            let mut select_result = self.mutation_execute_select(select_stmt, ctx)?;
            if !should_auto_commit {
                // Explicit tx: fully materialize before writes for atomicity.
                // A late runtime error (e.g., invalid REGEXP) will fail here
                // before any inserts, preventing partial writes in the transaction.
                let columns = select_result.columns().to_vec();
                let rows = <Self as MutationHost>::mutation_materialize_result(select_result)?;
                select_result = Box::new(crate::result::ExecutorResult::new(columns, rows));
            }

            // Process each row from the SELECT result (streaming or materialized)
            while select_result.next() {
                let select_row = select_result.row();
                if select_row.len() != column_indices.len() {
                    return Err(Error::InvalidArgument(format!(
                        "INSERT has {} columns but SELECT returns {} columns",
                        column_indices.len(),
                        select_row.len()
                    )));
                }

                // Build row values - initialize with DEFAULT values for missing columns
                // This matches the behavior of regular INSERT
                let mut row_values = Vec::with_capacity(schema_column_count);
                for i in 0..schema_column_count {
                    if let Some(ref default_expr) = default_exprs[i] {
                        let default_type = all_column_types[i];
                        row_values.push(evaluate_default_expr(default_expr, default_type)?);
                    } else {
                        row_values.push(Value::null_unknown());
                    }
                }

                // Fill in values from SELECT using pre-computed indices with type coercion
                for (i, value) in select_row.iter().enumerate() {
                    // Coerce value to target column type
                    let coerced = value.coerce_to_type(column_types[i]);
                    // Validate coercion didn't silently fail
                    validate_coercion(
                        value,
                        &coerced,
                        &column_names[i],
                        column_types[i],
                        column_vector_dims[i],
                    )?;
                    row_values[column_indices[i]] = coerced;
                }

                let mut row = Row::from_values(row_values);
                prepare_insert_row_constraints(
                    &mut *table,
                    &resulting_schema,
                    &compiled_table_checks,
                    &mut row,
                    &mut vm,
                )?;
                let row = if let Some(plan) = triggers {
                    let Some(row) =
                        self.mutation_fire_before_row_triggers(plan, None, Some(row), None, ctx)?
                    else {
                        continue;
                    };
                    validate_resulting_row_constraints(
                        &resulting_schema,
                        &compiled_table_checks,
                        &row,
                        &mut vm,
                    )?;
                    row
                } else {
                    row
                };
                // EXCLUDED/new-row expressions must observe the generated key,
                // not the NULL placeholder supplied by the statement.
                let saved_row_values = stmt.on_duplicate.then(|| row.as_slice().to_vec());

                // FK parent validation (zero-cost if no FKs)
                if let Some(ref fks) = fk_schema {
                    crate::mutation::foreign_key::check_parent_exists(
                        self.mutation_engine(),
                        table.txn_id(),
                        fks,
                        &row,
                    )?;
                }

                if stmt.do_nothing {
                    let schema_ref = select_schema.as_ref().unwrap();
                    // ON CONFLICT DO NOTHING — silently skip duplicates
                    let insert_result = insert_row_for_command_result(
                        &mut *table,
                        row,
                        needs_inserted_row,
                        auto_increment_pk_idx,
                        &mut last_insert_id,
                    );
                    match insert_result {
                        Ok(opt_row) => {
                            self.accept_inserted_row(
                                triggers,
                                opt_row,
                                has_returning,
                                &mut returning_rows,
                                ctx,
                            )?;
                            rows_affected += 1;
                        }
                        Err(ref e @ Error::PrimaryKeyConstraint { .. })
                        | Err(ref e @ Error::UniqueConstraint { .. }) => {
                            if !conflict_matches_target(&stmt.conflict_target, schema_ref, e) {
                                return Err(e.clone());
                            }
                            // DO NOTHING: conflict skipped, no RETURNING row
                        }
                        Err(e) => return Err(e),
                    }
                } else if stmt.on_duplicate {
                    let row_values = saved_row_values.as_ref().unwrap();
                    let schema_ref = select_schema.as_ref().unwrap();
                    let compiled_upsert = compiled_upsert.as_ref().ok_or_else(|| {
                        Error::internal("missing precompiled ON CONFLICT update plan")
                    })?;
                    // ON CONFLICT DO UPDATE / ON DUPLICATE KEY UPDATE
                    let insert_result = insert_row_for_command_result(
                        &mut *table,
                        row,
                        needs_inserted_row,
                        auto_increment_pk_idx,
                        &mut last_insert_id,
                    );
                    match insert_result {
                        Ok(opt_row) => {
                            self.accept_inserted_row(
                                triggers,
                                opt_row,
                                has_returning,
                                &mut returning_rows,
                                ctx,
                            )?;
                            rows_affected += 1;
                        }
                        Err(ref e @ Error::PrimaryKeyConstraint { row_id }) => {
                            if !conflict_matches_target(&stmt.conflict_target, schema_ref, e) {
                                return Err(Error::PrimaryKeyConstraint { row_id });
                            }
                            match apply_on_duplicate_update(
                                self,
                                &mut table,
                                schema_ref,
                                row_id,
                                None,
                                row_values,
                                compiled_upsert,
                                ctx,
                                has_returning,
                            ) {
                                Ok(Some(updated_row)) => {
                                    returning_rows.push(updated_row);
                                    rows_affected += 1;
                                }
                                Ok(None) => {
                                    rows_affected += 1;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Err(
                            ref e @ Error::UniqueConstraint {
                                ref index,
                                ref column,
                                ref value,
                                row_id: conflict_rid,
                            },
                        ) => {
                            if !conflict_matches_target(&stmt.conflict_target, schema_ref, e) {
                                return Err(Error::UniqueConstraint {
                                    index: index.clone(),
                                    column: column.clone(),
                                    value: value.clone(),
                                    row_id: conflict_rid,
                                });
                            }
                            // Use row_id from the error if available (cold segment check
                            // already found it). Only fall back to re-search if row_id < 0
                            // (hot index path sets row_id = -1 when unknown).
                            let found_row_id = if conflict_rid >= 0 {
                                Ok(Some(conflict_rid))
                            } else {
                                find_row_by_unique_index(
                                    &*table, schema_ref, index, column, row_values,
                                )
                            };
                            match found_row_id {
                                Ok(Some(row_id)) => {
                                    match apply_on_duplicate_update(
                                        self,
                                        &mut table,
                                        schema_ref,
                                        row_id,
                                        Some(column),
                                        row_values,
                                        compiled_upsert,
                                        ctx,
                                        has_returning,
                                    ) {
                                        Ok(Some(updated_row)) => {
                                            returning_rows.push(updated_row);
                                            rows_affected += 1;
                                        }
                                        Ok(None) => {
                                            rows_affected += 1;
                                        }
                                        Err(e) => return Err(e),
                                    }
                                }
                                Ok(None) => {
                                    return Err(Error::UniqueConstraint {
                                        index: index.clone(),
                                        column: column.clone(),
                                        value: value.clone(),
                                        row_id: -1,
                                    });
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    let inserted = insert_row_for_command_result(
                        &mut *table,
                        row,
                        needs_inserted_row,
                        auto_increment_pk_idx,
                        &mut last_insert_id,
                    )?;
                    self.accept_inserted_row(
                        triggers,
                        inserted,
                        has_returning,
                        &mut returning_rows,
                        ctx,
                    )?;
                    rows_affected += 1;
                }
            }
            // For streaming (auto-commit) path, check for runtime filter errors
            if let Some(err) = select_result.last_error() {
                return Err(err);
            }

            // Invalidate semantic cache for this table BEFORE commit
            // CRITICAL: Must invalidate before commit to prevent stale data window
            // where concurrent queries could see new data in storage but get old cached results
            if rows_affected > 0 {
                self.mutation_invalidate_semantic_cache(table_name);
                invalidate_semi_join_cache_for_table(table_name);
                invalidate_scalar_subquery_cache_for_table(table_name);
                invalidate_in_subquery_cache_for_table(table_name);
            }

            let mut returning_result = if has_returning {
                Some(build_returning_result(
                    &stmt.returning,
                    std::mem::take(&mut returning_rows),
                    schema_column_names_arc.as_ref().unwrap(),
                    ctx,
                )?)
            } else {
                None
            };

            // Commit if this is a standalone (auto-commit) transaction
            if should_auto_commit {
                if let Some(mut tx) = standalone_tx {
                    match tx.commit() {
                        Ok(()) => {}
                        Err(e)
                            if (stmt.on_duplicate || stmt.do_nothing)
                                && e.is_pk_or_unique_violation() =>
                        {
                            if stmt.on_duplicate && ctx.query_depth() == 0 {
                                // Commit-time PK/unique violation during upsert:
                                // a concurrent plain INSERT committed first. Retry once.
                                let retry_ctx = ctx.with_incremented_query_depth();
                                return self.execute_insert(stmt, &retry_ctx);
                            }
                            if stmt.do_nothing {
                                // DO NOTHING: returning 0 rows is the correct semantic
                                rows_affected = 0;
                                last_insert_id = 0;
                                returning_result = Some(build_returning_result(
                                    &stmt.returning,
                                    Vec::new(),
                                    schema_column_names_arc.as_ref().unwrap(),
                                    ctx,
                                )?);
                            } else {
                                return Err(e);
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            // Handle RETURNING clause for INSERT...SELECT
            if let Some(result) = returning_result {
                return Ok(result);
            }

            return Ok(Box::new(ExecResult::with_last_insert_id(
                rows_affected,
                last_insert_id,
            )));
        }

        // Process each row of values - use fast path for normal INSERT, slow path for conflict handling
        if stmt.do_nothing || stmt.on_duplicate {
            // ON DUPLICATE KEY UPDATE requires schema (CompactArc ref-count bump, not deep clone)
            let schema = self.mutation_engine().get_table_schema(table_name)?;

            // Pre-compile upsert expressions once for all conflicting rows in this batch.
            // Without this, compile_upsert work repeats for every conflict (O(n) cost).
            let compiled_upsert = if stmt.on_duplicate {
                Some(compile_upsert(self, &schema, stmt)?)
            } else {
                None
            };

            for value_row in &stmt.values {
                if value_row.len() != column_indices.len() {
                    return Err(Error::InvalidArgument(format!(
                        "INSERT has {} columns but {} values",
                        column_indices.len(),
                        value_row.len()
                    )));
                }

                // Build row values - need Vec for error handling paths
                let mut row_values = Vec::with_capacity(schema_column_count);
                for i in 0..schema_column_count {
                    if let Some(ref default_expr) = default_exprs[i] {
                        let default_type = all_column_types[i];
                        row_values.push(evaluate_default_expr(default_expr, default_type)?);
                    } else {
                        row_values.push(Value::null_unknown());
                    }
                }
                // Fill in provided values using pre-computed indices with type coercion
                for (i, expr) in value_row.iter().enumerate() {
                    // Handle DEFAULT keyword - skip this column to use pre-initialized default
                    if matches!(expr, Expression::Default(_)) {
                        continue;
                    }
                    // OPTIMIZATION: Try to extract literal value directly without VM compilation
                    // This avoids ~1-2μs per expression for simple literals (INTEGER, TEXT, etc.)
                    let value = if let Some(lit_value) = try_extract_literal(expr) {
                        lit_value
                    } else {
                        // Fall back to VM for complex expressions (Parameters, functions, etc.)
                        let program = compile_expression(expr, &[])?;
                        vm.execute_cow(&program, &base_exec_ctx)?
                    };
                    // Coerce to target type
                    let coerced = value.coerce_to_type(column_types[i]);
                    // Validate coercion didn't silently fail
                    validate_coercion(
                        &value,
                        &coerced,
                        &column_names[i],
                        column_types[i],
                        column_vector_dims[i],
                    )?;
                    row_values[column_indices[i]] = coerced;
                }

                // Create row from values (ON DUPLICATE KEY needs values for error handling)
                let mut row = Row::from_values(row_values.clone());
                prepare_insert_row_constraints(
                    &mut *table,
                    &resulting_schema,
                    &compiled_table_checks,
                    &mut row,
                    &mut vm,
                )?;
                let row = if let Some(plan) = triggers {
                    let Some(row) =
                        self.mutation_fire_before_row_triggers(plan, None, Some(row), None, ctx)?
                    else {
                        continue;
                    };
                    validate_resulting_row_constraints(
                        &resulting_schema,
                        &compiled_table_checks,
                        &row,
                        &mut vm,
                    )?;
                    row
                } else {
                    row
                };
                row_values = row.as_slice().to_vec();

                // FK parent validation (zero-cost if no FKs)
                if let Some(ref fks) = fk_schema {
                    crate::mutation::foreign_key::check_parent_exists(
                        self.mutation_engine(),
                        table.txn_id(),
                        fks,
                        &row,
                    )?;
                }

                if stmt.do_nothing {
                    // ON CONFLICT DO NOTHING — silently skip duplicates
                    let insert_result = insert_row_for_command_result(
                        &mut *table,
                        row,
                        needs_inserted_row,
                        auto_increment_pk_idx,
                        &mut last_insert_id,
                    );
                    match insert_result {
                        Ok(opt_row) => {
                            self.accept_inserted_row(
                                triggers,
                                opt_row,
                                has_returning,
                                &mut returning_rows,
                                ctx,
                            )?;
                            rows_affected += 1;
                        }
                        Err(ref e @ Error::PrimaryKeyConstraint { .. })
                        | Err(ref e @ Error::UniqueConstraint { .. }) => {
                            if !conflict_matches_target(&stmt.conflict_target, &schema, e) {
                                return Err(e.clone());
                            }
                            // DO NOTHING: conflict skipped, no RETURNING row
                        }
                        Err(e) => return Err(e),
                    }
                } else {
                    let compiled_upsert = compiled_upsert.as_ref().ok_or_else(|| {
                        Error::internal("missing precompiled ON CONFLICT update plan")
                    })?;
                    // ON CONFLICT DO UPDATE / ON DUPLICATE KEY UPDATE
                    let insert_result = insert_row_for_command_result(
                        &mut *table,
                        row,
                        needs_inserted_row,
                        auto_increment_pk_idx,
                        &mut last_insert_id,
                    );
                    match insert_result {
                        Ok(opt_row) => {
                            self.accept_inserted_row(
                                triggers,
                                opt_row,
                                has_returning,
                                &mut returning_rows,
                                ctx,
                            )?;
                            rows_affected += 1;
                        }
                        Err(ref e @ Error::PrimaryKeyConstraint { row_id }) => {
                            if !conflict_matches_target(&stmt.conflict_target, &schema, e) {
                                return Err(Error::PrimaryKeyConstraint { row_id });
                            }
                            match apply_on_duplicate_update(
                                self,
                                &mut table,
                                &schema,
                                row_id,
                                None,
                                &row_values,
                                compiled_upsert,
                                ctx,
                                has_returning,
                            ) {
                                Ok(Some(updated_row)) => {
                                    returning_rows.push(updated_row);
                                    rows_affected += 1;
                                }
                                Ok(None) => {
                                    rows_affected += 1;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Err(
                            ref e @ Error::UniqueConstraint {
                                ref index,
                                ref column,
                                ref value,
                                row_id: conflict_rid,
                            },
                        ) => {
                            if !conflict_matches_target(&stmt.conflict_target, &schema, e) {
                                return Err(Error::UniqueConstraint {
                                    index: index.clone(),
                                    column: column.clone(),
                                    value: value.clone(),
                                    row_id: conflict_rid,
                                });
                            }
                            let found_row_id = if conflict_rid >= 0 {
                                Ok(Some(conflict_rid))
                            } else {
                                find_row_by_unique_index(
                                    &*table,
                                    &schema,
                                    index,
                                    column,
                                    &row_values,
                                )
                            };
                            match found_row_id {
                                Ok(Some(row_id)) => {
                                    match apply_on_duplicate_update(
                                        self,
                                        &mut table,
                                        &schema,
                                        row_id,
                                        Some(column),
                                        &row_values,
                                        compiled_upsert,
                                        ctx,
                                        has_returning,
                                    ) {
                                        Ok(Some(updated_row)) => {
                                            returning_rows.push(updated_row);
                                            rows_affected += 1;
                                        }
                                        Ok(None) => {
                                            rows_affected += 1;
                                        }
                                        Err(e) => return Err(e),
                                    }
                                }
                                Ok(None) => {
                                    return Err(Error::UniqueConstraint {
                                        index: index.clone(),
                                        column: column.clone(),
                                        value: value.clone(),
                                        row_id: -1,
                                    });
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        } else {
            // Fast path: normal INSERT without clones
            for value_row in &stmt.values {
                if value_row.len() != column_indices.len() {
                    return Err(Error::InvalidArgument(format!(
                        "INSERT has {} columns but {} values",
                        column_indices.len(),
                        value_row.len()
                    )));
                }

                // Build row values - initialize with DEFAULT values for missing columns
                let mut row_values = Vec::with_capacity(schema_column_count);
                for i in 0..schema_column_count {
                    if let Some(ref default_expr) = default_exprs[i] {
                        // Evaluate the default expression using the actual column type
                        let default_type = all_column_types[i];
                        row_values.push(evaluate_default_expr(default_expr, default_type)?);
                    } else {
                        row_values.push(Value::null_unknown());
                    }
                }

                // Fill in provided values using pre-computed indices with type coercion
                for (i, expr) in value_row.iter().enumerate() {
                    // Handle DEFAULT keyword - skip this column to use pre-initialized default
                    if matches!(expr, Expression::Default(_)) {
                        continue;
                    }
                    // OPTIMIZATION: Try to extract literal value directly without VM compilation
                    // This avoids ~1-2μs per expression for simple literals (INTEGER, TEXT, etc.)
                    let value = if let Some(lit_value) = try_extract_literal(expr) {
                        lit_value
                    } else {
                        // Fall back to VM for complex expressions (Parameters, functions, etc.)
                        let program = compile_expression(expr, &[])?;
                        vm.execute_cow(&program, &base_exec_ctx)?
                    };
                    // Coerce to target type
                    let coerced = value.coerce_to_type(column_types[i]);
                    // Validate coercion didn't silently fail
                    validate_coercion(
                        &value,
                        &coerced,
                        &column_names[i],
                        column_types[i],
                        column_vector_dims[i],
                    )?;
                    row_values[column_indices[i]] = coerced;
                }

                // Insert row
                let mut row = Row::from_values(row_values);
                prepare_insert_row_constraints(
                    &mut *table,
                    &resulting_schema,
                    &compiled_table_checks,
                    &mut row,
                    &mut vm,
                )?;
                let row = if let Some(plan) = triggers {
                    let Some(row) =
                        self.mutation_fire_before_row_triggers(plan, None, Some(row), None, ctx)?
                    else {
                        continue;
                    };
                    validate_resulting_row_constraints(
                        &resulting_schema,
                        &compiled_table_checks,
                        &row,
                        &mut vm,
                    )?;
                    row
                } else {
                    row
                };

                // FK parent validation (zero-cost if no FKs)
                if let Some(ref fks) = fk_schema {
                    crate::mutation::foreign_key::check_parent_exists(
                        self.mutation_engine(),
                        table.txn_id(),
                        fks,
                        &row,
                    )?;
                }

                let inserted_row = insert_row_for_command_result(
                    &mut *table,
                    row,
                    needs_inserted_row,
                    auto_increment_pk_idx,
                    &mut last_insert_id,
                )?;
                self.accept_inserted_row(
                    triggers,
                    inserted_row,
                    has_returning,
                    &mut returning_rows,
                    ctx,
                )?;
                rows_affected += 1;
            }
        }

        // Invalidate semantic cache for this table BEFORE commit
        // CRITICAL: Must invalidate before commit to prevent stale data window
        if rows_affected > 0 {
            self.mutation_invalidate_semantic_cache(table_name);
            invalidate_semi_join_cache_for_table(table_name);
            invalidate_scalar_subquery_cache_for_table(table_name);
            invalidate_in_subquery_cache_for_table(table_name);
        }

        let mut returning_result = if has_returning {
            Some(build_returning_result(
                &stmt.returning,
                std::mem::take(&mut returning_rows),
                schema_column_names_arc.as_ref().unwrap(),
                ctx,
            )?)
        } else {
            None
        };

        // Commit if this is a standalone (auto-commit) transaction
        if should_auto_commit {
            if let Some(mut tx) = standalone_tx {
                match tx.commit() {
                    Ok(()) => {}
                    Err(e)
                        if (stmt.on_duplicate || stmt.do_nothing)
                            && e.is_pk_or_unique_violation() =>
                    {
                        if stmt.on_duplicate && ctx.query_depth() == 0 {
                            let retry_ctx = ctx.with_incremented_query_depth();
                            return self.execute_insert(stmt, &retry_ctx);
                        }
                        if stmt.do_nothing {
                            // DO NOTHING: returning 0 rows is the correct semantic
                            rows_affected = 0;
                            returning_result = Some(build_returning_result(
                                &stmt.returning,
                                Vec::new(),
                                schema_column_names_arc.as_ref().unwrap(),
                                ctx,
                            )?);
                        } else {
                            return Err(e);
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        // Handle RETURNING clause
        if let Some(result) = returning_result {
            return Ok(result);
        }

        Ok(Box::new(ExecResult::with_last_insert_id(
            rows_affected,
            last_insert_id,
        )))
    }

    /// Execute an INSERT statement with compiled cache support
    /// This variant uses the query cache to avoid recomputing schema-derived metadata
    /// on every INSERT execution, significantly reducing allocations for prepared statements.
    fn execute_insert_with_compiled_cache(
        &self,
        stmt: &InsertStatement,
        ctx: &ExecutionContext,
        compiled_cache: &Arc<RwLock<CompiledExecution>>,
    ) -> Result<Box<dyn QueryResult>> {
        let trigger_plan = self.mutation_prepare_dml_triggers(
            stmt.table_name.value_lower.as_str(),
            DmlTriggerEvent::Insert,
            &[],
            ctx,
        )?;
        if !trigger_plan.is_empty() {
            return self.execute_insert(stmt, ctx);
        }
        // Conflict handling requires special handling - fall back to non-cached path
        if stmt.on_duplicate || stmt.do_nothing {
            return self.execute_insert(stmt, ctx);
        }

        // OPTIMIZATION: Use pre-computed lowercase name to avoid allocation per query
        let table_name = &stmt.table_name.value_lower;

        // Check if there's an active explicit transaction
        let mut active_tx = self.mutation_active_transaction().lock().unwrap();

        let (mut table, should_auto_commit, standalone_tx) =
            if let Some(ref mut tx_state) = *active_tx {
                // Use the active transaction
                let table = tx_state.transaction.get_table(table_name)?;

                // Store a reference to this table for commit/rollback
                if !tx_state.tables.contains_key(table_name.as_str()) {
                    tx_state.tables.insert(
                        table_name.to_string(),
                        tx_state.transaction.get_table(table_name)?,
                    );
                }

                (table, false, None)
            } else {
                // No active transaction - create a standalone transaction with auto-commit
                let tx = self.mutation_engine().begin_transaction()?;
                let table = tx.get_table(table_name)?;
                (table, true, Some(tx))
            };

        // Drop the lock before doing work
        drop(active_tx);
        let resulting_schema = table.schema().clone();

        // Try to get cached compilation, or compile fresh if needed
        let current_epoch = self.mutation_engine().schema_epoch();
        let cached_insert = {
            let cache_read = compiled_cache.read().unwrap();
            if let CompiledExecution::Insert(ref cached) = *cache_read {
                if cached.cached_epoch == current_epoch && *cached.table_name == *table_name {
                    Some(cached.clone())
                } else {
                    None // Stale cache
                }
            } else {
                None
            }
        };

        // Use cached metadata or compile fresh
        let (
            column_indices,
            column_types,
            column_vector_dims,
            column_names,
            all_column_types,
            default_row_template,
        ) = if let Some(cached) = cached_insert {
            // Use cached values (Arc clone is cheap)
            (
                cached.column_indices,
                cached.column_types,
                cached.column_vector_dims,
                cached.column_names,
                cached.all_column_types,
                cached.default_row_template,
            )
        } else {
            // Compile and cache
            let schema = table.schema();
            let schema_column_count = schema.columns.len();

            let all_column_types: Vec<DataType> =
                schema.columns.iter().map(|c| c.data_type).collect();

            // DEFAULT expressions are executable statement plans, not cached
            // values. The template owns only the no-default NULL slots;
            // expressions are evaluated for each inserted row below.
            let default_row_template = vec![Value::null_unknown(); schema.columns.len()];

            let (column_indices, column_types, column_vector_dims, column_names) =
                if stmt.columns.is_empty() {
                    // No columns specified - insert into all columns in order
                    let indices: Vec<usize> = (0..schema_column_count).collect();
                    let types = all_column_types.clone();
                    let dims: Vec<u16> =
                        schema.columns.iter().map(|c| c.vector_dimensions).collect();
                    let names: Vec<SmartString> = schema
                        .columns
                        .iter()
                        .map(|c| SmartString::new(&c.name))
                        .collect();
                    (indices, types, dims, names)
                } else {
                    // Validate columns exist and pre-compute their indices
                    let col_map = schema.column_index_map();
                    let indices: Vec<usize> = stmt
                        .columns
                        .iter()
                        .map(|id| {
                            col_map
                                .get(id.value_lower.as_str())
                                .copied()
                                .ok_or_else(|| Error::ColumnNotFound(id.value.to_string()))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let types: Vec<DataType> = indices
                        .iter()
                        .map(|&idx| schema.columns[idx].data_type)
                        .collect();
                    let dims: Vec<u16> = indices
                        .iter()
                        .map(|&idx| schema.columns[idx].vector_dimensions)
                        .collect();
                    let names: Vec<SmartString> = indices
                        .iter()
                        .map(|&idx| SmartString::new(&schema.columns[idx].name))
                        .collect();
                    (indices, types, dims, names)
                };

            // Store in cache for next execution
            let compiled = CompiledInsert {
                table_name: SmartString::new(table_name),
                column_indices: Arc::new(column_indices.clone()),
                column_types: Arc::new(column_types.clone()),
                column_vector_dims: Arc::new(column_vector_dims.clone()),
                column_names: Arc::new(column_names.clone()),
                all_column_types: Arc::new(all_column_types.clone()),
                default_row_template: Arc::new(default_row_template.clone()),
                cached_epoch: current_epoch,
            };

            // Update the cache
            if let Ok(mut cache_write) = compiled_cache.write() {
                *cache_write = CompiledExecution::Insert(compiled);
            }

            (
                Arc::new(column_indices),
                Arc::new(column_types),
                Arc::new(column_vector_dims),
                Arc::new(column_names),
                Arc::new(all_column_types),
                Arc::new(default_row_template),
            )
        };

        let mut seen_insert_targets = rustc_hash::FxHashSet::default();
        for &column_index in column_indices.iter() {
            if !seen_insert_targets.insert(column_index) {
                return Err(Error::InvalidArgument(
                    "INSERT target column is specified more than once".to_string(),
                ));
            }
        }

        // Pre-compute FK info for parent validation (CompactArc ref-count bump, not deep clone)
        let fk_schema = if !table.schema().foreign_keys.is_empty() {
            Some(table.schema().clone())
        } else {
            None
        };
        let compiled_table_checks = compile_table_check_constraints(table.schema())?;
        let auto_increment_pk_idx = auto_increment_pk_index(table.schema());
        let default_exprs: Vec<Option<String>> = table
            .schema()
            .columns
            .iter()
            .map(|column| column.default_expr.clone())
            .collect();

        // Create VM for constant expression evaluation (reused for all INSERT values)
        use crate::expression::{compile_expression, ExecuteContext, ExprVM};
        let mut vm = ExprVM::new();
        let params = ctx.params();
        let named_params = ctx.named_params();
        let empty_row = Row::new();

        // OPTIMIZATION: Pre-build ExecuteContext once (reused for all expressions)
        let mut base_exec_ctx = ExecuteContext::new(&empty_row);
        if !params.is_empty() {
            base_exec_ctx = base_exec_ctx.with_params(params);
        }
        if !named_params.is_empty() {
            base_exec_ctx = base_exec_ctx.with_named_params(named_params);
        }
        base_exec_ctx = base_exec_ctx
            .with_transaction_id(ctx.transaction_id())
            .with_stored_function_invoker(ctx.stored_function_invoker());

        let mut rows_affected = 0i64;
        let mut last_insert_id = 0i64;

        // RETURNING clause support - collect inserted rows if RETURNING is specified
        let has_returning = !stmt.returning.is_empty();
        let mut returning_rows: Vec<Row> = Vec::new();
        let schema_column_names_arc = if has_returning {
            Some(table.schema().column_names_arc())
        } else {
            None
        };

        // Build lookup from schema column index to value position
        // This allows building row_values directly without cloning entire template
        let col_to_value_pos: Vec<Option<usize>> = {
            let mut lookup = vec![None; default_row_template.len()];
            for (value_pos, &col_idx) in column_indices.iter().enumerate() {
                lookup[col_idx] = Some(value_pos);
            }
            lookup
        };
        let num_columns = default_row_template.len();

        // Check if this is INSERT ... SELECT
        if let Some(ref select_stmt) = stmt.select {
            let mut select_result = self.mutation_execute_select(select_stmt, ctx)?;
            if !should_auto_commit {
                // Explicit tx: fully materialize before writes for atomicity
                let columns = select_result.columns().to_vec();
                let rows = <Self as MutationHost>::mutation_materialize_result(select_result)?;
                select_result = Box::new(crate::result::ExecutorResult::new(columns, rows));
            }

            // Process each row from the SELECT result
            while select_result.next() {
                let select_row = select_result.row();
                if select_row.len() != column_indices.len() {
                    return Err(Error::InvalidArgument(format!(
                        "INSERT has {} columns but SELECT returns {} columns",
                        column_indices.len(),
                        select_row.len()
                    )));
                }

                // OPTIMIZATION: Build row_values directly without cloning entire template
                // Only clone defaults for columns NOT in the insert list
                let mut row_values = Vec::with_capacity(num_columns);
                for (col_idx, default_val) in default_row_template.iter().enumerate() {
                    if let Some(value_pos) = col_to_value_pos[col_idx] {
                        // Column in insert list - use value from SELECT row
                        let value = &select_row[value_pos];
                        let coerced = value.coerce_to_type(column_types[value_pos]);
                        validate_coercion(
                            value,
                            &coerced,
                            &column_names[value_pos],
                            column_types[value_pos],
                            column_vector_dims[value_pos],
                        )?;
                        row_values.push(coerced);
                    } else if let Some(expression) = &default_exprs[col_idx] {
                        row_values.push(evaluate_default_expr(
                            expression,
                            all_column_types[col_idx],
                        )?);
                    } else {
                        row_values.push(default_val.clone());
                    }
                }

                // Insert row
                let mut row = Row::from_values(row_values);
                prepare_insert_row_constraints(
                    &mut *table,
                    &resulting_schema,
                    &compiled_table_checks,
                    &mut row,
                    &mut vm,
                )?;

                // FK parent validation (zero-cost if no FKs)
                if let Some(ref fks) = fk_schema {
                    crate::mutation::foreign_key::check_parent_exists(
                        self.mutation_engine(),
                        table.txn_id(),
                        fks,
                        &row,
                    )?;
                }

                if let Some(inserted_row) = insert_row_for_command_result(
                    &mut *table,
                    row,
                    has_returning,
                    auto_increment_pk_idx,
                    &mut last_insert_id,
                )? {
                    returning_rows.push(inserted_row);
                }
                rows_affected += 1;
            }
            if let Some(err) = select_result.last_error() {
                return Err(err);
            }
        } else {
            // Regular INSERT with VALUES
            for value_list in &stmt.values {
                if value_list.len() != column_indices.len() {
                    return Err(Error::InvalidArgument(format!(
                        "INSERT has {} columns but {} values provided",
                        column_indices.len(),
                        value_list.len()
                    )));
                }

                // OPTIMIZATION: Build row_values directly without cloning entire template
                // Only clone defaults for columns NOT in the insert list
                let mut row_values = Vec::with_capacity(num_columns);
                for (col_idx, default_val) in default_row_template.iter().enumerate() {
                    if let Some(value_pos) = col_to_value_pos[col_idx] {
                        // Column in insert list - evaluate expression
                        let expr = &value_list[value_pos];
                        if matches!(expr, Expression::Default(_)) {
                            if let Some(expression) = &default_exprs[col_idx] {
                                row_values.push(evaluate_default_expr(
                                    expression,
                                    all_column_types[col_idx],
                                )?);
                            } else {
                                row_values.push(default_val.clone());
                            }
                        } else {
                            // OPTIMIZATION: Try literal extraction first (avoids VM compilation)
                            let value = if let Some(lit_val) = try_extract_literal(expr) {
                                lit_val
                            } else {
                                // Fall back to VM evaluation for complex expressions
                                let program = compile_expression(expr, &[])?;
                                vm.execute_cow(&program, &base_exec_ctx)?
                            };

                            let target_type = column_types[value_pos];
                            let coerced = value.coerce_to_type(target_type);
                            validate_coercion(
                                &value,
                                &coerced,
                                &column_names[value_pos],
                                target_type,
                                column_vector_dims[value_pos],
                            )?;
                            row_values.push(coerced);
                        }
                    } else if let Some(expression) = &default_exprs[col_idx] {
                        row_values.push(evaluate_default_expr(
                            expression,
                            all_column_types[col_idx],
                        )?);
                    } else {
                        row_values.push(default_val.clone());
                    }
                }

                // Insert row
                let mut row = Row::from_values(row_values);
                prepare_insert_row_constraints(
                    &mut *table,
                    &resulting_schema,
                    &compiled_table_checks,
                    &mut row,
                    &mut vm,
                )?;

                // FK parent validation (zero-cost if no FKs)
                if let Some(ref fks) = fk_schema {
                    crate::mutation::foreign_key::check_parent_exists(
                        self.mutation_engine(),
                        table.txn_id(),
                        fks,
                        &row,
                    )?;
                }

                if let Some(inserted_row) = insert_row_for_command_result(
                    &mut *table,
                    row,
                    has_returning,
                    auto_increment_pk_idx,
                    &mut last_insert_id,
                )? {
                    returning_rows.push(inserted_row);
                }
                rows_affected += 1;
            }
        }

        // CRITICAL: Must invalidate before commit to prevent stale data window
        if rows_affected > 0 {
            self.mutation_invalidate_semantic_cache(table_name);
            invalidate_semi_join_cache_for_table(table_name);
            invalidate_scalar_subquery_cache_for_table(table_name);
            invalidate_in_subquery_cache_for_table(table_name);
        }

        let returning_result = if has_returning {
            Some(build_returning_result(
                &stmt.returning,
                returning_rows,
                schema_column_names_arc.as_ref().unwrap(),
                ctx,
            )?)
        } else {
            None
        };

        // Commit if this is a standalone (auto-commit) transaction
        if should_auto_commit {
            if let Some(mut tx) = standalone_tx {
                tx.commit()?;
            }
        }

        // Handle RETURNING clause
        if let Some(result) = returning_result {
            return Ok(result);
        }

        Ok(Box::new(ExecResult::with_last_insert_id(
            rows_affected,
            last_insert_id,
        )))
    }

    /// Execute an UPDATE statement
    fn execute_update(
        &self,
        stmt: &UpdateStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let updated_columns = stmt
            .updates
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let plan = self.mutation_prepare_dml_triggers(
            stmt.table_name.value_lower.as_str(),
            DmlTriggerEvent::Update,
            &updated_columns,
            ctx,
        )?;
        if plan.is_empty() {
            return self.execute_update_body(stmt, ctx, None);
        }
        self.execute_with_trigger_statement(&plan, ctx, |plan| {
            self.execute_update_body(stmt, ctx, Some(plan))
        })
    }

    fn execute_update_body(
        &self,
        stmt: &UpdateStatement,
        ctx: &ExecutionContext,
        triggers: Option<&DmlTriggerPlan>,
    ) -> Result<Box<dyn QueryResult>> {
        // OPTIMIZATION: Use pre-computed lowercase name to avoid allocation per query
        let table_name = &stmt.table_name.value_lower;

        // Check if there's an active explicit transaction
        let mut active_tx = self.mutation_active_transaction().lock().unwrap();

        let (mut table, should_auto_commit, standalone_tx) =
            if let Some(ref mut tx_state) = *active_tx {
                // Use the active transaction
                // NOTE: table_name is already lowercase (value_lower from AST)
                let table = tx_state.transaction.get_table(table_name)?;

                // Store a reference to this table for commit/rollback
                if !tx_state.tables.contains_key(table_name.as_str()) {
                    tx_state.tables.insert(
                        table_name.to_string(),
                        tx_state.transaction.get_table(table_name)?,
                    );
                }

                (table, false, None)
            } else {
                // No active transaction - create a standalone transaction with auto-commit
                let tx = self.mutation_engine().begin_transaction()?;
                let table = tx.get_table(table_name)?;
                (table, true, Some(tx))
            };

        // Drop the lock before doing work
        drop(active_tx);

        // Check for RETURNING clause
        let has_returning = !stmt.returning.is_empty();

        // Pre-compute column names and indices to avoid schema borrow conflicts
        let schema = table.schema();
        let constraint_schema = schema.clone();
        let compiled_table_checks = compile_table_check_constraints(schema)?;
        // OPTIMIZATION: Use CompactArc<Vec<String>> to share column names without cloning
        let column_names = schema.column_names_arc();
        let col_map = schema.column_index_map();
        for (column, expression) in &stmt.updates {
            if !col_map.contains_key(column.to_lowercase().as_str()) {
                return Err(Error::ColumnNotFound(column.to_string()));
            }
            if !<Self as MutationHost>::mutation_has_subqueries(expression) {
                crate::expression::compile_expression(expression, &column_names)?;
            }
        }

        // Pre-compute FK info for UPDATE validation
        // Determine which FK columns are being updated (for parent validation)
        let fk_cols_in_update: Vec<(usize, radixdb_core::ForeignKeyConstraint)> = {
            let col_map = schema.column_index_map();
            let updated_col_indices: Vec<usize> = stmt
                .updates
                .keys()
                .filter_map(|col_name| {
                    let col_lower = col_name.to_lowercase();
                    col_map.get(col_lower.as_str()).copied()
                })
                .collect();
            schema
                .foreign_keys
                .iter()
                .filter(|fk| updated_col_indices.contains(&fk.column_index))
                .map(|fk| (fk.column_index, fk.clone()))
                .collect()
        };
        let has_fk_updates = !fk_cols_in_update.is_empty();

        // Reject UPDATE on primary key columns. The engine assumes row_id == pk_value
        // throughout ~50 code paths (lookups, range scans, ORDER BY, FK cascade, WAL
        // recovery, etc.). Allowing PK mutation silently corrupts lookups.
        // This matches SQLite's behavior for rowid tables.
        if let Some(pk_idx) = schema.pk_column_index() {
            let col_map = schema.column_index_map();
            for col_name in stmt.updates.keys() {
                let col_lower = col_name.to_lowercase();
                if col_map.get(col_lower.as_str()).copied() == Some(pk_idx) {
                    let pk_col_name = &schema.columns[pk_idx].name;
                    return Err(radixdb_core::Error::invalid_argument(format!(
                        "cannot UPDATE primary key column '{}'. Use DELETE + INSERT instead",
                        pk_col_name
                    )));
                }
            }
        }

        // Check if this table is referenced by child tables via columns being updated.
        // This handles CASCADE/RESTRICT/SET NULL for UNIQUE columns referenced by child FKs.
        let all_referencing_fks = crate::mutation::foreign_key::find_referencing_fks_for_txn(
            self.mutation_engine(),
            table.txn_id(),
            table_name,
        );
        let referencing_fks_for_update: Arc<Vec<(String, radixdb_core::ForeignKeyConstraint)>> =
            if all_referencing_fks.is_empty() {
                Arc::new(Vec::new())
            } else {
                let col_map = schema.column_index_map();
                let updated_cols: Vec<usize> = stmt
                    .updates
                    .iter()
                    .filter_map(|(column, expression)| {
                        if Self::assignment_preserves_column(expression, column, table_name) {
                            None
                        } else {
                            col_map.get(column.to_lowercase().as_str()).copied()
                        }
                    })
                    .collect();
                let relevant: Vec<_> = all_referencing_fks
                    .iter()
                    .filter(|(_, fk)| {
                        col_map
                            .get(fk.referenced_column.to_lowercase().as_str())
                            .is_some_and(|&idx| updated_cols.contains(&idx))
                    })
                    .cloned()
                    .collect();
                Arc::new(relevant)
            };

        // Get FK schema via engine (CompactArc ref-count bump, no deep clone)
        let fk_update_schema = if has_fk_updates {
            Some(self.mutation_engine().get_table_schema(table_name)?)
        } else {
            None
        };

        // Pre-validate constant FK values in explicit transactions to prevent dirty state.
        // When SET parent_id = <literal>, we can check the parent exists BEFORE modifying rows.
        // This ensures statement-level atomicity for the most common FK update pattern.
        // For row-dependent expressions (SET fk = other_col), post-validation is still used.
        if has_fk_updates && !should_auto_commit {
            let col_map = schema.column_index_map();
            for (col_name, expr) in &stmt.updates {
                let col_lower = col_name.to_lowercase();
                if let Some(&col_idx) = col_map.get(col_lower.as_str()) {
                    if let Some(fk) = schema
                        .foreign_keys
                        .iter()
                        .find(|f| f.column_index == col_idx)
                    {
                        if let Some(value) = Self::try_extract_constant_fk_value(expr, ctx) {
                            if !value.is_null() {
                                crate::mutation::foreign_key::validate_fk_value(
                                    self.mutation_engine(),
                                    table.txn_id(),
                                    fk,
                                    &value,
                                    table_name,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        // Check if any update expressions contain subqueries
        let has_update_subqueries = stmt
            .updates
            .iter()
            .any(|(_, expr)| <Self as MutationHost>::mutation_has_subqueries(expr));

        // Check if any update expressions have correlated subqueries
        let has_correlated_updates = stmt.updates.iter().any(|(_, expr)| {
            <Self as MutationHost>::mutation_has_subqueries(expr)
                && <Self as MutationHost>::mutation_has_correlated_subqueries(expr)
        });

        // Pre-process update expressions if they contain NON-correlated subqueries
        // Correlated subqueries must be processed per-row with outer row context
        let processed_updates: Option<Vec<(String, Expression)>> =
            if has_update_subqueries && !has_correlated_updates {
                let processed: Result<Vec<_>> = stmt
                    .updates
                    .iter()
                    .map(|(col_name, expr)| {
                        let processed_expr = self.mutation_process_where_subqueries(expr, ctx)?;
                        Ok((col_name.to_string(), processed_expr))
                    })
                    .collect();
                Some(processed?)
            } else {
                None
            };

        // Row triggers must run outside the storage setter so nested SQL can
        // use the statement transaction without re-entering table internals.
        let requires_row_staging = has_correlated_updates || triggers.is_some();

        // Pre-compute column indices for correlated updates path only
        // For non-correlated path, we compile directly from source expressions
        // This avoids cloning Expression objects when they're not needed
        let update_indices: Vec<(usize, radixdb_core::DataType, u16, Expression, bool)> =
            if requires_row_staging {
                // Clone expressions only for the staged path. Non-correlated
                // subqueries have already been replaced with scalar values.
                {
                    let col_map = schema.column_index_map();
                    let staged_updates = processed_updates.clone().unwrap_or_else(|| {
                        stmt.updates
                            .iter()
                            .map(|(column, expression)| (column.to_string(), expression.clone()))
                            .collect()
                    });
                    staged_updates
                        .iter()
                        .filter_map(|(col_name, expr)| {
                            let is_correlated =
                                <Self as MutationHost>::mutation_has_subqueries(expr)
                                    && <Self as MutationHost>::mutation_has_correlated_subqueries(
                                        expr,
                                    );
                            let col_lower = col_name.to_lowercase();
                            col_map.get(&col_lower).map(|&idx| {
                                (
                                    idx,
                                    schema.columns[idx].data_type,
                                    schema.columns[idx].vector_dimensions,
                                    expr.clone(),
                                    is_correlated,
                                )
                            })
                        })
                        .collect()
                }
            } else {
                // Non-correlated path: empty vec, we compile directly from source later
                Vec::new()
            };

        // Build WHERE expression for storage layer
        // Try to convert to storage expression, fall back to in-memory filtering if not possible
        //
        // OPTIMIZATION: For correlated EXISTS/IN in WHERE, try semi-join optimization first.
        // This transforms O(outer × inner) per-row subquery execution to O(inner + outer).
        let (where_expr, needs_memory_filter, memory_where_clause): (
            Option<Box<dyn StorageExpr>>,
            bool,
            Option<Expression>,
        ) = if let Some(ref where_clause) = stmt.where_clause {
            let has_correlated_where =
                <Self as MutationHost>::mutation_has_subqueries(where_clause)
                    && <Self as MutationHost>::mutation_has_correlated_subqueries(where_clause);

            let processed_where = if has_correlated_where {
                // Try semi-join optimization for correlated EXISTS/IN
                // Avoid cloning upfront - only clone if no optimization succeeds
                let outer_tables = vec![table_name.to_string()];

                // Try EXISTS semi-join optimization
                let exists_optimized = self
                    .mutation_optimize_exists_to_semi_join(where_clause, ctx, &outer_tables, None)
                    .ok()
                    .flatten();

                // Try IN semi-join optimization (on EXISTS result or original)
                let expr_for_in = exists_optimized.as_ref().unwrap_or(where_clause.as_ref());
                let in_optimized = self
                    .mutation_optimize_in_to_semi_join(expr_for_in, ctx, &outer_tables)
                    .ok()
                    .flatten();

                // Determine final expression without unnecessary clones
                let current_expr = in_optimized
                    .or(exists_optimized)
                    .unwrap_or_else(|| (**where_clause).clone());

                // Process any remaining non-correlated subqueries
                if <Self as MutationHost>::mutation_has_subqueries(&current_expr) {
                    self.mutation_process_where_subqueries(&current_expr, ctx)?
                } else {
                    current_expr
                }
            } else if <Self as MutationHost>::mutation_has_subqueries(where_clause) {
                self.mutation_process_where_subqueries(where_clause, ctx)?
            } else {
                (**where_clause).clone()
            };

            // Try to push down predicate to storage layer
            let plan = pushdown::try_pushdown_plan(&processed_where, schema, Some(ctx));
            let needs_mem = plan.needs_memory_filter();
            (plan.storage_expr, needs_mem, plan.residual)
        } else {
            (None, false, None)
        };

        let function_registry = self.mutation_function_registry();

        // Create evaluator once and reuse for all rows (optimization)
        let mut evaluator = CompiledEvaluator::new(function_registry).with_context(ctx);
        evaluator.init_columns_arc(CompactArc::clone(&column_names));

        // Use RefCell to collect updated rows for RETURNING clause and FK validation
        use std::cell::RefCell;
        let returning_rows: RefCell<Vec<Row>> = RefCell::new(Vec::new());
        // Collect new FK values and referenced-column old/new pairs from setter
        let fk_new_values: RefCell<Vec<Row>> = RefCell::new(Vec::new());
        // (referenced_col_idx, old_value, new_value) for FK cascade enforcement
        let ref_col_changes: RefCell<Vec<(usize, Value, Value)>> = RefCell::new(Vec::new());

        // Pre-compute referenced column indices for FK cascade enforcement.
        // Shared by both correlated and non-correlated update paths.
        let ref_col_indices_for_fk: Vec<usize> = if !referencing_fks_for_update.is_empty() {
            let col_map = schema.column_index_map();
            referencing_fks_for_update
                .iter()
                .filter_map(|(_, fk)| {
                    col_map
                        .get(fk.referenced_column.to_lowercase().as_str())
                        .copied()
                })
                .collect::<rustc_hash::FxHashSet<usize>>()
                .into_iter()
                .collect()
        } else {
            Vec::new()
        };

        // Pre-partition FKs by referenced column index for post-update dispatch.
        // This avoids re-borrowing schema after table.update().
        let fks_by_ref_col: FxHashMap<usize, Vec<(String, radixdb_core::ForeignKeyConstraint)>> =
            if !referencing_fks_for_update.is_empty() {
                let col_map = schema.column_index_map();
                let mut map: FxHashMap<usize, Vec<(String, radixdb_core::ForeignKeyConstraint)>> =
                    FxHashMap::default();
                for (tbl, fk) in referencing_fks_for_update.iter() {
                    if let Some(&idx) = col_map.get(fk.referenced_column.to_lowercase().as_str()) {
                        map.entry(idx).or_default().push((tbl.clone(), fk.clone()));
                    }
                }
                map
            } else {
                FxHashMap::default()
            };

        // Pre-check RESTRICT constraints and CASCADE depth BEFORE writing parent rows.
        // Only scan rows if the FK tree actually has RESTRICT or exceeds depth limits.
        // Pure CASCADE/SET NULL trees within depth limits skip this scan entirely.
        if !fks_by_ref_col.is_empty() {
            let any_needs_precheck = fks_by_ref_col.values().any(|fks| {
                crate::mutation::foreign_key::fk_tree_needs_precheck(
                    self.mutation_engine(),
                    table.txn_id(),
                    fks,
                )
            });
            if any_needs_precheck {
                let parent_rows = table.collect_all_rows(where_expr.as_deref())?;
                for (_rid, row) in parent_rows.iter() {
                    if needs_memory_filter {
                        if let Some(ref mem_where) = memory_where_clause {
                            evaluator.set_row_array(row);
                            match evaluator.evaluate_bool(mem_where) {
                                Ok(true) => {}
                                _ => continue,
                            }
                        }
                    }
                    for (&col_idx, fks_for_col) in &fks_by_ref_col {
                        if let Some(old_val) = row.get(col_idx) {
                            if !old_val.is_null() {
                                crate::mutation::foreign_key::pre_check_restrict_for_update(
                                    self.mutation_engine(),
                                    table.txn_id(),
                                    table_name,
                                    old_val,
                                    fks_for_col,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        // Create a setter function that applies updates using pre-computed indices
        // If we need memory filtering, include the WHERE check in the setter
        // For correlated subqueries, we need special handling
        let rows_affected = if requires_row_staging {
            // Path for correlated subqueries: we need to pre-compute all values
            // because process_correlated_expression calls self methods and can't be
            // used inside the closure. Strategy:
            // 1. Scan table to find all rows (matching WHERE if applicable)
            // 2. For each row, build outer_row context and evaluate correlated expressions
            // 3. Store computed values keyed by PK
            // 4. Call table.update with a setter that looks up pre-computed values

            // Preserve physical row identity even for no-PK/composite-key rows.
            type StagedUpdate = (i64, Row, Vec<(usize, Value)>);
            let mut precomputed: Vec<StagedUpdate> = Vec::new();

            // Build column indices for scanning (all columns)
            let all_col_indices: Vec<usize> = (0..column_names.len()).collect();

            // OPTIMIZATION: Use schema's cached lowercase column names instead of computing
            // Use CompactArc<str> for zero-cost cloning in the per-row loop
            let column_names_lower = schema.column_names_lower_arc();
            let col_name_pairs: Vec<(CompactArc<str>, CompactArc<str>)> = column_names_lower
                .iter()
                .map(|col_lower| {
                    let qualified =
                        CompactArc::from(format!("{}.{}", table_name, col_lower).as_str());
                    (CompactArc::from(col_lower.as_str()), qualified)
                })
                .collect();

            // Reusable outer_row_map - cleared and reused each iteration
            // Uses CompactArc<str> keys for zero-cost cloning
            let mut outer_row_map: FxHashMap<CompactArc<str>, Value> =
                FxHashMap::with_capacity_and_hasher(col_name_pairs.len() * 2, Default::default());

            // Apply storage pushdown before evaluating correlated/volatile
            // assignments. Residual-only predicates are checked below.
            let mut scanner = table.scan(&all_col_indices, where_expr.as_deref())?;
            while scanner.next() {
                let row = scanner.row();
                let row_id = scanner.current_row_id()?;

                // Check WHERE condition if needed
                evaluator.set_row_array(row);
                if needs_memory_filter {
                    if let Some(ref where_clause) = memory_where_clause {
                        match evaluator.evaluate_bool(where_clause) {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(error) => return Err(error),
                        }
                    }
                }

                // Build outer row context from current row values using pre-computed names
                outer_row_map.clear();
                for (i, (col_lower, qualified)) in col_name_pairs.iter().enumerate() {
                    if let Some(value) = row.get(i) {
                        outer_row_map.insert(col_lower.clone(), value.clone());
                        outer_row_map.insert(qualified.clone(), value.clone());
                    }
                }

                // Create context with outer row for correlated subquery evaluation
                // Move map into context, we'll take it back after
                let mut correlated_ctx = ctx.with_outer_row(
                    std::mem::take(&mut outer_row_map),
                    CompactArc::clone(&column_names),
                );

                // Evaluate all update expressions
                let mut new_values: Vec<(usize, Value)> = Vec::with_capacity(update_indices.len());
                for (idx, col_type, vec_dims, expr, is_correlated) in update_indices.iter() {
                    let evaluated = if *is_correlated {
                        // Process correlated expression - this executes the subquery
                        let processed_expr =
                            self.mutation_process_correlated_expression(expr, &correlated_ctx)?;
                        // Now evaluate the processed expression (subquery replaced with value)
                        let mut eval =
                            CompiledEvaluator::new(function_registry).with_context(&correlated_ctx);
                        eval.init_columns_arc(CompactArc::clone(&column_names));
                        eval.set_row_array(row);
                        Some(eval.evaluate(&processed_expr)?)
                    } else {
                        Some(evaluator.evaluate(expr)?)
                    };

                    if let Some(new_value) = evaluated {
                        let coerced = new_value.coerce_to_type(*col_type);
                        validate_coercion(
                            &new_value,
                            &coerced,
                            &schema.columns[*idx].name,
                            *col_type,
                            *vec_dims,
                        )?;
                        new_values.push((*idx, coerced));
                    }
                }

                // Take back the map for reuse (zero-copy transfer)
                outer_row_map = correlated_ctx.take_outer_row().unwrap_or_default();

                if !new_values.is_empty() {
                    precomputed.push((row_id, row.clone(), new_values));
                }
            }
            drop(scanner);

            // Update each proven candidate by its internal row identity.
            let mut table_check_vm = crate::expression::ExprVM::new();
            let mut updated = 0i32;
            for (row_id, old_row, updates) in precomputed {
                let mut new_row = old_row.clone();
                for (idx, new_value) in &updates {
                    let _ = new_row.set(*idx, new_value.clone());
                }
                if let Some(plan) = triggers {
                    let Some(trigger_row) = self.mutation_fire_before_row_triggers(
                        plan,
                        Some(&old_row),
                        Some(new_row),
                        Some(row_id),
                        ctx,
                    )?
                    else {
                        continue;
                    };
                    new_row = trigger_row;
                }

                validate_resulting_row_constraints(
                    &constraint_schema,
                    &compiled_table_checks,
                    &new_row,
                    &mut table_check_vm,
                )?;

                for &column_index in &ref_col_indices_for_fk {
                    let old_value = old_row.get(column_index);
                    let new_value = new_row.get(column_index);
                    if let (Some(old_value), Some(new_value)) = (old_value, new_value) {
                        if old_value != new_value {
                            ref_col_changes.borrow_mut().push((
                                column_index,
                                old_value.clone(),
                                new_value.clone(),
                            ));
                        }
                    }
                }
                if has_fk_updates {
                    fk_new_values.borrow_mut().push(new_row.clone());
                }

                let final_row = new_row.clone();
                let mut setter =
                    |_row: Row| -> Result<(Row, bool)> { Ok((final_row.clone(), true)) };
                let row_count = table.update_by_row_ids(&[row_id], &mut setter)?;
                if row_count > 0 {
                    if has_returning {
                        returning_rows.borrow_mut().push(new_row.clone());
                    }
                    if let Some(plan) = triggers {
                        self.mutation_fire_after_row_triggers(
                            plan,
                            Some(&old_row),
                            Some(&new_row),
                            Some(row_id),
                            ctx,
                        )?;
                    }
                }
                updated += row_count;
            }
            updated
        } else {
            // Optimized path for non-correlated subqueries
            // CRITICAL: Pre-compile update expressions ONCE before the loop
            // Compile directly from source expressions (no intermediate cloning!)
            use crate::expression::{compile_expression, ExecuteContext, ExprVM, SharedProgram};

            let col_map = schema.column_index_map();
            let compiled_updates: Vec<(usize, String, radixdb_core::DataType, u16, SharedProgram)> =
                if let Some(ref processed) = processed_updates {
                    // Use pre-processed expressions (subqueries already evaluated)
                    processed
                        .iter()
                        .map(|(col_name, expr)| {
                            let col_lower = col_name.to_lowercase();
                            let &idx = col_map
                                .get(&col_lower)
                                .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                            let program = compile_expression(expr, &column_names)?;
                            Ok((
                                idx,
                                schema.columns[idx].name.clone(),
                                schema.columns[idx].data_type,
                                schema.columns[idx].vector_dimensions,
                                program,
                            ))
                        })
                        .collect::<Result<_>>()?
                } else {
                    // Compile directly from statement expressions
                    stmt.updates
                        .iter()
                        .map(|(col_name, expr)| {
                            let col_lower: String = col_name.to_lowercase().into();
                            let &idx = col_map
                                .get(&col_lower)
                                .ok_or_else(|| Error::ColumnNotFound(col_name.to_string()))?;
                            let program = compile_expression(expr, &column_names)?;
                            Ok((
                                idx,
                                schema.columns[idx].name.clone(),
                                schema.columns[idx].data_type,
                                schema.columns[idx].vector_dimensions,
                                program,
                            ))
                        })
                        .collect::<Result<_>>()?
                };

            if compiled_updates.len() != stmt.updates.len() {
                return Err(Error::internal(
                    "UPDATE compiler did not produce one program per assignment",
                ));
            }

            // Create VM once and reuse for all rows
            let mut vm = ExprVM::new();
            // Extract params before the closure so they can be captured
            let params = ctx.params();
            let named_params = ctx.named_params();
            let mut setter = |mut row: Row| -> Result<(Row, bool)> {
                // If we need in-memory WHERE filtering, check the condition first
                if needs_memory_filter {
                    evaluator.set_row_array(&row);
                    if let Some(ref where_expr) = memory_where_clause {
                        match evaluator.evaluate_bool(where_expr) {
                            Ok(true) => {}
                            Ok(false) => return Ok((row, false)),
                            Err(error) => return Err(error),
                        }
                    }
                }

                // Execute pre-compiled programs (no recompilation per row)
                let updates_to_apply: Vec<(usize, Value)> = {
                    let exec_ctx = ExecuteContext::new(&row)
                        .with_params(params)
                        .with_named_params(named_params)
                        .with_transaction_id(ctx.transaction_id())
                        .with_stored_function_invoker(ctx.stored_function_invoker());

                    let mut updates = Vec::with_capacity(compiled_updates.len());
                    for (idx, col_name, col_type, vec_dims, program) in &compiled_updates {
                        let v = vm.execute_cow(program, &exec_ctx)?;
                        let coerced = v.try_coerce_to_type(*col_type)?;
                        validate_coercion(&v, &coerced, col_name, *col_type, *vec_dims)?;
                        updates.push((*idx, coerced));
                    }
                    updates
                };

                // Now apply all the computed values to the row
                let changed = !updates_to_apply.is_empty();

                // Capture old values of referenced columns before applying changes
                let ref_old_values: Vec<(usize, Value)> =
                    if changed && !ref_col_indices_for_fk.is_empty() {
                        ref_col_indices_for_fk
                            .iter()
                            .filter_map(|&ci| row.get(ci).map(|v| (ci, v.clone())))
                            .collect()
                    } else {
                        Vec::new()
                    };

                for (idx, new_value) in updates_to_apply {
                    let _ = row.set(idx, new_value);
                }

                if changed {
                    validate_resulting_row_constraints(
                        &constraint_schema,
                        &compiled_table_checks,
                        &row,
                        &mut vm,
                    )?;
                }

                // Collect FK values for post-update validation
                if changed && has_fk_updates {
                    fk_new_values.borrow_mut().push(row.clone());
                }

                // Track referenced column changes for FK cascade enforcement
                if !ref_old_values.is_empty() {
                    for (ci, old_val) in &ref_old_values {
                        if let Some(new_val) = row.get(*ci) {
                            if old_val != new_val {
                                ref_col_changes.borrow_mut().push((
                                    *ci,
                                    old_val.clone(),
                                    new_val.clone(),
                                ));
                            }
                        }
                    }
                }

                // Collect row for RETURNING clause
                if changed && has_returning {
                    returning_rows.borrow_mut().push(row.clone());
                }

                Ok((row, changed))
            };

            // OPTIMIZATION: Use SELECT executor to find matching row_ids, then batch update.
            // This reuses ALL SELECT optimizations: indexes, semi-joins, parallel execution, etc.
            let rows = if where_expr.is_none() {
                if let Some(ref where_clause) = memory_where_clause {
                    if let Some(row_ids) = self.select_row_ids_for_dml(
                        table_name,
                        where_clause,
                        schema,
                        table.as_ref(),
                        ctx,
                    )? {
                        table.update_by_row_ids(&row_ids, &mut setter)?
                    } else {
                        // Fall back to storage layer (non-INTEGER PK or other unsupported case)
                        table.update(where_expr.as_deref(), &mut setter)?
                    }
                } else {
                    table.update(None, &mut setter)?
                }
            } else {
                // With partial pushdown, row-id selection from the residual
                // alone would discard the storage predicate and update a
                // superset. Let storage apply its conjunct and the setter
                // verify only the exact residual.
                table.update(where_expr.as_deref(), &mut setter)?
            };
            rows
        };

        // Post-update FK validation: check new FK values reference existing parent rows
        if has_fk_updates {
            let fk_rows = fk_new_values.into_inner();
            if let Some(ref fk_schema) = fk_update_schema {
                for row in &fk_rows {
                    crate::mutation::foreign_key::check_parent_exists(
                        self.mutation_engine(),
                        table.txn_id(),
                        fk_schema,
                        row,
                    )?;
                }
            }
        }

        // Post-update referenced-column change enforcement: apply CASCADE/SET NULL.
        // RESTRICT constraints were already pre-checked above, so this should not
        // fail for RESTRICT. CASCADE/SET NULL failures are propagated as errors.
        if !fks_by_ref_col.is_empty() {
            let changes = ref_col_changes.into_inner();
            for (col_idx, old_val, new_val) in &changes {
                if let Some(fks_for_col) = fks_by_ref_col.get(col_idx) {
                    crate::mutation::foreign_key::enforce_update_actions(
                        self.mutation_engine(),
                        table.txn_id(),
                        old_val,
                        new_val,
                        fks_for_col,
                    )?;
                }
            }
        }

        // Invalidate semantic cache for this table BEFORE commit
        // CRITICAL: Must invalidate before commit to prevent stale data window
        if rows_affected > 0 {
            self.mutation_invalidate_semantic_cache(table_name);
            invalidate_semi_join_cache_for_table(table_name);
            invalidate_scalar_subquery_cache_for_table(table_name);
            invalidate_in_subquery_cache_for_table(table_name);
        }

        let returning_result = if has_returning {
            let rows = returning_rows.into_inner();
            Some(build_returning_result(
                &stmt.returning,
                rows,
                &column_names,
                ctx,
            )?)
        } else {
            None
        };

        // Commit if this is a standalone (auto-commit) transaction
        if should_auto_commit {
            // Commit the transaction through the shared all-table marker protocol.
            if let Some(mut tx) = standalone_tx {
                tx.commit()?;
            }
        }

        // Handle RETURNING clause
        if let Some(result) = returning_result {
            return Ok(result);
        }

        Ok(Box::new(ExecResult::with_rows_affected(
            rows_affected as i64,
        )))
    }

    /// Try to extract a constant value from a SET expression for FK pre-validation.
    /// Returns Some(value) for literals, parameters, and negated literals.
    /// Returns None for column references, functions, subqueries, etc.
    fn try_extract_constant_fk_value(expr: &Expression, ctx: &ExecutionContext) -> Option<Value> {
        match expr {
            Expression::IntegerLiteral(lit) => Some(Value::Integer(lit.value)),
            Expression::FloatLiteral(lit) => Some(Value::Float(lit.value)),
            Expression::StringLiteral(lit) => Some(Value::text(lit.value.as_str())),
            Expression::BooleanLiteral(lit) => Some(Value::Boolean(lit.value)),
            Expression::NullLiteral(_) => Some(Value::null_unknown()),
            Expression::Prefix(prefix) if prefix.operator == "-" => match prefix.right.as_ref() {
                Expression::IntegerLiteral(lit) => Some(Value::Integer(-lit.value)),
                Expression::FloatLiteral(lit) => Some(Value::Float(-lit.value)),
                _ => None,
            },
            Expression::Parameter(param) => {
                if param.name.starts_with(':') {
                    ctx.get_named_param(&param.name[1..]).cloned()
                } else if param.index > 0 {
                    ctx.params().get(param.index - 1).cloned()
                } else {
                    None
                }
            }
            _ => None, // Column reference, function, subquery, etc. — can't pre-validate
        }
    }

    /// A direct `SET column = column` assignment cannot change referenced
    /// identity and therefore must not trigger RESTRICT or a cascade walk.
    fn assignment_preserves_column(
        expression: &Expression,
        column_name: &str,
        table_name: &str,
    ) -> bool {
        match expression {
            Expression::Identifier(identifier) => {
                identifier.value.eq_ignore_ascii_case(column_name)
            }
            Expression::QualifiedIdentifier(identifier) => {
                identifier.name.value.eq_ignore_ascii_case(column_name)
                    && identifier.qualifier.value.eq_ignore_ascii_case(table_name)
            }
            _ => false,
        }
    }

    /// Execute a DELETE statement
    fn execute_delete(
        &self,
        stmt: &DeleteStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let plan = self.mutation_prepare_dml_triggers(
            stmt.table_name.value_lower.as_str(),
            DmlTriggerEvent::Delete,
            &[],
            ctx,
        )?;
        if plan.is_empty() {
            return self.execute_delete_body(stmt, ctx, None);
        }
        self.execute_with_trigger_statement(&plan, ctx, |plan| {
            self.execute_delete_body(stmt, ctx, Some(plan))
        })
    }

    fn execute_delete_body(
        &self,
        stmt: &DeleteStatement,
        ctx: &ExecutionContext,
        triggers: Option<&DmlTriggerPlan>,
    ) -> Result<Box<dyn QueryResult>> {
        // OPTIMIZATION: Use pre-computed lowercase name to avoid allocation per query
        let table_name = &stmt.table_name.value_lower;
        // Use alias if provided, otherwise use table name
        let effective_name = stmt
            .alias
            .as_ref()
            .map(|a| a.value_lower.as_str())
            .unwrap_or(table_name.as_str());

        // Check if there's an active explicit transaction
        let mut active_tx = self.mutation_active_transaction().lock().unwrap();

        let (mut table, should_auto_commit, standalone_tx) =
            if let Some(ref mut tx_state) = *active_tx {
                // Use the active transaction
                // NOTE: table_name is already lowercase (value_lower from AST)
                let table = tx_state.transaction.get_table(table_name)?;

                // Store a reference to this table for commit/rollback
                if !tx_state.tables.contains_key(table_name.as_str()) {
                    tx_state.tables.insert(
                        table_name.to_string(),
                        tx_state.transaction.get_table(table_name)?,
                    );
                }

                (table, false, None)
            } else {
                // No active transaction - create a standalone transaction with auto-commit
                let tx = self.mutation_engine().begin_transaction()?;
                let table = tx.get_table(table_name)?;
                (table, true, Some(tx))
            };

        // Drop the lock before doing work
        drop(active_tx);

        // Check for RETURNING clause
        let has_returning = !stmt.returning.is_empty();
        let mut returning_rows: Vec<Row> = Vec::new();

        // Build WHERE expression - try to convert to storage expression
        // If that fails (complex expression like a + b > 100), fall back to in-memory filtering
        let schema = table.schema();

        // Check if WHERE has correlated subqueries (needs per-row evaluation)
        // This will be updated after semi-join optimization attempt
        let mut has_correlated = if let Some(ref where_clause) = stmt.where_clause {
            <Self as MutationHost>::mutation_has_subqueries(where_clause)
                && <Self as MutationHost>::mutation_has_correlated_subqueries(where_clause)
        } else {
            false
        };

        // OPTIMIZATION: For correlated EXISTS/IN in WHERE, try semi-join optimization first.
        // This transforms O(outer × inner) per-row subquery execution to O(inner + outer).
        let (where_expr, needs_memory_filter, memory_where_clause): (
            Option<Box<dyn StorageExpr>>,
            bool,
            Option<Expression>,
        ) = if let Some(ref where_clause) = stmt.where_clause {
            if has_correlated {
                // Try semi-join optimization for correlated EXISTS/IN
                // Avoid cloning upfront - only clone if no optimization succeeds
                let outer_tables = vec![table_name.to_string()];

                // Try EXISTS semi-join optimization
                let exists_optimized = self
                    .mutation_optimize_exists_to_semi_join(where_clause, ctx, &outer_tables, None)
                    .ok()
                    .flatten();

                // Try IN semi-join optimization (on EXISTS result or original)
                let expr_for_in = exists_optimized.as_ref().unwrap_or(where_clause.as_ref());
                let in_optimized = self
                    .mutation_optimize_in_to_semi_join(expr_for_in, ctx, &outer_tables)
                    .ok()
                    .flatten();

                // Determine final expression without unnecessary clones
                let (current_expr, any_optimized) = match (&exists_optimized, &in_optimized) {
                    (_, Some(_)) => (in_optimized.unwrap(), true),
                    (Some(_), None) => (exists_optimized.unwrap(), true),
                    (None, None) => ((**where_clause).clone(), false),
                };

                // Check if there are still correlated subqueries after optimization
                let still_correlated =
                    <Self as MutationHost>::mutation_has_correlated_subqueries(&current_expr);

                if any_optimized && !still_correlated {
                    // All correlated subqueries were optimized away - update flag
                    has_correlated = false;
                    let processed =
                        if <Self as MutationHost>::mutation_has_subqueries(&current_expr) {
                            self.mutation_process_where_subqueries(&current_expr, ctx)?
                        } else {
                            current_expr
                        };
                    let plan = pushdown::try_pushdown_plan(&processed, schema, Some(ctx));
                    let needs_mem = plan.needs_memory_filter();
                    (plan.storage_expr, needs_mem, plan.residual)
                } else {
                    // Still have correlated subqueries - use per-row processing with optimized expr
                    (None, true, Some(current_expr))
                }
            } else {
                let processed_where =
                    if <Self as MutationHost>::mutation_has_subqueries(where_clause) {
                        self.mutation_process_where_subqueries(where_clause, ctx)?
                    } else {
                        (**where_clause).clone()
                    };

                // Try to push down predicate to storage layer
                let plan = pushdown::try_pushdown_plan(&processed_where, schema, Some(ctx));
                let needs_mem = plan.needs_memory_filter();
                (plan.storage_expr, needs_mem, plan.residual)
            }
        } else {
            (None, false, None)
        };

        // Check if this table is referenced by child tables (for FK enforcement)
        let referencing_fks = crate::mutation::foreign_key::find_referencing_fks_for_txn(
            self.mutation_engine(),
            table.txn_id(),
            table_name,
        );

        // Get schema info for RETURNING clause processing
        let column_names_owned = schema.column_names_owned().to_vec();
        let column_count = schema.columns.len();
        let has_referencing_fks = !referencing_fks.is_empty();

        // Delete rows
        let needs_trigger_rows = triggers.is_some_and(DmlTriggerPlan::has_row_triggers);
        let rows_affected =
            if needs_memory_filter || has_returning || has_referencing_fks || needs_trigger_rows {
                // Complex WHERE expression, RETURNING, or FK enforcement - need to scan rows first
                // Scan all rows, filter with evaluator, collect for RETURNING, delete matching ones by primary key
                // Get schema via engine (CompactArc ref-count bump, no deep clone)
                let schema_arc = self.mutation_engine().get_table_schema(table_name)?;

                // Build column names with effective prefix (alias or table name)
                // This allows WHERE clauses to reference columns using the alias
                // OPTIMIZATION: Only build when needed (memory filter or RETURNING)
                let column_names_with_prefix: Vec<String> = column_names_owned
                    .iter()
                    .map(|c| format!("{}.{}", effective_name, c))
                    .collect();

                // Create evaluator for WHERE filtering
                let mut evaluator =
                    CompiledEvaluator::new(self.mutation_function_registry()).with_context(ctx);
                // Initialize with prefixed column names to support alias.column syntax
                evaluator.init_columns(&column_names_with_prefix);

                // Scan all rows and retain both the physical row identity and the
                // complete logical row. Foreign keys may reference a UUID primary
                // key or another full UNIQUE column; neither can be reconstructed
                // from the engine's internal i64 row ID.
                let column_indices: Vec<usize> = (0..column_count).collect();
                let mut scanner = table.scan(&column_indices, where_expr.as_deref())?;
                let mut rows_to_delete: Vec<(i64, Row)> = Vec::new();

                // Pre-compute column name mappings for correlated subqueries
                let column_names_arc = if has_correlated {
                    Some(CompactArc::new(column_names_owned.clone()))
                } else {
                    None
                };

                // OPTIMIZATION: Use schema's cached lowercase column names instead of computing
                // Each entry: (col_lower, effective_qualified, optional_table_qualified)
                // Uses CompactArc<str> for zero-cost cloning in the per-row loop
                let column_names_lower = schema.column_names_lower_arc();
                #[allow(clippy::type_complexity)]
                let col_name_triples: Vec<(
                    CompactArc<str>,
                    CompactArc<str>,
                    Option<CompactArc<str>>,
                )> = column_names_lower
                    .iter()
                    .map(|col_lower| {
                        let effective_qualified =
                            CompactArc::from(format!("{}.{}", effective_name, col_lower).as_str());
                        let table_qualified = if effective_name != table_name {
                            Some(CompactArc::from(
                                format!("{}.{}", table_name, col_lower).as_str(),
                            ))
                        } else {
                            None
                        };
                        (
                            CompactArc::from(col_lower.as_str()),
                            effective_qualified,
                            table_qualified,
                        )
                    })
                    .collect();

                // Reusable outer_row_map for correlated subqueries
                // Uses CompactArc<str> keys for zero-cost cloning
                let estimated_entries = col_name_triples.len() * 3; // up to 3 entries per column
                let mut outer_row_map: FxHashMap<CompactArc<str>, Value> =
                    FxHashMap::with_capacity_and_hasher(estimated_entries, Default::default());

                while scanner.next() {
                    let row = scanner.row();

                    // Check memory filter if needed
                    let matches = if needs_memory_filter {
                        evaluator.set_row_array(row);
                        if let Some(ref where_expr) = memory_where_clause {
                            if has_correlated {
                                // Build outer row context using pre-computed names
                                outer_row_map.clear();
                                for (i, (col_lower, effective_qualified, table_qualified)) in
                                    col_name_triples.iter().enumerate()
                                {
                                    if let Some(value) = row.get(i) {
                                        outer_row_map.insert(col_lower.clone(), value.clone());
                                        outer_row_map
                                            .insert(effective_qualified.clone(), value.clone());
                                        if let Some(tq) = table_qualified {
                                            outer_row_map.insert(tq.clone(), value.clone());
                                        }
                                    }
                                }

                                // Create context with outer row (move map, take it back later)
                                let mut correlated_ctx = ctx.with_outer_row(
                                    std::mem::take(&mut outer_row_map),
                                    column_names_arc.clone().unwrap(),
                                );

                                // Process correlated subquery with outer context
                                let processed = self.mutation_process_correlated_where(
                                    where_expr,
                                    &correlated_ctx,
                                )?;
                                // OPTIMIZATION: Take ownership instead of cloning
                                evaluator.set_outer_row_owned(
                                    correlated_ctx.take_outer_row().unwrap_or_default(),
                                );
                                let result = evaluator.evaluate_bool(&processed)?;
                                // Take back map for reuse instead of clearing
                                outer_row_map = evaluator.take_outer_row();
                                result
                            } else {
                                evaluator.evaluate_bool(where_expr)?
                            }
                        } else {
                            true
                        }
                    } else {
                        true // Storage layer already filtered
                    };

                    if matches {
                        rows_to_delete.push((scanner.current_row_id()?, row.clone()));
                    }
                }
                // Drop scanner to release borrow
                drop(scanner);

                if let Some(plan) = triggers {
                    let mut admitted = Vec::with_capacity(rows_to_delete.len());
                    for (row_id, old_row) in rows_to_delete {
                        if self
                            .mutation_fire_before_row_triggers(
                                plan,
                                Some(&old_row),
                                None,
                                Some(row_id),
                                ctx,
                            )?
                            .is_some()
                        {
                            admitted.push((row_id, old_row));
                        }
                    }
                    rows_to_delete = admitted;
                }

                // FK enforcement: check/cascade referencing child tables before deleting
                if has_referencing_fks && !rows_to_delete.is_empty() {
                    crate::mutation::foreign_key::enforce_delete_actions_iter(
                        self.mutation_engine(),
                        table.txn_id(),
                        table_name,
                        &schema_arc,
                        rows_to_delete.iter().map(|(_, row)| row),
                        &referencing_fks,
                    )?;
                }

                // Apply one physical-ID batch. Storage returns the exact staged
                // IDs so RETURNING remains correct if a concurrent recheck skips a
                // candidate; rows are moved, not cloned into a second full buffer.
                let row_ids: Vec<i64> = rows_to_delete.iter().map(|(row_id, _)| *row_id).collect();
                let mut deleted_row_ids = Vec::with_capacity(row_ids.len());
                let delete_count =
                    table.delete_candidate_row_ids_collect(&row_ids, None, &mut deleted_row_ids)?;
                let needs_deleted_rows =
                    has_returning || triggers.is_some_and(DmlTriggerPlan::has_after_row_triggers);
                if needs_deleted_rows {
                    let mut rows_by_id: FxHashMap<i64, Row> = rows_to_delete.into_iter().collect();
                    if has_returning {
                        returning_rows.reserve(deleted_row_ids.len());
                    }
                    for row_id in deleted_row_ids {
                        if let Some(row) = rows_by_id.remove(&row_id) {
                            if let Some(plan) = triggers {
                                self.mutation_fire_after_row_triggers(
                                    plan,
                                    Some(&row),
                                    None,
                                    Some(row_id),
                                    ctx,
                                )?;
                            }
                            if has_returning {
                                returning_rows.push(row);
                            }
                        }
                    }
                }
                delete_count
            } else {
                // Explicit two-phase DML access plan for every storage-pushdown
                // shape: discover internal row IDs with an exact empty projection,
                // then apply one bounded mutation batch. This works for INTEGER and
                // UUID primary keys because executor-visible PK values are not used
                // as storage row identity.
                let row_ids = table.collect_delete_candidate_row_ids(where_expr.as_deref())?;
                table.delete_candidate_row_ids(&row_ids, where_expr.as_deref())?
            };

        // Invalidate semantic cache for this table BEFORE commit
        // CRITICAL: Must invalidate before commit to prevent stale data window
        if rows_affected > 0 {
            self.mutation_invalidate_semantic_cache(table_name);
            invalidate_semi_join_cache_for_table(table_name);
            invalidate_scalar_subquery_cache_for_table(table_name);
            invalidate_in_subquery_cache_for_table(table_name);
        }

        let returning_result = if has_returning {
            Some(build_returning_result(
                &stmt.returning,
                returning_rows,
                &column_names_owned,
                ctx,
            )?)
        } else {
            None
        };

        // Commit if this is a standalone (auto-commit) transaction
        if should_auto_commit {
            // Commit the transaction through the shared all-table marker protocol.
            if let Some(mut tx) = standalone_tx {
                tx.commit()?;
            }
        }

        // Handle RETURNING clause
        if let Some(result) = returning_result {
            return Ok(result);
        }

        Ok(Box::new(ExecResult::with_rows_affected(
            rows_affected as i64,
        )))
    }

    /// Execute a TRUNCATE statement
    /// TRUNCATE is equivalent to DELETE without WHERE clause, but more efficient.
    ///
    /// **Non-rollbackable**: Like MySQL and Oracle, TRUNCATE physically destroys
    /// versions, arena, and indexes immediately. ROLLBACK cannot undo it.
    /// This is a deliberate trade-off: O(1) truncation vs rollback safety.
    /// Use `DELETE FROM table` if transactional rollback is needed.
    ///
    /// Fails with `TableHasActiveTransactions` if:
    /// - The current explicit transaction has already modified this table (INSERT/UPDATE/DELETE)
    /// - Another transaction holds uncommitted UPDATE/DELETE claims on the table
    fn execute_truncate(
        &self,
        stmt: &TruncateStatement,
        _ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        // OPTIMIZATION: Use pre-computed lowercase name to avoid allocation per query
        let table_name = &stmt.table_name.value_lower;

        // Check if there's an active explicit transaction
        let active_tx = self.mutation_active_transaction().lock().unwrap();

        let (txn_id, standalone_tx) = if let Some(tx_state) = active_tx.as_ref() {
            if tx_state.tables.contains_key(table_name.as_str()) {
                return Err(Error::TableHasActiveTransactions);
            }
            // Resolve the table before crossing the durable boundary. TRUNCATE
            // remains deliberately nonrollbackable inside an explicit
            // transaction, but now has one durable/runtime outcome.
            let _ = tx_state.transaction.get_table(table_name)?;
            (tx_state.transaction.id(), None)
        } else {
            let tx = self.mutation_engine().begin_transaction()?;
            let txn_id = tx.id();
            let _ = tx.get_table(table_name)?;
            (txn_id, Some(tx))
        };

        // Drop the lock before doing work
        drop(active_tx);

        // FK enforcement: block truncate if child tables reference this table
        // Uses the table's transaction for visibility (sees uncommitted child deletes)
        crate::mutation::foreign_key::check_no_referencing_rows(
            self.mutation_engine(),
            table_name,
            Some(txn_id),
        )?;

        let rows_affected = self
            .mutation_engine()
            .truncate_table_under_ddl_fence(table_name, txn_id)?;

        // Invalidate semantic cache for this table BEFORE commit
        // CRITICAL: Must invalidate before commit to prevent stale data window
        // (TRUNCATE always invalidates, regardless of rows_affected, for safety)
        self.mutation_invalidate_semantic_cache(table_name);
        invalidate_semi_join_cache_for_table(table_name);
        invalidate_scalar_subquery_cache_for_table(table_name);
        invalidate_in_subquery_cache_for_table(table_name);

        // This transaction has no versioned writes; close its registry state.
        if let Some(mut tx) = standalone_tx {
            tx.commit()?;
        }

        Ok(Box::new(ExecResult::with_rows_affected(
            rows_affected as i64,
        )))
    }
}

impl<T: MutationHost + ?Sized> DmlExecutorExt for T {}
