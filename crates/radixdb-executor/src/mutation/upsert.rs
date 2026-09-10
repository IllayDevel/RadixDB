//! ON CONFLICT compilation and application helpers.

use radixdb_core::{CompactArc, DataType, Error, Result, Row, Value};
use radixdb_sql::ast::{Expression, InsertStatement};
use radixdb_storage::expression::{ComparisonExpr, Expression as StorageExpr};
use radixdb_storage::traits::Table;

use super::dml_support::{validate_coercion, CompiledUpsert};
use super::host::MutationHost;
use super::validation::{compile_table_check_constraints, validate_resulting_row_constraints};
use crate::context::ExecutionContext;

/// Build a `CompiledUpsert` from `schema` and `stmt` once per INSERT statement.
/// This avoids re-compiling update expressions and re-parsing CHECK constraint SQL
/// for every conflicting row in an upsert batch.
pub(super) fn compile_upsert<T: MutationHost + ?Sized>(
    host: &T,
    schema: &radixdb_core::Schema,
    stmt: &InsertStatement,
) -> Result<CompiledUpsert> {
    use crate::expression::{CompileContext, ExprCompiler, SharedProgram};

    let col_map = schema.column_index_map();

    // Resolve each update column to its schema index, type, and AST expression.
    let mut seen_targets = rustc_hash::FxHashSet::default();
    let update_specs: Vec<(usize, DataType, u16, &Expression)> = stmt
        .update_columns
        .iter()
        .zip(stmt.update_expressions.iter())
        .map(|(col, expr)| {
            let idx = col_map
                .get(col.value_lower.as_str())
                .copied()
                .ok_or_else(|| Error::ColumnNotFound(col.value.to_string()))?;
            if !seen_targets.insert(idx) {
                return Err(Error::InvalidArgument(format!(
                    "ON CONFLICT target column '{}' is assigned more than once",
                    col.value
                )));
            }
            if schema.pk_column_index() == Some(idx) {
                return Err(Error::InvalidArgument(format!(
                    "cannot UPDATE primary key column '{}' in ON CONFLICT",
                    col.value
                )));
            }
            Ok((
                idx,
                schema.columns[idx].data_type,
                schema.columns[idx].vector_dimensions,
                expr,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let column_names: Vec<String> = schema.column_names_owned().to_vec();

    // Build EXCLUDED column name list for the second row source.
    let excluded_columns: Vec<String> = column_names
        .iter()
        .map(|c| format!("excluded.{}", c))
        .collect();

    // Compile each update expression with EXCLUDED as the second row source.
    let mut compiled_updates: Vec<(usize, DataType, u16, SharedProgram)> =
        Vec::with_capacity(update_specs.len());
    for (idx, col_type, vec_dims, expr) in &update_specs {
        let compile_ctx = CompileContext::new(&column_names, host.mutation_function_registry())
            .with_second_row(&excluded_columns);
        let compiler = ExprCompiler::new(&compile_ctx);
        match compiler.compile(expr) {
            Ok(program) => {
                compiled_updates.push((*idx, *col_type, *vec_dims, CompactArc::new(program)));
            }
            Err(e) => {
                return Err(Error::internal(format!(
                    "failed to compile ON CONFLICT update expression: {}",
                    e
                )));
            }
        }
    }

    let compiled_table_checks = compile_table_check_constraints(schema)?;

    Ok(CompiledUpsert {
        compiled_updates,
        compiled_table_checks,
    })
}

/// Apply ON DUPLICATE KEY UPDATE to an existing row.
/// `insert_values` contains the attempted insert row — accessible via `EXCLUDED.column`
/// in update expressions (PostgreSQL-style).
/// `conflict_column` is the unique constraint column(s) that caused the conflict (comma-separated).
/// Used to build WHERE clause when the table has no primary key.
/// When `capture_row` is true, returns the post-update row for RETURNING clauses.
#[allow(clippy::too_many_arguments)]
pub(super) fn apply_on_duplicate_update<T: MutationHost + ?Sized>(
    host: &T,
    table: &mut Box<dyn Table>,
    schema: &radixdb_core::Schema,
    row_id: i64,
    conflict_column: Option<&str>,
    insert_values: &[Value],
    compiled: &CompiledUpsert,
    ctx: &ExecutionContext,
    capture_row: bool,
) -> Result<Option<Row>> {
    // Build a WHERE clause to find the specific row
    // Use schema's cached pk_column_index for O(1) lookup
    let pk_col = schema
        .pk_column_index()
        .map(|idx| schema.columns[idx].name.clone());

    let where_expr: Option<Box<dyn StorageExpr>> = if let Some(pk_name) = pk_col {
        // Table has PK — target by primary key value
        let mut expr =
            ComparisonExpr::new(pk_name, radixdb_core::Operator::Eq, Value::Integer(row_id));
        expr.prepare_for_schema(schema);
        Some(Box::new(expr))
    } else if let Some(conflict_cols) = conflict_column {
        // No PK — target by the unique constraint columns that caused the conflict
        use radixdb_storage::expression::AndExpr;
        let col_names: Vec<&str> = conflict_cols.split(", ").collect();
        let col_map = schema.column_index_map();
        let mut comparisons: Vec<Box<dyn StorageExpr>> = Vec::with_capacity(col_names.len());
        for col_name in &col_names {
            let col_lower = col_name.to_lowercase();
            if let Some(&idx) = col_map.get(col_lower.as_str()) {
                let value = insert_values
                    .get(idx)
                    .cloned()
                    .unwrap_or(Value::null_unknown());
                let mut expr =
                    ComparisonExpr::new(col_name.to_string(), radixdb_core::Operator::Eq, value);
                expr.prepare_for_schema(schema);
                comparisons.push(Box::new(expr));
            }
        }
        if comparisons.len() == 1 {
            Some(comparisons.pop().unwrap())
        } else if comparisons.len() > 1 {
            let mut and_expr = AndExpr::new(comparisons);
            and_expr.prepare_for_schema(schema);
            Some(Box::new(and_expr))
        } else {
            None
        }
    } else {
        None
    };

    use crate::expression::{ExecuteContext, ExprVM};

    // Build the EXCLUDED row from insert_values
    let excluded_row = Row::from_values(insert_values.to_vec());

    // Create VM once and reuse for all rows
    let mut vm = ExprVM::new();

    // Extract params from execution context for use in the setter closure
    let params = ctx.params();
    let named_params = ctx.named_params();

    // Capture the post-update row for RETURNING clause
    let mut captured_row: Option<Row> = None;
    let table_txn_id = table.txn_id();

    // Create a setter function that applies the ON DUPLICATE KEY UPDATE
    let mut setter = |mut row: Row| -> Result<(Row, bool)> {
        // Collect all updates first to avoid borrow conflicts
        let updates_to_apply: Vec<(usize, Value)> = {
            // Use for_join to make EXCLUDED columns available as row2
            let mut exec_ctx = ExecuteContext::for_join(&row, &excluded_row);
            if !params.is_empty() {
                exec_ctx = exec_ctx.with_params(params);
            }
            if !named_params.is_empty() {
                exec_ctx = exec_ctx.with_named_params(named_params);
            }
            exec_ctx = exec_ctx
                .with_transaction_id(ctx.transaction_id())
                .with_stored_function_invoker(ctx.stored_function_invoker());

            let mut updates = Vec::with_capacity(compiled.compiled_updates.len());
            for (idx, col_type, vec_dims, program) in &compiled.compiled_updates {
                let v = vm.execute_cow(program, &exec_ctx)?;
                let coerced = v.try_coerce_to_type(*col_type)?;
                validate_coercion(
                    &v,
                    &coerced,
                    &schema.columns[*idx].name,
                    *col_type,
                    *vec_dims,
                )?;
                if !schema.columns[*idx].nullable && coerced.is_null() {
                    return Err(Error::not_null_constraint(
                        schema.columns[*idx].name.clone(),
                    ));
                }
                updates.push((*idx, coerced));
            }
            updates
        };

        // Apply updates. The complete post-update row is validated once by
        // the shared statement-level CHECK plan below.
        let changed = !updates_to_apply.is_empty();
        for (idx, new_value) in updates_to_apply {
            let _ = row.set(idx, new_value);
        }

        if changed {
            validate_resulting_row_constraints(
                schema,
                &compiled.compiled_table_checks,
                &row,
                &mut vm,
            )?;
            if !schema.foreign_keys.is_empty() {
                crate::mutation::foreign_key::check_parent_exists(
                    host.mutation_engine(),
                    table_txn_id,
                    schema,
                    &row,
                )?;
            }
        }

        // Capture post-update row for RETURNING
        if capture_row {
            captured_row = Some(row.clone());
        }

        Ok((row, changed))
    };

    // Prefer direct row_id lookup when we have a concrete conflicting row_id.
    // This avoids a second scan on non-PK upserts after conflict resolution.
    // row_id < 0 means "unknown" (sentinel from UniqueConstraint error).
    if row_id >= 0 {
        table.update_by_row_ids(&[row_id], &mut setter)?;
    } else {
        table.update(where_expr.as_deref(), &mut setter)?;
    }

    Ok(captured_row)
}
