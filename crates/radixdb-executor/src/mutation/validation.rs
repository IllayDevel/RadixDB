//! Final-row validation shared by every mutation path.

use radixdb_core::{Error, Result, Row, Schema, Value};
use radixdb_storage::traits::Table;

use crate::expression::{compile_expression, ExecuteContext, ExprVM, SharedProgram};

/// Compile every column- and table-level CHECK against the complete schema.
pub fn compile_table_check_constraints(schema: &Schema) -> Result<Vec<(String, SharedProgram)>> {
    const MAX_TABLE_CHECK_COUNT: usize = 65_535;
    const MAX_TABLE_CHECK_EXPRESSION_BYTES: usize = 16 * 1024 * 1024;

    let column_check_count = schema
        .columns
        .iter()
        .filter(|column| column.check_expr.is_some())
        .count();
    let check_count = column_check_count
        .checked_add(schema.table_checks.len())
        .ok_or_else(|| Error::InvalidArgument("CHECK constraint count overflow".into()))?;
    if check_count > MAX_TABLE_CHECK_COUNT {
        return Err(Error::InvalidArgument(format!(
            "table '{}' has too many CHECK constraints: {}",
            schema.table_name, check_count
        )));
    }

    let column_names = schema.column_names_owned();
    let mut compiled = Vec::with_capacity(check_count);
    let expressions = schema
        .columns
        .iter()
        .filter_map(|column| column.check_expr.as_ref())
        .chain(schema.table_checks.iter());
    for expression_text in expressions {
        if expression_text.len() > MAX_TABLE_CHECK_EXPRESSION_BYTES {
            return Err(Error::InvalidArgument(format!(
                "table CHECK expression exceeds {} bytes",
                MAX_TABLE_CHECK_EXPRESSION_BYTES
            )));
        }
        let sql = format!("SELECT {expression_text}");
        let statements = radixdb_sql::parse_sql(&sql).map_err(|error| {
            Error::Parse(format!(
                "invalid table CHECK expression '{}': {}",
                expression_text, error
            ))
        })?;
        let expression = match statements.as_slice() {
            [radixdb_sql::ast::Statement::Select(select)] if select.columns.len() == 1 => {
                &select.columns[0]
            }
            _ => {
                return Err(Error::Parse(format!(
                    "invalid table CHECK expression '{}': expected one expression",
                    expression_text
                )));
            }
        };
        let program = compile_expression(expression, column_names).map_err(|error| {
            Error::Parse(format!(
                "invalid table CHECK expression '{}': {}",
                expression_text, error
            ))
        })?;
        compiled.push((expression_text.clone(), program));
    }
    Ok(compiled)
}

/// Evaluate table-level CHECK programs against a complete post-change row.
pub fn validate_table_check_constraints(
    table_name: &str,
    compiled: &[(String, SharedProgram)],
    row: &Row,
    vm: &mut ExprVM,
) -> Result<()> {
    if compiled.is_empty() {
        return Ok(());
    }

    let context = ExecuteContext::new(row);
    for (expression, program) in compiled {
        match vm.execute_cow(program, &context)? {
            Value::Boolean(true) | Value::Null(_) => {}
            Value::Boolean(false) => {
                return Err(Error::CheckConstraintViolation {
                    column: format!("<table:{table_name}>"),
                    expression: expression.clone(),
                });
            }
            value => {
                return Err(Error::Type(format!(
                    "table CHECK '{}' on '{}' returned {:?}, expected BOOLEAN or NULL",
                    expression,
                    table_name,
                    value.data_type()
                )));
            }
        }
    }
    Ok(())
}

/// Validate the complete row at the final write boundary.
pub fn validate_resulting_row_constraints(
    schema: &Schema,
    compiled: &[(String, SharedProgram)],
    row: &Row,
    vm: &mut ExprVM,
) -> Result<()> {
    #[cfg(feature = "test-mutations")]
    if crate::test_mutations::constraint_validation_disabled() {
        return Ok(());
    }

    radixdb_storage::validation::validate_row_shape(schema, row)?;
    validate_table_check_constraints(&schema.table_name, compiled, row, vm)
}

/// Materialize generated values, then validate the exact row to be published.
pub fn prepare_insert_row_constraints(
    table: &mut dyn Table,
    schema: &Schema,
    compiled: &[(String, SharedProgram)],
    row: &mut Row,
    vm: &mut ExprVM,
) -> Result<()> {
    table.materialize_insert_values(row)?;
    validate_resulting_row_constraints(schema, compiled, row, vm)
}
