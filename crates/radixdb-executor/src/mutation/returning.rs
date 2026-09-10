//! RETURNING projection and result finalization.

use radixdb_core::{Result, Row, RowVec};
use radixdb_sql::ast::Expression;
use radixdb_storage::traits::QueryResult;

use crate::context::ExecutionContext;

/// Build a result from RETURNING clause expressions
///
/// Evaluates the RETURNING expressions for each affected row and returns
/// the results as a QueryResult.
pub(super) fn build_returning_result(
    returning: &[Expression],
    source_rows: Vec<Row>,
    column_names: &[String],
    ctx: &ExecutionContext,
) -> Result<Box<dyn QueryResult>> {
    use crate::result::ExecutorResult;
    use radixdb_sql::{Identifier, Position, Token, TokenType};

    // Expand Star expressions to all columns
    let mut expanded_exprs: Vec<Expression> = Vec::new();
    let mut result_columns: Vec<String> = Vec::new();

    for (i, expr) in returning.iter().enumerate() {
        match expr {
            Expression::Star(_) => {
                // Expand * to all columns
                for col_name in column_names {
                    result_columns.push(col_name.clone());
                    let token = Token::new(
                        TokenType::Identifier,
                        col_name.clone(),
                        Position::new(0, 0, 0),
                    );
                    expanded_exprs.push(Expression::Identifier(Identifier::new(
                        token,
                        col_name.clone(),
                    )));
                }
            }
            _ => {
                result_columns.push(get_returning_column_name(expr, i));
                expanded_exprs.push(expr.clone());
            }
        }
    }

    use crate::expression::{compile_expression, ExecuteContext, ExprVM, SharedProgram};

    // Pre-compile all RETURNING expressions
    let compiled_exprs: Vec<SharedProgram> = expanded_exprs
        .iter()
        .map(|expr| compile_expression(expr, column_names))
        .collect::<Result<Vec<_>>>()?;

    // Admission is data-independent: even a zero-row mutation must reject
    // an invalid RETURNING expression before commit.
    if source_rows.is_empty() {
        return Ok(Box::new(ExecutorResult::new(result_columns, RowVec::new())));
    }

    // Create VM for execution (reused for all rows)
    let mut vm = ExprVM::new();

    // Evaluate RETURNING expressions for each row
    let mut result_rows = RowVec::with_capacity(source_rows.len());
    for (row_id, row) in source_rows.into_iter().enumerate() {
        // CRITICAL: Include params from context for parameterized queries
        let exec_ctx = ExecuteContext::new(&row)
            .with_params(ctx.params())
            .with_named_params(ctx.named_params())
            .with_transaction_id(ctx.transaction_id())
            .with_stored_function_invoker(ctx.stored_function_invoker());

        let mut row_values = Vec::with_capacity(compiled_exprs.len());
        for program in &compiled_exprs {
            // CRITICAL: Propagate errors instead of silently returning NULL
            let value = vm.execute_cow(program, &exec_ctx)?;
            row_values.push(value);
        }
        result_rows.push((row_id as i64, Row::from_values(row_values)));
    }

    Ok(Box::new(ExecutorResult::new(result_columns, result_rows)))
}

/// Get a column name for a RETURNING expression
pub(super) fn get_returning_column_name(expr: &Expression, index: usize) -> String {
    match expr {
        Expression::Identifier(id) => id.value.to_string(),
        Expression::QualifiedIdentifier(qid) => qid.name.value.to_string(),
        Expression::Star(_) => "*".to_string(),
        Expression::Aliased(aliased) => aliased.alias.value.to_string(),
        Expression::FunctionCall(func) => {
            let args: Vec<String> = func
                .arguments
                .iter()
                .map(|a| get_returning_column_name(a, 0))
                .collect();
            format!("{}({})", func.function, args.join(", "))
        }
        _ => format!("column_{}", index),
    }
}
