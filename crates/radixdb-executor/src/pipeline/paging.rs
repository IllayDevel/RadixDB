//! LIMIT/OFFSET admission and application.

use radixdb_core::{Error, Result, Row, Value};
use radixdb_sql::ast::{Expression, SelectStatement};
use radixdb_storage::traits::QueryResult;

use crate::context::ExecutionContext;
use crate::expression::ExpressionEval;
use crate::result::LimitedResult;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageWindow {
    pub limit: Option<usize>,
    pub offset: usize,
}

impl PageWindow {
    pub fn evaluate(stmt: &SelectStatement, ctx: &ExecutionContext) -> Result<Self> {
        Ok(Self {
            limit: stmt
                .limit
                .as_deref()
                .map(|expr| evaluate_page_expression(expr, ctx, "LIMIT"))
                .transpose()?,
            offset: stmt
                .offset
                .as_deref()
                .map(|expr| evaluate_page_expression(expr, ctx, "OFFSET"))
                .transpose()?
                .unwrap_or(0),
        })
    }

    pub fn retained_top_n(self) -> Option<usize> {
        self.limit.map(|limit| limit.saturating_add(self.offset))
    }

    pub fn apply(
        self,
        result: Box<dyn QueryResult>,
        already_applied: bool,
    ) -> Box<dyn QueryResult> {
        if !already_applied && (self.limit.is_some() || self.offset > 0) {
            Box::new(LimitedResult::new(result, self.limit, self.offset))
        } else {
            result
        }
    }
}

pub fn evaluate_page_expression(
    expression: &Expression,
    ctx: &ExecutionContext,
    clause: &str,
) -> Result<usize> {
    let value = ExpressionEval::compile(expression, &[])?
        .with_context(ctx)
        .eval_slice(&Row::new())?;
    let Value::Integer(value) = value else {
        return Err(Error::Parse(format!(
            "{clause} must be an integer, got {value:?}"
        )));
    };
    if value < 0 {
        return Err(Error::Parse(format!(
            "{clause} must be non-negative, got {value}"
        )));
    }
    usize::try_from(value).map_err(|_| {
        Error::Parse(format!(
            "{clause} value {value} is too large for this platform"
        ))
    })
}

pub fn validate_order_ordinals(stmt: &SelectStatement, output_width: usize) -> Result<()> {
    for order_by in &stmt.order_by {
        if let Expression::IntegerLiteral(literal) = &order_by.expression {
            if literal.value < 1 || literal.value as u128 > output_width as u128 {
                return Err(Error::InvalidArgument(format!(
                    "ORDER BY position {} is outside the SELECT list of {} columns",
                    literal.value, output_width
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_sql::parse_sql;

    fn select(sql: &str) -> SelectStatement {
        let mut statements = parse_sql(sql).unwrap();
        match statements.remove(0) {
            radixdb_sql::ast::Statement::Select(select) => select,
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn order_ordinal_is_checked_against_public_shape() {
        assert!(validate_order_ordinals(&select("SELECT a ORDER BY 1"), 1).is_ok());
        assert!(validate_order_ordinals(&select("SELECT a ORDER BY 2"), 1).is_err());
    }
}
