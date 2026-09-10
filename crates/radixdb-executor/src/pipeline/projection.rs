//! Projection row-shape naming and expression evaluation.

use radixdb_core::{Result, Row, StringMap, Value};
use radixdb_sql::ast::Expression;

use crate::expression::CompiledEvaluator;

pub fn evaluate_expression(
    evaluator: &mut CompiledEvaluator<'_>,
    expression: &Expression,
    row: &Row,
    columns: &StringMap<usize>,
) -> Result<Value> {
    match expression {
        Expression::Identifier(identifier) => columns
            .get(identifier.value_lower.as_str())
            .map(|index| row.get(*index).cloned().unwrap_or_else(Value::null_unknown))
            .ok_or_else(|| radixdb_core::Error::ColumnNotFound(identifier.value.to_string())),
        Expression::QualifiedIdentifier(identifier) => {
            let qualified = format!(
                "{}.{}",
                identifier.qualifier.value_lower, identifier.name.value_lower
            );
            columns
                .get(&qualified)
                .or_else(|| columns.get(identifier.name.value_lower.as_str()))
                .map(|index| row.get(*index).cloned().unwrap_or_else(Value::null_unknown))
                .ok_or_else(|| {
                    radixdb_core::Error::ColumnNotFound(format!(
                        "{}.{}",
                        identifier.qualifier.value, identifier.name.value
                    ))
                })
        }
        Expression::Aliased(aliased) => {
            evaluate_expression(evaluator, &aliased.expression, row, columns)
        }
        _ => evaluator.evaluate(expression),
    }
}

pub fn output_column_names(
    expressions: &[Expression],
    source_columns: &[String],
    table_alias: Option<&str>,
) -> Vec<String> {
    let lowercase_columns = expressions
        .iter()
        .any(|expression| matches!(expression, Expression::QualifiedStar(_)))
        .then(|| {
            source_columns
                .iter()
                .map(|column| column.to_lowercase())
                .collect::<Vec<_>>()
        });
    let mut names = Vec::with_capacity(expressions.len());
    for (index, expression) in expressions.iter().enumerate() {
        match expression {
            Expression::Star(_) => {
                names.extend(source_columns.iter().cloned());
                continue;
            }
            Expression::QualifiedStar(star) => {
                let qualifier = star.qualifier.to_lowercase();
                let mut matched = false;
                if let Some(columns) = &lowercase_columns {
                    for (column_index, column) in columns.iter().enumerate() {
                        if column
                            .strip_prefix(qualifier.as_str())
                            .is_some_and(|suffix| suffix.starts_with('.'))
                        {
                            names.push(source_columns[column_index][qualifier.len() + 1..].into());
                            matched = true;
                        }
                    }
                }
                if !matched
                    && table_alias.is_some_and(|alias| alias.eq_ignore_ascii_case(&qualifier))
                {
                    names.extend(source_columns.iter().cloned());
                }
                continue;
            }
            _ => {}
        }
        names.push(match expression {
            Expression::Identifier(identifier) => identifier.value.to_string(),
            Expression::QualifiedIdentifier(identifier) => identifier.name.value.to_string(),
            Expression::Aliased(aliased) => aliased.alias.value.to_string(),
            Expression::FunctionCall(function) => function.function.to_string(),
            Expression::Cast(cast) => match &*cast.expr {
                Expression::Identifier(identifier) => identifier.value.to_string(),
                _ => format!("CAST(expr{})", index + 1),
            },
            _ => format!("expr{}", index + 1),
        });
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_sql::{ast::Statement, parse_sql};

    #[test]
    fn qualified_star_exposes_only_its_public_columns() {
        let mut statements = parse_sql("SELECT u.*").unwrap();
        let Statement::Select(select) = statements.remove(0) else {
            panic!("expected SELECT");
        };
        let expressions = select.columns;
        assert_eq!(
            output_column_names(&expressions, &["u.id".into(), "v.id".into()], None),
            vec!["id"]
        );
    }
}
