//! DISTINCT binding and streaming operator admission.

use radixdb_core::{Error, Result};
use radixdb_sql::ast::Expression;
use radixdb_storage::traits::QueryResult;

use crate::result::{DistinctOnResult, DistinctResult};

pub fn apply(input: Box<dyn QueryResult>, public_width: Option<usize>) -> Box<dyn QueryResult> {
    match public_width {
        Some(width) => Box::new(DistinctResult::with_column_count(input, Some(width))),
        None => Box::new(DistinctResult::new(input)),
    }
}

pub fn apply_on(
    input: Box<dyn QueryResult>,
    distinct_on: &[Expression],
    select_exprs: &[Expression],
) -> Result<Box<dyn QueryResult>> {
    let key_indices = resolve_on_indices(distinct_on, input.columns(), Some(select_exprs))?;
    Ok(Box::new(DistinctOnResult::new(input, key_indices)))
}

/// Resolve DISTINCT ON expressions against the physical row shape.
pub fn resolve_on_indices(
    distinct_on: &[Expression],
    result_columns: &[String],
    select_exprs: Option<&[Expression]>,
) -> Result<Vec<usize>> {
    distinct_on
        .iter()
        .map(|expr| {
            let resolved = (|| -> Option<usize> {
                match expr {
                    Expression::Identifier(id) => {
                        let direct = result_columns
                            .iter()
                            .position(|column| column.eq_ignore_ascii_case(&id.value_lower));
                        if direct.is_some() {
                            return direct;
                        }
                        for (index, select) in select_exprs?.iter().enumerate() {
                            if let Expression::Aliased(aliased) = select {
                                match &*aliased.expression {
                                    Expression::Identifier(selected)
                                        if selected.value_lower == id.value_lower =>
                                    {
                                        return Some(index);
                                    }
                                    Expression::QualifiedIdentifier(selected)
                                        if selected.name.value_lower == id.value_lower =>
                                    {
                                        return Some(index);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        None
                    }
                    Expression::QualifiedIdentifier(identifier) => {
                        let qualified = format!("{}.{}", identifier.qualifier, identifier.name);
                        if let Some(index) = result_columns
                            .iter()
                            .position(|column| column.eq_ignore_ascii_case(&qualified))
                        {
                            return Some(index);
                        }
                        if let Some(expressions) = select_exprs {
                            for (index, select) in expressions.iter().enumerate() {
                                let selected = match select {
                                    Expression::QualifiedIdentifier(selected) => Some(selected),
                                    Expression::Aliased(aliased) => match &*aliased.expression {
                                        Expression::QualifiedIdentifier(selected) => Some(selected),
                                        _ => None,
                                    },
                                    _ => None,
                                };
                                if selected.is_some_and(|selected| {
                                    selected.qualifier.value_lower
                                        == identifier.qualifier.value_lower
                                        && selected.name.value_lower == identifier.name.value_lower
                                }) {
                                    return Some(index);
                                }
                            }
                        }
                        let mut matches = result_columns
                            .iter()
                            .enumerate()
                            .filter(|(_, column)| {
                                column.eq_ignore_ascii_case(&identifier.name.value)
                            })
                            .map(|(index, _)| index);
                        let only = matches.next()?;
                        matches.next().is_none().then_some(only)
                    }
                    Expression::IntegerLiteral(literal) => {
                        let position = usize::try_from(literal.value).ok()?;
                        let width = select_exprs.map_or(result_columns.len(), <[_]>::len);
                        (position >= 1 && position <= width).then_some(position - 1)
                    }
                    _ => {
                        let rendered = expr.to_string();
                        result_columns
                            .iter()
                            .position(|column| column.eq_ignore_ascii_case(&rendered))
                    }
                }
            })();
            resolved.ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "DISTINCT ON expression `{expr}` does not resolve to exactly one output column"
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_sql::{ast::Statement, parse_sql};

    fn expression(sql: &str) -> Expression {
        let mut statements = parse_sql(&format!("SELECT {sql}")).unwrap();
        let Statement::Select(select) = statements.remove(0) else {
            panic!("expected SELECT");
        };
        select.columns.into_iter().next().unwrap()
    }

    #[test]
    fn qualified_fallback_must_be_unambiguous() {
        let expression = expression("source.id");
        assert!(resolve_on_indices(
            std::slice::from_ref(&expression),
            &["id".into(), "id".into()],
            None,
        )
        .is_err());
        assert_eq!(
            resolve_on_indices(&[expression], &["id".into()], None).unwrap(),
            vec![0]
        );
    }
}
