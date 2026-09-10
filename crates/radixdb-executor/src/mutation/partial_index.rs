// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! SQL binding for storage-owned partial-index predicates.

use radixdb_core::{Error, Result, Schema};
use radixdb_sql::ast::{
    Expression as AstExpression, Identifier, InfixOperator, PrefixOperator, Statement,
};
use radixdb_storage::index::PartialIndexPredicate;

pub fn bind_from_sql(canonical_sql: &str, schema: &Schema) -> Result<PartialIndexPredicate> {
    let sql = format!(
        "SELECT * FROM __partial_index_predicate WHERE {}",
        canonical_sql
    );
    let mut statements = radixdb_sql::parse_sql(&sql).map_err(|err| {
        Error::invalid_argument(format!(
            "cannot parse partial index predicate '{}': {}",
            canonical_sql, err
        ))
    })?;

    if statements.len() != 1 {
        return Err(Error::invalid_argument(format!(
            "partial index predicate '{}' produced {} parser statements",
            canonical_sql,
            statements.len()
        )));
    }

    let statement = statements.remove(0);
    let Statement::Select(select) = statement else {
        return Err(Error::invalid_argument(format!(
            "partial index predicate '{}' did not parse as SELECT predicate",
            canonical_sql
        )));
    };
    let Some(where_clause) = select.where_clause else {
        return Err(Error::invalid_argument(format!(
            "partial index predicate '{}' parsed without WHERE clause",
            canonical_sql
        )));
    };
    bind_from_ast(&where_clause, schema)
}

pub fn bind_from_ast(
    where_clause: &AstExpression,
    schema: &Schema,
) -> Result<PartialIndexPredicate> {
    let mut referenced_column_names = Vec::new();
    validate_ast(where_clause, schema, &mut referenced_column_names)?;
    referenced_column_names.sort_by_key(|name| name.to_lowercase());
    referenced_column_names.dedup_by(|a, b| a.eq_ignore_ascii_case(b));

    let storage_expr = crate::expr_converter::convert_ast_to_storage_expr(where_clause)
        .ok_or_else(|| {
            Error::invalid_argument(format!(
                "unsupported partial index predicate: {}",
                where_clause
            ))
        })?;
    PartialIndexPredicate::new(
        where_clause.to_string(),
        referenced_column_names,
        storage_expr,
        schema,
    )
}

fn validate_ast(
    expr: &AstExpression,
    schema: &Schema,
    referenced_column_names: &mut Vec<String>,
) -> Result<()> {
    match expr {
        AstExpression::Identifier(identifier) => {
            record_column_reference(identifier, schema, referenced_column_names)
        }
        AstExpression::QualifiedIdentifier(qualified) => Err(Error::invalid_argument(format!(
            "qualified column reference '{}' is not supported in partial index predicates",
            qualified
        ))),
        AstExpression::IntegerLiteral(_)
        | AstExpression::FloatLiteral(_)
        | AstExpression::StringLiteral(_)
        | AstExpression::BooleanLiteral(_)
        | AstExpression::NullLiteral(_) => Ok(()),
        AstExpression::Prefix(prefix) => match prefix.op_type {
            PrefixOperator::Not => validate_ast(&prefix.right, schema, referenced_column_names),
            _ => Err(Error::invalid_argument(format!(
                "unsupported partial index predicate prefix operator '{}'",
                prefix.operator
            ))),
        },
        AstExpression::Infix(infix) => match infix.op_type {
            InfixOperator::And
            | InfixOperator::Or
            | InfixOperator::Equal
            | InfixOperator::NotEqual
            | InfixOperator::LessThan
            | InfixOperator::LessEqual
            | InfixOperator::GreaterThan
            | InfixOperator::GreaterEqual => {
                validate_ast(&infix.left, schema, referenced_column_names)?;
                validate_ast(&infix.right, schema, referenced_column_names)
            }
            InfixOperator::Is | InfixOperator::IsNot => {
                validate_ast(&infix.left, schema, referenced_column_names)?;
                if matches!(
                    &*infix.right,
                    AstExpression::NullLiteral(_) | AstExpression::BooleanLiteral(_)
                ) {
                    Ok(())
                } else {
                    Err(Error::invalid_argument(format!(
                        "unsupported partial index predicate IS operand '{}'",
                        infix.right
                    )))
                }
            }
            _ => Err(Error::invalid_argument(format!(
                "unsupported partial index predicate operator '{}'",
                infix.operator
            ))),
        },
        AstExpression::In(in_expr) => {
            validate_ast(&in_expr.left, schema, referenced_column_names)?;
            match &*in_expr.right {
                AstExpression::ExpressionList(list) => list
                    .expressions
                    .iter()
                    .try_for_each(|expr| validate_literal(expr, schema, referenced_column_names)),
                AstExpression::List(list) => list
                    .elements
                    .iter()
                    .try_for_each(|expr| validate_literal(expr, schema, referenced_column_names)),
                _ => Err(Error::invalid_argument(format!(
                    "partial index IN predicates require a literal value list: {}",
                    in_expr
                ))),
            }
        }
        AstExpression::Between(between) => {
            validate_ast(&between.expr, schema, referenced_column_names)?;
            validate_literal(&between.lower, schema, referenced_column_names)?;
            validate_literal(&between.upper, schema, referenced_column_names)
        }
        AstExpression::Like(like) => {
            validate_ast(&like.left, schema, referenced_column_names)?;
            validate_literal(&like.pattern, schema, referenced_column_names)?;
            if like.escape.is_some() {
                return Err(Error::invalid_argument(
                    "partial index LIKE ESCAPE is unsupported by the durable runtime predicate",
                ));
            }
            Ok(())
        }
        AstExpression::ExpressionList(list) => list
            .expressions
            .iter()
            .try_for_each(|expr| validate_literal(expr, schema, referenced_column_names)),
        AstExpression::List(list) => list
            .elements
            .iter()
            .try_for_each(|expr| validate_literal(expr, schema, referenced_column_names)),
        _ => Err(Error::invalid_argument(format!(
            "unsupported partial index predicate expression: {}",
            expr
        ))),
    }
}

fn validate_literal(
    expr: &AstExpression,
    schema: &Schema,
    referenced_column_names: &mut Vec<String>,
) -> Result<()> {
    match expr {
        AstExpression::IntegerLiteral(_)
        | AstExpression::FloatLiteral(_)
        | AstExpression::StringLiteral(_)
        | AstExpression::BooleanLiteral(_)
        | AstExpression::NullLiteral(_) => Ok(()),
        _ => validate_ast(expr, schema, referenced_column_names),
    }
}

fn record_column_reference(
    identifier: &Identifier,
    schema: &Schema,
    referenced_column_names: &mut Vec<String>,
) -> Result<()> {
    let column_name = identifier.value.to_string();
    if !schema
        .column_index_map()
        .contains_key(identifier.value_lower.as_str())
    {
        return Err(Error::ColumnNotFound(column_name));
    }
    referenced_column_names.push(column_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::{DataType, SchemaBuilder};

    fn schema() -> Schema {
        SchemaBuilder::new("users")
            .add_primary_key("id", DataType::Integer)
            .add("email", DataType::Text)
            .add_nullable("__raf_deleted_at", DataType::Timestamp)
            .build()
    }

    #[test]
    fn rejects_like_escape_without_runtime_support() {
        let error = bind_from_sql("email LIKE 'owner!_%' ESCAPE '!'", &schema())
            .expect_err("durable predicate must not discard ESCAPE semantics");
        assert!(error.to_string().contains("ESCAPE"));
    }

    #[test]
    fn binds_canonical_sql_to_storage_predicate() {
        let predicate = bind_from_sql("__raf_deleted_at IS NULL", &schema()).unwrap();
        assert_eq!(
            predicate.referenced_column_names(),
            &["__raf_deleted_at".to_string()]
        );
        assert_eq!(predicate.referenced_column_ids(), &[2]);
    }
}
