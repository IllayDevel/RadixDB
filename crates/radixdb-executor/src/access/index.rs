//! Eligibility checks for physical index access paths.

use radixdb_sql::ast::{Expression, InfixOperator, SelectStatement};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::Engine;
use rustc_hash::FxHashSet;

use crate::operators::index_nested_loop::IndexLookupStrategy;
use crate::utils::{extract_base_column_name, flatten_and_predicates};

pub type IndexNestedLoopOpportunity = (String, IndexLookupStrategy, String, String, bool);

/// Select the strongest usable point-lookup edge from an INNER/LEFT JOIN ON
/// predicate. The complete equality set is retained to prove 0..1 cardinality
/// through PK or full non-partial UNIQUE metadata.
pub fn index_nested_loop_opportunity(
    engine: &MVCCEngine,
    right_expression: &Expression,
    join_condition: Option<&Expression>,
    join_type: &str,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
) -> Option<IndexNestedLoopOpportunity> {
    if join_type.contains("RIGHT") || join_type.contains("FULL") {
        return None;
    }

    let (table_name, effective_alias) = indexable_source(right_expression)?;
    let condition = join_condition?;
    let right_table_alias = effective_alias
        .as_deref()
        .or(right_alias)
        .unwrap_or(&table_name);
    let right_alias_lower = right_table_alias.to_lowercase();
    let right_alias_len = right_alias_lower.len();
    let starts_with_alias = |column: &str, alias: &str, alias_len: usize| {
        column.len() > alias_len
            && column.starts_with(alias)
            && column.as_bytes()[alias_len] == b'.'
    };

    let transaction = engine.begin_transaction().ok()?;
    let table = transaction.get_table(&table_name).ok()?;
    let schema = table.schema();
    let left_alias_lower = left_alias.map(str::to_lowercase);
    let mut best: Option<(u8, IndexLookupStrategy, String, String)> = None;
    let mut equality_inner_columns = FxHashSet::default();

    for predicate in flatten_and_predicates(condition) {
        let Some((left_column, right_column)) = simple_join_key(&predicate) else {
            continue;
        };
        let left_lower = left_column.to_lowercase();
        let right_lower = right_column.to_lowercase();
        let (inner, outer) = if starts_with_alias(&left_lower, &right_alias_lower, right_alias_len)
        {
            (left_column, right_column)
        } else if starts_with_alias(&right_lower, &right_alias_lower, right_alias_len) {
            (right_column, left_column)
        } else {
            continue;
        };
        if let Some(left_alias) = left_alias_lower.as_deref() {
            let outer_lower = outer.to_lowercase();
            if !starts_with_alias(&outer_lower, left_alias, left_alias.len()) {
                continue;
            }
        }

        let inner_unqualified = extract_base_column_name(&inner).to_string();
        let inner_lower = inner_unqualified.to_lowercase();
        equality_inner_columns.insert(inner_lower.clone());
        let candidate = if schema
            .pk_column_index()
            .is_some_and(|index| schema.columns[index].name_lower == inner_lower)
        {
            (3, IndexLookupStrategy::PrimaryKey)
        } else if table.has_cold_segments() {
            if table
                .collect_row_ids_by_index_values(&inner_unqualified, &[])
                .is_none()
            {
                continue;
            }
            let Some(index) = table.get_index_on_column(&inner_unqualified) else {
                continue;
            };
            let priority = if index.is_unique() { 2 } else { 1 };
            (
                priority,
                IndexLookupStrategy::SegmentedSecondaryIndex {
                    column_name: inner_unqualified.clone(),
                    index_name: index.name().to_string(),
                },
            )
        } else {
            let Some(index) = table.get_index_on_column(&inner_unqualified) else {
                continue;
            };
            let priority = if index.is_unique() { 2 } else { 1 };
            (priority, IndexLookupStrategy::SecondaryIndex(index))
        };
        if best
            .as_ref()
            .is_none_or(|(priority, _, _, _)| candidate.0 > *priority)
        {
            best = Some((candidate.0, candidate.1, inner_unqualified, outer));
        }
    }

    let pk_unique = schema.pk_column_index().is_some_and(|index| {
        equality_inner_columns.contains(schema.columns[index].name_lower.as_str())
    });
    let declared_unique = table
        .get_unique_non_pk_indexes()
        .into_iter()
        .filter(|index| index.partial_predicate().is_none())
        .any(|index| {
            !index.column_names().is_empty()
                && index
                    .column_names()
                    .iter()
                    .all(|column| equality_inner_columns.contains(column.to_lowercase().as_str()))
        });
    best.map(|(_, strategy, inner, outer)| {
        (
            table_name,
            strategy,
            inner,
            outer,
            pk_unique || declared_unique,
        )
    })
}

fn indexable_source(expression: &Expression) -> Option<(String, Option<String>)> {
    match expression {
        Expression::TableSource(source) if source.as_of.is_none() => {
            Some((source.name.value_lower.to_string(), None))
        }
        Expression::Aliased(aliased) => match aliased.expression.as_ref() {
            Expression::TableSource(source) if source.as_of.is_none() => {
                Some((source.name.value_lower.to_string(), None))
            }
            Expression::SubquerySource(source) => Some((
                simple_passthrough_table(&source.subquery)?,
                source.alias.as_ref().map(|alias| alias.value.to_string()),
            )),
            _ => None,
        },
        Expression::SubquerySource(source) => Some((
            simple_passthrough_table(&source.subquery)?,
            source.alias.as_ref().map(|alias| alias.value.to_string()),
        )),
        _ => None,
    }
}

fn simple_join_key(condition: &Expression) -> Option<(String, String)> {
    match condition {
        Expression::Infix(infix) if infix.op_type == InfixOperator::Equal => Some((
            qualified_column_name(&infix.left)?,
            qualified_column_name(&infix.right)?,
        )),
        Expression::Infix(infix) if infix.op_type == InfixOperator::And => {
            simple_join_key(&infix.left).or_else(|| simple_join_key(&infix.right))
        }
        _ => None,
    }
}

fn qualified_column_name(expression: &Expression) -> Option<String> {
    match expression {
        Expression::QualifiedIdentifier(identifier) => Some(format!(
            "{}.{}",
            identifier.qualifier.value, identifier.name.value
        )),
        Expression::Identifier(identifier) => Some(identifier.value.to_string()),
        _ => None,
    }
}

fn simple_passthrough_table(statement: &SelectStatement) -> Option<String> {
    if statement.with.is_some()
        || !statement.set_operations.is_empty()
        || !statement.group_by.columns.is_empty()
        || statement.having.is_some()
        || !statement.order_by.is_empty()
        || statement.limit.is_some()
        || statement.offset.is_some()
        || statement.where_clause.is_some()
        || statement.distinct
        || !statement.window_defs.is_empty()
    {
        return None;
    }
    match statement.table_expr.as_deref()? {
        Expression::TableSource(source) => Some(source.name.value_lower.to_string()),
        Expression::Aliased(aliased) => match aliased.expression.as_ref() {
            Expression::TableSource(source) => Some(source.name.value_lower.to_string()),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_sql::parse_sql;

    #[test]
    fn passthrough_source_rejects_semantic_modifiers() {
        let mut statements = parse_sql("SELECT * FROM t WHERE id > 0").unwrap();
        let radixdb_sql::ast::Statement::Select(statement) = statements.remove(0) else {
            panic!("expected SELECT");
        };
        assert_eq!(simple_passthrough_table(&statement), None);
    }
}
