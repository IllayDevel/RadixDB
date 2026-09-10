//! Column dependency planning at the physical scan boundary.

use radixdb_core::StringMap;
use radixdb_sql::ast::{Expression, SelectStatement};

use crate::utils::build_column_index_map;

/// A scan that reads only predicate, ordering and output dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionScanPlan {
    pub scan_indices: Vec<usize>,
    pub scan_columns: Vec<String>,
    pub output_indices_in_scan: Vec<usize>,
    pub output_columns: Vec<String>,
}

/// A scan containing one join key and the table-local predicate dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NarrowKeyStreamPlan {
    pub scan_indices: Vec<usize>,
    pub scan_columns: Vec<String>,
    pub key_index_in_scan: usize,
}

pub fn simple_projection_indices(
    select: &[Expression],
    all_columns: &[String],
) -> Option<(Vec<usize>, Vec<String>)> {
    if select.len() == 1
        && matches!(
            &select[0],
            Expression::Star(_) | Expression::QualifiedStar(_)
        )
    {
        return Some(((0..all_columns.len()).collect(), all_columns.to_vec()));
    }

    let columns = build_column_index_map(all_columns);
    let mut indices = Vec::with_capacity(select.len());
    let mut names = Vec::with_capacity(select.len());
    for expression in select {
        let (source, output) = match expression {
            Expression::Identifier(identifier) => (
                resolve_column(&columns, expression)?,
                identifier.value.to_string(),
            ),
            Expression::QualifiedIdentifier(identifier) => (
                resolve_column(&columns, expression)?,
                identifier.name.value.to_string(),
            ),
            Expression::Aliased(aliased) => (
                resolve_column(&columns, &aliased.expression)?,
                aliased.alias.value.to_string(),
            ),
            _ => return None,
        };
        indices.push(source);
        names.push(output);
    }
    Some((indices, names))
}

pub fn filtered_simple_projection(
    filter: &Expression,
    output_indices: &[usize],
    output_columns: &[String],
    all_columns: &[String],
) -> Option<ProjectionScanPlan> {
    if all_columns.is_empty() {
        return None;
    }
    let columns = build_column_index_map(all_columns);
    let mut filter_indices = Vec::new();
    if !collect_expression_columns(filter, &columns, &mut filter_indices) {
        return None;
    }

    let scan_indices = ordered_union_indices(
        all_columns.len(),
        output_indices.iter().copied().chain(filter_indices),
    )?;
    if scan_indices.len() == all_columns.len() {
        return None;
    }

    let mut positions = vec![None; all_columns.len()];
    for (position, source) in scan_indices.iter().copied().enumerate() {
        positions[source] = Some(position);
    }
    let output_indices_in_scan = output_indices
        .iter()
        .map(|source| positions.get(*source).copied().flatten())
        .collect::<Option<Vec<_>>>()?;
    Some(plan_from_indices(
        scan_indices,
        all_columns,
        output_indices_in_scan,
        output_columns,
    ))
}

pub fn narrow_key_stream(
    filter: Option<&Expression>,
    key_column: &str,
    all_columns: &[String],
) -> Option<NarrowKeyStreamPlan> {
    if all_columns.is_empty() {
        return None;
    }
    let columns = build_column_index_map(all_columns);
    let key_index = *columns.get(&key_column.to_lowercase())?;
    let mut required = vec![key_index];
    if let Some(filter) = filter {
        if !collect_expression_columns(filter, &columns, &mut required) {
            return None;
        }
    }
    let scan_indices = ordered_union_indices(all_columns.len(), required)?;
    let key_index_in_scan = scan_indices.iter().position(|index| *index == key_index)?;
    let scan_columns = scan_indices
        .iter()
        .map(|index| all_columns[*index].clone())
        .collect();
    Some(NarrowKeyStreamPlan {
        scan_indices,
        scan_columns,
        key_index_in_scan,
    })
}

pub fn filtered_expression_projection(
    filter: &Expression,
    select: &[Expression],
    output_columns: &[String],
    all_columns: &[String],
) -> Option<ProjectionScanPlan> {
    dependency_projection(
        std::iter::once(filter).chain(select.iter()),
        output_columns,
        all_columns,
    )
}

pub fn expression_projection(
    select: &[Expression],
    output_columns: &[String],
    all_columns: &[String],
) -> Option<ProjectionScanPlan> {
    dependency_projection(select.iter(), output_columns, all_columns)
}

pub fn ordered_distinct_projection(
    filter: Option<&Expression>,
    statement: &SelectStatement,
    output_columns: &[String],
    all_columns: &[String],
) -> Option<ProjectionScanPlan> {
    let expressions = filter
        .into_iter()
        .chain(statement.columns.iter())
        .chain(statement.order_by.iter().map(|order| &order.expression))
        .chain(statement.distinct_on.iter());
    dependency_projection(expressions, output_columns, all_columns)
}

fn dependency_projection<'a>(
    expressions: impl IntoIterator<Item = &'a Expression>,
    output_columns: &[String],
    all_columns: &[String],
) -> Option<ProjectionScanPlan> {
    if all_columns.is_empty() {
        return None;
    }
    let columns = build_column_index_map(all_columns);
    let mut required = Vec::new();
    for expression in expressions {
        if !collect_expression_columns(expression, &columns, &mut required) {
            return None;
        }
    }
    let scan_indices = ordered_union_indices(all_columns.len(), required)?;
    if scan_indices.len() == all_columns.len() {
        return None;
    }
    Some(plan_from_indices(
        scan_indices,
        all_columns,
        Vec::new(),
        output_columns,
    ))
}

fn plan_from_indices(
    scan_indices: Vec<usize>,
    all_columns: &[String],
    output_indices_in_scan: Vec<usize>,
    output_columns: &[String],
) -> ProjectionScanPlan {
    let scan_columns = scan_indices
        .iter()
        .map(|index| all_columns[*index].clone())
        .collect();
    ProjectionScanPlan {
        scan_indices,
        scan_columns,
        output_indices_in_scan,
        output_columns: output_columns.to_vec(),
    }
}

fn ordered_union_indices(
    column_count: usize,
    indices: impl IntoIterator<Item = usize>,
) -> Option<Vec<usize>> {
    let mut needed = vec![false; column_count];
    for index in indices {
        *needed.get_mut(index)? = true;
    }
    Some(
        needed
            .iter()
            .enumerate()
            .filter_map(|(index, needed)| needed.then_some(index))
            .collect(),
    )
}

fn resolve_column(columns: &StringMap<usize>, expression: &Expression) -> Option<usize> {
    match expression {
        Expression::Identifier(identifier) => columns.get(identifier.value_lower.as_str()).copied(),
        Expression::QualifiedIdentifier(identifier) => {
            let qualified = format!(
                "{}.{}",
                identifier.qualifier.value_lower, identifier.name.value_lower
            );
            columns
                .get(qualified.as_str())
                .or_else(|| columns.get(identifier.name.value_lower.as_str()))
                .copied()
        }
        _ => None,
    }
}

fn collect_expression_columns(
    expression: &Expression,
    columns: &StringMap<usize>,
    output: &mut Vec<usize>,
) -> bool {
    match expression {
        Expression::Identifier(_) | Expression::QualifiedIdentifier(_) => {
            if let Some(index) = resolve_column(columns, expression) {
                output.push(index);
                true
            } else {
                false
            }
        }
        Expression::Aliased(value) => {
            collect_expression_columns(&value.expression, columns, output)
        }
        Expression::FunctionCall(function) => {
            function
                .arguments
                .iter()
                .all(|argument| collect_expression_columns(argument, columns, output))
                && function
                    .filter
                    .as_ref()
                    .is_none_or(|filter| collect_expression_columns(filter, columns, output))
                && function
                    .order_by
                    .iter()
                    .all(|order| collect_expression_columns(&order.expression, columns, output))
        }
        Expression::Infix(value) => {
            collect_expression_columns(&value.left, columns, output)
                && collect_expression_columns(&value.right, columns, output)
        }
        Expression::Prefix(value) => collect_expression_columns(&value.right, columns, output),
        Expression::Distinct(value) => collect_expression_columns(&value.expr, columns, output),
        Expression::In(value) => {
            collect_expression_columns(&value.left, columns, output)
                && collect_expression_columns(&value.right, columns, output)
        }
        Expression::InHashSet(value) => collect_expression_columns(&value.column, columns, output),
        Expression::Between(value) => {
            collect_expression_columns(&value.expr, columns, output)
                && collect_expression_columns(&value.lower, columns, output)
                && collect_expression_columns(&value.upper, columns, output)
        }
        Expression::Like(value) => {
            collect_expression_columns(&value.left, columns, output)
                && collect_expression_columns(&value.pattern, columns, output)
                && value
                    .escape
                    .as_ref()
                    .is_none_or(|escape| collect_expression_columns(escape, columns, output))
        }
        Expression::List(value) => value
            .elements
            .iter()
            .all(|item| collect_expression_columns(item, columns, output)),
        Expression::ExpressionList(value) => value
            .expressions
            .iter()
            .all(|item| collect_expression_columns(item, columns, output)),
        Expression::Case(value) => {
            value
                .value
                .as_ref()
                .is_none_or(|item| collect_expression_columns(item, columns, output))
                && value.when_clauses.iter().all(|when| {
                    collect_expression_columns(&when.condition, columns, output)
                        && collect_expression_columns(&when.then_result, columns, output)
                })
                && value
                    .else_value
                    .as_ref()
                    .is_none_or(|item| collect_expression_columns(item, columns, output))
        }
        Expression::Cast(value) => collect_expression_columns(&value.expr, columns, output),
        Expression::IntegerLiteral(_)
        | Expression::FloatLiteral(_)
        | Expression::StringLiteral(_)
        | Expression::BooleanLiteral(_)
        | Expression::NullLiteral(_)
        | Expression::IntervalLiteral(_)
        | Expression::BoundValue(_)
        | Expression::Parameter(_)
        | Expression::Default(_) => true,
        Expression::Star(_)
        | Expression::QualifiedStar(_)
        | Expression::AllAny(_)
        | Expression::Exists(_)
        | Expression::ScalarSubquery(_)
        | Expression::Window(_)
        | Expression::TableSource(_)
        | Expression::JoinSource(_)
        | Expression::SubquerySource(_)
        | Expression::ValuesSource(_)
        | Expression::CteReference(_)
        | Expression::FunctionTableSource(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_sql::parse_sql;

    fn select(sql: &str) -> SelectStatement {
        let mut statements = parse_sql(sql).unwrap();
        match statements.remove(0) {
            radixdb_sql::ast::Statement::Select(statement) => statement,
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn filtered_projection_reads_union_in_source_order() {
        let statement = select("SELECT c, a FROM t WHERE b = 1");
        let all = vec!["a".into(), "b".into(), "c".into(), "unused".into()];
        let (output, names) = simple_projection_indices(&statement.columns, &all).unwrap();
        let plan = filtered_simple_projection(
            statement.where_clause.as_deref().unwrap(),
            &output,
            &names,
            &all,
        )
        .unwrap();
        assert_eq!(plan.scan_indices, [0, 1, 2]);
        assert_eq!(plan.output_indices_in_scan, [2, 0]);
    }

    #[test]
    fn constant_projection_can_request_exact_empty_scan() {
        let statement = select("SELECT 42 FROM t");
        let all = vec!["a".into(), "b".into()];
        let plan = expression_projection(&statement.columns, &["expr1".into()], &all).unwrap();
        assert!(plan.scan_indices.is_empty());
    }
}
