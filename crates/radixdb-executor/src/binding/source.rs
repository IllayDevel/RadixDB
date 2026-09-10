//! CTE, view, table-source, and JOIN column binding.

use std::sync::Arc;

use radixdb_core::{Error, Result};
use radixdb_functions::FunctionRegistry;
use radixdb_sql::ast::{Expression, JoinTableSource, SelectStatement, Statement};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::mvcc::ViewDefinition;
use radixdb_storage::traits::Engine;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::context::ExecutionContext;
use crate::dispatch::cache::QueryCache;
use crate::utils::extract_base_column_name;

const MAX_VIEW_BINDING_DEPTH: usize = 32;

/// Composition state required by source binding while the root owns the
/// concrete database object.
#[doc(hidden)]
pub trait SourceBindingHost {
    type BindingCache: Default;

    fn source_binding_engine(&self) -> &MVCCEngine;

    fn source_binding_functions(&self) -> &FunctionRegistry;

    fn source_binding_query_cache(&self) -> &QueryCache<Self::BindingCache>;

    fn source_binding_view(&self, name_lower: &str) -> Result<Option<Arc<ViewDefinition>>>;
}

/// Schema-only source and column binding owned by the executor crate.
#[doc(hidden)]
pub trait SourceBindingExt: SourceBindingHost {
    fn parse_view_statement(&self, view_query: &str) -> Result<Arc<Statement>> {
        if let Some(cached) = self.source_binding_query_cache().get(view_query) {
            if !matches!(cached.statement(), Statement::Select(_)) {
                return Err(Error::InvalidArgument(
                    "View definition is not a SELECT statement".to_string(),
                ));
            }
            return Ok(cached.statement);
        }

        let mut statements = radixdb_sql::parse_sql(view_query).map_err(|error| {
            Error::InvalidArgument(format!("Failed to parse view query: {error}"))
        })?;
        if statements.len() != 1 {
            return Err(Error::InvalidArgument(
                "View definition must contain exactly one statement".to_string(),
            ));
        }
        let statement = statements
            .pop()
            .expect("single statement length was checked");
        if !matches!(statement, Statement::Select(_)) {
            return Err(Error::InvalidArgument(
                "View definition is not a SELECT statement".to_string(),
            ));
        }

        Ok(self
            .source_binding_query_cache()
            .put(view_query, Arc::new(statement), false, 0)
            .statement)
    }

    fn collect_select_binding_columns(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
        columns: &mut FxHashMap<String, usize>,
        depth: usize,
    ) -> Result<()> {
        let has_star = statement.columns.iter().any(|expression| {
            matches!(
                expression,
                Expression::Star(_) | Expression::QualifiedStar(_)
            )
        });
        if has_star {
            if let Some(source) = &statement.table_expr {
                self.collect_join_binding_columns(source, context, columns, depth + 1)?;
            }
        }
        for expression in &statement.columns {
            match expression {
                Expression::Aliased(aliased) => {
                    add_bound_join_column(columns, &aliased.alias.value)
                }
                Expression::Identifier(identifier) => {
                    add_bound_join_column(columns, &identifier.value)
                }
                Expression::QualifiedIdentifier(identifier) => {
                    add_bound_join_column(columns, &identifier.name.value)
                }
                Expression::Star(_) | Expression::QualifiedStar(_) => {}
                _ => {}
            }
        }
        Ok(())
    }

    fn collect_join_binding_columns(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
        columns: &mut FxHashMap<String, usize>,
        depth: usize,
    ) -> Result<()> {
        if depth > MAX_VIEW_BINDING_DEPTH {
            return Ok(());
        }
        match expression {
            Expression::TableSource(source) => {
                let name = source.name.value_lower.as_str();
                if let Some((cte_columns, _, _)) = context.get_cte_by_lower(name) {
                    for column in cte_columns.iter() {
                        add_bound_join_column(columns, column);
                    }
                } else if let Some(view) = self.source_binding_view(name)? {
                    let statement = self.parse_view_statement(&view.query)?;
                    if let Statement::Select(select) = statement.as_ref() {
                        self.collect_select_binding_columns(select, context, columns, depth + 1)?;
                    }
                } else {
                    let schema = self.source_binding_engine().get_table_schema(name)?;
                    for column in &schema.columns {
                        add_bound_join_column(columns, &column.name);
                    }
                }
            }
            Expression::JoinSource(join) => {
                self.collect_join_binding_columns(&join.left, context, columns, depth + 1)?;
                self.collect_join_binding_columns(&join.right, context, columns, depth + 1)?;
            }
            Expression::Aliased(aliased) => {
                self.collect_join_binding_columns(&aliased.expression, context, columns, depth + 1)?
            }
            Expression::SubquerySource(source) => {
                self.collect_select_binding_columns(&source.subquery, context, columns, depth + 1)?
            }
            Expression::CteReference(source) => {
                if let Some((cte_columns, _, _)) =
                    context.get_cte_by_lower(&source.name.value_lower)
                {
                    for column in cte_columns.iter() {
                        add_bound_join_column(columns, column);
                    }
                }
            }
            Expression::FunctionTableSource(source) => {
                if source.column_aliases.is_empty() {
                    if let Some(function) = self
                        .source_binding_functions()
                        .get_tvf(source.function.value.as_str())
                    {
                        for column in function.column_names() {
                            add_bound_join_column(columns, &column);
                        }
                    }
                } else {
                    for column in &source.column_aliases {
                        add_bound_join_column(columns, &column.value);
                    }
                }
            }
            Expression::ValuesSource(source) => {
                if source.column_aliases.is_empty() {
                    if let Some(first_row) = source.rows.first() {
                        for index in 0..first_row.len() {
                            add_bound_join_column(columns, &format!("column{}", index + 1));
                        }
                    }
                } else {
                    for column in &source.column_aliases {
                        add_bound_join_column(columns, &column.value);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_join_statement_bindings(
        &self,
        statement: &SelectStatement,
        join_source: &JoinTableSource,
        context: &ExecutionContext,
    ) -> Result<()> {
        let mut left_columns = FxHashMap::default();
        let mut right_columns = FxHashMap::default();
        self.collect_join_binding_columns(&join_source.left, context, &mut left_columns, 0)?;
        self.collect_join_binding_columns(&join_source.right, context, &mut right_columns, 0)?;
        validate_join_output_bindings(statement, join_source, &left_columns, &right_columns)
    }
}

impl<T: SourceBindingHost + ?Sized> SourceBindingExt for T {}

fn add_bound_join_column(columns: &mut FxHashMap<String, usize>, name: &str) {
    let base = extract_base_column_name(name).to_lowercase();
    let count = columns.entry(base).or_insert(0);
    *count = count.saturating_add(1);
}

#[doc(hidden)]
pub fn collect_unqualified_join_columns(expression: &Expression, columns: &mut FxHashSet<String>) {
    match expression {
        Expression::Identifier(identifier) => {
            columns.insert(identifier.value_lower.to_string());
        }
        Expression::Infix(infix) => {
            collect_unqualified_join_columns(&infix.left, columns);
            collect_unqualified_join_columns(&infix.right, columns);
        }
        Expression::Prefix(prefix) => {
            collect_unqualified_join_columns(&prefix.right, columns);
        }
        Expression::In(value) => {
            collect_unqualified_join_columns(&value.left, columns);
            match value.right.as_ref() {
                Expression::ExpressionList(list) => {
                    for expression in &list.expressions {
                        collect_unqualified_join_columns(expression, columns);
                    }
                }
                Expression::List(list) => {
                    for expression in &list.elements {
                        collect_unqualified_join_columns(expression, columns);
                    }
                }
                other => collect_unqualified_join_columns(other, columns),
            }
        }
        Expression::Between(value) => {
            collect_unqualified_join_columns(&value.expr, columns);
            collect_unqualified_join_columns(&value.lower, columns);
            collect_unqualified_join_columns(&value.upper, columns);
        }
        Expression::Like(value) => {
            collect_unqualified_join_columns(&value.left, columns);
            collect_unqualified_join_columns(&value.pattern, columns);
            if let Some(escape) = &value.escape {
                collect_unqualified_join_columns(escape, columns);
            }
        }
        Expression::FunctionCall(function) => {
            for argument in &function.arguments {
                collect_unqualified_join_columns(argument, columns);
            }
            if let Some(filter) = &function.filter {
                collect_unqualified_join_columns(filter, columns);
            }
        }
        Expression::Aliased(aliased) => {
            collect_unqualified_join_columns(&aliased.expression, columns);
        }
        Expression::Cast(cast) => collect_unqualified_join_columns(&cast.expr, columns),
        Expression::Case(value) => {
            if let Some(expression) = &value.value {
                collect_unqualified_join_columns(expression, columns);
            }
            for clause in &value.when_clauses {
                collect_unqualified_join_columns(&clause.condition, columns);
                collect_unqualified_join_columns(&clause.then_result, columns);
            }
            if let Some(expression) = &value.else_value {
                collect_unqualified_join_columns(expression, columns);
            }
        }
        _ => {}
    }
}

fn validate_unqualified_join_expression(
    expression: &Expression,
    join_source: &JoinTableSource,
    left_columns: &FxHashMap<String, usize>,
    right_columns: &FxHashMap<String, usize>,
) -> Result<()> {
    let mut columns = FxHashSet::default();
    collect_unqualified_join_columns(expression, &mut columns);
    for column in columns {
        let matches = left_columns
            .get(&column)
            .copied()
            .unwrap_or(0)
            .saturating_add(right_columns.get(&column).copied().unwrap_or(0));
        if matches > 1
            && !join_column_is_coalesced(join_source, &column, left_columns, right_columns)
        {
            return Err(Error::AmbiguousColumn(column));
        }
    }
    Ok(())
}

#[doc(hidden)]
pub fn join_column_is_coalesced(
    join_source: &JoinTableSource,
    column: &str,
    left_columns: &FxHashMap<String, usize>,
    right_columns: &FxHashMap<String, usize>,
) -> bool {
    if left_columns.get(column).copied() != Some(1) || right_columns.get(column).copied() != Some(1)
    {
        return false;
    }
    join_source
        .join_type
        .to_ascii_uppercase()
        .contains("NATURAL")
        || join_source
            .using_columns
            .iter()
            .any(|using_column| using_column.value_lower.eq_ignore_ascii_case(column))
}

fn validate_join_output_bindings(
    statement: &SelectStatement,
    join_source: &JoinTableSource,
    left_columns: &FxHashMap<String, usize>,
    right_columns: &FxHashMap<String, usize>,
) -> Result<()> {
    for expression in &statement.columns {
        validate_unqualified_join_expression(expression, join_source, left_columns, right_columns)?;
    }

    let mut output_labels = FxHashMap::default();
    for column in &statement.columns {
        let label = match column {
            Expression::Aliased(aliased) => Some(aliased.alias.value_lower.as_str()),
            Expression::Identifier(identifier) => Some(identifier.value_lower.as_str()),
            Expression::QualifiedIdentifier(identifier) => {
                Some(identifier.name.value_lower.as_str())
            }
            _ => None,
        };
        if let Some(label) = label {
            let count = output_labels.entry(label.to_string()).or_insert(0usize);
            *count = count.saturating_add(1);
        }
    }

    for order in &statement.order_by {
        let unique_output_label = match &order.expression {
            Expression::Identifier(identifier) => {
                output_labels.get(identifier.value_lower.as_str()).copied() == Some(1)
            }
            _ => false,
        };
        if !unique_output_label {
            validate_unqualified_join_expression(
                &order.expression,
                join_source,
                left_columns,
                right_columns,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select_and_join(sql: &str) -> (SelectStatement, JoinTableSource) {
        let mut statements = radixdb_sql::parse_sql(sql).unwrap();
        let Statement::Select(select) = statements.pop().unwrap() else {
            panic!("expected SELECT");
        };
        let Some(Expression::JoinSource(join)) = select.table_expr.as_deref() else {
            panic!("expected JOIN source");
        };
        (select.clone(), join.as_ref().clone())
    }

    #[test]
    fn rejects_ambiguous_unqualified_projection() {
        let (select, join) =
            select_and_join("SELECT id FROM left_t JOIN right_t ON left_t.id = right_t.id");
        let left = FxHashMap::from_iter([("id".to_string(), 1)]);
        let right = FxHashMap::from_iter([("id".to_string(), 1)]);

        assert!(matches!(
            validate_join_output_bindings(&select, &join, &left, &right),
            Err(Error::AmbiguousColumn(column)) if column == "id"
        ));
    }

    #[test]
    fn unique_select_alias_disambiguates_order_by() {
        let (select, join) = select_and_join(
            "SELECT left_t.id AS selected_id FROM left_t JOIN right_t ON left_t.id = right_t.id ORDER BY selected_id",
        );
        let left = FxHashMap::from_iter([("id".to_string(), 1)]);
        let right = FxHashMap::from_iter([("id".to_string(), 1)]);

        validate_join_output_bindings(&select, &join, &left, &right).unwrap();
    }
}
