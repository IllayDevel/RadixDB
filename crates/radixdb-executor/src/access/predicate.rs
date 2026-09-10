//! Preparation of SQL predicates for physical storage scans.

use std::borrow::Cow;

use radixdb_core::{CompactArc, Schema, Value};
use radixdb_functions::FunctionRegistry;
use radixdb_sql::ast::*;
use radixdb_storage::expression::Expression as StorageExpression;
use rustc_hash::FxHashMap;

use crate::context::ExecutionContext;
use crate::optimizer::ExpressionSimplifier;
use crate::pushdown;
use crate::utils::substitute_outer_references;

pub type DirectPushdown = (Option<Box<dyn StorageExpression>>, bool);

/// Prepare a predicate for paths whose SQL binding has already been resolved
/// by their caller (temporal and certified narrow scans).
pub fn prepare_bound_predicate(
    predicate: Option<&Expression>,
    schema: &Schema,
    context: &ExecutionContext,
) -> DirectPushdown {
    predicate
        .map(|predicate| pushdown::try_pushdown(predicate, schema, Some(context)))
        .unwrap_or((None, false))
}

/// One prepared predicate shared by index selection and scan construction.
/// `effective` preserves the statement-level predicate for feedback and cache
/// identity; `residual` is the exact executor-side remainder after pushdown.
pub struct PreparedPredicate<'a> {
    effective: Option<Cow<'a, Expression>>,
    pub storage: Option<Box<dyn StorageExpression>>,
    pub residual: Option<Expression>,
}

impl<'a> PreparedPredicate<'a> {
    pub fn effective(&self) -> Option<&Expression> {
        self.effective.as_deref()
    }

    pub fn memory_filter(&self) -> Option<&Expression> {
        self.residual.as_ref().or_else(|| self.effective())
    }

    pub fn needs_memory_filter(&self) -> bool {
        self.residual.is_some()
    }
}

/// Resolve SELECT aliases, simplify constants, substitute correlated outer
/// values and split the result into storage and residual predicates.
#[allow(clippy::too_many_arguments)]
pub fn prepare_scan_predicate<'a>(
    select_columns: &[Expression],
    where_clause: Option<&'a Expression>,
    all_columns: &[String],
    schema: &Schema,
    table_alias: Option<&str>,
    context: &ExecutionContext,
    functions: &FunctionRegistry,
    where_has_subqueries: bool,
) -> PreparedPredicate<'a> {
    let aliases = build_alias_map_excluding(select_columns, Some(all_columns));
    let aliased = (!aliases.is_empty())
        .then(|| where_clause.map(|expression| substitute_aliases(expression, &aliases)))
        .flatten();
    let source = aliased.as_ref().or(where_clause);

    let simplified = source.and_then(|expression| {
        ExpressionSimplifier::with_registry(functions).try_simplify(expression)
    });
    let effective = if let Some(expression) = simplified {
        Some(Cow::Owned(expression))
    } else if let Some(expression) = aliased {
        Some(Cow::Owned(expression))
    } else {
        where_clause.map(Cow::Borrowed)
    };

    let Some(predicate) = effective.as_deref() else {
        return PreparedPredicate {
            effective,
            storage: None,
            residual: None,
        };
    };

    if where_has_subqueries {
        let residual = predicate.clone();
        return PreparedPredicate {
            effective,
            storage: None,
            residual: Some(residual),
        };
    }

    if let Some(outer_row) = context.outer_row() {
        let scoped_outer_row: FxHashMap<CompactArc<str>, Value> = outer_row
            .iter()
            .filter(|(name, _)| {
                let name = name.as_ref();
                if let Some(dot) = name.rfind('.') {
                    !table_alias
                        .is_some_and(|qualifier| name[..dot].eq_ignore_ascii_case(qualifier))
                } else {
                    schema.get_column_index(name).is_none()
                }
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        let substituted = substitute_outer_references(predicate, &scoped_outer_row);
        let plan = pushdown::try_pushdown_plan(&substituted, schema, Some(context));
        return if plan.storage_expr.is_some() {
            PreparedPredicate {
                effective,
                storage: plan.storage_expr,
                residual: plan.residual,
            }
        } else {
            PreparedPredicate {
                effective,
                storage: None,
                residual: Some(substituted),
            }
        };
    }

    let plan = pushdown::try_pushdown_plan(predicate, schema, Some(context));
    PreparedPredicate {
        effective,
        storage: plan.storage_expr,
        residual: plan.residual,
    }
}

/// Build aliases that do not shadow real FROM columns. SQL WHERE binding must
/// prefer a source column when an output alias has the same name.
pub fn build_alias_map_excluding<'a>(
    columns: &'a [Expression],
    base_columns: Option<&[String]>,
) -> FxHashMap<String, &'a Expression> {
    let alias_count = columns
        .iter()
        .filter(|expression| matches!(expression, Expression::Aliased(_)))
        .count();
    if alias_count == 0 {
        return FxHashMap::default();
    }

    let mut aliases = FxHashMap::with_capacity_and_hasher(alias_count, Default::default());
    for expression in columns {
        let Expression::Aliased(aliased) = expression else {
            continue;
        };
        let name = aliased.alias.value_lower.to_string();
        if base_columns.is_some_and(|base| {
            base.iter().any(|column| {
                column.eq_ignore_ascii_case(&name)
                    || column
                        .rsplit_once('.')
                        .is_some_and(|(_, tail)| tail.eq_ignore_ascii_case(&name))
            })
        }) {
            continue;
        }
        aliases.insert(name, aliased.expression.as_ref());
    }
    aliases
}

/// Substitute already-bound SELECT aliases inside a predicate.
pub fn substitute_aliases(
    expression: &Expression,
    aliases: &FxHashMap<String, &Expression>,
) -> Expression {
    match expression {
        Expression::Identifier(identifier) => aliases
            .get(identifier.value_lower.as_str())
            .map_or_else(|| expression.clone(), |source| (*source).clone()),
        Expression::Infix(infix) => Expression::Infix(InfixExpression {
            token: infix.token.clone(),
            left: Box::new(substitute_aliases(&infix.left, aliases)),
            operator: infix.operator.clone(),
            op_type: infix.op_type,
            right: Box::new(substitute_aliases(&infix.right, aliases)),
        }),
        Expression::Prefix(prefix) => Expression::Prefix(PrefixExpression {
            token: prefix.token.clone(),
            operator: prefix.operator.clone(),
            op_type: prefix.op_type,
            right: Box::new(substitute_aliases(&prefix.right, aliases)),
        }),
        Expression::Between(between) => Expression::Between(BetweenExpression {
            token: between.token.clone(),
            expr: Box::new(substitute_aliases(&between.expr, aliases)),
            lower: Box::new(substitute_aliases(&between.lower, aliases)),
            upper: Box::new(substitute_aliases(&between.upper, aliases)),
            not: between.not,
        }),
        Expression::In(input) => Expression::In(InExpression {
            token: input.token.clone(),
            left: Box::new(substitute_aliases(&input.left, aliases)),
            right: Box::new(substitute_aliases(&input.right, aliases)),
            not: input.not,
        }),
        Expression::FunctionCall(function) => Expression::FunctionCall(Box::new(FunctionCall {
            token: function.token.clone(),
            function: function.function.clone(),
            arguments: function
                .arguments
                .iter()
                .map(|argument| substitute_aliases(argument, aliases))
                .collect(),
            is_distinct: function.is_distinct,
            order_by: function.order_by.clone(),
            filter: function.filter.clone(),
        })),
        Expression::Case(case) => Expression::Case(Box::new(CaseExpression {
            token: case.token.clone(),
            value: case
                .value
                .as_ref()
                .map(|value| Box::new(substitute_aliases(value, aliases))),
            when_clauses: case
                .when_clauses
                .iter()
                .map(|when| WhenClause {
                    token: when.token.clone(),
                    condition: substitute_aliases(&when.condition, aliases),
                    then_result: substitute_aliases(&when.then_result, aliases),
                })
                .collect(),
            else_value: case
                .else_value
                .as_ref()
                .map(|value| Box::new(substitute_aliases(value, aliases))),
        })),
        Expression::List(list) => Expression::List(Box::new(ListExpression {
            token: list.token.clone(),
            elements: list
                .elements
                .iter()
                .map(|item| substitute_aliases(item, aliases))
                .collect(),
        })),
        Expression::Like(like) => Expression::Like(LikeExpression {
            token: like.token.clone(),
            left: Box::new(substitute_aliases(&like.left, aliases)),
            pattern: Box::new(substitute_aliases(&like.pattern, aliases)),
            operator: like.operator.clone(),
            escape: like
                .escape
                .as_ref()
                .map(|value| Box::new(substitute_aliases(value, aliases))),
        }),
        _ => expression.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::{DataType, SchemaColumn};
    use radixdb_sql::parse_sql;

    #[test]
    fn source_column_shadows_same_named_output_alias() {
        let statements = parse_sql("SELECT a AS b FROM t WHERE b = 1").unwrap();
        let [Statement::Select(statement)] = statements.as_slice() else {
            panic!("expected SELECT");
        };
        let aliases =
            build_alias_map_excluding(&statement.columns, Some(&["a".into(), "b".into()]));
        assert!(aliases.is_empty());
    }

    #[test]
    fn predicate_without_where_has_no_physical_parts() {
        let schema = Schema::new(
            "t",
            vec![SchemaColumn::new(0, "id", DataType::Integer, false, true)],
        );
        let context = ExecutionContext::new();
        let functions = FunctionRegistry::new();
        let prepared = prepare_scan_predicate(
            &[],
            None,
            &["id".into()],
            &schema,
            Some("t"),
            &context,
            &functions,
            false,
        );
        assert!(prepared.effective().is_none());
        assert!(prepared.storage.is_none());
        assert!(!prepared.needs_memory_filter());
    }
}
