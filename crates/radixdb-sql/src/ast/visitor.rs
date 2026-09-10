// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

/// Walk every expression reachable from an expression, including subqueries,
/// table sources, function modifiers, window specifications, and frame bounds.
/// Parameter counting and semantic-cache classification share this owner so a
/// newly added AST edge cannot silently diverge between consumers.
#[doc(hidden)]
pub fn walk_expression_tree(expression: &Expression, visitor: &mut dyn FnMut(&Expression)) {
    visitor(expression);
    match expression {
        Expression::Prefix(value) => walk_expression_tree(&value.right, visitor),
        Expression::Infix(value) => {
            walk_expression_tree(&value.left, visitor);
            walk_expression_tree(&value.right, visitor);
        }
        Expression::List(value) => {
            for expression in &value.elements {
                walk_expression_tree(expression, visitor);
            }
        }
        Expression::Distinct(value) => walk_expression_tree(&value.expr, visitor),
        Expression::Exists(value) => walk_select_tree(&value.subquery, visitor),
        Expression::AllAny(value) => {
            walk_expression_tree(&value.left, visitor);
            walk_select_tree(&value.subquery, visitor);
        }
        Expression::In(value) => {
            walk_expression_tree(&value.left, visitor);
            walk_expression_tree(&value.right, visitor);
        }
        Expression::InHashSet(value) => walk_expression_tree(&value.column, visitor),
        Expression::Between(value) => {
            walk_expression_tree(&value.expr, visitor);
            walk_expression_tree(&value.lower, visitor);
            walk_expression_tree(&value.upper, visitor);
        }
        Expression::Like(value) => {
            walk_expression_tree(&value.left, visitor);
            walk_expression_tree(&value.pattern, visitor);
            if let Some(escape) = &value.escape {
                walk_expression_tree(escape, visitor);
            }
        }
        Expression::ScalarSubquery(value) => walk_select_tree(&value.subquery, visitor),
        Expression::ExpressionList(value) => {
            for expression in &value.expressions {
                walk_expression_tree(expression, visitor);
            }
        }
        Expression::Case(value) => {
            if let Some(expression) = &value.value {
                walk_expression_tree(expression, visitor);
            }
            for clause in &value.when_clauses {
                walk_expression_tree(&clause.condition, visitor);
                walk_expression_tree(&clause.then_result, visitor);
            }
            if let Some(expression) = &value.else_value {
                walk_expression_tree(expression, visitor);
            }
        }
        Expression::Cast(value) => walk_expression_tree(&value.expr, visitor),
        Expression::FunctionCall(value) => walk_function_tree(value, visitor),
        Expression::Aliased(value) => walk_expression_tree(&value.expression, visitor),
        Expression::Window(value) => {
            walk_function_tree(&value.function, visitor);
            for expression in &value.partition_by {
                walk_expression_tree(expression, visitor);
            }
            for order in &value.order_by {
                walk_expression_tree(&order.expression, visitor);
            }
            if let Some(frame) = &value.frame {
                walk_frame_tree(frame, visitor);
            }
        }
        Expression::TableSource(value) => {
            if let Some(as_of) = &value.as_of {
                walk_expression_tree(&as_of.value, visitor);
            }
        }
        Expression::JoinSource(value) => {
            walk_expression_tree(&value.left, visitor);
            walk_expression_tree(&value.right, visitor);
            if let Some(condition) = &value.condition {
                walk_expression_tree(condition, visitor);
            }
        }
        Expression::SubquerySource(value) => walk_select_tree(&value.subquery, visitor),
        Expression::ValuesSource(value) => {
            for row in &value.rows {
                for expression in row {
                    walk_expression_tree(expression, visitor);
                }
            }
        }
        Expression::FunctionTableSource(value) => {
            for expression in &value.arguments {
                walk_expression_tree(expression, visitor);
            }
        }
        Expression::Identifier(_)
        | Expression::QualifiedIdentifier(_)
        | Expression::IntegerLiteral(_)
        | Expression::FloatLiteral(_)
        | Expression::StringLiteral(_)
        | Expression::BooleanLiteral(_)
        | Expression::NullLiteral(_)
        | Expression::IntervalLiteral(_)
        | Expression::BoundValue(_)
        | Expression::Parameter(_)
        | Expression::CteReference(_)
        | Expression::Star(_)
        | Expression::QualifiedStar(_)
        | Expression::Default(_) => {}
    }
}

/// Mutably walk every expression reachable from an expression.
///
/// The callback runs after children so callers may replace the current node
/// without hiding a subtree from the walk. This is the shared rewrite owner
/// for bind-time transformations such as stored-routine local parameters.
#[doc(hidden)]
pub fn walk_expression_tree_mut(
    expression: &mut Expression,
    visitor: &mut dyn FnMut(&mut Expression),
) {
    match expression {
        Expression::Prefix(value) => walk_expression_tree_mut(&mut value.right, visitor),
        Expression::Infix(value) => {
            walk_expression_tree_mut(&mut value.left, visitor);
            walk_expression_tree_mut(&mut value.right, visitor);
        }
        Expression::List(value) => {
            for expression in &mut value.elements {
                walk_expression_tree_mut(expression, visitor);
            }
        }
        Expression::Distinct(value) => walk_expression_tree_mut(&mut value.expr, visitor),
        Expression::Exists(value) => walk_select_tree_mut(&mut value.subquery, visitor),
        Expression::AllAny(value) => {
            walk_expression_tree_mut(&mut value.left, visitor);
            walk_select_tree_mut(&mut value.subquery, visitor);
        }
        Expression::In(value) => {
            walk_expression_tree_mut(&mut value.left, visitor);
            walk_expression_tree_mut(&mut value.right, visitor);
        }
        Expression::InHashSet(value) => walk_expression_tree_mut(&mut value.column, visitor),
        Expression::Between(value) => {
            walk_expression_tree_mut(&mut value.expr, visitor);
            walk_expression_tree_mut(&mut value.lower, visitor);
            walk_expression_tree_mut(&mut value.upper, visitor);
        }
        Expression::Like(value) => {
            walk_expression_tree_mut(&mut value.left, visitor);
            walk_expression_tree_mut(&mut value.pattern, visitor);
            if let Some(escape) = &mut value.escape {
                walk_expression_tree_mut(escape, visitor);
            }
        }
        Expression::ScalarSubquery(value) => {
            walk_select_tree_mut(&mut value.subquery, visitor);
        }
        Expression::ExpressionList(value) => {
            for expression in &mut value.expressions {
                walk_expression_tree_mut(expression, visitor);
            }
        }
        Expression::Case(value) => {
            if let Some(expression) = &mut value.value {
                walk_expression_tree_mut(expression, visitor);
            }
            for clause in &mut value.when_clauses {
                walk_expression_tree_mut(&mut clause.condition, visitor);
                walk_expression_tree_mut(&mut clause.then_result, visitor);
            }
            if let Some(expression) = &mut value.else_value {
                walk_expression_tree_mut(expression, visitor);
            }
        }
        Expression::Cast(value) => walk_expression_tree_mut(&mut value.expr, visitor),
        Expression::FunctionCall(value) => walk_function_tree_mut(value, visitor),
        Expression::Aliased(value) => {
            walk_expression_tree_mut(&mut value.expression, visitor);
        }
        Expression::Window(value) => {
            walk_function_tree_mut(&mut value.function, visitor);
            for expression in &mut value.partition_by {
                walk_expression_tree_mut(expression, visitor);
            }
            for order in &mut value.order_by {
                walk_expression_tree_mut(&mut order.expression, visitor);
            }
            if let Some(frame) = &mut value.frame {
                walk_frame_tree_mut(frame, visitor);
            }
        }
        Expression::TableSource(value) => {
            if let Some(as_of) = &mut value.as_of {
                walk_expression_tree_mut(&mut as_of.value, visitor);
            }
        }
        Expression::JoinSource(value) => {
            walk_expression_tree_mut(&mut value.left, visitor);
            walk_expression_tree_mut(&mut value.right, visitor);
            if let Some(condition) = &mut value.condition {
                walk_expression_tree_mut(condition, visitor);
            }
        }
        Expression::SubquerySource(value) => {
            walk_select_tree_mut(&mut value.subquery, visitor);
        }
        Expression::ValuesSource(value) => {
            for row in &mut value.rows {
                for expression in row {
                    walk_expression_tree_mut(expression, visitor);
                }
            }
        }
        Expression::FunctionTableSource(value) => {
            for expression in &mut value.arguments {
                walk_expression_tree_mut(expression, visitor);
            }
        }
        Expression::Identifier(_)
        | Expression::QualifiedIdentifier(_)
        | Expression::IntegerLiteral(_)
        | Expression::FloatLiteral(_)
        | Expression::StringLiteral(_)
        | Expression::BooleanLiteral(_)
        | Expression::NullLiteral(_)
        | Expression::IntervalLiteral(_)
        | Expression::BoundValue(_)
        | Expression::Parameter(_)
        | Expression::CteReference(_)
        | Expression::Star(_)
        | Expression::QualifiedStar(_)
        | Expression::Default(_) => {}
    }
    visitor(expression);
}

fn walk_function_tree(function: &FunctionCall, visitor: &mut dyn FnMut(&Expression)) {
    for expression in &function.arguments {
        walk_expression_tree(expression, visitor);
    }
    for order in &function.order_by {
        walk_expression_tree(&order.expression, visitor);
    }
    if let Some(filter) = &function.filter {
        walk_expression_tree(filter, visitor);
    }
}

fn walk_function_tree_mut(function: &mut FunctionCall, visitor: &mut dyn FnMut(&mut Expression)) {
    for expression in &mut function.arguments {
        walk_expression_tree_mut(expression, visitor);
    }
    for order in &mut function.order_by {
        walk_expression_tree_mut(&mut order.expression, visitor);
    }
    if let Some(filter) = &mut function.filter {
        walk_expression_tree_mut(filter, visitor);
    }
}

fn walk_frame_tree(frame: &WindowFrame, visitor: &mut dyn FnMut(&Expression)) {
    let mut walk_bound = |bound: &WindowFrameBound| match bound {
        WindowFrameBound::Preceding(expression) | WindowFrameBound::Following(expression) => {
            walk_expression_tree(expression, visitor);
        }
        _ => {}
    };
    walk_bound(&frame.start);
    if let Some(end) = &frame.end {
        walk_bound(end);
    }
}

fn walk_frame_tree_mut(frame: &mut WindowFrame, visitor: &mut dyn FnMut(&mut Expression)) {
    let mut walk_bound = |bound: &mut WindowFrameBound| match bound {
        WindowFrameBound::Preceding(expression) | WindowFrameBound::Following(expression) => {
            walk_expression_tree_mut(expression, visitor);
        }
        _ => {}
    };
    walk_bound(&mut frame.start);
    if let Some(end) = &mut frame.end {
        walk_bound(end);
    }
}

#[doc(hidden)]
pub fn walk_select_tree(select: &SelectStatement, visitor: &mut dyn FnMut(&Expression)) {
    if let Some(with) = &select.with {
        for cte in &with.ctes {
            walk_select_tree(&cte.query, visitor);
        }
    }
    for expression in select.distinct_on.iter().chain(&select.columns) {
        walk_expression_tree(expression, visitor);
    }
    if let Some(expression) = &select.table_expr {
        walk_expression_tree(expression, visitor);
    }
    if let Some(expression) = &select.where_clause {
        walk_expression_tree(expression, visitor);
    }
    for expression in &select.group_by.columns {
        walk_expression_tree(expression, visitor);
    }
    if let GroupByModifier::GroupingSets(sets) = &select.group_by.modifier {
        for set in sets {
            for expression in set {
                walk_expression_tree(expression, visitor);
            }
        }
    }
    if let Some(expression) = &select.having {
        walk_expression_tree(expression, visitor);
    }
    for window in &select.window_defs {
        for expression in &window.partition_by {
            walk_expression_tree(expression, visitor);
        }
        for order in &window.order_by {
            walk_expression_tree(&order.expression, visitor);
        }
        if let Some(frame) = &window.frame {
            walk_frame_tree(frame, visitor);
        }
    }
    for order in &select.order_by {
        walk_expression_tree(&order.expression, visitor);
    }
    if let Some(expression) = &select.limit {
        walk_expression_tree(expression, visitor);
    }
    if let Some(expression) = &select.offset {
        walk_expression_tree(expression, visitor);
    }
    for operation in &select.set_operations {
        walk_select_tree(&operation.right, visitor);
    }
}

#[doc(hidden)]
pub fn walk_select_tree_mut(
    select: &mut SelectStatement,
    visitor: &mut dyn FnMut(&mut Expression),
) {
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            walk_select_tree_mut(&mut cte.query, visitor);
        }
    }
    for expression in &mut select.distinct_on {
        walk_expression_tree_mut(expression, visitor);
    }
    for expression in &mut select.columns {
        walk_expression_tree_mut(expression, visitor);
    }
    if let Some(expression) = &mut select.table_expr {
        walk_expression_tree_mut(expression, visitor);
    }
    if let Some(expression) = &mut select.where_clause {
        walk_expression_tree_mut(expression, visitor);
    }
    for expression in &mut select.group_by.columns {
        walk_expression_tree_mut(expression, visitor);
    }
    if let GroupByModifier::GroupingSets(sets) = &mut select.group_by.modifier {
        for set in sets {
            for expression in set {
                walk_expression_tree_mut(expression, visitor);
            }
        }
    }
    if let Some(expression) = &mut select.having {
        walk_expression_tree_mut(expression, visitor);
    }
    for window in &mut select.window_defs {
        for expression in &mut window.partition_by {
            walk_expression_tree_mut(expression, visitor);
        }
        for order in &mut window.order_by {
            walk_expression_tree_mut(&mut order.expression, visitor);
        }
        if let Some(frame) = &mut window.frame {
            walk_frame_tree_mut(frame, visitor);
        }
    }
    for order in &mut select.order_by {
        walk_expression_tree_mut(&mut order.expression, visitor);
    }
    if let Some(expression) = &mut select.limit {
        walk_expression_tree_mut(expression, visitor);
    }
    if let Some(expression) = &mut select.offset {
        walk_expression_tree_mut(expression, visitor);
    }
    for operation in &mut select.set_operations {
        walk_select_tree_mut(&mut operation.right, visitor);
    }
}

/// Walk only physical table sources while respecting lexical CTE scopes.
///
/// The general expression walker deliberately reports the parser's raw table
/// source nodes. Before name binding, a CTE reference may still be represented
/// as a `TableSource`, so persisted dependency graphs need this scoped walker
/// to avoid treating a CTE alias as a durable catalog dependency.
#[doc(hidden)]
pub fn walk_physical_table_sources(
    select: &SelectStatement,
    visitor: &mut dyn FnMut(&SimpleTableSource),
) {
    walk_physical_table_sources_scoped(select, &FxHashSet::default(), visitor);
}

/// Walk physical relation sources nested inside a query or DML statement.
/// The direct DML target is intentionally not synthesized as an expression;
/// callers bind that explicit identifier separately.
#[doc(hidden)]
pub fn walk_statement_physical_table_sources(
    statement: &Statement,
    visitor: &mut dyn FnMut(&SimpleTableSource),
) {
    let visible_ctes = FxHashSet::default();
    match statement {
        Statement::Select(select) => walk_physical_table_sources(select, visitor),
        Statement::Insert(insert) => {
            for row in &insert.values {
                for expression in row {
                    walk_physical_sources_in_expression(expression, &visible_ctes, visitor);
                }
            }
            if let Some(select) = &insert.select {
                walk_physical_table_sources(select, visitor);
            }
            for expression in insert.update_expressions.iter().chain(&insert.returning) {
                walk_physical_sources_in_expression(expression, &visible_ctes, visitor);
            }
        }
        Statement::Update(update) => {
            for expression in update.updates.values() {
                walk_physical_sources_in_expression(expression, &visible_ctes, visitor);
            }
            if let Some(expression) = &update.where_clause {
                walk_physical_sources_in_expression(expression, &visible_ctes, visitor);
            }
            for expression in &update.returning {
                walk_physical_sources_in_expression(expression, &visible_ctes, visitor);
            }
        }
        Statement::Delete(delete) => {
            if let Some(expression) = &delete.where_clause {
                walk_physical_sources_in_expression(expression, &visible_ctes, visitor);
            }
            for expression in &delete.returning {
                walk_physical_sources_in_expression(expression, &visible_ctes, visitor);
            }
        }
        Statement::Call(call) => {
            for argument in &call.arguments {
                walk_physical_sources_in_expression(&argument.value, &visible_ctes, visitor);
            }
        }
        _ => {}
    }
}

fn walk_physical_table_sources_scoped(
    select: &SelectStatement,
    inherited_ctes: &FxHashSet<SmartString>,
    visitor: &mut dyn FnMut(&SimpleTableSource),
) {
    let mut visible_ctes = inherited_ctes.clone();
    if let Some(with) = &select.with {
        if with.is_recursive {
            visible_ctes.extend(with.ctes.iter().map(|cte| cte.name.value_lower.clone()));
            for cte in &with.ctes {
                walk_physical_table_sources_scoped(&cte.query, &visible_ctes, visitor);
            }
        } else {
            for cte in &with.ctes {
                walk_physical_table_sources_scoped(&cte.query, &visible_ctes, visitor);
                visible_ctes.insert(cte.name.value_lower.clone());
            }
        }
    }

    macro_rules! walk {
        ($expression:expr) => {
            walk_physical_sources_in_expression($expression, &visible_ctes, visitor)
        };
    }
    for expression in select.distinct_on.iter().chain(&select.columns) {
        walk!(expression);
    }
    if let Some(expression) = &select.table_expr {
        walk!(expression);
    }
    if let Some(expression) = &select.where_clause {
        walk!(expression);
    }
    for expression in &select.group_by.columns {
        walk!(expression);
    }
    if let GroupByModifier::GroupingSets(sets) = &select.group_by.modifier {
        for set in sets {
            for expression in set {
                walk!(expression);
            }
        }
    }
    if let Some(expression) = &select.having {
        walk!(expression);
    }
    for window in &select.window_defs {
        for expression in &window.partition_by {
            walk!(expression);
        }
        for order in &window.order_by {
            walk!(&order.expression);
        }
        if let Some(frame) = &window.frame {
            walk_physical_sources_in_frame(frame, &visible_ctes, visitor);
        }
    }
    for order in &select.order_by {
        walk!(&order.expression);
    }
    if let Some(expression) = &select.limit {
        walk!(expression);
    }
    if let Some(expression) = &select.offset {
        walk!(expression);
    }
    for operation in &select.set_operations {
        walk_physical_table_sources_scoped(&operation.right, &visible_ctes, visitor);
    }
}

fn walk_physical_sources_in_expression(
    expression: &Expression,
    visible_ctes: &FxHashSet<SmartString>,
    visitor: &mut dyn FnMut(&SimpleTableSource),
) {
    macro_rules! walk {
        ($expression:expr) => {
            walk_physical_sources_in_expression($expression, visible_ctes, visitor)
        };
    }
    match expression {
        Expression::Prefix(value) => walk!(&value.right),
        Expression::Infix(value) => {
            walk!(&value.left);
            walk!(&value.right);
        }
        Expression::List(value) => {
            for expression in &value.elements {
                walk!(expression);
            }
        }
        Expression::Distinct(value) => walk!(&value.expr),
        Expression::Exists(value) => {
            walk_physical_table_sources_scoped(&value.subquery, visible_ctes, visitor)
        }
        Expression::AllAny(value) => {
            walk!(&value.left);
            walk_physical_table_sources_scoped(&value.subquery, visible_ctes, visitor);
        }
        Expression::In(value) => {
            walk!(&value.left);
            walk!(&value.right);
        }
        Expression::InHashSet(value) => walk!(&value.column),
        Expression::Between(value) => {
            walk!(&value.expr);
            walk!(&value.lower);
            walk!(&value.upper);
        }
        Expression::Like(value) => {
            walk!(&value.left);
            walk!(&value.pattern);
            if let Some(escape) = &value.escape {
                walk!(escape);
            }
        }
        Expression::ScalarSubquery(value) => {
            walk_physical_table_sources_scoped(&value.subquery, visible_ctes, visitor)
        }
        Expression::ExpressionList(value) => {
            for expression in &value.expressions {
                walk!(expression);
            }
        }
        Expression::Case(value) => {
            if let Some(expression) = &value.value {
                walk!(expression);
            }
            for clause in &value.when_clauses {
                walk!(&clause.condition);
                walk!(&clause.then_result);
            }
            if let Some(expression) = &value.else_value {
                walk!(expression);
            }
        }
        Expression::Cast(value) => walk!(&value.expr),
        Expression::FunctionCall(value) => {
            for expression in &value.arguments {
                walk!(expression);
            }
            for order in &value.order_by {
                walk!(&order.expression);
            }
            if let Some(filter) = &value.filter {
                walk!(filter);
            }
        }
        Expression::Aliased(value) => walk!(&value.expression),
        Expression::Window(value) => {
            for expression in &value.function.arguments {
                walk!(expression);
            }
            for order in &value.function.order_by {
                walk!(&order.expression);
            }
            if let Some(filter) = &value.function.filter {
                walk!(filter);
            }
            for expression in &value.partition_by {
                walk!(expression);
            }
            for order in &value.order_by {
                walk!(&order.expression);
            }
            if let Some(frame) = &value.frame {
                walk_physical_sources_in_frame(frame, visible_ctes, visitor);
            }
        }
        Expression::TableSource(value) => {
            if !visible_ctes.contains(&value.name.value_lower) {
                visitor(value);
            }
            if let Some(as_of) = &value.as_of {
                walk!(&as_of.value);
            }
        }
        Expression::JoinSource(value) => {
            walk!(&value.left);
            walk!(&value.right);
            if let Some(condition) = &value.condition {
                walk!(condition);
            }
        }
        Expression::SubquerySource(value) => {
            walk_physical_table_sources_scoped(&value.subquery, visible_ctes, visitor)
        }
        Expression::ValuesSource(value) => {
            for row in &value.rows {
                for expression in row {
                    walk!(expression);
                }
            }
        }
        Expression::FunctionTableSource(value) => {
            for expression in &value.arguments {
                walk!(expression);
            }
        }
        Expression::Identifier(_)
        | Expression::QualifiedIdentifier(_)
        | Expression::IntegerLiteral(_)
        | Expression::FloatLiteral(_)
        | Expression::StringLiteral(_)
        | Expression::BooleanLiteral(_)
        | Expression::NullLiteral(_)
        | Expression::IntervalLiteral(_)
        | Expression::BoundValue(_)
        | Expression::Parameter(_)
        | Expression::CteReference(_)
        | Expression::Star(_)
        | Expression::QualifiedStar(_)
        | Expression::Default(_) => {}
    }
}

fn walk_physical_sources_in_frame(
    frame: &WindowFrame,
    visible_ctes: &FxHashSet<SmartString>,
    visitor: &mut dyn FnMut(&SimpleTableSource),
) {
    let mut walk_bound = |bound: &WindowFrameBound| match bound {
        WindowFrameBound::Preceding(expression) | WindowFrameBound::Following(expression) => {
            walk_physical_sources_in_expression(expression, visible_ctes, visitor)
        }
        _ => {}
    };
    walk_bound(&frame.start);
    if let Some(end) = &frame.end {
        walk_bound(end);
    }
}

#[doc(hidden)]
pub fn walk_statement_tree(statement: &Statement, visitor: &mut dyn FnMut(&Expression)) {
    match statement {
        Statement::Select(select) => walk_select_tree(select, visitor),
        Statement::Insert(insert) => {
            for row in &insert.values {
                for expression in row {
                    walk_expression_tree(expression, visitor);
                }
            }
            if let Some(select) = &insert.select {
                walk_select_tree(select, visitor);
            }
            for expression in insert.update_expressions.iter().chain(&insert.returning) {
                walk_expression_tree(expression, visitor);
            }
        }
        Statement::Update(update) => {
            for expression in update.updates.values() {
                walk_expression_tree(expression, visitor);
            }
            if let Some(expression) = &update.where_clause {
                walk_expression_tree(expression, visitor);
            }
            for expression in &update.returning {
                walk_expression_tree(expression, visitor);
            }
        }
        Statement::Delete(delete) => {
            if let Some(expression) = &delete.where_clause {
                walk_expression_tree(expression, visitor);
            }
            for expression in &delete.returning {
                walk_expression_tree(expression, visitor);
            }
        }
        Statement::Call(call) => {
            for argument in &call.arguments {
                walk_expression_tree(&argument.value, visitor);
            }
        }
        Statement::CreateTable(create) => {
            for column in &create.columns {
                for constraint in &column.constraints {
                    match constraint {
                        ColumnConstraint::Default(expression)
                        | ColumnConstraint::Check(expression) => {
                            walk_expression_tree(expression, visitor);
                        }
                        _ => {}
                    }
                }
            }
            for constraint in &create.table_constraints {
                if let TableConstraint::Check(expression) = constraint {
                    walk_expression_tree(expression, visitor);
                }
            }
            if let Some(select) = &create.as_select {
                walk_select_tree(select, visitor);
            }
        }
        Statement::AlterTable(alter) => {
            if let Some(column) = &alter.column_def {
                for constraint in &column.constraints {
                    match constraint {
                        ColumnConstraint::Default(expression)
                        | ColumnConstraint::Check(expression) => {
                            walk_expression_tree(expression, visitor);
                        }
                        _ => {}
                    }
                }
            }
            if let Some(TableConstraint::Check(expression)) = &alter.table_constraint {
                walk_expression_tree(expression, visitor);
            }
        }
        Statement::CreateIndex(create) => {
            for (_, expression) in &create.options {
                walk_expression_tree(expression, visitor);
            }
            if let Some(expression) = &create.where_clause {
                walk_expression_tree(expression, visitor);
            }
        }
        Statement::CreateView(create) => walk_select_tree(&create.query, visitor),
        Statement::Set(set) => walk_expression_tree(&set.value, visitor),
        Statement::Pragma(pragma) => {
            if let Some(expression) = &pragma.value {
                walk_expression_tree(expression, visitor);
            }
        }
        Statement::Expression(expression) => walk_expression_tree(&expression.expression, visitor),
        Statement::Explain(explain) => walk_statement_tree(&explain.statement, visitor),
        _ => {}
    }
}

/// Mutably walk every expression reachable from the SQL statement kinds that
/// may be embedded in procedural code.
#[doc(hidden)]
pub fn walk_statement_tree_mut(
    statement: &mut Statement,
    visitor: &mut dyn FnMut(&mut Expression),
) {
    match statement {
        Statement::Select(select) => walk_select_tree_mut(select, visitor),
        Statement::Insert(insert) => {
            for row in &mut insert.values {
                for expression in row {
                    walk_expression_tree_mut(expression, visitor);
                }
            }
            if let Some(select) = &mut insert.select {
                walk_select_tree_mut(select, visitor);
            }
            for expression in &mut insert.update_expressions {
                walk_expression_tree_mut(expression, visitor);
            }
            for expression in &mut insert.returning {
                walk_expression_tree_mut(expression, visitor);
            }
        }
        Statement::Update(update) => {
            for expression in update.updates.values_mut() {
                walk_expression_tree_mut(expression, visitor);
            }
            if let Some(expression) = &mut update.where_clause {
                walk_expression_tree_mut(expression, visitor);
            }
            for expression in &mut update.returning {
                walk_expression_tree_mut(expression, visitor);
            }
        }
        Statement::Delete(delete) => {
            if let Some(expression) = &mut delete.where_clause {
                walk_expression_tree_mut(expression, visitor);
            }
            for expression in &mut delete.returning {
                walk_expression_tree_mut(expression, visitor);
            }
        }
        Statement::Call(call) => {
            for argument in &mut call.arguments {
                walk_expression_tree_mut(&mut argument.value, visitor);
            }
        }
        _ => {}
    }
}
