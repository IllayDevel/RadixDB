pub(super) fn select_has_correlated_navigation(
    select: &SelectStatement,
    plan: &ReferenceExpandPlan,
) -> bool {
    let mut found = false;
    radixdb_sql::ast::walk_select_tree(select, &mut |expression| {
        if found {
            return;
        }
        if matches!(
            expression,
            Expression::Exists(_) | Expression::AllAny(_) | Expression::ScalarSubquery(_)
        ) && expression_contains_bound_navigation(expression, plan)
        {
            found = true;
        }
    });
    found
}

pub(super) fn retain_correlated_navigation_columns(
    select: &mut SelectStatement,
    plan: &ReferenceExpandPlan,
    engine: &dyn Engine,
    aliases: &[Identifier],
) -> Result<()> {
    let mut seen = FxHashSet::default();
    let mut predicates = Vec::new();
    for path in &plan.paths {
        let edge_index = *path
            .edge_indices
            .last()
            .ok_or_else(|| Error::internal("ReferenceExpand path has no edge"))?;
        let terminal = column_name(engine, &path.identity.terminal_column)?;
        if !seen.insert((edge_index, terminal.clone())) {
            continue;
        }
        let token = select.token.clone();
        let column = qualified_column(&token, aliases[edge_index].clone(), terminal);
        let null = Expression::NullLiteral(NullLiteral {
            token: token.clone(),
        });
        let is_null = Expression::Infix(InfixExpression::new(
            token.clone(),
            Box::new(column.clone()),
            "IS",
            Box::new(null.clone()),
        ));
        let is_not_null = Expression::Infix(InfixExpression::new(
            token.clone(),
            Box::new(column),
            "IS NOT",
            Box::new(null),
        ));
        predicates.push(Expression::Infix(InfixExpression::new(
            token,
            Box::new(is_null),
            "OR",
            Box::new(is_not_null),
        )));
    }
    if let Some(existing) = select.where_clause.take() {
        predicates.insert(0, *existing);
    }
    select.where_clause = combine_predicates_with_and(predicates).map(Box::new);
    Ok(())
}

pub(super) fn reference_edge_aliases(
    source: &Expression,
    plan: &ReferenceExpandPlan,
    token: &radixdb_sql::Token,
) -> Vec<Identifier> {
    let mut occupied = FxHashSet::default();
    collect_relation_names(source, &mut occupied);
    plan.edges
        .iter()
        .enumerate()
        .map(|(edge_index, _)| {
            let mut suffix = 0usize;
            loop {
                let candidate = if suffix == 0 {
                    format!("__radix_nav_{}_{}", plan.schema_scope_id, edge_index)
                } else {
                    format!(
                        "__radix_nav_{}_{}_{}",
                        plan.schema_scope_id, edge_index, suffix
                    )
                };
                if occupied.insert(candidate.to_lowercase()) {
                    break Identifier::new(token.clone(), candidate);
                }
                suffix = suffix.saturating_add(1);
            }
        })
        .collect()
}

pub(super) fn collect_relation_names(source: &Expression, output: &mut FxHashSet<String>) {
    match source {
        Expression::TableSource(table) => {
            output.insert(
                table
                    .alias
                    .as_ref()
                    .unwrap_or(&table.name)
                    .value_lower()
                    .to_string(),
            );
        }
        Expression::JoinSource(join) => {
            collect_relation_names(&join.left, output);
            collect_relation_names(&join.right, output);
        }
        Expression::SubquerySource(subquery) => {
            if let Some(alias) = &subquery.alias {
                output.insert(alias.value_lower().to_string());
            }
        }
        Expression::CteReference(cte) => {
            output.insert(
                cte.alias
                    .as_ref()
                    .unwrap_or(&cte.name)
                    .value_lower()
                    .to_string(),
            );
        }
        Expression::ValuesSource(values) => {
            if let Some(alias) = &values.alias {
                output.insert(alias.value_lower().to_string());
            }
        }
        Expression::FunctionTableSource(function) => {
            if let Some(alias) = &function.alias {
                output.insert(alias.value_lower().to_string());
            }
        }
        _ => {}
    }
}

pub(super) fn reference_parent_edges(plan: &ReferenceExpandPlan) -> Result<Vec<Option<usize>>> {
    let mut by_identity = FxHashMap::default();
    for (edge_index, edge) in plan.edges.iter().enumerate() {
        by_identity.insert(edge.identity.clone(), edge_index);
    }
    plan.edges
        .iter()
        .map(|edge| {
            if edge.identity.steps.len() == 1 {
                return Ok(None);
            }
            let parent = ReferenceExpandEdgeIdentity {
                root: edge.identity.root.clone(),
                steps: edge.identity.steps[..edge.identity.steps.len() - 1].to_vec(),
            };
            by_identity
                .get(&parent)
                .copied()
                .map(Some)
                .ok_or_else(|| Error::internal("ReferenceExpand graph is missing a parent edge"))
        })
        .collect()
}

pub(super) fn attach_reference_edges_to_sources(
    source: Expression,
    plan: &ReferenceExpandPlan,
    engine: &dyn Engine,
    aliases: &[Identifier],
    parent_edges: &[Option<usize>],
    relation_ordinal: &mut u32,
) -> Result<Expression> {
    match source {
        Expression::JoinSource(mut join) => {
            join.left = Box::new(attach_reference_edges_to_sources(
                *join.left,
                plan,
                engine,
                aliases,
                parent_edges,
                relation_ordinal,
            )?);
            join.right = Box::new(attach_reference_edges_to_sources(
                *join.right,
                plan,
                engine,
                aliases,
                parent_edges,
                relation_ordinal,
            )?);
            Ok(Expression::JoinSource(join))
        }
        Expression::TableSource(table) => {
            let ordinal = *relation_ordinal;
            *relation_ordinal = relation_ordinal.checked_add(1).ok_or_else(|| {
                Error::navigation(
                    NavigationErrorCode::UnsupportedReferenceShape,
                    "query contains too many relation instances",
                )
            })?;
            let root_alias = table.alias.as_ref().unwrap_or(&table.name).clone();
            let token = table.token.clone();
            let mut attached = Expression::TableSource(table);
            for (edge_index, edge) in plan.edges.iter().enumerate() {
                if edge.identity.root.ordinal != ordinal {
                    continue;
                }
                let source_alias = parent_edges[edge_index]
                    .map(|parent| aliases[parent].clone())
                    .unwrap_or_else(|| root_alias.clone());
                let source_column = column_name(engine, &edge.source_column)?;
                let target_column = column_name(engine, &edge.target_key_column)?;
                let left = qualified_column(&token, source_alias, source_column);
                let right = qualified_column(&token, aliases[edge_index].clone(), target_column);
                let condition = Expression::Infix(InfixExpression::new(
                    token.clone(),
                    Box::new(left),
                    "=",
                    Box::new(right),
                ));
                let target = Expression::TableSource(Box::new(SimpleTableSource {
                    token: token.clone(),
                    name: Identifier::new(
                        token.clone(),
                        edge.target_key_column.table().table_name(),
                    ),
                    alias: Some(aliases[edge_index].clone()),
                    as_of: None,
                }));
                attached = Expression::JoinSource(Box::new(JoinTableSource {
                    token: token.clone(),
                    left: Box::new(attached),
                    join_type: "LEFT".into(),
                    right: Box::new(target),
                    condition: Some(Box::new(condition)),
                    using_columns: Vec::new(),
                }));
            }
            Ok(attached)
        }
        other => {
            if matches!(
                other,
                Expression::SubquerySource(_)
                    | Expression::CteReference(_)
                    | Expression::ValuesSource(_)
                    | Expression::FunctionTableSource(_)
            ) {
                *relation_ordinal = relation_ordinal.checked_add(1).ok_or_else(|| {
                    Error::navigation(
                        NavigationErrorCode::UnsupportedReferenceShape,
                        "query contains too many relation instances",
                    )
                })?;
            }
            Ok(other)
        }
    }
}

pub(super) fn qualified_column(
    token: &radixdb_sql::Token,
    qualifier: Identifier,
    column: impl Into<String>,
) -> Expression {
    Expression::QualifiedIdentifier(QualifiedIdentifier {
        token: token.clone(),
        qualifier: Box::new(qualifier),
        intermediate: None,
        name: Box::new(Identifier::new(token.clone(), column.into())),
    })
}

pub(super) fn add_canonical_navigation_result_aliases(
    columns: &mut [Expression],
    plan: &ReferenceExpandPlan,
) -> Result<()> {
    for expression in columns {
        let Expression::QualifiedIdentifier(path) = expression else {
            continue;
        };
        if plan.path_index_for_display(&path.to_string()).is_none() {
            continue;
        }
        let alias = Identifier::new(path.token.clone(), path.to_string());
        *expression = Expression::Aliased(radixdb_sql::ast::AliasedExpression {
            token: path.token.clone(),
            expression: Box::new(Expression::QualifiedIdentifier(path.clone())),
            alias,
        });
    }
    Ok(())
}

pub(super) fn rewrite_select_navigation_current_scope(
    select: &mut SelectStatement,
    plan: &ReferenceExpandPlan,
    engine: &dyn Engine,
    aliases: &[Identifier],
) -> Result<()> {
    let replacements = plan
        .paths
        .iter()
        .map(|path| {
            let edge_index = *path
                .edge_indices
                .last()
                .ok_or_else(|| Error::internal("ReferenceExpand path has no edge"))?;
            let terminal = column_name(engine, &path.identity.terminal_column)?;
            Ok((
                path.display_paths.clone(),
                aliases[edge_index].clone(),
                terminal,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let rewrite = &mut |expression: &mut Expression| {
        let Expression::QualifiedIdentifier(identifier) = expression else {
            return Ok(());
        };
        let display = identifier.to_string();
        let Some((_, alias, terminal)) = replacements
            .iter()
            .find(|(displays, _, _)| displays.iter().any(|item| item == &display))
        else {
            return Ok(());
        };
        *expression = qualified_column(&identifier.token, alias.clone(), terminal.clone());
        Ok(())
    };

    for expression in select.distinct_on.iter_mut().chain(&mut select.columns) {
        walk_expression_tree_mut(expression, rewrite)?;
    }
    if let Some(expression) = select.where_clause.as_deref_mut() {
        walk_expression_tree_mut(expression, rewrite)?;
    }
    for expression in &mut select.group_by.columns {
        walk_expression_tree_mut(expression, rewrite)?;
    }
    if let GroupByModifier::GroupingSets(sets) = &mut select.group_by.modifier {
        for expression in sets.iter_mut().flatten() {
            walk_expression_tree_mut(expression, rewrite)?;
        }
    }
    if let Some(expression) = select.having.as_deref_mut() {
        walk_expression_tree_mut(expression, rewrite)?;
    }
    for window in &mut select.window_defs {
        for expression in &mut window.partition_by {
            walk_expression_tree_mut(expression, rewrite)?;
        }
        for order in &mut window.order_by {
            walk_expression_tree_mut(&mut order.expression, rewrite)?;
        }
    }
    for order in &mut select.order_by {
        walk_expression_tree_mut(&mut order.expression, rewrite)?;
    }
    rewrite_navigation_in_source_conditions(select.table_expr.as_deref_mut(), rewrite)?;
    rewrite_nested_navigation_selects(select, rewrite)?;
    Ok(())
}

pub(super) fn rewrite_nested_navigation_selects(
    select: &mut SelectStatement,
    rewrite: &mut impl FnMut(&mut Expression) -> Result<()>,
) -> Result<()> {
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            rewrite_navigation_select_recursive(&mut cte.query, rewrite)?;
        }
    }
    for expression in select.distinct_on.iter_mut().chain(&mut select.columns) {
        rewrite_nested_navigation_in_expression(expression, rewrite)?;
    }
    if let Some(expression) = select.where_clause.as_deref_mut() {
        rewrite_nested_navigation_in_expression(expression, rewrite)?;
    }
    for expression in &mut select.group_by.columns {
        rewrite_nested_navigation_in_expression(expression, rewrite)?;
    }
    if let GroupByModifier::GroupingSets(sets) = &mut select.group_by.modifier {
        for expression in sets.iter_mut().flatten() {
            rewrite_nested_navigation_in_expression(expression, rewrite)?;
        }
    }
    if let Some(expression) = select.having.as_deref_mut() {
        rewrite_nested_navigation_in_expression(expression, rewrite)?;
    }
    for window in &mut select.window_defs {
        for expression in &mut window.partition_by {
            rewrite_nested_navigation_in_expression(expression, rewrite)?;
        }
        for order in &mut window.order_by {
            rewrite_nested_navigation_in_expression(&mut order.expression, rewrite)?;
        }
    }
    for order in &mut select.order_by {
        rewrite_nested_navigation_in_expression(&mut order.expression, rewrite)?;
    }
    if let Some(expression) = select.limit.as_deref_mut() {
        rewrite_nested_navigation_in_expression(expression, rewrite)?;
    }
    if let Some(expression) = select.offset.as_deref_mut() {
        rewrite_nested_navigation_in_expression(expression, rewrite)?;
    }
    rewrite_nested_navigation_in_source(select.table_expr.as_deref_mut(), rewrite)?;
    Ok(())
}

pub(super) fn rewrite_navigation_select_recursive(
    select: &mut SelectStatement,
    rewrite: &mut impl FnMut(&mut Expression) -> Result<()>,
) -> Result<()> {
    let mut local_relations = FxHashSet::default();
    if let Some(source) = select.table_expr.as_deref() {
        collect_relation_names(source, &mut local_relations);
    }
    let has_local_source = select.table_expr.is_some();
    {
        let mut rewrite_current_scope = |expression: &mut Expression| {
            if let Expression::QualifiedIdentifier(identifier) = expression {
                // SQL lexical scoping wins over an outer navigation path. A
                // two-component path (`reference.field`) binds to a local root
                // whenever this SELECT owns one; an explicitly rooted path is
                // local when its visible relation name is shadowed here.
                let shadows_outer = if identifier.component_count() == 2 {
                    has_local_source
                } else {
                    local_relations.contains(identifier.qualifier.value_lower())
                };
                if shadows_outer {
                    return Ok(());
                }
            }
            rewrite(expression)
        };

        for expression in select.distinct_on.iter_mut().chain(&mut select.columns) {
            walk_expression_tree_mut(expression, &mut rewrite_current_scope)?;
        }
        if let Some(expression) = select.where_clause.as_deref_mut() {
            walk_expression_tree_mut(expression, &mut rewrite_current_scope)?;
        }
        for expression in &mut select.group_by.columns {
            walk_expression_tree_mut(expression, &mut rewrite_current_scope)?;
        }
        if let GroupByModifier::GroupingSets(sets) = &mut select.group_by.modifier {
            for expression in sets.iter_mut().flatten() {
                walk_expression_tree_mut(expression, &mut rewrite_current_scope)?;
            }
        }
        if let Some(expression) = select.having.as_deref_mut() {
            walk_expression_tree_mut(expression, &mut rewrite_current_scope)?;
        }
        for window in &mut select.window_defs {
            for expression in &mut window.partition_by {
                walk_expression_tree_mut(expression, &mut rewrite_current_scope)?;
            }
            for order in &mut window.order_by {
                walk_expression_tree_mut(&mut order.expression, &mut rewrite_current_scope)?;
            }
        }
        for order in &mut select.order_by {
            walk_expression_tree_mut(&mut order.expression, &mut rewrite_current_scope)?;
        }
        rewrite_navigation_in_source_conditions(
            select.table_expr.as_deref_mut(),
            &mut rewrite_current_scope,
        )?;
    }
    rewrite_nested_navigation_selects(select, rewrite)
}

pub(super) fn rewrite_nested_navigation_in_source(
    source: Option<&mut Expression>,
    rewrite: &mut impl FnMut(&mut Expression) -> Result<()>,
) -> Result<()> {
    let Some(source) = source else {
        return Ok(());
    };
    match source {
        Expression::JoinSource(join) => {
            rewrite_nested_navigation_in_source(Some(&mut join.left), rewrite)?;
            rewrite_nested_navigation_in_source(Some(&mut join.right), rewrite)?;
            if let Some(condition) = join.condition.as_deref_mut() {
                rewrite_nested_navigation_in_expression(condition, rewrite)?;
            }
        }
        Expression::SubquerySource(subquery) => {
            rewrite_navigation_select_recursive(&mut subquery.subquery, rewrite)?
        }
        Expression::TableSource(table) => {
            if let Some(as_of) = &mut table.as_of {
                rewrite_nested_navigation_in_expression(&mut as_of.value, rewrite)?;
            }
        }
        Expression::ValuesSource(values) => {
            for expression in values.rows.iter_mut().flatten() {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
        }
        Expression::FunctionTableSource(function) => {
            for expression in &mut function.arguments {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn rewrite_nested_navigation_in_expression(
    expression: &mut Expression,
    rewrite: &mut impl FnMut(&mut Expression) -> Result<()>,
) -> Result<()> {
    match expression {
        Expression::Exists(value) => {
            rewrite_navigation_select_recursive(&mut value.subquery, rewrite)
        }
        Expression::AllAny(value) => {
            rewrite_nested_navigation_in_expression(&mut value.left, rewrite)?;
            rewrite_navigation_select_recursive(&mut value.subquery, rewrite)
        }
        Expression::ScalarSubquery(value) => {
            rewrite_navigation_select_recursive(&mut value.subquery, rewrite)
        }
        Expression::Prefix(value) => {
            rewrite_nested_navigation_in_expression(&mut value.right, rewrite)
        }
        Expression::Infix(value) => {
            rewrite_nested_navigation_in_expression(&mut value.left, rewrite)?;
            rewrite_nested_navigation_in_expression(&mut value.right, rewrite)
        }
        Expression::List(value) => {
            for expression in &mut value.elements {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
            Ok(())
        }
        Expression::Distinct(value) => {
            rewrite_nested_navigation_in_expression(&mut value.expr, rewrite)
        }
        Expression::In(value) => {
            rewrite_nested_navigation_in_expression(&mut value.left, rewrite)?;
            rewrite_nested_navigation_in_expression(&mut value.right, rewrite)
        }
        Expression::InHashSet(value) => {
            rewrite_nested_navigation_in_expression(&mut value.column, rewrite)
        }
        Expression::Between(value) => {
            rewrite_nested_navigation_in_expression(&mut value.expr, rewrite)?;
            rewrite_nested_navigation_in_expression(&mut value.lower, rewrite)?;
            rewrite_nested_navigation_in_expression(&mut value.upper, rewrite)
        }
        Expression::Like(value) => {
            rewrite_nested_navigation_in_expression(&mut value.left, rewrite)?;
            rewrite_nested_navigation_in_expression(&mut value.pattern, rewrite)?;
            if let Some(escape) = &mut value.escape {
                rewrite_nested_navigation_in_expression(escape, rewrite)?;
            }
            Ok(())
        }
        Expression::ExpressionList(value) => {
            for expression in &mut value.expressions {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
            Ok(())
        }
        Expression::Case(value) => {
            if let Some(expression) = &mut value.value {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
            for clause in &mut value.when_clauses {
                rewrite_nested_navigation_in_expression(&mut clause.condition, rewrite)?;
                rewrite_nested_navigation_in_expression(&mut clause.then_result, rewrite)?;
            }
            if let Some(expression) = &mut value.else_value {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
            Ok(())
        }
        Expression::Cast(value) => {
            rewrite_nested_navigation_in_expression(&mut value.expr, rewrite)
        }
        Expression::FunctionCall(value) => {
            for expression in &mut value.arguments {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
            for order in &mut value.order_by {
                rewrite_nested_navigation_in_expression(&mut order.expression, rewrite)?;
            }
            if let Some(filter) = &mut value.filter {
                rewrite_nested_navigation_in_expression(filter, rewrite)?;
            }
            Ok(())
        }
        Expression::Aliased(value) => {
            rewrite_nested_navigation_in_expression(&mut value.expression, rewrite)
        }
        Expression::Window(value) => {
            for expression in &mut value.function.arguments {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
            for order in &mut value.function.order_by {
                rewrite_nested_navigation_in_expression(&mut order.expression, rewrite)?;
            }
            if let Some(filter) = &mut value.function.filter {
                rewrite_nested_navigation_in_expression(filter, rewrite)?;
            }
            for expression in &mut value.partition_by {
                rewrite_nested_navigation_in_expression(expression, rewrite)?;
            }
            for order in &mut value.order_by {
                rewrite_nested_navigation_in_expression(&mut order.expression, rewrite)?;
            }
            Ok(())
        }
        Expression::SubquerySource(value) => {
            rewrite_navigation_select_recursive(&mut value.subquery, rewrite)
        }
        _ => Ok(()),
    }
}

pub(super) fn rewrite_navigation_in_source_conditions(
    source: Option<&mut Expression>,
    rewrite: &mut impl FnMut(&mut Expression) -> Result<()>,
) -> Result<()> {
    let Some(source) = source else {
        return Ok(());
    };
    if let Expression::JoinSource(join) = source {
        rewrite_navigation_in_source_conditions(Some(&mut join.left), rewrite)?;
        rewrite_navigation_in_source_conditions(Some(&mut join.right), rewrite)?;
        if let Some(condition) = join.condition.as_deref_mut() {
            walk_expression_tree_mut(condition, rewrite)?;
        }
    }
    Ok(())
}

pub(super) fn single_root_table_source(select: &SelectStatement) -> Result<&SimpleTableSource> {
    match select.table_expr.as_deref() {
        Some(Expression::TableSource(table)) if select.with.is_none() => Ok(table),
        _ => Err(Error::NotSupported(
            "navigable predicates over joins, subqueries, CTEs, or table functions require NR-10"
                .to_string(),
        )),
    }
}

pub(super) fn append_root_projection(
    expressions: &mut Vec<Expression>,
    names: &mut Vec<String>,
    source_columns: &[String],
    token: &radixdb_sql::Token,
) {
    for column in source_columns {
        expressions.push(Expression::Identifier(radixdb_sql::ast::Identifier::new(
            token.clone(),
            column.clone(),
        )));
        names.push(column.clone());
    }
}

pub(super) fn reference_output_name(expression: &Expression, index: usize) -> String {
    match expression {
        Expression::Identifier(identifier) => identifier.value().to_string(),
        Expression::QualifiedIdentifier(identifier) => identifier.name.value().to_string(),
        Expression::Aliased(aliased) => aliased.alias.value().to_string(),
        Expression::FunctionCall(function) => function.function.to_string(),
        Expression::Cast(cast) => match cast.expr.as_ref() {
            Expression::Identifier(identifier) => identifier.value().to_string(),
            _ => format!("CAST(expr{})", index + 1),
        },
        _ => format!("expr{}", index + 1),
    }
}

pub(super) fn is_direct_navigation_projection(
    expression: &Expression,
    plan: &ReferenceExpandPlan,
) -> bool {
    let expression = match expression {
        Expression::Aliased(aliased) => aliased.expression.as_ref(),
        other => other,
    };
    matches!(expression, Expression::QualifiedIdentifier(path) if plan.path_index_for_display(&path.to_string()).is_some())
}

pub(super) fn expression_contains_subquery(expression: &Expression) -> bool {
    let mut found = false;
    radixdb_sql::ast::walk_expression_tree(expression, &mut |expression| {
        found |= matches!(
            expression,
            Expression::Exists(_) | Expression::AllAny(_) | Expression::ScalarSubquery(_)
        );
    });
    found
}

pub(super) fn rewrite_navigation_expression(
    expression: &Expression,
    plan: &ReferenceExpandPlan,
    hidden_names: &[String],
    visible_root: &str,
) -> Result<Expression> {
    let mut rewritten = expression.clone();
    walk_expression_tree_mut(&mut rewritten, &mut |expression| {
        if let Expression::QualifiedIdentifier(identifier) = expression {
            if let Some(path_index) = plan.path_index_for_display(&identifier.to_string()) {
                let hidden = hidden_names.get(path_index).ok_or_else(|| {
                    Error::internal("ReferenceExpand hidden path mapping is incomplete")
                })?;
                *expression = Expression::Identifier(radixdb_sql::ast::Identifier::new(
                    identifier.token.clone(),
                    hidden.clone(),
                ));
            } else if identifier.component_count() == 2
                && identifier
                    .qualifier
                    .value()
                    .eq_ignore_ascii_case(visible_root)
            {
                *expression = Expression::Identifier(identifier.name.as_ref().clone());
            }
        } else if matches!(
            expression,
            Expression::Exists(_) | Expression::AllAny(_) | Expression::ScalarSubquery(_)
        ) {
            return Err(Error::NotSupported(
                "subqueries combined with navigable references require NR-10".to_string(),
            ));
        }
        Ok(())
    })?;
    Ok(rewritten)
}

pub(super) fn walk_expression_tree_mut(
    expression: &mut Expression,
    visitor: &mut impl FnMut(&mut Expression) -> Result<()>,
) -> Result<()> {
    visitor(expression)?;
    match expression {
        Expression::Prefix(value) => walk_expression_tree_mut(&mut value.right, visitor)?,
        Expression::Infix(value) => {
            walk_expression_tree_mut(&mut value.left, visitor)?;
            walk_expression_tree_mut(&mut value.right, visitor)?;
        }
        Expression::List(value) => {
            for expression in &mut value.elements {
                walk_expression_tree_mut(expression, visitor)?;
            }
        }
        Expression::Distinct(value) => walk_expression_tree_mut(&mut value.expr, visitor)?,
        Expression::In(value) => {
            walk_expression_tree_mut(&mut value.left, visitor)?;
            walk_expression_tree_mut(&mut value.right, visitor)?;
        }
        Expression::InHashSet(value) => walk_expression_tree_mut(&mut value.column, visitor)?,
        Expression::Between(value) => {
            walk_expression_tree_mut(&mut value.expr, visitor)?;
            walk_expression_tree_mut(&mut value.lower, visitor)?;
            walk_expression_tree_mut(&mut value.upper, visitor)?;
        }
        Expression::Like(value) => {
            walk_expression_tree_mut(&mut value.left, visitor)?;
            walk_expression_tree_mut(&mut value.pattern, visitor)?;
            if let Some(escape) = &mut value.escape {
                walk_expression_tree_mut(escape, visitor)?;
            }
        }
        Expression::ExpressionList(value) => {
            for expression in &mut value.expressions {
                walk_expression_tree_mut(expression, visitor)?;
            }
        }
        Expression::Case(value) => {
            if let Some(expression) = &mut value.value {
                walk_expression_tree_mut(expression, visitor)?;
            }
            for clause in &mut value.when_clauses {
                walk_expression_tree_mut(&mut clause.condition, visitor)?;
                walk_expression_tree_mut(&mut clause.then_result, visitor)?;
            }
            if let Some(expression) = &mut value.else_value {
                walk_expression_tree_mut(expression, visitor)?;
            }
        }
        Expression::Cast(value) => walk_expression_tree_mut(&mut value.expr, visitor)?,
        Expression::FunctionCall(value) => {
            for argument in &mut value.arguments {
                walk_expression_tree_mut(argument, visitor)?;
            }
            for order in &mut value.order_by {
                walk_expression_tree_mut(&mut order.expression, visitor)?;
            }
            if let Some(filter) = &mut value.filter {
                walk_expression_tree_mut(filter, visitor)?;
            }
        }
        Expression::Aliased(value) => walk_expression_tree_mut(&mut value.expression, visitor)?,
        Expression::Window(value) => {
            for argument in &mut value.function.arguments {
                walk_expression_tree_mut(argument, visitor)?;
            }
            for order in &mut value.function.order_by {
                walk_expression_tree_mut(&mut order.expression, visitor)?;
            }
            if let Some(filter) = &mut value.function.filter {
                walk_expression_tree_mut(filter, visitor)?;
            }
            for partition in &mut value.partition_by {
                walk_expression_tree_mut(partition, visitor)?;
            }
            for order in &mut value.order_by {
                walk_expression_tree_mut(&mut order.expression, visitor)?;
            }
        }
        Expression::Exists(_) | Expression::AllAny(_) | Expression::ScalarSubquery(_) => {}
        Expression::Identifier(_)
        | Expression::IntegerLiteral(_)
        | Expression::FloatLiteral(_)
        | Expression::StringLiteral(_)
        | Expression::BooleanLiteral(_)
        | Expression::NullLiteral(_)
        | Expression::IntervalLiteral(_)
        | Expression::BoundValue(_)
        | Expression::Parameter(_)
        | Expression::TableSource(_)
        | Expression::JoinSource(_)
        | Expression::SubquerySource(_)
        | Expression::ValuesSource(_)
        | Expression::CteReference(_)
        | Expression::FunctionTableSource(_)
        | Expression::Star(_)
        | Expression::QualifiedStar(_)
        | Expression::Default(_)
        | Expression::QualifiedIdentifier(_) => {}
    }
    Ok(())
}

pub(super) fn materialize_runtime_parameters(
    expression: &mut Expression,
    ctx: &ExecutionContext,
) -> Result<()> {
    walk_expression_tree_mut(expression, &mut |expression| {
        let Expression::Parameter(parameter) = expression else {
            return Ok(());
        };
        let value = if parameter.name.starts_with(':') {
            let name = &parameter.name[1..];
            ctx.get_named_param(name).cloned().ok_or_else(|| {
                Error::invalid_argument(format!("missing named parameter :{name}"))
            })?
        } else {
            ctx.get_param(parameter.index).cloned().ok_or_else(|| {
                Error::invalid_argument(format!(
                    "missing positional parameter ${}",
                    parameter.index
                ))
            })?
        };
        *expression = Expression::BoundValue(Box::new(value));
        Ok(())
    })
}

pub(super) fn direct_navigation_path_index(
    expression: &Expression,
    plan: &ReferenceExpandPlan,
) -> Option<usize> {
    let Expression::QualifiedIdentifier(path) = expression else {
        return None;
    };
    plan.path_index_for_display(&path.to_string())
}

pub(super) fn constant_for_target_pushdown(expression: &Expression) -> bool {
    match expression {
        Expression::IntegerLiteral(_)
        | Expression::FloatLiteral(_)
        | Expression::StringLiteral(_)
        | Expression::BooleanLiteral(_)
        | Expression::NullLiteral(_)
        | Expression::IntervalLiteral(_)
        | Expression::BoundValue(_)
        | Expression::Parameter(_) => true,
        Expression::Prefix(value) => constant_for_target_pushdown(&value.right),
        Expression::List(value) => value.elements.iter().all(constant_for_target_pushdown),
        Expression::ExpressionList(value) => {
            value.expressions.iter().all(constant_for_target_pushdown)
        }
        Expression::Cast(value) => constant_for_target_pushdown(&value.expr),
        _ => false,
    }
}

pub(super) fn target_edge_for_paths(paths: &[usize], plan: &ReferenceExpandPlan) -> Option<usize> {
    let first = *paths.first()?;
    let edge = *plan.paths.get(first)?.edge_indices.last()?;
    paths
        .iter()
        .all(|path| {
            plan.paths
                .get(*path)
                .and_then(|path| path.edge_indices.last())
                .is_some_and(|candidate| *candidate == edge)
        })
        .then_some(edge)
}

/// Return the edge for a conservative null-rejecting target-only conjunct.
///
/// This deliberately recognizes only forms whose NULL truth table is fixed by
/// SQL itself. OR, NOT, IS NULL, CASE, arithmetic and function calls remain at
/// the post-expand filter even when a particular function happens to be strict.
pub(super) fn null_rejecting_target_edge(
    expression: &Expression,
    plan: &ReferenceExpandPlan,
) -> Option<usize> {
    match expression {
        Expression::Infix(value)
            if matches!(
                value.op_type(),
                InfixOperator::Equal
                    | InfixOperator::NotEqual
                    | InfixOperator::LessThan
                    | InfixOperator::LessEqual
                    | InfixOperator::GreaterThan
                    | InfixOperator::GreaterEqual
            ) =>
        {
            let left = direct_navigation_path_index(&value.left, plan);
            let right = direct_navigation_path_index(&value.right, plan);
            let paths = [left, right].into_iter().flatten().collect::<Vec<_>>();
            if paths.is_empty()
                || left.is_none() && !constant_for_target_pushdown(&value.left)
                || right.is_none() && !constant_for_target_pushdown(&value.right)
            {
                None
            } else {
                target_edge_for_paths(&paths, plan)
            }
        }
        Expression::Infix(value) if value.op_type() == InfixOperator::IsNot => {
            let left = direct_navigation_path_index(&value.left, plan);
            let right = direct_navigation_path_index(&value.right, plan);
            match (left, right, value.left.as_ref(), value.right.as_ref()) {
                (Some(path), None, _, Expression::NullLiteral(_))
                | (None, Some(path), Expression::NullLiteral(_), _) => {
                    target_edge_for_paths(&[path], plan)
                }
                _ => None,
            }
        }
        Expression::In(value)
            if direct_navigation_path_index(&value.left, plan).is_some()
                && constant_for_target_pushdown(&value.right) =>
        {
            target_edge_for_paths(&[direct_navigation_path_index(&value.left, plan)?], plan)
        }
        Expression::Between(value)
            if direct_navigation_path_index(&value.expr, plan).is_some()
                && constant_for_target_pushdown(&value.lower)
                && constant_for_target_pushdown(&value.upper) =>
        {
            target_edge_for_paths(&[direct_navigation_path_index(&value.expr, plan)?], plan)
        }
        Expression::Like(value)
            if direct_navigation_path_index(&value.left, plan).is_some()
                && constant_for_target_pushdown(&value.pattern)
                && value
                    .escape
                    .as_deref()
                    .is_none_or(constant_for_target_pushdown) =>
        {
            target_edge_for_paths(&[direct_navigation_path_index(&value.left, plan)?], plan)
        }
        _ => None,
    }
}

pub(super) fn expression_contains_bound_navigation(
    expression: &Expression,
    plan: &ReferenceExpandPlan,
) -> bool {
    let mut found = false;
    radixdb_sql::ast::walk_expression_tree(expression, &mut |expression| {
        if !found {
            if let Expression::QualifiedIdentifier(path) = expression {
                found = plan.path_index_for_display(&path.to_string()).is_some();
            }
        }
    });
    found
}
use super::*;
