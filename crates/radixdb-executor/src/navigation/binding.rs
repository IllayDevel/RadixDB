use super::*;

impl NavigationExpr {
    pub fn identity(&self) -> &NavigationPathIdentity {
        &self.identity
    }

    pub fn root_relation(&self) -> &RootRelationInstance {
        &self.identity.root
    }

    pub fn steps(&self) -> &[ReferenceStep] {
        &self.steps
    }

    pub fn terminal_column(&self) -> &SchemaColumnId {
        &self.identity.terminal_column
    }

    pub fn terminal_type(&self) -> DataType {
        self.terminal_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn display_path(&self) -> &str {
        &self.display_path
    }

    pub fn validate(&self, engine: &dyn Engine) -> Result<()> {
        engine.validate_schema_table_id(self.identity.root.table())?;
        for step in &self.steps {
            engine.validate_schema_table_id(step.source_column().table())?;
            engine.validate_schema_table_id(step.target_table())?;
        }
        engine.validate_schema_table_id(self.terminal_column().table())
    }
}

#[derive(Debug, Clone)]
struct ScopeRoot {
    ordinal: u32,
    visible_name_lower: String,
    table: Option<SchemaTableId>,
}

fn collect_source_visible_names(source: &Expression, output: &mut FxHashSet<String>) {
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
            collect_source_visible_names(&join.left, output);
            collect_source_visible_names(&join.right, output);
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

struct NavigationBinder<'a> {
    engine: &'a dyn Engine,
    ctx: Option<&'a ExecutionContext>,
    next_relation_ordinal: u32,
}

impl<'a> NavigationBinder<'a> {
    fn new(engine: &'a dyn Engine) -> Self {
        Self {
            engine,
            ctx: None,
            next_relation_ordinal: 0,
        }
    }

    fn with_context(engine: &'a dyn Engine, ctx: &'a ExecutionContext) -> Self {
        Self {
            engine,
            ctx: Some(ctx),
            next_relation_ordinal: 0,
        }
    }

    fn bind_select(
        &mut self,
        select: &SelectStatement,
        output: &mut Vec<NavigationExpr>,
    ) -> Result<()> {
        let visible_ctes: FxHashSet<&str> = select
            .with
            .as_ref()
            .into_iter()
            .flat_map(|with| with.ctes.iter())
            .map(|cte| cte.name.value_lower())
            .collect();

        let mut roots = Vec::new();
        if let Some(source) = select.table_expr.as_deref() {
            self.collect_roots(source, &visible_ctes, &mut roots)?;
        }

        self.visit_select_expressions(select, &roots, output)
    }

    fn visit_select_expressions(
        &mut self,
        select: &SelectStatement,
        roots: &[ScopeRoot],
        output: &mut Vec<NavigationExpr>,
    ) -> Result<()> {
        if let Some(source) = select.table_expr.as_deref() {
            self.visit_source_expressions(source, roots, output)?;
        }
        for expression in select.distinct_on.iter().chain(&select.columns) {
            self.visit_expression(expression, roots, output)?;
        }
        if let Some(expression) = select.where_clause.as_deref() {
            self.visit_expression(expression, roots, output)?;
        }
        for expression in &select.group_by.columns {
            self.visit_expression(expression, roots, output)?;
        }
        if let GroupByModifier::GroupingSets(sets) = &select.group_by.modifier {
            for set in sets {
                for expression in set {
                    self.visit_expression(expression, roots, output)?;
                }
            }
        }
        if let Some(expression) = select.having.as_deref() {
            self.visit_expression(expression, roots, output)?;
        }
        for window in &select.window_defs {
            for expression in &window.partition_by {
                self.visit_expression(expression, roots, output)?;
            }
            for order in &window.order_by {
                self.visit_expression(&order.expression, roots, output)?;
            }
            if let Some(frame) = &window.frame {
                self.visit_frame(frame, roots, output)?;
            }
        }
        for order in &select.order_by {
            self.visit_expression(&order.expression, roots, output)?;
        }
        if let Some(expression) = select.limit.as_deref() {
            self.visit_expression(expression, roots, output)?;
        }
        if let Some(expression) = select.offset.as_deref() {
            self.visit_expression(expression, roots, output)?;
        }
        Ok(())
    }

    fn collect_roots(
        &mut self,
        source: &Expression,
        visible_ctes: &FxHashSet<&str>,
        roots: &mut Vec<ScopeRoot>,
    ) -> Result<()> {
        match source {
            Expression::TableSource(table) => {
                let visible_name = table.alias.as_ref().unwrap_or(&table.name);
                let table_id = if visible_ctes.contains(table.name.value_lower())
                    || self
                        .ctx
                        .is_some_and(|ctx| ctx.get_cte_by_lower(table.name.value_lower()).is_some())
                    || !self.engine.table_exists(table.name.value())?
                {
                    None
                } else {
                    Some(self.engine.bind_schema_table_id(table.name.value())?)
                };
                self.push_root(visible_name.value_lower(), table_id, roots)?;
            }
            Expression::JoinSource(join) => {
                self.collect_roots(&join.left, visible_ctes, roots)?;
                self.collect_roots(&join.right, visible_ctes, roots)?;
            }
            Expression::SubquerySource(subquery) => {
                if let Some(alias) = &subquery.alias {
                    self.push_root(alias.value_lower(), None, roots)?;
                }
            }
            Expression::CteReference(cte) => {
                let visible_name = cte.alias.as_ref().unwrap_or(&cte.name);
                self.push_root(visible_name.value_lower(), None, roots)?;
            }
            Expression::ValuesSource(values) => {
                if let Some(alias) = &values.alias {
                    self.push_root(alias.value_lower(), None, roots)?;
                }
            }
            Expression::FunctionTableSource(function) => {
                if let Some(alias) = &function.alias {
                    self.push_root(alias.value_lower(), None, roots)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn push_root(
        &mut self,
        visible_name_lower: &str,
        table: Option<SchemaTableId>,
        roots: &mut Vec<ScopeRoot>,
    ) -> Result<()> {
        let ordinal = self.next_relation_ordinal;
        self.next_relation_ordinal =
            self.next_relation_ordinal.checked_add(1).ok_or_else(|| {
                Error::navigation(
                    NavigationErrorCode::UnsupportedReferenceShape,
                    "query contains too many relation instances",
                )
            })?;
        roots.push(ScopeRoot {
            ordinal,
            visible_name_lower: visible_name_lower.to_string(),
            table,
        });
        Ok(())
    }

    fn visit_source_expressions(
        &mut self,
        source: &Expression,
        roots: &[ScopeRoot],
        output: &mut Vec<NavigationExpr>,
    ) -> Result<()> {
        match source {
            Expression::TableSource(table) => {
                if let Some(as_of) = &table.as_of {
                    self.visit_expression(&as_of.value, roots, output)?;
                }
            }
            Expression::JoinSource(join) => {
                self.visit_source_expressions(&join.left, roots, output)?;
                self.visit_source_expressions(&join.right, roots, output)?;
                if let Some(condition) = &join.condition {
                    self.visit_expression(condition, roots, output)?;
                }
            }
            Expression::ValuesSource(values) => {
                for row in &values.rows {
                    for expression in row {
                        self.visit_expression(expression, roots, output)?;
                    }
                }
            }
            Expression::FunctionTableSource(function) => {
                for expression in &function.arguments {
                    self.visit_expression(expression, roots, output)?;
                }
            }
            Expression::SubquerySource(_) | Expression::CteReference(_) => {}
            _ => {}
        }
        Ok(())
    }

    fn visit_expression(
        &mut self,
        expression: &Expression,
        roots: &[ScopeRoot],
        output: &mut Vec<NavigationExpr>,
    ) -> Result<()> {
        match expression {
            Expression::QualifiedIdentifier(path) => {
                if let Some(bound) = self.bind_path(path, roots)? {
                    output.push(bound);
                }
            }
            Expression::QualifiedStar(star) => {
                self.bind_navigation_wildcard(&star.qualifier, roots)?
            }
            Expression::Prefix(value) => self.visit_expression(&value.right, roots, output)?,
            Expression::Infix(value) => {
                self.visit_expression(&value.left, roots, output)?;
                self.visit_expression(&value.right, roots, output)?;
            }
            Expression::List(value) => {
                for expression in &value.elements {
                    self.visit_expression(expression, roots, output)?;
                }
            }
            Expression::Distinct(value) => self.visit_expression(&value.expr, roots, output)?,
            Expression::Exists(value) => {
                self.visit_correlated_navigation(&value.subquery, roots, output)?
            }
            Expression::AllAny(value) => {
                self.visit_expression(&value.left, roots, output)?;
                self.visit_correlated_navigation(&value.subquery, roots, output)?;
            }
            Expression::In(value) => {
                self.visit_expression(&value.left, roots, output)?;
                self.visit_expression(&value.right, roots, output)?;
            }
            Expression::InHashSet(value) => self.visit_expression(&value.column, roots, output)?,
            Expression::Between(value) => {
                self.visit_expression(&value.expr, roots, output)?;
                self.visit_expression(&value.lower, roots, output)?;
                self.visit_expression(&value.upper, roots, output)?;
            }
            Expression::Like(value) => {
                self.visit_expression(&value.left, roots, output)?;
                self.visit_expression(&value.pattern, roots, output)?;
                if let Some(escape) = &value.escape {
                    self.visit_expression(escape, roots, output)?;
                }
            }
            Expression::ScalarSubquery(value) => {
                self.visit_correlated_navigation(&value.subquery, roots, output)?
            }
            Expression::ExpressionList(value) => {
                for expression in &value.expressions {
                    self.visit_expression(expression, roots, output)?;
                }
            }
            Expression::Case(value) => {
                if let Some(expression) = &value.value {
                    self.visit_expression(expression, roots, output)?;
                }
                for clause in &value.when_clauses {
                    self.visit_expression(&clause.condition, roots, output)?;
                    self.visit_expression(&clause.then_result, roots, output)?;
                }
                if let Some(expression) = &value.else_value {
                    self.visit_expression(expression, roots, output)?;
                }
            }
            Expression::Cast(value) => self.visit_expression(&value.expr, roots, output)?,
            Expression::FunctionCall(value) => {
                for expression in &value.arguments {
                    self.visit_expression(expression, roots, output)?;
                }
                for order in &value.order_by {
                    self.visit_expression(&order.expression, roots, output)?;
                }
                if let Some(filter) = &value.filter {
                    self.visit_expression(filter, roots, output)?;
                }
            }
            Expression::Aliased(value) => {
                self.visit_expression(&value.expression, roots, output)?;
            }
            Expression::Window(value) => {
                for expression in &value.function.arguments {
                    self.visit_expression(expression, roots, output)?;
                }
                for order in &value.function.order_by {
                    self.visit_expression(&order.expression, roots, output)?;
                }
                if let Some(filter) = &value.function.filter {
                    self.visit_expression(filter, roots, output)?;
                }
                for expression in &value.partition_by {
                    self.visit_expression(expression, roots, output)?;
                }
                for order in &value.order_by {
                    self.visit_expression(&order.expression, roots, output)?;
                }
                if let Some(frame) = &value.frame {
                    self.visit_frame(frame, roots, output)?;
                }
            }
            Expression::SubquerySource(_) => {}
            Expression::TableSource(_)
            | Expression::JoinSource(_)
            | Expression::ValuesSource(_)
            | Expression::FunctionTableSource(_)
            | Expression::Identifier(_)
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
            | Expression::Default(_) => {}
        }
        Ok(())
    }

    fn visit_correlated_navigation(
        &mut self,
        select: &SelectStatement,
        outer_roots: &[ScopeRoot],
        output: &mut Vec<NavigationExpr>,
    ) -> Result<()> {
        let mut shadowed = FxHashSet::default();
        if let Some(source) = select.table_expr.as_deref() {
            collect_source_visible_names(source, &mut shadowed);
        }
        if let Some(with) = &select.with {
            shadowed.extend(
                with.ctes
                    .iter()
                    .map(|cte| cte.name.value_lower().to_string()),
            );
        }
        let visible_outer = outer_roots
            .iter()
            .filter(|root| !shadowed.contains(&root.visible_name_lower))
            .cloned()
            .collect::<Vec<_>>();
        if visible_outer.is_empty() {
            return Ok(());
        }
        if select.table_expr.is_none() {
            return self.visit_select_expressions(select, &visible_outer, output);
        }

        let names = visible_outer
            .iter()
            .map(|root| root.visible_name_lower.as_str())
            .collect::<FxHashSet<_>>();
        let mut candidates = Vec::new();
        radixdb_sql::ast::walk_select_tree(select, &mut |expression| {
            let Expression::QualifiedIdentifier(path) = expression else {
                return;
            };
            if path.component_count() > 2 && names.contains(path.qualifier.value_lower()) {
                candidates.push(path.clone());
            }
        });
        for path in candidates {
            if let Some(bound) = self.bind_path(&path, &visible_outer)? {
                output.push(bound);
            }
        }
        Ok(())
    }

    fn visit_frame(
        &mut self,
        frame: &WindowFrame,
        roots: &[ScopeRoot],
        output: &mut Vec<NavigationExpr>,
    ) -> Result<()> {
        self.visit_frame_bound(&frame.start, roots, output)?;
        if let Some(end) = &frame.end {
            self.visit_frame_bound(end, roots, output)?;
        }
        Ok(())
    }

    fn visit_frame_bound(
        &mut self,
        bound: &WindowFrameBound,
        roots: &[ScopeRoot],
        output: &mut Vec<NavigationExpr>,
    ) -> Result<()> {
        if let WindowFrameBound::Preceding(expression) | WindowFrameBound::Following(expression) =
            bound
        {
            self.visit_expression(expression, roots, output)?;
        }
        Ok(())
    }

    fn bind_path(
        &self,
        path: &QualifiedIdentifier,
        roots: &[ScopeRoot],
    ) -> Result<Option<NavigationExpr>> {
        let components: Vec<&str> = path.components().map(|item| item.value()).collect();
        debug_assert!(components.len() >= 2);
        let root_name_lower = components[0].to_lowercase();
        let explicit_roots: Vec<&ScopeRoot> = roots
            .iter()
            .filter(|root| root.visible_name_lower == root_name_lower)
            .collect();

        if !explicit_roots.is_empty() {
            // Alias-first resolution leaves every ordinary two-component name
            // to the existing column binder, including duplicate-alias errors.
            if components.len() == 2 {
                return Ok(None);
            }
            if explicit_roots.len() != 1 {
                return Err(Error::navigation(
                    NavigationErrorCode::AmbiguousRoot,
                    format!("relation root '{}' is ambiguous", components[0]),
                ));
            }
            let root = explicit_roots[0];
            if root.table.is_none() {
                return Err(Error::navigation(
                    NavigationErrorCode::UnsupportedReferenceShape,
                    format!(
                        "navigation root '{}' is not a physical catalog table",
                        components[0]
                    ),
                ));
            }
            return self
                .bind_from_root(
                    root,
                    &components[1..components.len() - 1],
                    components.last().unwrap(),
                    path,
                )
                .map(Some);
        }

        let source_component = components[0];
        let mut candidates = Vec::new();
        let mut non_reference_roots = 0usize;
        for root in roots {
            let Some(table) = &root.table else {
                continue;
            };
            let schema = self.engine.get_table_schema(table.table_name())?;
            if !schema.has_column(source_component) {
                continue;
            }
            let source = self.engine.bind_schema_column_id(table, source_component)?;
            if self.engine.get_reference_descriptor(&source)?.is_some() {
                candidates.push(root);
            } else {
                non_reference_roots += 1;
            }
        }

        if candidates.len() > 1 {
            return Err(Error::navigation(
                NavigationErrorCode::AmbiguousRoot,
                format!(
                    "reference shorthand '{}' matches {} relation instances; qualify the root",
                    source_component,
                    candidates.len()
                ),
            ));
        }
        if let Some(root) = candidates.first() {
            return self
                .bind_from_root(
                    root,
                    &components[..components.len() - 1],
                    components.last().unwrap(),
                    path,
                )
                .map(Some);
        }
        if non_reference_roots > 0 {
            return Err(Error::navigation(
                NavigationErrorCode::NotAReference,
                format!("column '{}' is not a navigable reference", source_component),
            ));
        }
        // A two-part name with no alias/FK candidate may be an ordinary
        // correlated qualifier resolved by a legacy outer scope. Only a
        // longer path is unambiguously navigation syntax at this stage.
        if components.len() == 2 {
            return Ok(None);
        }
        Err(Error::navigation(
            NavigationErrorCode::UnknownRoot,
            format!(
                "'{}' is neither a visible relation root nor an unambiguous reference column",
                source_component
            ),
        ))
    }

    fn bind_from_root(
        &self,
        root: &ScopeRoot,
        step_names: &[&str],
        terminal_name: &str,
        path: &QualifiedIdentifier,
    ) -> Result<NavigationExpr> {
        if step_names.len() > MAX_NAVIGATION_STEPS {
            return Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                format!(
                    "navigation path '{}' has {} steps; maximum is {MAX_NAVIGATION_STEPS}",
                    path,
                    step_names.len()
                ),
            ));
        }
        let root_table = root.table.as_ref().expect("physical root checked");
        let mut current_table = root_table.clone();
        let mut steps = Vec::with_capacity(step_names.len());
        let mut nullable = false;

        for step_name in step_names {
            let source = self
                .engine
                .bind_schema_column_id(&current_table, step_name)
                .map_err(|error| match error {
                    Error::ColumnNotFound(_) => Error::navigation(
                        NavigationErrorCode::NotAReference,
                        format!(
                            "'{}.{}' is not a reference column",
                            current_table.table_name(),
                            step_name
                        ),
                    ),
                    other => other,
                })?;
            let descriptor = self
                .engine
                .get_reference_descriptor(&source)?
                .ok_or_else(|| {
                    Error::navigation(
                        NavigationErrorCode::NotAReference,
                        format!(
                            "'{}.{}' is not a reference column",
                            current_table.table_name(),
                            step_name
                        ),
                    )
                })?;
            nullable |= descriptor.source_nullable();
            let step = ReferenceStep {
                identity: ReferenceStepIdentity {
                    source_column: descriptor.source().clone(),
                    target_key_column: descriptor.target().clone(),
                },
                target_key: descriptor.target_key(),
                source_nullable: descriptor.source_nullable(),
            };
            current_table = descriptor.target().table().clone();
            steps.push(step);
        }

        let terminal_column = self
            .engine
            .bind_schema_column_id(&current_table, terminal_name)
            .map_err(|error| match error {
                Error::ColumnNotFound(_) => Error::navigation(
                    NavigationErrorCode::TargetColumnNotFound,
                    format!(
                        "target column '{}.{}' does not exist",
                        current_table.table_name(),
                        terminal_name
                    ),
                ),
                other => other,
            })?;
        let terminal_schema = self.engine.get_table_schema(current_table.table_name())?;
        let terminal = terminal_schema
            .get_column(terminal_column.ordinal())
            .ok_or_else(|| {
                Error::navigation(
                    NavigationErrorCode::SchemaChanged,
                    format!(
                        "terminal column ordinal {} no longer exists in '{}'",
                        terminal_column.ordinal(),
                        current_table.table_name()
                    ),
                )
            })?;
        nullable |= terminal.nullable;

        if self.engine.schema_epoch() != root_table.schema_generation() {
            return Err(Error::navigation(
                NavigationErrorCode::SchemaChanged,
                format!("schema generation changed while binding path '{}'", path),
            ));
        }

        let root_relation = RootRelationInstance {
            ordinal: root.ordinal,
            table: root_table.clone(),
        };
        let identity = NavigationPathIdentity {
            root: root_relation,
            steps: steps.iter().map(|step| step.identity.clone()).collect(),
            terminal_column,
        };
        Ok(NavigationExpr {
            identity,
            steps,
            terminal_type: terminal.data_type,
            nullable,
            display_path: path.to_string(),
        })
    }

    fn bind_navigation_wildcard(&self, qualifier: &str, roots: &[ScopeRoot]) -> Result<()> {
        let qualifier_lower = qualifier.to_lowercase();
        if roots
            .iter()
            .any(|root| root.visible_name_lower == qualifier_lower)
        {
            return Ok(());
        }

        let mut matches = 0usize;
        for root in roots {
            let Some(table) = &root.table else {
                continue;
            };
            let schema = self.engine.get_table_schema(table.table_name())?;
            if !schema.has_column(qualifier) {
                continue;
            }
            let source = self.engine.bind_schema_column_id(table, qualifier)?;
            if self.engine.get_reference_descriptor(&source)?.is_some() {
                matches += 1;
            }
        }
        match matches {
            0 => Ok(()),
            1 => Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                format!("navigation wildcard '{}.*' is not allowed", qualifier),
            )),
            _ => Err(Error::navigation(
                NavigationErrorCode::AmbiguousRoot,
                format!("navigation wildcard root '{}' is ambiguous", qualifier),
            )),
        }
    }
}

/// Bind every navigation path in a SELECT without reading rows or choosing a
/// physical execution strategy.
pub fn bind_navigation_paths(
    engine: &dyn Engine,
    select: &SelectStatement,
) -> Result<Vec<NavigationExpr>> {
    let generation = engine.schema_epoch();
    let mut output = Vec::new();
    NavigationBinder::new(engine).bind_select(select, &mut output)?;
    if engine.schema_epoch() != generation {
        return Err(Error::navigation(
            NavigationErrorCode::SchemaChanged,
            "schema generation changed while binding statement",
        ));
    }
    Ok(output)
}

pub fn bind_reference_expand_plan(
    engine: &dyn Engine,
    select: &SelectStatement,
) -> Result<Option<ReferenceExpandPlan>> {
    let paths = bind_navigation_paths(engine, select)?;
    if paths.is_empty() {
        Ok(None)
    } else {
        ReferenceExpandPlan::build(paths).map(Some)
    }
}

pub fn bind_reference_expand_plan_for_execution(
    engine: &dyn Engine,
    select: &SelectStatement,
    ctx: &ExecutionContext,
) -> Result<Option<ReferenceExpandPlan>> {
    let generation = engine.schema_epoch();
    let mut paths = Vec::new();
    NavigationBinder::with_context(engine, ctx).bind_select(select, &mut paths)?;
    if engine.schema_epoch() != generation {
        return Err(Error::navigation(
            NavigationErrorCode::SchemaChanged,
            "schema generation changed while binding statement",
        ));
    }
    if paths.is_empty() {
        Ok(None)
    } else {
        ReferenceExpandPlan::build(paths).map(Some)
    }
}

/// Enforce the permanent read-only boundary before a write statement opens a
/// transaction, creates a statement savepoint, or reads source rows.
pub fn reject_navigation_in_write_statement(
    engine: &dyn Engine,
    statement: &Statement,
) -> Result<()> {
    let mut paths = Vec::new();
    match statement {
        Statement::Insert(insert) => {
            let roots = dml_target_roots(engine, &insert.table_name, None)?;
            let mut binder = NavigationBinder::new(engine);
            for row in &insert.values {
                for expression in row {
                    bind_dml_expression(engine, &mut binder, &roots, expression, &mut paths)?;
                }
            }
            if let Some(select) = insert.select.as_deref() {
                bind_dml_select_tree(engine, select, &mut paths)?;
            }
            for expression in insert.update_expressions.iter().chain(&insert.returning) {
                bind_dml_expression(engine, &mut binder, &roots, expression, &mut paths)?;
            }
        }
        Statement::Update(update) => {
            let roots = dml_target_roots(engine, &update.table_name, None)?;
            let mut binder = NavigationBinder::new(engine);
            for expression in update.updates.values() {
                bind_dml_expression(engine, &mut binder, &roots, expression, &mut paths)?;
            }
            if let Some(expression) = update.where_clause.as_deref() {
                bind_dml_expression(engine, &mut binder, &roots, expression, &mut paths)?;
            }
            for expression in &update.returning {
                bind_dml_expression(engine, &mut binder, &roots, expression, &mut paths)?;
            }
        }
        Statement::Delete(delete) => {
            let roots = dml_target_roots(engine, &delete.table_name, delete.alias.as_ref())?;
            let mut binder = NavigationBinder::new(engine);
            if let Some(expression) = delete.where_clause.as_deref() {
                bind_dml_expression(engine, &mut binder, &roots, expression, &mut paths)?;
            }
            for expression in &delete.returning {
                bind_dml_expression(engine, &mut binder, &roots, expression, &mut paths)?;
            }
        }
        Statement::CreateTable(create) => {
            if let Some(select) = create.as_select.as_deref() {
                bind_dml_select_tree(engine, select, &mut paths)?;
            }
        }
        Statement::CreateView(create) => {
            bind_dml_select_tree(engine, &create.query, &mut paths)?;
        }
        Statement::Explain(explain) => {
            return reject_navigation_in_write_statement(engine, &explain.statement)
        }
        _ => return Ok(()),
    }

    if let Some(path) = paths.first() {
        if matches!(statement, Statement::CreateView(_)) {
            return Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                format!(
                    "navigation path '{}' cannot be persisted in a VIEW definition in the first version",
                    path.display_path()
                ),
            ));
        }
        return Err(Error::navigation(
            NavigationErrorCode::ReadOnly,
            format!(
                "navigation path '{}' cannot appear in a write statement; name the target table and relationship explicitly",
                path.display_path()
            ),
        ));
    }
    Ok(())
}

fn dml_target_roots(
    engine: &dyn Engine,
    table_name: &Identifier,
    alias: Option<&Identifier>,
) -> Result<Vec<ScopeRoot>> {
    let table = if engine.table_exists(table_name.value())? {
        Some(engine.bind_schema_table_id(table_name.value())?)
    } else {
        None
    };
    Ok(vec![ScopeRoot {
        ordinal: 0,
        visible_name_lower: alias.unwrap_or(table_name).value_lower().to_string(),
        table,
    }])
}

fn bind_dml_expression(
    engine: &dyn Engine,
    binder: &mut NavigationBinder<'_>,
    roots: &[ScopeRoot],
    expression: &Expression,
    output: &mut Vec<NavigationExpr>,
) -> Result<()> {
    binder.visit_expression(expression, roots, output)?;
    if output.is_empty() {
        bind_nested_dml_selects(engine, expression, output)?;
    }
    Ok(())
}

fn bind_nested_dml_selects(
    engine: &dyn Engine,
    expression: &Expression,
    output: &mut Vec<NavigationExpr>,
) -> Result<()> {
    let mut error = None;
    radixdb_sql::ast::walk_expression_tree(expression, &mut |node| {
        if error.is_some() || !output.is_empty() {
            return;
        }
        let nested = match node {
            Expression::Exists(value) => Some(value.subquery.as_ref()),
            Expression::AllAny(value) => Some(value.subquery.as_ref()),
            Expression::ScalarSubquery(value) => Some(value.subquery.as_ref()),
            Expression::SubquerySource(value) => Some(value.subquery.as_ref()),
            _ => None,
        };
        if let Some(select) = nested {
            if let Err(current) = bind_dml_select_tree(engine, select, output) {
                error = Some(current);
            }
        }
    });
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn bind_dml_select_tree(
    engine: &dyn Engine,
    select: &SelectStatement,
    output: &mut Vec<NavigationExpr>,
) -> Result<()> {
    NavigationBinder::new(engine).bind_select(select, output)?;
    if !output.is_empty() {
        return Ok(());
    }
    if let Some(with) = &select.with {
        for cte in &with.ctes {
            bind_dml_select_tree(engine, &cte.query, output)?;
            if !output.is_empty() {
                return Ok(());
            }
        }
    }
    for operation in &select.set_operations {
        bind_dml_select_tree(engine, &operation.right, output)?;
        if !output.is_empty() {
            return Ok(());
        }
    }

    let mut error = None;
    radixdb_sql::ast::walk_select_tree(select, &mut |node| {
        if error.is_some() || !output.is_empty() {
            return;
        }
        let nested = match node {
            Expression::Exists(value) => Some(value.subquery.as_ref()),
            Expression::AllAny(value) => Some(value.subquery.as_ref()),
            Expression::ScalarSubquery(value) => Some(value.subquery.as_ref()),
            Expression::SubquerySource(value) => Some(value.subquery.as_ref()),
            _ => None,
        };
        if let Some(nested) = nested {
            if let Err(current) = bind_dml_select_tree(engine, nested, output) {
                error = Some(current);
            }
        }
    });
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
