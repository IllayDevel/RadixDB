impl Executor {
    /// Generate EXPLAIN output for a statement
    fn explain_statement(
        &self,
        stmt: &Statement,
        lines: &mut Vec<String>,
        indent: usize,
        ctx: &ExecutionContext,
    ) {
        let prefix = "  ".repeat(indent);

        match stmt {
            Statement::Select(select) => {
                self.explain_select(select, lines, indent, ctx);
            }
            Statement::Insert(insert) => {
                lines.push(format!("{}INSERT INTO {}", prefix, insert.table_name));
                if let Some(ref select) = insert.select {
                    lines.push(format!("{}  Source:", prefix));
                    self.explain_select(select, lines, indent + 2, ctx);
                } else {
                    lines.push(format!(
                        "{}  Values: {} row(s)",
                        prefix,
                        insert.values.len()
                    ));
                }
            }
            Statement::Update(update) => {
                lines.push(format!("{}UPDATE {}", prefix, update.table_name));
                lines.push(format!(
                    "{}  Set: {} column(s)",
                    prefix,
                    update.updates.len()
                ));
                if let Some(ref where_clause) = update.where_clause {
                    lines.push(format!("{}  Filter: {}", prefix, where_clause));
                }
            }
            Statement::Delete(delete) => {
                lines.push(format!("{}DELETE FROM {}", prefix, delete.table_name));
                if let Some(ref where_clause) = delete.where_clause {
                    lines.push(format!("{}  Filter: {}", prefix, where_clause));
                }
                self.append_delete_access_path(delete, lines, indent, ctx);
            }
            _ => {
                lines.push(format!("{}Statement: {}", prefix, stmt));
            }
        }
    }

    /// Describe the actual executor/storage boundary used by DELETE.
    ///
    /// This is intentionally a stable physical contract rather than a cost
    /// estimate. Storage-pushdown DELETE is split into row-ID discovery and one
    /// mutation batch; expressions that require executor evaluation, RETURNING
    /// payloads or FK actions retain the explicit full-row fallback.
    fn append_delete_access_path(
        &self,
        delete: &DeleteStatement,
        lines: &mut Vec<String>,
        indent: usize,
        ctx: &ExecutionContext,
    ) {
        let prefix = "  ".repeat(indent);
        let table_name = delete.table_name.value_lower.as_str();
        let has_referencing_fks =
            !crate::mutation::foreign_key::find_referencing_fks(&self.engine, table_name)
                .is_empty();

        let (needs_executor_filter, predicate_columns) = match delete.where_clause.as_deref() {
            None => (false, Some(Vec::new())),
            Some(where_clause) => {
                let Ok(tx) = self.engine.begin_transaction() else {
                    return lines.push(format!(
                        "{}  DML Access Path: unavailable (table metadata could not be opened)",
                        prefix
                    ));
                };
                let Ok(table) = tx.get_table(table_name) else {
                    return lines.push(format!(
                        "{}  DML Access Path: unavailable (table metadata could not be opened)",
                        prefix
                    ));
                };
                let (storage_expr, needs_memory_filter) =
                    pushdown::try_pushdown(where_clause, table.schema(), Some(ctx));
                let columns = storage_expr.as_deref().and_then(|expression| {
                    let mut indices = Vec::new();
                    expression.collect_column_indices(&mut indices).then(|| {
                        indices.sort_unstable();
                        indices.dedup();
                        indices
                            .into_iter()
                            .filter_map(|index| {
                                table
                                    .schema()
                                    .columns
                                    .get(index)
                                    .map(|column| column.name.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                });
                (needs_memory_filter, columns)
            }
        };

        let mut fallback_reasons = Vec::new();
        if needs_executor_filter {
            fallback_reasons.push("executor_predicate");
        }
        if !delete.returning.is_empty() {
            fallback_reasons.push("returning_payload");
        }
        if has_referencing_fks {
            fallback_reasons.push("foreign_key_actions");
        }

        if fallback_reasons.is_empty() {
            lines.push(format!(
                "{}  DML Candidate Source: dml.row_id_candidates.storage_exact_projection",
                prefix
            ));
            lines.push(format!(
                "{}  DML Read Boundary: {}",
                prefix,
                if delete.where_clause.is_some() {
                    "predicate_columns + row_identity"
                } else {
                    "row_identity"
                }
            ));
            lines.push(format!(
                "{}  DML Predicate Columns: {}",
                prefix,
                match predicate_columns.as_deref() {
                    Some([]) => "none".to_string(),
                    Some(columns) => columns.join(", "),
                    None => "unknown (storage fallback)".to_string(),
                }
            ));
            lines.push(format!(
                "{}  DML Mutation: dml.batch_delete.hot_mvcc+cold_tombstone",
                prefix
            ));
        } else {
            lines.push(format!(
                "{}  DML Candidate Source: dml.executor.full_row_scan",
                prefix
            ));
            lines.push(format!(
                "{}  DML Read Boundary: full_row (reason={})",
                prefix,
                fallback_reasons.join("+")
            ));
            lines.push(format!(
                "{}  DML Mutation: dml.per_row.primary_key_fallback",
                prefix
            ));
        }
    }

    /// Generate EXPLAIN output for a SELECT statement
    fn explain_select(
        &self,
        select: &SelectStatement,
        lines: &mut Vec<String>,
        indent: usize,
        ctx: &ExecutionContext,
    ) {
        let prefix = "  ".repeat(indent);

        // CTE info
        if let Some(ref with) = select.with {
            lines.push(format!("{}WITH (CTEs: {})", prefix, with.ctes.len()));
            for cte in &with.ctes {
                lines.push(format!(
                    "{}  {} = ({})",
                    prefix,
                    cte.name,
                    if cte.is_recursive {
                        "RECURSIVE"
                    } else {
                        "non-recursive"
                    }
                ));
            }
        }

        // Main operation
        if select.distinct {
            lines.push(format!("{}SELECT DISTINCT", prefix));
        } else {
            lines.push(format!("{}SELECT", prefix));
        }

        // Columns - use same threshold as EXPLAIN ANALYZE (5 columns)
        let col_count = select.columns.len();
        if col_count <= 5 {
            let cols: Vec<String> = select.columns.iter().map(|c| format!("{}", c)).collect();
            lines.push(format!("{}  Columns: {}", prefix, cols.join(", ")));
        } else {
            lines.push(format!("{}  Columns: {} column(s)", prefix, col_count));
        }
        self.append_select_path_debug(select, lines, indent, None, None);

        // FROM clause with access plan
        // Check for vector search fast path first
        let vector_plan = if let Some(ref table_expr) = select.table_expr {
            if let Some(table_name) = extract_table_name(table_expr) {
                self.detect_vector_search_plan(select, &table_name)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(ref vplan) = vector_plan {
            // Vector search detected: show vector-specific scan plan
            let inner_prefix = "  ".repeat(indent + 1);
            Self::append_scan_plan_lines(lines, &inner_prefix, vplan, None);
            lines.push(format!(
                "{}   Vector Access: runtime candidate (operator not instrumented)",
                inner_prefix
            ));
        } else if let Some(ref table_expr) = select.table_expr {
            let classification = super::query_classification::QueryClassification::classify(select);
            self.explain_table_expr_with_where(
                table_expr,
                select.where_clause.as_deref(),
                Some(&select.columns),
                !classification.has_window_functions
                    && !classification.has_aggregation
                    && !classification.has_group_by,
                lines,
                indent + 1,
                ctx,
            );
        }

        // GROUP BY (including ROLLUP, CUBE, GROUPING SETS)
        {
            let gb_str = format!("{}", select.group_by);
            if !gb_str.is_empty() {
                lines.push(format!("{}  Group By: {}", prefix, gb_str));
            }
        }

        // HAVING
        if let Some(ref having) = select.having {
            lines.push(format!("{}  Having: {}", prefix, having));
        }

        // ORDER BY
        if !select.order_by.is_empty() {
            let orders: Vec<String> = select.order_by.iter().map(|o| format!("{}", o)).collect();
            lines.push(format!("{}  Order By: {}", prefix, orders.join(", ")));
        }

        // LIMIT/OFFSET
        if let Some(ref limit) = select.limit {
            lines.push(format!("{}  Limit: {}", prefix, limit));
        }
        if let Some(ref offset) = select.offset {
            lines.push(format!("{}  Offset: {}", prefix, offset));
        }

        // Set operations
        if !select.set_operations.is_empty() {
            for set_op in &select.set_operations {
                lines.push(format!("{}  {}", prefix, set_op.operation));
                self.explain_select(&set_op.right, lines, indent + 2, ctx);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn explain_table_expr_with_where(
        &self,
        expr: &Expression,
        where_clause: Option<&Expression>,
        select_columns: Option<&[Expression]>,
        allow_index_nested_loop: bool,
        lines: &mut Vec<String>,
        indent: usize,
        ctx: &ExecutionContext,
    ) {
        let mut next_operator_id = 0_u32;
        self.explain_table_expr_inner(
            expr,
            where_clause,
            lines,
            indent,
            true,
            select_columns,
            allow_index_nested_loop,
            ctx,
            &mut next_operator_id,
            None,
        )
    }

    // The recursive plan walker deliberately carries rendering and binding
    // context explicitly so every recursive branch shares one live renderer.
    #[allow(clippy::too_many_arguments)]
    fn explain_table_expr_inner(
        &self,
        expr: &Expression,
        where_clause: Option<&Expression>,
        lines: &mut Vec<String>,
        indent: usize,
        show_join_cost: bool,
        select_columns: Option<&[Expression]>,
        allow_index_nested_loop: bool,
        ctx: &ExecutionContext,
        next_operator_id: &mut u32,
        parent_operator_id: Option<u32>,
    ) {
        let prefix = "  ".repeat(indent);

        match expr {
            Expression::TableSource(simple) => {
                // Try to get the table and analyze access plan
                if let Ok(tx) = self.engine.begin_transaction() {
                    if let Ok(table) = tx.get_table(&simple.name.value) {
                        // Build storage expression from WHERE clause for analysis
                        let storage_expr = if let Some(where_expr) = where_clause {
                            let schema = table.schema();
                            let (expr, _) = pushdown::try_pushdown(where_expr, schema, Some(ctx));
                            expr
                        } else {
                            None
                        };

                        // Get the scan plan
                        let scan_plan = table.explain_scan(storage_expr.as_deref());

                        // For SeqScan, use the AST expression's Display format instead of storage expr Debug
                        let scan_plan = match scan_plan {
                            ScanPlan::SeqScan { table, filter: _ } if where_clause.is_some() => {
                                ScanPlan::SeqScan {
                                    table,
                                    filter: Some(format!("{}", where_clause.unwrap())),
                                }
                            }
                            other => other,
                        };

                        Self::append_scan_plan_lines(lines, &prefix, &scan_plan, None);
                        Self::append_scan_projection_path_lines(
                            lines,
                            &prefix,
                            &scan_plan,
                            select_columns,
                        );

                        // Add alias if present
                        if let Some(ref alias) = simple.alias {
                            lines.push(format!("{}   Alias: {}", prefix, alias));
                        }

                        return;
                    }
                }

                // Fallback if table not found
                let mut table_info = format!("{}-> Seq Scan on {}", prefix, simple.name);
                if let Some(ref alias) = simple.alias {
                    table_info.push_str(&format!(" AS {}", alias));
                }
                lines.push(table_info);
                if let Some(ref where_expr) = where_clause {
                    lines.push(format!("{}   Filter: {}", prefix, where_expr));
                }
            }
            Expression::SubquerySource(subquery) => {
                let mut sub_info = format!("{}-> Subquery Scan", prefix);
                if let Some(ref alias) = subquery.alias {
                    sub_info.push_str(&format!(" AS {}", alias));
                }
                lines.push(sub_info);
                if let Some(wc) = where_clause {
                    lines.push(format!("{}     Filter: {}", prefix, wc));
                }
                self.explain_select(&subquery.subquery, lines, indent + 1, ctx);
            }
            Expression::JoinSource(join) => {
                let operator_id = *next_operator_id;
                *next_operator_id = next_operator_id.saturating_add(1);
                let (mut left_where, right_where, join_filter) = if let Some(wc) = where_clause {
                    partition_where_for_explain(
                        wc,
                        &join.left,
                        &join.right,
                        &join.join_type,
                        self.engine.as_ref(),
                    )
                } else {
                    (None, None, None)
                };
                // Determine join algorithm including INLJ check
                let (mut join_algorithm, mut lookup_strategy, mut lookup_unique) = self
                    .determine_join_algorithm_for_explain(
                        &join.left,
                        &join.right,
                        join.condition.as_deref(),
                        join.join_type.as_ref(),
                        &join.using_columns,
                        allow_index_nested_loop,
                    );
                // A scalar count over an INTEGER-PK equality can have its own
                // narrow semijoin executor before the general join planner.
                // Preserve that exact EXPLAIN classification without
                // re-enabling ordinary per-row index NL for other aggregates.
                let count_lookup_strategy = (!allow_index_nested_loop
                    && (is_count_star_select_for_explain(select_columns)
                        || count_qualified_column_for_explain(select_columns).is_some()))
                .then(|| {
                    self.determine_join_algorithm_for_explain(
                        &join.left,
                        &join.right,
                        join.condition.as_deref(),
                        join.join_type.as_ref(),
                        &join.using_columns,
                        true,
                    )
                    .1
                })
                .flatten();
                left_where = self.propagate_index_join_filter_for_explain(
                    join,
                    left_where,
                    right_where.as_ref(),
                    lookup_strategy.as_ref(),
                );
                let count_pk_semijoin = self.is_count_pk_semijoin_for_explain(
                    join,
                    select_columns,
                    left_where.as_ref(),
                    right_where.as_ref(),
                    join_filter.as_ref(),
                    count_lookup_strategy.as_ref().or(lookup_strategy.as_ref()),
                );
                if count_pk_semijoin {
                    join_algorithm = "Count PK Semi Join".to_string();
                    if count_lookup_strategy.is_some() {
                        lookup_strategy = count_lookup_strategy;
                    }
                    lookup_unique = true;
                }

                if show_join_cost {
                    // Get cost estimate from query planner
                    let planner = self.get_query_planner();
                    let left_table_name = extract_table_name(&join.left);
                    let right_table_name = extract_table_name(&join.right);

                    // Get table statistics for cost estimation
                    let left_stats = left_table_name
                        .as_ref()
                        .and_then(|name| planner.get_table_stats(name));
                    let right_stats = right_table_name
                        .as_ref()
                        .and_then(|name| planner.get_table_stats(name));

                    // Calculate estimated rows and cost
                    let (estimated_rows, estimated_cost) = match (left_stats, right_stats) {
                        (Some(ls), Some(rs)) => {
                            // Hash join cost estimation
                            let left_rows = ls.row_count.max(1);
                            let right_rows = rs.row_count.max(1);
                            // Simplified join cardinality estimate
                            let rows =
                                if join_algorithm == "Nested Loop" && join.condition.is_none() {
                                    // Cross join: left * right
                                    left_rows * right_rows
                                } else {
                                    // Equality join: estimate as smaller side (pessimistic)
                                    left_rows.min(right_rows)
                                };
                            // Cost = build cost + probe cost
                            let cost = if join_algorithm == "Hash Join" {
                                (left_rows.min(right_rows) as f64)
                                    + (left_rows.max(right_rows) as f64 * 0.1)
                            } else {
                                // Nested loop: O(n*m) but with early termination
                                (left_rows as f64) * (right_rows as f64).sqrt()
                            };
                            (rows, cost)
                        }
                        (Some(ls), None) => {
                            // Only left stats available
                            (ls.row_count, ls.row_count as f64 * 10.0)
                        }
                        (None, Some(rs)) => {
                            // Only right stats available
                            (rs.row_count, rs.row_count as f64 * 10.0)
                        }
                        (None, None) => {
                            // No stats - use default estimate
                            (1000, 10000.0)
                        }
                    };

                    // Show join algorithm, type, cost and rows
                    lines.push(format!(
                        "{}-> {} ({} Join) (cost={:.2} rows={})",
                        prefix, join_algorithm, join.join_type, estimated_cost, estimated_rows
                    ));
                } else {
                    // Plan-only mode (EXPLAIN ANALYZE child nodes): no cost estimates
                    lines.push(format!(
                        "{}-> {} ({} Join)",
                        prefix, join_algorithm, join.join_type
                    ));
                }
                let mut left_relations = rustc_hash::FxHashSet::default();
                let mut right_relations = rustc_hash::FxHashSet::default();
                collect_table_aliases(&join.left, &mut left_relations);
                collect_table_aliases(&join.right, &mut right_relations);
                let mut left_relations = left_relations.into_iter().collect::<Vec<_>>();
                let mut right_relations = right_relations.into_iter().collect::<Vec<_>>();
                left_relations.sort_unstable();
                right_relations.sort_unstable();
                lines.push(format!(
                    "{}   Physical Edge Identity: operator_id={}, parent_id={}, left_relations=[{}], right_relations=[{}], barrier={}",
                    prefix,
                    operator_id,
                    parent_operator_id.map_or_else(|| "none".to_string(), |id| id.to_string()),
                    left_relations.join(","),
                    right_relations.join(","),
                    explain_join_barrier(join),
                ));
                lines.push(format!(
                    "{}   Join Access Path: {}",
                    prefix,
                    if count_pk_semijoin {
                        "join.count_pk_semijoin"
                    } else {
                        Self::join_access_path_id(&join_algorithm, lookup_strategy.as_ref())
                    }
                ));
                Self::append_join_lookup_details(lines, &prefix, lookup_strategy.as_ref());
                if lookup_strategy.is_some() {
                    lines.push(format!(
                        "{}   Join Lookup Cardinality: {}",
                        prefix,
                        if lookup_unique { "0..1" } else { "0..N" }
                    ));
                }
                lines.push(format!(
                    "{}   Join Projection Boundary: {}",
                    prefix,
                    if count_pk_semijoin {
                        "join.count_pk_semijoin.scalar_count"
                    } else {
                        Self::classify_join_projection_boundary(
                            select_columns,
                            &join_algorithm,
                            join.condition.as_deref(),
                        )
                    }
                ));
                if count_pk_semijoin {
                    Self::append_count_pk_semijoin_details(lines, &prefix);
                }
                if let Some(ref condition) = join.condition {
                    lines.push(format!("{}   Join Cond: {}", prefix, condition));
                }
                if !join.using_columns.is_empty() {
                    let cols: Vec<String> =
                        join.using_columns.iter().map(|c| c.to_string()).collect();
                    lines.push(format!("{}   Using: ({})", prefix, cols.join(", ")));
                }
                if let Some(ref jf) = join_filter {
                    lines.push(format!("{}   Join Filter: {}", prefix, jf));
                }
                self.explain_table_expr_inner(
                    &join.left,
                    left_where.as_ref(),
                    lines,
                    indent + 1,
                    show_join_cost,
                    None,
                    allow_index_nested_loop,
                    ctx,
                    next_operator_id,
                    Some(operator_id),
                );
                self.explain_table_expr_inner(
                    &join.right,
                    right_where.as_ref(),
                    lines,
                    indent + 1,
                    show_join_cost,
                    None,
                    allow_index_nested_loop,
                    ctx,
                    next_operator_id,
                    Some(operator_id),
                );
            }
            Expression::CteReference(cte_ref) => {
                let mut cte_info = format!("{}-> CTE Scan on {}", prefix, cte_ref.name);
                if let Some(ref alias) = cte_ref.alias {
                    cte_info.push_str(&format!(" AS {}", alias));
                }
                lines.push(cte_info);
            }
            Expression::FunctionTableSource(fs) => {
                let mut info = format!("{}-> Function Scan on {}", prefix, fs.function.value);
                if let Some(ref alias) = fs.alias {
                    info.push_str(&format!(" AS {}", alias.value));
                }
                lines.push(info);
                if let Some(ref wc) = where_clause {
                    lines.push(format!("{}     Filter: {}", prefix, wc));
                }
            }
            Expression::ValuesSource(vs) => {
                let mut info = format!("{}-> Values Scan", prefix);
                if let Some(ref alias) = vs.alias {
                    info.push_str(&format!(" AS {}", alias));
                }
                lines.push(info);
                if let Some(ref wc) = where_clause {
                    lines.push(format!("{}     Filter: {}", prefix, wc));
                }
            }
            _ => {
                lines.push(format!("{}-> Scan: {}", prefix, expr));
            }
        }
    }

    fn classify_join_projection_boundary(
        select_columns: Option<&[Expression]>,
        join_algorithm: &str,
        join_condition: Option<&Expression>,
    ) -> &'static str {
        let Some(columns) = select_columns else {
            return "join.projection.unknown";
        };

        if columns
            .iter()
            .any(|expr| matches!(expr, Expression::Star(_) | Expression::QualifiedStar(_)))
        {
            return "join.projection.full_row";
        }

        if !columns.iter().all(Self::is_simple_join_projection_expr) {
            return "join.projection.fallback.complex_select";
        }

        let has_residual_on = join_condition
            .is_some_and(|condition| !Self::is_pure_equality_join_condition(condition));

        if join_algorithm.starts_with("Nested Loop") {
            return "join.projection.fused_after_predicate";
        }

        if has_residual_on {
            return "join.projection.fallback.residual_on";
        }

        if join_algorithm.starts_with("Index Nested Loop") {
            return "join.projection.fused_rowid_lookup";
        }

        if join_algorithm.starts_with("Hash Join") {
            return "join.projection.fused";
        }

        if join_algorithm.starts_with("Merge Join") {
            return "join.projection.fallback.merge";
        }

        "join.projection.unknown"
    }

    fn is_simple_join_projection_expr(expr: &Expression) -> bool {
        match expr {
            Expression::Identifier(_) | Expression::QualifiedIdentifier(_) => true,
            Expression::Aliased(aliased) => {
                matches!(
                    aliased.expression.as_ref(),
                    Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
                )
            }
            _ => false,
        }
    }

    fn is_pure_equality_join_condition(expr: &Expression) -> bool {
        match expr {
            Expression::Infix(infix) if infix.operator == "=" => {
                let left_is_col = matches!(
                    infix.left.as_ref(),
                    Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
                );
                let right_is_col = matches!(
                    infix.right.as_ref(),
                    Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
                );
                left_is_col && right_is_col
            }
            Expression::Infix(infix) if infix.operator.eq_ignore_ascii_case("AND") => {
                Self::is_pure_equality_join_condition(&infix.left)
                    && Self::is_pure_equality_join_condition(&infix.right)
            }
            _ => false,
        }
    }

    /// Mirror the runtime join-key equivalence pushdown in child scan plans.
    /// Without this step EXPLAIN labels the join as indexed but still displays
    /// the outer child as an unbounded scan, even though execution derives the
    /// equivalent predicate from the constrained inner join key.
    fn propagate_index_join_filter_for_explain(
        &self,
        join: &JoinTableSource,
        left_where: Option<Expression>,
        right_where: Option<&Expression>,
        lookup_strategy: Option<&IndexLookupStrategy>,
    ) -> Option<Expression> {
        if lookup_strategy.is_none() {
            return left_where;
        }
        let Some(right_filter) = right_where else {
            return left_where;
        };
        let left_alias = extract_table_alias(&join.left);
        let right_alias = extract_table_alias(&join.right);
        let Some((_, _, inner_column, outer_column, _)) = self.check_index_nested_loop_opportunity(
            &join.right,
            join.condition.as_deref(),
            &join.join_type,
            left_alias.as_deref(),
            right_alias.as_deref(),
        ) else {
            return left_where;
        };
        if !filter_references_column(right_filter, &inner_column) {
            return left_where;
        }
        let Some(outer_filter) =
            substitute_filter_column(right_filter, &inner_column, &outer_column)
        else {
            return left_where;
        };

        match left_where {
            Some(existing) => combine_predicates_with_and(vec![existing, outer_filter]),
            None => Some(outer_filter),
        }
    }

    /// Determine the join algorithm for EXPLAIN output
    /// This uses the same QueryPlanner logic as actual execution for consistency
    fn determine_join_algorithm_for_explain(
        &self,
        join_left: &Expression,
        join_right: &Expression,
        join_condition: Option<&Expression>,
        join_type: &str,
        using_columns: &[Identifier],
        allow_index_nested_loop: bool,
    ) -> (String, Option<IndexLookupStrategy>, bool) {
        // Check for INLJ opportunity on right side (default check)
        if let Some(cond) = join_condition.filter(|_| allow_index_nested_loop) {
            // Get aliases for the check
            let left_alias = extract_table_alias(join_left);
            let right_alias = extract_table_alias(join_right);
            let join_type_upper = join_type.to_uppercase();

            // Check if INLJ is possible (right side has index/PK for join column)
            let inlj_info = self.check_index_nested_loop_opportunity(
                join_right,
                Some(cond),
                &join_type_upper,
                left_alias.as_deref(),
                right_alias.as_deref(),
            );

            if let Some((_, strategy, _, _, lookup_unique)) = inlj_info {
                let algo_name = match &strategy {
                    IndexLookupStrategy::PrimaryKey => "Index Nested Loop (PK)".to_string(),
                    IndexLookupStrategy::SecondaryIndex(_)
                    | IndexLookupStrategy::SegmentedSecondaryIndex { .. } => {
                        "Index Nested Loop".to_string()
                    }
                };
                return (
                    format!("Runtime-adaptive Join (runtime candidate: {algo_name})"),
                    Some(strategy),
                    lookup_unique,
                );
            }

            // Check swapped direction for INLJ (left side has index/PK)
            let swapped_info = self.check_index_nested_loop_opportunity(
                join_left,
                Some(cond),
                &join_type_upper,
                right_alias.as_deref(),
                left_alias.as_deref(),
            );

            if let Some((_, strategy, _, _, lookup_unique)) = swapped_info {
                let algo_name = match &strategy {
                    IndexLookupStrategy::PrimaryKey => "Index Nested Loop (PK)".to_string(),
                    IndexLookupStrategy::SecondaryIndex(_)
                    | IndexLookupStrategy::SegmentedSecondaryIndex { .. } => {
                        "Index Nested Loop".to_string()
                    }
                };
                return (
                    format!("Runtime-adaptive Join (runtime candidate: {algo_name})"),
                    Some(strategy),
                    lookup_unique,
                );
            }
        }

        // Use QueryPlanner for algorithm selection (same as execution path)
        let planner = self.get_query_planner();
        let left_table_name = extract_table_name(join_left);
        let right_table_name = extract_table_name(join_right);

        // Get row counts from statistics (or use defaults)
        let left_rows = left_table_name
            .as_ref()
            .and_then(|name| planner.get_table_stats(name))
            .map(|s| s.row_count as usize)
            .unwrap_or(1000);
        let right_rows = right_table_name
            .as_ref()
            .and_then(|name| planner.get_table_stats(name))
            .map(|s| s.row_count as usize)
            .unwrap_or(1000);

        // Determine if we have equality keys
        let has_equality_keys = if let Some(cond) = join_condition {
            is_equality_condition(cond)
        } else {
            !using_columns.is_empty()
        };

        // Get the runtime join decision from QueryPlanner
        let decision = planner.plan_runtime_join_with_sort_info(
            left_rows,
            right_rows,
            has_equality_keys,
            false, // Can't determine sort status from AST alone
            false,
        );

        // Format the algorithm name with details
        let algo_name = match decision.algorithm {
            RuntimeJoinAlgorithm::HashJoin => {
                if decision.swap_sides {
                    "Hash Join (build: right)".to_string()
                } else {
                    "Hash Join (build: left)".to_string()
                }
            }
            RuntimeJoinAlgorithm::MergeJoin => "Merge Join".to_string(),
            RuntimeJoinAlgorithm::NestedLoop => "Nested Loop".to_string(),
        };

        (
            format!("Runtime-adaptive Join (runtime candidate: {algo_name})"),
            None,
            false,
        )
    }

    fn join_access_path_id(
        join_algorithm: &str,
        lookup_strategy: Option<&IndexLookupStrategy>,
    ) -> &'static str {
        match lookup_strategy {
            Some(IndexLookupStrategy::PrimaryKey) => "join.index_nested_loop.pk",
            Some(IndexLookupStrategy::SecondaryIndex(_))
            | Some(IndexLookupStrategy::SegmentedSecondaryIndex { .. }) => {
                "join.index_nested_loop.secondary"
            }
            None if join_algorithm.contains("Hash Join") => "join.hash.candidate",
            None if join_algorithm.contains("Merge Join") => "join.merge.candidate",
            None if join_algorithm.contains("Nested Loop") => "join.nested_loop.candidate",
            None => "join.unknown",
        }
    }

    fn append_join_lookup_details(
        lines: &mut Vec<String>,
        prefix: &str,
        lookup_strategy: Option<&IndexLookupStrategy>,
    ) {
        match lookup_strategy {
            Some(IndexLookupStrategy::PrimaryKey) => {
                lines.push(format!("{}   Join Lookup: primary_key", prefix));
            }
            Some(IndexLookupStrategy::SecondaryIndex(index)) => {
                lines.push(format!("{}   Join Lookup Index: {}", prefix, index.name()));
            }
            Some(IndexLookupStrategy::SegmentedSecondaryIndex {
                column_name,
                index_name,
            }) => {
                lines.push(format!("{}   Join Lookup Index: {}", prefix, index_name));
                lines.push(format!("{}   Join Lookup Column: {}", prefix, column_name));
                lines.push(format!("{}   Join Lookup Source: hot+cold", prefix));
            }
            None => {}
        }
    }

    fn is_count_pk_semijoin_for_explain(
        &self,
        join: &JoinTableSource,
        select_columns: Option<&[Expression]>,
        left_filter: Option<&Expression>,
        right_filter: Option<&Expression>,
        join_filter: Option<&Expression>,
        lookup_strategy: Option<&IndexLookupStrategy>,
    ) -> bool {
        if !join.using_columns.is_empty()
            || !is_single_equality_join_condition_for_explain(join.condition.as_deref())
            || right_filter.is_some()
            || join_filter.is_some()
            || !matches!(lookup_strategy, Some(IndexLookupStrategy::PrimaryKey))
        {
            return false;
        }

        let (Some(left), Some(right)) = (
            base_table_source_for_explain(&join.left),
            base_table_source_for_explain(&join.right),
        ) else {
            return false;
        };
        if left.as_of.is_some() || right.as_of.is_some() {
            return false;
        }

        let Ok(txn) = self.engine.begin_transaction() else {
            return false;
        };
        let Ok(left_table) = txn.get_table(&left.name.value_lower) else {
            return false;
        };
        let Ok(right_table) = txn.get_table(&right.name.value_lower) else {
            return false;
        };
        let right_schema = right_table.schema();
        let Some(pk_index) = right_schema.pk_column_index() else {
            return false;
        };
        if right_schema.columns[pk_index].data_type != radixdb_core::DataType::Integer {
            return false;
        }
        let count_star = is_count_star_select_for_explain(select_columns);
        let count_column = count_qualified_column_for_explain(select_columns);
        let count_supported = if join.join_type.eq_ignore_ascii_case("INNER") && count_star {
            true
        } else if (join.join_type.eq_ignore_ascii_case("INNER")
            || join.join_type.eq_ignore_ascii_case("LEFT"))
            && count_column.is_some()
        {
            let counted = count_column.expect("count column checked");
            let right_alias = right
                .alias
                .as_ref()
                .map_or(right.name.value.as_str(), |alias| alias.value.as_str());
            counted.qualifier.value.eq_ignore_ascii_case(right_alias)
                && right_schema
                    .find_column(&counted.name.value_lower)
                    .is_some_and(|(_, column)| !column.nullable)
        } else {
            false
        };
        if !count_supported {
            return false;
        }
        if let Some(filter) = left_filter {
            let (_, needs_memory_filter) =
                pushdown::try_pushdown(filter, left_table.schema(), None);
            if needs_memory_filter {
                return false;
            }
        }
        true
    }

    /// Explain the physical boundary of the narrow count-only path without
    /// pretending that a plan-only EXPLAIN has runtime cardinalities. Actual
    /// batch/key/hit counters are published by EXPLAIN ANALYZE's execution
    /// instrumentation and by the isolated benchmark report.
    fn append_count_pk_semijoin_details(lines: &mut Vec<String>, prefix: &str) {
        lines.push(format!(
            "{prefix}   Child Projection: exact filter dependencies + parent join key"
        ));
        lines.push(format!(
            "{prefix}   Parent Membership: INTEGER PRIMARY KEY, metadata-only bounded batches"
        ));
        lines.push(format!(
            "{prefix}   Runtime Counters: child keys, PK batches/keys/hits, parent payload rows, joined rows"
        ));
    }

    /// Detect if a SELECT statement would use the vector search fast path.
    /// Returns the appropriate ScanPlan (VectorSearch or VectorBruteForce) if detected.
    fn detect_vector_search_plan(
        &self,
        select: &SelectStatement,
        table_name: &str,
    ) -> Option<ScanPlan> {
        // Same conditions as the execution fast path in query.rs
        if select.limit.is_none()
            || select.order_by.len() != 1
            || !select.group_by.columns.is_empty()
            || select.having.is_some()
            || select.distinct
        {
            return None;
        }

        let order_by = &select.order_by[0];
        if !order_by.ascending {
            return None;
        }

        // Find VEC_DISTANCE function: direct call, alias, or <=> operator
        let (fn_name_upper, vec_col_name) = match &order_by.expression {
            Expression::FunctionCall(fc) => {
                let name = fc.function.to_uppercase();
                if !matches!(
                    name.as_str(),
                    "VEC_DISTANCE_L2" | "VEC_DISTANCE_COSINE" | "VEC_DISTANCE_IP"
                ) {
                    return None;
                }
                if fc.arguments.len() != 2 {
                    return None;
                }
                let col = Self::extract_vec_col_name(&fc.arguments[0])?;
                (name.to_string(), col)
            }
            Expression::Identifier(id) => {
                // Alias lookup in SELECT columns
                if let Some(fc) =
                    Self::find_vec_distance_alias_for_explain(&id.value_lower, &select.columns)
                {
                    let name = fc.function.to_uppercase();
                    let col = Self::extract_vec_col_name(&fc.arguments[0])?;
                    (name.to_string(), col)
                } else {
                    let infix = Self::find_vec_distance_infix_alias_for_explain(
                        &id.value_lower,
                        &select.columns,
                    )?;
                    // <=> operator is equivalent to VEC_DISTANCE_L2
                    let col = Self::extract_vec_col_name(&infix.left)?;
                    ("VEC_DISTANCE_L2".to_string(), col)
                }
            }
            Expression::Infix(infix) if infix.op_type == InfixOperator::VectorDistance => {
                let col = Self::extract_vec_col_name(&infix.left)?;
                ("VEC_DISTANCE_L2".to_string(), col)
            }
            _ => return None,
        };

        let metric = match fn_name_upper.as_str() {
            "VEC_DISTANCE_L2" => "L2",
            "VEC_DISTANCE_COSINE" => "Cosine",
            "VEC_DISTANCE_IP" => "InnerProduct",
            _ => return None,
        };

        // Extract k from LIMIT
        let k = Self::extract_limit_value(select)?;

        // Check for WHERE clause filter text
        let filter = select.where_clause.as_ref().map(|w| format!("{}", w));

        // Ordinary SQL requires exact Top-K. HNSW remains available only as an
        // explicit approximate index API, so EXPLAIN must describe the exact
        // vector scan selected by execution even when an HNSW index exists.
        Some(ScanPlan::VectorBruteForce {
            table: table_name.to_string(),
            vector_column: vec_col_name,
            metric: metric.to_string(),
            k,
            filter,
        })
    }

    /// Extract vector column name from a function argument expression
    fn extract_vec_col_name(expr: &Expression) -> Option<String> {
        match expr {
            Expression::Identifier(id) => Some(id.value.to_string()),
            Expression::QualifiedIdentifier(qid) => Some(qid.name.value.to_string()),
            _ => None,
        }
    }

    /// Find a VEC_DISTANCE function call aliased in SELECT columns (for EXPLAIN)
    fn find_vec_distance_alias_for_explain<'a>(
        alias_lower: &str,
        columns: &'a [Expression],
    ) -> Option<&'a FunctionCall> {
        for col_expr in columns {
            if let Expression::Aliased(aliased) = col_expr {
                if aliased.alias.value_lower == alias_lower {
                    if let Expression::FunctionCall(fc) = &*aliased.expression {
                        let fn_upper = fc.function.to_uppercase();
                        if matches!(
                            fn_upper.as_str(),
                            "VEC_DISTANCE_L2" | "VEC_DISTANCE_COSINE" | "VEC_DISTANCE_IP"
                        ) && fc.arguments.len() == 2
                        {
                            return Some(fc.as_ref());
                        }
                    }
                }
            }
        }
        None
    }

    /// Find a <=> infix operator aliased in SELECT columns (for EXPLAIN)
    fn find_vec_distance_infix_alias_for_explain<'a>(
        alias_lower: &str,
        columns: &'a [Expression],
    ) -> Option<&'a InfixExpression> {
        for col_expr in columns {
            if let Expression::Aliased(aliased) = col_expr {
                if aliased.alias.value_lower == alias_lower {
                    if let Expression::Infix(infix) = &*aliased.expression {
                        if infix.op_type == InfixOperator::VectorDistance {
                            return Some(infix);
                        }
                    }
                }
            }
        }
        None
    }

    /// Extract integer limit value from a SELECT statement (simple cases only)
    fn extract_limit_value(select: &SelectStatement) -> Option<usize> {
        let limit_expr = select.limit.as_deref()?;
        match limit_expr {
            Expression::IntegerLiteral(lit) => {
                if lit.value >= 0 {
                    let k = lit.value as usize;
                    if let Some(ref offset_expr) = select.offset {
                        if let Expression::IntegerLiteral(off) = &**offset_expr {
                            if off.value >= 0 {
                                return Some(k.saturating_add(off.value as usize));
                            }
                        }
                    }
                    Some(k)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
