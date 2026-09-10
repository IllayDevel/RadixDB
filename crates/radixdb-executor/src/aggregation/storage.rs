use super::*;

impl<'host, H: AggregationHost + ?Sized> AggregationExecutor<'host, H> {
    pub(super) fn storage_aggregate_expr_name(func_name: &str, fc: &FunctionCall) -> String {
        if fc.arguments.is_empty() || matches!(fc.arguments.first(), Some(Expression::Star(_))) {
            format!("{}(*)", func_name)
        } else if let Some(Expression::Identifier(ident)) = fc.arguments.first() {
            format!("{}({})", func_name, ident.value)
        } else {
            format!("{}(?)", func_name)
        }
    }

    pub(super) fn storage_aggregate_from_call(
        fc: &FunctionCall,
        col_map: &FxHashMap<&str, usize>,
    ) -> Option<(
        String,
        radixdb_storage::mvcc::version_store::AggregateOp,
        usize,
    )> {
        use radixdb_storage::mvcc::version_store::AggregateOp;

        if fc.filter.is_some() || fc.is_distinct || !fc.order_by.is_empty() {
            return None;
        }

        let func_name = fc.function.to_uppercase();
        let (op, col_idx) = match func_name.as_str() {
            "COUNT" => {
                if fc.arguments.is_empty()
                    || matches!(fc.arguments.first(), Some(Expression::Star(_)))
                {
                    (AggregateOp::CountStar, 0)
                } else if let Some(Expression::Identifier(ident)) = fc.arguments.first() {
                    let col_name = ident.value.to_lowercase();
                    (AggregateOp::Count, *col_map.get(col_name.as_str())?)
                } else {
                    return None;
                }
            }
            "SUM" => {
                if let Some(Expression::Identifier(ident)) = fc.arguments.first() {
                    let col_name = ident.value.to_lowercase();
                    (AggregateOp::Sum, *col_map.get(col_name.as_str())?)
                } else {
                    return None;
                }
            }
            "AVG" => {
                if let Some(Expression::Identifier(ident)) = fc.arguments.first() {
                    let col_name = ident.value.to_lowercase();
                    (AggregateOp::Avg, *col_map.get(col_name.as_str())?)
                } else {
                    return None;
                }
            }
            "MIN" => {
                if let Some(Expression::Identifier(ident)) = fc.arguments.first() {
                    let col_name = ident.value.to_lowercase();
                    (AggregateOp::Min, *col_map.get(col_name.as_str())?)
                } else {
                    return None;
                }
            }
            "MAX" => {
                if let Some(Expression::Identifier(ident)) = fc.arguments.first() {
                    let col_name = ident.value.to_lowercase();
                    (AggregateOp::Max, *col_map.get(col_name.as_str())?)
                } else {
                    return None;
                }
            }
            _ => return None,
        };

        Some((
            Self::storage_aggregate_expr_name(&func_name, fc),
            op,
            col_idx,
        ))
    }

    pub(super) fn collect_storage_aggregate_dependencies(
        expr: &Expression,
        col_map: &FxHashMap<&str, usize>,
        out: &mut Vec<(
            String,
            radixdb_storage::mvcc::version_store::AggregateOp,
            usize,
        )>,
    ) -> bool {
        match expr {
            Expression::FunctionCall(fc) => {
                if is_aggregate_function(&fc.function) {
                    if let Some(dep) = Self::storage_aggregate_from_call(fc, col_map) {
                        out.push(dep);
                        true
                    } else {
                        false
                    }
                } else {
                    fc.arguments
                        .iter()
                        .all(|arg| Self::collect_storage_aggregate_dependencies(arg, col_map, out))
                        && fc.filter.as_ref().is_none_or(|filter| {
                            Self::collect_storage_aggregate_dependencies(filter, col_map, out)
                        })
                        && fc.order_by.iter().all(|order| {
                            Self::collect_storage_aggregate_dependencies(
                                &order.expression,
                                col_map,
                                out,
                            )
                        })
                }
            }
            Expression::Aliased(aliased) => {
                Self::collect_storage_aggregate_dependencies(&aliased.expression, col_map, out)
            }
            Expression::Infix(infix) => {
                Self::collect_storage_aggregate_dependencies(&infix.left, col_map, out)
                    && Self::collect_storage_aggregate_dependencies(&infix.right, col_map, out)
            }
            Expression::Prefix(prefix) => {
                Self::collect_storage_aggregate_dependencies(&prefix.right, col_map, out)
            }
            Expression::Distinct(distinct) => {
                Self::collect_storage_aggregate_dependencies(&distinct.expr, col_map, out)
            }
            Expression::In(in_expr) => {
                Self::collect_storage_aggregate_dependencies(&in_expr.left, col_map, out)
                    && Self::collect_storage_aggregate_dependencies(&in_expr.right, col_map, out)
            }
            Expression::InHashSet(in_expr) => {
                Self::collect_storage_aggregate_dependencies(&in_expr.column, col_map, out)
            }
            Expression::Between(between) => {
                Self::collect_storage_aggregate_dependencies(&between.expr, col_map, out)
                    && Self::collect_storage_aggregate_dependencies(&between.lower, col_map, out)
                    && Self::collect_storage_aggregate_dependencies(&between.upper, col_map, out)
            }
            Expression::Like(like) => {
                Self::collect_storage_aggregate_dependencies(&like.left, col_map, out)
                    && Self::collect_storage_aggregate_dependencies(&like.pattern, col_map, out)
                    && like.escape.as_ref().is_none_or(|escape| {
                        Self::collect_storage_aggregate_dependencies(escape, col_map, out)
                    })
            }
            Expression::List(list) => list
                .elements
                .iter()
                .all(|item| Self::collect_storage_aggregate_dependencies(item, col_map, out)),
            Expression::ExpressionList(list) => list
                .expressions
                .iter()
                .all(|item| Self::collect_storage_aggregate_dependencies(item, col_map, out)),
            Expression::Case(case) => {
                case.value.as_ref().is_none_or(|value| {
                    Self::collect_storage_aggregate_dependencies(value, col_map, out)
                }) && case.when_clauses.iter().all(|when| {
                    Self::collect_storage_aggregate_dependencies(&when.condition, col_map, out)
                        && Self::collect_storage_aggregate_dependencies(
                            &when.then_result,
                            col_map,
                            out,
                        )
                }) && case.else_value.as_ref().is_none_or(|else_value| {
                    Self::collect_storage_aggregate_dependencies(else_value, col_map, out)
                })
            }
            Expression::Cast(cast) => {
                Self::collect_storage_aggregate_dependencies(&cast.expr, col_map, out)
            }
            Expression::AllAny(_)
            | Expression::Exists(_)
            | Expression::ScalarSubquery(_)
            | Expression::Window(_)
            | Expression::TableSource(_)
            | Expression::JoinSource(_)
            | Expression::SubquerySource(_)
            | Expression::ValuesSource(_)
            | Expression::CteReference(_)
            | Expression::FunctionTableSource(_) => false,
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
            | Expression::Star(_)
            | Expression::QualifiedStar(_)
            | Expression::Default(_) => true,
        }
    }

    /// Try to use storage-level aggregation for GROUP BY queries.
    ///
    /// This optimization bypasses row materialization by computing aggregates
    /// directly from arena storage using Arc::clone for group keys.
    ///
    /// Returns None if the optimization cannot be applied.
    ///
    /// Currently only applies to simple queries with:
    /// - GROUP BY columns that match SELECT identifiers exactly (same order)
    /// - Simple aggregates (COUNT, SUM, AVG, MIN, MAX) on column references
    /// - No ROLLUP, CUBE, or GROUPING SETS
    /// - Optional WHERE if it can be fully converted to a storage expression
    /// - Optional HAVING that can be evaluated against the grouped result row
    pub fn try_storage_aggregation(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        all_columns: &[String],
        classification: &QueryClassification,
    ) -> Option<Box<dyn QueryResult>> {
        use radixdb_sql::ast::GroupByModifier;
        use radixdb_storage::mvcc::version_store::AggregateOp;

        // HAVING can be applied after the storage-level grouped rows are
        // produced if it references columns present in that small result row.
        if !classification.has_group_by {
            return None;
        }
        if classification.where_has_parameters || classification.where_has_subqueries {
            return None;
        }

        // Only for simple GROUP BY (no ROLLUP, CUBE, or GROUPING SETS)
        if !matches!(stmt.group_by.modifier, GroupByModifier::None) {
            return None;
        }

        // Only for simple GROUP BY expressions (column references)
        let group_by_cols = &stmt.group_by.columns;
        if group_by_cols.is_empty() {
            return None;
        }

        // Build column name -> index map
        let col_map: FxHashMap<&str, usize> = all_columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();

        // Extract group-by column names and indices (in GROUP BY order)
        let mut group_by_indices: Vec<usize> = Vec::new();
        let mut group_by_col_names: Vec<String> = Vec::new();
        for expr in group_by_cols {
            match expr {
                Expression::Identifier(ident) => {
                    let col_name = ident.value.to_lowercase().to_string();
                    if let Some(&idx) = col_map.get(col_name.as_str()) {
                        group_by_indices.push(idx);
                        group_by_col_names.push(col_name);
                    } else {
                        return None; // Unknown column
                    }
                }
                _ => return None, // Non-column GROUP BY not supported
            }
        }

        // Parse SELECT columns - identify which are group-by columns vs aggregates
        // Track positions so we can verify GROUP BY columns come first
        let mut select_group_count = 0;
        let mut seen_aggregate = false;
        let mut aggregates: Vec<(AggregateOp, usize)> = Vec::new();
        let mut agg_aliases: Vec<(String, usize)> = Vec::new();
        let mut result_columns: Vec<String> = Vec::new();

        for col_expr in &stmt.columns {
            match col_expr {
                Expression::Identifier(ident) => {
                    // This must be a GROUP BY column
                    let col_name_lower = ident.value.to_lowercase().to_string();
                    if !group_by_col_names.contains(&col_name_lower) {
                        return None; // Column not in GROUP BY
                    }
                    if seen_aggregate {
                        // GROUP BY columns must come before aggregates for this optimization
                        return None;
                    }
                    if group_by_col_names.get(select_group_count) != Some(&col_name_lower) {
                        return None; // SELECT group columns must match GROUP BY order
                    }
                    select_group_count += 1;
                    result_columns.push(ident.value.to_string());
                }
                Expression::FunctionCall(fc) => {
                    seen_aggregate = true;
                    let (expr_name, op, col_idx) = Self::storage_aggregate_from_call(fc, &col_map)?;
                    let agg_idx = aggregates.len();
                    agg_aliases.push((expr_name.clone(), group_by_indices.len() + agg_idx));
                    aggregates.push((op, col_idx));

                    result_columns.push(expr_name);
                }
                Expression::Aliased(aliased) => {
                    // Handle aliased identifier (group by column with alias)
                    if let Expression::Identifier(ident) = aliased.expression.as_ref() {
                        let col_name_lower = ident.value.to_lowercase().to_string();
                        if !group_by_col_names.contains(&col_name_lower) {
                            return None; // Column not in GROUP BY
                        }
                        if seen_aggregate {
                            return None;
                        }
                        if group_by_col_names.get(select_group_count) != Some(&col_name_lower) {
                            return None; // SELECT group columns must match GROUP BY order
                        }
                        select_group_count += 1;
                        result_columns.push(aliased.alias.value.to_string());
                    }
                    // Handle aliased aggregate
                    else if let Expression::FunctionCall(fc) = aliased.expression.as_ref() {
                        seen_aggregate = true;
                        let (expr_name, op, col_idx) =
                            Self::storage_aggregate_from_call(fc, &col_map)?;
                        let agg_idx = aggregates.len();
                        agg_aliases.push((expr_name, group_by_indices.len() + agg_idx));
                        aggregates.push((op, col_idx));
                        result_columns.push(aliased.alias.value.to_string());
                    } else {
                        return None; // Other aliased expression not supported
                    }
                }
                _ => return None, // Unsupported expression type
            }
        }

        // SELECT must include at least all GROUP BY columns (can have more)
        // but for simplicity, require exact match with GROUP BY column count
        if select_group_count != group_by_col_names.len() {
            return None;
        }

        let public_column_count = result_columns.len();
        let public_aggregate_count = aggregates.len();
        for (idx, col_name) in group_by_col_names.iter().enumerate() {
            if result_columns
                .get(idx)
                .is_some_and(|name| !name.eq_ignore_ascii_case(col_name))
            {
                agg_aliases.push((col_name.clone(), idx));
            }
        }
        let mut known_agg_aliases: FxHashSet<String> = agg_aliases
            .iter()
            .map(|(name, _)| name.to_lowercase())
            .collect();
        let mut retained_order_dependencies = Vec::new();

        // ORDER BY may reference an aggregate that is intentionally absent
        // from the SELECT list. Keep direct hidden aggregates in the storage
        // result until the outer ORDER BY has consumed them; the public
        // projection removes them afterwards. More complex expressions still
        // use the canonical aggregation path, which can rewrite aggregate
        // subexpressions before evaluation.
        for order_by in &stmt.order_by {
            let mut hidden_deps = Vec::new();
            if !Self::collect_storage_aggregate_dependencies(
                &order_by.expression,
                &col_map,
                &mut hidden_deps,
            ) {
                return None;
            }
            let has_new_dependency = hidden_deps
                .iter()
                .any(|(name, _, _)| !known_agg_aliases.contains(&name.to_lowercase()));
            if has_new_dependency
                && !matches!(
                    &order_by.expression,
                    Expression::FunctionCall(function)
                        if is_aggregate_function(&function.function)
                )
            {
                return None;
            }
            for (expr_name, op, col_idx) in hidden_deps {
                if !known_agg_aliases.insert(expr_name.to_lowercase()) {
                    continue;
                }
                let agg_idx = aggregates.len();
                agg_aliases.push((expr_name.clone(), group_by_indices.len() + agg_idx));
                aggregates.push((op, col_idx));
                retained_order_dependencies.push(expr_name);
            }
        }

        if let Some(having) = stmt.having.as_ref() {
            let mut hidden_deps = Vec::new();
            if !Self::collect_storage_aggregate_dependencies(having, &col_map, &mut hidden_deps) {
                return None;
            }
            for (expr_name, op, col_idx) in hidden_deps {
                if !known_agg_aliases.insert(expr_name.to_lowercase()) {
                    continue;
                }
                let agg_idx = aggregates.len();
                agg_aliases.push((expr_name, group_by_indices.len() + agg_idx));
                aggregates.push((op, col_idx));
            }
        }

        let where_storage_expr = if let Some(where_expr) = stmt.where_clause.as_ref() {
            let (storage_expr, needs_memory_filter) =
                crate::pushdown::try_pushdown(where_expr, table.schema(), Some(ctx));
            if needs_memory_filter {
                return None;
            }
            Some(storage_expr?)
        } else {
            None
        };

        // Call storage-level aggregation
        let results = if let Some(where_expr) = where_storage_expr.as_ref() {
            table.compute_filtered_grouped_aggregates(
                &group_by_indices,
                &aggregates,
                where_expr.as_ref(),
            )?
        } else {
            table.compute_grouped_aggregates(&group_by_indices, &aggregates)?
        };

        // Convert to rows
        let mut rows = RowVec::new();
        for (row_id, r) in results.into_iter().enumerate() {
            let mut values = r.group_values;
            values.extend(r.aggregate_values);
            rows.push((
                row_id as i64,
                Row::from_compact_vec(CompactVec::from_vec(values)),
            ));
        }

        if let Some(having) = stmt.having.as_ref() {
            let having_filter =
                RowFilter::with_aliases_and_context(having, &result_columns, &agg_aliases, ctx)
                    .ok()?;
            let mut filtered_rows = RowVec::new();
            for (_, row) in rows {
                if having_filter.matches_checked(&row).ok()? {
                    let row_id = filtered_rows.len() as i64;
                    filtered_rows.push((row_id, row));
                }
            }
            rows = filtered_rows;
        }

        let retained_column_count = public_column_count + retained_order_dependencies.len();
        if aggregates.len() > public_aggregate_count + retained_order_dependencies.len() {
            let mut projected_rows = RowVec::with_capacity(rows.len());
            for (_, row) in rows {
                let mut values = CompactVec::with_capacity(retained_column_count);
                for value in row.as_slice().iter().take(retained_column_count) {
                    values.push(value.clone());
                }
                let row_id = projected_rows.len() as i64;
                projected_rows.push((row_id, Row::from_compact_vec(values)));
            }
            rows = projected_rows;
        }

        result_columns.extend(retained_order_dependencies);
        result_columns.truncate(retained_column_count);
        Some(Box::new(ExecutorResult::new(result_columns, rows)))
    }

    /// Try fast COUNT(DISTINCT col) using compiled cache
    ///
    /// This is a compiled fast path for simple `SELECT COUNT(DISTINCT col) FROM table` queries.
    /// On first execution, it analyzes and compiles the query pattern.
    /// On subsequent executions, it skips parsing and directly fetches the distinct count.
    ///
    /// # Arguments
    /// * `stmt` - The SELECT statement
    /// * `compiled` - The compiled execution state (shared via RwLock)
    ///
    /// # Returns
    /// * `Some(Ok(result))` - Query succeeded via fast path
    /// * `Some(Err(e))` - Query failed
    /// * `None` - Query doesn't qualify for this fast path (use normal path)
    pub(crate) fn try_fast_count_distinct_compiled(
        &self,
        stmt: &SelectStatement,
        compiled: &RwLock<CompiledExecution>,
    ) -> Option<Result<Box<dyn QueryResult>>> {
        // Quick reject: must not be in an explicit transaction (for simplicity)
        {
            let active_tx = match self.host.aggregation_active_transaction().try_lock() {
                Ok(guard) => guard,
                Err(_) => return None,
            };
            if active_tx.is_some() {
                return None;
            }
        }

        // Try read lock first - check if already compiled
        {
            let compiled_guard = match compiled.read() {
                Ok(guard) => guard,
                Err(_) => return None,
            };
            match &*compiled_guard {
                CompiledExecution::NotOptimizable(epoch)
                    if self.host.aggregation_engine().schema_epoch() == *epoch =>
                {
                    return None
                }
                CompiledExecution::CountDistinct(cd) => {
                    // Fast path: validate epoch and execute directly
                    if self.host.aggregation_engine().schema_epoch() == cd.cached_epoch {
                        return Some(self.execute_compiled_count_distinct(cd));
                    }
                    // Schema changed - fall through to recompile
                }
                CompiledExecution::NotOptimizable(_) | CompiledExecution::Unknown => {} // Epoch changed or first run - fall through to recompile
                // Other variants - not a COUNT DISTINCT query
                _ => return None,
            }
        }

        // First execution or schema changed - compile and cache (write lock)
        self.compile_and_execute_count_distinct(stmt, compiled)
    }

    /// Execute using pre-compiled COUNT(DISTINCT col) info
    pub(super) fn execute_compiled_count_distinct(
        &self,
        cd: &CompiledCountDistinct,
    ) -> Result<Box<dyn QueryResult>> {
        // Get table and count distinct values directly
        let tx = self.host.aggregation_engine().begin_transaction()?;
        let table = tx.get_table(&cd.table_name)?;

        let count = table
            .get_partition_count(&cd.column_name)
            .ok_or_else(|| radixdb_core::Error::internal("Index no longer available for column"))?;

        // Build result
        let mut result_values = CompactVec::with_capacity(1);
        result_values.push(Value::Integer(count as i64));
        let row = Row::from_compact_vec(result_values);
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, row));

        Ok(Box::new(ExecutorResult::new(
            vec![cd.result_column_name.clone()],
            rows,
        )))
    }

    /// Compile and execute COUNT(DISTINCT col), caching the compiled state
    pub(super) fn compile_and_execute_count_distinct(
        &self,
        stmt: &SelectStatement,
        compiled: &RwLock<CompiledExecution>,
    ) -> Option<Result<Box<dyn QueryResult>>> {
        use radixdb_core::SmartString;

        // Acquire write lock
        let mut compiled_guard = match compiled.write() {
            Ok(guard) => guard,
            Err(_) => return None,
        };

        // Double-check (another thread may have compiled while we waited)
        match &*compiled_guard {
            CompiledExecution::NotOptimizable(epoch)
                if self.host.aggregation_engine().schema_epoch() == *epoch =>
            {
                return None
            }
            CompiledExecution::CountDistinct(cd) => {
                if self.host.aggregation_engine().schema_epoch() == cd.cached_epoch {
                    return Some(self.execute_compiled_count_distinct(cd));
                }
                // Schema changed, continue to recompile
            }
            CompiledExecution::NotOptimizable(_) | CompiledExecution::Unknown => {} // Epoch changed or first run - recompile
            _ => return None,
        }

        // Pattern detection: SELECT COUNT(DISTINCT col) FROM table
        // Must have:
        // - Exactly one column expression
        // - That column is COUNT(DISTINCT col) function call
        // - No WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, CTEs, set operations
        // - Single table source (no joins)

        if stmt.columns.len() != 1 {
            *compiled_guard =
                CompiledExecution::NotOptimizable(self.host.aggregation_engine().schema_epoch());
            return None;
        }

        // Check for disqualifying clauses
        if stmt.where_clause.is_some()
            || !stmt.group_by.columns.is_empty()
            || stmt.having.is_some()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
            || stmt.offset.is_some()
            || stmt.with.is_some()
            || !stmt.set_operations.is_empty()
        {
            *compiled_guard =
                CompiledExecution::NotOptimizable(self.host.aggregation_engine().schema_epoch());
            return None;
        }

        // Extract COUNT(DISTINCT col) pattern
        let (column_name, result_column_name) = match &stmt.columns[0] {
            Expression::FunctionCall(func) => {
                // Must be COUNT function
                if func.function.to_uppercase() != "COUNT" {
                    *compiled_guard = CompiledExecution::NotOptimizable(
                        self.host.aggregation_engine().schema_epoch(),
                    );
                    return None;
                }
                // Must be DISTINCT - if not, don't mark as NotOptimizable
                // because COUNT(*) fast path might handle it
                if !func.is_distinct {
                    return None;
                }
                if func.arguments.len() != 1 {
                    *compiled_guard = CompiledExecution::NotOptimizable(
                        self.host.aggregation_engine().schema_epoch(),
                    );
                    return None;
                }
                // Get column name from argument
                let col = match &func.arguments[0] {
                    Expression::Identifier(ident) => ident.value.to_lowercase(),
                    _ => {
                        *compiled_guard = CompiledExecution::NotOptimizable(
                            self.host.aggregation_engine().schema_epoch(),
                        );
                        return None;
                    }
                };
                let result_name = format!("COUNT(DISTINCT {})", col);
                (col, result_name)
            }
            Expression::Aliased(aliased) => {
                // Handle COUNT(DISTINCT col) AS alias
                match aliased.expression.as_ref() {
                    Expression::FunctionCall(func) => {
                        // Must be COUNT function
                        if func.function.to_uppercase() != "COUNT" {
                            *compiled_guard = CompiledExecution::NotOptimizable(
                                self.host.aggregation_engine().schema_epoch(),
                            );
                            return None;
                        }
                        // Must be DISTINCT - if not, don't mark as NotOptimizable
                        // because COUNT(*) fast path might handle it
                        if !func.is_distinct {
                            return None;
                        }
                        if func.arguments.len() != 1 {
                            *compiled_guard = CompiledExecution::NotOptimizable(
                                self.host.aggregation_engine().schema_epoch(),
                            );
                            return None;
                        }
                        let col = match &func.arguments[0] {
                            Expression::Identifier(ident) => ident.value.to_lowercase(),
                            _ => {
                                *compiled_guard = CompiledExecution::NotOptimizable(
                                    self.host.aggregation_engine().schema_epoch(),
                                );
                                return None;
                            }
                        };
                        (col, aliased.alias.value.to_string())
                    }
                    _ => {
                        *compiled_guard = CompiledExecution::NotOptimizable(
                            self.host.aggregation_engine().schema_epoch(),
                        );
                        return None;
                    }
                }
            }
            _ => {
                *compiled_guard = CompiledExecution::NotOptimizable(
                    self.host.aggregation_engine().schema_epoch(),
                );
                return None;
            }
        };

        // Extract table name (bail out for AS OF temporal queries)
        let table_name = match stmt.table_expr.as_deref() {
            Some(Expression::TableSource(ts)) => {
                if ts.as_of.is_some() {
                    *compiled_guard = CompiledExecution::NotOptimizable(
                        self.host.aggregation_engine().schema_epoch(),
                    );
                    return None;
                }
                ts.name.value_lower.clone()
            }
            _ => {
                *compiled_guard = CompiledExecution::NotOptimizable(
                    self.host.aggregation_engine().schema_epoch(),
                );
                return None;
            }
        };

        // Try to get table and verify index exists
        let tx = match self.host.aggregation_engine().begin_transaction() {
            Ok(tx) => tx,
            Err(_) => {
                *compiled_guard = CompiledExecution::NotOptimizable(
                    self.host.aggregation_engine().schema_epoch(),
                );
                return None;
            }
        };

        let table = match tx.get_table(&table_name) {
            Ok(t) => t,
            Err(_) => {
                *compiled_guard = CompiledExecution::NotOptimizable(
                    self.host.aggregation_engine().schema_epoch(),
                );
                return None;
            }
        };

        // Check if column has an index (required for fast path)
        if table.get_partition_count(&column_name).is_none() {
            *compiled_guard =
                CompiledExecution::NotOptimizable(self.host.aggregation_engine().schema_epoch());
            return None;
        }

        // Get the count
        let count = table.get_partition_count(&column_name).unwrap();

        // Cache the compiled state
        let compiled_cd = CompiledCountDistinct {
            table_name: SmartString::new(&table_name),
            column_name: SmartString::new(&column_name),
            result_column_name: result_column_name.clone(),
            cached_epoch: self.host.aggregation_engine().schema_epoch(),
        };
        *compiled_guard = CompiledExecution::CountDistinct(compiled_cd);
        drop(compiled_guard);

        // Build result
        let mut result_values = CompactVec::with_capacity(1);
        result_values.push(Value::Integer(count as i64));
        let row = Row::from_compact_vec(result_values);
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, row));

        Some(Ok(Box::new(ExecutorResult::new(
            vec![result_column_name],
            rows,
        ))))
    }

    /// COUNT(*) fast path for simple queries
    ///
    /// This is a compiled fast path for simple `SELECT COUNT(*) FROM table` queries.
    /// On first execution, it analyzes and compiles the query pattern.
    /// On subsequent executions, it skips parsing and directly fetches the row count.
    ///
    /// # Arguments
    /// * `stmt` - The SELECT statement
    /// * `compiled` - The compiled execution state (shared via RwLock)
    ///
    /// # Returns
    /// * `Some(Ok(result))` - Query succeeded via fast path
    /// * `Some(Err(e))` - Query failed
    /// * `None` - Query doesn't qualify for this fast path (use normal path)
    pub(crate) fn try_fast_count_star_compiled(
        &self,
        stmt: &SelectStatement,
        compiled: &RwLock<CompiledExecution>,
    ) -> Option<Result<Box<dyn QueryResult>>> {
        // Quick reject: must not be in an explicit transaction (for simplicity)
        {
            let active_tx = match self.host.aggregation_active_transaction().try_lock() {
                Ok(guard) => guard,
                Err(_) => return None,
            };
            if active_tx.is_some() {
                return None;
            }
        }

        // Try read lock first - check if already compiled
        {
            let compiled_guard = match compiled.read() {
                Ok(guard) => guard,
                Err(_) => return None,
            };
            match &*compiled_guard {
                CompiledExecution::NotOptimizable(epoch)
                    if self.host.aggregation_engine().schema_epoch() == *epoch =>
                {
                    return None
                }
                CompiledExecution::CountStar(cs) => {
                    // Fast path: validate epoch and execute directly
                    if self.host.aggregation_engine().schema_epoch() == cs.cached_epoch {
                        return Some(self.execute_compiled_count_star(cs));
                    }
                    // Schema changed - fall through to recompile
                }
                CompiledExecution::NotOptimizable(_) | CompiledExecution::Unknown => {} // Epoch changed or first run - fall through to recompile
                // Other variants - not a COUNT(*) query
                _ => return None,
            }
        }

        // First execution or schema changed - compile and cache (write lock)
        self.compile_and_execute_count_star(stmt, compiled)
    }

    /// Execute using pre-compiled COUNT(*) info
    pub(super) fn execute_compiled_count_star(
        &self,
        cs: &crate::compiled_plan::CompiledCountStar,
    ) -> Result<Box<dyn QueryResult>> {
        // Get table and count rows directly
        let tx = self.host.aggregation_engine().begin_transaction()?;
        let table = tx.get_table(&cs.table_name)?;

        let count = table.row_count();

        // Build result
        let mut result_values = CompactVec::with_capacity(1);
        result_values.push(Value::Integer(count as i64));
        let row = Row::from_compact_vec(result_values);
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, row));

        Ok(Box::new(ExecutorResult::new(
            vec![cs.result_column_name.clone()],
            rows,
        )))
    }

    /// Compile and execute COUNT(*), caching the compiled state
    pub(super) fn compile_and_execute_count_star(
        &self,
        stmt: &SelectStatement,
        compiled: &RwLock<CompiledExecution>,
    ) -> Option<Result<Box<dyn QueryResult>>> {
        use crate::compiled_plan::CompiledCountStar;
        use radixdb_core::SmartString;

        // Acquire write lock
        let mut compiled_guard = match compiled.write() {
            Ok(guard) => guard,
            Err(_) => return None,
        };

        // Double-check (another thread may have compiled while we waited)
        match &*compiled_guard {
            CompiledExecution::NotOptimizable(epoch)
                if self.host.aggregation_engine().schema_epoch() == *epoch =>
            {
                return None
            }
            CompiledExecution::CountStar(cs) => {
                if self.host.aggregation_engine().schema_epoch() == cs.cached_epoch {
                    return Some(self.execute_compiled_count_star(cs));
                }
                // Schema changed, continue to recompile
            }
            CompiledExecution::NotOptimizable(_) | CompiledExecution::Unknown => {} // Epoch changed or first run - recompile
            _ => return None,
        }

        // Pattern detection: SELECT COUNT(*) FROM table
        // Must have:
        // - Exactly one column expression
        // - That column is COUNT(*) or COUNT(1) function call (not DISTINCT)
        // - No WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, CTEs, set operations
        // - Single table source (no joins)

        if stmt.columns.len() != 1 {
            *compiled_guard =
                CompiledExecution::NotOptimizable(self.host.aggregation_engine().schema_epoch());
            return None;
        }

        // Check for disqualifying clauses
        if stmt.where_clause.is_some()
            || !stmt.group_by.columns.is_empty()
            || stmt.having.is_some()
            || !stmt.order_by.is_empty()
            || stmt.limit.is_some()
            || stmt.offset.is_some()
            || stmt.with.is_some()
            || !stmt.set_operations.is_empty()
        {
            *compiled_guard =
                CompiledExecution::NotOptimizable(self.host.aggregation_engine().schema_epoch());
            return None;
        }

        // Extract COUNT(*) pattern
        let result_column_name = match &stmt.columns[0] {
            Expression::FunctionCall(func) => {
                // Must be COUNT function
                if func.function.to_uppercase() != "COUNT" {
                    *compiled_guard = CompiledExecution::NotOptimizable(
                        self.host.aggregation_engine().schema_epoch(),
                    );
                    return None;
                }
                // Must NOT be DISTINCT - if DISTINCT, don't mark as NotOptimizable
                // because COUNT DISTINCT fast path might handle it
                if func.is_distinct {
                    return None;
                }
                // Must not have FILTER clause
                if func.filter.is_some() {
                    *compiled_guard = CompiledExecution::NotOptimizable(
                        self.host.aggregation_engine().schema_epoch(),
                    );
                    return None;
                }
                // Must be COUNT(*) or COUNT(1)
                match func.arguments.len() {
                    0 => {
                        // COUNT(*) without explicit star is rare but handle it
                        "COUNT(*)".to_string()
                    }
                    1 => {
                        match &func.arguments[0] {
                            Expression::Star(_) => "COUNT(*)".to_string(),
                            Expression::IntegerLiteral(lit) => {
                                // COUNT(1) is equivalent to COUNT(*)
                                if lit.value == 1 {
                                    "COUNT(1)".to_string()
                                } else {
                                    *compiled_guard = CompiledExecution::NotOptimizable(
                                        self.host.aggregation_engine().schema_epoch(),
                                    );
                                    return None;
                                }
                            }
                            _ => {
                                // COUNT(col) without DISTINCT - not our fast path
                                *compiled_guard = CompiledExecution::NotOptimizable(
                                    self.host.aggregation_engine().schema_epoch(),
                                );
                                return None;
                            }
                        }
                    }
                    _ => {
                        *compiled_guard = CompiledExecution::NotOptimizable(
                            self.host.aggregation_engine().schema_epoch(),
                        );
                        return None;
                    }
                }
            }
            Expression::Aliased(aliased) => {
                // Handle COUNT(*) AS alias
                match aliased.expression.as_ref() {
                    Expression::FunctionCall(func) => {
                        // Must be COUNT function
                        if func.function.to_uppercase() != "COUNT" {
                            *compiled_guard = CompiledExecution::NotOptimizable(
                                self.host.aggregation_engine().schema_epoch(),
                            );
                            return None;
                        }
                        // Must NOT be DISTINCT - if DISTINCT, don't mark as NotOptimizable
                        // because COUNT DISTINCT fast path might handle it
                        if func.is_distinct {
                            return None;
                        }
                        // Must not have FILTER clause
                        if func.filter.is_some() {
                            *compiled_guard = CompiledExecution::NotOptimizable(
                                self.host.aggregation_engine().schema_epoch(),
                            );
                            return None;
                        }
                        match func.arguments.len() {
                            0 => aliased.alias.value.to_string(),
                            1 => match &func.arguments[0] {
                                Expression::Star(_) => aliased.alias.value.to_string(),
                                Expression::IntegerLiteral(lit) if lit.value == 1 => {
                                    aliased.alias.value.to_string()
                                }
                                Expression::IntegerLiteral(_) => {
                                    *compiled_guard = CompiledExecution::NotOptimizable(
                                        self.host.aggregation_engine().schema_epoch(),
                                    );
                                    return None;
                                }
                                _ => {
                                    *compiled_guard = CompiledExecution::NotOptimizable(
                                        self.host.aggregation_engine().schema_epoch(),
                                    );
                                    return None;
                                }
                            },
                            _ => {
                                *compiled_guard = CompiledExecution::NotOptimizable(
                                    self.host.aggregation_engine().schema_epoch(),
                                );
                                return None;
                            }
                        }
                    }
                    _ => {
                        *compiled_guard = CompiledExecution::NotOptimizable(
                            self.host.aggregation_engine().schema_epoch(),
                        );
                        return None;
                    }
                }
            }
            _ => {
                *compiled_guard = CompiledExecution::NotOptimizable(
                    self.host.aggregation_engine().schema_epoch(),
                );
                return None;
            }
        };

        // Extract table name (bail out for AS OF temporal queries)
        let table_name = match stmt.table_expr.as_deref() {
            Some(Expression::TableSource(ts)) => {
                if ts.as_of.is_some() {
                    *compiled_guard = CompiledExecution::NotOptimizable(
                        self.host.aggregation_engine().schema_epoch(),
                    );
                    return None;
                }
                ts.name.value_lower.clone()
            }
            _ => {
                *compiled_guard = CompiledExecution::NotOptimizable(
                    self.host.aggregation_engine().schema_epoch(),
                );
                return None;
            }
        };

        // Try to get table and get count
        let tx = match self.host.aggregation_engine().begin_transaction() {
            Ok(tx) => tx,
            Err(_) => {
                *compiled_guard = CompiledExecution::NotOptimizable(
                    self.host.aggregation_engine().schema_epoch(),
                );
                return None;
            }
        };

        let table = match tx.get_table(&table_name) {
            Ok(t) => t,
            Err(_) => {
                *compiled_guard = CompiledExecution::NotOptimizable(
                    self.host.aggregation_engine().schema_epoch(),
                );
                return None;
            }
        };

        // Get the count
        let count = table.row_count();

        // Cache the compiled state
        let compiled_cs = CompiledCountStar {
            table_name: SmartString::new(&table_name),
            result_column_name: result_column_name.clone(),
            cached_epoch: self.host.aggregation_engine().schema_epoch(),
        };
        *compiled_guard = CompiledExecution::CountStar(compiled_cs);
        drop(compiled_guard);

        // Build result
        let mut result_values = CompactVec::with_capacity(1);
        result_values.push(Value::Integer(count as i64));
        let row = Row::from_compact_vec(result_values);
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, row));

        Some(Ok(Box::new(ExecutorResult::new(
            vec![result_column_name],
            rows,
        ))))
    }
}
