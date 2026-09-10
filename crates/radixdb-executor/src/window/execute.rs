use super::*;

impl<'host, H: WindowHost + ?Sized> WindowExecutor<'host, H> {
    /// Execute SELECT with window functions
    /// Accepts &[(i64, Row)] to allow RowVec to be passed directly via deref
    pub(crate) fn execute_select_with_window_functions(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_select_with_window_functions_internal(
            stmt,
            ctx,
            base_rows,
            base_columns,
            None,
            None,
        )
    }

    /// Execute SELECT with window functions, with optional pre-sorted state
    /// When pre_sorted is Some, rows are already sorted by the specified column,
    /// allowing us to skip sorting for window functions that ORDER BY the same column
    pub(crate) fn execute_select_with_window_functions_presorted(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
        pre_sorted: Option<WindowPreSortedState>,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_select_with_window_functions_internal(
            stmt,
            ctx,
            base_rows,
            base_columns,
            pre_sorted,
            None,
        )
    }

    /// Execute SELECT with window functions, with pre-grouped partitions
    /// When pre_grouped is provided, rows are already grouped by partition column,
    /// allowing us to skip hash-based grouping for window functions
    pub(crate) fn execute_select_with_window_functions_pregrouped(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
        pre_grouped: WindowPreGroupedState,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_select_with_window_functions_internal(
            stmt,
            ctx,
            base_rows,
            base_columns,
            None,
            Some(pre_grouped),
        )
    }

    /// Internal implementation with pre-sorted and pre-grouped state parameters
    pub(super) fn execute_select_with_window_functions_internal(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
        pre_sorted: Option<WindowPreSortedState>,
        pre_grouped: Option<WindowPreGroupedState>,
    ) -> Result<Box<dyn QueryResult>> {
        // Parse window functions from the SELECT list
        let window_functions = self.parse_window_functions(stmt, base_columns)?;

        if window_functions.is_empty() {
            // No window functions found, return base result
            let mut rows = RowVec::with_capacity(base_rows.len());
            for (id, row) in base_rows.iter() {
                rows.push((*id, row.clone()));
            }
            return Ok(Box::new(ExecutorResult::new(base_columns.to_vec(), rows)));
        }

        // OPTIMIZATION: LIMIT pushdown for PARTITION BY queries
        // Only safe when there is no top-level ORDER BY (otherwise the sort must see all
        // rows before LIMIT can be applied) and when all window functions share the same
        // PARTITION BY so one partition map can serve them all.
        if stmt.order_by.is_empty() && stmt.offset.is_none() {
            if let Some(limit_expr) = &stmt.limit {
                let has_partition_by = window_functions
                    .iter()
                    .any(|wf| !wf.partition_by_exprs.is_empty());

                if has_partition_by && Self::all_partitions_match(&window_functions) {
                    if let Expression::IntegerLiteral(lit) = limit_expr.as_ref() {
                        let limit_val = lit.value;
                        if limit_val > 0 {
                            return self.execute_select_with_window_functions_streaming(
                                stmt,
                                ctx,
                                base_rows,
                                base_columns,
                                &window_functions,
                                limit_val as usize,
                            );
                        }
                    }
                }
            }
        }

        // Build column index map for base columns
        let mut col_index_map = build_column_index_map(base_columns);

        // Build a mapping from aggregate expression patterns to their column names
        // This handles cases like:
        // - SUM(val) AS grp_sum -> maps "sum(val)" to column index of "grp_sum"
        // - COALESCE(SUM(val), 0) AS total -> maps "sum(val)" to column index of "total"
        for col_expr in stmt.columns.iter() {
            if let Expression::Aliased(aliased) = col_expr {
                let alias_lower = aliased.alias.value_lower.as_str();
                if let Some(&idx) = col_index_map.get(alias_lower) {
                    // Extract all aggregate patterns from this expression (including nested ones)
                    let patterns = self.extract_aggregate_patterns(aliased.expression.as_ref());
                    for pattern in patterns {
                        col_index_map.insert(pattern.to_lowercase(), idx);
                    }
                }
            }
        }

        // Step 1: Compute all window function values upfront
        // OPTIMIZATION: Use FxHashMap for fastest lookups with trusted keys

        // OPTIMIZATION: Precompute ORDER BY values ONCE for each unique ORDER BY clause
        // This avoids redundant computation when multiple window functions share the same ORDER BY
        // We use a Vec for the cache since the number of unique ORDER BY clauses is typically small
        // NOTE: We use string representation for semantic comparison because PartialEq on expressions
        // compares token positions, making structurally identical expressions from different window
        // functions appear different.
        let mut order_by_cache: Vec<(String, ColumnarOrderByValues)> = Vec::new();
        for wf in &window_functions {
            if !wf.order_by.is_empty() {
                // Create a semantic key from the ORDER BY expressions (ignores token positions)
                let cache_key = Self::order_by_cache_key(&wf.order_by);
                // Check if this ORDER BY clause is already in the cache
                let already_cached = order_by_cache.iter().any(|(key, _)| key == &cache_key);
                if !already_cached {
                    let precomputed = self.precompute_order_by_values(
                        &wf.order_by,
                        base_rows,
                        base_columns,
                        &col_index_map,
                        ctx,
                    )?;
                    order_by_cache.push((cache_key, precomputed));
                }
            }
        }

        let mut window_value_map: StringMap<Vec<Value>> = StringMap::new();
        for wf in &window_functions {
            let window_values = self.compute_window_function(
                wf,
                base_rows,
                base_columns,
                &col_index_map,
                ctx,
                pre_sorted.as_ref(),
                pre_grouped.as_ref(),
                &order_by_cache,
            )?;
            window_value_map.insert(wf.column_name.to_lowercase(), window_values);
        }

        // Step 2: Build output columns and rows based on the SELECT list
        // The result should respect the SELECT list order, not just append window functions
        let mut result_columns = Vec::new();

        // Parse the SELECT list to determine output column order
        let select_items = self.parse_select_list_for_window(stmt, base_columns, &window_functions);

        for item in &select_items {
            result_columns.push(item.output_name.clone());
        }

        // Step 3: Build result using COLUMNAR storage
        // OPTIMIZATION: Instead of allocating one Row per result row, we store data column-major
        // and use ColumnarResult which materializes rows lazily with a single reused buffer.
        // This reduces allocations from O(num_rows) to O(num_columns).
        let num_rows = base_rows.len();

        // Build aliases from col_index_map for expression evaluation
        let agg_aliases: Vec<(String, usize)> =
            col_index_map.iter().map(|(k, v)| (k.clone(), *v)).collect();

        // Pre-transform expressions with window functions by replacing Window expr with Identifier
        // This is done once, not per row
        let transformed_items: Vec<_> = select_items
            .iter()
            .map(|item| match &item.source {
                SelectItemSource::ExpressionWithWindow(expr, wf_names) => {
                    let mut counter = 0;
                    let transformed =
                        Self::replace_windows_with_identifiers(expr, wf_names, &mut counter);
                    (item, Some(transformed), Some(wf_names.clone()))
                }
                _ => (item, None, None),
            })
            .collect();

        // Build extended columns (base_columns + synthetic window columns)
        let mut extended_columns = base_columns.to_vec();
        let mut added_wf_names: Vec<String> = Vec::new();
        for (_, _, wf_names_opt) in &transformed_items {
            if let Some(wf_names) = wf_names_opt {
                for wf_name in wf_names {
                    if !added_wf_names.contains(wf_name) {
                        extended_columns.push(wf_name.clone());
                        added_wf_names.push(wf_name.clone());
                    }
                }
            }
        }

        // Collect Expression items (with base_columns) and their indices
        let base_expr_items: Vec<(usize, &Expression)> = transformed_items
            .iter()
            .enumerate()
            .filter_map(|(i, (item, _, _))| {
                if let SelectItemSource::Expression(expr) = &item.source {
                    Some((i, expr))
                } else {
                    None
                }
            })
            .collect();

        // Collect ExpressionWithWindow transformed expressions and their indices
        let ext_expr_items: Vec<(usize, &Expression)> = transformed_items
            .iter()
            .enumerate()
            .filter_map(|(i, (_, transformed_opt, _))| transformed_opt.as_ref().map(|t| (i, t)))
            .collect();

        // Pre-compile base expressions (Expression items with base_columns)
        // CRITICAL: Propagate compilation errors instead of silently producing NULLs
        let base_exprs: Vec<Expression> =
            base_expr_items.iter().map(|(_, e)| (*e).clone()).collect();
        let mut base_eval = if !base_exprs.is_empty() {
            Some(
                MultiExpressionEval::compile_with_aliases(&base_exprs, base_columns, &agg_aliases)?
                    .with_context(ctx),
            )
        } else {
            None
        };

        // Pre-compile extended expressions (ExpressionWithWindow with extended_columns)
        // CRITICAL: Propagate compilation errors instead of silently producing NULLs
        let ext_exprs: Vec<Expression> = ext_expr_items.iter().map(|(_, e)| (*e).clone()).collect();
        let mut ext_eval = if !ext_exprs.is_empty() {
            Some(
                MultiExpressionEval::compile_with_aliases(
                    &ext_exprs,
                    &extended_columns,
                    &agg_aliases,
                )?
                .with_context(ctx),
            )
        } else {
            None
        };

        // OPTIMIZATION: Pre-allocate ext_values buffer for extended expressions
        let ext_values_capacity = if !ext_expr_items.is_empty() {
            base_rows.first().map_or(0, |r| r.1.len()) + added_wf_names.len()
        } else {
            0
        };
        let mut ext_values: CompactVec<Value> = CompactVec::with_capacity(ext_values_capacity);

        // Number of output columns
        let num_items = select_items.len();

        // COLUMNAR STORAGE OPTIMIZATION: Build columns in column-major order
        // This is more cache-efficient than row-by-row iteration and enables
        // moving window function Vecs directly (zero-copy for window results).
        //
        // Phase 1: Build non-expression columns (WindowFunction, BaseColumn)
        // Phase 2: Fill expression columns row-by-row (requires evaluation)
        let mut column_data: Vec<Vec<Value>> = Vec::with_capacity(num_items);

        // Track which window functions are used in expressions (need to keep them)
        let wf_names_in_exprs: std::collections::HashSet<&str> =
            added_wf_names.iter().map(|s| s.as_str()).collect();

        // Phase 1: Build columns for WindowFunction and BaseColumn items
        // Expression columns get placeholder Vecs (filled in Phase 2)
        for (item, _, _) in &transformed_items {
            match &item.source {
                SelectItemSource::WindowFunction(wf_name_lower) => {
                    let source_values = window_value_map.get(wf_name_lower).ok_or_else(|| {
                        Error::internal(format!(
                            "window projection source {wf_name_lower} is missing"
                        ))
                    })?;
                    if source_values.len() != num_rows {
                        return Err(Error::internal(format!(
                            "window projection source {wf_name_lower} has {} rows, expected {num_rows}",
                            source_values.len()
                        )));
                    }
                    // OPTIMIZATION: Move or clone the entire Vec at once
                    // If this window function is also used in an expression, we need to keep it
                    if wf_names_in_exprs.contains(wf_name_lower.as_str()) {
                        // Clone the entire Vec (more cache-efficient than element-by-element)
                        let values = source_values.clone();
                        column_data.push(values);
                    } else {
                        // Move the Vec directly (zero-copy)
                        let values = window_value_map.remove(wf_name_lower).ok_or_else(|| {
                            Error::internal(format!(
                                "window projection source {wf_name_lower} disappeared"
                            ))
                        })?;
                        column_data.push(values);
                    }
                }
                SelectItemSource::BaseColumn(base_col_idx) => {
                    // OPTIMIZATION: Build base column in one pass (column-wise)
                    let mut values = Vec::with_capacity(num_rows);
                    for (_, base_row) in base_rows {
                        values.push(base_row.get(*base_col_idx).cloned().ok_or_else(|| {
                            Error::internal(format!(
                                "window base projection index {base_col_idx} is outside row width {}",
                                base_row.len()
                            ))
                        })?);
                    }
                    column_data.push(values);
                }
                SelectItemSource::Expression(_) | SelectItemSource::ExpressionWithWindow(_, _) => {
                    // Placeholder - will be filled in Phase 2
                    column_data.push(vec![NULL_VALUE; num_rows]);
                }
            }
        }

        // Phase 2: Fill expression columns row-by-row (requires evaluation)
        let has_expressions = !base_expr_items.is_empty() || !ext_expr_items.is_empty();
        if has_expressions {
            for (row_idx, (_, base_row)) in base_rows.iter().enumerate() {
                // Evaluate base expressions and update their column values
                if let Some(ref mut eval) = base_eval {
                    let base_results = eval.eval_all(base_row)?;
                    if base_results.len() != base_expr_items.len() {
                        return Err(Error::internal(format!(
                            "window base projection produced {} values, expected {}",
                            base_results.len(),
                            base_expr_items.len()
                        )));
                    }
                    for (eval_idx, (item_idx, _)) in base_expr_items.iter().enumerate() {
                        column_data[*item_idx][row_idx] = base_results[eval_idx].clone();
                    }
                }

                // Evaluate extended expressions (if any) and update their column values
                if !ext_expr_items.is_empty() {
                    if let Some(ref mut eval) = ext_eval {
                        // Reuse ext_values buffer: clear and refill
                        ext_values.clear();
                        ext_values.extend(base_row.iter().cloned());
                        for wf_name in &added_wf_names {
                            let wf_value = window_value_map
                                .get(wf_name)
                                .and_then(|vals| vals.get(row_idx).cloned())
                                .ok_or_else(|| {
                                    Error::internal(format!(
                                        "window expression source {wf_name} is missing row {row_idx}"
                                    ))
                                })?;
                            ext_values.push(wf_value);
                        }
                        let ext_row = Row::from_compact_vec(ext_values.clone());

                        let ext_result_values = eval.eval_all(&ext_row)?;
                        if ext_result_values.len() != ext_expr_items.len() {
                            return Err(Error::internal(format!(
                                "window extended projection produced {} values, expected {}",
                                ext_result_values.len(),
                                ext_expr_items.len()
                            )));
                        }
                        for (eval_idx, (item_idx, _)) in ext_expr_items.iter().enumerate() {
                            column_data[*item_idx][row_idx] = ext_result_values[eval_idx].clone();
                        }
                    }
                }
            }
        }

        self.append_hidden_window_order_columns(
            stmt,
            ctx,
            base_rows,
            base_columns,
            &mut result_columns,
            &mut column_data,
        )?;

        // Return ColumnarResult which materializes rows lazily with zero per-row allocation
        Ok(Box::new(ColumnarResult::new(result_columns, column_data)))
    }

    /// Preserve source expressions required by top-level ORDER BY / DISTINCT ON
    /// after the window SELECT projection. These columns are internal: the outer
    /// executor consumes them for sorting/deduplication and then truncates the
    /// public row back to the SELECT width.
    pub(super) fn append_hidden_window_order_columns(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
        result_columns: &mut Vec<String>,
        column_data: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        if stmt.order_by.is_empty() && stmt.distinct_on.is_empty() {
            return Ok(());
        }

        let base_index = build_column_index_map(base_columns);
        let mut append_expression = |expression: &Expression| -> Result<()> {
            // The outer ORDER BY mapper already resolves output aliases and
            // expressions that are present in the SELECT list.
            let selected = stmt.columns.iter().any(|selected| match selected {
                Expression::Aliased(aliased) => {
                    aliased.expression.as_ref().to_string() == expression.to_string()
                        || matches!(expression, Expression::Identifier(id)
                            if aliased.alias.value_lower == id.value_lower)
                }
                other => other.to_string() == expression.to_string(),
            });
            if selected || matches!(expression, Expression::IntegerLiteral(_)) {
                return Ok(());
            }

            let (column_name, values) = match expression {
                Expression::Identifier(id) => {
                    let Some(&index) = base_index.get(id.value_lower.as_str()) else {
                        // It may be a SELECT alias; the outer mapper will resolve it.
                        if result_columns
                            .iter()
                            .any(|name| name.eq_ignore_ascii_case(id.value_lower.as_str()))
                        {
                            return Ok(());
                        }
                        return Err(Error::ColumnNotFound(id.value.to_string()));
                    };
                    let name = base_columns[index].clone();
                    let values = base_rows
                        .iter()
                        .map(|(_, row)| row.get(index).cloned().unwrap_or(NULL_VALUE))
                        .collect();
                    (name, values)
                }
                Expression::QualifiedIdentifier(id) => {
                    let qualified = format!("{}.{}", id.qualifier.value_lower, id.name.value_lower);
                    let Some(&index) = base_index
                        .get(qualified.as_str())
                        .or_else(|| base_index.get(id.name.value_lower.as_str()))
                    else {
                        return Err(Error::ColumnNotFound(format!(
                            "{}.{}",
                            id.qualifier.value, id.name.value
                        )));
                    };
                    let name = base_columns[index].clone();
                    let values = base_rows
                        .iter()
                        .map(|(_, row)| row.get(index).cloned().unwrap_or(NULL_VALUE))
                        .collect();
                    (name, values)
                }
                _ => {
                    let name = expression.to_string();
                    if result_columns
                        .iter()
                        .any(|column| column.eq_ignore_ascii_case(&name))
                    {
                        return Ok(());
                    }
                    let mut evaluator =
                        ExpressionEval::compile(expression, base_columns)?.with_context(ctx);
                    let values = base_rows
                        .iter()
                        .map(|(_, row)| evaluator.eval(row))
                        .collect::<Result<Vec<_>>>()?;
                    (name, values)
                }
            };

            if !result_columns
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&column_name))
            {
                result_columns.push(column_name);
                column_data.push(values);
            }
            Ok(())
        };

        for order in &stmt.order_by {
            append_expression(&order.expression)?;
        }
        for expression in &stmt.distinct_on {
            append_expression(expression)?;
        }
        Ok(())
    }

    /// Streaming execution for window functions with LIMIT pushdown
    /// Processes partitions one at a time and stops early when LIMIT is reached
    pub(super) fn execute_select_with_window_functions_streaming(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: &[(i64, Row)],
        base_columns: &[String],
        window_functions: &[WindowFunctionInfo],
        limit: usize,
    ) -> Result<Box<dyn QueryResult>> {
        // Use the first window function with PARTITION BY for partitioning
        let primary_wf = window_functions
            .iter()
            .find(|wf| !wf.partition_by_exprs.is_empty())
            .unwrap(); // Safe: we checked has_partition_by before calling this

        // Build column index map
        let col_index_map = build_column_index_map(base_columns);

        // Build partition map from the primary window function
        let partitions =
            Self::build_partition_map(primary_wf, base_rows, base_columns, &col_index_map, ctx)?;

        // Build result columns from SELECT list
        let select_items = self.parse_select_list_for_window(stmt, base_columns, window_functions);
        let result_columns: Vec<String> =
            select_items.iter().map(|i| i.output_name.clone()).collect();

        // Precompute ORDER BY values once for each unique ORDER BY clause
        let mut order_by_cache: Vec<(String, ColumnarOrderByValues)> = Vec::new();
        for wf in window_functions {
            if !wf.order_by.is_empty() {
                let cache_key = Self::order_by_cache_key(&wf.order_by);
                let already_cached = order_by_cache.iter().any(|(key, _)| key == &cache_key);
                if !already_cached {
                    let precomputed = self.precompute_order_by_values(
                        &wf.order_by,
                        base_rows,
                        base_columns,
                        &col_index_map,
                        ctx,
                    )?;
                    order_by_cache.push((cache_key, precomputed));
                }
            }
        }

        // Resolve window functions from registry
        let resolved_wfs: Vec<_> = window_functions
            .iter()
            .map(|wf| {
                let is_agg = self.host.window_function_registry().is_aggregate(&wf.name);
                let func = if is_agg {
                    None
                } else {
                    self.host.window_function_registry().get_window(&wf.name)
                };
                (wf, func, is_agg)
            })
            .collect();

        // Precompute aggregate window functions once over all rows (not per partition)
        let mut precomputed_agg: StringMap<Vec<Value>> = StringMap::new();
        for (wf, _, is_agg) in &resolved_wfs {
            if *is_agg {
                let cache_key = Self::order_by_cache_key(&wf.order_by);
                let precomputed_order_by = order_by_cache
                    .iter()
                    .find(|(key, _)| key == &cache_key)
                    .map(|(_, v)| v);
                let agg_results = self.compute_aggregate_window_function(
                    wf,
                    base_rows,
                    base_columns,
                    &col_index_map,
                    ctx,
                    None,
                    precomputed_order_by,
                )?;
                precomputed_agg.insert(wf.column_name.to_lowercase(), agg_results);
            }
        }

        // Process partitions one at a time, stopping when we have enough rows
        let mut result_rows = RowVec::with_capacity(limit);
        let mut result_row_id = 0i64;

        let partitions_vec: Vec<_> = partitions.into_iter().collect();

        for (_partition_key, row_indices) in partitions_vec {
            if result_rows.len() >= limit {
                break;
            }

            // Compute window functions for this partition.
            let mut window_value_map: StringMap<Vec<Value>> = StringMap::new();

            for (wf, win_func_opt, is_agg) in &resolved_wfs {
                if *is_agg {
                    // Slice precomputed aggregate results for this partition
                    let key = wf.column_name.to_lowercase();
                    if let Some(all_results) = precomputed_agg.get(&key) {
                        let partition_vals: Vec<Value> = row_indices
                            .iter()
                            .map(|&i| all_results[i].clone())
                            .collect();
                        window_value_map.insert(key, partition_vals);
                    }
                } else if let Some(win_func) = win_func_opt {
                    let cache_key = Self::order_by_cache_key(&wf.order_by);
                    let precomputed_order_by = order_by_cache
                        .iter()
                        .find(|(key, _)| key == &cache_key)
                        .map(|(_, v)| v);
                    let (sorted_values, sorted_indices) = self.compute_window_for_partition(
                        win_func.as_ref(),
                        wf,
                        base_rows,
                        row_indices.clone(),
                        precomputed_order_by,
                        base_columns,
                        &col_index_map,
                        ctx,
                        false,
                    )?;
                    // Remap: sorted_values[pos] corresponds to row sorted_indices[pos].
                    // Build a vec indexed by position within row_indices using an O(n) index map.
                    let orig_to_local: FxHashMap<usize, usize> = row_indices
                        .iter()
                        .enumerate()
                        .map(|(local, &orig)| (orig, local))
                        .collect();
                    let mut by_orig = vec![NULL_VALUE; row_indices.len()];
                    for (pos, &orig_idx) in sorted_indices.iter().enumerate() {
                        if let Some(&local) = orig_to_local.get(&orig_idx) {
                            by_orig[local] = sorted_values[pos].clone();
                        }
                    }
                    window_value_map.insert(wf.column_name.to_lowercase(), by_orig);
                }
            }

            // Precompile expression evaluators once per partition (not per row)
            enum CompiledItem<'a> {
                BaseColumn(usize),
                WindowFunction(&'a str),
                Expression(ExpressionEval),
                ExpressionWithWindow(ExpressionEval, Vec<&'a str>),
            }
            let num_items = select_items.len();
            let mut compiled_items: Vec<CompiledItem<'_>> = Vec::with_capacity(num_items);
            for item in &select_items {
                compiled_items.push(match &item.source {
                    SelectItemSource::BaseColumn(idx) => CompiledItem::BaseColumn(*idx),
                    SelectItemSource::WindowFunction(name) => {
                        CompiledItem::WindowFunction(name.as_str())
                    }
                    SelectItemSource::Expression(expr) => {
                        CompiledItem::Expression(ExpressionEval::compile(expr, base_columns)?)
                    }
                    SelectItemSource::ExpressionWithWindow(expr, wf_names) => {
                        let mut counter = 0;
                        let transformed =
                            Self::replace_windows_with_identifiers(expr, wf_names, &mut counter);
                        let mut ext_columns = base_columns.to_vec();
                        for wf_name in wf_names {
                            ext_columns.push(wf_name.clone());
                        }
                        let eval = ExpressionEval::compile(&transformed, &ext_columns)?;
                        let name_refs: Vec<&str> = wf_names.iter().map(|s| s.as_str()).collect();
                        CompiledItem::ExpressionWithWindow(eval, name_refs)
                    }
                });
            }

            let ext_capacity = base_rows.first().map_or(0, |r| r.1.len()) + 1;
            let mut ext_values: CompactVec<Value> = CompactVec::with_capacity(ext_capacity);

            // Output rows in partition order (row_indices order)
            for (local_pos, &orig_idx) in row_indices.iter().enumerate() {
                if result_rows.len() >= limit {
                    break;
                }

                let base_row = &base_rows[orig_idx].1;
                let mut values: CompactVec<Value> = CompactVec::with_capacity(num_items);

                for ci in &mut compiled_items {
                    let value = match ci {
                        CompiledItem::BaseColumn(col_idx) => {
                            base_row.get(*col_idx).cloned().unwrap_or(NULL_VALUE)
                        }
                        CompiledItem::WindowFunction(wf_name_lower) => window_value_map
                            .get(*wf_name_lower)
                            .and_then(|vals| vals.get(local_pos).cloned())
                            .unwrap_or(NULL_VALUE),
                        CompiledItem::Expression(eval) => eval.eval(base_row)?,
                        CompiledItem::ExpressionWithWindow(eval, wf_name_refs) => {
                            ext_values.clear();
                            ext_values.extend(base_row.iter().cloned());
                            for wf_name in wf_name_refs.iter() {
                                let wf_value = window_value_map
                                    .get(*wf_name)
                                    .and_then(|vals| vals.get(local_pos).cloned())
                                    .unwrap_or(NULL_VALUE);
                                ext_values.push(wf_value);
                            }
                            let ext_row = Row::from_compact_vec(ext_values.clone());
                            eval.eval(&ext_row)?
                        }
                    };
                    values.push(value);
                }
                result_rows.push((result_row_id, Row::from_compact_vec(values)));
                result_row_id += 1;
            }
        }

        Ok(Box::new(ExecutorResult::new(result_columns, result_rows)))
    }

    /// Lazy partition fetching for window functions with LIMIT pushdown
    /// Fetches partitions one at a time from the index and stops when LIMIT is reached
    /// This is the key optimization for PARTITION BY + LIMIT queries
    pub fn execute_select_with_window_functions_lazy_partition(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        table: &dyn Table,
        base_columns: &[String],
        partition_col: &str,
        limit: usize,
    ) -> Result<Box<dyn QueryResult>> {
        // Parse window functions from SELECT list
        let window_functions = self.parse_window_functions(stmt, base_columns)?;
        if window_functions.is_empty() {
            return Err(Error::internal(
                "No window functions found for lazy partition fetch",
            ));
        }

        // Build column index map
        let col_index_map = build_column_index_map(base_columns);

        // Resolve window functions from registry
        let resolved_wfs: Vec<_> = window_functions
            .iter()
            .map(|wf| {
                let is_agg = self.host.window_function_registry().is_aggregate(&wf.name);
                let func = if is_agg {
                    None
                } else {
                    self.host.window_function_registry().get_window(&wf.name)
                };
                (wf, func, is_agg)
            })
            .collect();

        // Build result columns from SELECT list
        let select_items = self.parse_select_list_for_window(stmt, base_columns, &window_functions);
        let result_columns: Vec<String> =
            select_items.iter().map(|i| i.output_name.clone()).collect();

        // Get partition values from the index (lazy iteration key!)
        let partition_values = match table.get_partition_values(partition_col) {
            Some(values) => values,
            None => return Err(Error::internal("Failed to get partition values from index")),
        };

        // Process partitions one at a time, stopping when we have enough rows
        let mut result_rows = RowVec::with_capacity(limit);
        let mut result_row_id = 0i64;

        for partition_value in partition_values {
            if result_rows.len() >= limit {
                break;
            }

            // Fetch rows for this partition only (KEY OPTIMIZATION!)
            let partition_rows =
                match table.get_rows_for_partition_value(partition_col, &partition_value) {
                    Some(rows) => rows,
                    None => continue,
                };

            if partition_rows.is_empty() {
                continue;
            }

            // Precompute ORDER BY values for each unique ORDER BY clause
            let mut order_by_cache: Vec<(String, ColumnarOrderByValues)> = Vec::new();
            for wf in &window_functions {
                if !wf.order_by.is_empty() {
                    let cache_key = Self::order_by_cache_key(&wf.order_by);
                    let already_cached = order_by_cache.iter().any(|(key, _)| key == &cache_key);
                    if !already_cached {
                        let precomputed = self.precompute_order_by_values(
                            &wf.order_by,
                            &partition_rows,
                            base_columns,
                            &col_index_map,
                            ctx,
                        )?;
                        order_by_cache.push((cache_key, precomputed));
                    }
                }
            }

            // Compute ALL window functions for this partition.
            // Values are stored keyed by local position within row_indices.
            let row_indices: Vec<usize> = (0..partition_rows.len()).collect();
            let mut window_value_map: StringMap<Vec<Value>> = StringMap::new();

            for (wf, win_func_opt, is_agg) in &resolved_wfs {
                let cache_key = Self::order_by_cache_key(&wf.order_by);
                let precomputed_order_by = order_by_cache
                    .iter()
                    .find(|(key, _)| key == &cache_key)
                    .map(|(_, v)| v);

                if *is_agg {
                    let agg_results = self.compute_aggregate_window_function(
                        wf,
                        &partition_rows,
                        base_columns,
                        &col_index_map,
                        ctx,
                        None,
                        precomputed_order_by,
                    )?;
                    // agg_results is already indexed by local row index
                    window_value_map.insert(wf.column_name.to_lowercase(), agg_results);
                } else if let Some(win_func) = win_func_opt {
                    let (sorted_values, sorted_indices) = self.compute_window_for_partition(
                        win_func.as_ref(),
                        wf,
                        &partition_rows,
                        row_indices.clone(),
                        precomputed_order_by,
                        base_columns,
                        &col_index_map,
                        ctx,
                        false,
                    )?;
                    // Remap from sorted order to local row order
                    let mut by_local = vec![NULL_VALUE; row_indices.len()];
                    for (pos, &local_idx) in sorted_indices.iter().enumerate() {
                        by_local[local_idx] = sorted_values[pos].clone();
                    }
                    window_value_map.insert(wf.column_name.to_lowercase(), by_local);
                }
            }

            // Precompile expression evaluators once per partition (not per row)
            enum CompiledItemLazy<'a> {
                BaseColumn(usize),
                WindowFunction(&'a str),
                Expression(ExpressionEval),
                ExpressionWithWindow(ExpressionEval, Vec<&'a str>),
            }
            let num_items = select_items.len();
            let mut compiled_items: Vec<CompiledItemLazy<'_>> = Vec::with_capacity(num_items);
            for item in &select_items {
                compiled_items.push(match &item.source {
                    SelectItemSource::BaseColumn(idx) => CompiledItemLazy::BaseColumn(*idx),
                    SelectItemSource::WindowFunction(name) => {
                        CompiledItemLazy::WindowFunction(name.as_str())
                    }
                    SelectItemSource::Expression(expr) => {
                        CompiledItemLazy::Expression(ExpressionEval::compile(expr, base_columns)?)
                    }
                    SelectItemSource::ExpressionWithWindow(expr, wf_names) => {
                        let mut counter = 0;
                        let transformed =
                            Self::replace_windows_with_identifiers(expr, wf_names, &mut counter);
                        let mut ext_columns = base_columns.to_vec();
                        for wf_name in wf_names {
                            ext_columns.push(wf_name.clone());
                        }
                        let eval = ExpressionEval::compile(&transformed, &ext_columns)?;
                        let name_refs: Vec<&str> = wf_names.iter().map(|s| s.as_str()).collect();
                        CompiledItemLazy::ExpressionWithWindow(eval, name_refs)
                    }
                });
            }

            let mut ext_values: CompactVec<Value> =
                CompactVec::with_capacity(partition_rows.first().map_or(0, |r| r.1.len()) + 1);

            // Output rows in partition order
            for (local_pos, &row_idx) in row_indices.iter().enumerate() {
                if result_rows.len() >= limit {
                    break;
                }

                let (_, base_row) = &partition_rows[row_idx];
                let mut values: CompactVec<Value> = CompactVec::with_capacity(num_items);

                for ci in &mut compiled_items {
                    let val = match ci {
                        CompiledItemLazy::BaseColumn(col_idx) => {
                            base_row.get(*col_idx).cloned().unwrap_or(NULL_VALUE)
                        }
                        CompiledItemLazy::WindowFunction(wf_name_lower) => window_value_map
                            .get(*wf_name_lower)
                            .and_then(|vals| vals.get(local_pos).cloned())
                            .unwrap_or(NULL_VALUE),
                        CompiledItemLazy::Expression(eval) => eval.eval(base_row)?,
                        CompiledItemLazy::ExpressionWithWindow(eval, wf_name_refs) => {
                            ext_values.clear();
                            ext_values.extend(base_row.iter().cloned());
                            for wf_name in wf_name_refs.iter() {
                                let wf_value = window_value_map
                                    .get(*wf_name)
                                    .and_then(|vals| vals.get(local_pos).cloned())
                                    .unwrap_or(NULL_VALUE);
                                ext_values.push(wf_value);
                            }
                            let ext_row = Row::from_compact_vec(ext_values.clone());
                            eval.eval(&ext_row)?
                        }
                    };
                    values.push(val);
                }
                result_rows.push((result_row_id, Row::from_compact_vec(values)));
                result_row_id += 1;
            }
        }

        Ok(Box::new(ExecutorResult::new(result_columns, result_rows)))
    }
}
