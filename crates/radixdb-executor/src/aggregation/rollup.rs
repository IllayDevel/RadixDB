use super::*;

impl<'host, H: AggregationHost + ?Sized> AggregationExecutor<'host, H> {
    /// Execute ROLLUP/CUBE aggregation
    ///
    /// ROLLUP(a, b, c) generates grouping sets:
    ///   (a, b, c) - most detailed
    ///   (a, b)    - subtotal for a, b
    ///   (a)       - subtotal for a
    ///   ()        - grand total
    ///
    /// CUBE(a, b) generates all combinations:
    ///   (a, b), (a), (b), ()
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_rollup_aggregation(
        &self,
        aggregations: &[SqlAggregateFunction],
        group_by_items: &[GroupByItem],
        rows: &[(i64, Row)],
        columns: &[String],
        col_index_map: &StringMap<usize>,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<(Vec<String>, RowVec)> {
        // Generate grouping sets based on modifier type
        let grouping_sets = match &stmt.group_by.modifier {
            GroupByModifier::Rollup => Self::generate_rollup_sets(group_by_items.len()),
            GroupByModifier::Cube => Self::generate_cube_sets(group_by_items.len()),
            GroupByModifier::GroupingSets(sets) => {
                Self::generate_explicit_grouping_sets(sets, group_by_items)
            }
            GroupByModifier::None => {
                // Shouldn't happen, but handle it
                vec![GroupingSet {
                    active_columns: vec![true; group_by_items.len()],
                }]
            }
        };

        // Check if any aggregation has an expression (e.g., SUM(val * 2)) or ORDER BY
        let has_agg_expression = aggregations
            .iter()
            .any(|a| a.expression.is_some() || !a.order_by.is_empty());

        // Pre-compute aggregate column indices
        let agg_col_indices: Vec<Option<usize>> = aggregations
            .iter()
            .map(|agg| {
                if agg.column == "*" || agg.expression.is_some() {
                    None
                } else {
                    Self::lookup_column_index(&agg.column_lower, col_index_map)
                }
            })
            .collect();

        // Pre-compute GROUP BY column indices
        enum PrecomputedGroupBy<'a> {
            ColumnIndex(usize),
            Position(usize),
            Expression(&'a Expression),
            NotFound,
        }

        let precomputed_group_by: Vec<PrecomputedGroupBy> = group_by_items
            .iter()
            .map(|item| match item {
                GroupByItem::Column(col_name) => {
                    if let Some(idx) =
                        Self::lookup_column_index(&col_name.to_lowercase(), col_index_map)
                    {
                        PrecomputedGroupBy::ColumnIndex(idx)
                    } else {
                        PrecomputedGroupBy::NotFound
                    }
                }
                GroupByItem::Position(pos) => PrecomputedGroupBy::Position(pos.saturating_sub(1)),
                GroupByItem::Expression { expr, .. } => PrecomputedGroupBy::Expression(expr),
            })
            .collect();

        // Pre-compile GROUP BY and aggregate expressions for VM-based evaluation
        let has_expr_group_by = precomputed_group_by
            .iter()
            .any(|item| matches!(item, PrecomputedGroupBy::Expression(_)));

        // Pre-compile GROUP BY expressions
        // CRITICAL: Propagate errors instead of silently ignoring compilation failures
        use crate::expression::{compile_expression, ExecuteContext, ExprVM, SharedProgram};
        let compiled_group_by_exprs: Vec<Option<SharedProgram>> = precomputed_group_by
            .iter()
            .map(|item| match item {
                PrecomputedGroupBy::Expression(expr) => compile_expression(expr, columns).map(Some),
                _ => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;

        // Pre-compile aggregate expressions
        // CRITICAL: Propagate errors instead of silently ignoring compilation failures
        let compiled_agg_expressions: Vec<Option<SharedProgram>> = if has_agg_expression {
            aggregations
                .iter()
                .map(|agg| {
                    agg.expression
                        .as_ref()
                        .map(|e| compile_expression(e, columns))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![None; aggregations.len()]
        };

        // Pre-compile aggregate-local ORDER BY expressions. Grouping modifiers
        // share the same ordered aggregate contract as regular GROUP BY.
        let compiled_agg_order_by: Vec<Vec<SharedProgram>> = if has_agg_expression {
            aggregations
                .iter()
                .map(|agg| {
                    agg.order_by
                        .iter()
                        .map(|order| compile_expression(&order.expression, columns))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![Vec::new(); aggregations.len()]
        };

        // Pre-compile FILTER expressions for aggregate functions
        let has_filters = aggregations.iter().any(|a| a.filter.is_some());
        let compiled_filters: Vec<Option<SharedProgram>> = if has_filters {
            aggregations
                .iter()
                .map(|agg| {
                    agg.filter
                        .as_ref()
                        .map(|f| compile_expression(f, columns))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![None; aggregations.len()]
        };

        let mut expr_vm = if has_expr_group_by || has_agg_expression || has_filters {
            Some(ExprVM::new())
        } else {
            None
        };

        // Collect all results from all grouping sets
        let mut all_result_rows: RowVec = RowVec::new();
        let mut row_id = 0i64;

        for grouping_set in &grouping_sets {
            // Count active columns in this grouping set
            let active_count = grouping_set.active_columns.iter().filter(|&&x| x).count();

            if active_count == 0 {
                // Grand total: aggregate all rows without grouping
                let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = aggregations
                    .iter()
                    .map(|agg| {
                        self.host
                            .aggregation_function_registry()
                            .get_aggregate(&agg.name)
                    })
                    .collect();

                // Configure aggregate functions
                for (i, agg) in aggregations.iter().enumerate() {
                    if !agg.extra_args.is_empty() {
                        if let Some(ref mut func) = agg_funcs[i] {
                            func.configure(&agg.extra_args);
                        }
                    }
                    if !agg.order_by.is_empty() {
                        if let Some(ref mut func) = agg_funcs[i] {
                            func.set_order_by_specs(
                                agg.order_by
                                    .iter()
                                    .map(|order| {
                                        AggregateOrderBySpec::new(
                                            order.ascending,
                                            order.nulls_first,
                                        )
                                    })
                                    .collect(),
                            );
                        }
                    }
                }

                let count_star_value = Value::Integer(1);
                let mut expr_values: Vec<Value> = vec![Value::null_unknown(); aggregations.len()];

                for (_, row) in rows {
                    // Create execution context for this row
                    // CRITICAL: Include params for parameterized queries
                    let exec_ctx = ExecuteContext::new(row)
                        .with_params(ctx.params())
                        .with_named_params(ctx.named_params())
                        .with_transaction_id(ctx.transaction_id())
                        .with_stored_function_invoker(ctx.stored_function_invoker());

                    for (i, agg) in aggregations.iter().enumerate() {
                        if let Some(ref mut func) = agg_funcs[i] {
                            // Check FILTER clause first - skip row if filter is false
                            if let Some(ref filter_program) = compiled_filters[i] {
                                if let Some(ref mut vm) = expr_vm {
                                    match vm.execute_cow(filter_program, &exec_ctx) {
                                        Ok(Value::Boolean(true)) => {}
                                        _ => continue,
                                    }
                                } else {
                                    continue;
                                }
                            }

                            let value = if let Some(ref expr_program) = compiled_agg_expressions[i]
                            {
                                if let Some(ref mut vm) = expr_vm {
                                    expr_values[i] = vm
                                        .execute_cow(expr_program, &exec_ctx)
                                        .map_err(|error| {
                                            radixdb_core::Error::expression_evaluation(format!(
                                                "{}({}): {}",
                                                agg.name, agg.column, error
                                            ))
                                        })?;
                                    Some(&expr_values[i])
                                } else {
                                    None
                                }
                            } else {
                                if let Some(col_idx) = agg_col_indices[i] {
                                    row.get(col_idx)
                                } else {
                                    Some(&count_star_value)
                                }
                            };

                            if let Some(v) = value {
                                if !compiled_agg_order_by[i].is_empty() && func.supports_order_by()
                                {
                                    if let Some(ref mut vm) = expr_vm {
                                        let mut sort_keys =
                                            Vec::with_capacity(compiled_agg_order_by[i].len());
                                        for order_program in &compiled_agg_order_by[i] {
                                            sort_keys.push(
                                                vm.execute_cow(order_program, &exec_ctx).map_err(
                                                    |error| {
                                                        radixdb_core::Error::expression_evaluation(
                                                            format!(
                                                                "{} ORDER BY: {}",
                                                                agg.name, error
                                                            ),
                                                        )
                                                    },
                                                )?,
                                            );
                                        }
                                        func.accumulate_with_sort_key(v, sort_keys, agg.distinct);
                                    } else {
                                        func.accumulate(v, agg.distinct);
                                    }
                                } else {
                                    func.accumulate(v, agg.distinct);
                                }
                            }
                        }
                    }
                }

                // Build result row: all GROUP BY columns are NULL, then aggregates, then grouping flags
                // Use CompactVec directly to avoid Vec→CompactVec conversion
                let mut row_values: CompactVec<Value> = CompactVec::with_capacity(
                    group_by_items.len() + aggregations.len() + group_by_items.len(),
                );
                for _ in group_by_items {
                    row_values.push(Value::null_unknown());
                }
                for (i, agg) in aggregations.iter().enumerate() {
                    let value = if let Some(ref func) = agg_funcs[i] {
                        func.try_result()?
                    } else if agg.name == "COUNT" && agg.column == "*" {
                        Value::Integer(rows.len() as i64)
                    } else {
                        Value::null_unknown()
                    };
                    row_values.push(value);
                }
                // Add GROUPING flags: all columns are rolled up in grand total (GROUPING = 1)
                for _ in group_by_items {
                    row_values.push(Value::Integer(1));
                }
                all_result_rows.push((row_id, Row::from_compact_vec(row_values)));
                row_id += 1;
            } else {
                // Partial grouping: group by active columns only
                // Use hash-based grouping with collision handling: u64 hash -> Vec<GroupEntry>
                // Each hash bucket can contain multiple groups (handles hash collisions correctly)
                // FxHashMap is optimized for trusted keys in embedded database context
                let mut groups: FxHashMap<u64, Vec<GroupEntry>> = FxHashMap::default();
                let mut key_buffer: Vec<Value> = Vec::with_capacity(active_count);

                for (row_idx, (_, row)) in rows.iter().enumerate() {
                    key_buffer.clear();

                    // Create execution context for this row
                    // CRITICAL: Include params for parameterized queries
                    let exec_ctx = ExecuteContext::new(row)
                        .with_params(ctx.params())
                        .with_named_params(ctx.named_params())
                        .with_transaction_id(ctx.transaction_id())
                        .with_stored_function_invoker(ctx.stored_function_invoker());

                    // Only include active columns in the key
                    for (col_idx, &is_active) in grouping_set.active_columns.iter().enumerate() {
                        if is_active {
                            let value = match &precomputed_group_by[col_idx] {
                                PrecomputedGroupBy::ColumnIndex(idx) => {
                                    row.get(*idx).cloned().unwrap_or_else(Value::null_unknown)
                                }
                                PrecomputedGroupBy::Position(idx) => {
                                    row.get(*idx).cloned().unwrap_or_else(Value::null_unknown)
                                }
                                PrecomputedGroupBy::Expression(_) => {
                                    // Use pre-compiled expression with VM
                                    if let (Some(ref mut vm), Some(ref program)) =
                                        (&mut expr_vm, &compiled_group_by_exprs[col_idx])
                                    {
                                        vm.execute_cow(program, &exec_ctx).map_err(|e| {
                                            radixdb_core::Error::expression_evaluation(format!(
                                                "GROUP BY: {}",
                                                e
                                            ))
                                        })?
                                    } else {
                                        Value::null_unknown()
                                    }
                                }
                                PrecomputedGroupBy::NotFound => Value::null_unknown(),
                            };
                            key_buffer.push(value);
                        }
                    }

                    // Compute hash and handle bucket with proper collision detection
                    let hash = hash_group_key(&key_buffer);
                    match groups.entry(hash) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            let bucket = e.get_mut();
                            // Search bucket for matching group (handles hash collisions)
                            if let Some(entry) = bucket
                                .iter_mut()
                                .find(|entry| entry.key_values == key_buffer)
                            {
                                entry.row_indices.push(row_idx);
                            } else {
                                // Hash collision: different key with same hash
                                bucket.push(GroupEntry {
                                    key_values: key_buffer.clone(),
                                    row_indices: vec![row_idx],
                                });
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            // First entry for this hash
                            e.insert(vec![GroupEntry {
                                key_values: key_buffer.clone(),
                                row_indices: vec![row_idx],
                            }]);
                        }
                    }
                }

                // Process each group
                let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = aggregations
                    .iter()
                    .map(|agg| {
                        self.host
                            .aggregation_function_registry()
                            .get_aggregate(&agg.name)
                    })
                    .collect();

                // Configure aggregate functions
                for (i, agg) in aggregations.iter().enumerate() {
                    if !agg.extra_args.is_empty() {
                        if let Some(ref mut func) = agg_funcs[i] {
                            func.configure(&agg.extra_args);
                        }
                    }
                    if !agg.order_by.is_empty() {
                        if let Some(ref mut func) = agg_funcs[i] {
                            func.set_order_by_specs(
                                agg.order_by
                                    .iter()
                                    .map(|order| {
                                        AggregateOrderBySpec::new(
                                            order.ascending,
                                            order.nulls_first,
                                        )
                                    })
                                    .collect(),
                            );
                        }
                    }
                }

                let mut expr_values: Vec<Value> = vec![Value::null_unknown(); aggregations.len()];

                // Note: We don't sort groups here for performance.
                // SQL does not guarantee result order without ORDER BY.
                // Users who need ordered results should add ORDER BY to their query.
                // Flatten buckets: each bucket may contain multiple groups (hash collisions)
                for (_hash, bucket) in groups {
                    for group in bucket {
                        // Reset aggregate functions
                        for f in agg_funcs.iter_mut().flatten() {
                            f.reset();
                        }

                        let count_star_value = Value::Integer(1);
                        for &row_idx in &group.row_indices {
                            let (_, row) = &rows[row_idx];

                            // Create execution context for this row
                            // CRITICAL: Include params for parameterized queries
                            let exec_ctx = ExecuteContext::new(row)
                                .with_params(ctx.params())
                                .with_named_params(ctx.named_params())
                                .with_transaction_id(ctx.transaction_id())
                                .with_stored_function_invoker(ctx.stored_function_invoker());

                            for (i, agg) in aggregations.iter().enumerate() {
                                if let Some(ref mut func) = agg_funcs[i] {
                                    // Check FILTER clause first - skip row if filter is false
                                    if let Some(ref filter_program) = compiled_filters[i] {
                                        if let Some(ref mut vm) = expr_vm {
                                            match vm.execute_cow(filter_program, &exec_ctx) {
                                                Ok(Value::Boolean(true)) => {}
                                                _ => continue,
                                            }
                                        } else {
                                            continue;
                                        }
                                    }

                                    let value = if let Some(ref expr_program) =
                                        compiled_agg_expressions[i]
                                    {
                                        if let Some(ref mut vm) = expr_vm {
                                            expr_values[i] = vm
                                                .execute_cow(expr_program, &exec_ctx)
                                                .map_err(|error| {
                                                    radixdb_core::Error::expression_evaluation(
                                                        format!(
                                                            "{}({}): {}",
                                                            agg.name, agg.column, error
                                                        ),
                                                    )
                                                })?;
                                            Some(&expr_values[i])
                                        } else {
                                            None
                                        }
                                    } else {
                                        if let Some(col_idx) = agg_col_indices[i] {
                                            row.get(col_idx)
                                        } else {
                                            Some(&count_star_value)
                                        }
                                    };

                                    if let Some(v) = value {
                                        if !compiled_agg_order_by[i].is_empty()
                                            && func.supports_order_by()
                                        {
                                            if let Some(ref mut vm) = expr_vm {
                                                let mut sort_keys = Vec::with_capacity(
                                                    compiled_agg_order_by[i].len(),
                                                );
                                                for order_program in &compiled_agg_order_by[i] {
                                                    sort_keys.push(
                                                        vm.execute_cow(order_program, &exec_ctx)
                                                            .map_err(|error| {
                                                                radixdb_core::Error::expression_evaluation(
                                                                    format!(
                                                                        "{} ORDER BY: {}",
                                                                        agg.name, error
                                                                    ),
                                                                )
                                                            })?,
                                                    );
                                                }
                                                func.accumulate_with_sort_key(
                                                    v,
                                                    sort_keys,
                                                    agg.distinct,
                                                );
                                            } else {
                                                func.accumulate(v, agg.distinct);
                                            }
                                        } else {
                                            func.accumulate(v, agg.distinct);
                                        }
                                    }
                                }
                            }
                        }

                        // Build result row
                        // For GROUP BY columns: use key value if active, NULL if rolled up
                        // Use CompactVec directly to avoid Vec→CompactVec conversion
                        let mut row_values: CompactVec<Value> = CompactVec::with_capacity(
                            group_by_items.len() + aggregations.len() + group_by_items.len(),
                        );

                        let mut key_idx = 0;
                        for &is_active in &grouping_set.active_columns {
                            if is_active {
                                row_values.push(group.key_values[key_idx].clone());
                                key_idx += 1;
                            } else {
                                row_values.push(Value::null_unknown());
                            }
                        }

                        for (i, agg) in aggregations.iter().enumerate() {
                            let value = if let Some(ref func) = agg_funcs[i] {
                                func.try_result()?
                            } else if agg.name == "COUNT" && agg.column == "*" {
                                Value::Integer(group.row_indices.len() as i64)
                            } else {
                                Value::null_unknown()
                            };
                            row_values.push(value);
                        }

                        // Add GROUPING flags: 0 if column is active (grouped), 1 if rolled up
                        for &is_active in &grouping_set.active_columns {
                            row_values.push(Value::Integer(if is_active { 0 } else { 1 }));
                        }

                        all_result_rows.push((row_id, Row::from_compact_vec(row_values)));
                        row_id += 1;
                    } // end for group in bucket
                } // end for bucket in groups
            }
        }

        // Build result columns
        let mut result_columns: Vec<String> =
            self.resolve_group_by_column_names_new(group_by_items, columns, col_index_map);
        result_columns.extend(aggregations.iter().map(|a| a.get_column_name()));

        // Add hidden grouping flag columns for GROUPING() function support
        // These will be used by the projection phase and stripped from final output
        for i in 0..group_by_items.len() {
            result_columns.push(format!("__grouping_{}__", i));
        }

        Ok((result_columns, all_result_rows))
    }

    /// Generate grouping sets for ROLLUP
    /// ROLLUP(a, b, c) generates: (a,b,c), (a,b), (a), ()
    pub(super) fn generate_rollup_sets(num_columns: usize) -> Vec<GroupingSet> {
        let mut sets = Vec::with_capacity(num_columns + 1);

        // From most specific to least specific (grand total)
        for active_count in (0..=num_columns).rev() {
            let mut active_columns = vec![false; num_columns];
            for item in active_columns.iter_mut().take(active_count) {
                *item = true;
            }
            sets.push(GroupingSet { active_columns });
        }

        sets
    }

    /// Generate grouping sets for CUBE
    /// CUBE(a, b) generates: (a,b), (a), (b), ()
    pub(super) fn generate_cube_sets(num_columns: usize) -> Vec<GroupingSet> {
        let mut sets = Vec::with_capacity(1 << num_columns);

        // Generate all 2^n combinations
        for mask in (0..(1 << num_columns)).rev() {
            let mut active_columns = vec![false; num_columns];
            for (i, item) in active_columns.iter_mut().enumerate() {
                if mask & (1 << (num_columns - 1 - i)) != 0 {
                    *item = true;
                }
            }
            sets.push(GroupingSet { active_columns });
        }

        sets
    }

    /// Generate grouping sets from explicit GROUPING SETS clause
    /// GROUPING SETS ((a, b), (a), ()) generates exactly those three sets
    pub(super) fn generate_explicit_grouping_sets(
        sets: &[Vec<Expression>],
        group_by_items: &[GroupByItem],
    ) -> Vec<GroupingSet> {
        let num_columns = group_by_items.len();
        let mut result = Vec::with_capacity(sets.len());

        // Build a lookup from canonical key to group_by_items index
        // Uses the same canonical key function for consistent matching
        let item_to_index: StringMap<usize> = group_by_items
            .iter()
            .enumerate()
            .map(|(i, item)| (group_by_item_canonical_key(item), i))
            .collect();

        for set in sets {
            let mut active_columns = vec![false; num_columns];

            for expr in set {
                // Get the canonical key for this expression
                let key = expression_canonical_key(expr);

                // Find the index in group_by_items
                if let Some(&idx) = item_to_index.get(&key) {
                    active_columns[idx] = true;
                }
            }

            result.push(GroupingSet { active_columns });
        }

        result
    }

    /// Resolve GROUP BY column names for result (new version supporting GroupByItem)
    pub(super) fn resolve_group_by_column_names_new(
        &self,
        group_by_items: &[GroupByItem],
        columns: &[String],
        col_index_map: &StringMap<usize>,
    ) -> Vec<String> {
        let mut names = Vec::new();
        for item in group_by_items {
            match item {
                GroupByItem::Column(col_name) => {
                    // Use lookup_column_index to handle qualified names (e.g., "t.dept" -> "dept")
                    if let Some(idx) =
                        Self::lookup_column_index(&col_name.to_lowercase(), col_index_map)
                    {
                        if idx < columns.len() {
                            names.push(columns[idx].clone());
                        } else {
                            names.push(col_name.clone());
                        }
                    } else {
                        names.push(col_name.clone());
                    }
                }
                GroupByItem::Position(pos) => {
                    // Position is 1-indexed
                    let idx = pos.saturating_sub(1);
                    if idx < columns.len() {
                        names.push(columns[idx].clone());
                    } else {
                        names.push(format!("${}", pos));
                    }
                }
                GroupByItem::Expression { display_name, .. } => {
                    // Use the display name (alias or generated name)
                    names.push(display_name.clone());
                }
            }
        }
        names
    }

    /// Look up column index, handling both qualified (e.g., "o.amount") and unqualified names.
    /// If a qualified name lookup fails, tries the unqualified part (after the dot).
    pub(super) fn lookup_column_index(
        column_lower: &str,
        col_index_map: &StringMap<usize>,
    ) -> Option<usize> {
        // First try exact match
        if let Some(&idx) = col_index_map.get(column_lower) {
            return Some(idx);
        }

        // If it's a qualified name (contains a dot), try the unqualified part
        if let Some(dot_pos) = column_lower.rfind('.') {
            let unqualified = &column_lower[dot_pos + 1..];
            if let Some(&idx) = col_index_map.get(unqualified) {
                return Some(idx);
            }
        }

        None
    }

    /// Apply HAVING clause to aggregated results
    pub(super) fn apply_having(
        &self,
        result: Box<dyn QueryResult>,
        having: &Expression,
        columns: &[String],
        agg_aliases: &[(String, usize)],
        expr_aliases: &[(String, usize)],
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        // Materialize the result
        let mut rows = RowVec::new();
        let mut result = result;
        let mut row_id = 0i64;
        while result.next() {
            rows.push((row_id, result.take_row()));
            row_id += 1;
        }

        // Combine all aliases for HAVING clause evaluation
        let mut all_aliases: Vec<(String, usize)> = agg_aliases.to_vec();
        all_aliases.extend_from_slice(expr_aliases);

        // Create RowFilter with all aliases and context
        let having_filter =
            RowFilter::with_aliases_and_context(having, columns, &all_aliases, ctx)?;

        // Filter rows using the pre-compiled filter
        let mut filtered_rows = RowVec::new();
        let mut new_id = 0i64;
        for (_, row) in rows {
            if having_filter.matches_checked(&row)? {
                filtered_rows.push((new_id, row));
                new_id += 1;
            }
        }

        Ok(Box::new(ExecutorResult::new(
            columns.to_vec(),
            filtered_rows,
        )))
    }
}
