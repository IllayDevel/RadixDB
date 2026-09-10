use super::*;

impl<'host, H: AggregationHost + ?Sized> AggregationExecutor<'host, H> {
    /// Execute SELECT with aggregation (GROUP BY support)
    pub(crate) fn execute_select_with_aggregation(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        base_rows: RowVec,
        base_columns: &[String],
    ) -> Result<Box<dyn QueryResult>> {
        // Parse aggregations and group by columns
        let (aggregations, _non_agg_columns) = self.parse_aggregations(stmt)?;
        let group_by_columns = self.parse_group_by(stmt, base_columns)?;

        // Create column index map for fast lookup (FxHashMap for speed)
        let col_index_map = build_column_index_map(base_columns);

        // Determine if we can push LIMIT to aggregation for early termination
        // This is safe when:
        // 1. There's a LIMIT but no ORDER BY (order doesn't matter)
        // 2. No HAVING clause (filtering might reduce groups below limit)
        // 3. No DISTINCT (deduplication might reduce results)
        let can_push_limit = stmt.limit.is_some()
            && stmt.order_by.is_empty()
            && stmt.having.is_none()
            && !stmt.distinct;

        let aggregation_limit = if can_push_limit {
            stmt.limit.as_ref().and_then(|limit_expr| {
                let mut eval = ExpressionEval::compile(limit_expr, &[])
                    .ok()?
                    .with_context(ctx);
                eval.eval_slice(&Row::new()).ok().and_then(|v| match v {
                    radixdb_core::Value::Integer(n) if n >= 0 => Some(n as usize),
                    _ => None,
                })
            })
        } else {
            None
        };

        // Build result
        // having_applied_inline tracks if HAVING was already applied during fast aggregation
        let (result_columns, result_rows, having_applied_inline) = if group_by_columns.is_empty() {
            // Global aggregation (no GROUP BY)
            let (cols, rows) = self.execute_global_aggregation(
                &aggregations,
                &base_rows,
                base_columns,
                &col_index_map,
                ctx,
            )?;
            (cols, rows, false)
        } else if stmt.group_by.modifier != GroupByModifier::None {
            // ROLLUP or CUBE aggregation
            let (cols, rows) = self.execute_rollup_aggregation(
                &aggregations,
                &group_by_columns,
                &base_rows,
                base_columns,
                &col_index_map,
                stmt,
                ctx,
            )?;
            (cols, rows, false)
        } else {
            // Regular grouped aggregation - pass limit for early termination
            // May apply HAVING inline for simple cases (returns having_applied flag)
            self.execute_grouped_aggregation(
                &aggregations,
                &group_by_columns,
                &base_rows,
                base_columns,
                &col_index_map,
                stmt,
                ctx,
                aggregation_limit,
            )?
        };

        // Apply HAVING clause BEFORE projection (HAVING may reference aggregates not in SELECT)
        // Skip if HAVING was already applied inline during fast aggregation
        let (having_columns, having_rows) = if let Some(ref having) = stmt.having {
            if having_applied_inline {
                // HAVING already applied inline, skip separate filtering
                (result_columns, result_rows)
            } else {
                // Pre-process scalar subqueries in HAVING clause
                // This executes subqueries like (SELECT AVG(a) FROM t) and replaces them with values
                let processed_having = self
                    .host
                    .aggregation_process_where_subqueries(having, ctx)?;

                // Build aggregate expression aliases for HAVING clause
                // IMPORTANT: Include ALL aggregates, not just aliased ones,
                // because CompiledEvaluator needs expression_aliases to match FunctionCall expressions
                let group_by_count = group_by_columns.len();
                let agg_aliases: Vec<(String, usize)> = aggregations
                    .iter()
                    .enumerate()
                    .map(|(i, agg)| (agg.get_expression_name(), group_by_count + i))
                    .collect();

                // Build GROUP BY expression aliases for HAVING clause
                // This maps expressions like "x + y" to their GROUP BY column indices
                let expr_aliases: Vec<(String, usize)> = group_by_columns
                    .iter()
                    .enumerate()
                    .filter_map(|(i, item)| {
                        if let GroupByItem::Expression { expr, .. } = item {
                            Some((self.expression_to_string(expr), i))
                        } else {
                            None
                        }
                    })
                    .collect();

                let tmp_result = Box::new(ExecutorResult::new(result_columns.clone(), result_rows));
                let mut having_result = self.apply_having(
                    tmp_result,
                    &processed_having,
                    &result_columns,
                    &agg_aliases,
                    &expr_aliases,
                    ctx,
                )?;

                // Collect rows after HAVING filter
                let mut filtered_rows = RowVec::new();
                let mut row_id = 0i64;
                while having_result.next() {
                    filtered_rows.push((row_id, having_result.take_row()));
                    row_id += 1;
                }
                (result_columns, filtered_rows)
            }
        } else {
            (result_columns, result_rows)
        };

        // Check for hidden aggregates (ORDER BY only) BEFORE cloning
        // These will be removed after sorting by the ProjectedResult wrapper
        let group_by_count = group_by_columns.len();
        let hidden_aggs: Vec<(usize, &SqlAggregateFunction)> = aggregations
            .iter()
            .enumerate()
            .filter(|(_, agg)| agg.hidden)
            .collect();

        // Apply post-aggregation expression evaluation and column projection
        // Only clone if we need the original data for hidden_aggs processing
        let (final_columns, final_rows) = if hidden_aggs.is_empty() {
            // No hidden aggregates - move data directly (no clone)
            self.apply_post_aggregation_expressions(stmt, ctx, having_columns, having_rows)?
        } else {
            // Hidden aggregates exist - need to clone to preserve original for later use
            let having_col_index_map = build_column_index_map(&having_columns);
            let (mut cols, mut rows) = self.apply_post_aggregation_expressions(
                stmt,
                ctx,
                having_columns.clone(),
                having_rows.clone(),
            )?;

            // Append hidden aggregates to the result for ORDER BY to use
            for (agg_idx, agg) in &hidden_aggs {
                // Get the column name for this aggregate
                let col_name = agg.get_column_name();
                cols.push(col_name.clone());

                // Find the index in having_columns (group_by_count + aggregate index)
                let having_idx = group_by_count + agg_idx;

                // Append the value from each row
                for (row_idx, (_, row)) in rows.iter_mut().enumerate() {
                    if let Some((_, having_row)) = having_rows.get(row_idx) {
                        if let Some(val) = having_row.get(having_idx) {
                            row.push(val.clone());
                        } else {
                            row.push(Value::null_unknown());
                        }
                    } else {
                        row.push(Value::null_unknown());
                    }
                }
            }

            // Also try to find by column name in case index doesn't match
            // (This handles cases where aggregate was deduplicated but still marked hidden)
            for (_, agg) in &hidden_aggs {
                let col_name = agg.get_column_name();
                let col_lower = col_name.to_lowercase();
                if let Some(&idx) = having_col_index_map.get(&col_lower) {
                    // Only add if not already added by index
                    if !cols.iter().any(|c| c.eq_ignore_ascii_case(&col_name)) {
                        cols.push(col_name);
                        for (row_idx, (_, row)) in rows.iter_mut().enumerate() {
                            if let Some((_, having_row)) = having_rows.get(row_idx) {
                                if let Some(val) = having_row.get(idx) {
                                    row.push(val.clone());
                                } else {
                                    row.push(Value::null_unknown());
                                }
                            } else {
                                row.push(Value::null_unknown());
                            }
                        }
                    }
                }
            }

            (cols, rows)
        };

        let result: Box<dyn QueryResult> = Box::new(ExecutorResult::new(final_columns, final_rows));

        Ok(result)
    }
}
