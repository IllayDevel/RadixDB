impl Executor {
    pub(crate) fn evaluate_page_expression(
        expr: &Expression,
        ctx: &ExecutionContext,
        clause: &str,
    ) -> Result<usize> {
        crate::pipeline::paging::evaluate_page_expression(expr, ctx, clause)
    }

    /// Execute a SELECT statement
    pub(crate) fn execute_select(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        // Nested SELECT/CTE/set-operation scopes enter here directly rather
        // than through execute_statement. Bind their navigation graph at the
        // same scope boundary; lowered/extracted source statements no longer
        // contain navigation and therefore cannot recurse here indefinitely.
        if let Some(plan) = super::navigation::bind_reference_expand_plan_for_execution(
            self.engine.as_ref(),
            stmt,
            ctx,
        )? {
            return self.execute_reference_projection(stmt, &plan, ctx);
        }

        // Clear query-local caches only at the top level. The timeout owner is
        // installed by Executor::execute_with_context and remains attached to
        // the returned QueryResult until cursor close/drop.
        if ctx.query_depth() == 0 {
            // Every subquery cache is query-local. Keeping entries across top-level
            // statements would require database, snapshot and bound parameters in
            // the key plus cross-thread invalidation. Clearing here preserves reuse
            // inside one statement without publishing results into another
            // execution context.
            crate::context::clear_scalar_subquery_cache();
            crate::context::clear_in_subquery_cache();
            crate::context::clear_semi_join_cache();
            clear_exists_predicate_cache();
            clear_exists_index_cache();
            clear_exists_fetcher_cache();
            clear_count_counter_cache();
            clear_exists_schema_cache();
            clear_exists_pred_key_cache();
            clear_exists_correlation_cache();
            clear_batch_aggregate_cache();
            clear_batch_aggregate_info_cache();
        }

        // Check for cancellation at entry point
        ctx.check_cancelled()?;

        // Validate: aggregate functions are not allowed in WHERE clause
        if let Some(ref where_clause) = stmt.where_clause {
            if expression_contains_aggregate(where_clause) {
                return Err(Error::invalid_argument(
                    "aggregate functions are not allowed in WHERE clause (use HAVING instead)",
                ));
            }
        }

        // Check for CTEs (WITH clause)
        if self.has_cte(stmt) {
            return self.execute_select_with_ctes(stmt, ctx);
        }

        // WITH must be materialized or inlined before JOIN binding so CTE
        // columns are available in the request-local execution context.
        if let Some(Expression::JoinSource(join_source)) = stmt.table_expr.as_deref() {
            self.validate_join_statement_bindings(stmt, join_source, ctx)?;
        }

        // OPTIMIZATION: Get cached query classification ONCE at entry point
        // This classification is passed through the call chain to avoid
        // redundant hash computations and cache lookups (was 10+ calls per query)
        let logical_planning_started =
            radixdb_storage::instrumentation::join_planning_probe_active()
                .then(radixdb_core::time_compat::Instant::now);
        let classification = get_classification(stmt);
        if classification.has_joins {
            if let Some(started) = logical_planning_started {
                radixdb_storage::instrumentation::record_join_logical_planning(started.elapsed());
            }
        }

        // Evaluate the public page window once. It remains the sole owner of
        // trailing LIMIT/OFFSET for both simple and compound SELECTs.
        let page = PageWindow::evaluate(stmt, ctx)?;
        let limit = page.limit;
        let offset = page.offset;

        // Execute the main query. In a compound SELECT the trailing
        // ORDER BY/LIMIT/OFFSET belongs to the complete set expression, not to
        // its left operand. Removing that tail here also prevents private
        // ORDER BY/JOIN dependency columns from changing UNION arity.
        let left_operand;
        let (execution_stmt, execution_classification) = if stmt.set_operations.is_empty() {
            (stmt, Arc::clone(&classification))
        } else {
            left_operand = {
                let mut statement = stmt.clone();
                statement.set_operations.clear();
                statement.order_by.clear();
                statement.limit = None;
                statement.offset = None;
                statement
            };
            let left_classification = get_classification(&left_operand);
            (&left_operand, left_classification)
        };

        // The third return value indicates if LIMIT/OFFSET was already applied (by storage-level pushdown)
        // The fourth return value contains deferred projection info if applicable
        let (mut result, mut columns, limit_offset_applied, deferred_projection) =
            self.execute_select_internal(execution_stmt, ctx, &execution_classification)?;

        let expected_columns = self.count_select_columns(stmt);
        // Apply set operations (UNION, INTERSECT, EXCEPT)
        let mut limit_offset_applied = limit_offset_applied;
        if !stmt.set_operations.is_empty() {
            result = pipeline_set::execute_set_operations(
                result,
                &stmt.set_operations,
                ctx,
                None,
                |right, set_ctx| self.execute_select(right, set_ctx),
            )?;
            columns = CompactArc::new(result.columns().to_vec());
            // Set semantics must validate/materialize every operand first. The
            // outer logical query remains the sole owner of LIMIT/OFFSET.
            limit_offset_applied = false;
        }

        // Count expected SELECT columns (before any extra ORDER BY columns)
        let distinct_after_order = stmt.distinct
            && stmt.distinct_on.is_empty()
            && expected_columns > 0
            && columns.len() > expected_columns
            && !stmt.order_by.is_empty();

        // Apply DISTINCT (skip for DISTINCT ON — it's applied after ORDER BY)
        // When ORDER BY references columns not in SELECT, we add extra columns for sorting.
        // DISTINCT should only consider the original SELECT columns, not the extra ORDER BY columns.
        if stmt.distinct && stmt.distinct_on.is_empty() && !distinct_after_order {
            result = if columns.len() > expected_columns && expected_columns > 0 {
                pipeline_distinct::apply(result, Some(expected_columns))
            } else {
                pipeline_distinct::apply(result, None)
            };
        }

        // Apply ORDER BY (with TOP-N optimization if LIMIT is present)
        // Note: LIMIT/OFFSET was already evaluated earlier for set operations optimization
        // Skip ORDER BY if storage-level optimization already applied sorting + LIMIT/OFFSET
        // classification was already obtained at entry point and passed through
        if !stmt.order_by.is_empty() && !limit_offset_applied {
            let ordinal_width = if expected_columns > 0 {
                expected_columns
            } else {
                columns.len()
            };
            crate::pipeline::paging::validate_order_ordinals(stmt, ordinal_width)?;

            // Helper to format aggregate function call as column name
            let format_agg_column = |func: &radixdb_sql::ast::FunctionCall| -> String {
                if func.arguments.is_empty() {
                    format!("{}(*)", func.function)
                } else {
                    let arguments = func
                        .arguments
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{}({arguments})", func.function)
                }
            };

            // Check if ORDER BY expression can be mapped to existing column (handles aggregates)
            let try_map_to_column = |ob: &radixdb_sql::ast::OrderByExpression| -> Option<usize> {
                match &ob.expression {
                    Expression::Identifier(id) => {
                        // First, try matching against output column names
                        if let Some(pos) = columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&id.value_lower))
                        {
                            return Some(pos);
                        }
                        // Also check if this identifier matches the original expression of an aliased column
                        // e.g., SELECT val AS amount ... ORDER BY val should use the amount column
                        for (i, select_expr) in stmt.columns.iter().enumerate() {
                            if let Expression::Aliased(aliased) = select_expr {
                                // Check if the aliased expression is an identifier matching our ORDER BY
                                if let Expression::Identifier(aliased_id) = &*aliased.expression {
                                    if aliased_id.value_lower == id.value_lower {
                                        return Some(i);
                                    }
                                }
                                // Also check qualified identifier (table.column)
                                if let Expression::QualifiedIdentifier(qid) = &*aliased.expression {
                                    if qid.name.value_lower == id.value_lower {
                                        return Some(i);
                                    }
                                }
                            }
                        }
                        None
                    }
                    Expression::QualifiedIdentifier(qid) => {
                        // Handle qualified column names like "c.name" for ORDER BY
                        // First, try matching against SELECT expressions directly
                        for (i, select_expr) in stmt.columns.iter().enumerate() {
                            match select_expr {
                                Expression::QualifiedIdentifier(sel_qid)
                                    // Direct match: ORDER BY c.name matches SELECT c.name
                                    if sel_qid.qualifier.value_lower == qid.qualifier.value_lower
                                        && sel_qid.name.value_lower == qid.name.value_lower =>
                                {
                                    return Some(i);
                                }
                                Expression::Aliased(aliased) => {
                                    // Check if the aliased expression matches
                                    if let Expression::QualifiedIdentifier(sel_qid) =
                                        aliased.expression.as_ref()
                                    {
                                        if sel_qid.qualifier.value_lower
                                            == qid.qualifier.value_lower
                                            && sel_qid.name.value_lower == qid.name.value_lower
                                        {
                                            return Some(i);
                                        }
                                    }
                                    // Also check if the alias matches the base column name
                                    if aliased.alias.value_lower == qid.name.value_lower {
                                        return Some(i);
                                    }
                                }
                                _ => {}
                            }
                        }
                        // Fallback: try full qualified name first, then unqualified
                        let full_name =
                            format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                        if let Some(pos) = columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&full_name))
                        {
                            return Some(pos);
                        }
                        columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&qid.name.value_lower))
                    }
                    Expression::IntegerLiteral(lit) => Some((lit.value as usize).saturating_sub(1)),
                    Expression::FunctionCall(func) => {
                        // Check if this is an aggregate function that exists as a column
                        let col_name = format_agg_column(func);
                        if let Some(pos) = columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&col_name))
                        {
                            return Some(pos);
                        }
                        // Also check if any SELECT column is an aliased version of this expression
                        // e.g., SELECT SUM(amount) AS total ... ORDER BY SUM(amount)
                        for (i, select_expr) in stmt.columns.iter().enumerate() {
                            match select_expr {
                                Expression::Aliased(aliased) => {
                                    if let Expression::FunctionCall(sel_func) = &*aliased.expression
                                    {
                                        if sel_func.function.eq_ignore_ascii_case(&func.function) {
                                            // Compare arguments
                                            let sel_col_name = format_agg_column(sel_func);
                                            if sel_col_name.eq_ignore_ascii_case(&col_name) {
                                                return Some(i);
                                            }
                                        }
                                    }
                                }
                                Expression::FunctionCall(sel_func)
                                    if sel_func.function.eq_ignore_ascii_case(&func.function) =>
                                {
                                    let sel_col_name = format_agg_column(sel_func);
                                    if sel_col_name.eq_ignore_ascii_case(&col_name) {
                                        return Some(i);
                                    }
                                }
                                _ => {}
                            }
                        }
                        let expr_name = ob.expression.to_string();
                        if let Some(pos) = columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&expr_name))
                        {
                            return Some(pos);
                        }
                        None
                    }
                    _ => {
                        // For any other expression (Infix, Prefix, Cast, etc.),
                        // check if it matches an aliased SELECT expression
                        // e.g., ORDER BY val * 2 when SELECT has val * 2 as doubled
                        let order_expr_str = format!("{}", ob.expression);
                        if let Some(pos) = columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&order_expr_str))
                        {
                            return Some(pos);
                        }
                        for (i, select_expr) in stmt.columns.iter().enumerate() {
                            match select_expr {
                                Expression::Aliased(aliased) => {
                                    let aliased_expr_str = format!("{}", aliased.expression);
                                    // Compare the string representations of expressions
                                    if order_expr_str == aliased_expr_str {
                                        return Some(i);
                                    }
                                }
                                other if order_expr_str == other.to_string() => {
                                    return Some(i);
                                }
                                _ => {}
                            }
                        }
                        None
                    }
                }
            };

            // Check if any ORDER BY expression needs evaluation (not just column refs or position)
            let has_complex_order_by = stmt
                .order_by
                .iter()
                .any(|ob| try_map_to_column(ob).is_none());

            // If ORDER BY has complex expressions, evaluate them and sort by keys
            if has_complex_order_by {
                let has_distinct_on = !stmt.distinct_on.is_empty();
                let needs_post_sort_distinct = has_distinct_on || distinct_after_order;

                // Complex expressions use the same bounded Top-N contract as
                // mapped columns. Evaluate one source row at a time and retain
                // only OFFSET+LIMIT rows plus their sort keys.
                if let Some(limit_rows) = limit.filter(|_| !needs_post_sort_distinct) {
                    use super::utils::RetainedRowsBudget;

                    let heap_capacity = limit_rows.saturating_add(offset);
                    let mut heap = BinaryHeap::with_capacity(heap_capacity.min(16_384));
                    let mut retained =
                        RetainedRowsBudget::with_request_memory("complex ORDER BY TopN", ctx)?;
                    let mut evaluator =
                        CompiledEvaluator::new(&self.function_registry).with_context(ctx);
                    evaluator.init_columns(&columns);
                    let correlated = classification.order_by_has_correlated_subqueries;
                    let columns_lower: Vec<CompactArc<str>> = columns
                        .iter()
                        .map(|column| CompactArc::from(column.to_lowercase().as_str()))
                        .collect();
                    let table_alias: Option<SmartString> =
                        stmt.table_expr.as_ref().and_then(|te| match te.as_ref() {
                            Expression::TableSource(source) => source
                                .alias
                                .as_ref()
                                .map(|alias| alias.value_lower.clone())
                                .or_else(|| Some(source.name.value_lower.clone())),
                            Expression::Aliased(aliased) => Some(aliased.alias.value_lower.clone()),
                            _ => None,
                        });
                    let qualified_names: Option<Vec<CompactArc<str>>> =
                        table_alias.as_ref().map(|alias| {
                            columns_lower
                                .iter()
                                .map(|column| {
                                    CompactArc::from(format!("{}.{}", alias, column).as_str())
                                })
                                .collect()
                        });
                    let specs = CompactArc::new(
                        stmt.order_by
                            .iter()
                            .map(|order| (order.ascending, order.nulls_first))
                            .collect(),
                    );
                    let order_key_context = ComplexOrderKeyContext {
                        stmt,
                        columns: &columns,
                        execution: ctx,
                        columns_lower: &columns_lower,
                        qualified_names: qualified_names.as_deref(),
                        correlated,
                    };
                    let mut ordinal = 0_u64;

                    while result.next() {
                        if ordinal & 0xff == 0 {
                            ctx.check_cancelled()?;
                        }
                        let row = result.take_row();
                        let keys = self.evaluate_complex_order_keys(
                            &order_key_context,
                            &row,
                            &mut evaluator,
                        )?;
                        let candidate = ComplexTopNRow {
                            row,
                            keys,
                            ordinal,
                            specs: CompactArc::clone(&specs),
                        };
                        ordinal = ordinal.saturating_add(1);

                        if heap_capacity == 0 {
                            continue;
                        }
                        if heap.len() < heap_capacity {
                            retained.admit_row_and_values(&candidate.row, &candidate.keys)?;
                            heap.push(candidate);
                        } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                            let evicted = heap.pop().expect("non-empty TopN heap");
                            retained.release_row_and_values(&evicted.row, &evicted.keys);
                            retained.admit_row_and_values(&candidate.row, &candidate.keys)?;
                            heap.push(candidate);
                        }
                    }
                    if let Some(error) = result.last_error() {
                        return Err(error);
                    }

                    let peak_candidates = retained.peak_rows();
                    let peak_bytes = retained.peak_bytes();
                    let mut retained_rows = heap.into_vec();
                    retained_rows.sort_unstable();
                    let mut result_rows =
                        RowVec::with_capacity(limit_rows.min(retained_rows.len()));
                    for (position, entry) in retained_rows.into_iter().enumerate() {
                        retained.release_row_and_values(&entry.row, &entry.keys);
                        if position >= offset && result_rows.len() < limit_rows {
                            retained.admit(&entry.row)?;
                            result_rows.push((result_rows.len() as i64, entry.row));
                        }
                    }
                    radixdb_storage::instrumentation::record_join_top_n(
                        ordinal,
                        peak_candidates as u64,
                        peak_bytes as u64,
                        result_rows.len() as u64,
                    );
                    let needs_extra_col_removal =
                        columns.len() > expected_columns && expected_columns > 0;
                    let mut result_rows = result_rows;
                    if needs_extra_col_removal {
                        for (_, row) in result_rows.iter_mut() {
                            row.truncate(expected_columns);
                        }
                    }
                    let output_columns = if needs_extra_col_removal {
                        CompactArc::new(columns[..expected_columns].to_vec())
                    } else {
                        CompactArc::clone(&columns)
                    };
                    return Ok(Box::new(TopNResult::from_rows_with_budget(
                        output_columns.as_ref().clone(),
                        result_rows,
                        retained,
                    )));
                }

                // Materialize current result if needed
                let mut rows = RowVec::new();
                let mut row_id_counter = 0i64;
                while result.next() {
                    rows.push((row_id_counter, result.take_row()));
                    row_id_counter += 1;
                }
                if let Some(err) = result.last_error() {
                    return Err(err);
                }

                // Create evaluator for ORDER BY expressions
                let mut evaluator = CompiledEvaluator::new(&self.function_registry);
                evaluator = evaluator.with_context(ctx);
                evaluator.init_columns(&columns);

                // Check if any ORDER BY expression contains a correlated subquery
                // Use cached classification to avoid expensive AST traversal
                let has_correlated_order_by = classification.order_by_has_correlated_subqueries;

                // OPTIMIZATION: Instead of cloning rows and appending sort keys,
                // compute sort keys separately and use index-based sorting.
                // This avoids O(n * row_size) cloning overhead.
                let num_order_cols = stmt.order_by.len();

                // Compute sort keys for each row: Vec<Vec<Value>>
                // Each inner Vec contains the evaluated ORDER BY expressions for that row
                let sort_keys: Vec<Vec<Value>> = if has_correlated_order_by {
                    // For correlated subqueries, we need to process per-row with outer context
                    let columns_arc = CompactArc::clone(&columns);
                    // Extract table alias for qualified column names
                    let order_table_alias: Option<SmartString> =
                        stmt.table_expr.as_ref().and_then(|te| match te.as_ref() {
                            radixdb_sql::ast::Expression::TableSource(source) => source
                                .alias
                                .as_ref()
                                .map(|a| a.value_lower.clone())
                                .or_else(|| Some(source.name.value_lower.clone())),
                            radixdb_sql::ast::Expression::Aliased(aliased) => {
                                Some(aliased.alias.value_lower.clone())
                            }
                            _ => None,
                        });

                    // OPTIMIZATION: Pre-compute lowercase column names once before row loop
                    // This avoids per-row to_lowercase() calls. Use CompactArc<str> for zero-cost clone.
                    let columns_lower: Vec<CompactArc<str>> = columns
                        .iter()
                        .map(|c| CompactArc::from(c.to_lowercase().as_str()))
                        .collect();
                    // Also pre-compute qualified names if alias is present
                    let qualified_names: Option<Vec<CompactArc<str>>> =
                        order_table_alias.as_ref().map(|alias| {
                            columns_lower
                                .iter()
                                .map(|c| CompactArc::from(format!("{}.{}", alias, c).as_str()))
                                .collect()
                        });

                    rows.iter()
                        .map(|(_, row)| -> Result<Vec<Value>> {
                            // Build outer row context from current row
                            let mut outer_row_map: FxHashMap<CompactArc<str>, Value> =
                                FxHashMap::default();
                            for (idx, col_lower) in columns_lower.iter().enumerate() {
                                let val = row.get(idx).cloned().unwrap_or(Value::null_unknown());
                                // Use pre-computed lowercase and qualified names (Arc clone is cheap)
                                if let Some(ref qualified) = qualified_names {
                                    outer_row_map.insert(qualified[idx].clone(), val.clone());
                                    outer_row_map.insert(col_lower.clone(), val);
                                // move
                                } else {
                                    outer_row_map.insert(col_lower.clone(), val);
                                    // move directly, no clone
                                }
                            }

                            // Create context with outer row for correlated subquery evaluation
                            let correlated_ctx =
                                ctx.with_outer_row(outer_row_map, columns_arc.clone());

                            evaluator.set_row_array(row);
                            stmt.order_by
                                .iter()
                                .map(|ob| -> Result<Value> {
                                    // Try processing correlated subqueries first
                                    if Self::has_correlated_subqueries(&ob.expression) {
                                        let processed_expr = self.process_correlated_expression(
                                            &ob.expression,
                                            &correlated_ctx,
                                        )?;
                                        let mut corr_eval =
                                            CompiledEvaluator::new(&self.function_registry);
                                        corr_eval.init_columns(&columns);
                                        corr_eval.set_row_array(row);
                                        corr_eval = corr_eval.with_context(&correlated_ctx);
                                        corr_eval.evaluate(&processed_expr)
                                    } else {
                                        evaluator.evaluate(&ob.expression)
                                    }
                                })
                                .collect()
                        })
                        .collect::<Result<Vec<_>>>()?
                } else {
                    rows.iter()
                        .map(|(_, row)| -> Result<Vec<Value>> {
                            evaluator.set_row_array(row);
                            stmt.order_by
                                .iter()
                                .map(|ob| {
                                    evaluator.evaluate(&ob.expression).map_err(|source| {
                                        Error::internal(format!(
                                            "ORDER BY expression `{}` failed against output columns {:?}: {}",
                                            ob.expression, columns, source
                                        ))
                                    })
                                })
                                .collect()
                        })
                        .collect::<Result<Vec<_>>>()?
                };

                // Create indices and sort them based on sort_keys
                let mut indices: Vec<usize> = (0..rows.len()).collect();

                // Use sort_unstable_by for ~10-20% speedup (stability not needed for ORDER BY)
                indices.sort_unstable_by(|&a_idx, &b_idx| {
                    let a_keys = &sort_keys[a_idx];
                    let b_keys = &sort_keys[b_idx];

                    for i in 0..num_order_cols {
                        let ascending = stmt.order_by[i].ascending;
                        let nulls_first = stmt.order_by[i].nulls_first;
                        let a_val = a_keys.get(i);
                        let b_val = b_keys.get(i);

                        // Check if either value is NULL
                        let a_is_null =
                            a_val.is_none() || a_val.map(|v| v.is_null()).unwrap_or(true);
                        let b_is_null =
                            b_val.is_none() || b_val.map(|v| v.is_null()).unwrap_or(true);

                        // Handle NULL comparison
                        if a_is_null || b_is_null {
                            if a_is_null && b_is_null {
                                continue; // Both NULL, move to next column
                            }
                            // Default: NULLS LAST for ASC, NULLS FIRST for DESC
                            let nulls_come_first = nulls_first.unwrap_or(!ascending);
                            return if a_is_null {
                                if nulls_come_first {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            } else if nulls_come_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            };
                        }

                        let cmp = match (a_val, b_val) {
                            (Some(av), Some(bv)) => av.partial_cmp(bv).unwrap_or(Ordering::Equal),
                            _ => Ordering::Equal,
                        };
                        let cmp = if !ascending { cmp.reverse() } else { cmp };
                        if cmp != Ordering::Equal {
                            return cmp;
                        }
                    }
                    Ordering::Equal
                });

                // Reorder rows using sorted indices
                // OPTIMIZATION: For LIMIT queries without DISTINCT ON, only collect needed rows
                // For full results, use in-place cycle-based permutation
                // When DISTINCT ON is active, skip early LIMIT (applied after dedup)
                let final_rows: RowVec =
                    if (limit.is_some() || offset > 0) && !needs_post_sort_distinct {
                        // With LIMIT/OFFSET: Only collect the rows we actually need
                        let take_count = limit.unwrap_or(usize::MAX);
                        indices
                            .into_iter()
                            .skip(offset)
                            .take(take_count)
                            .enumerate()
                            .map(|(new_idx, i)| (new_idx as i64, std::mem::take(&mut rows[i].1)))
                            .collect()
                    } else {
                        // No LIMIT or DISTINCT ON active: Use in-place cycle-based permutation
                        let n = rows.len();
                        for start in 0..n {
                            // Skip if already in correct position or already processed
                            if indices[start] == start || indices[start] == usize::MAX {
                                continue;
                            }

                            // Follow the cycle
                            let mut current = start;
                            loop {
                                let target = indices[current];
                                if target == start {
                                    // Cycle complete
                                    indices[current] = usize::MAX; // Mark as processed
                                    break;
                                }
                                rows.swap(current, target);
                                indices[current] = usize::MAX; // Mark as processed
                                current = target;
                            }
                        }
                        rows
                    };

                // Project to expected columns if needed
                // When DISTINCT ON is active, keep extra columns until after dedup
                let mut result_rows = final_rows;
                let needs_extra_col_removal =
                    columns.len() > expected_columns && expected_columns > 0;
                if needs_extra_col_removal && !needs_post_sort_distinct {
                    for (_, row) in result_rows.iter_mut() {
                        row.truncate(expected_columns);
                    }
                }

                // Use original column names if expected_columns matches
                let output_columns = if needs_extra_col_removal && !needs_post_sort_distinct {
                    CompactArc::new(columns[..expected_columns].to_vec())
                } else {
                    CompactArc::clone(&columns)
                };
                let mut result: Box<dyn QueryResult> = Box::new(ExecutorResult::with_arc_columns(
                    CompactArc::clone(&output_columns),
                    result_rows,
                ));

                // Apply DISTINCT ON if active
                if has_distinct_on {
                    result = pipeline_distinct::apply_on(result, &stmt.distinct_on, &stmt.columns)?;

                    // Remove extra columns after DISTINCT ON dedup
                    if needs_extra_col_removal {
                        result = Box::new(ProjectedResult::new(result, expected_columns));
                    }

                    // Apply LIMIT/OFFSET after DISTINCT ON
                    result = page.apply(result, false);
                } else if distinct_after_order {
                    result = pipeline_distinct::apply(result, Some(expected_columns));
                    if needs_extra_col_removal {
                        result = Box::new(ProjectedResult::new(result, expected_columns));
                    }
                    result = page.apply(result, false);
                }

                return Ok(result);
            }

            // Pre-compute column indices to avoid string comparisons during sort
            // OPTIMIZATION: Use eq_ignore_ascii_case to avoid allocations
            // Tuple: (col_idx, ascending, nulls_first)
            let order_specs: Vec<(Option<usize>, bool, Option<bool>)> = stmt
                .order_by
                .iter()
                .map(|ob| (try_map_to_column(ob), ob.ascending, ob.nulls_first))
                .collect();
            // TOP-N OPTIMIZATION: Use bounded heap when LIMIT is present
            // This is O(n log k) instead of O(n log n), where k = limit
            // Skip when DISTINCT ON is active: DISTINCT ON must happen between ORDER BY and LIMIT
            if let Some(lim) = limit {
                if stmt.distinct_on.is_empty() && !distinct_after_order {
                    // Use TopNResult for ORDER BY + LIMIT (5-50x faster for large datasets)
                    result = Box::new(TopNResult::new_with_context(
                        result,
                        move |a, b| pipeline_ordering::compare_rows(a, b, &order_specs),
                        lim,
                        offset,
                        ctx,
                    )?);

                    // Apply deferred projection if applicable
                    // This reduces allocations from O(matched_rows) to O(limit)
                    if let Some((col_indices, output_names)) = deferred_projection {
                        result = Box::new(StreamingProjectionResult::new(
                            result,
                            col_indices,
                            output_names,
                        ));
                    } else if columns.len() > expected_columns && expected_columns > 0 {
                        // Remove extra ORDER BY columns if needed (no deferred projection)
                        result = Box::new(ProjectedResult::new(result, expected_columns));
                    }

                    // LIMIT/OFFSET already applied by TopNResult
                    return Ok(result);
                }
                // DISTINCT ON with LIMIT: fall through to full sort
                // (DISTINCT ON is applied after ORDER BY, LIMIT after DISTINCT ON)
            }
            {
                // No LIMIT - use full sort
                // OPTIMIZATION: Try radix sort for integer columns (O(n) vs O(n log n))
                // Build RadixOrderSpec only if all columns have valid indices
                let radix_specs: Vec<RadixOrderSpec> = order_specs
                    .iter()
                    .filter_map(|(col_idx, ascending, nulls_first)| {
                        col_idx.map(|idx| RadixOrderSpec {
                            col_idx: idx,
                            ascending: *ascending,
                            nulls_first: *nulls_first,
                        })
                    })
                    .collect();

                // Use radix sort if all columns have valid indices
                if radix_specs.len() == order_specs.len() {
                    // All columns have valid indices - try radix sort
                    result = Box::new(OrderedResult::new_radix(
                        result,
                        &radix_specs,
                        move |a, b| pipeline_ordering::compare_rows(a, b, &order_specs),
                    )?);
                } else {
                    // Some columns missing - use comparison sort
                    result = Box::new(OrderedResult::new(result, move |a, b| {
                        pipeline_ordering::compare_rows(a, b, &order_specs)
                    })?);
                }
            }
        }

        if distinct_after_order {
            result = pipeline_distinct::apply(result, Some(expected_columns));
        }

        // Apply DISTINCT ON before removing extra columns, so keys can reference
        // columns not in SELECT (they may be among the extra ORDER BY columns or
        // the full source columns).
        if !stmt.distinct_on.is_empty() {
            result = pipeline_distinct::apply_on(result, &stmt.distinct_on, &stmt.columns)?;
        }

        // Remove extra ORDER BY columns that were added for sorting
        // This happens when ORDER BY references columns not in SELECT
        let shape = RowShape::new(CompactArc::clone(&columns), expected_columns)?;
        result = shape.project_public(result);

        // Apply LIMIT/OFFSET (only if not already applied by TopNResult or storage-level pushdown)
        result = page.apply(result, limit_offset_applied);

        Ok(result)
    }

    /// Count the number of columns in the SELECT clause
    fn count_select_columns(&self, stmt: &SelectStatement) -> usize {
        // Check for SELECT * or SELECT t.* anywhere in the select list
        // If there's any Star or QualifiedStar, we can't determine the exact count
        // without knowing the table columns, so return 0 to disable projection truncation
        for col in &stmt.columns {
            if matches!(col, Expression::Star(_) | Expression::QualifiedStar(_)) {
                return 0; // Don't project - star expansion makes count unknown
            }
        }
        stmt.columns.len()
    }

    /// Execute the core SELECT logic
    /// Returns (result, columns, limit_offset_applied)
    /// The third value indicates if LIMIT/OFFSET was already applied at the storage level
    pub(crate) fn execute_select_internal(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> SelectResult {
        // Get table source
        let table_expr = match &stmt.table_expr {
            Some(expr) => expr.as_ref(),
            None => {
                // SELECT without FROM (e.g., SELECT 1+1)
                return self.execute_expression_select(stmt, ctx, classification);
            }
        };

        // Execute based on table source type
        match table_expr {
            Expression::TableSource(table_source) => {
                // Check if this is a CTE from context (for subqueries referencing outer CTEs)
                let table_name = &table_source.name.value_lower;
                if let Some((columns, rows, _)) = ctx.get_cte_by_lower(table_name) {
                    // Execute query against CTE data
                    // Dereference Arc to get &Vec<(i64, Row)>, then wrap in RowVec
                    return self.execute_query_on_memory_result(
                        stmt,
                        ctx,
                        columns.to_vec(),
                        RowVec::from_vec((**rows).clone()),
                    );
                }

                // Check if this is actually a view (single lookup, no double RwLock acquisition)
                if let Some(view_def) = self.visible_view_lowercase(table_name)? {
                    return self.execute_view_query(&view_def, stmt, ctx, classification);
                }
                self.execute_simple_table_scan(table_source, stmt, ctx, classification)
            }
            Expression::JoinSource(join_source) => {
                self.execute_join_source(join_source, stmt, ctx, classification)
            }
            Expression::SubquerySource(subquery_source) => {
                self.execute_subquery_source(subquery_source, stmt, ctx, classification)
            }
            Expression::ValuesSource(values_source) => {
                self.execute_values_source(values_source, stmt, ctx, classification)
            }
            Expression::FunctionTableSource(tvf_source) => {
                self.execute_tvf_source(tvf_source, stmt, ctx, classification)
            }
            _ => Err(Error::NotSupported(
                "Unsupported FROM clause type".to_string(),
            )),
        }
    }

    /// Evaluate a TVF source: look up function, evaluate args, generate rows, determine columns.
    /// Execute a TVF for use in a join context: generate rows, build qualified columns,
    /// and apply an optional filter. Shared by execute_table_expression_with_filter
    /// and execute_table_expression_with_filter_limit.
    fn execute_tvf_for_join(
        tvf_source: &FunctionTableSource,
        ctx: &ExecutionContext,
        filter: Option<&Expression>,
        limit: Option<usize>,
    ) -> Result<(Box<dyn QueryResult>, Vec<String>)> {
        // Only push limit to TVF generation when no filter will discard rows afterwards.
        // With a filter, we must generate all rows first, then filter, then let
        // downstream processing handle the limit.
        let tvf_limit = if filter.is_some() { None } else { limit };

        // Extract range bounds from the filter to narrow TVF generation range.
        // This avoids materializing millions of rows when a selective predicate exists.
        let range_hint = filter.and_then(|filter_expr| {
            let col_name: SmartString = if !tvf_source.column_aliases.is_empty() {
                tvf_source.column_aliases[0].value_lower.clone()
            } else {
                SmartString::from("value")
            };
            let mut min_bound: Option<i64> = None;
            let mut max_bound: Option<i64> = None;
            Self::collect_range_bounds(filter_expr, &col_name, ctx, &mut min_bound, &mut max_bound);
            if min_bound.is_some() || max_bound.is_some() {
                Some((min_bound, max_bound))
            } else {
                None
            }
        });

        let (result_rows, column_names) =
            Self::evaluate_tvf_with_range(tvf_source, ctx, tvf_limit, range_hint)?;

        let table_alias = tvf_source
            .alias
            .as_ref()
            .map(|a| a.value.to_string())
            .unwrap_or_else(|| tvf_source.function.value.to_string());

        let qualified_columns: Vec<String> = column_names
            .iter()
            .map(|col| format!("{}.{}", table_alias, col))
            .collect();

        let mut result: Box<dyn QueryResult> =
            Box::new(StreamingRowsResult::new(column_names.clone(), result_rows));
        if let Some(filter_expr) = filter {
            let row_filter = RowFilter::new(filter_expr, &qualified_columns)?.with_context(ctx);
            result = Box::new(FilteredResult::from_filter(result, row_filter));
        }

        Ok((result, qualified_columns))
    }

    /// Extract a simple LIMIT value from a SelectStatement, if present and evaluable.
    fn extract_limit_hint(stmt: &SelectStatement, ctx: &ExecutionContext) -> Option<usize> {
        let limit_expr = stmt.limit.as_ref()?;
        let offset = stmt.offset.as_ref().and_then(|off| {
            ExpressionEval::compile(off, &[])
                .ok()?
                .with_context(ctx)
                .eval_slice(&Row::new())
                .ok()
                .and_then(|v| v.as_int64())
                .map(|v| v.max(0) as usize)
        });
        let limit_val = ExpressionEval::compile(limit_expr, &[])
            .ok()?
            .with_context(ctx)
            .eval_slice(&Row::new())
            .ok()?
            .as_int64()?;
        if limit_val < 0 {
            return None;
        }
        let total = (limit_val as usize).saturating_add(offset.unwrap_or(0));
        Some(total)
    }

    /// Execute a table-valued function source (e.g., generate_series(1, 10))
    fn execute_tvf_source(
        &self,
        tvf_source: &FunctionTableSource,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &Arc<QueryClassification>,
    ) -> SelectResult {
        // Push LIMIT down to TVF generation only when nothing can filter/reshape/reorder:
        // WHERE, GROUP BY, HAVING, ORDER BY, DISTINCT, aggregation, window functions, set operations
        let limit_hint = if stmt.where_clause.is_none()
            && stmt.group_by.columns.is_empty()
            && stmt.having.is_none()
            && stmt.order_by.is_empty()
            && !stmt.distinct
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !classification.has_set_operations
        {
            Self::extract_limit_hint(stmt, ctx)
        } else {
            None
        };

        // Short-circuit: if WHERE is a constant-false expression (e.g., 1=0),
        // skip TVF generation entirely and return empty result.
        if let Some(ref where_clause) = stmt.where_clause {
            if let Ok(eval) = ExpressionEval::compile(where_clause, &[]) {
                if let Ok(val) = eval.with_context(ctx).eval_slice(&Row::new()) {
                    if val == Value::Boolean(false) {
                        let columns = if !tvf_source.column_aliases.is_empty() {
                            tvf_source
                                .column_aliases
                                .iter()
                                .map(|id| id.value.to_string())
                                .collect()
                        } else {
                            vec!["value".to_string()]
                        };
                        return self.execute_query_on_memory_result(
                            stmt,
                            ctx,
                            columns,
                            RowVec::new(),
                        );
                    }
                }
            }
        }

        // Extract range bounds from WHERE clause to narrow TVF generation range.
        // This avoids materializing millions of rows when a selective predicate exists.
        let range_hint = Self::extract_tvf_range_hint(tvf_source, stmt, ctx);

        let (result_rows, columns) =
            Self::evaluate_tvf_with_range(tvf_source, ctx, limit_hint, range_hint)?;

        // Identity TVF scans now reach the client incrementally. More complex
        // relational shapes still use the established in-memory pipeline, but
        // their retained source is guarded by the common blocking budget.
        let identity_projection = stmt.columns.len() == 1
            && match &stmt.columns[0] {
                Expression::Star(_) => true,
                Expression::Identifier(identifier) => columns
                    .first()
                    .is_some_and(|column| column.eq_ignore_ascii_case(&identifier.value_lower)),
                Expression::QualifiedIdentifier(identifier) => {
                    columns.first().is_some_and(|column| {
                        column.eq_ignore_ascii_case(&identifier.name.value_lower)
                    })
                }
                _ => false,
            }
            && stmt.where_clause.is_none()
            && stmt.group_by.columns.is_empty()
            && stmt.having.is_none()
            && stmt.order_by.is_empty()
            && !stmt.distinct
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !classification.has_set_operations;
        if identity_projection {
            let columns = CompactArc::new(columns);
            return Ok((
                Box::new(StreamingRowsResult::new(columns.to_vec(), result_rows)),
                columns,
                false,
                None,
            ));
        }

        use super::utils::RetainedRowsBudget;
        let mut retained = RetainedRowsBudget::new("TVF relational pipeline");
        let mut materialized = RowVec::new();
        for (index, item) in result_rows.enumerate() {
            if index & 0xff == 0 {
                ctx.check_cancelled()?;
            }
            let (_, row) = item?;
            retained.admit(&row)?;
            materialized.push((index as i64, row));
        }

        // Delegate to execute_query_on_memory_result which handles
        // WHERE, ORDER BY, LIMIT, GROUP BY, aggregation, window functions
        self.execute_query_on_memory_result(stmt, ctx, columns, materialized)
    }

    /// Extract integer range bounds from the WHERE clause for a TVF.
    /// Returns (min_bound, max_bound) where each is inclusive.
    /// Only handles simple comparisons on the TVF's output column with integer literals.
    fn extract_tvf_range_hint(
        tvf_source: &FunctionTableSource,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
    ) -> Option<(Option<i64>, Option<i64>)> {
        let where_clause = stmt.where_clause.as_ref()?;

        // Determine the TVF value column name (from alias or default "value")
        let col_name: SmartString = if !tvf_source.column_aliases.is_empty() {
            tvf_source.column_aliases[0].value_lower.clone()
        } else {
            SmartString::from("value")
        };

        let mut min_bound: Option<i64> = None;
        let mut max_bound: Option<i64> = None;

        Self::collect_range_bounds(where_clause, &col_name, ctx, &mut min_bound, &mut max_bound);

        if min_bound.is_some() || max_bound.is_some() {
            Some((min_bound, max_bound))
        } else {
            None
        }
    }

    /// Recursively collect range bounds from AND-conjuncted comparison predicates.
    fn collect_range_bounds(
        expr: &Expression,
        col_name: &str,
        ctx: &ExecutionContext,
        min_bound: &mut Option<i64>,
        max_bound: &mut Option<i64>,
    ) {
        match expr {
            Expression::Infix(infix) if infix.operator == "AND" => {
                Self::collect_range_bounds(&infix.left, col_name, ctx, min_bound, max_bound);
                Self::collect_range_bounds(&infix.right, col_name, ctx, min_bound, max_bound);
            }
            Expression::Infix(infix) => {
                // Try col OP literal and literal OP col
                let (is_col_left, literal_val) = if Self::is_tvf_column(&infix.left, col_name) {
                    (true, Self::try_eval_to_i64(&infix.right, ctx))
                } else if Self::is_tvf_column(&infix.right, col_name) {
                    (false, Self::try_eval_to_i64(&infix.left, ctx))
                } else {
                    return;
                };

                let val = match literal_val {
                    Some(v) => v,
                    None => return,
                };

                // Normalize to col OP val form
                let op = if is_col_left {
                    infix.operator.as_str()
                } else {
                    // Flip: literal OP col → col FLIPPED_OP literal
                    match infix.operator.as_str() {
                        ">" => "<",
                        ">=" => "<=",
                        "<" => ">",
                        "<=" => ">=",
                        "=" => "=",
                        _ => return,
                    }
                };

                match op {
                    ">=" => {
                        *min_bound = Some(min_bound.map_or(val, |cur| cur.max(val)));
                    }
                    ">" => {
                        let bound = val.saturating_add(1);
                        *min_bound = Some(min_bound.map_or(bound, |cur| cur.max(bound)));
                    }
                    "<=" => {
                        *max_bound = Some(max_bound.map_or(val, |cur| cur.min(val)));
                    }
                    "<" => {
                        let bound = val.saturating_sub(1);
                        *max_bound = Some(max_bound.map_or(bound, |cur| cur.min(bound)));
                    }
                    "=" => {
                        *min_bound = Some(min_bound.map_or(val, |cur| cur.max(val)));
                        *max_bound = Some(max_bound.map_or(val, |cur| cur.min(val)));
                    }
                    _ => {}
                }
            }
            Expression::Between(between)
                if !between.not && Self::is_tvf_column(&between.expr, col_name) =>
            {
                if let Some(lo) = Self::try_eval_to_i64(&between.lower, ctx) {
                    *min_bound = Some(min_bound.map_or(lo, |cur| cur.max(lo)));
                }
                if let Some(hi) = Self::try_eval_to_i64(&between.upper, ctx) {
                    *max_bound = Some(max_bound.map_or(hi, |cur| cur.min(hi)));
                }
            }
            _ => {}
        }
    }

    /// Check if an expression refers to the TVF's output column.
    fn is_tvf_column(expr: &Expression, col_name: &str) -> bool {
        match expr {
            Expression::Identifier(id) => id.value_lower.eq_ignore_ascii_case(col_name),
            Expression::QualifiedIdentifier(qi) => {
                qi.name.value_lower.eq_ignore_ascii_case(col_name)
            }
            _ => false,
        }
    }

    /// Try to extract an exact i64 constant from an expression (for TVF range pushdown).
    /// Only accepts syntactically visible constants (integer literals, negated integer literals,
    /// simple arithmetic on integer literals). Never evaluates function calls or other
    /// potentially volatile expressions — those would be evaluated once here but per-row
    /// in the real WHERE filter, changing semantics.
    fn try_eval_to_i64(expr: &Expression, _ctx: &ExecutionContext) -> Option<i64> {
        match expr {
            Expression::IntegerLiteral(lit) => Some(lit.value),
            Expression::Prefix(p) if p.operator == "-" => {
                if let Expression::IntegerLiteral(lit) = &*p.right {
                    lit.value.checked_neg()
                } else {
                    None
                }
            }
            // Simple constant arithmetic: 2+3, 10-1, 2*5
            Expression::Infix(inf) => {
                let l = Self::try_eval_to_i64(&inf.left, _ctx)?;
                let r = Self::try_eval_to_i64(&inf.right, _ctx)?;
                match inf.operator.as_str() {
                    "+" => l.checked_add(r),
                    "-" => l.checked_sub(r),
                    "*" => l.checked_mul(r),
                    "/" if r != 0 => Some(l / r),
                    _ => None,
                }
            }
            // Everything else (floats, function calls, casts, subqueries, etc.)
            // is either non-integer or potentially volatile — reject.
            _ => None,
        }
    }

    /// Evaluate a TVF with optional range clamping on start/stop args.
    fn evaluate_tvf_with_range(
        tvf_source: &FunctionTableSource,
        ctx: &ExecutionContext,
        limit: Option<usize>,
        range_hint: Option<(Option<i64>, Option<i64>)>,
    ) -> Result<(radixdb_functions::tvf::TableRowStream, Vec<String>)> {
        use radixdb_functions::global_registry;

        let func_name = &tvf_source.function.value;
        let tvf = global_registry().get_tvf(func_name).ok_or_else(|| {
            Error::NotSupported(format!("Unknown table-valued function: {}", func_name))
        })?;

        let declared_columns = tvf.column_names();
        if !tvf_source.column_aliases.is_empty()
            && tvf_source.column_aliases.len() != declared_columns.len()
        {
            return Err(Error::InvalidArgument(format!(
                "table-valued function {} returns {} columns but alias declares {}",
                func_name,
                declared_columns.len(),
                tvf_source.column_aliases.len()
            )));
        }

        // Evaluate arguments to Values
        let mut arg_values = Vec::with_capacity(tvf_source.arguments.len());
        for arg in &tvf_source.arguments {
            let value = ExpressionEval::compile(arg, &[])?
                .with_context(ctx)
                .eval_slice(&Row::new())?;
            arg_values.push(value);
        }

        // Apply range clamping for integer series: narrow start/stop based on WHERE bounds.
        // Only safe when: 2-3 integer args, step is 1 or -1 (or defaulted).
        if let Some((min_bound, max_bound)) = range_hint {
            if arg_values.len() >= 2 {
                let all_integer = arg_values.iter().all(|v| matches!(v, Value::Integer(_)));
                if all_integer {
                    let start = arg_values[0].as_int64().unwrap();
                    let stop = arg_values[1].as_int64().unwrap();
                    let step = if arg_values.len() == 3 {
                        arg_values[2].as_int64().unwrap()
                    } else if start <= stop {
                        1
                    } else {
                        -1
                    };

                    // Preserve the series' original direction. If range
                    // narrowing makes the interval empty, GENERATE_SERIES must
                    // return no rows rather than re-detecting the opposite
                    // direction from the clamped endpoints.
                    if arg_values.len() == 2 {
                        arg_values.push(Value::Integer(step));
                    }

                    // Only clamp for unit step (1 or -1) where bounds align exactly
                    if step == 1 {
                        if let Some(lo) = min_bound {
                            if lo > start {
                                arg_values[0] = Value::Integer(lo);
                            }
                        }
                        if let Some(hi) = max_bound {
                            if hi < stop {
                                arg_values[1] = Value::Integer(hi);
                            }
                        }
                    } else if step == -1 {
                        // Descending: start is the high end, stop is the low end
                        if let Some(hi) = max_bound {
                            if hi < start {
                                arg_values[0] = Value::Integer(hi);
                            }
                        }
                        if let Some(lo) = min_bound {
                            if lo > stop {
                                arg_values[1] = Value::Integer(lo);
                            }
                        }
                    }
                }
            }
        }

        let result_rows = tvf.stream(&arg_values, limit)?;

        let columns: Vec<String> = if !tvf_source.column_aliases.is_empty() {
            tvf_source
                .column_aliases
                .iter()
                .map(|id| id.value.to_string())
                .collect()
        } else {
            declared_columns
        };

        Ok((result_rows, columns))
    }

    /// Execute SELECT without FROM (expressions only)
    fn execute_expression_select(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> SelectResult {
        // SELECT without FROM has one logical zero-column input row. Reuse the
        // ordinary in-memory pipeline so WHERE, aggregate/HAVING, windows,
        // projection, ORDER and paging have one set of semantics.
        let mut rows = RowVec::with_capacity(1);
        rows.push((0, Row::new()));

        // Without grouping, HAVING filters the single logical group. Route it
        // through the same boolean admission as WHERE instead of ignoring it.
        let mut rewritten = None;
        if classification.has_having && !classification.has_aggregation {
            let mut effective = stmt.clone();
            if let Some(having) = effective.having.take() {
                effective.where_clause = Some(match effective.where_clause.take() {
                    Some(where_clause) => Box::new(Expression::Infix(InfixExpression::new(
                        Token::new(TokenType::Keyword, "AND", Position::default()),
                        where_clause,
                        "AND".to_string(),
                        having,
                    ))),
                    None => having,
                });
            }
            rewritten = Some(effective);
        }

        self.execute_query_on_memory_result(
            rewritten.as_ref().unwrap_or(stmt),
            ctx,
            Vec::new(),
            rows,
        )
    }

    /// Execute a query against in-memory data (for CTEs referenced from context)
    fn execute_query_on_memory_result(
        &self,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        columns: Vec<String>,
        rows: RowVec,
    ) -> SelectResult {
        // Check if ORDER BY has complex expressions (e.g., -value, SUM(x), val*2)
        // that the CTE path's simple column-name-based sort can't handle.
        // If so, skip ORDER BY/LIMIT in the CTE path and let outer execute_select handle it.
        let has_complex_order_by = stmt.order_by.iter().any(|ob| {
            !matches!(
                &ob.expression,
                Expression::Identifier(_)
                    | Expression::QualifiedIdentifier(_)
                    | Expression::IntegerLiteral(_)
            )
        });

        // Also skip when DISTINCT is present: the outer execute_select applies
        // DISTINCT after projection but before LIMIT. If we apply LIMIT here first,
        // DISTINCT sees too few rows and produces wrong results.
        let skip_order_limit = has_complex_order_by || stmt.distinct;

        let (result_cols, result_rows, order_limit_applied) =
            self.execute_query_on_cte_result_inner(stmt, ctx, columns, rows, skip_order_limit)?;
        let result_cols = CompactArc::new(result_cols);
        Ok((
            Box::new(ExecutorResult::with_arc_columns(
                CompactArc::clone(&result_cols),
                result_rows,
            )),
            result_cols,
            order_limit_applied,
            None,
        ))
    }

    /// Check if ORDER BY or DISTINCT ON references columns not in SELECT
    /// OPTIMIZATION: Use HashSet for O(1) lookup and eq_ignore_ascii_case to avoid allocations
    fn order_by_needs_extra_columns(&self, stmt: &SelectStatement, all_columns: &[String]) -> bool {
        if stmt.order_by.is_empty() && stmt.distinct_on.is_empty() {
            return false;
        }

        // Check if SELECT contains * or t.* - it includes all columns, so ORDER BY is always covered
        // This handles both "SELECT *" and "SELECT *, expr" cases
        let has_star = stmt
            .columns
            .iter()
            .any(|c| matches!(c, Expression::Star(_)));
        if has_star {
            return false;
        }

        // For t.*, check if all ORDER BY columns are covered by the qualified star
        let has_qualified_star = stmt.columns.iter().any(|c| {
            if let Expression::QualifiedStar(qs) = c {
                // Check if ORDER BY columns match this qualifier
                stmt.order_by.iter().all(|ob| {
                    if let Expression::QualifiedIdentifier(qid) = &ob.expression {
                        qid.qualifier.value_lower == qs.qualifier.to_lowercase()
                    } else if let Expression::Identifier(_) = &ob.expression {
                        // Simple identifier might be covered by qualified star
                        true
                    } else {
                        false
                    }
                })
            } else {
                false
            }
        });
        if has_qualified_star {
            return false;
        }

        // Get SELECT column names (lowercase) using HashSet for O(1) lookup
        // Include both unqualified and fully qualified names for disambiguation
        let mut select_columns: FxHashSet<String> = stmt
            .columns
            .iter()
            .filter_map(|expr| self.extract_select_column_name(expr))
            .map(|s| s.to_lowercase())
            .collect();
        // Also add qualified names for join column disambiguation
        for expr in &stmt.columns {
            match expr {
                Expression::QualifiedIdentifier(qi) => {
                    select_columns.insert(format!(
                        "{}.{}",
                        qi.qualifier.value_lower, qi.name.value_lower
                    ));
                }
                Expression::Aliased(a) => {
                    if let Expression::QualifiedIdentifier(qi) = &*a.expression {
                        select_columns.insert(format!(
                            "{}.{}",
                            qi.qualifier.value_lower, qi.name.value_lower
                        ));
                    }
                }
                _ => {}
            }
        }

        // Check if any ORDER BY column is not in SELECT
        for ob in &stmt.order_by {
            match &ob.expression {
                Expression::Identifier(id)
                    if !select_columns.contains(id.value_lower.as_str())
                        && all_columns
                            .iter()
                            .any(|c| c.eq_ignore_ascii_case(id.value_lower.as_str())) =>
                {
                    return true;
                }
                Expression::QualifiedIdentifier(qi) => {
                    let full_name = format!("{}.{}", qi.qualifier.value_lower, qi.name.value_lower);
                    if !select_columns.contains(full_name.as_str())
                        && all_columns.iter().any(|c| {
                            c.eq_ignore_ascii_case(&full_name)
                                || c.eq_ignore_ascii_case(qi.name.value_lower.as_str())
                        })
                    {
                        return true;
                    }
                }
                _ if !Self::expression_is_selected(&ob.expression, &stmt.columns) => return true,
                _ => {}
            }
        }

        // Check if any DISTINCT ON column is not in SELECT
        for expr in &stmt.distinct_on {
            match expr {
                Expression::Identifier(id) => {
                    if !select_columns.contains(id.value_lower.as_str())
                        && all_columns
                            .iter()
                            .any(|c| c.eq_ignore_ascii_case(id.value_lower.as_str()))
                    {
                        return true;
                    }
                }
                Expression::QualifiedIdentifier(qi) => {
                    let full_name = format!("{}.{}", qi.qualifier.value_lower, qi.name.value_lower);
                    // For qualified identifiers, only match the fully qualified name
                    // to avoid ambiguity when both tables have the same column name
                    if !select_columns.contains(full_name.as_str())
                        && all_columns.iter().any(|c| {
                            c.eq_ignore_ascii_case(&full_name)
                                || c.eq_ignore_ascii_case(qi.name.value_lower.as_str())
                        })
                    {
                        return true;
                    }
                }
                _ => {
                    // Computed expression (e.g., amount + 0) — always needs extra column
                    return true;
                }
            }
        }

        false
    }

    fn expression_is_selected(expr: &Expression, select_exprs: &[Expression]) -> bool {
        let expr_text = expr.to_string();
        select_exprs.iter().any(|select_expr| match select_expr {
            Expression::Aliased(aliased) => {
                aliased.alias.value.eq_ignore_ascii_case(&expr_text)
                    || aliased.expression.to_string() == expr_text
            }
            other => other.to_string() == expr_text,
        })
    }

    /// Extract column name from expression for ORDER BY handling
    #[allow(clippy::only_used_in_recursion)]
    fn extract_select_column_name(&self, expr: &Expression) -> Option<String> {
        match expr {
            Expression::Identifier(id) => Some(id.value.to_string()),
            Expression::QualifiedIdentifier(qid) => Some(qid.name.value.to_string()),
            Expression::Aliased(aliased) => self.extract_select_column_name(&aliased.expression),
            Expression::Star(_) | Expression::QualifiedStar(_) => None, // SELECT * or t.* includes all columns
            _ => None,
        }
    }

    /// Check if deferred projection optimization is applicable.
    ///
    /// For ORDER BY + LIMIT queries where SELECT columns are simple column references,
    /// we can defer projection until after sorting and limiting. This reduces allocations
    /// from O(matched_rows) to O(limit).
    ///
    /// Returns (column_indices, output_column_names) if optimization applies, None otherwise.
    fn get_deferred_projection_info(
        &self,
        stmt: &SelectStatement,
        source_columns_lower: &[String],
        source_columns: &[String],
        classification: &std::sync::Arc<QueryClassification>,
    ) -> Option<(Vec<usize>, Vec<String>)> {
        // Must have ORDER BY + LIMIT
        if stmt.order_by.is_empty() || stmt.limit.is_none() {
            return None;
        }

        // No aggregation or window functions (these are handled separately)
        if classification.has_aggregation || classification.has_window_functions {
            return None;
        }

        // No DISTINCT ON — deferred projection would bypass the DISTINCT ON step
        if classification.has_distinct_on {
            return None;
        }

        // ORDER BY columns must exist in source columns (so sorting can happen before projection)
        for ob in &stmt.order_by {
            let col_exists = match &ob.expression {
                Expression::Identifier(id) => source_columns_lower
                    .iter()
                    .any(|c| c == id.value_lower.as_str()),
                Expression::QualifiedIdentifier(qid) => source_columns_lower
                    .iter()
                    .any(|c| c == qid.name.value_lower.as_str()),
                _ => false, // Complex expression - can't evaluate on source columns
            };
            if !col_exists {
                return None;
            }
        }

        // Calculate projection indices - all SELECT columns must be simple column references
        let mut indices = Vec::with_capacity(stmt.columns.len());
        let mut output_names = Vec::with_capacity(stmt.columns.len());

        for expr in &stmt.columns {
            match expr {
                Expression::Identifier(id) => {
                    let idx = source_columns_lower
                        .iter()
                        .position(|c| c == id.value_lower.as_str())?;
                    indices.push(idx);
                    output_names.push(source_columns[idx].clone());
                }
                Expression::QualifiedIdentifier(qid) => {
                    let idx = source_columns_lower
                        .iter()
                        .position(|c| c == qid.name.value_lower.as_str())?;
                    indices.push(idx);
                    output_names.push(source_columns[idx].clone());
                }
                Expression::Aliased(aliased) => match aliased.expression.as_ref() {
                    Expression::Identifier(id) => {
                        let idx = source_columns_lower
                            .iter()
                            .position(|c| c == id.value_lower.as_str())?;
                        indices.push(idx);
                        output_names.push(aliased.alias.value.to_string());
                    }
                    Expression::QualifiedIdentifier(qid) => {
                        let idx = source_columns_lower
                            .iter()
                            .position(|c| c == qid.name.value_lower.as_str())?;
                        indices.push(idx);
                        output_names.push(aliased.alias.value.to_string());
                    }
                    _ => return None, // Complex expression
                },
                Expression::Star(_) | Expression::QualifiedStar(_) => {
                    return None; // SELECT * doesn't benefit
                }
                _ => return None, // Function call, arithmetic, etc.
            }
        }

        Some((indices, output_names))
    }

    /// Build the final streaming projection immediately above a JOIN cursor.
    ///
    /// SELECT expressions come first. ORDER BY / DISTINCT ON expressions that
    /// are not already selected are appended as private columns and removed by
    /// the existing outer wrappers after they have consumed them. This keeps a
    /// live JOIN graph connected to post-JOIN processing without forcing a
    /// RowVec boundary merely to evaluate CASE/scalar expressions.
    fn streaming_join_post_projection(
        &self,
        stmt: &SelectStatement,
        source_columns: &[String],
        classification: &Arc<QueryClassification>,
    ) -> Option<(Vec<Expression>, Vec<String>)> {
        if classification.has_group_by
            || classification.has_aggregation
            || classification.has_window_functions
            || classification.select_has_scalar_subqueries
            || classification.select_has_correlated_subqueries
            || classification.order_by_has_correlated_subqueries
            || stmt.columns.iter().any(|expression| {
                matches!(
                    expression,
                    Expression::Star(_) | Expression::QualifiedStar(_)
                )
            })
        {
            return None;
        }

        let mut expressions = stmt.columns.clone();
        let mut output_columns = self.get_output_column_names(&stmt.columns, source_columns, None);
        for expression in stmt
            .order_by
            .iter()
            .map(|order| &order.expression)
            .chain(stmt.distinct_on.iter())
        {
            if !Self::expression_is_selected(expression, &stmt.columns)
                && !expressions
                    .iter()
                    .any(|existing| existing.to_string() == expression.to_string())
            {
                expressions.push(expression.clone());
                output_columns.push(expression_binding_name(expression));
            }
        }
        Some((expressions, output_columns))
    }

    fn join_result_proves_statement_order(
        stmt: &SelectStatement,
        result: &dyn QueryResult,
        result_columns: &[String],
    ) -> bool {
        let Some(certified) = result.ascending_nulls_last_ordering() else {
            return false;
        };
        if stmt.order_by.is_empty() {
            return false;
        }
        let mut required = Vec::with_capacity(stmt.order_by.len());
        for order in &stmt.order_by {
            if !order.ascending || order.nulls_first == Some(true) {
                return false;
            }
            let expression_text = order.expression.to_string();
            let selected = stmt.columns.iter().position(|selected| match selected {
                Expression::Aliased(aliased) => {
                    aliased.expression.to_string() == expression_text
                        || aliased.alias.value.eq_ignore_ascii_case(&expression_text)
                }
                other => other.to_string() == expression_text,
            });
            let index = selected.or_else(|| match &order.expression {
                Expression::QualifiedIdentifier(identifier) => {
                    let qualified = identifier.to_string();
                    result_columns
                        .iter()
                        .position(|column| column.eq_ignore_ascii_case(&qualified))
                }
                Expression::Identifier(identifier) => {
                    let mut matches = result_columns.iter().enumerate().filter(|(_, column)| {
                        column.eq_ignore_ascii_case(identifier.value.as_str())
                            || column.rsplit_once('.').is_some_and(|(_, base)| {
                                base.eq_ignore_ascii_case(identifier.value.as_str())
                            })
                    });
                    let (index, _) = matches.next()?;
                    matches.next().is_none().then_some(index)
                }
                _ => None,
            });
            let Some(index) = index else {
                return false;
            };
            required.push(index);
        }
        certified.starts_with(&required)
    }

    /// Try to get distinct values directly from an index
    ///
    /// This optimization works for queries like:
    /// - SELECT DISTINCT col FROM table (where col is indexed)
    ///
    /// Conditions:
    /// - Single column in SELECT (not *, not expression)
    /// - No WHERE clause
    /// - No GROUP BY, HAVING
    /// - No ORDER BY (could be extended later)
    /// - No LIMIT/OFFSET (could be extended later)
    /// - The column must have an index
    fn try_distinct_pushdown(
        &self,
        table: &dyn radixdb_storage::traits::Table,
        stmt: &SelectStatement,
        all_columns: &[String],
        classification: &std::sync::Arc<QueryClassification>,
    ) -> Result<Option<Box<dyn radixdb_storage::traits::QueryResult>>> {
        // Must be DISTINCT
        if !stmt.distinct {
            return Ok(None);
        }

        // DISTINCT ON uses key-based dedup, not full-row dedup — cannot use index pushdown
        if !stmt.distinct_on.is_empty() {
            return Ok(None);
        }

        // Quick eligibility checks using cached classification
        if classification.has_where {
            return Ok(None);
        }
        if classification.has_group_by {
            return Ok(None);
        }
        if classification.has_having {
            return Ok(None);
        }
        if classification.has_aggregation {
            return Ok(None);
        }
        if classification.has_window_functions {
            return Ok(None);
        }
        // ORDER BY is OK - we can sort the distinct values after

        // Must have exactly one column
        if stmt.columns.len() != 1 {
            return Ok(None);
        }

        // Get the column name (must be a simple identifier, not an expression)
        let column_name = match &stmt.columns[0] {
            Expression::Identifier(id) => id.value_lower.to_string(),
            Expression::QualifiedIdentifier(qid) => qid.name.value_lower.to_string(),
            Expression::Aliased(aliased) => match &*aliased.expression {
                Expression::Identifier(id) => id.value_lower.to_string(),
                Expression::QualifiedIdentifier(qid) => qid.name.value_lower.to_string(),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        // Verify this column exists in the table
        let column_exists = all_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&column_name));
        if !column_exists {
            return Ok(None);
        }

        // Try to get distinct values from the index
        let distinct_values = table.get_partition_values(&column_name).or_else(|| {
            // Fallback: dictionary-based extraction from cold volumes
            let schema = table.schema();
            let col_idx = *schema.column_index_map().get(&column_name)?;
            table.compute_distinct_values(col_idx)
        });

        if let Some(distinct_values) = distinct_values {
            // Build output column name (use alias if present)
            let output_name = match &stmt.columns[0] {
                Expression::Aliased(aliased) => aliased.alias.value.to_string(),
                Expression::Identifier(id) => id.value.to_string(),
                Expression::QualifiedIdentifier(qid) => qid.name.value.to_string(),
                _ => column_name,
            };

            // Convert values to rows
            let rows: RowVec = distinct_values
                .into_iter()
                .enumerate()
                .map(|(i, v)| (i as i64, Row::from_values(vec![v])))
                .collect();

            let result = ExecutorResult::new(vec![output_name], rows);
            return Ok(Some(Box::new(result)));
        }

        Ok(None)
    }
}
