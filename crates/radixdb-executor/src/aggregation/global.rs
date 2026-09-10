use super::*;

impl<'host, H: AggregationHost + ?Sized> AggregationExecutor<'host, H> {
    /// Execute global aggregation (no GROUP BY)
    ///
    /// Optimized with parallel processing for large datasets:
    /// - Partitions data into chunks
    /// - Processes chunks in parallel
    /// - Merges partial results
    pub(super) fn execute_global_aggregation(
        &self,
        aggregations: &[SqlAggregateFunction],
        rows: &[(i64, Row)],
        columns: &[String],
        col_index_map: &StringMap<usize>,
        ctx: &ExecutionContext,
    ) -> Result<(Vec<String>, RowVec)> {
        // Check if any aggregation has an expression (e.g., SUM(val * 2)), ORDER BY, or FILTER
        let has_expression = aggregations
            .iter()
            .any(|a| a.expression.is_some() || !a.order_by.is_empty() || a.filter.is_some());

        // FAST PATH: Single COUNT(*) - just return row count directly
        // Only use fast path when no expressions/filters are involved
        if !has_expression
            && aggregations.len() == 1
            && aggregations[0].name == "COUNT"
            && aggregations[0].column == "*"
            && !aggregations[0].distinct
            && aggregations[0].filter.is_none()
        {
            let result_columns: Vec<String> =
                aggregations.iter().map(|a| a.get_column_name()).collect();
            let mut result_rows = RowVec::with_capacity(1);
            result_rows.push((0, Row::from_values(vec![Value::Integer(rows.len() as i64)])));
            return Ok((result_columns, result_rows));
        }

        // Pre-compute column indices for faster access
        // OPTIMIZATION: Use pre-computed column_lower instead of calling to_lowercase() each time
        // Handle both qualified (e.g., "o.amount") and unqualified column names
        let agg_col_indices: Vec<Option<usize>> = aggregations
            .iter()
            .map(|agg| {
                if agg.column == "*" || agg.expression.is_some() {
                    None // Don't use column index for expressions
                } else {
                    Self::lookup_column_index(&agg.column_lower, col_index_map)
                }
            })
            .collect();

        // FAST PATH: Single SUM on integer column without DISTINCT (no expressions)
        if !has_expression
            && aggregations.len() == 1
            && aggregations[0].name == "SUM"
            && !aggregations[0].distinct
        {
            if let Some(col_idx) = agg_col_indices[0] {
                let result = self.fast_sum_column(rows, col_idx);
                let result_columns: Vec<String> =
                    aggregations.iter().map(|a| a.get_column_name()).collect();
                let mut result_rows = RowVec::with_capacity(1);
                result_rows.push((0, Row::from_values(vec![result])));
                return Ok((result_columns, result_rows));
            }
        }

        // FAST PATH: Single AVG on column without DISTINCT (no expressions)
        if !has_expression
            && aggregations.len() == 1
            && aggregations[0].name == "AVG"
            && !aggregations[0].distinct
        {
            if let Some(col_idx) = agg_col_indices[0] {
                let result = self.fast_avg_column(rows, col_idx);
                let result_columns: Vec<String> =
                    aggregations.iter().map(|a| a.get_column_name()).collect();
                let mut result_rows = RowVec::with_capacity(1);
                result_rows.push((0, Row::from_values(vec![result])));
                return Ok((result_columns, result_rows));
            }
        }

        // Check if any aggregation uses DISTINCT (can't parallelize easily)
        #[cfg(feature = "parallel")]
        let has_distinct = aggregations.iter().any(|a| a.distinct);

        // Pre-compile filter, expression, and ORDER BY programs for VM-based evaluation
        // CRITICAL: Propagate errors instead of silently ignoring compilation failures
        use crate::expression::{compile_expression, ExecuteContext, ExprVM, SharedProgram};
        let compiled_filters: Vec<Option<SharedProgram>> = if has_expression {
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
        let compiled_agg_expressions: Vec<Option<SharedProgram>> = if has_expression {
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
        // Pre-compile ORDER BY expressions for each aggregation
        // CRITICAL: Propagate errors instead of silently skipping failed compilations
        let compiled_order_by: Vec<Vec<SharedProgram>> = if has_expression {
            aggregations
                .iter()
                .map(|agg| {
                    agg.order_by
                        .iter()
                        .map(|o| compile_expression(&o.expression, columns))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![Vec::new(); aggregations.len()]
        };
        let mut expr_vm = if has_expression {
            Some(ExprVM::new())
        } else {
            None
        };

        // Use parallel processing for large datasets without DISTINCT
        #[cfg(feature = "parallel")]
        let use_parallel = rows.len() >= 100_000
            && !has_distinct
            && !has_expression
            && aggregations
                .iter()
                .all(|agg| matches!(agg.name.as_str(), "COUNT" | "SUM" | "MIN" | "MAX"));
        #[cfg(not(feature = "parallel"))]
        let use_parallel = false;

        let result_values: Vec<Value> = if use_parallel {
            // PARALLEL: Split into chunks and process in parallel
            #[cfg(feature = "parallel")]
            let chunk_size = (rows.len() / rayon::current_num_threads()).max(1000);
            #[cfg(not(feature = "parallel"))]
            let chunk_size = rows.len();
            let function_registry = &self.host.aggregation_function_registry();

            // Process chunks in parallel, each producing partial aggregates
            #[cfg(feature = "parallel")]
            let partial_results: Vec<Vec<Value>> = rows
                .par_chunks(chunk_size)
                .map(|chunk| -> Result<Vec<Value>> {
                    let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = aggregations
                        .iter()
                        .map(|agg| function_registry.get_aggregate(&agg.name))
                        .collect();

                    // Configure aggregate functions with extra arguments (e.g., separator for STRING_AGG)
                    for (i, agg) in aggregations.iter().enumerate() {
                        if !agg.extra_args.is_empty() {
                            if let Some(ref mut func) = agg_funcs[i] {
                                func.configure(&agg.extra_args);
                            }
                        }
                    }

                    // Pre-create static Value for COUNT(*)
                    let count_star_value = Value::Integer(1);
                    for (_, row) in chunk {
                        for (i, _agg) in aggregations.iter().enumerate() {
                            if let Some(ref mut func) = agg_funcs[i] {
                                // OPTIMIZATION: Avoid cloning by using reference directly
                                let value_ref = if let Some(col_idx) = agg_col_indices[i] {
                                    row.get(col_idx)
                                } else {
                                    Some(&count_star_value) // COUNT(*)
                                };
                                if let Some(v) = value_ref {
                                    func.accumulate(v, false);
                                }
                                // Skip if None (missing column) - this is effectively null
                            }
                        }
                    }

                    // Return partial results
                    let values = agg_funcs
                        .iter()
                        .map(|function| {
                            function.as_ref().map_or_else(
                                || Ok(Value::null_unknown()),
                                |function| function.try_result(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(values)
                })
                .collect::<Result<Vec<_>>>()?;
            #[cfg(not(feature = "parallel"))]
            let partial_results: Vec<Vec<Value>> = rows
                .chunks(chunk_size)
                .map(|chunk| -> Result<Vec<Value>> {
                    let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = aggregations
                        .iter()
                        .map(|agg| function_registry.get_aggregate(&agg.name))
                        .collect();

                    // Configure aggregate functions with extra arguments (e.g., separator for STRING_AGG)
                    for (i, agg) in aggregations.iter().enumerate() {
                        if !agg.extra_args.is_empty() {
                            if let Some(ref mut func) = agg_funcs[i] {
                                func.configure(&agg.extra_args);
                            }
                        }
                    }

                    // Pre-create static Value for COUNT(*)
                    let count_star_value = Value::Integer(1);
                    for (_, row) in chunk {
                        for (i, _agg) in aggregations.iter().enumerate() {
                            if let Some(ref mut func) = agg_funcs[i] {
                                // OPTIMIZATION: Avoid cloning by using reference directly
                                let value_ref = if let Some(col_idx) = agg_col_indices[i] {
                                    row.get(col_idx)
                                } else {
                                    Some(&count_star_value) // COUNT(*)
                                };
                                if let Some(v) = value_ref {
                                    func.accumulate(v, false);
                                }
                                // Skip if None (missing column) - this is effectively null
                            }
                        }
                    }

                    // Return partial results
                    let values = agg_funcs
                        .iter()
                        .map(|function| {
                            function.as_ref().map_or_else(
                                || Ok(Value::null_unknown()),
                                |function| function.try_result(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(values)
                })
                .collect::<Result<Vec<_>>>()?;

            // Merge partial results
            self.merge_partial_aggregates(aggregations, partial_results)?
        } else {
            // SEQUENTIAL: Check if we can use the fast compiled path
            // Fast path: no expressions, no filters, no order by on any aggregate
            // NOTE: This is conservative - it could potentially be extended to handle:
            // - Simple column expressions (not computed expressions)
            // - STRING_AGG with extra_args (separator) by passing to CompiledAggregate
            // For now, we keep it strict to ensure correctness.
            let can_use_compiled = aggregations.iter().all(|agg| {
                agg.expression.is_none()
                    && agg.filter.is_none()
                    && agg.order_by.is_empty()
                    && agg.extra_args.is_empty()
            });

            if can_use_compiled {
                // FAST PATH: Use CompiledAggregate for zero virtual dispatch
                let mut compiled_aggs: Vec<CompiledAggregate> = aggregations
                    .iter()
                    .map(|agg| {
                        let is_count_star = agg.name == "COUNT" && agg.column == "*";
                        CompiledAggregate::compile(
                            &agg.name,
                            is_count_star,
                            agg.distinct,
                            self.host
                                .aggregation_function_registry()
                                .get_aggregate(&agg.name),
                        )
                        .unwrap_or_else(|| {
                            // Fallback for unknown aggregates
                            CompiledAggregate::dynamic(
                                self.host
                                    .aggregation_function_registry()
                                    .get_aggregate(&agg.name)
                                    .unwrap_or_else(|| {
                                        Box::new(
                                            radixdb_functions::aggregate::CountFunction::default(),
                                        )
                                    }),
                            )
                        })
                    })
                    .collect();

                // Pre-create static Value for COUNT(*)
                let count_star_value = Value::Integer(1);

                // Hot loop with compiled aggregates - zero virtual dispatch
                for (_, row) in rows {
                    for i in 0..compiled_aggs.len() {
                        let value = if let Some(col_idx) = agg_col_indices[i] {
                            row.get(col_idx)
                        } else {
                            Some(&count_star_value) // COUNT(*)
                        };

                        if let Some(v) = value {
                            compiled_aggs[i].accumulate(v);
                        }
                    }
                }

                // Collect results
                compiled_aggs
                    .iter()
                    .map(|aggregate| aggregate.try_result())
                    .collect::<Result<Vec<_>>>()?
            } else {
                // SLOW PATH: Original algorithm with dynamic dispatch for complex aggregates
                let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = aggregations
                    .iter()
                    .map(|agg| {
                        self.host
                            .aggregation_function_registry()
                            .get_aggregate(&agg.name)
                    })
                    .collect();

                // Configure aggregate functions with extra arguments (e.g., separator for STRING_AGG)
                for (i, agg) in aggregations.iter().enumerate() {
                    if !agg.extra_args.is_empty() {
                        if let Some(ref mut func) = agg_funcs[i] {
                            func.configure(&agg.extra_args);
                        }
                    }
                }

                // Configure ORDER BY for ordered-set aggregates (ARRAY_AGG, STRING_AGG, etc.)
                for (i, agg) in aggregations.iter().enumerate() {
                    if !agg.order_by.is_empty() {
                        if let Some(ref mut func) = agg_funcs[i] {
                            let specs: Vec<AggregateOrderBySpec> = agg
                                .order_by
                                .iter()
                                .map(|o| AggregateOrderBySpec::new(o.ascending, o.nulls_first))
                                .collect();
                            func.set_order_by_specs(specs);
                        }
                    }
                }

                // Pre-create static Value for COUNT(*)
                let count_star_value = Value::Integer(1);

                // Buffer for evaluated expression values (to avoid repeated allocation)
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
                                        Ok(Value::Boolean(true)) => {} // Continue with accumulation
                                        Ok(Value::Boolean(false)) | Ok(Value::Null(_)) => continue, // Skip this row
                                        Ok(_) => continue, // Non-boolean treated as false
                                        Err(e) => {
                                            return Err(
                                                radixdb_core::Error::expression_evaluation(
                                                    format!("{} FILTER: {}", agg.name, e),
                                                ),
                                            );
                                        }
                                    }
                                } else {
                                    // Can't evaluate filter without VM - skip
                                    continue;
                                }
                            }

                            // Get the value to accumulate
                            let value = if let Some(ref expr_program) = compiled_agg_expressions[i]
                            {
                                // Evaluate the expression for this row using VM
                                if let Some(ref mut vm) = expr_vm {
                                    match vm.execute_cow(expr_program, &exec_ctx) {
                                        Ok(val) => {
                                            expr_values[i] = val;
                                            Some(&expr_values[i])
                                        }
                                        Err(e) => {
                                            return Err(
                                                radixdb_core::Error::expression_evaluation(
                                                    format!("{}({}): {}", agg.name, agg.column, e),
                                                ),
                                            );
                                        }
                                    }
                                } else {
                                    None
                                }
                            } else {
                                // Simple column reference or COUNT(*)
                                if let Some(col_idx) = agg_col_indices[i] {
                                    row.get(col_idx)
                                } else {
                                    Some(&count_star_value)
                                }
                            };

                            if let Some(v) = value {
                                // Check if this aggregate has ORDER BY and supports it
                                if !compiled_order_by[i].is_empty() && func.supports_order_by() {
                                    // Evaluate ORDER BY expressions to get sort keys using pre-compiled programs
                                    if let Some(ref mut vm) = expr_vm {
                                        let mut sort_keys =
                                            Vec::with_capacity(compiled_order_by[i].len());
                                        for order_program in &compiled_order_by[i] {
                                            match vm.execute_cow(order_program, &exec_ctx) {
                                                Ok(key) => sort_keys.push(key),
                                                Err(e) => {
                                                    return Err(
                                                        radixdb_core::Error::expression_evaluation(
                                                            format!("{} ORDER BY: {}", agg.name, e),
                                                        ),
                                                    );
                                                }
                                            }
                                        }
                                        func.accumulate_with_sort_key(v, sort_keys, agg.distinct);
                                    } else {
                                        // No VM - fall back to regular accumulate
                                        func.accumulate(v, agg.distinct);
                                    }
                                } else {
                                    func.accumulate(v, agg.distinct);
                                }
                            }
                        }
                    }
                }

                aggregations
                    .iter()
                    .enumerate()
                    .map(|(i, agg)| -> Result<Value> {
                        if let Some(ref func) = agg_funcs[i] {
                            func.try_result()
                        } else if agg.name == "COUNT" && agg.column == "*" {
                            Ok(Value::Integer(rows.len() as i64))
                        } else {
                            Ok(Value::null_unknown())
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };

        // Build result columns
        let result_columns: Vec<String> =
            aggregations.iter().map(|a| a.get_column_name()).collect();

        let mut result_rows = RowVec::with_capacity(1);
        result_rows.push((0, Row::from_values(result_values)));
        Ok((result_columns, result_rows))
    }

    /// Merge partial aggregate results from parallel processing
    pub(super) fn merge_partial_aggregates(
        &self,
        aggregations: &[SqlAggregateFunction],
        partial_results: Vec<Vec<Value>>,
    ) -> Result<Vec<Value>> {
        if partial_results.is_empty() {
            return Ok(aggregations.iter().map(|_| Value::null_unknown()).collect());
        }

        aggregations
            .iter()
            .enumerate()
            .map(|(i, agg)| {
                let partials: Vec<&Value> = partial_results.iter().map(|r| &r[i]).collect();
                self.merge_single_aggregate(&agg.name, partials)
            })
            .collect()
    }

    /// Merge partial results for a single aggregate function
    pub(super) fn merge_single_aggregate(
        &self,
        func_name: &str,
        partials: Vec<&Value>,
    ) -> Result<Value> {
        // OPTIMIZATION: func_name comes from SqlAggregateFunction.name which is already uppercase
        match func_name {
            "COUNT" => partials
                .into_iter()
                .try_fold(Value::Integer(0), |total, value| {
                    let (Value::Integer(total), Value::Integer(value)) = (total, value) else {
                        return Err(radixdb_core::Error::invalid_argument(
                            "invalid parallel COUNT state",
                        ));
                    };
                    total
                        .checked_add(*value)
                        .map(Value::Integer)
                        .ok_or_else(|| {
                            radixdb_core::Error::invalid_argument("COUNT result overflow")
                        })
                }),
            "SUM" => {
                let mut sum = radixdb_functions::aggregate::SumFunction::default();
                for value in partials {
                    sum.accumulate(value, false);
                }
                sum.try_result()
            }
            "AVG" => {
                // For AVG, we get partial AVGs, but we need SUM/COUNT
                // This is approximate - for exact results, we'd need to track count separately
                // For now, just average the partial averages (less accurate for uneven chunks)
                let mut sum: f64 = 0.0;
                let mut count = 0;

                for val in &partials {
                    match val {
                        Value::Float(f) => {
                            sum += f;
                            count += 1;
                        }
                        Value::Integer(n) => {
                            sum += *n as f64;
                            count += 1;
                        }
                        _ => {}
                    }
                }

                if count > 0 {
                    Ok(Value::Float(sum / count as f64))
                } else {
                    Ok(Value::null_unknown())
                }
            }
            "MIN" => {
                // Take minimum of all partials
                let mut min_val: Option<Value> = None;

                // OPTIMIZATION: Only clone when value actually changes
                for val in partials {
                    if matches!(val, Value::Null(_)) {
                        continue;
                    }
                    match &min_val {
                        None => min_val = Some(val.clone()),
                        Some(current) if val < current => min_val = Some(val.clone()),
                        _ => {} // Keep current, no clone needed
                    }
                }

                Ok(min_val.unwrap_or_else(Value::null_unknown))
            }
            "MAX" => {
                // Take maximum of all partials
                let mut max_val: Option<Value> = None;

                // OPTIMIZATION: Only clone when value actually changes
                for val in partials {
                    if matches!(val, Value::Null(_)) {
                        continue;
                    }
                    match &max_val {
                        None => max_val = Some(val.clone()),
                        Some(current) if val > current => max_val = Some(val.clone()),
                        _ => {} // Keep current, no clone needed
                    }
                }

                Ok(max_val.unwrap_or_else(Value::null_unknown))
            }
            _ => {
                // For unknown functions, just take the first non-null
                Ok(partials
                    .into_iter()
                    .find(|v| !matches!(v, Value::Null(_)))
                    .cloned()
                    .unwrap_or_else(Value::null_unknown))
            }
        }
    }

    /// Try to use fast single-pass aggregation for simple cases
    ///
    /// Returns Some((columns, rows)) if fast path was used, None otherwise.
    /// Fast path is used when:
    /// - All GROUP BY items are simple column references
    /// - All aggregates are COUNT, SUM, AVG, MIN, or MAX (no DISTINCT, FILTER, ORDER BY, or expression)
    ///
    /// When `limit` is provided and there's no ORDER BY, enables early termination:
    /// once we have `limit` complete groups, we stop creating new groups.
    ///
    /// When `having_filter` is provided, applies HAVING inline during row generation,
    /// avoiding a separate filtering pass.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_fast_aggregation(
        &self,
        aggregations: &[SqlAggregateFunction],
        group_by_items: &[GroupByItem],
        rows: &[(i64, Row)],
        _columns: &[String],
        col_index_map: &StringMap<usize>,
        limit: Option<usize>,
        having_filter: Option<&SimpleHavingFilter>,
    ) -> Result<Option<(Vec<String>, RowVec)>> {
        // Check if all GROUP BY items are simple column references
        let group_by_indices: Vec<usize> = group_by_items
            .iter()
            .filter_map(|item| match item {
                GroupByItem::Column(col_name) => {
                    Self::lookup_column_index(&col_name.to_lowercase(), col_index_map)
                }
                _ => None,
            })
            .collect();

        // All GROUP BY items must be resolved to column indices
        if group_by_indices.len() != group_by_items.len() {
            return Ok(None);
        }

        // Check if all aggregates are simple (COUNT/SUM/AVG/MIN/MAX without DISTINCT/FILTER/ORDER BY/expression)
        let simple_aggs: Vec<Option<SimpleAgg>> = aggregations
            .iter()
            .map(|agg| {
                // Must not have DISTINCT, FILTER, ORDER BY, or expression
                if agg.distinct
                    || agg.filter.is_some()
                    || !agg.order_by.is_empty()
                    || agg.expression.is_some()
                {
                    return None;
                }

                match agg.name.to_uppercase().as_str() {
                    "COUNT" => {
                        if agg.column == "*" {
                            Some(SimpleAgg::Count(None))
                        } else {
                            Self::lookup_column_index(&agg.column_lower, col_index_map)
                                .map(|idx| SimpleAgg::Count(Some(idx)))
                        }
                    }
                    "SUM" => {
                        if agg.column == "*" {
                            None // SUM(*) is not valid
                        } else {
                            Self::lookup_column_index(&agg.column_lower, col_index_map)
                                .map(SimpleAgg::Sum)
                        }
                    }
                    "AVG" => {
                        if agg.column == "*" {
                            None // AVG(*) is not valid
                        } else {
                            Self::lookup_column_index(&agg.column_lower, col_index_map)
                                .map(SimpleAgg::Avg)
                        }
                    }
                    "MIN" => {
                        if agg.column == "*" {
                            None // MIN(*) is not valid
                        } else {
                            Self::lookup_column_index(&agg.column_lower, col_index_map)
                                .map(SimpleAgg::Min)
                        }
                    }
                    "MAX" => {
                        if agg.column == "*" {
                            None // MAX(*) is not valid
                        } else {
                            Self::lookup_column_index(&agg.column_lower, col_index_map)
                                .map(SimpleAgg::Max)
                        }
                    }
                    _ => None, // Other aggregates not supported in fast path
                }
            })
            .collect();

        // All aggregates must be resolved for fast path
        if simple_aggs.iter().any(|a| a.is_none()) {
            return Ok(None);
        }

        let simple_aggs: Vec<SimpleAgg> = simple_aggs.into_iter().map(|a| a.unwrap()).collect();

        // OPTIMIZATION: Single-column GROUP BY uses direct Value storage (no Vec allocation per row)
        if group_by_indices.len() == 1 {
            return self.try_fast_aggregation_single_column(
                &group_by_indices[0],
                &simple_aggs,
                aggregations,
                group_by_items,
                rows,
                limit,
                having_filter,
            );
        }

        // Fast path: single-pass streaming aggregation for multi-column GROUP BY
        // Store aggregate state directly in hash map instead of row indices
        // SmallVec for inline storage when ≤4 aggregations (common case)
        use smallvec::SmallVec;
        type AggVec<T> = SmallVec<[T; 4]>;

        struct FastGroupState {
            // SUM and AVG share the canonical checked accumulator used by the
            // ordinary aggregate implementations. Keeping a raw f64 here made
            // the fast GROUP BY path change INTEGER results into FLOAT and lose
            // precision above 2^53.
            numeric_states: AggVec<NumericAccumulator>,
            counts: AggVec<i64>,               // For COUNT
            min_values: AggVec<Option<Value>>, // For MIN
            max_values: AggVec<Option<Value>>, // For MAX
        }

        // Pre-allocate hash map with estimated capacity to reduce resizing.
        // Estimate: for high-cardinality groupings, assume ~1/3 of rows are unique groups.
        // OPTIMIZATION: Use hashbrown::HashMap with Vec<Value> key directly - HashMap handles
        // collisions efficiently with open addressing, avoiding our manual Vec-based collision chaining.
        // Using raw_entry_mut API for O(1) lookup without cloning keys.
        // NOTE: Uses FxHash here because raw_entry_mut().from_hash() requires compatible hasher.
        // Value::hash() is simple (optimized for AHash), but FxHash still works correctly.
        // Start small - HashMap grows efficiently, over-allocation wastes memory
        let estimated_groups = (rows.len() / 32).clamp(16, 256);
        type FxBuildHasher = BuildHasherDefault<FxHasher>;
        let mut groups: hashbrown::HashMap<Vec<Value>, FastGroupState, FxBuildHasher> =
            hashbrown::HashMap::with_capacity_and_hasher(
                estimated_groups,
                FxBuildHasher::default(),
            );
        let num_aggs = simple_aggs.len();

        // Track for early termination optimization
        let group_limit = limit.unwrap_or(usize::MAX);
        let has_limit = limit.is_some();
        let mut current_group_count: usize = 0;

        for (_, row) in rows {
            // OPTIMIZATION: Hash directly from row references (no clone for hashing)
            // FxHasher has zero initialization cost unlike AHash
            let mut hasher = FxHasher::default();
            for &idx in &group_by_indices {
                if let Some(value) = row.get(idx) {
                    value.hash(&mut hasher);
                } else {
                    Value::null_unknown().hash(&mut hasher);
                }
            }
            let hash = hasher.finish();

            // OPTIMIZATION: Use raw_entry_mut for O(1) lookup without cloning
            // - Compute hash from row references (already done above)
            // - Compare stored keys against row references (no clone for lookup)
            // - Only clone when inserting a new group
            // OPTIMIZATION: Unrolled comparison for common cases (2-3 columns)
            // Avoids loop overhead and enables better branch prediction
            let num_group_cols = group_by_indices.len();
            let entry = groups.raw_entry_mut().from_hash(hash, |stored_key| {
                if stored_key.len() != num_group_cols {
                    return false;
                }
                // Inline helper for value comparison
                #[inline(always)]
                fn val_eq(stored: &Value, row_val: Option<&Value>) -> bool {
                    match row_val {
                        Some(rv) => stored == rv,
                        None => matches!(stored, Value::Null(_)),
                    }
                }
                match num_group_cols {
                    2 => {
                        // Unrolled 2-column comparison (most common multi-column case)
                        val_eq(&stored_key[0], row.get(group_by_indices[0]))
                            && val_eq(&stored_key[1], row.get(group_by_indices[1]))
                    }
                    3 => {
                        // Unrolled 3-column comparison
                        val_eq(&stored_key[0], row.get(group_by_indices[0]))
                            && val_eq(&stored_key[1], row.get(group_by_indices[1]))
                            && val_eq(&stored_key[2], row.get(group_by_indices[2]))
                    }
                    _ => {
                        // Generic loop for 4+ columns
                        for i in 0..num_group_cols {
                            if !val_eq(&stored_key[i], row.get(group_by_indices[i])) {
                                return false;
                            }
                        }
                        true
                    }
                }
            });

            let state = match entry {
                RawEntryMut::Occupied(occupied) => occupied.into_mut(),
                RawEntryMut::Vacant(vacant) => {
                    // New group - check limit before creating
                    if has_limit && current_group_count >= group_limit {
                        continue;
                    }
                    // Only clone values when creating a new group
                    let key_values: Vec<Value> = group_by_indices
                        .iter()
                        .map(|&idx| row.get(idx).cloned().unwrap_or_else(Value::null_unknown))
                        .collect();
                    current_group_count += 1;
                    let (_, state) = vacant.insert_hashed_nocheck(
                        hash,
                        key_values,
                        FastGroupState {
                            numeric_states: smallvec::smallvec![NumericAccumulator::default(); num_aggs],
                            counts: smallvec::smallvec![0; num_aggs],
                            min_values: smallvec::smallvec![None; num_aggs],
                            max_values: smallvec::smallvec![None; num_aggs],
                        },
                    );
                    state
                }
            };

            // Accumulate aggregates
            for (i, agg) in simple_aggs.iter().enumerate() {
                match agg {
                    SimpleAgg::Count(_) => {
                        if agg.count_includes_row(row) {
                            state.counts[i] += 1;
                        }
                    }
                    SimpleAgg::Sum(col_idx) | SimpleAgg::Avg(col_idx) => {
                        if let Some(value) = row.get(*col_idx) {
                            state.numeric_states[i].accumulate(value);
                        }
                    }
                    SimpleAgg::Min(col_idx) => {
                        if let Some(value) = row.get(*col_idx) {
                            if !value.is_null() {
                                match &state.min_values[i] {
                                    None => state.min_values[i] = Some(value.clone()),
                                    Some(current) if value < current => {
                                        state.min_values[i] = Some(value.clone())
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    SimpleAgg::Max(col_idx) => {
                        if let Some(value) = row.get(*col_idx) {
                            if !value.is_null() {
                                match &state.max_values[i] {
                                    None => state.max_values[i] = Some(value.clone()),
                                    Some(current) if value > current => {
                                        state.max_values[i] = Some(value.clone())
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        // Build result columns
        let mut result_columns = Vec::with_capacity(group_by_items.len() + aggregations.len());

        // Add GROUP BY column names (use column index to get actual name)
        for (i, item) in group_by_items.iter().enumerate() {
            let name = match item {
                GroupByItem::Column(col_name) => col_name.clone(),
                _ => format!("col{}", i),
            };
            result_columns.push(name);
        }

        // Add aggregate column names
        for agg in aggregations {
            let col_name = if let Some(ref alias) = agg.alias {
                alias.clone()
            } else {
                agg.get_expression_name()
            };
            result_columns.push(col_name);
        }

        // Build result rows from HashMap entries
        // OPTIMIZATION: Apply HAVING filter inline if provided, avoiding separate filtering pass
        let mut result_rows = RowVec::new();
        let mut row_id = 0i64;
        for (key_values, mut state) in groups.into_iter() {
            // Apply inline HAVING filter if provided (supports AND combinations)
            if let Some(filter) = having_filter {
                // All conditions must pass (AND semantics)
                let mut passes = true;
                for cond in &filter.conditions {
                    let agg_value = match &simple_aggs[cond.agg_index] {
                        SimpleAgg::Count(_) => Some(state.counts[cond.agg_index] as f64),
                        SimpleAgg::Sum(_) => state.numeric_states[cond.agg_index]
                            .sum_result()
                            .ok()
                            .and_then(|value| value.as_float64()),
                        SimpleAgg::Avg(_) => state.numeric_states[cond.agg_index]
                            .average_result()
                            .ok()
                            .and_then(|value| value.as_float64()),
                        SimpleAgg::Min(_) => state.min_values[cond.agg_index]
                            .as_ref()
                            .and_then(|v| v.as_float64()),
                        SimpleAgg::Max(_) => state.max_values[cond.agg_index]
                            .as_ref()
                            .and_then(|v| v.as_float64()),
                    };
                    match agg_value {
                        Some(val) => {
                            if !cond.matches(val) {
                                passes = false;
                                break;
                            }
                        }
                        None => {
                            passes = false;
                            break;
                        }
                    }
                }
                if !passes {
                    continue;
                }
            }

            // Use CompactVec directly to avoid Vec→CompactVec conversion
            let mut values: CompactVec<Value> =
                CompactVec::with_capacity(key_values.len() + simple_aggs.len());
            values.extend(key_values);

            for (i, agg) in simple_aggs.iter().enumerate() {
                let value = match agg {
                    SimpleAgg::Count(_) => Value::Integer(state.counts[i]),
                    SimpleAgg::Sum(_) => state.numeric_states[i].sum_result()?,
                    SimpleAgg::Avg(_) => state.numeric_states[i].average_result()?,
                    SimpleAgg::Min(_) => state.min_values[i]
                        .take()
                        .unwrap_or_else(Value::null_unknown),
                    SimpleAgg::Max(_) => state.max_values[i]
                        .take()
                        .unwrap_or_else(Value::null_unknown),
                };
                values.push(value);
            }

            result_rows.push((row_id, Row::from_compact_vec(values)));
            row_id += 1;
        }

        Ok(Some((result_columns, result_rows)))
    }
}
