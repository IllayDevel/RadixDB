use super::*;

impl<'host, H: WindowHost + ?Sized> WindowExecutor<'host, H> {
    /// Compute aggregate function as window function (SUM, COUNT, AVG, MIN, MAX)
    /// cached_order_by: Optional precomputed ORDER BY values from cache to avoid redundant computation
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_aggregate_window_function(
        &self,
        wf_info: &WindowFunctionInfo,
        rows: &[(i64, Row)],
        columns: &[String],
        col_index_map: &StringMap<usize>,
        ctx: &ExecutionContext,
        pre_sorted: Option<&WindowPreSortedState>,
        cached_order_by: Option<&ColumnarOrderByValues>,
    ) -> Result<Vec<Value>> {
        // Check if this is COUNT(*) - Star expression means count all rows
        let is_count_star =
            !wf_info.arguments.is_empty() && matches!(wf_info.arguments[0], Expression::Star(_));

        // Get the column index for the aggregate argument
        let arg_col_idx: Option<usize> = if !wf_info.arguments.is_empty() && !is_count_star {
            self.resolve_column_index(&wf_info.arguments[0], col_index_map)
        } else {
            // COUNT(*) has no arguments or Star expression
            None
        };

        // Check if the argument is an expression that needs evaluation
        // This handles cases like SUM(val * 2) or SUM(SUM(val)) in grouped results
        // But NOT for COUNT(*) which should just count rows
        let has_expression_arg =
            !wf_info.arguments.is_empty() && !is_count_star && arg_col_idx.is_none();

        // Pre-compute expression values for all rows if needed
        let expression_values: Vec<Value> = if has_expression_arg {
            let mut eval =
                ExpressionEval::compile(&wf_info.arguments[0], columns)?.with_context(ctx);
            rows.iter()
                .map(|(_, row)| eval.eval(row))
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };

        // Use precomputed ORDER BY values from cache if available
        let empty_order_by = ColumnarOrderByValues {
            columns: vec![],
            ascending: vec![],
            nulls_first: vec![],
            num_rows: 0,
        };
        let order_by_values: &ColumnarOrderByValues = cached_order_by.unwrap_or(&empty_order_by);

        // Group rows by partition key
        let partitions = Self::build_partition_map(wf_info, rows, columns, col_index_map, ctx)?;

        // Check if we can skip sorting (index optimization)
        // Only applies when there's no PARTITION BY (single partition)
        let skip_sorting =
            wf_info.partition_by_exprs.is_empty() && self.check_rows_presorted(wf_info, pre_sorted);

        // Compute aggregate for each partition
        // OPTIMIZATION: Use Option<Value> with vec![None; n] - None is just a discriminant
        // (no data to clone), much faster than vec![NULL_VALUE; n] which clones ~32 byte Values
        let mut results: Vec<Option<Value>> = vec![None; rows.len()];

        for (_key, mut row_indices) in partitions {
            // Sort partition by ORDER BY if specified (skip if pre-sorted)
            if !wf_info.order_by.is_empty() {
                // Only sort if not already pre-sorted by index
                if !skip_sorting {
                    Self::sort_by_order_values(&mut row_indices, order_by_values);
                }

                // With ORDER BY, compute aggregate with frame specification
                // Default frame is UNBOUNDED PRECEDING to CURRENT ROW if no explicit frame
                let partition_len = row_indices.len();

                // OPTIMIZATION: Precompute peer group boundaries in O(n) instead of O(n²)
                // For RANGE frames, rows with the same ORDER BY value are "peers"
                // After sorting, peers are adjacent, so we can find boundaries in one pass
                // peer_groups[i] = (start_idx, end_idx) where end_idx is exclusive
                let peer_groups: Vec<(usize, usize)> = {
                    let mut groups = Vec::with_capacity(partition_len);
                    if partition_len == 0 {
                        groups
                    } else {
                        let mut group_start = 0;
                        // Compare adjacent rows by reference to avoid O(n) cloning
                        let mut prev_row_idx = row_indices[0];

                        for (i, &row_idx) in row_indices.iter().enumerate().skip(1) {
                            // Compare ORDER BY values without cloning
                            if !order_by_values.rows_equal(prev_row_idx, row_idx) {
                                // New peer group starts - fill in previous group for all its members
                                for _ in group_start..i {
                                    groups.push((group_start, i));
                                }
                                group_start = i;
                            }
                            prev_row_idx = row_idx;
                        }
                        // Fill in the last group
                        for _ in group_start..partition_len {
                            groups.push((group_start, partition_len));
                        }
                        groups
                    }
                };

                // Get aggregate function ONCE, reuse with reset() for each row
                let mut agg_func = self
                    .host
                    .window_function_registry()
                    .get_aggregate(&wf_info.name)
                    .ok_or_else(|| {
                        Error::NotSupported(format!("Unknown aggregate function: {}", wf_info.name))
                    })?;

                // Check if we can use O(n) incremental accumulation instead of O(n²) reset.
                // Safe when: frame start is UNBOUNDED PRECEDING (frame only grows),
                // frame end is not PRECEDING (which would make end < current row),
                // and no DISTINCT (distinct requires full set tracking).
                let has_range_value_offset = wf_info.frame.as_ref().is_some_and(|frame| {
                    matches!(frame.unit, WindowFrameUnit::Range)
                        && (matches!(
                            frame.start,
                            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
                        ) || frame.end.as_ref().is_some_and(|end| {
                            matches!(
                                end,
                                WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
                            )
                        }))
                });
                let can_incremental = !has_range_value_offset
                    && wf_info.frame.as_ref().is_none_or(|f| {
                        let start_ok = matches!(f.start, WindowFrameBound::UnboundedPreceding);
                        let end_ok = f.end.as_ref().is_none_or(|e| {
                            match e {
                                // PRECEDING end: frame can shrink, not monotonic
                                WindowFrameBound::Preceding(_)
                                | WindowFrameBound::UnboundedPreceding => false,
                                // RANGE FOLLOWING: NULL ORDER BY values cause fallback to partition_len,
                                // then later rows compute smaller frame_end — not monotonic with NULLs
                                WindowFrameBound::Following(_)
                                    if matches!(f.unit, WindowFrameUnit::Range) =>
                                {
                                    false
                                }
                                _ => true,
                            }
                        });
                        start_ok && end_ok
                    });

                if can_incremental && !wf_info.is_distinct {
                    // O(n) incremental path: frame start is always 0, only end grows.
                    // Accumulate new values as frame_end advances, never reset.
                    let mut prev_end = 0usize;
                    for (i, &row_idx) in row_indices.iter().enumerate() {
                        let frame_end = if let Some(ref frame) = wf_info.frame {
                            let is_range = matches!(frame.unit, WindowFrameUnit::Range);
                            if let Some(ref end_bound) = frame.end {
                                match end_bound {
                                    WindowFrameBound::UnboundedFollowing => partition_len,
                                    WindowFrameBound::CurrentRow => {
                                        if is_range {
                                            peer_groups[i].1
                                        } else {
                                            i + 1
                                        }
                                    }
                                    WindowFrameBound::Following(expr) => {
                                        if is_range {
                                            if let (
                                                Some(curr_f64),
                                                Expression::IntegerLiteral(lit),
                                            ) = (
                                                order_by_values.get_first(row_idx).and_then(|v| {
                                                    match v {
                                                        Value::Integer(i) => Some(*i as f64),
                                                        Value::Float(f) => Some(*f),
                                                        _ => None,
                                                    }
                                                }),
                                                expr.as_ref(),
                                            ) {
                                                let upper = curr_f64 + lit.value as f64;
                                                let mut end_idx = 0;
                                                for (j, &idx) in row_indices.iter().enumerate() {
                                                    if let Some(rv) = order_by_values
                                                        .get_first(idx)
                                                        .and_then(|v| match v {
                                                            Value::Integer(i) => Some(*i as f64),
                                                            Value::Float(f) => Some(*f),
                                                            _ => None,
                                                        })
                                                    {
                                                        if rv <= upper {
                                                            end_idx = j + 1;
                                                        }
                                                    }
                                                }
                                                end_idx
                                            } else {
                                                partition_len
                                            }
                                        } else if let Expression::IntegerLiteral(lit) =
                                            expr.as_ref()
                                        {
                                            (i + lit.value as usize + 1).min(partition_len)
                                        } else {
                                            partition_len
                                        }
                                    }
                                    _ => {
                                        // PRECEDING / UNBOUNDED PRECEDING end bounds are excluded
                                        // by the can_incremental guard above. This branch is
                                        // unreachable but kept as a safe fallback.
                                        partition_len
                                    }
                                }
                            } else {
                                // No explicit end: default CURRENT ROW
                                if is_range {
                                    peer_groups[i].1
                                } else {
                                    i + 1
                                }
                            }
                        } else {
                            // Default frame: RANGE UNBOUNDED PRECEDING TO CURRENT ROW
                            peer_groups[i].1
                        };

                        // Accumulate only the new values since prev_end
                        for j in prev_end..frame_end {
                            let value = if let Some(col_idx) = arg_col_idx {
                                rows[row_indices[j]]
                                    .1
                                    .get(col_idx)
                                    .cloned()
                                    .unwrap_or_else(Value::null_unknown)
                            } else if has_expression_arg {
                                expression_values[row_indices[j]].clone()
                            } else {
                                Value::Integer(1)
                            };
                            agg_func.accumulate(&value, false);
                        }
                        if frame_end > prev_end {
                            prev_end = frame_end;
                        }
                        results[row_idx] = Some(agg_func.try_result()?);
                    }
                } else {
                    for (i, &row_idx) in row_indices.iter().enumerate() {
                        // Reset aggregate state for new frame computation
                        agg_func.reset();

                        // Compute frame bounds based on frame specification
                        let (frame_start, frame_end) = if has_range_value_offset {
                            self.compute_simple_frame_bounds(
                                wf_info,
                                i,
                                partition_len,
                                peer_groups[i],
                                &row_indices,
                                order_by_values,
                            )?
                        } else if let Some(ref frame) = wf_info.frame {
                            let is_range = matches!(frame.unit, WindowFrameUnit::Range);

                            // For RANGE frames with numeric offsets, we need value-based comparison
                            // Get the current row's ORDER BY value for RANGE calculations
                            let current_order_value = if is_range && !order_by_values.is_empty() {
                                order_by_values.get_first(row_idx).cloned()
                            } else {
                                None
                            };

                            // Helper to convert value to f64 for range comparisons
                            let value_to_f64 = |v: &Value| -> Option<f64> {
                                match v {
                                    Value::Integer(i) => Some(*i as f64),
                                    Value::Float(f) => Some(*f),
                                    _ => None,
                                }
                            };

                            // Calculate start bound
                            let start = match &frame.start {
                                WindowFrameBound::UnboundedPreceding => 0,
                                WindowFrameBound::CurrentRow => {
                                    if is_range {
                                        // For RANGE, start of peer group (O(1) lookup)
                                        peer_groups[i].0
                                    } else {
                                        i
                                    }
                                }
                                WindowFrameBound::Preceding(expr) => {
                                    if is_range {
                                        // RANGE PRECEDING: find first row where value >= current - offset
                                        if let (Some(curr_val), Expression::IntegerLiteral(lit)) =
                                            (&current_order_value, expr.as_ref())
                                        {
                                            if let Some(curr_f64) = value_to_f64(curr_val) {
                                                let lower_bound = curr_f64 - lit.value as f64;
                                                // Linear scan from start to find first row in range
                                                let mut start_idx = 0;
                                                for (j, &idx) in row_indices.iter().enumerate() {
                                                    if let Some(row_val) = order_by_values
                                                        .get_first(idx)
                                                        .and_then(value_to_f64)
                                                    {
                                                        if row_val >= lower_bound {
                                                            start_idx = j;
                                                            break;
                                                        }
                                                    }
                                                }
                                                start_idx
                                            } else {
                                                0
                                            }
                                        } else {
                                            0
                                        }
                                    } else {
                                        // ROWS PRECEDING: simple row offset
                                        if let Expression::IntegerLiteral(lit) = expr.as_ref() {
                                            i.saturating_sub(lit.value as usize)
                                        } else {
                                            0
                                        }
                                    }
                                }
                                WindowFrameBound::Following(expr) => {
                                    if is_range {
                                        // RANGE FOLLOWING as start: find first row where value >= current + offset
                                        if let (Some(curr_val), Expression::IntegerLiteral(lit)) =
                                            (&current_order_value, expr.as_ref())
                                        {
                                            if let Some(curr_f64) = value_to_f64(curr_val) {
                                                let lower_bound = curr_f64 + lit.value as f64;
                                                let mut start_idx = partition_len;
                                                for (j, &idx) in row_indices.iter().enumerate() {
                                                    if let Some(row_val) = order_by_values
                                                        .get_first(idx)
                                                        .and_then(value_to_f64)
                                                    {
                                                        if row_val >= lower_bound {
                                                            start_idx = j;
                                                            break;
                                                        }
                                                    }
                                                }
                                                start_idx
                                            } else {
                                                i
                                            }
                                        } else {
                                            i
                                        }
                                    } else if let Expression::IntegerLiteral(lit) = expr.as_ref() {
                                        (i + lit.value as usize).min(partition_len - 1)
                                    } else {
                                        i
                                    }
                                }
                                WindowFrameBound::UnboundedFollowing => partition_len - 1,
                            };

                            // Calculate end bound
                            let end = if let Some(ref end_bound) = frame.end {
                                match end_bound {
                                    WindowFrameBound::UnboundedFollowing => partition_len,
                                    WindowFrameBound::CurrentRow => {
                                        if is_range {
                                            // For RANGE, end of peer group (O(1) lookup)
                                            peer_groups[i].1
                                        } else {
                                            i + 1
                                        }
                                    }
                                    WindowFrameBound::Following(expr) => {
                                        if is_range {
                                            // RANGE FOLLOWING: find last row where value <= current + offset
                                            if let (
                                                Some(curr_val),
                                                Expression::IntegerLiteral(lit),
                                            ) = (&current_order_value, expr.as_ref())
                                            {
                                                if let Some(curr_f64) = value_to_f64(curr_val) {
                                                    let upper_bound = curr_f64 + lit.value as f64;
                                                    // Scan from end backwards to find last row in range
                                                    let mut end_idx = 0;
                                                    for (j, &idx) in row_indices.iter().enumerate()
                                                    {
                                                        if let Some(row_val) = order_by_values
                                                            .get_first(idx)
                                                            .and_then(value_to_f64)
                                                        {
                                                            if row_val <= upper_bound {
                                                                end_idx = j + 1;
                                                                // exclusive end
                                                            }
                                                        }
                                                    }
                                                    end_idx
                                                } else {
                                                    partition_len
                                                }
                                            } else {
                                                partition_len
                                            }
                                        } else if let Expression::IntegerLiteral(lit) =
                                            expr.as_ref()
                                        {
                                            (i + lit.value as usize + 1).min(partition_len)
                                        } else {
                                            partition_len
                                        }
                                    }
                                    WindowFrameBound::Preceding(expr) => {
                                        if is_range {
                                            // RANGE PRECEDING as end: find last row where value <= current - offset
                                            if let (
                                                Some(curr_val),
                                                Expression::IntegerLiteral(lit),
                                            ) = (&current_order_value, expr.as_ref())
                                            {
                                                if let Some(curr_f64) = value_to_f64(curr_val) {
                                                    let upper_bound = curr_f64 - lit.value as f64;
                                                    let mut end_idx = 0;
                                                    for (j, &idx) in row_indices.iter().enumerate()
                                                    {
                                                        if let Some(row_val) = order_by_values
                                                            .get_first(idx)
                                                            .and_then(value_to_f64)
                                                        {
                                                            if row_val <= upper_bound {
                                                                end_idx = j + 1;
                                                            }
                                                        }
                                                    }
                                                    end_idx
                                                } else {
                                                    i + 1
                                                }
                                            } else {
                                                i + 1
                                            }
                                        } else if let Expression::IntegerLiteral(lit) =
                                            expr.as_ref()
                                        {
                                            (i + 1).saturating_sub(lit.value as usize)
                                        } else {
                                            i + 1
                                        }
                                    }
                                    WindowFrameBound::UnboundedPreceding => 0,
                                }
                            } else {
                                // No end bound specified, SQL standard says implicit end is CURRENT ROW
                                // For ROWS: current row index + 1 (exclusive)
                                // For RANGE: end of current peer group
                                if is_range {
                                    peer_groups[i].1
                                } else {
                                    i + 1
                                }
                            };

                            (start, end)
                        } else {
                            // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            // SQL standard: peer rows (same ORDER BY value) share the same frame end
                            (0, peer_groups[i].1)
                        };

                        // Accumulate values within the frame
                        for &idx in &row_indices[frame_start..frame_end] {
                            let value = if let Some(col_idx) = arg_col_idx {
                                rows[idx]
                                    .1
                                    .get(col_idx)
                                    .cloned()
                                    .unwrap_or_else(Value::null_unknown)
                            } else if has_expression_arg {
                                // Expression argument (e.g., val * 2) - use pre-computed value
                                expression_values[idx].clone()
                            } else {
                                // COUNT(*) counts all rows
                                Value::Integer(1)
                            };
                            agg_func.accumulate(&value, wf_info.is_distinct);
                        }
                        results[row_idx] = Some(agg_func.try_result()?);
                    }
                } // end of else (non-incremental path)
            } else {
                // Without ORDER BY, compute aggregate over entire partition
                let mut agg_func = self
                    .host
                    .window_function_registry()
                    .get_aggregate(&wf_info.name)
                    .ok_or_else(|| {
                        Error::NotSupported(format!("Unknown aggregate function: {}", wf_info.name))
                    })?;

                // Accumulate all values in the partition
                for &row_idx in &row_indices {
                    let value = if let Some(col_idx) = arg_col_idx {
                        rows[row_idx]
                            .1
                            .get(col_idx)
                            .cloned()
                            .unwrap_or_else(Value::null_unknown)
                    } else if has_expression_arg {
                        // Expression argument (e.g., val * 2) - use pre-computed value
                        expression_values[row_idx].clone()
                    } else {
                        // COUNT(*) counts all rows
                        Value::Integer(1)
                    };
                    agg_func.accumulate(&value, wf_info.is_distinct);
                }
                let aggregate_result = agg_func.try_result()?;

                // Assign the same aggregate result to all rows in the partition
                for &row_idx in &row_indices {
                    results[row_idx] = Some(aggregate_result.clone());
                }
            }
        }

        // Unwrap all values (all indices should have been written)
        Ok(results
            .into_iter()
            .map(|opt| opt.unwrap_or(NULL_VALUE))
            .collect())
    }

    /// Extract aggregate function patterns from an expression (including nested ones)
    /// This handles cases like COALESCE(SUM(val), 0) where SUM(val) is nested
    pub(super) fn extract_aggregate_patterns(&self, expr: &Expression) -> Vec<String> {
        let mut patterns = Vec::new();
        self.collect_aggregate_patterns(expr, &mut patterns);
        patterns
    }

    /// Helper to recursively collect aggregate patterns from an expression
    pub(super) fn collect_aggregate_patterns(&self, expr: &Expression, patterns: &mut Vec<String>) {
        match expr {
            Expression::FunctionCall(func) => {
                if self
                    .host
                    .window_function_registry()
                    .is_aggregate(&func.function)
                {
                    // This is an aggregate function - generate its pattern
                    let pattern = if func.arguments.is_empty()
                        || matches!(func.arguments.first(), Some(Expression::Star(_)))
                    {
                        format!("{}(*)", func.function)
                    } else if func.arguments.len() == 1 {
                        match &func.arguments[0] {
                            Expression::Identifier(id) => {
                                format!("{}({})", func.function, id.value)
                            }
                            Expression::QualifiedIdentifier(qid) => {
                                // Generate BOTH qualified and unqualified patterns
                                // e.g., for SUM(o.amount), add "SUM(amount)" first
                                let unqualified = format!("{}({})", func.function, qid.name.value);
                                patterns.push(unqualified);
                                // Then add qualified pattern "SUM(o.amount)"
                                format!(
                                    "{}({}.{})",
                                    func.function, qid.qualifier.value, qid.name.value
                                )
                            }
                            Expression::Distinct(d) => {
                                // Handle DISTINCT, e.g., COUNT(DISTINCT val)
                                match d.expr.as_ref() {
                                    Expression::Identifier(id) => {
                                        format!("{}(DISTINCT {})", func.function, id.value)
                                    }
                                    Expression::QualifiedIdentifier(qid) => {
                                        // Generate both qualified and unqualified patterns
                                        let unqualified = format!(
                                            "{}(DISTINCT {})",
                                            func.function, qid.name.value
                                        );
                                        patterns.push(unqualified);
                                        format!(
                                            "{}(DISTINCT {}.{})",
                                            func.function, qid.qualifier.value, qid.name.value
                                        )
                                    }
                                    _ => return,
                                }
                            }
                            _ => return,
                        }
                    } else {
                        return;
                    };
                    patterns.push(pattern);
                } else {
                    // Non-aggregate function - check its arguments for nested aggregates
                    for arg in &func.arguments {
                        self.collect_aggregate_patterns(arg, patterns);
                    }
                }
            }
            Expression::Infix(infix) => {
                self.collect_aggregate_patterns(&infix.left, patterns);
                self.collect_aggregate_patterns(&infix.right, patterns);
            }
            Expression::Prefix(prefix) => {
                self.collect_aggregate_patterns(&prefix.right, patterns);
            }
            Expression::Case(case) => {
                if let Some(ref value) = case.value {
                    self.collect_aggregate_patterns(value, patterns);
                }
                for when_clause in &case.when_clauses {
                    self.collect_aggregate_patterns(&when_clause.condition, patterns);
                    self.collect_aggregate_patterns(&when_clause.then_result, patterns);
                }
                if let Some(ref else_value) = case.else_value {
                    self.collect_aggregate_patterns(else_value, patterns);
                }
            }
            Expression::Cast(cast) => {
                self.collect_aggregate_patterns(&cast.expr, patterns);
            }
            Expression::Aliased(aliased) => {
                self.collect_aggregate_patterns(&aliased.expression, patterns);
            }
            Expression::List(list) => {
                for e in &list.elements {
                    self.collect_aggregate_patterns(e, patterns);
                }
            }
            _ => {}
        }
    }
}
