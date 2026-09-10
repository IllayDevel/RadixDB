use super::*;

impl<'host, H: AggregationHost + ?Sized> AggregationExecutor<'host, H> {
    /// Single-column GROUP BY fast aggregation - avoids Vec<Value> allocation per row
    ///
    /// For single-column GROUP BY (the most common case), we can store the group key
    /// as a single Value instead of Vec<Value>, eliminating allocation overhead.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_fast_aggregation_single_column(
        &self,
        group_col_idx: &usize,
        simple_aggs: &[SimpleAgg],
        aggregations: &[SqlAggregateFunction],
        group_by_items: &[GroupByItem],
        rows: &[(i64, Row)],
        limit: Option<usize>,
        having_filter: Option<&SimpleHavingFilter>,
    ) -> Result<Option<(Vec<String>, RowVec)>> {
        // State for single-column GROUP BY - stores single Value instead of Vec<Value>
        // SmallVec for inline storage when ≤4 aggregations (common case)
        use smallvec::SmallVec;
        type AggVec<T> = SmallVec<[T; 4]>;

        struct SingleColGroupState {
            key_value: Value,
            numeric_states: AggVec<NumericAccumulator>,
            counts: AggVec<i64>,
            min_values: AggVec<Option<Value>>,
            max_values: AggVec<Option<Value>>,
        }

        // Start small - HashMap grows efficiently, over-allocation wastes memory
        let estimated_groups = (rows.len() / 32).clamp(16, 256);
        let num_aggs = simple_aggs.len();
        let col_idx = *group_col_idx;

        // Track for early termination optimization
        let group_limit = limit.unwrap_or(usize::MAX);
        let has_limit = limit.is_some();

        // A typed path is admissible only after proving the complete input is
        // homogeneous. Sampling used to silently drop a later value of another
        // type in the typed loop.
        let mut has_integer = false;
        let mut integer_or_null = true;
        let mut has_text = false;
        let mut text_or_null = true;
        for (_, row) in rows {
            match row.get(col_idx) {
                Some(Value::Integer(value)) => {
                    has_integer = true;
                    text_or_null = false;
                    if *value == i64::MIN {
                        // I64Map reserves i64::MIN as its empty sentinel.
                        // The general Value-key path supports the full domain.
                        integer_or_null = false;
                    }
                }
                Some(Value::Text(_)) => {
                    has_text = true;
                    integer_or_null = false;
                }
                Some(Value::Null(_)) | None => {}
                Some(_) => {
                    integer_or_null = false;
                    text_or_null = false;
                }
            }
        }

        let use_integer_fast_path = has_integer && integer_or_null;
        let use_string_fast_path = has_text && text_or_null;

        if use_integer_fast_path {
            // Ultra-fast path for Integer GROUP BY: no Value cloning, no hashing overhead
            // SmallVec for inline storage when ≤4 aggregations (common case)
            // Avoids heap allocation on clone for new groups
            use smallvec::SmallVec;
            type AggVec<T> = SmallVec<[T; 4]>;

            #[derive(Clone)]
            struct IntGroupState {
                numeric_states: AggVec<NumericAccumulator>,
                counts: AggVec<i64>,
                min_values: AggVec<Option<Value>>,
                max_values: AggVec<Option<Value>>,
            }

            let mut groups: I64Map<IntGroupState> = I64Map::with_capacity(estimated_groups);
            // Separate tracking for NULL group (SQL: all NULLs group together)
            let mut null_group: Option<IntGroupState> = None;
            let mut current_group_count: usize = 0;

            // OPTIMIZATION: Pre-allocate template state - clone is faster than separate smallvec! calls
            let state_template = IntGroupState {
                numeric_states: smallvec::smallvec![NumericAccumulator::default(); num_aggs],
                counts: smallvec::smallvec![0; num_aggs],
                min_values: smallvec::smallvec![None; num_aggs],
                max_values: smallvec::smallvec![None; num_aggs],
            };

            for (_, row) in rows {
                // Extract integer key directly - no clone, no hash
                // Handle NULL values separately (they form their own group)
                let key_opt = match row.get(col_idx) {
                    Some(Value::Integer(v)) => Some(*v),
                    Some(Value::Null(_)) => None, // NULL goes to null_group (inline pattern)
                    None => None,                 // Missing value treated as NULL
                    _ => unreachable!("integer fast path requires homogeneous group keys"),
                };

                let state = if let Some(key) = key_opt {
                    // OPTIMIZATION: Single hash lookup using entry API instead of
                    // contains_key + entry (was doing 2 hash lookups)
                    use radixdb_core::i64_map::Entry;
                    match groups.entry(key) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(e) => {
                            // Early termination check for new groups
                            if has_limit && current_group_count >= group_limit {
                                continue;
                            }
                            current_group_count += 1;
                            e.insert(state_template.clone())
                        }
                    }
                } else {
                    // NULL group
                    if null_group.is_none() {
                        if has_limit && current_group_count >= group_limit {
                            continue;
                        }
                        current_group_count += 1;
                        null_group = Some(state_template.clone());
                    }
                    null_group.as_mut().unwrap()
                };

                // Accumulate aggregates
                for (i, agg) in simple_aggs.iter().enumerate() {
                    match agg {
                        SimpleAgg::Count(_) => {
                            if agg.count_includes_row(row) {
                                state.counts[i] += 1;
                            }
                        }
                        SimpleAgg::Sum(sum_col_idx) | SimpleAgg::Avg(sum_col_idx) => {
                            if let Some(value) = row.get(*sum_col_idx) {
                                state.numeric_states[i].accumulate(value);
                            }
                        }
                        SimpleAgg::Min(min_col_idx) => {
                            if let Some(value) = row.get(*min_col_idx) {
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
                        SimpleAgg::Max(max_col_idx) => {
                            if let Some(value) = row.get(*max_col_idx) {
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
            let mut result_columns = Vec::with_capacity(1 + aggregations.len());
            let group_col_name = match &group_by_items[0] {
                GroupByItem::Column(col_name) => col_name.clone(),
                _ => "col0".to_string(),
            };
            result_columns.push(group_col_name);
            for agg in aggregations {
                let col_name = if let Some(ref alias) = agg.alias {
                    alias.clone()
                } else {
                    agg.get_expression_name()
                };
                result_columns.push(col_name);
            }

            // Helper to check HAVING filter
            let passes_having = |state: &IntGroupState| -> bool {
                if let Some(filter) = having_filter {
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
                                    return false;
                                }
                            }
                            None => return false,
                        }
                    }
                }
                true
            };

            // Helper to build row from state
            // Use CompactVec directly to avoid Vec→CompactVec conversion
            let build_row = |key_value: Value, mut state: IntGroupState| -> Result<Row> {
                let mut values: CompactVec<Value> =
                    CompactVec::with_capacity(1 + simple_aggs.len());
                values.push(key_value);
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
                Ok(Row::from_compact_vec(values))
            };

            // Build result rows with inline HAVING filter (supports AND combinations)
            let mut result_rows = RowVec::new();
            let mut row_id = 0i64;
            for (key, state) in groups.into_iter() {
                if passes_having(&state) {
                    result_rows.push((row_id, build_row(Value::Integer(key), state)?));
                    row_id += 1;
                }
            }

            // Add NULL group if it exists and passes HAVING
            if let Some(ng) = null_group {
                if passes_having(&ng) {
                    result_rows.push((row_id, build_row(Value::null_unknown(), ng)?));
                }
            }

            return Ok(Some((result_columns, result_rows)));
        }

        // String fast path for Text GROUP BY: direct SmartString key, no Value::eq overhead
        // SmallVec for inline storage when ≤4 aggregations (common case)
        if use_string_fast_path {
            use smallvec::SmallVec;
            type AggVec<T> = SmallVec<[T; 4]>;

            #[derive(Clone)]
            struct StringGroupState {
                numeric_states: AggVec<NumericAccumulator>,
                counts: AggVec<i64>,
                min_values: AggVec<Option<Value>>,
                max_values: AggVec<Option<Value>>,
            }

            let mut groups: FxHashMap<radixdb_core::SmartString, StringGroupState> =
                FxHashMap::with_capacity_and_hasher(estimated_groups, Default::default());
            // Separate tracking for NULL group (SQL: all NULLs group together)
            let mut null_group: Option<StringGroupState> = None;
            let mut current_group_count: usize = 0;

            // OPTIMIZATION: Pre-allocate template state - clone is faster than separate smallvec! calls
            let state_template = StringGroupState {
                numeric_states: smallvec::smallvec![NumericAccumulator::default(); num_aggs],
                counts: smallvec::smallvec![0; num_aggs],
                min_values: smallvec::smallvec![None; num_aggs],
                max_values: smallvec::smallvec![None; num_aggs],
            };

            for (_, row) in rows {
                // Extract string key directly - only clone when creating new group
                // Handle NULL values separately (they form their own group)
                let key_str_opt = match row.get(col_idx) {
                    Some(Value::Text(s)) => Some(s),
                    Some(Value::Null(_)) => None, // NULL goes to null_group (inline pattern)
                    None => None,                 // Missing value treated as NULL
                    _ => unreachable!("text fast path requires homogeneous group keys"),
                };

                let state = if let Some(key_str) = key_str_opt {
                    // OPTIMIZATION: get_mut first (no clone for existing groups)
                    // For aggregation with many rows but few groups, most rows hit existing groups
                    // This avoids cloning the key on every row - major perf win
                    match groups.get_mut(key_str) {
                        Some(existing) => existing,
                        None => {
                            // Early termination check for new groups
                            if has_limit && current_group_count >= group_limit {
                                continue;
                            }
                            current_group_count += 1;
                            // Clone key only when creating new group
                            groups.insert(key_str.clone(), state_template.clone());
                            // SAFETY: we just inserted, so key exists
                            groups.get_mut(key_str).unwrap()
                        }
                    }
                } else {
                    // NULL group
                    if null_group.is_none() {
                        if has_limit && current_group_count >= group_limit {
                            continue;
                        }
                        current_group_count += 1;
                        null_group = Some(state_template.clone());
                    }
                    null_group.as_mut().unwrap()
                };

                // Accumulate aggregates
                for (i, agg) in simple_aggs.iter().enumerate() {
                    match agg {
                        SimpleAgg::Count(_) => {
                            if agg.count_includes_row(row) {
                                state.counts[i] += 1;
                            }
                        }
                        SimpleAgg::Sum(sum_col_idx) | SimpleAgg::Avg(sum_col_idx) => {
                            if let Some(value) = row.get(*sum_col_idx) {
                                state.numeric_states[i].accumulate(value);
                            }
                        }
                        SimpleAgg::Min(min_col_idx) => {
                            if let Some(value) = row.get(*min_col_idx) {
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
                        SimpleAgg::Max(max_col_idx) => {
                            if let Some(value) = row.get(*max_col_idx) {
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
            let mut result_columns = Vec::with_capacity(1 + aggregations.len());
            let group_col_name = match &group_by_items[0] {
                GroupByItem::Column(col_name) => col_name.clone(),
                _ => "col0".to_string(),
            };
            result_columns.push(group_col_name);
            for agg in aggregations {
                let col_name = if let Some(ref alias) = agg.alias {
                    alias.clone()
                } else {
                    agg.get_expression_name()
                };
                result_columns.push(col_name);
            }

            // Helper to check HAVING filter
            let passes_having = |state: &StringGroupState| -> bool {
                if let Some(filter) = having_filter {
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
                                    return false;
                                }
                            }
                            None => return false,
                        }
                    }
                }
                true
            };

            // Helper to build row from state
            // Use CompactVec directly to avoid Vec→CompactVec conversion
            let build_row = |key_value: Value, mut state: StringGroupState| -> Result<Row> {
                let mut values: CompactVec<Value> =
                    CompactVec::with_capacity(1 + simple_aggs.len());
                values.push(key_value);
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
                Ok(Row::from_compact_vec(values))
            };

            // Build result rows with inline HAVING filter
            let mut result_rows = RowVec::new();
            let mut row_id = 0i64;
            for (key, state) in groups.into_iter() {
                if passes_having(&state) {
                    result_rows.push((row_id, build_row(Value::Text(key), state)?));
                    row_id += 1;
                }
            }

            // Add NULL group if it exists and passes HAVING
            if let Some(ng) = null_group {
                if passes_having(&ng) {
                    result_rows.push((row_id, build_row(Value::null_unknown(), ng)?));
                }
            }

            return Ok(Some((result_columns, result_rows)));
        }

        // Fallback: general single-column path with Value storage
        // Use hash -> Vec to handle collisions (different values with same hash)
        let mut groups: FxHashMap<u64, Vec<SingleColGroupState>> =
            FxHashMap::with_capacity_and_hasher(estimated_groups, Default::default());
        let mut current_group_count: usize = 0;

        for (_, row) in rows {
            // OPTIMIZATION: Hash directly from row reference (no clone for hashing)
            let row_value = row.get(col_idx);
            let mut hasher = AHasher::default();
            if let Some(value) = row_value {
                value.hash(&mut hasher);
            } else {
                Value::null_unknown().hash(&mut hasher);
            }
            let hash = hasher.finish();

            // Get or create bucket for this hash
            let bucket = groups.entry(hash).or_default();

            // OPTIMIZATION: Compare row value directly against stored keys (no clone for lookup)
            // Inline is_null() check as pattern match to avoid function call overhead
            let existing_idx = bucket.iter().position(|s| match row_value {
                Some(rv) => &s.key_value == rv,
                None => matches!(s.key_value, Value::Null(_)),
            });

            let state = if let Some(idx) = existing_idx {
                // Existing group - no clone needed!
                &mut bucket[idx]
            } else {
                // New group - check limit before creating
                if has_limit && current_group_count >= group_limit {
                    continue;
                }
                // Only clone when creating a new group
                let key_value = row_value.cloned().unwrap_or_else(Value::null_unknown);
                bucket.push(SingleColGroupState {
                    key_value,
                    numeric_states: smallvec::smallvec![NumericAccumulator::default(); num_aggs],
                    counts: smallvec::smallvec![0; num_aggs],
                    min_values: smallvec::smallvec![None; num_aggs],
                    max_values: smallvec::smallvec![None; num_aggs],
                });
                current_group_count += 1;
                bucket.last_mut().unwrap()
            };

            // Accumulate aggregates
            for (i, agg) in simple_aggs.iter().enumerate() {
                match agg {
                    SimpleAgg::Count(_) => {
                        if agg.count_includes_row(row) {
                            state.counts[i] += 1;
                        }
                    }
                    SimpleAgg::Sum(sum_col_idx) | SimpleAgg::Avg(sum_col_idx) => {
                        if let Some(value) = row.get(*sum_col_idx) {
                            state.numeric_states[i].accumulate(value);
                        }
                    }
                    SimpleAgg::Min(min_col_idx) => {
                        if let Some(value) = row.get(*min_col_idx) {
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
                    SimpleAgg::Max(max_col_idx) => {
                        if let Some(value) = row.get(*max_col_idx) {
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
        let mut result_columns = Vec::with_capacity(1 + aggregations.len());

        // Single GROUP BY column name
        let group_col_name = match &group_by_items[0] {
            GroupByItem::Column(col_name) => col_name.clone(),
            _ => "col0".to_string(),
        };
        result_columns.push(group_col_name);

        // Add aggregate column names
        for agg in aggregations {
            let col_name = if let Some(ref alias) = agg.alias {
                alias.clone()
            } else {
                agg.get_expression_name()
            };
            result_columns.push(col_name);
        }

        // Build result rows with inline HAVING filter (supports AND combinations)
        let mut result_rows = RowVec::new();
        let mut row_id = 0i64;
        for mut state in groups.into_values().flatten() {
            // Apply inline HAVING filter
            if let Some(filter) = having_filter {
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
            let mut values: CompactVec<Value> = CompactVec::with_capacity(1 + simple_aggs.len());
            values.push(state.key_value);

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

    /// Execute grouped aggregation (with GROUP BY)
    ///
    /// Optimized version that:
    /// 1. Uses hash-based grouping instead of Vec<Value> keys
    /// 2. Pre-allocates aggregate functions once, resets per group
    /// 3. Pre-computes column indices for aggregate columns
    /// 4. Supports complex expressions in GROUP BY (e.g., function calls)
    ///
    /// When `limit` is provided (and there's no ORDER BY), enables early termination
    /// for streaming aggregation - stops creating new groups after limit is reached.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_grouped_aggregation(
        &self,
        aggregations: &[SqlAggregateFunction],
        group_by_items: &[GroupByItem],
        rows: &[(i64, Row)],
        columns: &[String],
        col_index_map: &StringMap<usize>,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        limit: Option<usize>,
    ) -> Result<(Vec<String>, RowVec, bool)> {
        // Keep recognizing the optimization shape, but deliberately evaluate
        // HAVING after aggregation. The inline representation is f64-based and
        // cannot preserve the exact INTEGER/DECIMAL comparison contract.
        let _inline_having_candidate = stmt
            .having
            .as_ref()
            .and_then(|having| try_parse_simple_having(having, aggregations));

        // FAST PATH: For simple aggregates (COUNT/SUM/AVG/MIN/MAX without DISTINCT/FILTER/ORDER BY/expression),
        // use single-pass streaming aggregation that accumulates values directly
        if let Some(result) = self.try_fast_aggregation(
            aggregations,
            group_by_items,
            rows,
            columns,
            col_index_map,
            limit,
            None,
        )? {
            // HAVING is evaluated by the ordinary post-aggregation path. Keeping
            // it there preserves exact Value comparison and propagates checked
            // SUM errors instead of reducing both sides to f64 in the fast path.
            return Ok((result.0, result.1, false));
        }

        // Check if any aggregation has an expression (e.g., SUM(val * 2)), ORDER BY, or FILTER
        let has_agg_expression = aggregations
            .iter()
            .any(|a| a.expression.is_some() || !a.order_by.is_empty() || a.filter.is_some());

        // Pre-compute aggregate column indices (once, not per row)
        // OPTIMIZATION: Use pre-computed column_lower instead of calling to_lowercase() each time
        // Handle both qualified (e.g., "o.amount") and unqualified column names
        let agg_col_indices: Vec<Option<usize>> = aggregations
            .iter()
            .map(|agg| {
                if agg.column == "*" || agg.expression.is_some() {
                    None // COUNT(*) and expressions don't use column index
                } else {
                    Self::lookup_column_index(&agg.column_lower, col_index_map)
                }
            })
            .collect();

        // Use hash-based grouping with collision handling: u64 hash -> Vec<GroupEntry>
        // Each hash bucket can contain multiple groups (handles hash collisions correctly)
        // Uses u64 keys for performance (8 bytes vs hundreds of bytes for Vec<Value>)
        // FxHashMap is optimized for trusted keys in embedded database context
        let mut groups: FxHashMap<u64, Vec<GroupEntry>> = FxHashMap::default();

        // Temporary buffer for computing group key hash (reused across rows)
        let mut key_buffer: Vec<Value> = Vec::with_capacity(group_by_items.len());

        // OPTIMIZATION: Pre-compute column indices for GROUP BY items to avoid to_lowercase() per row
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
                    // Use lookup_column_index to handle qualified names (e.g., "t.dept" -> "dept")
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

        // OPTIMIZATION: Check if we have any Expression GROUP BY items
        // If so, pre-compile expressions and use VM for evaluation
        let has_expr_group_by = precomputed_group_by
            .iter()
            .any(|item| matches!(item, PrecomputedGroupBy::Expression(_)));

        // Pre-compile GROUP BY expressions for VM-based evaluation
        // CRITICAL: Propagate errors instead of silently ignoring compilation failures
        use crate::expression::{compile_expression, ExecuteContext, ExprVM, SharedProgram};
        let compiled_group_by_exprs: Vec<Option<SharedProgram>> = precomputed_group_by
            .iter()
            .map(|item| match item {
                PrecomputedGroupBy::Expression(expr) => compile_expression(expr, columns).map(Some),
                _ => Ok(None),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut expr_vm = if has_expr_group_by || has_agg_expression {
            Some(ExprVM::new())
        } else {
            None
        };

        // OPTIMIZATION: Check if all GROUP BY items are simple column indices
        // In this case, we can hash directly from the row without cloning
        let all_simple_columns = precomputed_group_by.iter().all(|item| {
            matches!(
                item,
                PrecomputedGroupBy::ColumnIndex(_) | PrecomputedGroupBy::Position(_)
            )
        });

        // Track for early termination optimization
        let group_limit = limit.unwrap_or(usize::MAX);
        let has_limit = limit.is_some();
        let mut current_group_count: usize = 0; // Track actual group count for LIMIT optimization

        if all_simple_columns && expr_vm.is_none() {
            // Fast path: extract key values directly from row columns
            let column_indices: Vec<usize> = precomputed_group_by
                .iter()
                .map(|item| match item {
                    PrecomputedGroupBy::ColumnIndex(idx) => *idx,
                    PrecomputedGroupBy::Position(idx) => *idx,
                    _ => unreachable!(),
                })
                .collect();

            // OPTIMIZATION: Single-column GROUP BY uses direct hash map (no Vec<Value> overhead)
            if column_indices.len() == 1 {
                let col_idx = column_indices[0];
                // Use ValueMap for Value keys (HashDoS resistant with AHash)
                let mut single_col_groups: ValueMap<Vec<usize>> = ValueMap::default();

                for (row_idx, (_, row)) in rows.iter().enumerate() {
                    let key_value = row
                        .get(col_idx)
                        .cloned()
                        .unwrap_or_else(Value::null_unknown);

                    // Early termination: skip rows that would create new groups beyond the limit
                    if has_limit
                        && single_col_groups.len() >= group_limit
                        && !single_col_groups.contains_key(&key_value)
                    {
                        continue;
                    }

                    // Use entry API with proper Value equality
                    single_col_groups
                        .entry(key_value)
                        .or_default()
                        .push(row_idx);
                }

                // Convert to GroupEntry format for downstream processing
                for (key_value, row_indices) in single_col_groups {
                    groups
                        .entry(0) // Use dummy hash, we'll flatten anyway
                        .or_default()
                        .push(GroupEntry {
                            key_values: vec![key_value],
                            row_indices,
                        });
                }
            } else if column_indices.len() == 2 {
                // OPTIMIZATION: 2-column GROUP BY uses tuple keys instead of Vec<Value>
                // Tuples are 30% faster than Vec per CLAUDE.md (no heap allocation)
                let col_idx0 = column_indices[0];
                let col_idx1 = column_indices[1];
                // AHash for HashDoS resistance (user-controlled GROUP BY keys)
                let mut two_col_groups: ahash::AHashMap<(Value, Value), Vec<usize>> =
                    ahash::AHashMap::default();

                for (row_idx, (_, row)) in rows.iter().enumerate() {
                    let key = (
                        row.get(col_idx0)
                            .cloned()
                            .unwrap_or_else(Value::null_unknown),
                        row.get(col_idx1)
                            .cloned()
                            .unwrap_or_else(Value::null_unknown),
                    );

                    // Early termination: skip rows that would create new groups beyond the limit
                    if has_limit
                        && two_col_groups.len() >= group_limit
                        && !two_col_groups.contains_key(&key)
                    {
                        continue;
                    }

                    two_col_groups.entry(key).or_default().push(row_idx);
                }

                // Convert to GroupEntry format for downstream processing
                for ((v0, v1), row_indices) in two_col_groups {
                    groups
                        .entry(0) // Use dummy hash, we'll flatten anyway
                        .or_default()
                        .push(GroupEntry {
                            key_values: vec![v0, v1],
                            row_indices,
                        });
                }
            } else if column_indices.len() == 3 {
                // OPTIMIZATION: 3-column GROUP BY uses tuple keys (no Vec heap allocation)
                let col_idx0 = column_indices[0];
                let col_idx1 = column_indices[1];
                let col_idx2 = column_indices[2];
                let mut three_col_groups: ahash::AHashMap<(Value, Value, Value), Vec<usize>> =
                    ahash::AHashMap::default();

                for (row_idx, (_, row)) in rows.iter().enumerate() {
                    let key = (
                        row.get(col_idx0)
                            .cloned()
                            .unwrap_or_else(Value::null_unknown),
                        row.get(col_idx1)
                            .cloned()
                            .unwrap_or_else(Value::null_unknown),
                        row.get(col_idx2)
                            .cloned()
                            .unwrap_or_else(Value::null_unknown),
                    );

                    // Early termination: skip rows that would create new groups beyond the limit
                    if has_limit
                        && three_col_groups.len() >= group_limit
                        && !three_col_groups.contains_key(&key)
                    {
                        continue;
                    }

                    three_col_groups.entry(key).or_default().push(row_idx);
                }

                for ((v0, v1, v2), row_indices) in three_col_groups {
                    groups.entry(0).or_default().push(GroupEntry {
                        key_values: vec![v0, v1, v2],
                        row_indices,
                    });
                }
            } else {
                // 4+ columns: use AHashMap<Vec<Value>> directly (no collision handling needed)
                let mut multi_col_groups: ahash::AHashMap<Vec<Value>, Vec<usize>> =
                    ahash::AHashMap::default();

                for (row_idx, (_, row)) in rows.iter().enumerate() {
                    key_buffer.clear();
                    for &idx in &column_indices {
                        key_buffer.push(row.get(idx).cloned().unwrap_or_else(Value::null_unknown));
                    }

                    // Early termination: skip rows that would create new groups beyond the limit
                    if has_limit
                        && multi_col_groups.len() >= group_limit
                        && !multi_col_groups.contains_key(&key_buffer)
                    {
                        continue;
                    }

                    multi_col_groups
                        .entry(key_buffer.clone())
                        .or_default()
                        .push(row_idx);
                }

                for (key_values, row_indices) in multi_col_groups {
                    groups.entry(0).or_default().push(GroupEntry {
                        key_values,
                        row_indices,
                    });
                }
            }
        } else {
            // Slow path: need to evaluate expressions, use buffer
            for (row_idx, (_, row)) in rows.iter().enumerate() {
                key_buffer.clear();

                // Create execution context for this row
                // CRITICAL: Include params for parameterized queries
                let exec_ctx = ExecuteContext::new(row)
                    .with_params(ctx.params())
                    .with_named_params(ctx.named_params())
                    .with_transaction_id(ctx.transaction_id())
                    .with_stored_function_invoker(ctx.stored_function_invoker());

                for (i, item) in precomputed_group_by.iter().enumerate() {
                    let value = match item {
                        PrecomputedGroupBy::ColumnIndex(idx) => {
                            row.get(*idx).cloned().unwrap_or_else(Value::null_unknown)
                        }
                        PrecomputedGroupBy::Position(idx) => {
                            row.get(*idx).cloned().unwrap_or_else(Value::null_unknown)
                        }
                        PrecomputedGroupBy::Expression(_) => {
                            // Use pre-compiled expression with VM
                            if let (Some(ref mut vm), Some(ref program)) =
                                (&mut expr_vm, &compiled_group_by_exprs[i])
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

                // Compute hash of key (8-byte key for fast lookups)
                let hash = hash_group_key(&key_buffer);

                // OPTIMIZATION: Single scan to find existing group OR check limit
                // Previously we scanned twice: once for key_exists check, once for find()
                match groups.entry(hash) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        let bucket = e.get_mut();
                        // Single scan: find position of matching group
                        let existing_idx = bucket
                            .iter()
                            .position(|entry| entry.key_values == key_buffer);

                        if let Some(idx) = existing_idx {
                            // Existing group - just add this row to it
                            bucket[idx].row_indices.push(row_idx);
                        } else {
                            // Hash collision: different key with same hash
                            // Check limit before creating new group
                            if has_limit && current_group_count >= group_limit {
                                continue;
                            }
                            bucket.push(GroupEntry {
                                key_values: key_buffer.clone(),
                                row_indices: vec![row_idx],
                            });
                            current_group_count += 1;
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        // First entry for this hash - check limit before creating
                        if has_limit && current_group_count >= group_limit {
                            continue;
                        }
                        e.insert(vec![GroupEntry {
                            key_values: key_buffer.clone(),
                            row_indices: vec![row_idx],
                        }]);
                        current_group_count += 1;
                    }
                }
            }
        }

        // Convert groups to Vec for parallel processing
        // Flatten buckets: each bucket may contain multiple groups (hash collisions)
        let groups_vec: Vec<GroupEntry> = groups.into_values().flatten().collect();

        // Pre-compile aggregate filter and expression programs for VM-based evaluation
        // CRITICAL: Propagate errors instead of silently ignoring compilation failures
        let compiled_agg_filters: Vec<Option<SharedProgram>> = if has_agg_expression {
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

        // Pre-compile ORDER BY expressions for each aggregation
        // CRITICAL: Propagate errors instead of silently ignoring compilation failures
        let compiled_agg_order_by: Vec<Vec<SharedProgram>> = if has_agg_expression {
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

        // Determine if parallel processing is beneficial
        // Don't use parallel processing when expressions are involved (harder to handle)
        // Key insight: parallel creates aggregate functions PER GROUP, so for many small groups
        // (e.g., 10k groups with 3 rows each), the allocation overhead dominates.
        // Only parallelize when groups are large enough to amortize the allocation cost.
        #[cfg(feature = "parallel")]
        let total_rows: usize = groups_vec.iter().map(|g| g.row_indices.len()).sum();
        #[cfg(feature = "parallel")]
        let avg_rows_per_group = total_rows / groups_vec.len().max(1);
        #[cfg(feature = "parallel")]
        let use_parallel = groups_vec.len() >= 4
            && total_rows >= 10_000
            && avg_rows_per_group >= 50
            && !has_agg_expression
            && aggregations.iter().all(|aggregate| {
                matches!(aggregate.name.as_str(), "COUNT" | "SUM" | "MIN" | "MAX")
            });
        #[cfg(not(feature = "parallel"))]
        let use_parallel = false;

        // Process groups (parallel or sequential based on data size)
        let result_rows: RowVec = if use_parallel {
            // PARALLEL: Process each group independently using Rayon
            let function_registry = &self.host.aggregation_function_registry();

            #[cfg(feature = "parallel")]
            let rows_vec: Vec<Row> = groups_vec
                .into_par_iter()
                .map(|group| -> Result<Row> {
                    // Each thread creates its own aggregate functions
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

                    // Accumulate values for this group
                    // Pre-create static Value for COUNT(*)
                    let count_star_value = Value::Integer(1);
                    for &row_idx in &group.row_indices {
                        let (_, row) = &rows[row_idx];
                        for (i, agg) in aggregations.iter().enumerate() {
                            if let Some(ref mut func) = agg_funcs[i] {
                                // OPTIMIZATION: Avoid cloning by using reference directly
                                let value_ref = if let Some(col_idx) = agg_col_indices[i] {
                                    row.get(col_idx)
                                } else {
                                    Some(&count_star_value)
                                };
                                if let Some(v) = value_ref {
                                    func.accumulate(v, agg.distinct);
                                }
                            }
                        }
                    }

                    // Build result row
                    // Use CompactVec directly to avoid Vec→CompactVec conversion
                    let mut row_values: CompactVec<Value> =
                        CompactVec::with_capacity(group_by_items.len() + aggregations.len());
                    row_values.extend(group.key_values);

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

                    Ok(Row::from_compact_vec(row_values))
                })
                .collect::<Result<Vec<_>>>()?;
            #[cfg(not(feature = "parallel"))]
            let rows_vec: Vec<Row> = groups_vec
                .into_iter()
                .map(|group| -> Result<Row> {
                    // Each thread creates its own aggregate functions
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

                    // Accumulate values for this group
                    // Pre-create static Value for COUNT(*)
                    let count_star_value = Value::Integer(1);
                    for &row_idx in &group.row_indices {
                        let (_, row) = &rows[row_idx];
                        for (i, agg) in aggregations.iter().enumerate() {
                            if let Some(ref mut func) = agg_funcs[i] {
                                // OPTIMIZATION: Avoid cloning by using reference directly
                                let value_ref = if let Some(col_idx) = agg_col_indices[i] {
                                    row.get(col_idx)
                                } else {
                                    Some(&count_star_value)
                                };
                                if let Some(v) = value_ref {
                                    func.accumulate(v, agg.distinct);
                                }
                            }
                        }
                    }

                    // Build result row
                    // Use CompactVec directly to avoid Vec→CompactVec conversion
                    let mut row_values: CompactVec<Value> =
                        CompactVec::with_capacity(group_by_items.len() + aggregations.len());
                    row_values.extend(group.key_values);

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

                    Ok(Row::from_compact_vec(row_values))
                })
                .collect::<Result<Vec<_>>>()?;
            // Convert to RowVec with sequential IDs
            rows_vec
                .into_iter()
                .enumerate()
                .map(|(idx, row)| (idx as i64, row))
                .collect()
        } else {
            // SEQUENTIAL: For small datasets, avoid parallel overhead, or when expressions are involved
            let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = aggregations
                .iter()
                .map(|agg| {
                    self.host
                        .aggregation_function_registry()
                        .get_aggregate(&agg.name)
                })
                .collect();

            // Configure aggregate functions with extra arguments (e.g., separator for STRING_AGG)
            // This is done once, not per group, as configuration persists across resets
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

            // Buffer for evaluated expression values (to avoid repeated allocation)
            let mut expr_values: Vec<Value> = vec![Value::null_unknown(); aggregations.len()];

            let mut result_rows_seq = RowVec::with_capacity(groups_vec.len());
            let mut row_id = 0i64;
            for group in groups_vec {
                // Reset aggregate functions for this group
                for f in agg_funcs.iter_mut().flatten() {
                    f.reset();
                }

                // Accumulate values for this group
                // Pre-create static Value for COUNT(*)
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
                            if let Some(ref filter_program) = compiled_agg_filters[i] {
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

                            // Check if this aggregate has an expression to evaluate
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
                                if !compiled_agg_order_by[i].is_empty() && func.supports_order_by()
                                {
                                    // Evaluate ORDER BY expressions to get sort keys using pre-compiled programs
                                    if let Some(ref mut vm) = expr_vm {
                                        let mut sort_keys =
                                            Vec::with_capacity(compiled_agg_order_by[i].len());
                                        for order_program in &compiled_agg_order_by[i] {
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

                // Build result row
                // Use CompactVec directly to avoid Vec→CompactVec conversion
                let mut row_values: CompactVec<Value> =
                    CompactVec::with_capacity(group_by_items.len() + aggregations.len());
                row_values.extend(group.key_values);

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

                result_rows_seq.push((row_id, Row::from_compact_vec(row_values)));
                row_id += 1;
            }
            result_rows_seq
        };

        // Build result columns
        let mut result_columns: Vec<String> =
            self.resolve_group_by_column_names_new(group_by_items, columns, col_index_map);
        result_columns.extend(aggregations.iter().map(|a| a.get_column_name()));

        // Slow path doesn't apply HAVING inline, so return false
        Ok((result_columns, result_rows, false))
    }
}
