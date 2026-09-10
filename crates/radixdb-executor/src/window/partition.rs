use super::*;

impl<'host, H: WindowHost + ?Sized> WindowExecutor<'host, H> {
    /// Compute window function values for all rows
    /// pre_sorted: Optional info about whether rows are already sorted by an indexed column
    /// pre_grouped: Optional pre-grouped partitions from index (avoids hash-based grouping)
    /// order_by_cache: Precomputed ORDER BY values cache (keyed by semantic string representation)
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_window_function(
        &self,
        wf_info: &WindowFunctionInfo,
        rows: &[(i64, Row)],
        columns: &[String],
        col_index_map: &StringMap<usize>,
        ctx: &ExecutionContext,
        pre_sorted: Option<&WindowPreSortedState>,
        pre_grouped: Option<&WindowPreGroupedState>,
        order_by_cache: &[(String, ColumnarOrderByValues)],
    ) -> Result<Vec<Value>> {
        // Check if this is an aggregate function used as window function
        let is_aggregate = self
            .host
            .window_function_registry()
            .is_aggregate(&wf_info.name);

        if is_aggregate {
            // Handle aggregate functions as window functions (SUM, COUNT, AVG, etc.)
            // Look up precomputed ORDER BY values from cache using semantic key
            let cache_key = Self::order_by_cache_key(&wf_info.order_by);
            let cached_order_by = order_by_cache
                .iter()
                .find(|(key, _)| key == &cache_key)
                .map(|(_, v)| v);
            return self.compute_aggregate_window_function(
                wf_info,
                rows,
                columns,
                col_index_map,
                ctx,
                pre_sorted,
                cached_order_by,
            );
        }

        // Get the window function from registry
        let window_func = self
            .host
            .window_function_registry()
            .get_window(&wf_info.name)
            .ok_or_else(|| {
                Error::NotSupported(format!("Unknown window function: {}", wf_info.name))
            })?;

        // Look up precomputed ORDER BY values from cache using semantic key
        let precomputed_order_by: Option<&ColumnarOrderByValues> = if !wf_info.order_by.is_empty() {
            let cache_key = Self::order_by_cache_key(&wf_info.order_by);
            order_by_cache
                .iter()
                .find(|(key, _)| key == &cache_key)
                .map(|(_, v)| v)
        } else {
            None
        };

        // If there's no partitioning, treat all rows as one partition
        if wf_info.partition_by_exprs.is_empty() {
            // Pre-allocate results array and use direct-writing variant
            let mut results: Vec<Value> = vec![NULL_VALUE; rows.len()];
            let row_indices: Vec<usize> = (0..rows.len()).collect();

            self.compute_window_for_partition_direct(
                &*window_func,
                wf_info,
                rows,
                row_indices,
                precomputed_order_by,
                columns,
                col_index_map,
                ctx,
                &mut results,
            )?;

            return Ok(results);
        }

        // Group rows by partition key
        // OPTIMIZATION: Use pre-grouped partitions from index if available (avoids O(n) hashing)
        // Only valid when this WF partitions by the exact same single simple column that
        // the planner used to build the pre-grouped map.
        let partitions: FxHashMap<PartitionKey, Vec<usize>> = if let Some(pg) =
            pre_grouped.filter(|pg| {
                wf_info.partition_by.len() == 1
                    && wf_info.partition_by.len() == wf_info.partition_by_exprs.len()
                    && wf_info.partition_by[0].to_lowercase() == pg.partition_column
            }) {
            pg.partition_map.clone()
        } else {
            Self::build_partition_map(wf_info, rows, columns, col_index_map, ctx)?
        };

        // Compute window function for each partition
        // Use parallel execution for large number of partitions
        let partition_count = partitions.len();
        let use_parallel = partition_count >= 10 && rows.len() >= 1000;

        if use_parallel {
            // Compute each disjoint partition independently, then publish its
            // values into the final row order. This keeps the parallel owner
            // safe without sharing a mutable raw-pointer facade across workers.
            let partitions_vec: Vec<_> = partitions.into_iter().collect();
            #[cfg(feature = "parallel")]
            let partition_results: Result<Vec<_>> = partitions_vec
                .par_iter()
                .map(|(_key, row_indices)| {
                    self.compute_window_for_partition(
                        &*window_func,
                        wf_info,
                        rows,
                        row_indices.clone(),
                        precomputed_order_by,
                        columns,
                        col_index_map,
                        ctx,
                        false,
                    )
                })
                .collect();
            #[cfg(not(feature = "parallel"))]
            let partition_results: Result<Vec<_>> = partitions_vec
                .iter()
                .map(|(_key, row_indices)| {
                    self.compute_window_for_partition(
                        &*window_func,
                        wf_info,
                        rows,
                        row_indices.clone(),
                        precomputed_order_by,
                        columns,
                        col_index_map,
                        ctx,
                        false,
                    )
                })
                .collect();

            let mut results = vec![NULL_VALUE; rows.len()];
            for (values, row_indices) in partition_results? {
                for (value, row_index) in values.into_iter().zip(row_indices) {
                    results[row_index] = value;
                }
            }
            Ok(results)
        } else {
            // Sequential execution for small partition counts
            // MEMORY OPTIMIZATION: Write directly to final results array instead of
            // creating per-partition Vec<Value> and then mapping back
            let mut results: Vec<Value> = vec![NULL_VALUE; rows.len()];

            for (_key, row_indices) in partitions {
                // MEMORY OPTIMIZATION: Direct writing variant - writes to results in-place
                self.compute_window_for_partition_direct(
                    &*window_func,
                    wf_info,
                    rows,
                    row_indices,
                    precomputed_order_by,
                    columns,
                    col_index_map,
                    ctx,
                    &mut results,
                )?;
            }

            Ok(results)
        }
    }

    /// Compute window function for a single partition
    /// Returns (results in sorted order, sorted row indices) to avoid re-sorting in the caller
    /// precomputed_order_by: Optional precomputed ORDER BY values for ALL rows (avoids recomputation)
    /// skip_sorting: If true, skip sorting (rows are already pre-sorted by index)
    ///
    /// MEMORY OPTIMIZATION: This function uses index-based value access instead of cloning
    /// values into intermediate Vec<Value> collections. For LEAD/LAG/FIRST_VALUE/LAST_VALUE,
    /// values are accessed directly from all_rows via indices. For RANK/DENSE_RANK, we use
    /// ColumnarOrderByValues::rows_equal() to compare ORDER BY values without cloning.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_window_for_partition(
        &self,
        window_func: &dyn WindowFunction,
        wf_info: &WindowFunctionInfo,
        all_rows: &[(i64, Row)],
        mut row_indices: Vec<usize>,
        precomputed_order_by: Option<&ColumnarOrderByValues>,
        columns: &[String],
        _col_index_map: &StringMap<usize>,
        ctx: &ExecutionContext,
        skip_sorting: bool,
    ) -> Result<(Vec<Value>, Vec<usize>)> {
        // Suppress unused variable warnings - these are needed for compute_lead_lag, compute_ntile, etc.
        let _ = columns;

        // Empty fallback for when no precomputed values are provided
        let empty_order_by = ColumnarOrderByValues {
            columns: vec![],
            ascending: vec![],
            nulls_first: vec![],
            num_rows: 0,
        };
        let order_by_values = precomputed_order_by.unwrap_or(&empty_order_by);

        // Sort partition by ORDER BY if specified (skip if already pre-sorted by index)
        if !skip_sorting && !wf_info.order_by.is_empty() && !order_by_values.is_empty() {
            Self::sort_by_order_values(&mut row_indices, order_by_values);
        }

        let navigation_values =
            self.precompute_navigation_values(wf_info, all_rows, columns, ctx)?;

        // MEMORY OPTIMIZATION: Compute rank info directly from ColumnarOrderByValues
        // instead of cloning ORDER BY values into a Vec<Value>
        // This uses rows_equal() for O(n) comparison without allocating order_values
        let is_rank_function = matches!(wf_info.name.as_str(), "RANK" | "DENSE_RANK");
        let is_rank = wf_info.name == "RANK";
        let rank_info = if is_rank_function && !order_by_values.is_empty() {
            Self::precompute_rank_info_columnar(&row_indices, order_by_values)
        } else {
            vec![]
        };

        // Compute window function for each row in the partition
        let mut results = Vec::with_capacity(row_indices.len());
        let partition_len = row_indices.len();

        // Precompute peer group ends for RANGE frame semantics
        let peer_groups = if !wf_info.order_by.is_empty() && !order_by_values.is_empty() {
            Self::precompute_peer_group_bounds(&row_indices, order_by_values)
        } else {
            vec![(0, partition_len); partition_len]
        };

        for (i, &row_idx) in row_indices.iter().enumerate() {
            // Handle special functions
            let value = match wf_info.name.as_str() {
                // MEMORY OPTIMIZATION: Access values directly from all_rows via indices
                // instead of cloning into partition_values Vec
                "LEAD" | "LAG" => self.compute_lead_lag_indexed(
                    wf_info,
                    all_rows,
                    &row_indices,
                    navigation_values.as_deref(),
                    i,
                    &all_rows[row_idx].1,
                    columns,
                    ctx,
                )?,
                "NTILE" => self.compute_ntile(wf_info, row_indices.len(), i, ctx)?,
                "RANK" | "DENSE_RANK" => Self::compute_rank_fast(is_rank, &rank_info, i),
                "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                    // Compute frame bounds for navigation functions
                    let peer_group = if i < peer_groups.len() {
                        peer_groups[i]
                    } else {
                        (0, partition_len)
                    };
                    let (frame_start, frame_end) = self.compute_simple_frame_bounds(
                        wf_info,
                        i,
                        partition_len,
                        peer_group,
                        &row_indices,
                        order_by_values,
                    )?;

                    // MEMORY OPTIMIZATION: Access values directly via indices
                    match wf_info.name.as_str() {
                        "FIRST_VALUE" => self.compute_first_value_indexed(
                            all_rows,
                            &row_indices,
                            navigation_values.as_deref(),
                            frame_start,
                            frame_end,
                        )?,
                        "LAST_VALUE" => self.compute_last_value_indexed(
                            all_rows,
                            &row_indices,
                            navigation_values.as_deref(),
                            frame_start,
                            frame_end,
                        )?,
                        "NTH_VALUE" => self.compute_nth_value_indexed(
                            wf_info,
                            all_rows,
                            &row_indices,
                            navigation_values.as_deref(),
                            frame_start,
                            frame_end,
                            ctx,
                        )?,
                        _ => unreachable!(),
                    }
                }
                "PERCENT_RANK" => Self::compute_percent_rank_fast(&peer_groups, partition_len, i),
                "CUME_DIST" => Self::compute_cume_dist_fast(&peer_groups, partition_len, i),
                "ROW_NUMBER" => Value::Integer(
                    i64::try_from(
                        i.checked_add(1).ok_or_else(|| {
                            Error::invalid_argument("ROW_NUMBER position overflow")
                        })?,
                    )
                    .map_err(|_| Error::invalid_argument("ROW_NUMBER result exceeds INTEGER"))?,
                ),
                _ => window_func.process(&[], &[], i)?,
            };
            results.push(value);
        }

        Ok((results, row_indices))
    }

    /// Direct-writing variant of compute_window_for_partition
    /// Writes results directly to the provided results array, avoiding per-partition Vec allocation
    ///
    /// MEMORY OPTIMIZATION: Instead of returning (Vec<Value>, Vec<usize>) and mapping back,
    /// this writes directly to results[orig_idx] for each computed value.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_window_for_partition_direct(
        &self,
        window_func: &dyn WindowFunction,
        wf_info: &WindowFunctionInfo,
        all_rows: &[(i64, Row)],
        mut row_indices: Vec<usize>,
        precomputed_order_by: Option<&ColumnarOrderByValues>,
        columns: &[String],
        _col_index_map: &StringMap<usize>,
        ctx: &ExecutionContext,
        results: &mut [Value],
    ) -> Result<()> {
        let _ = columns;

        // Empty fallback for when no precomputed values are provided
        let empty_order_by = ColumnarOrderByValues {
            columns: vec![],
            ascending: vec![],
            nulls_first: vec![],
            num_rows: 0,
        };
        let order_by_values = precomputed_order_by.unwrap_or(&empty_order_by);

        // Sort partition by ORDER BY if specified
        if !wf_info.order_by.is_empty() && !order_by_values.is_empty() {
            Self::sort_by_order_values(&mut row_indices, order_by_values);
        }

        let navigation_values =
            self.precompute_navigation_values(wf_info, all_rows, columns, ctx)?;

        // Compute rank info for RANK/DENSE_RANK
        let is_rank_function = matches!(wf_info.name.as_str(), "RANK" | "DENSE_RANK");
        let is_rank = wf_info.name == "RANK";
        let rank_info = if is_rank_function && !order_by_values.is_empty() {
            Self::precompute_rank_info_columnar(&row_indices, order_by_values)
        } else {
            vec![]
        };

        let partition_len = row_indices.len();

        // Precompute peer group ends for RANGE frame semantics
        let peer_groups = if !wf_info.order_by.is_empty() && !order_by_values.is_empty() {
            Self::precompute_peer_group_bounds(&row_indices, order_by_values)
        } else {
            vec![(0, partition_len); partition_len]
        };

        // Compute and write directly to results array
        for (i, &row_idx) in row_indices.iter().enumerate() {
            let value = match wf_info.name.as_str() {
                "LEAD" | "LAG" => self.compute_lead_lag_indexed(
                    wf_info,
                    all_rows,
                    &row_indices,
                    navigation_values.as_deref(),
                    i,
                    &all_rows[row_idx].1,
                    columns,
                    ctx,
                )?,
                "NTILE" => self.compute_ntile(wf_info, partition_len, i, ctx)?,
                "RANK" | "DENSE_RANK" => Self::compute_rank_fast(is_rank, &rank_info, i),
                "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                    let peer_group = if i < peer_groups.len() {
                        peer_groups[i]
                    } else {
                        (0, partition_len)
                    };
                    let (frame_start, frame_end) = self.compute_simple_frame_bounds(
                        wf_info,
                        i,
                        partition_len,
                        peer_group,
                        &row_indices,
                        order_by_values,
                    )?;
                    match wf_info.name.as_str() {
                        "FIRST_VALUE" => self.compute_first_value_indexed(
                            all_rows,
                            &row_indices,
                            navigation_values.as_deref(),
                            frame_start,
                            frame_end,
                        )?,
                        "LAST_VALUE" => self.compute_last_value_indexed(
                            all_rows,
                            &row_indices,
                            navigation_values.as_deref(),
                            frame_start,
                            frame_end,
                        )?,
                        "NTH_VALUE" => self.compute_nth_value_indexed(
                            wf_info,
                            all_rows,
                            &row_indices,
                            navigation_values.as_deref(),
                            frame_start,
                            frame_end,
                            ctx,
                        )?,
                        _ => unreachable!(),
                    }
                }
                "PERCENT_RANK" => Self::compute_percent_rank_fast(&peer_groups, partition_len, i),
                "CUME_DIST" => Self::compute_cume_dist_fast(&peer_groups, partition_len, i),
                "ROW_NUMBER" => Value::Integer(
                    i64::try_from(
                        i.checked_add(1).ok_or_else(|| {
                            Error::invalid_argument("ROW_NUMBER position overflow")
                        })?,
                    )
                    .map_err(|_| Error::invalid_argument("ROW_NUMBER result exceeds INTEGER"))?,
                ),
                _ => window_func.process(&[], &[], i)?,
            };
            // DIRECT WRITE: Write to original row position, avoiding the mapping step
            results[row_idx] = value;
        }

        Ok(())
    }

    /// Precompute rank information using ColumnarOrderByValues directly (no cloning)
    ///
    /// Returns a vector of (group_start, dense_rank) for each position in sorted_indices.
    /// Uses rows_equal() for O(n) comparison without allocating intermediate Vec<Value>.
    #[inline]
    pub(super) fn precompute_rank_info_columnar(
        sorted_indices: &[usize],
        order_by: &ColumnarOrderByValues,
    ) -> Vec<(usize, i64)> {
        let n = sorted_indices.len();
        if n == 0 {
            return vec![];
        }

        let mut result = Vec::with_capacity(n);

        // First row: group starts at 0, dense_rank = 1
        result.push((0, 1));

        let mut current_group_start = 0;
        let mut current_dense_rank: i64 = 1;

        for i in 1..n {
            // Compare ORDER BY values of adjacent sorted rows without cloning
            if !order_by.rows_equal(sorted_indices[i - 1], sorted_indices[i]) {
                // New group starts
                current_group_start = i;
                current_dense_rank += 1;
            }
            result.push((current_group_start, current_dense_rank));
        }

        result
    }

    /// Precompute peer-group start/end positions for RANGE and rank semantics.
    pub(super) fn precompute_peer_group_bounds(
        sorted_indices: &[usize],
        order_by: &ColumnarOrderByValues,
    ) -> Vec<(usize, usize)> {
        let n = sorted_indices.len();
        if n == 0 {
            return vec![];
        }
        let mut bounds = vec![(0, n); n];
        let mut group_start = 0;
        for i in 1..n {
            if !order_by.rows_equal(sorted_indices[i - 1], sorted_indices[i]) {
                for bound in bounds.iter_mut().take(i).skip(group_start) {
                    *bound = (group_start, i);
                }
                group_start = i;
            }
        }
        for bound in bounds.iter_mut().take(n).skip(group_start) {
            *bound = (group_start, n);
        }
        bounds
    }

    pub(super) fn precompute_navigation_values(
        &self,
        wf_info: &WindowFunctionInfo,
        all_rows: &[(i64, Row)],
        columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<Option<Vec<Value>>> {
        if !matches!(
            wf_info.name.as_str(),
            "LEAD" | "LAG" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE"
        ) || wf_info.arguments.is_empty()
        {
            return Ok(None);
        }

        let mut eval = ExpressionEval::compile(&wf_info.arguments[0], columns)?.with_context(ctx);
        all_rows
            .iter()
            .map(|(_, row)| eval.eval(row))
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }

    /// Compute LEAD or LAG using index-based access (no cloning)
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_lead_lag_indexed(
        &self,
        wf_info: &WindowFunctionInfo,
        _all_rows: &[(i64, Row)],
        sorted_indices: &[usize],
        argument_values: Option<&[Value]>,
        current_pos: usize,
        current_row_data: &Row,
        columns: &[String],
        ctx: &ExecutionContext,
    ) -> Result<Value> {
        // Get offset (default 1)
        let offset = if wf_info.arguments.len() > 1 {
            let mut eval = ExpressionEval::compile(&wf_info.arguments[1], &[])?.with_context(ctx);
            match eval.eval_slice(&Row::new())? {
                Value::Integer(n) => n as usize,
                _ => 1,
            }
        } else {
            1
        };

        // Get default value - use current row context for column references
        let default_value = if wf_info.arguments.len() > 2 {
            let mut eval =
                ExpressionEval::compile(&wf_info.arguments[2], columns)?.with_context(ctx);
            eval.eval(current_row_data)?
        } else {
            Value::null_unknown()
        };

        // Calculate target position in sorted order
        let target_pos = if wf_info.name == "LEAD" {
            current_pos.checked_add(offset)
        } else {
            // LAG
            current_pos.checked_sub(offset)
        };

        match target_pos {
            Some(pos) if pos < sorted_indices.len() => {
                // Access value directly from all_rows via index
                let target_row_idx = sorted_indices[pos];
                if let Some(values) = argument_values {
                    Ok(values
                        .get(target_row_idx)
                        .cloned()
                        .unwrap_or_else(Value::null_unknown))
                } else {
                    Ok(default_value)
                }
            }
            _ => Ok(default_value),
        }
    }

    /// Compute FIRST_VALUE using index-based access (no cloning)
    pub(super) fn compute_first_value_indexed(
        &self,
        _all_rows: &[(i64, Row)],
        sorted_indices: &[usize],
        argument_values: Option<&[Value]>,
        frame_start: usize,
        frame_end: usize,
    ) -> Result<Value> {
        if frame_start >= frame_end || frame_start >= sorted_indices.len() {
            return Ok(Value::null_unknown());
        }
        let row_idx = sorted_indices[frame_start];
        if let Some(values) = argument_values {
            Ok(values
                .get(row_idx)
                .cloned()
                .unwrap_or_else(Value::null_unknown))
        } else {
            Ok(Value::null_unknown())
        }
    }

    /// Compute LAST_VALUE using index-based access (no cloning)
    pub(super) fn compute_last_value_indexed(
        &self,
        _all_rows: &[(i64, Row)],
        sorted_indices: &[usize],
        argument_values: Option<&[Value]>,
        frame_start: usize,
        frame_end: usize,
    ) -> Result<Value> {
        if frame_start >= frame_end || frame_end > sorted_indices.len() {
            return Ok(Value::null_unknown());
        }
        let row_idx = sorted_indices[frame_end - 1];
        if let Some(values) = argument_values {
            Ok(values
                .get(row_idx)
                .cloned()
                .unwrap_or_else(Value::null_unknown))
        } else {
            Ok(Value::null_unknown())
        }
    }

    /// Compute NTH_VALUE using index-based access (no cloning)
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compute_nth_value_indexed(
        &self,
        wf_info: &WindowFunctionInfo,
        _all_rows: &[(i64, Row)],
        sorted_indices: &[usize],
        argument_values: Option<&[Value]>,
        frame_start: usize,
        frame_end: usize,
        ctx: &ExecutionContext,
    ) -> Result<Value> {
        if frame_start >= frame_end {
            return Ok(Value::null_unknown());
        }

        // Get n (1-indexed position) from second argument
        let n = if wf_info.arguments.len() > 1 {
            let mut eval = ExpressionEval::compile(&wf_info.arguments[1], &[])?.with_context(ctx);
            match eval.eval_slice(&Row::new())? {
                Value::Integer(n) if n > 0 => n as usize,
                _ => return Ok(Value::null_unknown()),
            }
        } else {
            return Ok(Value::null_unknown());
        };

        // n is 1-indexed within the frame
        let frame_len = frame_end - frame_start;
        if n > frame_len {
            return Ok(Value::null_unknown());
        }

        let pos_in_frame = n - 1;
        let row_idx = sorted_indices[frame_start + pos_in_frame];
        if let Some(values) = argument_values {
            Ok(values
                .get(row_idx)
                .cloned()
                .unwrap_or_else(Value::null_unknown))
        } else {
            Ok(Value::null_unknown())
        }
    }

    /// Compute PERCENT_RANK using ColumnarOrderByValues (no cloning)
    /// PERCENT_RANK = (rank - 1) / (total_rows - 1)
    pub(super) fn compute_percent_rank_fast(
        peer_groups: &[(usize, usize)],
        partition_len: usize,
        current_pos: usize,
    ) -> Value {
        if partition_len <= 1 {
            return Value::Float(0.0);
        }
        let group_start = peer_groups
            .get(current_pos)
            .map_or(current_pos, |bounds| bounds.0);
        Value::Float(group_start as f64 / (partition_len - 1) as f64)
    }

    /// Compute CUME_DIST using ColumnarOrderByValues (no cloning)
    /// CUME_DIST = (number of rows with value <= current) / total_rows
    pub(super) fn compute_cume_dist_fast(
        peer_groups: &[(usize, usize)],
        partition_len: usize,
        current_pos: usize,
    ) -> Value {
        if partition_len == 0 {
            return Value::Float(1.0);
        }
        let group_end = peer_groups
            .get(current_pos)
            .map_or(current_pos + 1, |bounds| bounds.1);
        Value::Float(group_end as f64 / partition_len as f64)
    }

    /// Compute NTILE function
    pub(super) fn compute_ntile(
        &self,
        wf_info: &WindowFunctionInfo,
        partition_size: usize,
        current_row: usize,
        ctx: &ExecutionContext,
    ) -> Result<Value> {
        // Get n (number of buckets)
        let n = if !wf_info.arguments.is_empty() {
            let mut eval = ExpressionEval::compile(&wf_info.arguments[0], &[])?.with_context(ctx);
            match eval.eval_slice(&Row::new())? {
                Value::Integer(n) if n > 0 => n as usize,
                value => {
                    return Err(Error::InvalidArgument(format!(
                        "NTILE argument must be a positive INTEGER, got {}",
                        value
                    )))
                }
            }
        } else {
            return Err(Error::InvalidArgument(
                "NTILE requires exactly one argument".to_string(),
            ));
        };

        // NTILE divides rows into n buckets as evenly as possible.
        // If partition_size doesn't divide evenly by n, the first (partition_size % n)
        // buckets get one extra row.
        //
        // For example, NTILE(3) with 7 rows:
        // - base_size = 7 / 3 = 2 (rows per bucket)
        // - remainder = 7 % 3 = 1 (1 bucket gets an extra row)
        // - Bucket 1: rows 0, 1, 2 (3 rows - gets extra)
        // - Bucket 2: rows 3, 4 (2 rows)
        // - Bucket 3: rows 5, 6 (2 rows)

        if n >= partition_size {
            // More buckets than rows - each row gets its own bucket
            return Ok(Value::Integer((current_row + 1).min(n) as i64));
        }

        let base_size = partition_size / n;
        let remainder = partition_size % n;

        // Calculate which bucket this row belongs to
        // First 'remainder' buckets have (base_size + 1) rows
        // Remaining buckets have base_size rows
        let bucket = if current_row < remainder * (base_size + 1) {
            // Row is in one of the larger buckets
            current_row / (base_size + 1) + 1
        } else {
            // Row is in one of the smaller buckets
            let rows_in_larger_buckets = remainder * (base_size + 1);
            let row_in_smaller_section = current_row - rows_in_larger_buckets;
            remainder + row_in_smaller_section / base_size + 1
        };

        Ok(Value::Integer(bucket as i64))
    }

    /// Compute RANK or DENSE_RANK function using precomputed rank info.
    ///
    /// This is an O(1) lookup using the precomputed (group_start, dense_rank) tuple.
    #[inline]
    pub(super) fn compute_rank_fast(
        is_rank: bool,
        rank_info: &[(usize, i64)],
        current_row: usize,
    ) -> Value {
        if rank_info.is_empty() || current_row >= rank_info.len() {
            return Value::Integer(1);
        }

        let (group_start, dense_rank) = rank_info[current_row];

        if is_rank {
            // RANK: 1-indexed position of first row in group
            Value::Integer((group_start + 1) as i64)
        } else {
            // DENSE_RANK: sequential group number
            Value::Integer(dense_rank)
        }
    }

    /// Compute frame bounds for navigation functions (FIRST_VALUE, LAST_VALUE, NTH_VALUE).
    /// `peer_group_end` is the exclusive end of the current row's peer group (for RANGE semantics).
    /// Returns (start, end) where end is exclusive.
    pub(super) fn compute_simple_frame_bounds(
        &self,
        wf_info: &WindowFunctionInfo,
        current_row: usize,
        partition_len: usize,
        peer_group: (usize, usize),
        row_indices: &[usize],
        order_by_values: &ColumnarOrderByValues,
    ) -> Result<(usize, usize)> {
        if let Some(ref frame) = wf_info.frame {
            let is_range = matches!(frame.unit, WindowFrameUnit::Range);
            // Calculate start bound
            let start = match &frame.start {
                WindowFrameBound::UnboundedPreceding => 0,
                WindowFrameBound::CurrentRow if is_range => peer_group.0,
                WindowFrameBound::CurrentRow => current_row,
                WindowFrameBound::Preceding(expr) => {
                    if is_range {
                        self.range_offset_boundary(
                            expr,
                            true,
                            true,
                            current_row,
                            row_indices,
                            order_by_values,
                        )?
                    } else if let Expression::IntegerLiteral(lit) = expr.as_ref() {
                        current_row.saturating_sub(lit.value as usize)
                    } else {
                        0
                    }
                }
                WindowFrameBound::Following(expr) => {
                    if is_range {
                        self.range_offset_boundary(
                            expr,
                            false,
                            true,
                            current_row,
                            row_indices,
                            order_by_values,
                        )?
                    } else if let Expression::IntegerLiteral(lit) = expr.as_ref() {
                        (current_row + lit.value as usize).min(partition_len)
                    } else {
                        current_row
                    }
                }
                WindowFrameBound::UnboundedFollowing => partition_len,
            };

            // Calculate end bound (exclusive)
            let end = match &frame.end {
                Some(WindowFrameBound::UnboundedFollowing) => partition_len,
                Some(WindowFrameBound::CurrentRow) if is_range => peer_group.1,
                Some(WindowFrameBound::CurrentRow) => current_row + 1,
                Some(WindowFrameBound::Following(expr)) => {
                    if is_range {
                        self.range_offset_boundary(
                            expr,
                            false,
                            false,
                            current_row,
                            row_indices,
                            order_by_values,
                        )?
                    } else if let Expression::IntegerLiteral(lit) = expr.as_ref() {
                        (current_row + lit.value as usize + 1).min(partition_len)
                    } else {
                        partition_len
                    }
                }
                Some(WindowFrameBound::Preceding(expr)) => {
                    if is_range {
                        self.range_offset_boundary(
                            expr,
                            true,
                            false,
                            current_row,
                            row_indices,
                            order_by_values,
                        )?
                    } else if let Expression::IntegerLiteral(lit) = expr.as_ref() {
                        if lit.value as usize <= current_row {
                            current_row - lit.value as usize + 1
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }
                Some(WindowFrameBound::UnboundedPreceding) => 0,
                None => {
                    // If no end is specified, default to CURRENT ROW
                    if is_range {
                        peer_group.1
                    } else {
                        current_row + 1
                    }
                }
            };

            Ok((start, end))
        } else {
            // No explicit frame specified
            // SQL standard:
            // - With ORDER BY: default is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            // - Without ORDER BY: default is the entire partition
            if wf_info.order_by.is_empty() {
                // No ORDER BY - entire partition
                Ok((0, partition_len))
            } else {
                // Has ORDER BY - default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                // SQL standard: peer rows (same ORDER BY value) share the same frame end
                Ok((0, peer_group.1))
            }
        }
    }

    pub(super) fn range_offset_boundary(
        &self,
        offset_expr: &Expression,
        preceding: bool,
        is_start: bool,
        current_row: usize,
        row_indices: &[usize],
        order_by_values: &ColumnarOrderByValues,
    ) -> Result<usize> {
        if order_by_values.num_columns() != 1 {
            return Err(Error::InvalidArgument(
                "RANGE with a value offset requires exactly one ORDER BY expression".to_string(),
            ));
        }
        let Expression::IntegerLiteral(offset) = offset_expr else {
            return Err(Error::InvalidArgument(
                "RANGE offset must be a non-negative INTEGER literal".to_string(),
            ));
        };
        if offset.value < 0 {
            return Err(Error::InvalidArgument(
                "RANGE offset must be non-negative".to_string(),
            ));
        }

        let current_source_row = *row_indices
            .get(current_row)
            .ok_or_else(|| Error::internal("window RANGE current row is outside the partition"))?;
        let Some(Value::Integer(current_value)) = order_by_values.get_first(current_source_row)
        else {
            return Err(Error::NotSupported(
                "RANGE value offsets currently require a non-NULL INTEGER ORDER BY key".to_string(),
            ));
        };

        let ascending = order_by_values.is_ascending(0);
        let moves_toward_lower_values = (ascending && preceding) || (!ascending && !preceding);
        let target = if moves_toward_lower_values {
            *current_value as i128 - offset.value as i128
        } else {
            *current_value as i128 + offset.value as i128
        };

        for (position, source_row) in row_indices.iter().copied().enumerate() {
            let Some(Value::Integer(candidate)) = order_by_values.get_first(source_row) else {
                return Err(Error::NotSupported(
                    "RANGE value offsets currently require non-NULL INTEGER ORDER BY keys"
                        .to_string(),
                ));
            };
            let comparison = if ascending {
                (*candidate as i128).cmp(&target)
            } else {
                target.cmp(&(*candidate as i128))
            };
            if (is_start && comparison != Ordering::Less)
                || (!is_start && comparison == Ordering::Greater)
            {
                return Ok(position);
            }
        }
        Ok(row_indices.len())
    }

    /// Resolve column index from expression, trying both qualified and unqualified names
    pub(super) fn resolve_column_index(
        &self,
        expr: &Expression,
        col_index_map: &StringMap<usize>,
    ) -> Option<usize> {
        match expr {
            Expression::Identifier(id) => col_index_map.get(id.value_lower.as_str()).copied(),
            Expression::QualifiedIdentifier(qid) => {
                // Try fully qualified name first (e.g., "s.qty")
                let qualified =
                    format!("{}.{}", qid.qualifier.value, qid.name.value).to_lowercase();
                col_index_map
                    .get(&qualified)
                    // Then try unqualified (e.g., "qty") for CTE/subquery cases
                    .or_else(|| col_index_map.get(qid.name.value_lower.as_str()))
                    .copied()
            }
            Expression::FunctionCall(func) => {
                // Handle aggregate functions that have been computed in GROUP BY
                // e.g., SUM(val) -> look for column named "SUM(val)"
                let func_col_name = if func.arguments.is_empty()
                    || matches!(func.arguments.first(), Some(Expression::Star(_)))
                {
                    format!("{}(*)", func.function)
                } else if func.arguments.len() == 1 {
                    match &func.arguments[0] {
                        Expression::Identifier(id) => {
                            format!("{}({})", func.function, id.value)
                        }
                        Expression::QualifiedIdentifier(qid) => {
                            format!("{}({})", func.function, qid.name.value)
                        }
                        _ => format!("{}(expr)", func.function),
                    }
                } else {
                    format!("{}(...)", func.function)
                };

                // First try exact match (e.g., "sum(val)")
                if let Some(idx) = col_index_map.get(&func_col_name.to_lowercase()).copied() {
                    return Some(idx);
                }

                // If not found and this is an aggregate function, the result might be aliased
                // In that case, we need the caller to use expression evaluation instead
                // Return None to trigger the expression evaluation path
                None
            }
            _ => None,
        }
    }

    /// Precompute ORDER BY values for all rows using columnar layout
    /// Returns a ColumnarOrderByValues structure for cache-efficient sorting
    pub(super) fn precompute_order_by_values(
        &self,
        order_by: &[OrderByExpression],
        rows: &[(i64, Row)],
        columns: &[String],
        col_index_map: &StringMap<usize>,
        ctx: &ExecutionContext,
    ) -> Result<ColumnarOrderByValues> {
        let num_rows = rows.len();
        let num_cols = order_by.len();

        if num_cols == 0 || num_rows == 0 {
            return Ok(ColumnarOrderByValues {
                columns: vec![],
                ascending: vec![],
                nulls_first: vec![],
                num_rows: 0,
            });
        }

        // Resolve the complete sort contract once (not per row).
        let ascending_flags: Vec<bool> = order_by.iter().map(|ob| ob.ascending).collect();
        let nulls_first_flags: Vec<bool> = order_by
            .iter()
            .map(|ob| ob.nulls_first.unwrap_or(!ob.ascending))
            .collect();

        // Check if any ORDER BY expression is complex (not a simple column reference)
        let has_complex_expr = order_by.iter().any(|ob| {
            !matches!(
                &ob.expression,
                Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
            )
        });

        // Pre-allocate columns with exact capacity
        let mut result_columns: Vec<Vec<Value>> = (0..num_cols)
            .map(|_| Vec::with_capacity(num_rows))
            .collect();

        if has_complex_expr {
            // Build aliases from col_index_map (e.g., "sum(val)" -> column_index)
            // This allows ORDER BY SUM(val) to resolve to the correct column
            let agg_aliases: Vec<(String, usize)> =
                col_index_map.iter().map(|(k, v)| (k.clone(), *v)).collect();

            // Extract order_by expressions
            let order_exprs: Vec<Expression> =
                order_by.iter().map(|ob| ob.expression.clone()).collect();

            // Compile all expressions with aliases
            let eval =
                MultiExpressionEval::compile_with_aliases(&order_exprs, columns, &agg_aliases)?;
            let mut eval = eval.with_context(ctx);
            for (_, row) in rows {
                let values = eval.eval_all(row)?;
                for (col_idx, value) in values.into_iter().enumerate() {
                    if col_idx < result_columns.len() {
                        result_columns[col_idx].push(value);
                    }
                }
            }
        } else {
            // Fast path: simple column references
            let order_by_indices: Vec<Option<usize>> = order_by
                .iter()
                .map(|ob| match &ob.expression {
                    Expression::Identifier(id) => {
                        col_index_map.get(id.value_lower.as_str()).copied()
                    }
                    Expression::QualifiedIdentifier(qid) => {
                        let qualified =
                            format!("{}.{}", qid.qualifier.value, qid.name.value).to_lowercase();
                        col_index_map
                            .get(&qualified)
                            .or_else(|| col_index_map.get(qid.name.value_lower.as_str()))
                            .copied()
                    }
                    _ => None,
                })
                .collect();

            // Extract values column by column for better cache locality
            for (col_idx, idx_opt) in order_by_indices.iter().enumerate() {
                let col = &mut result_columns[col_idx];
                match idx_opt {
                    Some(src_idx) => {
                        for (_, row) in rows {
                            col.push(
                                row.get(*src_idx)
                                    .cloned()
                                    .unwrap_or_else(Value::null_unknown),
                            );
                        }
                    }
                    None => {
                        for _ in 0..num_rows {
                            col.push(Value::null_unknown());
                        }
                    }
                }
            }
        }

        Ok(ColumnarOrderByValues {
            columns: result_columns,
            ascending: ascending_flags,
            nulls_first: nulls_first_flags,
            num_rows,
        })
    }

    /// Check if rows are already pre-sorted by the window ORDER BY column
    /// Returns true if we can skip sorting
    pub(super) fn check_rows_presorted(
        &self,
        wf_info: &WindowFunctionInfo,
        pre_sorted: Option<&WindowPreSortedState>,
    ) -> bool {
        let pre_sorted = match pre_sorted {
            Some(ps) => ps,
            None => return false,
        };

        // Only optimize if there's exactly one ORDER BY column (simple case)
        if wf_info.order_by.len() != 1 {
            return false;
        }

        let order_by = &wf_info.order_by[0];

        // Extract column name from ORDER BY expression
        let order_col = match &order_by.expression {
            Expression::Identifier(id) => id.value_lower.clone(),
            Expression::QualifiedIdentifier(qid) => qid.name.value_lower.clone(),
            _ => return false, // Complex expressions can't be pre-sorted
        };

        // Check if pre-sorted column matches and direction matches
        order_col == pre_sorted.column && order_by.ascending == pre_sorted.ascending
    }

    /// Sort row indices using precomputed ORDER BY values (columnar layout)
    pub(super) fn sort_by_order_values(
        row_indices: &mut [usize],
        order_by_values: &ColumnarOrderByValues,
    ) {
        if row_indices.len() < 2 || order_by_values.is_empty() {
            return;
        }

        // Single-column path avoids the outer column loop, but uses the same
        // canonical comparator as multi-column ordering. The previous sampled
        // type specialization could misclassify a later mixed value and used
        // i64::MAX/f64::MAX as NULL sentinels, colliding with real values.
        if order_by_values.num_columns() == 1 {
            let ascending = order_by_values.is_ascending(0);
            let nulls_first = order_by_values.nulls_first(0);
            let col = &order_by_values.columns[0];
            Self::sort_single_column_columnar(row_indices, col, ascending, nulls_first);
            return;
        }

        // Multi-column ORDER BY: use parallel sort for large partitions
        const PARALLEL_THRESHOLD: usize = 10_000;

        if row_indices.len() >= PARALLEL_THRESHOLD {
            #[cfg(feature = "parallel")]
            row_indices.par_sort_unstable_by(|&a, &b| {
                Self::compare_order_values_columnar(order_by_values, a, b)
            });
            #[cfg(not(feature = "parallel"))]
            row_indices.sort_unstable_by(|&a, &b| {
                Self::compare_order_values_columnar(order_by_values, a, b)
            });
        } else {
            row_indices.sort_unstable_by(|&a, &b| {
                Self::compare_order_values_columnar(order_by_values, a, b)
            });
        }
    }

    /// Compare two rows by their ORDER BY values (columnar layout)
    #[inline]
    pub(super) fn compare_order_values_columnar(
        order_by_values: &ColumnarOrderByValues,
        a: usize,
        b: usize,
    ) -> Ordering {
        for col_idx in 0..order_by_values.num_columns() {
            let a_val = order_by_values.get(a, col_idx);
            let b_val = order_by_values.get(b, col_idx);

            let cmp = Self::compare_order_values(
                a_val,
                b_val,
                order_by_values.is_ascending(col_idx),
                order_by_values.nulls_first(col_idx),
            );

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }

    /// Compare one ORDER BY key. NULL placement is resolved independently
    /// from the direction used for non-NULL values.
    #[inline]
    pub(super) fn compare_order_values(
        a: Option<&Value>,
        b: Option<&Value>,
        ascending: bool,
        nulls_first: bool,
    ) -> Ordering {
        let a_is_null = a.is_none_or(Value::is_null);
        let b_is_null = b.is_none_or(Value::is_null);
        match (a_is_null, b_is_null) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let cmp = a
                    .expect("non-NULL ORDER BY value")
                    .cmp(b.expect("non-NULL ORDER BY value"));
                if ascending {
                    cmp
                } else {
                    cmp.reverse()
                }
            }
        }
    }

    /// Single column generic sort (columnar layout)
    pub(super) fn sort_single_column_columnar(
        row_indices: &mut [usize],
        col: &[Value],
        ascending: bool,
        nulls_first: bool,
    ) {
        const PARALLEL_THRESHOLD: usize = 10_000;

        let compare = |&a: &usize, &b: &usize| -> Ordering {
            let a_val = col.get(a);
            let b_val = col.get(b);

            Self::compare_order_values(a_val, b_val, ascending, nulls_first)
        };

        if row_indices.len() >= PARALLEL_THRESHOLD {
            #[cfg(feature = "parallel")]
            row_indices.par_sort_unstable_by(compare);
            #[cfg(not(feature = "parallel"))]
            row_indices.sort_unstable_by(compare);
        } else {
            row_indices.sort_unstable_by(compare);
        }
    }
}
