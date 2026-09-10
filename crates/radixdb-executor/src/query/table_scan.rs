impl Executor {
    /// Execute a simple table scan
    fn execute_simple_table_scan(
        &self,
        table_source: &SimpleTableSource,
        stmt: &SelectStatement,
        ctx: &ExecutionContext,
        classification: &std::sync::Arc<QueryClassification>,
    ) -> SelectResult {
        // OPTIMIZATION: Use pre-computed lowercase name to avoid allocation per query
        let table_name = &table_source.name.value_lower;

        let source = open_query_source(
            &self.engine,
            &self.active_transaction,
            table_name,
            table_source.as_of.is_some(),
        )?;
        let current = match source {
            QuerySourceHandle::Current(current) => current,
            QuerySourceHandle::Temporal(transaction) => {
                return self.execute_temporal_query(
                    table_name,
                    table_source
                        .as_of
                        .as_ref()
                        .expect("temporal source lost AS OF"),
                    stmt,
                    ctx,
                    transaction.as_ref(),
                    classification,
                );
            }
        };
        let (table, _standalone_tx, in_explicit_transaction) = current.into_parts();

        // Build column list from schema (using cached version to avoid repeated clones)
        let all_columns: Vec<String> = table.schema().column_names_owned().to_vec();
        // Get pre-cached lowercase column names to avoid per-query to_lowercase() calls
        let all_columns_lower = table.schema().column_names_lower_arc();

        // Get table alias for correlated subquery support
        let table_alias: Option<String> = table_source
            .alias
            .as_ref()
            .map(|a| a.value_lower.to_string())
            .or_else(|| Some(table_name.to_string()));

        // classification is passed from caller to avoid redundant cache lookups

        // AGGREGATION PUSHDOWN: Try to compute simple aggregates directly on storage
        // This avoids all row materialization for queries like:
        // - SELECT COUNT(*) FROM table
        // - SELECT SUM(col), MIN(col), MAX(col) FROM table
        // Must check before any row collection happens
        if classification.has_aggregation && !classification.has_window_functions {
            // First try global aggregation pushdown (no GROUP BY, no WHERE)
            if let Some(result) =
                self.try_aggregation_pushdown(table.as_ref(), stmt, ctx, classification)?
            {
                let columns = CompactArc::new(result.columns().to_vec());
                return Ok((result, columns, false, None));
            }

            // Try filtered aggregation pushdown (WHERE + aggregates, no GROUP BY)
            // Pushes both the filter and aggregation into the storage layer
            if classification.has_where {
                if let Some(result) = self.try_filtered_aggregation_pushdown(
                    table.as_ref(),
                    stmt,
                    ctx,
                    classification,
                    &all_columns,
                )? {
                    let columns = CompactArc::new(result.columns().to_vec());
                    return Ok((result, columns, false, None));
                }
            }

            // Try storage-level GROUP BY aggregation.
            // This computes aggregates directly in storage without executor-side row collection.
            if classification.has_group_by {
                if let Some(result) = self.try_storage_aggregation(
                    table.as_ref(),
                    stmt,
                    ctx,
                    &all_columns,
                    classification,
                ) {
                    let columns = CompactArc::new(result.columns().to_vec());
                    return Ok((result, columns, false, None));
                }
            }

            // Try streaming aggregation for expressions like AVG(col) * 100
            // This avoids collecting all rows by streaming through a scanner
            if let Some(result) =
                self.try_streaming_global_aggregation(table.as_ref(), stmt, ctx, classification)?
            {
                let columns = CompactArc::new(result.columns().to_vec());
                return Ok((result, columns, false, None));
            }
        }

        // DISTINCT PUSHDOWN: Try to get distinct values directly from index
        // This avoids all row materialization for queries like:
        // - SELECT DISTINCT col FROM table (where col is indexed)
        // Returns distinct values in O(unique values) instead of O(rows)
        if classification.has_distinct {
            if let Some(result) =
                self.try_distinct_pushdown(table.as_ref(), stmt, &all_columns, classification)?
            {
                let columns = CompactArc::new(result.columns().to_vec());
                return Ok((result, columns, false, None));
            }
        }

        // Check if ORDER BY references columns not in SELECT
        let order_by_needs_extra_columns = self.order_by_needs_extra_columns(stmt, &all_columns);

        let prepared_predicate = access_predicate::prepare_scan_predicate(
            &stmt.columns,
            stmt.where_clause.as_deref(),
            &all_columns,
            table.schema(),
            table_alias.as_deref(),
            ctx,
            self.function_registry.as_ref(),
            classification.where_has_subqueries,
        );
        let where_to_use = prepared_predicate.effective();
        let plugin_candidate_rows = self.try_plugin_candidate_scan(
            table_name,
            table_alias.as_deref(),
            table.as_ref(),
            where_to_use,
            &all_columns,
            ctx,
        )?;

        // Check if this query might reference outer columns (correlated)
        let has_outer_context = ctx.outer_row().is_some();

        // SEMANTIC CACHE: Check if we can serve this query from cache
        // Eligible queries: simple column projections with WHERE, no aggregation/window/grouping, no outer context
        // Use cached classification for is_select_star check
        let is_select_star = classification.is_select_star;

        let has_aggregation_window_grouping = classification.has_aggregation
            || classification.has_window_functions
            || classification.has_group_by;
        // Use cached classification for subquery detection (avoids AST traversal)
        let has_subqueries_in_where = classification.where_has_subqueries;

        // CRITICAL: Check if WHERE clause contains parameters ($1, $2, etc.)
        // Parameterized queries CANNOT be cached because:
        // 1. The cache stores results tied to specific parameter values
        // 2. But the AST only has parameter indices ($1), not actual values
        // 3. A cache "hit" would return wrong results for different parameter values
        // This was causing 100x slowdown for SELECT by ID queries due to:
        // - Cache misses on every lookup (unique predicates)
        // - Streaming disabled for cache-eligible queries
        // - Cache insertions (write locks) on every query execution
        // Use cached classification for parameter detection (avoids AST traversal)
        let has_parameters_in_where = classification.where_has_parameters;

        // Cache eligibility: We cache SELECT * queries because:
        // 1. The cache stores full table rows with their original column layout
        // 2. Subsumption detection works on full rows for filtering
        // 3. For non-SELECT * queries, we would need to project cached rows on hit
        //
        // CRITICAL: Disable caching during explicit transactions (BEGIN/COMMIT)
        // to preserve MVCC isolation guarantees. A transaction must see its own
        // consistent snapshot, not cached results from other transactions.
        let cache_eligible = is_select_star
            && plugin_candidate_rows.is_none()
            && where_to_use.is_some()
            && !has_aggregation_window_grouping
            && !has_outer_context
            && !has_subqueries_in_where
            && !has_parameters_in_where // Parameters can't be cached (values not in AST)
            && !classification.has_nondeterministic_functions // NOW(), RANDOM(), etc. return different values per execution
            && !classification.has_order_by
            && !classification.has_distinct
            && !classification.has_limit
            && !in_explicit_transaction; // MVCC safety: no caching in transactions
        let semantic_cache_generation = cache_eligible.then(|| self.semantic_cache.generation());

        // Try cache lookup for eligible queries
        if cache_eligible {
            if let Some(where_expr) = where_to_use {
                use super::semantic_cache::CacheLookupResult;

                match self
                    .semantic_cache
                    .lookup(table_name, &all_columns, Some(where_expr))
                {
                    CacheLookupResult::ExactHit(rows_arc) => {
                        // Exact cache hit - return cached rows with zero-copy sharing
                        let output_columns = CompactArc::new(self.get_output_column_names(
                            &stmt.columns,
                            &all_columns,
                            table_alias.as_deref(),
                        ));
                        let result = ExecutorResult::with_arc_columns_shared_rows(
                            CompactArc::clone(&output_columns),
                            rows_arc,
                        );
                        return Ok((Box::new(result), output_columns, false, None));
                    }
                    CacheLookupResult::SubsumptionHit {
                        rows: rows_arc,
                        filter,
                        columns,
                    } => {
                        // Subsumption hit - filter cached rows
                        // Clone the Vec since we need to filter (creates new Vec anyway)
                        use super::semantic_cache::SemanticCache;
                        let filtered_vec = SemanticCache::filter_rows(
                            (*rows_arc).clone(),
                            &filter,
                            &columns,
                            &self.function_registry,
                        )?;
                        // Convert Vec<Row> to RowVec
                        let filtered_rows: RowVec = filtered_vec
                            .into_iter()
                            .enumerate()
                            .map(|(i, row)| (i as i64, row))
                            .collect();
                        let output_columns = CompactArc::new(self.get_output_column_names(
                            &stmt.columns,
                            &all_columns,
                            table_alias.as_deref(),
                        ));
                        let result = ExecutorResult::with_arc_columns(
                            CompactArc::clone(&output_columns),
                            filtered_rows,
                        );
                        return Ok((Box::new(result), output_columns, false, None));
                    }
                    CacheLookupResult::Miss => {
                        // Cache miss - continue with normal execution
                        // Result will be inserted into cache below
                    }
                }
            }
        }

        let storage_expr = &prepared_predicate.storage;
        let needs_memory_filter = prepared_predicate.needs_memory_filter();
        let memory_where_to_use = prepared_predicate.memory_filter();

        // ZONE MAP PRUNING: Short-circuit if zone maps indicate no rows can match
        // This checks min/max statistics per segment to skip entire scan when
        // the WHERE clause predicates are outside all segment ranges
        // IMPORTANT: Don't short-circuit if we have aggregation, window functions, or GROUP BY
        // because aggregation on empty results needs to produce output (e.g., COUNT=0)
        let has_aggregation_or_grouping = classification.has_aggregation
            || classification.has_window_functions
            || classification.has_group_by;

        if !has_aggregation_or_grouping {
            if let Some(ref expr) = storage_expr {
                if self
                    .get_query_planner()
                    .can_prune_entire_scan(&*table, expr.as_ref())
                {
                    // Zone maps indicate no segments can match - return empty result
                    let output_columns = CompactArc::new(self.get_output_column_names(
                        &stmt.columns,
                        &all_columns,
                        table_alias.as_deref(),
                    ));
                    let result = ExecutorResult::with_arc_columns(
                        CompactArc::clone(&output_columns),
                        RowVec::new(),
                    );
                    return Ok((Box::new(result), output_columns, true, None));
                }
            }
        }

        // FAST PATH: MIN/MAX index optimization
        // For queries like `SELECT MIN(col) FROM table` or `SELECT MAX(col) FROM table`
        // without WHERE or GROUP BY, use the index directly (O(1) instead of O(n))
        if storage_expr.is_none() && !needs_memory_filter && !classification.has_group_by {
            if let Some((result, columns)) =
                self.try_min_max_index_optimization(stmt, &*table, &all_columns)?
            {
                return Ok((result, columns, false, None));
            }
        }

        // FAST PATH: COUNT(*) pushdown optimization
        // For queries like `SELECT COUNT(*) FROM table` without WHERE or GROUP BY,
        // use the table's row_count() method instead of scanning all rows
        if storage_expr.is_none() && !needs_memory_filter && !classification.has_group_by {
            if let Some((result, columns)) = self.try_count_star_optimization(stmt, &*table)? {
                return Ok((result, columns, false, None));
            }
        }

        // FAST PATH: Streaming GROUP BY optimization using B-tree index
        // For queries like `SELECT user_id, SUM(amount) FROM orders GROUP BY user_id`
        // where user_id has a B-tree index, we iterate through the index in sorted order
        // and aggregate each group without using a hash map (SQLite-style sorted GROUP BY)
        if storage_expr.is_none()
            && !needs_memory_filter
            && classification.has_group_by
            && classification.has_aggregation
            && !classification.has_window_functions
            && !classification.has_order_by
        // No ORDER BY to avoid re-sorting
        {
            if let Some((result, columns)) =
                self.try_streaming_group_by(stmt, &*table, &all_columns, ctx)?
            {
                return Ok((result, columns, false, None));
            }
        }

        // FAST PATH: ORDER BY + LIMIT optimization (TOP-N)
        // For queries like `SELECT * FROM table ORDER BY indexed_col LIMIT 10`,
        // use index to get rows in sorted order directly, avoiding full table sort
        if classification.has_limit
            && stmt.order_by.len() == 1
            && !classification.has_group_by
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !classification.has_distinct
            && storage_expr.is_none()
            && !needs_memory_filter
        {
            if let Some((result, columns)) =
                self.try_order_by_index_optimization(stmt, &*table, &all_columns, ctx)?
            {
                // Note: ORDER BY + LIMIT already handles LIMIT at storage level
                return Ok((result, columns, true, None));
            }
        }

        // FAST PATH: exact parallel vector Top-K.
        // For queries like `SELECT id, VEC_DISTANCE_L2(embedding, '...') AS dist
        //   FROM documents ORDER BY dist LIMIT 10`
        // Supports WHERE by pre-filtering rows before exact distance evaluation.
        // Skip when transaction has local changes — HNSW index and collect_rows_by_ids
        // only see committed data, so local INSERTs/UPDATEs would be missed.
        if classification.has_limit
            && stmt.order_by.len() == 1
            && !classification.has_group_by
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !classification.has_distinct
            && !table.has_local_changes()
        {
            if let Some((result, columns)) =
                self.try_vector_search_optimization(stmt, &*table, &all_columns, ctx)?
            {
                return Ok((result, columns, true, None));
            }
        }

        // FAST PATH: Keyset pagination optimization
        // For queries like `SELECT * FROM table WHERE id > X ORDER BY id LIMIT Y`,
        // use the PK's ordering to start iteration from X directly.
        // This provides O(limit) complexity instead of O(n) for full scans.
        // Note: OFFSET is not supported - queries with OFFSET fall through to regular execution.
        if classification.has_limit
            && stmt.offset.is_none()
            && stmt.order_by.len() == 1
            && !classification.has_group_by
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !classification.has_distinct
            && !needs_memory_filter
        {
            if let Some((result, columns)) = self.try_keyset_pagination_optimization(
                stmt,
                where_to_use,
                &*table,
                &all_columns,
                table_alias.as_deref(),
                ctx,
            )? {
                return Ok((result, columns, true, None));
            }
        }

        // FAST PATH: equality-prefix + trailing range over a composite index.
        // Unlike the PK-only keyset path, this operator also merges persisted
        // artifact-backed postings with the hot MVCC index and preserves index order.
        if classification.has_limit
            && stmt.order_by.len() == 1
            && !classification.has_group_by
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !classification.has_distinct
            && !needs_memory_filter
        {
            if let Some(ref expr) = storage_expr {
                if let Some((result, columns)) = self.try_composite_ordered_range_optimization(
                    stmt,
                    expr.as_ref(),
                    &*table,
                    &all_columns,
                    table_alias.as_deref(),
                    ctx,
                )? {
                    return Ok((result, columns, true, None));
                }
            }
        }

        // FAST PATH: IN subquery index optimization
        // For queries like `SELECT * FROM table WHERE id IN (SELECT col FROM other_table WHERE ...)`
        // where 'id' has an index or is PRIMARY KEY, probe directly instead of scanning all rows
        // Skip if query has aggregation: projection cannot compile aggregate functions
        if needs_memory_filter
            && !has_outer_context
            && !classification.has_group_by
            && !classification.has_aggregation
        {
            if let Some(where_expr) = where_to_use {
                if let Some((result, columns)) = self.try_in_subquery_index_optimization(
                    stmt,
                    where_expr,
                    &*table,
                    &all_columns,
                    table_alias.as_deref(),
                    ctx,
                    classification,
                )? {
                    return Ok((result, columns, false, None));
                }
            }
        }

        // FAST PATH: IN list literal index optimization
        // For queries like `SELECT * FROM table WHERE id IN (1, 2, 3, 5, 8)`
        // where 'id' has an index or is PRIMARY KEY, probe directly instead of scanning all rows
        // Skip if query has aggregation: projection cannot compile aggregate functions
        if needs_memory_filter
            && !has_outer_context
            && !classification.has_group_by
            && !classification.has_aggregation
        {
            if let Some(where_expr) = where_to_use {
                if let Some((result, columns)) = self.try_in_list_index_optimization(
                    stmt,
                    where_expr,
                    &*table,
                    &all_columns,
                    table_alias.as_deref(),
                    ctx,
                    classification,
                )? {
                    return Ok((result, columns, false, None));
                }
            }
        }

        // FAST PATH: LIMIT pushdown optimization
        // For simple queries like `SELECT * FROM table LIMIT 10` or
        // `SELECT * FROM table WHERE indexed_col = value LIMIT 10` without ORDER BY,
        // we can stop scanning early at the storage layer
        let can_pushdown_limit = classification.has_limit
            && !classification.has_order_by
            && !classification.has_group_by
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !classification.has_distinct
            && !needs_memory_filter; // Allow with storage_expr (WHERE on indexed columns)

        if can_pushdown_limit {
            let limit = if let Some(ref limit_expr) = stmt.limit {
                match ExpressionEval::compile(limit_expr, &[])?
                    .with_context(ctx)
                    .eval_slice(&Row::new())?
                {
                    Value::Integer(l) if l >= 0 => l as usize,
                    Value::Integer(l) => {
                        return Err(Error::Parse(format!(
                            "LIMIT must be non-negative, got {}",
                            l
                        )));
                    }
                    Value::Float(f) if f >= 0.0 => f as usize,
                    Value::Float(f) => {
                        return Err(Error::Parse(format!(
                            "LIMIT must be non-negative, got {}",
                            f
                        )));
                    }
                    _ => usize::MAX,
                }
            } else {
                usize::MAX
            };

            let offset = if let Some(ref offset_expr) = stmt.offset {
                match ExpressionEval::compile(offset_expr, &[])?
                    .with_context(ctx)
                    .eval_slice(&Row::new())?
                {
                    Value::Integer(o) if o >= 0 => o as usize,
                    Value::Integer(o) => {
                        return Err(Error::Parse(format!(
                            "OFFSET must be non-negative, got {}",
                            o
                        )));
                    }
                    Value::Float(f) if f >= 0.0 => f as usize,
                    Value::Float(f) => {
                        return Err(Error::Parse(format!(
                            "OFFSET must be non-negative, got {}",
                            f
                        )));
                    }
                    _ => 0,
                }
            } else {
                0
            };

            if let Some((column_indices, output_column_names)) =
                self.get_simple_projection_indices(&stmt.columns, &all_columns)
            {
                // Simple column projection can be pushed to the table boundary.
                // Implementations keep their existing LIMIT/OFFSET order and
                // early-termination strategy; cold columnar tables can also avoid
                // reading unused columns.
                let rows = table.collect_rows_with_limit_unordered_projected(
                    &column_indices,
                    storage_expr.as_deref(),
                    limit,
                    offset,
                )?;
                let output_columns = CompactArc::new(output_column_names);
                let result =
                    ExecutorResult::with_arc_columns(CompactArc::clone(&output_columns), rows);
                // LIMIT/OFFSET already applied during scanner consumption.
                return Ok((Box::new(result), output_columns, true, None));
            }

            if !classification.select_has_scalar_subqueries {
                let output_columns = self.get_output_column_names(
                    &stmt.columns,
                    &all_columns,
                    table_alias.as_deref(),
                );
                if let Some(plan) = self.build_expression_projection_scan_plan(
                    &stmt.columns,
                    &output_columns,
                    &all_columns,
                ) {
                    // Complex expressions can still be evaluated from a narrow
                    // dependency row. Use exact projection semantics because the
                    // dependency set may legitimately be empty for constant
                    // expressions.
                    let rows = table.collect_rows_with_limit_unordered_exact_projected(
                        &plan.scan_indices,
                        storage_expr.as_deref(),
                        limit,
                        offset,
                    )?;
                    let scan_columns_lower: Vec<String> =
                        plan.scan_columns.iter().map(|c| c.to_lowercase()).collect();
                    let projected_rows = self.project_rows_with_alias(
                        &stmt.columns,
                        rows,
                        &plan.scan_columns,
                        Some(&scan_columns_lower),
                        ctx,
                        table_alias.as_deref(),
                    )?;
                    let output_columns = CompactArc::new(plan.output_columns);
                    let result = ExecutorResult::with_arc_columns(
                        CompactArc::clone(&output_columns),
                        projected_rows,
                    );
                    return Ok((Box::new(result), output_columns, true, None));
                }
            }

            // Fallback: complex SELECT expressions with unsupported dependency
            // extraction still need full rows so the evaluator can access every
            // source column it may reference.
            let rows =
                table.collect_rows_with_limit_unordered(storage_expr.as_deref(), limit, offset)?;

            // Project rows according to SELECT expressions
            // Note: collect_rows_with_limit always returns full rows (all columns),
            // so we must always project here regardless of scanner_handled_projection
            let projected_rows = self.project_rows_with_alias(
                &stmt.columns,
                rows,
                &all_columns,
                Some(&all_columns_lower),
                ctx,
                table_alias.as_deref(),
            )?;
            let output_columns = CompactArc::new(self.get_output_column_names(
                &stmt.columns,
                &all_columns,
                table_alias.as_deref(),
            ));

            let result = ExecutorResult::with_arc_columns(
                CompactArc::clone(&output_columns),
                projected_rows,
            );
            // LIMIT/OFFSET already applied at storage level
            return Ok((Box::new(result), output_columns, true, None));
        }

        // STREAMING PATH: For simple queries without aggregation/window/ORDER BY
        // Use streaming result to avoid materializing all rows into Vec
        //
        // For cache-eligible queries, we disable streaming ONLY if the table is small
        // enough to potentially benefit from caching. Large tables (>100K rows) would
        // exceed the cache limit anyway, so we allow streaming for them.
        //
        // OPTIMIZATION: Use row_count_hint() which is O(1) instead of row_count() which
        // is O(n) with visibility checks. The hint returns an upper bound (versions.len())
        // which is safe for this decision - if hint > threshold, actual count is also large.
        let should_disable_streaming_for_cache = cache_eligible && {
            let table_row_count = table.row_count_hint();
            table_row_count <= super::semantic_cache::DEFAULT_MAX_CACHED_ROWS
        };

        let can_use_streaming = !classification.has_order_by
            && plugin_candidate_rows.is_none()
            && !classification.has_group_by
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !order_by_needs_extra_columns
            && !has_outer_context // Can't stream with outer context (correlated subqueries)
            && !should_disable_streaming_for_cache; // Only disable streaming for small cacheable queries

        // Check for CORRELATED subqueries in WHERE - these require per-row evaluation
        // Non-correlated scalar subqueries (like SELECT AVG(...)) are OK - they're evaluated once
        // and the result is used as a literal value for all rows
        let has_correlated_where_subqueries = classification.where_has_correlated_subqueries;

        // CRITICAL OPTIMIZATION: When needs_memory_filter is true AND we have LIMIT,
        // skip the streaming path. The streaming path pre-materializes ALL rows from
        // storage (via collect_visible_rows) before FilteredResult applies the filter.
        // This defeats early termination for LIMIT queries.
        // Fall through to the parallel/sequential path which has proper early termination.
        let skip_streaming_for_memory_filter_with_limit =
            needs_memory_filter && stmt.limit.is_some() && stmt.order_by.is_empty();

        if can_use_streaming
            && !has_correlated_where_subqueries
            && !skip_streaming_for_memory_filter_with_limit
        {
            // Check if we have simple column projection (no complex expressions)
            let simple_projection = self.get_simple_projection_indices(&stmt.columns, &all_columns);

            if let Some((column_indices, output_columns)) = simple_projection {
                if needs_memory_filter && !classification.where_has_subqueries {
                    if let Some(where_expr) = memory_where_to_use {
                        if let Some(plan) = self.build_filtered_simple_projection_scan_plan(
                            where_expr,
                            &column_indices,
                            &output_columns,
                            &all_columns,
                        ) {
                            let scanner = access_scan::open_exact_projection_scan(
                                table.as_ref(),
                                &plan.scan_indices,
                                storage_expr.as_deref(),
                                ctx,
                            )?;
                            let mut result: Box<dyn QueryResult> =
                                Box::new(ScannerResult::new(scanner, plan.scan_columns.clone()));

                            let filter =
                                RowFilter::new(where_expr, &plan.scan_columns)?.with_context(ctx);
                            result = Box::new(FilteredResult::from_filter(result, filter));

                            let output_columns = CompactArc::new(plan.output_columns);
                            result = Box::new(StreamingProjectionResult::new(
                                result,
                                plan.output_indices_in_scan,
                                (*output_columns).clone(),
                            ));
                            return Ok((result, output_columns, false, None));
                        }
                    }
                }

                // All columns are simple references - we can stream!
                //
                // If no residual memory filter is needed, push the projection into
                // the storage scanner and avoid streaming full rows only to trim
                // them later. Duplicate projections (SELECT a, a) are part of the
                // scan contract: the scanner returns one projected value per
                // requested index.
                let scanner_handles_projection = !needs_memory_filter;
                let scan_columns: Vec<usize> = if scanner_handles_projection {
                    column_indices.clone()
                } else {
                    (0..all_columns.len()).collect()
                };
                let scanner_columns = if scanner_handles_projection {
                    output_columns.clone()
                } else {
                    all_columns.to_vec()
                };
                let scanner = if scanner_handles_projection {
                    access_scan::open_exact_projection_scan(
                        table.as_ref(),
                        &scan_columns,
                        storage_expr.as_deref(),
                        ctx,
                    )?
                } else {
                    access_scan::open_scan(
                        table.as_ref(),
                        &scan_columns,
                        storage_expr.as_deref(),
                        ctx,
                    )?
                };

                // Wrap scanner in ScannerResult
                let mut result: Box<dyn QueryResult> =
                    Box::new(ScannerResult::new(scanner, scanner_columns));

                // If we need memory filtering (complex WHERE that couldn't be pushed down)
                if needs_memory_filter {
                    if let Some(where_expr) = memory_where_to_use {
                        // Check if there are non-correlated scalar subqueries to process
                        // These need to be evaluated once before streaming
                        let has_scalar_subqueries = classification.where_has_subqueries
                            && !classification.where_has_correlated_subqueries;

                        let filter = if has_scalar_subqueries {
                            // Process scalar subqueries to resolve them to literal values
                            let processed = self.process_where_subqueries(where_expr, ctx)?;
                            RowFilter::new(&processed, &all_columns)?.with_context(ctx)
                        } else {
                            // No scalar subqueries - use expression directly
                            RowFilter::new(where_expr, &all_columns)?.with_context(ctx)
                        };
                        result = Box::new(FilteredResult::from_filter(result, filter));
                    }
                }

                // Apply projection if needed (not SELECT *)
                // OPTIMIZATION: Check if projection is identity without allocating a Vec
                let is_identity_projection = column_indices.len() == all_columns.len()
                    && column_indices.iter().enumerate().all(|(i, &idx)| idx == i);
                // Check if column names differ (aliases)
                let names_differ = output_columns.len() != all_columns.len()
                    || output_columns
                        .iter()
                        .zip(all_columns.iter())
                        .any(|(out, all)| out != all);
                // Need StreamingProjectionResult if either:
                // 1. Non-identity projection (different columns or reordered)
                // 2. Column names differ (aliases like "SELECT id AS a")
                if scanner_handles_projection {
                    let output_columns = CompactArc::new(output_columns);
                    // LIMIT/OFFSET NOT applied yet - streaming path
                    return Ok((result, output_columns, false, None));
                } else if !column_indices.is_empty() && (!is_identity_projection || names_differ) {
                    let output_columns = CompactArc::new(output_columns);
                    result = Box::new(StreamingProjectionResult::new(
                        result,
                        column_indices,
                        (*output_columns).clone(),
                    ));
                    // LIMIT/OFFSET NOT applied yet - streaming path
                    return Ok((result, output_columns, false, None));
                } else {
                    // SELECT * - no projection needed
                    // LIMIT/OFFSET NOT applied yet - streaming path
                    return Ok((result, CompactArc::new(output_columns), false, None));
                }
            } else if !classification.select_has_scalar_subqueries {
                // STREAMING WITH EXPRESSIONS: Use ExprMappedResult for complex projections
                // This avoids batch allocation when SELECT contains expressions like CASE
                // NOTE: Only use this path when SELECT doesn't have subqueries (which need special processing)
                if needs_memory_filter && !classification.where_has_subqueries {
                    if let Some(where_expr) = memory_where_to_use {
                        let output_columns = self.get_output_column_names(
                            &stmt.columns,
                            &all_columns,
                            table_alias.as_deref(),
                        );
                        if let Some(plan) = self.build_filtered_expression_projection_scan_plan(
                            where_expr,
                            &stmt.columns,
                            &output_columns,
                            &all_columns,
                        ) {
                            let scanner = access_scan::open_exact_projection_scan(
                                table.as_ref(),
                                &plan.scan_indices,
                                storage_expr.as_deref(),
                                ctx,
                            )?;
                            let mut result: Box<dyn QueryResult> =
                                Box::new(ScannerResult::new(scanner, plan.scan_columns.clone()));

                            let filter =
                                RowFilter::new(where_expr, &plan.scan_columns)?.with_context(ctx);
                            result = Box::new(FilteredResult::from_filter(result, filter));

                            let output_columns = CompactArc::new(plan.output_columns);
                            result = Box::new(ExprMappedResult::with_context(
                                result,
                                stmt.columns.clone(),
                                (*output_columns).clone(),
                                ctx,
                            )?);

                            return Ok((result, output_columns, false, None));
                        }
                    }
                }

                let column_idx_vec: Vec<usize> = (0..all_columns.len()).collect();
                let scanner = access_scan::open_scan(
                    table.as_ref(),
                    &column_idx_vec,
                    storage_expr.as_deref(),
                    ctx,
                )?;

                let mut result: Box<dyn QueryResult> =
                    Box::new(ScannerResult::new(scanner, all_columns.clone()));

                // If we need memory filtering
                if needs_memory_filter {
                    if let Some(where_expr) = memory_where_to_use {
                        let has_scalar_subqueries = classification.where_has_subqueries
                            && !classification.where_has_correlated_subqueries;

                        let filter = if has_scalar_subqueries {
                            let processed = self.process_where_subqueries(where_expr, ctx)?;
                            RowFilter::new(&processed, &all_columns)?.with_context(ctx)
                        } else {
                            RowFilter::new(where_expr, &all_columns)?.with_context(ctx)
                        };
                        result = Box::new(FilteredResult::from_filter(result, filter));
                    }
                }

                // Use ExprMappedResult for expression-based projection with buffer reuse
                let output_columns = self.get_output_column_names(
                    &stmt.columns,
                    &all_columns,
                    table_alias.as_deref(),
                );
                let output_columns = CompactArc::new(output_columns);

                result = Box::new(ExprMappedResult::with_context(
                    result,
                    stmt.columns.clone(),
                    (*output_columns).clone(),
                    ctx,
                )?);

                return Ok((result, output_columns, false, None));
            }
        }

        let can_use_ordered_projection_scan_plan = (classification.has_order_by
            || classification.has_distinct_on)
            && (classification.has_limit || classification.has_offset)
            && !classification.has_group_by
            && !classification.has_aggregation
            && !classification.has_window_functions
            && !classification.select_has_scalar_subqueries
            && !classification.order_by_has_correlated_subqueries;

        let ordered_projection_scan_plan = if can_use_ordered_projection_scan_plan
            && !needs_memory_filter
        {
            let output_columns =
                self.get_output_column_names(&stmt.columns, &all_columns, table_alias.as_deref());
            self.build_ordered_distinct_projection_scan_plan(stmt, &output_columns, &all_columns)
        } else {
            None
        };

        // Collect rows - choose optimal path based on query type
        // PARALLEL EXECUTION: Use parallel filtering for large datasets
        let parallel_config = ParallelConfig::default();

        let rows_result: CollectedTableRows = if let Some(candidate) = plugin_candidate_rows {
            CollectedTableRows::full(candidate.rows)
        } else if needs_memory_filter {
            // Path 1: Need in-memory filtering (subqueries or complex expressions)
            // For memory filter, we need all columns to evaluate the WHERE clause
            let column_idx_vec: Vec<usize> = (0..all_columns.len()).collect();

            // OPTIMIZATION: Delay scanner creation until we know we need all rows.
            // For early termination path, we'll collect rows directly with a limit.
            // This avoids materializing all rows upfront when we only need a few.

            // Check if WHERE contains correlated subqueries
            // Use cached classification to avoid expensive AST traversal
            let has_correlated = classification.where_has_subqueries
                && classification.where_has_correlated_subqueries;

            // Check if WHERE contains any subqueries (correlated or not)
            // Use cached classification to avoid redundant traversal of the expression tree
            let has_subqueries = classification.where_has_subqueries;

            // SEMI-JOIN OPTIMIZATION: Try to optimize correlated EXISTS subqueries
            // This transforms EXISTS (SELECT ... WHERE outer.col = inner.col AND ...)
            // into: outer.col IN (SELECT DISTINCT inner_col FROM inner WHERE ...)
            // This changes O(outer × inner) to O(inner + outer) - massive performance win!
            let (processed_where, has_correlated) = if has_correlated {
                if let Some(where_expr) = where_to_use {
                    // Get outer table names for semi-join detection
                    let outer_tables = Self::collect_outer_table_names(&stmt.table_expr);

                    // Extract limit value for optimization decision
                    // With small LIMIT, index-nested-loop with early termination is faster
                    let outer_limit = stmt.limit.as_ref().and_then(|limit_expr| {
                        if let Expression::IntegerLiteral(lit) = limit_expr.as_ref() {
                            Some(lit.value)
                        } else {
                            None
                        }
                    });

                    // ANTI-JOIN OPTIMIZATION: For pure NOT EXISTS without LIMIT, use HashJoinOperator::Anti
                    // This is faster than InHashSet because:
                    // 1. Bulk hash table build and probe (no per-row expression evaluation)
                    // 2. Better cache efficiency
                    // 3. Direct row iteration without VM overhead
                    //
                    // HOWEVER: For queries with LIMIT, we skip anti-join because it materializes
                    // ALL outer rows before applying LIMIT. The streaming InHashSet path can stop
                    // early once LIMIT is reached, making it faster for limited result sets.
                    if let Some(not_exists_info) =
                        Self::try_extract_not_exists_info(where_expr, &outer_tables)
                    {
                        // Check if NOT EXISTS is the ONLY predicate (pure anti-join case)
                        let is_pure_not_exists = matches!(where_expr, Expression::Prefix(_));

                        // Only use anti-join for queries without LIMIT (or very large LIMIT)
                        // For LIMIT queries, the streaming InHashSet path is faster due to early termination
                        let has_limit = outer_limit.is_some()
                            && outer_limit.unwrap() < ANTI_JOIN_LIMIT_THRESHOLD;
                        let use_anti_join = is_pure_not_exists && !has_limit;

                        if use_anti_join {
                            // Materialize outer table rows
                            let mut outer_rows = table.collect_all_rows(storage_expr.as_deref())?;

                            // Execute anti-join
                            let anti_join_result = self.execute_anti_join(
                                &not_exists_info,
                                CompactArc::new(outer_rows.drain_rows().collect()),
                                &all_columns,
                                ctx,
                            )?;

                            // Project and return result
                            let projected_rows = self.project_rows_with_alias(
                                &stmt.columns,
                                anti_join_result,
                                &all_columns,
                                Some(&all_columns_lower),
                                ctx,
                                table_alias.as_deref(),
                            )?;
                            let output_columns = CompactArc::new(self.get_output_column_names(
                                &stmt.columns,
                                &all_columns,
                                table_alias.as_deref(),
                            ));

                            // Apply LIMIT if present
                            let final_rows = if let Some(limit_expr) = &stmt.limit {
                                if let Expression::IntegerLiteral(lit) = limit_expr.as_ref() {
                                    let limit = lit.value as usize;
                                    let offset = stmt
                                        .offset
                                        .as_ref()
                                        .and_then(|o| {
                                            if let Expression::IntegerLiteral(lit) = o.as_ref() {
                                                Some(lit.value as usize)
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap_or(0);
                                    projected_rows
                                        .into_iter()
                                        .skip(offset)
                                        .take(limit)
                                        .collect()
                                } else {
                                    projected_rows
                                }
                            } else {
                                projected_rows
                            };

                            let result = ExecutorResult::with_arc_columns(
                                CompactArc::clone(&output_columns),
                                final_rows,
                            );
                            return Ok((Box::new(result), output_columns, true, None));
                        }
                    }

                    // Try semi-join optimizations for both EXISTS and IN subqueries
                    // These transform O(outer × inner) to O(inner + outer)
                    // Avoid cloning upfront - only clone if no optimization succeeds

                    // 1. Try EXISTS semi-join optimization
                    let exists_optimized = self
                        .try_optimize_exists_to_semi_join(
                            where_expr,
                            ctx,
                            &outer_tables,
                            outer_limit,
                        )
                        .ok()
                        .flatten();

                    // 2. Try IN semi-join optimization (on EXISTS result or original)
                    let expr_for_in = exists_optimized.as_ref().unwrap_or(where_expr);
                    let in_optimized = self
                        .try_optimize_in_to_semi_join(expr_for_in, ctx, &outer_tables)
                        .ok()
                        .flatten();

                    // Determine final expression without unnecessary clones
                    let (current_expr, any_optimized) = match (exists_optimized, in_optimized) {
                        (_, Some(in_opt)) => (in_opt, true),
                        (Some(exists_opt), None) => (exists_opt, true),
                        (None, None) => (where_expr.clone(), false), // Clone only when needed
                    };

                    // Check if there are still correlated subqueries after optimizations
                    let still_correlated = Self::has_correlated_subqueries(&current_expr);

                    if any_optimized && !still_correlated {
                        // All correlated subqueries were optimized away
                        (Some(current_expr), false)
                    } else if any_optimized {
                        // Some optimizations applied but still have correlated parts
                        (Some(current_expr), true)
                    } else {
                        // No optimizations applied - keep original for per-row processing
                        (Some(current_expr), true)
                    }
                } else {
                    (None, false)
                }
            } else if let Some(where_expr) = memory_where_to_use {
                if has_subqueries {
                    // Pre-process uncorrelated subqueries once
                    (Some(self.process_where_subqueries(where_expr, ctx)?), false)
                } else {
                    (Some(where_expr.clone()), false)
                }
            } else {
                (None, false)
            };

            // A nested EXISTS/IN may be rewritten completely, but the
            // remaining predicate can still reference the parent row directly
            // (for example `inner.parent_id = outer.id`). Keep the combined
            // current+parent binding map whenever this SELECT was entered with
            // an outer context; otherwise qualified local columns can be
            // resolved against an incomplete scope after the rewrite.
            let has_correlated = has_correlated || has_outer_context;

            // FAST PATH: InHashSet index optimization (from EXISTS → semi-join transformation)
            // If EXISTS was transformed to InHashSet and the column is PK/indexed,
            // probe directly instead of scanning all rows
            // Skip if there are correlated subqueries in SELECT columns
            // Skip if query has aggregation: projection cannot compile aggregate functions
            // Use cached classification to avoid AST traversal
            if storage_expr.is_none()
                && !has_correlated
                && !classification.has_group_by
                && !classification.has_aggregation
                && !classification.select_has_correlated_subqueries
            {
                if let Some(ref where_expr) = processed_where {
                    if let Some((result, columns)) = self.try_in_hashset_index_optimization(
                        stmt,
                        where_expr,
                        &*table,
                        &all_columns,
                        table_alias.as_deref(),
                        ctx,
                    )? {
                        return Ok((result, columns, false, None));
                    }
                }
            }

            // Check if we can use the PARALLEL PATH:
            // For simple WHERE without subqueries, collect all rows first
            // then filter in parallel. This is much faster for large tables.
            // CRITICAL: Cannot use parallel path when there's outer context because
            // the WHERE clause may reference outer columns (e.g., products.id in
            // a correlated subquery like WHERE product_id = products.id)
            let use_parallel_path =
                !has_correlated && !has_outer_context && processed_where.is_some();

            if use_parallel_path {
                let where_expr = processed_where.as_ref().unwrap();

                // EARLY TERMINATION: For LIMIT queries without ORDER BY, GROUP BY,
                // aggregation, or window functions, use streaming filter with early
                // termination. This is critical for NOT IN performance.
                let can_early_terminate = !classification.has_order_by
                    && !classification.has_group_by
                    && !classification.has_aggregation
                    && !classification.has_window_functions;

                let early_termination_target = if can_early_terminate {
                    let offset = stmt
                        .offset
                        .as_ref()
                        .and_then(|offset_expr| {
                            ExpressionEval::compile(offset_expr, &[])
                                .ok()
                                .and_then(|e| e.with_context(ctx).eval_slice(&Row::new()).ok())
                                .and_then(|v| {
                                    if let Value::Integer(o) = v {
                                        Some(o.max(0) as usize)
                                    } else {
                                        None
                                    }
                                })
                        })
                        .unwrap_or(0);

                    stmt.limit.as_ref().and_then(|limit_expr| {
                        ExpressionEval::compile(limit_expr, &[])
                            .ok()
                            .and_then(|e| e.with_context(ctx).eval_slice(&Row::new()).ok())
                            .and_then(|v| {
                                if let Value::Integer(l) = v {
                                    Some(offset + l.max(0) as usize)
                                } else {
                                    None
                                }
                            })
                    })
                } else {
                    None
                };

                // Use early termination path if we have a target
                if let Some(target) = early_termination_target {
                    // OPTIMIZATION: For memory-filter + LIMIT, use iterative fetching
                    // with early termination. We fetch in batches to avoid loading
                    // all rows when only a few are needed.
                    let mut result_rows = RowVec::with_capacity(target.min(1000));

                    if needs_memory_filter {
                        if let Some((output_indices, output_columns)) =
                            self.get_simple_projection_indices(&stmt.columns, &all_columns)
                        {
                            if let Some(plan) = self.build_filtered_simple_projection_scan_plan(
                                where_expr,
                                &output_indices,
                                &output_columns,
                                &all_columns,
                            ) {
                                // PARTIAL PUSHDOWN + NARROW PROJECTION: storage applies the
                                // pushable predicate part, while memory filtering and final
                                // projection operate on a row containing only predicate/output
                                // columns. This keeps LIMIT early termination without dragging
                                // unused payload columns through the batch loop.
                                let mut batch_size = target.max(100);
                                let mut offset = 0usize;
                                let mut memory_eval =
                                    ExpressionEval::compile(where_expr, &plan.scan_columns)?
                                        .with_context(ctx);

                                loop {
                                    let batch = table.collect_rows_with_limit_unordered_projected(
                                        &plan.scan_indices,
                                        storage_expr.as_deref(),
                                        batch_size,
                                        offset,
                                    )?;

                                    if batch.is_empty() {
                                        break;
                                    }

                                    for (row_id, row) in batch {
                                        if memory_eval.eval_bool_checked(&row)? {
                                            result_rows.push((row_id, row));
                                            if result_rows.len() >= target {
                                                break;
                                            }
                                        }
                                    }

                                    if result_rows.len() >= target {
                                        break;
                                    }

                                    offset += batch_size;
                                    batch_size *= 2;
                                }

                                let scan_columns_lower: Vec<String> =
                                    plan.scan_columns.iter().map(|c| c.to_lowercase()).collect();
                                let projected_rows = self.project_rows_with_alias(
                                    &stmt.columns,
                                    result_rows,
                                    &plan.scan_columns,
                                    Some(&scan_columns_lower),
                                    ctx,
                                    table_alias.as_deref(),
                                )?;
                                let output_columns = CompactArc::new(plan.output_columns);
                                let result = ExecutorResult::with_arc_columns(
                                    CompactArc::clone(&output_columns),
                                    projected_rows,
                                );
                                return Ok((Box::new(result), output_columns, true, None));
                            }
                        }

                        if !classification.select_has_scalar_subqueries {
                            let output_columns = self.get_output_column_names(
                                &stmt.columns,
                                &all_columns,
                                table_alias.as_deref(),
                            );
                            if let Some(plan) = self.build_filtered_expression_projection_scan_plan(
                                where_expr,
                                &stmt.columns,
                                &output_columns,
                                &all_columns,
                            ) {
                                // Same narrow-row contract for complex SELECT expressions:
                                // keep only residual predicate columns plus expression
                                // dependencies, then evaluate the expression projection at
                                // the API boundary.
                                let mut batch_size = target.max(100);
                                let mut offset = 0usize;
                                let mut memory_eval =
                                    ExpressionEval::compile(where_expr, &plan.scan_columns)?
                                        .with_context(ctx);

                                loop {
                                    let batch = table.collect_rows_with_limit_unordered_projected(
                                        &plan.scan_indices,
                                        storage_expr.as_deref(),
                                        batch_size,
                                        offset,
                                    )?;

                                    if batch.is_empty() {
                                        break;
                                    }

                                    for (row_id, row) in batch {
                                        if memory_eval.eval_bool_checked(&row)? {
                                            result_rows.push((row_id, row));
                                            if result_rows.len() >= target {
                                                break;
                                            }
                                        }
                                    }

                                    if result_rows.len() >= target {
                                        break;
                                    }

                                    offset += batch_size;
                                    batch_size *= 2;
                                }

                                let scan_columns_lower: Vec<String> =
                                    plan.scan_columns.iter().map(|c| c.to_lowercase()).collect();
                                let projected_rows = self.project_rows_with_alias(
                                    &stmt.columns,
                                    result_rows,
                                    &plan.scan_columns,
                                    Some(&scan_columns_lower),
                                    ctx,
                                    table_alias.as_deref(),
                                )?;
                                let output_columns = CompactArc::new(plan.output_columns);
                                let result = ExecutorResult::with_arc_columns(
                                    CompactArc::clone(&output_columns),
                                    projected_rows,
                                );
                                return Ok((Box::new(result), output_columns, true, None));
                            }
                        }

                        // PARTIAL PUSHDOWN: Storage handles some filtering, memory handles rest.
                        // Use batched fetching with increasing batch sizes to minimize work.
                        let mut batch_size = target.max(100); // Start with at least target rows
                        let mut offset = 0usize;

                        // Pre-compile filter ONCE outside the loop
                        let mut memory_eval =
                            ExpressionEval::compile(where_expr, &all_columns)?.with_context(ctx);

                        loop {
                            // Fetch batch from storage with storage filter + limit
                            // Now returns RowVec directly with row IDs preserved
                            let batch = table.collect_rows_with_limit(
                                storage_expr.as_deref(),
                                batch_size,
                                offset,
                            )?;

                            if batch.is_empty() {
                                break; // No more rows from storage
                            }

                            // Apply full WHERE filter (includes both pushed and non-pushed parts)
                            for (row_id, row) in batch {
                                if memory_eval.eval_bool_checked(&row)? {
                                    result_rows.push((row_id, row));
                                    if result_rows.len() >= target {
                                        break;
                                    }
                                }
                            }

                            if result_rows.len() >= target {
                                break; // Got enough rows
                            }

                            // Need more rows - increase batch size and offset
                            offset += batch_size;
                            batch_size *= 2; // Exponential backoff
                        }
                    } else {
                        // NO MEMORY FILTER NEEDED: Full filter pushed to storage.
                        // Use storage-level limit directly. Returns RowVec directly.
                        result_rows =
                            table.collect_rows_with_limit(storage_expr.as_deref(), target, 0)?;
                    }

                    // Project rows and return early - LIMIT/OFFSET already applied
                    let projected_rows = self.project_rows_with_alias(
                        &stmt.columns,
                        result_rows,
                        &all_columns,
                        Some(&all_columns_lower),
                        ctx,
                        table_alias.as_deref(),
                    )?;
                    let output_columns = CompactArc::new(self.get_output_column_names(
                        &stmt.columns,
                        &all_columns,
                        table_alias.as_deref(),
                    ));
                    let result = ExecutorResult::with_arc_columns(
                        CompactArc::clone(&output_columns),
                        projected_rows,
                    );
                    return Ok((Box::new(result), output_columns, true, None)); // true = LIMIT applied
                }

                if can_use_ordered_projection_scan_plan {
                    let output_columns = self.get_output_column_names(
                        &stmt.columns,
                        &all_columns,
                        table_alias.as_deref(),
                    );
                    if let Some(plan) = self.build_filtered_ordered_distinct_projection_scan_plan(
                        where_expr,
                        stmt,
                        &output_columns,
                        &all_columns,
                    ) {
                        // ORDER BY/DISTINCT ON may need columns that are not part
                        // of the public SELECT output. Keep those dependencies in
                        // the scanner row, but avoid dragging unrelated payload
                        // columns through the sort/distinct materialization step.
                        let scanner = access_scan::open_exact_projection_scan(
                            table.as_ref(),
                            &plan.scan_indices,
                            storage_expr.as_deref(),
                            ctx,
                        )?;
                        let all_rows = self.collect_scanner_rows(scanner, ctx)?;
                        let filtered = parallel::parallel_filter(
                            all_rows,
                            where_expr,
                            &plan.scan_columns,
                            &self.function_registry,
                            &parallel_config,
                            ctx,
                        )?;
                        CollectedTableRows::projected(filtered, plan.scan_columns)
                    } else {
                        // Normal path: collect all rows first (for ORDER BY, aggregation, etc.)
                        // Now we create the scanner since we need all rows
                        let scanner = access_scan::open_scan(
                            table.as_ref(),
                            &column_idx_vec,
                            storage_expr.as_deref(),
                            ctx,
                        )?;
                        let all_rows = self.collect_scanner_rows(scanner, ctx)?;

                        // Apply parallel filtering if we have enough rows
                        // CRITICAL: Propagate errors with ? instead of silently swallowing them
                        let filtered = parallel::parallel_filter(
                            all_rows,
                            where_expr,
                            &all_columns,
                            &self.function_registry,
                            &parallel_config,
                            ctx,
                        )?;
                        CollectedTableRows::full(filtered)
                    }
                } else {
                    // Normal path: collect all rows first (for ORDER BY, aggregation, etc.)
                    // Now we create the scanner since we need all rows
                    let scanner = access_scan::open_scan(
                        table.as_ref(),
                        &column_idx_vec,
                        storage_expr.as_deref(),
                        ctx,
                    )?;
                    let all_rows = self.collect_scanner_rows(scanner, ctx)?;

                    // Apply parallel filtering if we have enough rows
                    // CRITICAL: Propagate errors with ? instead of silently swallowing them
                    let filtered = parallel::parallel_filter(
                        all_rows,
                        where_expr,
                        &all_columns,
                        &self.function_registry,
                        &parallel_config,
                        ctx,
                    )?;
                    CollectedTableRows::full(filtered)
                }
            } else {
                // SEQUENTIAL PATH: For correlated subqueries or complex cases
                // Create evaluator once and reuse for all rows
                let mut eval = if processed_where.is_some() {
                    let mut e = CompiledEvaluator::new(&self.function_registry);
                    e = e.with_context(ctx);
                    e.init_columns(&all_columns);
                    Some(e)
                } else {
                    None
                };

                // OPTIMIZATION: Pre-compute column name mappings outside the loop
                // This avoids repeated to_lowercase() and format!() calls per row
                let column_keys: Option<Vec<ColumnKeyMapping>> = if has_correlated {
                    Some(ColumnKeyMapping::build_mappings(
                        &all_columns,
                        table_alias.as_deref(),
                    ))
                } else {
                    None
                };

                // OPTIMIZATION: Pre-allocate outer_row_map with capacity and reuse
                let base_capacity = all_columns.len() * 2 + ctx.outer_row().map_or(0, |m| m.len());
                let mut outer_row_map: FxHashMap<CompactArc<str>, Value> = FxHashMap::default();
                outer_row_map.reserve(base_capacity);

                // OPTIMIZATION: Wrap all_columns in Arc once, reuse for all rows (only if needed)
                let all_columns_arc: Option<CompactArc<Vec<String>>> = if has_correlated {
                    Some(CompactArc::new(all_columns.clone()))
                } else {
                    None
                };

                // LIMIT EARLY TERMINATION: For correlated subqueries without ORDER BY,
                // we can stop as soon as we have enough matching rows.
                // This turns O(outer_size) EXISTS evaluations into O(LIMIT) evaluations.
                let early_termination_target: Option<usize> =
                    if has_correlated && stmt.order_by.is_empty() {
                        let offset = stmt
                            .offset
                            .as_ref()
                            .and_then(|offset_expr| {
                                ExpressionEval::compile(offset_expr, &[])
                                    .ok()
                                    .and_then(|e| e.with_context(ctx).eval_slice(&Row::new()).ok())
                                    .and_then(|v| {
                                        if let Value::Integer(o) = v {
                                            Some(o.max(0) as usize)
                                        } else {
                                            None
                                        }
                                    })
                            })
                            .unwrap_or(0);

                        stmt.limit.as_ref().and_then(|limit_expr| {
                            ExpressionEval::compile(limit_expr, &[])
                                .ok()
                                .and_then(|e| e.with_context(ctx).eval_slice(&Row::new()).ok())
                                .and_then(|v| {
                                    if let Value::Integer(l) = v {
                                        Some(offset + l.max(0) as usize)
                                    } else {
                                        None
                                    }
                                })
                        })
                    } else {
                        None
                    };

                // Create scanner for the correlated subquery path
                // For correlated subqueries, we can't push down the WHERE clause to storage
                // because it depends on outer row values that change per row
                let mut scanner = access_scan::open_scan(
                    table.as_ref(),
                    &column_idx_vec,
                    storage_expr.as_deref(),
                    ctx,
                )?;

                // Pre-allocate to reduce reallocations - 64 avoids first 6 grow operations
                let mut rows: RowVec = RowVec::with_capacity(64);
                let mut row_count = 0u64;
                while scanner.next() {
                    // Check for cancellation every 100 rows (more frequent for slow queries)
                    row_count += 1;
                    if row_count.is_multiple_of(100) {
                        ctx.check_cancelled()?;
                    }

                    let row = scanner.take_row();

                    // Apply in-memory WHERE filter if needed (for complex expressions or subqueries)
                    if let (Some(ref where_expr), Some(ref mut evaluator)) =
                        (&processed_where, &mut eval)
                    {
                        evaluator.set_row_array(&row);

                        // For correlated subqueries, process per-row with outer context
                        if has_correlated {
                            // OPTIMIZATION: Clear and reuse outer_row_map instead of creating new
                            outer_row_map.clear();

                            // OPTIMIZATION: Copy parent outer context if exists (for nested correlated subqueries)
                            // Parent context doesn't change per row, but we need to clone since
                            // std::mem::take moves the map out each iteration
                            if let Some(parent_outer_row) = ctx.outer_row() {
                                outer_row_map.extend(
                                    parent_outer_row.iter().map(|(k, v)| (k.clone(), v.clone())),
                                );
                            }

                            // Use pre-computed column mappings
                            if let Some(ref keys) = column_keys {
                                for mapping in keys {
                                    if let Some(value) = row.get(mapping.index) {
                                        // OPTIMIZATION: Clone value once and reuse for all key insertions
                                        // Previously we cloned 2-3 times per column
                                        let cloned_value = value.clone();

                                        // Insert with unqualified part first (if column had a dot)
                                        if let Some(ref upart) = mapping.unqualified_part {
                                            outer_row_map
                                                .insert(upart.clone(), cloned_value.clone());
                                        }

                                        // Insert with qualified name if available
                                        if let Some(ref qname) = mapping.qualified_name {
                                            outer_row_map
                                                .insert(qname.clone(), cloned_value.clone());
                                        }

                                        // Insert with lowercase column name (move, no clone)
                                        outer_row_map
                                            .insert(mapping.col_lower.clone(), cloned_value);
                                    }
                                }
                            }

                            // Create context with outer row (cheap due to Arc)
                            // SAFETY: all_columns_arc is always Some when has_correlated is true
                            let mut correlated_ctx = ctx.with_outer_row(
                                std::mem::take(&mut outer_row_map),
                                all_columns_arc.clone().unwrap(), // Arc clone = cheap
                            );

                            // FAST PATH: If WHERE is just EXISTS or NOT EXISTS, evaluate directly
                            // without creating AST nodes. This saves ~2-3μs per row.
                            let (result, used_evaluator) = if let Expression::Exists(exists) =
                                where_expr
                            {
                                let r = self
                                    .execute_exists_subquery(&exists.subquery, &correlated_ctx)?;
                                (r, false)
                            } else if let Expression::Prefix(prefix) = where_expr {
                                if prefix.operator.eq_ignore_ascii_case("NOT") {
                                    if let Expression::Exists(exists) = prefix.right.as_ref() {
                                        let r = !self.execute_exists_subquery(
                                            &exists.subquery,
                                            &correlated_ctx,
                                        )?;
                                        (r, false)
                                    } else {
                                        // Not a simple NOT EXISTS, use standard path
                                        let processed = self.process_correlated_where(
                                            where_expr,
                                            &correlated_ctx,
                                        )?;
                                        ctx.check_cancelled()?;
                                        evaluator.set_outer_row_owned(
                                            correlated_ctx.take_outer_row().unwrap_or_default(),
                                        );
                                        evaluator.set_row_array(&row);
                                        let r = evaluator.evaluate_bool(&processed)?;
                                        (r, true)
                                    }
                                } else {
                                    // Not EXISTS/NOT EXISTS, use standard path
                                    let processed =
                                        self.process_correlated_where(where_expr, &correlated_ctx)?;
                                    ctx.check_cancelled()?;
                                    evaluator.set_outer_row_owned(
                                        correlated_ctx.take_outer_row().unwrap_or_default(),
                                    );
                                    evaluator.set_row_array(&row);
                                    let r = evaluator.evaluate_bool(&processed)?;
                                    (r, true)
                                }
                            } else {
                                // Complex WHERE expression, use standard path
                                let processed =
                                    self.process_correlated_where(where_expr, &correlated_ctx)?;

                                // Check for cancellation after processing each correlated subquery
                                // This is critical for slow correlated subqueries
                                ctx.check_cancelled()?;

                                // OPTIMIZATION: Take ownership instead of cloning - avoids HashMap clone
                                evaluator.set_outer_row_owned(
                                    correlated_ctx.take_outer_row().unwrap_or_default(),
                                );
                                evaluator.set_row_array(&row);

                                (evaluator.evaluate_bool(&processed)?, true)
                            };

                            // Take back the map for reuse
                            if used_evaluator {
                                outer_row_map = evaluator.take_outer_row();
                            } else {
                                outer_row_map = correlated_ctx.take_outer_row().unwrap_or_default();
                            }

                            if !result {
                                continue;
                            }
                        } else {
                            // Standard evaluation for non-correlated subqueries
                            if !evaluator.evaluate_bool(where_expr)? {
                                continue;
                            }
                        }
                    }

                    rows.push((row_count as i64, row));

                    // LIMIT EARLY TERMINATION: Stop if we have enough rows
                    if let Some(target) = early_termination_target {
                        if rows.len() >= target {
                            break;
                        }
                    }
                }
                CollectedTableRows::full(rows)
            }
        } else if storage_expr.is_some() {
            // Path 2: WHERE clause with pushdown - use scanner for index optimization
            if let Some(plan) = &ordered_projection_scan_plan {
                let scanner = access_scan::open_exact_projection_scan(
                    table.as_ref(),
                    &plan.scan_indices,
                    storage_expr.as_deref(),
                    ctx,
                )?;
                let rows = self.collect_scanner_rows(scanner, ctx)?;
                CollectedTableRows::projected(rows, plan.scan_columns.clone())
            } else {
                // Fallback: downstream projection/order evaluation still uses the full source schema.
                let column_idx_vec: Vec<usize> = (0..all_columns.len()).collect();
                let scanner = access_scan::open_scan(
                    table.as_ref(),
                    &column_idx_vec,
                    storage_expr.as_deref(),
                    ctx,
                )?;
                let rows = self.collect_scanner_rows(scanner, ctx)?;
                CollectedTableRows::full(rows)
            }
        } else {
            // Path 3: Full scan without WHERE - use collect_all_rows
            // Projection is handled later by the executor which is more efficient
            //
            // OPTIMIZATION: For window functions, check if we can use index-based fetching:
            // 1. PARTITION BY on indexed column -> fetch rows grouped by partition
            // 2. ORDER BY on indexed column -> fetch rows in sorted order
            let has_window = classification.has_window_functions;
            let has_agg = classification.has_aggregation;

            if let Some(plan) = &ordered_projection_scan_plan {
                let scanner = access_scan::open_exact_projection_scan(
                    table.as_ref(),
                    &plan.scan_indices,
                    None,
                    ctx,
                )?;
                let rows = self.collect_scanner_rows(scanner, ctx)?;
                CollectedTableRows::projected(rows, plan.scan_columns.clone())
            } else if has_window && !has_agg {
                // First try PARTITION BY optimization (bigger speedup, avoids O(n) hashing)
                if let Some(partition_col) = Self::extract_window_partition_info(stmt) {
                    let col_lower = partition_col.to_lowercase();
                    let schema = table.schema();
                    let pk_columns = schema.primary_key_columns();
                    let is_pk = pk_columns.len() == 1 && pk_columns[0].name_lower == col_lower;
                    let has_index = is_pk || table.get_index_on_column(&partition_col).is_some();

                    if has_index {
                        // OPTIMIZATION: If we have LIMIT without ORDER BY, use lazy partition fetching
                        // This avoids fetching all partitions when only a few are needed
                        // NOTE: Cannot use this optimization if there's a top-level ORDER BY
                        // because we need all rows to sort before applying LIMIT
                        let has_order_by = !stmt.order_by.is_empty();
                        if !has_order_by {
                            if let Some(limit_expr) = &stmt.limit {
                                if let Expression::IntegerLiteral(lit) = limit_expr.as_ref() {
                                    if lit.value > 0 {
                                        let limit_val = lit.value as usize;
                                        // Use lazy partition fetching - returns early!
                                        let result = self
                                            .execute_select_with_window_functions_lazy_partition(
                                                stmt,
                                                ctx,
                                                table.as_ref(),
                                                &all_columns,
                                                &partition_col,
                                                limit_val,
                                            );
                                        if let Ok(query_result) = result {
                                            let columns =
                                                CompactArc::new(query_result.columns().to_vec());
                                            return Ok((query_result, columns, false, None));
                                        }
                                        // Fall through to regular path if optimization fails
                                    }
                                }
                            }
                        }

                        // Regular path: Fetch rows grouped by partition (no hash grouping needed)
                        if let Some(grouped_data) =
                            table.collect_rows_grouped_by_partition(&partition_col)
                        {
                            // Flatten rows and build partition map
                            let mut all_rows = RowVec::new();
                            let mut partition_map: rustc_hash::FxHashMap<
                                smallvec::SmallVec<[Value; 4]>,
                                Vec<usize>,
                            > = rustc_hash::FxHashMap::default();

                            for (partition_value, partition_rows) in grouped_data {
                                let start_idx = all_rows.len();
                                let partition_size = partition_rows.len();
                                // Extend RowVec with (row_id, Row) tuples
                                for item in partition_rows {
                                    all_rows.push(item);
                                }

                                // Build partition key and indices
                                let key: smallvec::SmallVec<[Value; 4]> =
                                    smallvec::smallvec![partition_value];
                                let indices: Vec<usize> =
                                    (start_idx..start_idx + partition_size).collect();
                                partition_map.insert(key, indices);
                            }

                            CollectedTableRows::full_pregrouped(
                                all_rows,
                                WindowPreGroupedState {
                                    partition_map,
                                    partition_column: col_lower.clone(),
                                },
                            )
                        } else {
                            CollectedTableRows::full(table.collect_all_rows(None)?)
                        }
                    } else {
                        CollectedTableRows::full(table.collect_all_rows(None)?)
                    }
                }
                // Then try ORDER BY optimization (avoids sorting)
                else if let Some((col_name, ascending)) = Self::extract_window_order_info(stmt) {
                    let col_lower = col_name.to_lowercase();
                    let schema = table.schema();
                    let pk_columns = schema.primary_key_columns();
                    let is_pk = pk_columns.len() == 1 && pk_columns[0].name_lower == col_lower;
                    // The ordered secondary-index API uses structural Value
                    // ordering (NULL first), which is not a complete SQL NULL
                    // placement contract. It is nevertheless exact when the
                    // current index proves that it contains no NULL key. A
                    // single-column primary key is non-NULL by definition.
                    let has_index = is_pk
                        || table.get_index_on_column(&col_name).is_some_and(|index| {
                            index.get_all_values().iter().all(|value| !value.is_null())
                        });

                    if has_index {
                        // OPTIMIZATION: If we have LIMIT without top-level ORDER BY,
                        // push the limit down to fetch only needed rows.
                        // This is safe ONLY for window functions that don't depend on total row count.
                        // Safe: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE
                        // Unsafe: NTILE, PERCENT_RANK, CUME_DIST (need total count)
                        let has_order_by = !stmt.order_by.is_empty();
                        let is_window_safe = Self::is_window_safe_for_limit_pushdown(stmt);
                        let fetch_limit = if !has_order_by && is_window_safe {
                            if let Some(limit_expr) = &stmt.limit {
                                if let Expression::IntegerLiteral(lit) = limit_expr.as_ref() {
                                    if lit.value > 0 {
                                        lit.value as usize
                                    } else {
                                        usize::MAX
                                    }
                                } else {
                                    usize::MAX
                                }
                            } else {
                                usize::MAX
                            }
                        } else {
                            usize::MAX
                        };

                        // Fetch rows in sorted order from the index (no re-fetch needed)
                        if let Some(sorted_rows) = table.collect_rows_ordered_by_index(
                            &col_name,
                            ascending,
                            fetch_limit,
                            0,
                        ) {
                            CollectedTableRows::full_presorted(
                                sorted_rows,
                                WindowPreSortedState {
                                    column: col_lower,
                                    ascending,
                                },
                            )
                        } else {
                            CollectedTableRows::full(table.collect_all_rows(None)?)
                        }
                    } else {
                        CollectedTableRows::full(table.collect_all_rows(None)?)
                    }
                } else {
                    CollectedTableRows::full(table.collect_all_rows(None)?)
                }
            } else {
                CollectedTableRows::full(table.collect_all_rows(None)?)
            }
        };

        // Destructure: rows and optional window optimization states
        let CollectedTableRows {
            rows,
            window_presorted_state,
            window_pregrouped_state,
            source_columns: row_source_columns,
        } = rows_result;
        let row_source_columns = row_source_columns.unwrap_or_else(|| all_columns.clone());
        let row_source_columns_lower: Vec<String> = row_source_columns
            .iter()
            .map(|column| column.to_lowercase())
            .collect();
        let row_source_is_full = row_source_columns.len() == all_columns.len()
            && row_source_columns
                .iter()
                .zip(all_columns.iter())
                .all(|(source, full)| source.eq_ignore_ascii_case(full));

        // Record cardinality feedback for future estimate improvements
        // This helps the optimizer learn from actual query execution
        if let Some(where_expr) = where_to_use {
            let actual_rows = rows.len() as u64;
            // Only record feedback if we have a meaningful predicate and enough rows
            if actual_rows >= 10 || rows.is_empty() {
                if let Some(estimated_rows) = self
                    .get_query_planner()
                    .estimate_scan_rows(table_name, Some(where_expr))
                {
                    self.get_query_planner().record_feedback(
                        table_name,
                        where_expr,
                        None, // Column-specific feedback not yet implemented
                        estimated_rows,
                        actual_rows,
                    );
                }
            }
        }

        // Handle the combination of window functions and aggregation
        // Order: Aggregation first (GROUP BY), then window functions
        let has_window = classification.has_window_functions;
        let has_agg = classification.has_aggregation;

        if has_agg && has_window {
            // Both aggregation and window functions:
            // 1. First apply GROUP BY aggregation
            // 2. Then apply window functions on the aggregated result
            let agg_result = self.execute_aggregation_for_window(stmt, ctx, &rows, &all_columns)?;
            let agg_columns = agg_result.0.clone();
            let agg_rows = agg_result.1;

            // Apply window functions on aggregated rows
            let result =
                self.execute_select_with_window_functions(stmt, ctx, &agg_rows, &agg_columns)?;
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Check if we need window functions only (no aggregation)
        if has_window {
            // Use optimized paths if rows were pre-fetched with index optimization
            let result = if let Some(pregrouped) = window_pregrouped_state {
                // PARTITION BY optimization: rows are already grouped by partition
                self.execute_select_with_window_functions_pregrouped(
                    stmt,
                    ctx,
                    &rows,
                    &all_columns,
                    pregrouped,
                )?
            } else if window_presorted_state.is_some() {
                // ORDER BY optimization: rows are already sorted
                self.execute_select_with_window_functions_presorted(
                    stmt,
                    ctx,
                    &rows,
                    &all_columns,
                    window_presorted_state,
                )?
            } else {
                // Default path: no optimization
                self.execute_select_with_window_functions(stmt, ctx, &rows, &all_columns)?
            };
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Check if we need aggregation only (no window functions)
        if has_agg {
            let result = self.execute_select_with_aggregation(stmt, ctx, rows, &all_columns)?;
            let columns = CompactArc::new(result.columns().to_vec());
            return Ok((result, columns, false, None));
        }

        // Project rows according to SELECT expressions
        // Check if deferred projection is applicable (ORDER BY + LIMIT with simple columns)
        // This reduces allocations from O(matched_rows) to O(limit)
        let deferred_projection_info = self.get_deferred_projection_info(
            stmt,
            &row_source_columns_lower,
            &row_source_columns,
            classification,
        );

        let (projected_rows, output_columns, deferred_proj) = if order_by_needs_extra_columns {
            // When ORDER BY references columns not in SELECT, we need to:
            // 1. Include those columns in the output (appended at end)
            // 2. Sort will happen in execute_select
            // 3. Extra columns will be projected out after sorting
            let (projected_rows, extra_columns) = self.project_rows_with_order_by(
                &stmt.columns,
                &stmt.order_by,
                &stmt.distinct_on,
                rows,
                &row_source_columns,
                ctx,
            )?;
            // Get base column names
            let mut output_columns =
                self.get_output_column_names(&stmt.columns, &all_columns, table_alias.as_deref());
            // The projection helper is the single source of truth for both the
            // hidden value order and the corresponding column names.
            output_columns.extend(extra_columns);
            (projected_rows, output_columns, None)
        } else if let Some((col_indices, output_names)) = deferred_projection_info {
            // DEFERRED PROJECTION: Skip projection now, do it after ORDER BY + LIMIT
            // Return all source columns; projection will be applied after TopNResult
            (
                rows,
                row_source_columns.to_vec(),
                Some((col_indices, output_names)),
            )
        } else {
            // Standard projection
            let projected_rows = self.project_rows_with_alias(
                &stmt.columns,
                rows,
                &row_source_columns,
                Some(&row_source_columns_lower),
                ctx,
                table_alias.as_deref(),
            )?;
            let output_columns =
                self.get_output_column_names(&stmt.columns, &all_columns, table_alias.as_deref());
            (projected_rows, output_columns, None)
        };

        // SEMANTIC CACHE: Insert result for eligible queries
        // For SELECT * queries, cache the raw rows before returning
        //
        // Note: The cache stores Vec<Row>, so we extract rows from RowVec for caching.
        // The result keeps the original RowVec.
        // Skip caching when deferred projection is used (rows are not projected yet)
        if cache_eligible && deferred_proj.is_none() && row_source_is_full {
            if let Some(where_expr) = where_to_use {
                // Clone rows for cache (cache needs Vec<Row>)
                let rows_for_cache: Vec<Row> = projected_rows.rows().cloned().collect();
                self.semantic_cache.insert_if_generation(
                    semantic_cache_generation.expect("eligible cache query has generation"),
                    table_name,
                    all_columns.clone(),
                    rows_for_cache,
                    Some(where_expr.clone()),
                );
            }
        }

        let output_columns = CompactArc::new(output_columns);
        let result =
            ExecutorResult::with_arc_columns(CompactArc::clone(&output_columns), projected_rows);
        Ok((Box::new(result), output_columns, false, deferred_proj))
    }
}
