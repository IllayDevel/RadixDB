impl Executor {
    fn validate_explain_select_sources(&self, select: &SelectStatement) -> Result<()> {
        self.validate_explain_select_sources_in_scope(select, &AHashSet::new())
    }

    fn validate_explain_select_sources_in_scope(
        &self,
        select: &SelectStatement,
        inherited_ctes: &AHashSet<String>,
    ) -> Result<()> {
        let mut cte_scope = inherited_ctes.clone();
        if let Some(with_clause) = &select.with {
            cte_scope.extend(
                with_clause
                    .ctes
                    .iter()
                    .map(|cte| cte.name.value_lower.to_string()),
            );
            for cte in &with_clause.ctes {
                self.validate_explain_select_sources_in_scope(&cte.query, &cte_scope)?;
            }
        }
        if let Some(source) = select.table_expr.as_deref() {
            self.validate_explain_table_source(source, &cte_scope)?;
        }
        for set_operation in &select.set_operations {
            self.validate_explain_select_sources_in_scope(&set_operation.right, &cte_scope)?;
        }
        Ok(())
    }

    fn validate_explain_table_source(
        &self,
        source: &Expression,
        cte_scope: &AHashSet<String>,
    ) -> Result<()> {
        match source {
            Expression::TableSource(table) => {
                if cte_scope.contains(table.name.value_lower.as_str()) {
                    return Ok(());
                }
                let transaction = self.engine.begin_transaction()?;
                match transaction.get_table(table.name.value.as_str()) {
                    Ok(_) => Ok(()),
                    Err(error @ radixdb_core::Error::TableNotFound(_)) => {
                        if self
                            .visible_view_lowercase(table.name.value_lower.as_str())?
                            .is_some()
                        {
                            Ok(())
                        } else {
                            Err(error)
                        }
                    }
                    Err(error) => Err(error),
                }
            }
            Expression::JoinSource(join) => {
                self.validate_explain_table_source(&join.left, cte_scope)?;
                self.validate_explain_table_source(&join.right, cte_scope)
            }
            Expression::SubquerySource(subquery) => {
                self.validate_explain_select_sources_in_scope(&subquery.subquery, cte_scope)
            }
            Expression::Aliased(aliased) => {
                self.validate_explain_table_source(&aliased.expression, cte_scope)
            }
            Expression::ValuesSource(_)
            | Expression::FunctionTableSource(_)
            | Expression::CteReference(_) => Ok(()),
            _ => Ok(()),
        }
    }

    /// Execute EXPLAIN statement - shows query plan
    pub(crate) fn execute_explain(
        &self,
        stmt: &ExplainStatement,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let mut plan_lines: Vec<String> = Vec::new();

        let navigation_plan = if let Statement::Select(select) = stmt.statement.as_ref() {
            super::navigation::bind_reference_expand_plan(self.engine.as_ref(), select)?
        } else {
            None
        };
        if let Some(plan) = navigation_plan.as_ref() {
            if !stmt.analyze {
                plan_lines = plan.explain_lines(self.engine.as_ref())?;
                let columns = vec!["plan".to_string()];
                let rows: RowVec = plan_lines
                    .into_iter()
                    .enumerate()
                    .map(|(i, line)| {
                        (
                            i as i64,
                            Row::from_values(vec![Value::Text(SmartString::from_string(line))]),
                        )
                    })
                    .collect();
                return Ok(Box::new(ExecutorResult::new(columns, rows)));
            }
        }

        // For SELECT statements with CTEs, try to get the inlined version for accurate EXPLAIN
        let inlined_statement: Option<Box<Statement>> =
            if let Statement::Select(select) = &*stmt.statement {
                if let Some(ref with_clause) = select.with {
                    self.try_inline_ctes(select, with_clause)
                        .map(|inlined| Box::new(Statement::Select(inlined)))
                } else {
                    None
                }
            } else {
                None
            };

        // Use inlined statement if available, otherwise original
        let explain_stmt = inlined_statement.as_ref().unwrap_or(&stmt.statement);

        if stmt.analyze {
            // EXPLAIN ANALYZE: Execute the query and collect statistics
            let start = radixdb_core::time_compat::Instant::now();
            radixdb_storage::instrumentation::begin_metadata_pk_count_probe();
            radixdb_storage::instrumentation::begin_artifact_columnar_group_probe();
            radixdb_storage::instrumentation::begin_join_execution_probe();
            radixdb_storage::instrumentation::begin_join_planning_probe();
            crate::query::begin_plugin_planner_probe();
            let (execution, navigation_metrics) =
                match (navigation_plan.as_ref(), stmt.statement.as_ref()) {
                    (Some(plan), Statement::Select(select)) => {
                        let (result, metrics) =
                            self.execute_reference_projection_with_metrics(select, plan, ctx)?;
                        (Ok(result), Some(metrics))
                    }
                    _ => (self.execute_statement(&stmt.statement, ctx), None),
                };
            let mut result = match execution {
                Ok(result) => result,
                Err(error) => {
                    let _ = radixdb_storage::instrumentation::end_artifact_columnar_group_probe();
                    let _ = radixdb_storage::instrumentation::end_metadata_pk_count_probe();
                    let _ = radixdb_storage::instrumentation::end_join_execution_probe_with_trace();
                    let _ = radixdb_storage::instrumentation::end_join_planning_probe();
                    let _ = crate::query::end_plugin_planner_probe();
                    return Err(error);
                }
            };

            // Drain the result while request-local probes are still active.
            // JOIN operators are lazy pull cursors, so ending the probes before
            // this loop would omit most physical work from EXPLAIN ANALYZE.
            let mut row_count = 0usize;
            while result.next() {
                row_count += 1;
            }
            let execution_error = result.last_error();
            // Operator wrappers publish terminal counters and release their
            // request memory on close/drop. Keep probes active through that
            // boundary as well.
            drop(result);
            let artifact_columnar_group = radixdb_storage::instrumentation::end_artifact_columnar_group_probe();
            let metadata_pk_count = radixdb_storage::instrumentation::end_metadata_pk_count_probe();
            let (join_execution, join_execution_trace) =
                radixdb_storage::instrumentation::end_join_execution_probe_with_trace();
            let join_planning = radixdb_storage::instrumentation::end_join_planning_probe();
            let plugin_planner = crate::query::end_plugin_planner_probe();
            let duration = start.elapsed();
            let join_peak_memory_bytes = ctx.peak_join_memory_bytes();
            let join_retained_memory_bytes = ctx.retained_join_memory_bytes();
            if let Some(error) = execution_error {
                return Err(error);
            }

            // Format duration nicely
            let time_str = if duration.as_secs() > 0 {
                format!("{:.2}s", duration.as_secs_f64())
            } else if duration.as_millis() > 0 {
                format!(
                    "{:.2}ms",
                    duration.as_millis() as f64 + (duration.as_micros() % 1000) as f64 / 1000.0
                )
            } else {
                format!("{:.2}µs", duration.as_micros() as f64)
            };

            // Generate plan with actual statistics (using inlined version if available)
            let analyze_stats = ExplainAnalyzeStats {
                row_count,
                time_str: &time_str,
                query_wall_nanos: u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX),
                join_peak_memory_bytes,
                join_retained_memory_bytes,
                metadata_pk_count,
                artifact_columnar_group,
                join_execution,
                join_execution_trace: &join_execution_trace,
                join_planning: &join_planning,
                plugin_planner,
                artifact_cache: ArtifactCacheExplainStats::default(),
            };
            self.explain_statement_with_stats(explain_stmt, &mut plan_lines, 0, analyze_stats, ctx);
            if let Some(plan) = navigation_plan {
                plan_lines.extend(plan.explain_lines_with_metrics(
                    self.engine.as_ref(),
                    navigation_metrics.as_ref(),
                )?);
            }

            // Return the plan as a result
            let columns = vec!["plan".to_string()];
            let rows: RowVec = plan_lines
                .into_iter()
                .enumerate()
                .map(|(i, line)| {
                    (
                        i as i64,
                        Row::from_values(vec![Value::Text(SmartString::from_string(line))]),
                    )
                })
                .collect();

            Ok(Box::new(ExecutorResult::new(columns, rows)))
        } else {
            // Regular EXPLAIN: Just show the plan without executing (using inlined version if available)
            if let Statement::Select(select) = explain_stmt.as_ref() {
                self.validate_explain_select_sources(select)?;
            }
            self.explain_statement(explain_stmt, &mut plan_lines, 0, ctx);

            // Return as a single-column result
            let columns = vec!["plan".to_string()];
            let rows: RowVec = plan_lines
                .into_iter()
                .enumerate()
                .map(|(i, line)| {
                    (
                        i as i64,
                        Row::from_values(vec![Value::Text(SmartString::from_string(line))]),
                    )
                })
                .collect();

            Ok(Box::new(ExecutorResult::new(columns, rows)))
        }
    }

    /// Generate EXPLAIN output with actual execution statistics
    fn explain_statement_with_stats(
        &self,
        stmt: &Statement,
        lines: &mut Vec<String>,
        indent: usize,
        stats: ExplainAnalyzeStats<'_>,
        ctx: &ExecutionContext,
    ) {
        let prefix = "  ".repeat(indent);

        match stmt {
            Statement::Select(select) => {
                let distinct_str = if select.distinct { " DISTINCT" } else { "" };
                lines.push(format!(
                    "{}SELECT{} (actual time={}, rows={})",
                    prefix, distinct_str, stats.time_str, stats.row_count
                ));
                self.explain_select_columns(select, lines, indent);
                self.append_select_path_debug(
                    select,
                    lines,
                    indent,
                    Some(stats.metadata_pk_count),
                    Some(stats.artifact_columnar_group),
                );
                if stats.plugin_planner.attempts != 0 {
                    lines.push(format!(
                        "{}  Plugin Candidate Scan (attempts={}, plans={}, spans={}, candidates={}, rechecks={}, fallbacks={})",
                        prefix,
                        stats.plugin_planner.attempts,
                        stats.plugin_planner.plans,
                        stats.plugin_planner.spans,
                        stats.plugin_planner.candidates,
                        stats.plugin_planner.rechecks,
                        stats.plugin_planner.fallbacks,
                    ));
                }
                Self::append_artifact_cache_execution_details(
                    lines,
                    &"  ".repeat(indent + 1),
                    stats.artifact_cache,
                );
                Self::append_join_execution_details(
                    lines,
                    &"  ".repeat(indent + 1),
                    stats.join_execution,
                );
                Self::append_join_execution_trace(
                    lines,
                    &"  ".repeat(indent + 1),
                    stats.join_execution_trace,
                );
                Self::append_join_planning_details(
                    lines,
                    &"  ".repeat(indent + 1),
                    stats.join_planning,
                    stats.join_execution,
                );
                Self::append_join_phase_details(
                    lines,
                    &"  ".repeat(indent + 1),
                    stats.join_planning,
                    stats.join_execution,
                    stats.query_wall_nanos,
                );
                Self::append_join_memory_details(
                    lines,
                    &"  ".repeat(indent + 1),
                    stats.join_execution,
                    stats.join_peak_memory_bytes,
                    stats.join_retained_memory_bytes,
                );

                // FROM clause with access plan
                // Check for vector search fast path first
                let vector_plan = if let Some(ref table_expr) = select.table_expr {
                    if let Some(table_name) = extract_table_name(table_expr) {
                        self.detect_vector_search_plan(select, &table_name)
                    } else {
                        None
                    }
                } else {
                    None
                };

                if stats.metadata_pk_count.applied > 0 {
                    Self::append_metadata_pk_count_execution_details(
                        lines,
                        &"  ".repeat(indent + 1),
                        stats.metadata_pk_count,
                    );
                } else if stats.artifact_columnar_group.applies > 0 {
                    Self::append_artifact_columnar_group_execution_details(
                        lines,
                        &"  ".repeat(indent + 1),
                        stats.artifact_columnar_group,
                        stats.row_count,
                    );
                } else if let Some(ref vplan) = vector_plan {
                    let inner_prefix = "  ".repeat(indent + 1);
                    Self::append_scan_plan_lines(lines, &inner_prefix, vplan, None);
                    lines.push(format!(
                        "{}   Vector Access: runtime candidate (operator not instrumented)",
                        inner_prefix
                    ));
                } else if let Some(ref table_expr) = select.table_expr {
                    let classification =
                        super::query_classification::QueryClassification::classify(select);
                    self.explain_table_expr_with_where(
                        table_expr,
                        select.where_clause.as_deref(),
                        Some(&select.columns),
                        !classification.has_window_functions
                            && !classification.has_aggregation
                            && !classification.has_group_by,
                        lines,
                        indent + 1,
                        ctx,
                    );
                }

                // GROUP BY (including ROLLUP, CUBE, GROUPING SETS)
                {
                    let gb_str = format!("{}", select.group_by);
                    if !gb_str.is_empty() {
                        lines.push(format!("{}  Group By: {}", prefix, gb_str));
                    }
                }

                // HAVING
                if let Some(ref having) = select.having {
                    lines.push(format!("{}  Having: {}", prefix, having));
                }

                // ORDER BY
                if !select.order_by.is_empty() {
                    let orders: Vec<String> =
                        select.order_by.iter().map(|o| format!("{}", o)).collect();
                    lines.push(format!("{}  Order By: {}", prefix, orders.join(", ")));
                }

                // LIMIT/OFFSET
                if let Some(ref limit) = select.limit {
                    lines.push(format!("{}  Limit: {}", prefix, limit));
                }
                if let Some(ref offset) = select.offset {
                    lines.push(format!("{}  Offset: {}", prefix, offset));
                }
            }
            Statement::Insert(insert) => {
                lines.push(format!(
                    "{}INSERT INTO {} (actual time={}, rows={})",
                    prefix, insert.table_name, stats.time_str, stats.row_count
                ));
                if let Some(ref select) = insert.select {
                    lines.push(format!("{}  Source:", prefix));
                    self.explain_select(select, lines, indent + 2, ctx);
                } else {
                    lines.push(format!(
                        "{}  Values: {} row(s)",
                        prefix,
                        insert.values.len()
                    ));
                }
            }
            Statement::Update(update) => {
                lines.push(format!(
                    "{}UPDATE {} (actual time={}, rows={})",
                    prefix, update.table_name, stats.time_str, stats.row_count
                ));
                lines.push(format!(
                    "{}  Set: {} column(s)",
                    prefix,
                    update.updates.len()
                ));
                if let Some(ref where_clause) = update.where_clause {
                    lines.push(format!("{}  Filter: {}", prefix, where_clause));
                }
            }
            Statement::Delete(delete) => {
                lines.push(format!(
                    "{}DELETE FROM {} (actual time={}, rows={})",
                    prefix, delete.table_name, stats.time_str, stats.row_count
                ));
                if let Some(ref where_clause) = delete.where_clause {
                    lines.push(format!("{}  Filter: {}", prefix, where_clause));
                }
                self.append_delete_access_path(delete, lines, indent, ctx);
            }
            _ => {
                lines.push(format!(
                    "{}Statement: {} (actual time={}, rows={})",
                    prefix, stmt, stats.time_str, stats.row_count
                ));
            }
        }
    }

    /// Helper to show just the SELECT columns
    fn explain_select_columns(
        &self,
        select: &SelectStatement,
        lines: &mut Vec<String>,
        indent: usize,
    ) {
        let prefix = "  ".repeat(indent);

        // Show columns
        let col_count = select.columns.len();
        if col_count <= 5 {
            let cols: Vec<String> = select.columns.iter().map(|c| format!("{}", c)).collect();
            lines.push(format!("{}  Columns: {}", prefix, cols.join(", ")));
        } else {
            lines.push(format!("{}  Columns: {} column(s)", prefix, col_count));
        }
    }

    fn append_scan_plan_lines(
        lines: &mut Vec<String>,
        prefix: &str,
        scan_plan: &ScanPlan,
        first_line_suffix: Option<String>,
    ) {
        let suffix = first_line_suffix.unwrap_or_default();
        let plan_str = format!("{}", scan_plan);
        for (i, line) in plan_str.lines().enumerate() {
            if i == 0 {
                lines.push(format!("{}-> {}{}", prefix, line, suffix));
            } else {
                lines.push(format!("{}   {}", prefix, line));
            }
        }
        for line in scan_plan.stable_explain_lines() {
            lines.push(format!("{}   {}", prefix, line));
        }
    }

    fn append_scan_projection_path_lines(
        lines: &mut Vec<String>,
        prefix: &str,
        scan_plan: &ScanPlan,
        select_columns: Option<&[Expression]>,
    ) {
        let Some(path) = Self::classify_scan_projection_path(scan_plan, select_columns) else {
            return;
        };
        lines.push(format!(
            "{}   Scan Runtime: artifact_prefetch.nvme_cpu_saturation",
            prefix
        ));
        lines.push(format!("{}   Scan Projection Path: {}", prefix, path));
    }

    fn append_join_execution_details(
        lines: &mut Vec<String>,
        prefix: &str,
        probe: radixdb_storage::instrumentation::JoinExecutionProbeSnapshot,
    ) {
        if probe.operator_calls == 0 {
            return;
        }
        lines.push(format!(
            "{}Physical JOIN Summary: operators={}, inputs=({}, {}), outputs={}, values=({} -> {}), max_width={}, candidates={}, lookups={}, lookup_candidates={}, wall={:.3}ms",
            prefix,
            probe.operator_calls,
            probe.left_input_rows,
            probe.right_input_rows,
            probe.output_rows,
            probe.input_values,
            probe.output_values,
            probe.max_output_width,
            probe.candidate_pairs,
            probe.lookup_calls,
            probe.lookup_candidate_rows,
            probe.wall_nanos as f64 / 1_000_000.0,
        ));
        if let Some(kind) = probe.first_kind {
            lines.push(format!(
                "{}Physical JOIN Root Edge: kind={}, left_rows={}, right_rows={}, output_rows={}",
                prefix,
                kind.stable_name(),
                probe.first_left_input_rows,
                probe.first_right_input_rows,
                probe.first_output_rows,
            ));
        }
        lines.push(format!(
            "{}Physical JOIN Operators: hash_streaming={}, hash_parallel={}, merge={}, nested_loop={}, index_nested_loop={}, batch_index_nested_loop={}",
            prefix,
            probe.hash_streaming_calls,
            probe.hash_parallel_calls,
            probe.merge_calls,
            probe.nested_loop_calls,
            probe.index_nested_loop_calls,
            probe.batch_index_nested_loop_calls,
        ));
        lines.push(format!(
            "{}Physical JOIN Transport: owned_rows={}, deferred_rows={}/{}, deferred_boundary_rows={}, copied_values={}, copied_bytes={}, source_opens={}, source_rescans={}",
            prefix,
            probe.owned_rows_constructed,
            probe.deferred_rows,
            probe.deferred_rows_consumed,
            probe.deferred_boundary_rows,
            probe.copied_values,
            probe.copied_bytes,
            probe.source_open_calls,
            probe.source_rescans,
        ));
        if probe.lookup_key_rows > 0 {
            lines.push(format!(
                "{}Physical JOIN Key Batches: key_rows={}, distinct_keys={}, repeated_keys_eliminated={}",
                prefix,
                probe.lookup_key_rows,
                probe.lookup_distinct_keys,
                probe.lookup_repeated_keys_eliminated,
            ));
            lines.push(format!(
                "{}Physical Batch JOIN Stages: outer_pull_including_child={:.3}ms, key_prepare={:.3}ms, lookup_fetch={:.3}ms, candidate_map={:.3}ms",
                prefix,
                probe.outer_pull_nanos as f64 / 1_000_000.0,
                probe.key_prepare_nanos as f64 / 1_000_000.0,
                probe.lookup_nanos as f64 / 1_000_000.0,
                probe.candidate_map_nanos as f64 / 1_000_000.0,
            ));
        }
        if probe.top_n_calls > 0 {
            lines.push(format!(
                "{}Physical JOIN Top-N: calls={}, input_rows={}, output_rows={}, peak_candidates={}, peak_bytes={}",
                prefix,
                probe.top_n_calls,
                probe.top_n_input_rows,
                probe.top_n_output_rows,
                probe.top_n_peak_candidates,
                probe.top_n_peak_bytes,
            ));
        }
        if probe.ordered_sort_calls > 0 {
            let mode = if probe.ordered_sort_spill_runs == 0 {
                "bounded_memory"
            } else {
                "external_merge"
            };
            lines.push(format!(
                "{}Physical ORDER BY: mode={}, calls={}, input_rows={}, spill_runs={}, peak_rows={}, peak_bytes={}, collect={:.3}ms, finalize={:.3}ms",
                prefix,
                mode,
                probe.ordered_sort_calls,
                probe.ordered_sort_input_rows,
                probe.ordered_sort_spill_runs,
                probe.ordered_sort_peak_rows,
                probe.ordered_sort_peak_bytes,
                probe.ordered_collect_nanos as f64 / 1_000_000.0,
                probe.ordered_finalize_nanos as f64 / 1_000_000.0,
            ));
        }
        if probe.ordering_skip_calls > 0 {
            lines.push(format!(
                "{}Physical JOIN Ordering: certified_index_order_skips={}",
                prefix, probe.ordering_skip_calls,
            ));
        }
    }

    fn append_join_planning_details(
        lines: &mut Vec<String>,
        prefix: &str,
        planning: &radixdb_storage::instrumentation::JoinPlanningProbeSnapshot,
        execution: radixdb_storage::instrumentation::JoinExecutionProbeSnapshot,
    ) {
        for (component, record) in planning.components.iter().enumerate() {
            let reordered = record.original_order != record.planned_order;
            lines.push(format!(
                "{}Physical JOIN Costed Order[{}]: {} (original={}, reordered={})",
                prefix,
                component,
                record.planned_order.join(" -> "),
                record.original_order.join(" -> "),
                reordered,
            ));
            lines.push(format!(
                "{}Physical JOIN Cost Basis[{}]: table/column statistics + selectivity + distinctness + index availability + projected width + LIMIT; estimated_root_rows={}, estimated_cost={}, safe_limit={}",
                prefix,
                component,
                record.root_estimated_rows,
                record.estimated_cost,
                record
                    .safe_limit
                    .map_or_else(|| "none".to_string(), |limit| limit.to_string()),
            ));
            if component != 0 || execution.operator_calls == 0 {
                continue;
            }
            let estimated = record.root_estimated_rows;
            let actual = execution.first_left_input_rows;
            let ratio = match (estimated, actual) {
                (0, 0) => 1.0,
                (0, _) | (_, 0) => f64::INFINITY,
                _ => (estimated.max(actual) as f64) / (estimated.min(actual) as f64),
            };
            let diagnostic = if ratio >= 10.0 {
                "join.estimate_extreme_miss"
            } else {
                "join.estimate_within_threshold"
            };
            let ratio = if ratio.is_finite() {
                format!("{ratio:.3}x")
            } else {
                "infinite".to_string()
            };
            lines.push(format!(
                "{}Physical JOIN Estimate Error[{}]: estimated_root_rows={}, actual_root_rows={}, ratio={}, diagnostic={}",
                prefix, component, estimated, actual, ratio, diagnostic,
            ));
        }
    }

    fn append_join_execution_trace(
        lines: &mut Vec<String>,
        prefix: &str,
        trace: &radixdb_storage::instrumentation::JoinExecutionTraceSnapshot,
    ) {
        for item in &trace.records {
            let record = item.record;
            lines.push(format!(
                "{}Physical JOIN Actual[{}]: kind={}, inputs=({}, {}), output={}, widths=({},{},{}), candidates={}, lookups={}/{}, wall={:.3}ms, stages_ms=outer:{:.3}|keys:{:.3}|lookup:{:.3}|candidate_map:{:.3}",
                prefix,
                item.execution_slot,
                item.kind.stable_name(),
                record.left_rows,
                record.right_rows,
                record.output_rows,
                record.left_width,
                record.right_width,
                record.output_width,
                record.candidate_pairs,
                record.lookup_calls,
                record.lookup_candidate_rows,
                record.wall_nanos as f64 / 1_000_000.0,
                record.outer_pull_nanos as f64 / 1_000_000.0,
                record.key_prepare_nanos as f64 / 1_000_000.0,
                record.lookup_nanos as f64 / 1_000_000.0,
                record.candidate_map_nanos as f64 / 1_000_000.0,
            ));
        }
        if trace.truncated_operators > 0 {
            lines.push(format!(
                "{}Physical JOIN Actual Trace: truncated_operators={}, limit=64",
                prefix, trace.truncated_operators,
            ));
        }
    }

    fn append_join_phase_details(
        lines: &mut Vec<String>,
        prefix: &str,
        planning: &radixdb_storage::instrumentation::JoinPlanningProbeSnapshot,
        execution: radixdb_storage::instrumentation::JoinExecutionProbeSnapshot,
        query_wall_nanos: u64,
    ) {
        if execution.operator_calls == 0
            && planning.logical_planning_calls == 0
            && planning.physical_planning_calls == 0
        {
            return;
        }
        let planning_wall = planning
            .logical_planning_nanos
            .saturating_add(planning.physical_planning_nanos);
        let executor_wall = query_wall_nanos.saturating_sub(planning_wall);
        lines.push(format!(
            "{}Physical JOIN Phases: logical_planning={:.3}ms/{} calls, physical_planning={:.3}ms/{} calls, executor_wall={:.3}ms, operator_wall_sum_non_additive={:.3}ms, query_wall={:.3}ms",
            prefix,
            planning.logical_planning_nanos as f64 / 1_000_000.0,
            planning.logical_planning_calls,
            planning.physical_planning_nanos as f64 / 1_000_000.0,
            planning.physical_planning_calls,
            executor_wall as f64 / 1_000_000.0,
            execution.wall_nanos as f64 / 1_000_000.0,
            query_wall_nanos as f64 / 1_000_000.0,
        ));
        lines.push(format!(
            "{}Protocol Boundary: excluded_from_embedded_explain=true, counters=protocol_round_trip_nanos+protocol_encode_nanos+protocol_socket_write_nanos",
            prefix,
        ));
    }

    fn append_join_memory_details(
        lines: &mut Vec<String>,
        prefix: &str,
        execution: radixdb_storage::instrumentation::JoinExecutionProbeSnapshot,
        peak_bytes: usize,
        retained_bytes: usize,
    ) {
        if execution.operator_calls == 0 {
            return;
        }
        lines.push(format!(
            "{}Physical JOIN Memory: request_peak_bytes={}, retained_after_drain_bytes={}, top_n_peak_bytes={}, ordered_peak_bytes={}, spill_runs={}",
            prefix,
            peak_bytes,
            retained_bytes,
            execution.top_n_peak_bytes,
            execution.ordered_sort_peak_bytes,
            execution.ordered_sort_spill_runs,
        ));
    }

    fn classify_scan_projection_path(
        scan_plan: &ScanPlan,
        select_columns: Option<&[Expression]>,
    ) -> Option<&'static str> {
        match scan_plan {
            ScanPlan::SegmentedScan { .. } => {}
            _ => return None,
        }

        let Some(columns) = select_columns else {
            return Some("scan.cold_artifact.unknown_projection");
        };

        if columns.iter().any(Self::is_projection_star_expr) {
            Some("scan.cold_artifact.full_prefetch")
        } else if columns.iter().all(Self::is_simple_column_projection) {
            Some("scan.cold_artifact.projected_prefetch")
        } else if columns
            .iter()
            .all(Self::projection_dependencies_extractable)
        {
            Some("scan.cold_artifact.expression_projected_prefetch")
        } else {
            Some("scan.cold_artifact.full_row_executor_fallback")
        }
    }

    fn append_select_path_debug(
        &self,
        select: &SelectStatement,
        lines: &mut Vec<String>,
        indent: usize,
        metadata_pk_count: Option<radixdb_storage::instrumentation::MetadataPkCountProbeSnapshot>,
        artifact_columnar_group: Option<radixdb_storage::instrumentation::ArtifactColumnarGroupProbeSnapshot>,
    ) {
        let prefix = "  ".repeat(indent);
        lines.push(format!(
            "{}  Projection Boundary: {}",
            prefix,
            self.classify_projection_boundary(select)
        ));

        if metadata_pk_count.is_some_and(|probe| probe.applied > 0) {
            lines.push(format!(
                "{}  Aggregation Path: aggregation.pk_metadata_count",
                prefix
            ));
            return;
        }

        if artifact_columnar_group.is_some_and(|probe| probe.applies > 0) {
            lines.push(format!(
                "{}  Aggregation Path: aggregation.artifact_columnar_group_by",
                prefix
            ));
            return;
        }

        if let Some(reason) = artifact_columnar_group.and_then(Self::artifact_columnar_group_fallback_summary) {
            lines.push(format!(
                "{}  Aggregation Path: aggregation.storage_group_by",
                prefix
            ));
            lines.push(format!(
                "{}  Aggregation Fallback: aggregation.artifact_columnar_group_by:{}",
                prefix, reason
            ));
            return;
        }

        if let Some(range) = self.metadata_pk_count_plan_candidate(select) {
            lines.push(format!(
                "{}  Aggregation Candidate: aggregation.pk_metadata_count (runtime validation required)",
                prefix
            ));
            lines.push(format!(
                "{}  Normalized INTEGER PK Bounds: lower={}, upper={}",
                prefix,
                Self::format_integer_primary_key_bound(range.lower_bound(), "-∞"),
                Self::format_integer_primary_key_bound(range.upper_bound(), "+∞"),
            ));
        }

        if let Some((path, reason)) = self.classify_aggregation_path(select) {
            lines.push(format!("{}  Aggregation Path: {}", prefix, path));
            if let Some(reason) = reason {
                lines.push(format!("{}  Aggregation Fallback: {}", prefix, reason));
            }
        }

        if let Some(probe) = metadata_pk_count.filter(|probe| probe.attempts > 0) {
            lines.push(format!(
                "{}  Metadata PK Count Attempt: fallback (attempts={}, fallbacks={})",
                prefix, probe.attempts, probe.fallbacks
            ));
        }
    }

    /// Determine whether plan-only EXPLAIN can name the narrow metadata-count
    /// candidate without touching table data.  This is deliberately not named
    /// an executed path: MVCC snapshots, a concurrent seal, or a candidate
    /// limit can still make runtime choose the generic aggregate path.
    /// `EXPLAIN ANALYZE` reports that final choice from request-local probes.
    fn metadata_pk_count_plan_candidate(
        &self,
        select: &SelectStatement,
    ) -> Option<radixdb_storage::traits::table::IntegerPrimaryKeyRange> {
        let classification = super::query_classification::QueryClassification::classify(select);
        if !classification.has_where
            || !classification.has_aggregation
            || classification.has_having
            || classification.has_window_functions
            || classification.has_joins
            || classification.has_group_by
            || classification.where_has_parameters
            || classification.where_has_subqueries
            || select.columns.len() != 1
            || select.distinct
            || !select.order_by.is_empty()
        {
            return None;
        }

        let mut expression = &select.columns[0];
        if let Expression::Aliased(aliased) = expression {
            expression = &aliased.expression;
        }
        let Expression::FunctionCall(function) = expression else {
            return None;
        };
        if !function.function.eq_ignore_ascii_case("COUNT")
            || function.is_distinct
            || !function.order_by.is_empty()
            || function.filter.is_some()
            || !matches!(function.arguments.as_slice(), [Expression::Star(_)])
        {
            return None;
        }

        let table_expr = select.table_expr.as_deref()?;
        let table_name = extract_table_name(table_expr)?;
        let tx = self.engine.begin_transaction().ok()?;
        let table = tx.get_table(&table_name).ok()?;
        let schema = table.schema();
        let pk_idx = schema.pk_column_index()?;
        let pk_column = schema.columns.get(pk_idx)?;
        if pk_column.data_type != radixdb_core::DataType::Integer {
            return None;
        }

        let where_expr = select.where_clause.as_deref()?;
        let (storage_expr, needs_memory_filter) = pushdown::try_pushdown(where_expr, schema, None);
        if needs_memory_filter {
            return None;
        }
        let storage_expr = storage_expr?;
        radixdb_storage::traits::table::IntegerPrimaryKeyRange::from_conjunctive_comparisons(
            &storage_expr.collect_comparisons(),
            pk_column.name_lower.as_str(),
            true,
        )
    }

    fn format_integer_primary_key_bound(bound: Option<(i64, bool)>, unbounded: &str) -> String {
        match bound {
            Some((value, true)) => format!("[{value}]"),
            Some((value, false)) => format!("({value})"),
            None => unbounded.to_string(),
        }
    }

    /// Render the physical metadata-only count that was actually executed by
    /// this `EXPLAIN ANALYZE`, rather than a static scan-shaped prediction.
    fn append_metadata_pk_count_execution_details(
        lines: &mut Vec<String>,
        prefix: &str,
        probe: radixdb_storage::instrumentation::MetadataPkCountProbeSnapshot,
    ) {
        lines.push(format!(
            "{}-> Metadata INTEGER PRIMARY KEY Count (actual rows=1)",
            prefix
        ));
        lines.push(format!(
            "{}   Access Path: aggregation.pk_metadata_count",
            prefix
        ));
        lines.push(format!(
            "{}   Access Source: artifact-backed row-id metadata + hot version membership",
            prefix
        ));
        lines.push(format!(
            "{}   Metadata PK Count: intervals={}, candidates={}, visible={}, visibility_exclusions={}, hot_candidates={}",
            prefix,
            probe.intervals,
            probe.candidate_rows,
            probe.visible_rows,
            probe.visibility_exclusions,
            probe.hot_candidates,
        ));
    }

    /// Render the artifact-backed columnar grouped-aggregate operator that was actually
    /// executed by this `EXPLAIN ANALYZE`.
    fn append_artifact_columnar_group_execution_details(
        lines: &mut Vec<String>,
        prefix: &str,
        probe: radixdb_storage::instrumentation::ArtifactColumnarGroupProbeSnapshot,
        row_count: usize,
    ) {
        lines.push(format!(
            "{}-> artifact-backed Columnar GROUP BY (actual rows={})",
            prefix, row_count
        ));
        lines.push(format!(
            "{}   Access Path: aggregation.artifact_columnar_group_by",
            prefix
        ));
        lines.push(format!(
            "{}   Access Source: descriptor-backed artifact-backed typed columns",
            prefix
        ));
        lines.push(format!(
            "{}   artifact-backed Columnar Group: applies={}, row_groups={}, selected_blocks={}, input_rows={}, output_groups={}, accumulator={}, scheduler={}, scheduled_segments={}, local_merges={}, merged_groups={}",
            prefix,
            probe.applies,
            probe.row_groups,
            probe.selected_blocks,
            probe.input_rows,
            probe.output_groups,
            if probe.direct_accumulators > 0 {
                "direct_array"
            } else {
                "hash_map"
            },
            if probe.scheduler_runs > 0 {
                "parallel"
            } else {
                "serial"
            },
            probe.scheduled_segments,
            probe.local_merges,
            probe.merged_groups,
        ));
    }

    fn artifact_columnar_group_fallback_summary(
        probe: radixdb_storage::instrumentation::ArtifactColumnarGroupProbeSnapshot,
    ) -> Option<&'static str> {
        if probe.fallbacks == 0 {
            return None;
        }
        [
            ("row_state", probe.fallback_row_state),
            ("no_cold_artifact", probe.fallback_no_cold_artifact),
            ("schema", probe.fallback_schema),
            ("group_key", probe.fallback_group_key),
            ("aggregate", probe.fallback_aggregate),
            ("visibility", probe.fallback_visibility),
            ("storage", probe.fallback_storage),
            ("column_shape", probe.fallback_column_shape),
            ("accumulator", probe.fallback_accumulator),
        ]
        .into_iter()
        .find_map(|(reason, count)| (count > 0).then_some(reason))
    }

    fn append_artifact_cache_execution_details(
        lines: &mut Vec<String>,
        prefix: &str,
        stats: ArtifactCacheExplainStats,
    ) {
        if !stats.has_artifact_activity() {
            lines.push(format!(
                "{}artifact-backed Physical I/O: request-local counters unavailable",
                prefix
            ));
            return;
        }
        lines.push(format!(
            "{}artifact-backed Physical I/O: descriptor_opens={}, payload_file_opens={}, stat_calls={}, identity_checks={}, fadvise_calls={}, fadvise_errors={}, pread_calls={}, pread_bytes={}, singleflight_leaders={}, singleflight_followers={}, payload_decompress_calls={}, payload_decompress_nanos={}, column_deserialize_calls={}, column_deserialize_nanos={}, cache_hits={}, cache_misses={}, cache_insert_bytes={}",
            prefix,
            stats.descriptor_open_calls,
            stats.file_open_calls,
            stats.file_stat_calls,
            stats.file_identity_checks,
            stats.fadvise_calls,
            stats.fadvise_errors,
            stats.pread_calls,
            stats.pread_bytes,
            stats.singleflight_leaders,
            stats.singleflight_followers,
            stats.payload_decompress_calls,
            stats.payload_decompress_nanos,
            stats.column_deserialize_calls,
            stats.column_deserialize_nanos,
            stats.cache_hits,
            stats.cache_misses,
            stats.cache_insert_bytes,
        ));
    }

    fn classify_projection_boundary(&self, select: &SelectStatement) -> &'static str {
        if Self::select_has_projection_star(select) {
            return "projection.full_row";
        }

        if Self::select_has_aggregate_or_window(select) {
            return "projection.aggregate_or_window_result";
        }

        match select.table_expr.as_deref() {
            None => {
                if select
                    .columns
                    .iter()
                    .all(Self::projection_dependencies_extractable)
                {
                    "projection.constant_or_expression"
                } else {
                    "projection.executor_fallback"
                }
            }
            Some(Expression::TableSource(_)) => {
                if select.columns.iter().all(Self::is_simple_column_projection) {
                    "projection.table_scan"
                } else if Self::select_projection_dependencies_extractable(select) {
                    "projection.expression_dependency_scan"
                } else {
                    "projection.executor_full_row_fallback"
                }
            }
            Some(Expression::JoinSource(_)) => {
                if select.columns.iter().all(Self::is_simple_column_projection) {
                    "projection.join_operator_candidate"
                } else {
                    "projection.join_executor_fallback"
                }
            }
            Some(_) => "projection.executor_boundary",
        }
    }

    fn classify_aggregation_path(
        &self,
        select: &SelectStatement,
    ) -> Option<(&'static str, Option<&'static str>)> {
        if !Self::select_has_aggregate_or_window(select)
            && select.group_by.columns.is_empty()
            && matches!(select.group_by.modifier, GroupByModifier::None)
        {
            return None;
        }

        if select.columns.iter().any(Self::expression_contains_window)
            || !select.window_defs.is_empty()
        {
            return Some((
                "aggregation.executor_fallback",
                Some("window-functions-not-storage-aggregation"),
            ));
        }

        if !matches!(select.group_by.modifier, GroupByModifier::None) {
            return Some(("aggregation.executor_fallback", Some("grouping-modifier")));
        }

        if select.group_by.columns.is_empty() {
            return Some(("aggregation.scalar_or_executor", None));
        }

        if !matches!(
            select.table_expr.as_deref(),
            Some(Expression::TableSource(_))
        ) {
            return Some((
                "aggregation.executor_fallback",
                Some("non-simple-table-source"),
            ));
        }

        if !select
            .group_by
            .columns
            .iter()
            .all(|expr| matches!(expr, Expression::Identifier(_)))
        {
            return Some(("aggregation.executor_fallback", Some("non-column-group-by")));
        }

        if !Self::select_columns_match_storage_group_by_shape(select) {
            return Some((
                "aggregation.executor_fallback",
                Some("unsupported-select-aggregate-shape"),
            ));
        }

        if select
            .having
            .as_deref()
            .is_some_and(|having| !Self::storage_having_dependencies_extractable(having))
        {
            return Some(("aggregation.executor_fallback", Some("unsupported-having")));
        }

        if !self.select_where_fully_pushable(select) {
            return Some((
                "aggregation.executor_fallback",
                Some("where-not-fully-pushable"),
            ));
        }

        Some(("aggregation.storage_group_by", None))
    }

    fn select_where_fully_pushable(&self, select: &SelectStatement) -> bool {
        let Some(where_expr) = select.where_clause.as_deref() else {
            return true;
        };
        let Some(table_expr) = select.table_expr.as_deref() else {
            return false;
        };
        let Some(table_name) = extract_table_name(table_expr) else {
            return false;
        };
        let Ok(tx) = self.engine.begin_transaction() else {
            return false;
        };
        let Ok(table) = tx.get_table(&table_name) else {
            return false;
        };
        let (storage_expr, needs_memory_filter) =
            pushdown::try_pushdown(where_expr, table.schema(), None);
        storage_expr.is_some() && !needs_memory_filter
    }

    fn select_has_projection_star(select: &SelectStatement) -> bool {
        select.columns.iter().any(Self::is_projection_star_expr)
    }

    fn is_projection_star_expr(expr: &Expression) -> bool {
        match expr {
            Expression::Star(_) | Expression::QualifiedStar(_) => true,
            Expression::Aliased(aliased) => {
                matches!(
                    aliased.expression.as_ref(),
                    Expression::Star(_) | Expression::QualifiedStar(_)
                )
            }
            _ => false,
        }
    }

    fn select_has_aggregate_or_window(select: &SelectStatement) -> bool {
        !select.group_by.columns.is_empty()
            || !matches!(select.group_by.modifier, GroupByModifier::None)
            || select
                .having
                .as_deref()
                .is_some_and(expression_contains_aggregate)
            || !select.window_defs.is_empty()
            || select.columns.iter().any(|expr| {
                expression_contains_aggregate(expr) || Self::expression_contains_window(expr)
            })
    }

    fn is_simple_column_projection(expr: &Expression) -> bool {
        match expr {
            Expression::Identifier(_) | Expression::QualifiedIdentifier(_) => true,
            Expression::Aliased(aliased) => matches!(
                aliased.expression.as_ref(),
                Expression::Identifier(_) | Expression::QualifiedIdentifier(_)
            ),
            _ => false,
        }
    }

    fn select_projection_dependencies_extractable(select: &SelectStatement) -> bool {
        select
            .columns
            .iter()
            .all(Self::projection_dependencies_extractable)
            && select
                .where_clause
                .as_deref()
                .is_none_or(Self::projection_dependencies_extractable)
            && select
                .order_by
                .iter()
                .all(|order| Self::projection_dependencies_extractable(&order.expression))
            && select
                .distinct_on
                .iter()
                .all(Self::projection_dependencies_extractable)
    }

    fn projection_dependencies_extractable(expr: &Expression) -> bool {
        match expr {
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
            | Expression::Default(_) => true,
            Expression::Aliased(aliased) => {
                Self::projection_dependencies_extractable(&aliased.expression)
            }
            Expression::FunctionCall(func) => {
                func.arguments
                    .iter()
                    .all(Self::projection_dependencies_extractable)
                    && func
                        .filter
                        .as_deref()
                        .is_none_or(Self::projection_dependencies_extractable)
                    && func
                        .order_by
                        .iter()
                        .all(|order| Self::projection_dependencies_extractable(&order.expression))
            }
            Expression::Infix(infix) => {
                Self::projection_dependencies_extractable(&infix.left)
                    && Self::projection_dependencies_extractable(&infix.right)
            }
            Expression::Prefix(prefix) => Self::projection_dependencies_extractable(&prefix.right),
            Expression::Distinct(distinct) => {
                Self::projection_dependencies_extractable(&distinct.expr)
            }
            Expression::In(in_expr) => {
                Self::projection_dependencies_extractable(&in_expr.left)
                    && Self::projection_dependencies_extractable(&in_expr.right)
            }
            Expression::InHashSet(in_expr) => {
                Self::projection_dependencies_extractable(&in_expr.column)
            }
            Expression::Between(between) => {
                Self::projection_dependencies_extractable(&between.expr)
                    && Self::projection_dependencies_extractable(&between.lower)
                    && Self::projection_dependencies_extractable(&between.upper)
            }
            Expression::Like(like) => {
                Self::projection_dependencies_extractable(&like.left)
                    && Self::projection_dependencies_extractable(&like.pattern)
                    && like
                        .escape
                        .as_deref()
                        .is_none_or(Self::projection_dependencies_extractable)
            }
            Expression::List(list) => list
                .elements
                .iter()
                .all(Self::projection_dependencies_extractable),
            Expression::ExpressionList(list) => list
                .expressions
                .iter()
                .all(Self::projection_dependencies_extractable),
            Expression::Case(case) => {
                case.value
                    .as_deref()
                    .is_none_or(Self::projection_dependencies_extractable)
                    && case.when_clauses.iter().all(|when| {
                        Self::projection_dependencies_extractable(&when.condition)
                            && Self::projection_dependencies_extractable(&when.then_result)
                    })
                    && case
                        .else_value
                        .as_deref()
                        .is_none_or(Self::projection_dependencies_extractable)
            }
            Expression::Cast(cast) => Self::projection_dependencies_extractable(&cast.expr),
            Expression::Star(_)
            | Expression::QualifiedStar(_)
            | Expression::AllAny(_)
            | Expression::Exists(_)
            | Expression::ScalarSubquery(_)
            | Expression::Window(_)
            | Expression::TableSource(_)
            | Expression::JoinSource(_)
            | Expression::SubquerySource(_)
            | Expression::ValuesSource(_)
            | Expression::CteReference(_)
            | Expression::FunctionTableSource(_) => false,
        }
    }

    fn expression_contains_window(expr: &Expression) -> bool {
        match expr {
            Expression::Window(_) => true,
            Expression::Aliased(aliased) => Self::expression_contains_window(&aliased.expression),
            Expression::FunctionCall(func) => {
                func.arguments.iter().any(Self::expression_contains_window)
            }
            Expression::Infix(infix) => {
                Self::expression_contains_window(&infix.left)
                    || Self::expression_contains_window(&infix.right)
            }
            Expression::Prefix(prefix) => Self::expression_contains_window(&prefix.right),
            Expression::Case(case) => {
                case.value
                    .as_deref()
                    .is_some_and(Self::expression_contains_window)
                    || case.when_clauses.iter().any(|when| {
                        Self::expression_contains_window(&when.condition)
                            || Self::expression_contains_window(&when.then_result)
                    })
                    || case
                        .else_value
                        .as_deref()
                        .is_some_and(Self::expression_contains_window)
            }
            Expression::Cast(cast) => Self::expression_contains_window(&cast.expr),
            _ => false,
        }
    }

    fn select_columns_match_storage_group_by_shape(select: &SelectStatement) -> bool {
        let group_names: Vec<String> = select
            .group_by
            .columns
            .iter()
            .filter_map(|expr| match expr {
                Expression::Identifier(id) => Some(id.value_lower.to_string()),
                _ => None,
            })
            .collect();
        if group_names.len() != select.group_by.columns.len() {
            return false;
        }

        let mut select_group_count = 0usize;
        let mut seen_aggregate = false;

        for expr in &select.columns {
            match expr {
                Expression::Identifier(id) => {
                    if seen_aggregate {
                        return false;
                    }
                    if group_names.get(select_group_count) != Some(&id.value_lower.to_string()) {
                        return false;
                    }
                    select_group_count += 1;
                }
                Expression::FunctionCall(func) => {
                    seen_aggregate = true;
                    if !Self::is_storage_group_aggregate_call(func) {
                        return false;
                    }
                }
                Expression::Aliased(aliased) => match aliased.expression.as_ref() {
                    Expression::Identifier(id) => {
                        if seen_aggregate {
                            return false;
                        }
                        if group_names.get(select_group_count) != Some(&id.value_lower.to_string())
                        {
                            return false;
                        }
                        select_group_count += 1;
                    }
                    Expression::FunctionCall(func) => {
                        seen_aggregate = true;
                        if !Self::is_storage_group_aggregate_call(func) {
                            return false;
                        }
                    }
                    _ => return false,
                },
                _ => return false,
            }
        }

        select_group_count == group_names.len()
    }

    fn is_storage_group_aggregate_call(func: &FunctionCall) -> bool {
        if func.filter.is_some() || func.is_distinct || !func.order_by.is_empty() {
            return false;
        }

        let name = func.function.to_uppercase();
        match name.as_str() {
            "COUNT" => {
                func.arguments.is_empty()
                    || matches!(func.arguments.first(), Some(Expression::Star(_)))
                    || matches!(func.arguments.first(), Some(Expression::Identifier(_)))
            }
            "SUM" | "AVG" | "MIN" | "MAX" => {
                matches!(func.arguments.first(), Some(Expression::Identifier(_)))
            }
            _ => false,
        }
    }

    fn storage_having_dependencies_extractable(expr: &Expression) -> bool {
        match expr {
            Expression::FunctionCall(func) if is_aggregate_function(&func.function) => {
                Self::is_storage_group_aggregate_call(func)
            }
            Expression::FunctionCall(func) => {
                func.arguments
                    .iter()
                    .all(Self::storage_having_dependencies_extractable)
                    && func
                        .filter
                        .as_deref()
                        .is_none_or(Self::storage_having_dependencies_extractable)
                    && func.order_by.iter().all(|order| {
                        Self::storage_having_dependencies_extractable(&order.expression)
                    })
            }
            Expression::Aliased(aliased) => {
                Self::storage_having_dependencies_extractable(&aliased.expression)
            }
            Expression::Infix(infix) => {
                Self::storage_having_dependencies_extractable(&infix.left)
                    && Self::storage_having_dependencies_extractable(&infix.right)
            }
            Expression::Prefix(prefix) => {
                Self::storage_having_dependencies_extractable(&prefix.right)
            }
            Expression::Cast(cast) => Self::storage_having_dependencies_extractable(&cast.expr),
            Expression::Case(case) => {
                case.value
                    .as_deref()
                    .is_none_or(Self::storage_having_dependencies_extractable)
                    && case.when_clauses.iter().all(|when| {
                        Self::storage_having_dependencies_extractable(&when.condition)
                            && Self::storage_having_dependencies_extractable(&when.then_result)
                    })
                    && case
                        .else_value
                        .as_deref()
                        .is_none_or(Self::storage_having_dependencies_extractable)
            }
            Expression::Identifier(_)
            | Expression::QualifiedIdentifier(_)
            | Expression::IntegerLiteral(_)
            | Expression::FloatLiteral(_)
            | Expression::StringLiteral(_)
            | Expression::BooleanLiteral(_)
            | Expression::NullLiteral(_)
            | Expression::IntervalLiteral(_)
            | Expression::Parameter(_)
            | Expression::Default(_) => true,
            _ => false,
        }
    }
}
