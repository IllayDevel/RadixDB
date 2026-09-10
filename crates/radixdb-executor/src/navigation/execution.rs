use super::*;

struct TargetBatch {
    rows_by_key: UniqueLookupRows,
    column_positions: FxHashMap<usize, usize>,
    strategy: ReferenceLookupStrategy,
    storage_mode: ReferenceStorageMode,
    direction: ReferenceExecutionDirection,
    projected_columns: usize,
    reverse_source_index_eligible: bool,
}

#[derive(Default)]
struct EdgeSourceBatch {
    keys: Vec<Value>,
    row_indices_by_key: Vec<Vec<usize>>,
    key_positions: FxHashMap<Value, usize>,
    null_row_indices: Vec<usize>,
}

impl EdgeSourceBatch {
    fn push(
        &mut self,
        key: Value,
        row_index: usize,
        metrics: &mut ReferenceExpandMetrics,
        retained: &mut RetainedRowsBudget,
    ) -> Result<()> {
        if key.is_null() {
            metrics.null_source_keys = metrics.null_source_keys.saturating_add(1);
            self.null_row_indices.push(row_index);
            return Ok(());
        }

        match self.key_positions.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                metrics.repeated_keys_eliminated =
                    metrics.repeated_keys_eliminated.saturating_add(1);
                self.row_indices_by_key[*entry.get()].push(row_index);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                retained.admit_values(std::slice::from_ref(&key))?;
                let key_index = self.keys.len();
                entry.insert(key_index);
                self.keys.push(key);
                self.row_indices_by_key.push(vec![row_index]);
            }
        }
        Ok(())
    }

    fn release_keys(&self, retained: &mut RetainedRowsBudget) {
        for key in &self.keys {
            retained.release_values(std::slice::from_ref(key));
        }
    }
}

const INDEX_NESTED_LOOP_MAX_KEYS: usize = 32;
const TARGET_HASH_MIN_KEYS: usize = 128;
const TARGET_HASH_DENSITY_DENOMINATOR: usize = 4;

impl<H: NavigationHost + ?Sized> NavigationExecutor<'_, H> {
    pub(super) fn execute_reference_projection(
        &self,
        select: &SelectStatement,
        plan: &ReferenceExpandPlan,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        self.execute_reference_projection_with_metrics(select, plan, ctx)
            .map(|(result, _)| result)
    }

    pub(super) fn execute_reference_projection_with_metrics(
        &self,
        select: &SelectStatement,
        plan: &ReferenceExpandPlan,
        ctx: &ExecutionContext,
    ) -> Result<(Box<dyn QueryResult>, ReferenceExpandMetrics)> {
        let _execution = ctx.enter_reference_expand();
        let result = self.execute_reference_projection_with_metrics_inner(select, plan, ctx);
        match &result {
            Ok((_, metrics)) => {
                radixdb_storage::instrumentation::record_navigation_query(
                    metrics.instrumentation(),
                );
            }
            Err(Error::QueryCancelled) => {
                radixdb_storage::instrumentation::record_navigation_cancellation(
                    ctx.did_time_out(),
                );
            }
            Err(_) => {}
        }
        result
    }

    fn execute_reference_projection_with_metrics_inner(
        &self,
        select: &SelectStatement,
        plan: &ReferenceExpandPlan,
        ctx: &ExecutionContext,
    ) -> Result<(Box<dyn QueryResult>, ReferenceExpandMetrics)> {
        if plan.can_execute_graph_aggregation(select) {
            return self.execute_reference_graph_with_metrics(select, plan, ctx);
        }
        if plan.requires_planner_left_join(select) {
            let lowered =
                plan.lower_to_planner_left_joins(select, self.host.navigation_engine().as_ref())?;
            let result = self.host.navigation_execute_select(&lowered, ctx)?;
            return Ok((
                result,
                ReferenceExpandMetrics {
                    paths_planned: plan.paths.len(),
                    paths_executed: plan.paths.len(),
                    planner_left_join_edges: plan.edges.len(),
                    ..ReferenceExpandMetrics::default()
                },
            ));
        }
        self.execute_reference_graph_with_metrics(select, plan, ctx)
    }

    fn execute_reference_graph_with_metrics(
        &self,
        select: &SelectStatement,
        plan: &ReferenceExpandPlan,
        ctx: &ExecutionContext,
    ) -> Result<(Box<dyn QueryResult>, ReferenceExpandMetrics)> {
        plan.validate(self.host.navigation_engine().as_ref())?;
        let mut graph =
            plan.prepare_graph_execution(select, self.host.navigation_engine().as_ref())?;
        if let Some(where_clause) = &mut graph.rewritten_where {
            materialize_runtime_parameters(where_clause, ctx)?;
        }
        for filters in &mut graph.target_predicates {
            for filter in filters {
                materialize_runtime_parameters(filter, ctx)?;
            }
        }
        ctx.check_cancelled()?;

        let mut source = self
            .host
            .navigation_execute_select(&graph.source_select, ctx)?;
        let mut rows = RowVec::new();
        let mut retained = RetainedRowsBudget::new("ReferenceExpandPredicate");
        let mut metrics = ReferenceExpandMetrics {
            paths_planned: plan.paths.len(),
            paths_executed: plan.paths.len(),
            ..ReferenceExpandMetrics::default()
        };
        let mut edge_sources = (0..plan.edges.len())
            .map(|_| EdgeSourceBatch::default())
            .collect::<Vec<_>>();
        let root_edge_indices = plan
            .edges
            .iter()
            .enumerate()
            .filter_map(|(edge_index, edge)| (edge.identity.steps.len() == 1).then_some(edge_index))
            .collect::<Vec<_>>();
        let mut source_index = 0usize;
        while source.next() {
            if source_index & 0xff == 0 {
                ctx.check_cancelled()?;
            }
            let mut row = source.take_row();
            row.reserve(graph.edge_key_positions.len() + graph.hidden_path_positions.len());
            for (edge_index, edge) in plan.edges.iter().enumerate() {
                let key = if edge.identity.steps.len() == 1 {
                    let source_position = graph
                        .source_column_positions
                        .get(edge.source_column.ordinal())
                        .copied()
                        .flatten()
                        .ok_or_else(|| {
                            Error::internal(
                                "ReferenceExpand source projection omitted a root reference key",
                            )
                        })?;
                    row.get(source_position).cloned().ok_or_else(|| {
                        Error::internal("ReferenceExpand root key is outside the source projection")
                    })?
                } else {
                    Value::null(graph.edge_key_types[edge_index])
                };
                row.push(key);
            }
            for path in &plan.paths {
                row.push(Value::null(path.terminal_type));
            }
            retained.admit(&row)?;
            for edge_index in &root_edge_indices {
                let key = row
                    .get(graph.edge_key_positions[*edge_index])
                    .cloned()
                    .ok_or_else(|| {
                        Error::internal(
                            "ReferenceExpand root key is outside the augmented source row",
                        )
                    })?;
                edge_sources[*edge_index].push(key, source_index, &mut metrics, &mut retained)?;
            }
            rows.push((source_index as i64, row));
            source_index += 1;
        }
        if let Some(error) = source.last_error() {
            let _ = source.close();
            return Err(error);
        }
        source.close()?;
        self.host.navigation_source_materialized(plan, ctx);

        metrics.source_rows = rows.len();
        let mut paths_by_edge = vec![Vec::new(); plan.edges.len()];
        for (path_index, path) in plan.paths.iter().enumerate() {
            let terminal_edge = *path
                .edge_indices
                .last()
                .ok_or_else(|| Error::internal("ReferenceExpand path has no edge"))?;
            paths_by_edge[terminal_edge].push(path_index);
        }
        let children_by_edge = reference_children_by_edge(plan)?;

        let target_filters = graph
            .target_predicates
            .iter()
            .map(|filters| {
                filters
                    .iter()
                    .map(|filter| {
                        RowFilter::new(filter, &graph.augmented_columns)
                            .map(|filter| filter.with_context(ctx))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let mut keep_rows = vec![true; rows.len()];

        let has_active_transaction = self
            .host
            .navigation_active_transaction()
            .lock()
            .unwrap()
            .is_some();
        // See the direct projection path above: the statement visibility fence
        // keeps this graph's root and every target batch on one commit epoch.
        let standalone_transaction = if has_active_transaction {
            None
        } else {
            Some(self.host.navigation_engine().begin_transaction()?)
        };

        for (edge_index, edge_paths) in paths_by_edge.iter().enumerate() {
            let edge = &plan.edges[edge_index];
            let source_batch = std::mem::take(&mut edge_sources[edge_index]);
            metrics.distinct_keys = metrics
                .distinct_keys
                .saturating_add(source_batch.keys.len());

            let has_target_predicate = !target_filters[edge_index].is_empty();
            if has_target_predicate {
                metrics.target_predicate_edges = metrics.target_predicate_edges.saturating_add(1);
                metrics.left_to_inner_edges = metrics.left_to_inner_edges.saturating_add(1);
                for row_index in &source_batch.null_row_indices {
                    keep_rows[*row_index] = false;
                }
            }

            // A null parent leaves each descendant reference null. Dispatch
            // those row indices directly instead of rediscovering them through
            // another complete pass over the source relation.
            for child_index in &children_by_edge[edge_index] {
                for row_index in &source_batch.null_row_indices {
                    edge_sources[*child_index].push(
                        Value::null(graph.edge_key_types[*child_index]),
                        *row_index,
                        &mut metrics,
                        &mut retained,
                    )?;
                }
            }

            if source_batch.keys.is_empty() {
                continue;
            }

            ctx.check_cancelled()?;
            let (target_table, source_table) =
                if let Some(transaction) = standalone_transaction.as_deref() {
                    (
                        transaction.get_table(edge.target_key_column.table().table_name())?,
                        transaction.get_table(edge.source_column.table().table_name())?,
                    )
                } else {
                    let active = self.host.navigation_active_transaction().lock().unwrap();
                    let state = active.as_ref().ok_or_else(|| {
                        Error::internal("active transaction disappeared during ReferenceExpand")
                    })?;
                    (
                        state
                            .transaction
                            .get_table(edge.target_key_column.table().table_name())?,
                        state
                            .transaction
                            .get_table(edge.source_column.table().table_name())?,
                    )
                };
            let mut target = lookup_target_batch(
                target_table.as_ref(),
                edge,
                &source_batch.keys,
                ctx,
                &mut retained,
            )?;
            target.reverse_source_index_eligible = target.direction
                == ReferenceExecutionDirection::TargetFirst
                && source_index_is_snapshot_eligible(source_table.as_ref(), edge);

            metrics.lookup_batches = metrics.lookup_batches.saturating_add(1);
            metrics.lookup_hits = metrics.lookup_hits.saturating_add(target.rows_by_key.len());
            metrics.lookup_misses = metrics.lookup_misses.saturating_add(
                source_batch
                    .keys
                    .len()
                    .saturating_sub(target.rows_by_key.len()),
            );

            let mut rejected_distinct_keys = 0usize;
            for (key_index, key) in source_batch.keys.iter().enumerate() {
                if key_index & 0xff == 0 {
                    ctx.check_cancelled()?;
                }
                let target_values = target.rows_by_key.get(key).ok_or_else(|| {
                    Error::internal("verified ReferenceExpand target disappeared")
                })?;

                // Compute every terminal/child value once per distinct target,
                // then fan it out only to rows that actually reference the key.
                let terminal_values = edge_paths
                    .iter()
                    .map(|path_index| {
                        target_terminal_value(&plan.paths[*path_index], &target, target_values)
                            .map(|value| (graph.hidden_path_positions[*path_index], value))
                    })
                    .collect::<Result<Vec<_>>>()?;

                if has_target_predicate {
                    let mut target_row = Row::from_values(vec![
                        Value::null(DataType::Null);
                        graph.augmented_columns.len()
                    ]);
                    for (position, value) in &terminal_values {
                        target_row.set(*position, value.clone())?;
                    }
                    let passes =
                        target_filters[edge_index]
                            .iter()
                            .try_fold(true, |passes, filter| {
                                if passes {
                                    filter.matches_checked(&target_row)
                                } else {
                                    Ok(false)
                                }
                            })?;
                    if !passes {
                        rejected_distinct_keys = rejected_distinct_keys.saturating_add(1);
                        for row_index in &source_batch.row_indices_by_key[key_index] {
                            keep_rows[*row_index] = false;
                        }
                    }
                }

                let child_values = target_child_edge_values(
                    edge_index,
                    &children_by_edge,
                    plan,
                    &target,
                    target_values,
                )?;
                for row_index in &source_batch.row_indices_by_key[key_index] {
                    let row = &mut rows[*row_index].1;
                    retained.release(row);
                    for (position, value) in &terminal_values {
                        row.set(*position, value.clone())?;
                    }
                    for (child_index, value) in &child_values {
                        row.set(graph.edge_key_positions[*child_index], value.clone())?;
                    }
                    retained.admit(row)?;

                    for (child_index, value) in &child_values {
                        edge_sources[*child_index].push(
                            value.clone(),
                            *row_index,
                            &mut metrics,
                            &mut retained,
                        )?;
                    }
                }
            }

            metrics.target_predicate_keys_rejected = metrics
                .target_predicate_keys_rejected
                .saturating_add(rejected_distinct_keys);
            record_edge_metrics(
                &mut metrics,
                edge_index,
                &target,
                source_batch.keys.len(),
                has_target_predicate,
                rejected_distinct_keys,
            );
            target
                .rows_by_key
                .visit_values(|values| retained.release_values(values));
            source_batch.release_keys(&mut retained);
        }

        // Null-rejecting target conjuncts may safely lower LEFT execution to
        // INNER. Apply the per-distinct-key decision before the authoritative
        // full predicate without another edge-by-edge source scan.
        if keep_rows.iter().any(|keep| !keep) {
            let mut retained_rows = RowVec::with_capacity(rows.len());
            for (index, ((row_id, row), keep)) in rows.into_iter().zip(keep_rows).enumerate() {
                if index & 0xff == 0 {
                    ctx.check_cancelled()?;
                }
                if keep {
                    retained_rows.push((row_id, row));
                } else {
                    retained.release(&row);
                }
            }
            rows = retained_rows;
        }

        let filtered = if let Some(where_clause) = &graph.rewritten_where {
            let full_filter =
                RowFilter::new(where_clause, &graph.augmented_columns)?.with_context(ctx);
            let mut filtered = RowVec::with_capacity(rows.len());
            for (index, (row_id, row)) in rows.into_iter().enumerate() {
                if index & 0xff == 0 {
                    ctx.check_cancelled()?;
                }
                if full_filter.matches_checked(&row)? {
                    filtered.push((row_id, row));
                } else {
                    retained.release(&row);
                }
            }
            filtered
        } else {
            rows
        };

        if let Some(aggregation_select) = graph.aggregation_select.as_ref() {
            let result = self.host.execute_select_with_aggregation(
                aggregation_select,
                ctx,
                filtered,
                &graph.augmented_columns,
            )?;
            ctx.check_cancelled()?;
            return Ok((result, metrics));
        }

        let mut projected = self.host.navigation_project_rows_with_alias(
            &graph.rewritten_projection,
            filtered,
            &graph.augmented_columns,
            None,
            ctx,
            graph.table_alias.as_deref(),
        )?;
        let offset = graph
            .offset
            .as_deref()
            .map(|expression| {
                crate::pipeline::paging::evaluate_page_expression(expression, ctx, "OFFSET")
            })
            .transpose()?
            .unwrap_or(0);
        let limit = graph
            .limit
            .as_deref()
            .map(|expression| {
                crate::pipeline::paging::evaluate_page_expression(expression, ctx, "LIMIT")
            })
            .transpose()?
            .unwrap_or(usize::MAX);
        if offset != 0 || limit != usize::MAX {
            projected = projected.into_iter().skip(offset).take(limit).collect();
        }

        ctx.check_cancelled()?;
        Ok((
            Box::new(ExecutorResult::new(graph.output_columns, projected)),
            metrics,
        ))
    }
}

pub(super) fn graph_aggregation_source_ordinals(
    source_columns: &[String],
    visible_root: &str,
    table_name: &str,
    edges: &[ReferenceExpandEdge],
    aggregation_select: &SelectStatement,
    rewritten_where: Option<&Expression>,
) -> Vec<usize> {
    let by_name = source_columns
        .iter()
        .enumerate()
        .map(|(ordinal, name)| (name.to_lowercase(), ordinal))
        .collect::<FxHashMap<_, _>>();
    let mut required = FxHashSet::default();
    for edge in edges {
        if edge.identity.steps.len() == 1 {
            required.insert(edge.source_column.ordinal());
        }
    }

    let visible_root = visible_root.to_lowercase();
    let table_name = table_name.to_lowercase();
    let mut needs_all = false;
    let mut collect = |expression: &Expression| {
        radixdb_sql::ast::walk_expression_tree(expression, &mut |node| match node {
            Expression::Identifier(identifier) => {
                if let Some(&ordinal) = by_name.get(identifier.value_lower()) {
                    required.insert(ordinal);
                }
            }
            Expression::QualifiedIdentifier(identifier)
                if !identifier.is_multi_part_path()
                    && (identifier.qualifier.value_lower() == visible_root
                        || identifier.qualifier.value_lower() == table_name) =>
            {
                if let Some(&ordinal) = by_name.get(identifier.name.value_lower()) {
                    required.insert(ordinal);
                }
            }
            Expression::Star(_) => needs_all = true,
            Expression::QualifiedStar(star)
                if star.qualifier.eq_ignore_ascii_case(&visible_root)
                    || star.qualifier.eq_ignore_ascii_case(&table_name) =>
            {
                needs_all = true;
            }
            _ => {}
        });
    };

    for expression in &aggregation_select.columns {
        collect(expression);
    }
    for expression in &aggregation_select.group_by.columns {
        collect(expression);
    }
    if let GroupByModifier::GroupingSets(sets) = &aggregation_select.group_by.modifier {
        for expression in sets.iter().flatten() {
            collect(expression);
        }
    }
    if let Some(having) = aggregation_select.having.as_deref() {
        collect(having);
    }
    if let Some(where_clause) = rewritten_where {
        collect(where_clause);
    }

    if needs_all {
        return (0..source_columns.len()).collect();
    }
    let mut ordinals = required.into_iter().collect::<Vec<_>>();
    ordinals.sort_unstable();
    ordinals
}

fn reference_children_by_edge(plan: &ReferenceExpandPlan) -> Result<Vec<Vec<usize>>> {
    let mut by_identity = FxHashMap::default();
    for (edge_index, edge) in plan.edges.iter().enumerate() {
        by_identity.insert(edge.identity.clone(), edge_index);
    }
    let mut children = vec![Vec::new(); plan.edges.len()];
    for (child_index, child) in plan.edges.iter().enumerate() {
        if child.identity.steps.len() == 1 {
            continue;
        }
        let parent_identity = ReferenceExpandEdgeIdentity {
            root: child.identity.root.clone(),
            steps: child.identity.steps[..child.identity.steps.len() - 1].to_vec(),
        };
        let parent_index = by_identity
            .get(&parent_identity)
            .copied()
            .ok_or_else(|| Error::internal("ReferenceExpand graph is missing a parent edge"))?;
        if parent_index >= child_index {
            return Err(Error::internal(
                "ReferenceExpand graph is not in topological prefix order",
            ));
        }
        children[parent_index].push(child_index);
    }
    Ok(children)
}

fn target_child_edge_values(
    edge_index: usize,
    children_by_edge: &[Vec<usize>],
    plan: &ReferenceExpandPlan,
    target: &TargetBatch,
    target_values: &[Value],
) -> Result<Vec<(usize, Value)>> {
    let mut values = Vec::with_capacity(children_by_edge[edge_index].len());
    for child_index in &children_by_edge[edge_index] {
        let child = &plan.edges[*child_index];
        let position = target
            .column_positions
            .get(&child.source_column.ordinal())
            .copied()
            .ok_or_else(|| {
                Error::internal(
                    "ReferenceExpand parent projection omitted a child reference column",
                )
            })?;
        let key = target_values.get(position).cloned().ok_or_else(|| {
            Error::internal("ReferenceExpand parent row is narrower than its projection")
        })?;
        values.push((*child_index, key));
    }
    Ok(values)
}

fn target_terminal_value(
    path: &ReferenceExpandPath,
    target: &TargetBatch,
    target_values: &[Value],
) -> Result<Value> {
    let terminal_ordinal = path.identity.terminal_column.ordinal();
    let position = target
        .column_positions
        .get(&terminal_ordinal)
        .copied()
        .ok_or_else(|| {
            Error::internal("ReferenceExpand target projection omitted terminal column")
        })?;
    target_values.get(position).cloned().ok_or_else(|| {
        Error::internal("ReferenceExpand target row is narrower than its projection")
    })
}

fn record_edge_metrics(
    metrics: &mut ReferenceExpandMetrics,
    edge_index: usize,
    target: &TargetBatch,
    distinct_keys: usize,
    target_predicate_pushdown: bool,
    rejected_distinct_keys: usize,
) {
    match target.strategy {
        ReferenceLookupStrategy::DirectUnique => {
            metrics.direct_edges = metrics.direct_edges.saturating_add(1)
        }
        ReferenceLookupStrategy::IndexNestedLoop => {
            metrics.index_nested_loop_edges = metrics.index_nested_loop_edges.saturating_add(1)
        }
        ReferenceLookupStrategy::UniqueBatch => {
            metrics.batch_edges = metrics.batch_edges.saturating_add(1)
        }
        ReferenceLookupStrategy::TargetHashScan => {
            metrics.hash_edges = metrics.hash_edges.saturating_add(1)
        }
        ReferenceLookupStrategy::MergeJoin => {
            metrics.merge_edges = metrics.merge_edges.saturating_add(1)
        }
        ReferenceLookupStrategy::SnapshotScanFallback => {
            metrics.fallback_edges = metrics.fallback_edges.saturating_add(1)
        }
    }
    if target.direction == ReferenceExecutionDirection::TargetFirst {
        metrics.target_first_edges = metrics.target_first_edges.saturating_add(1);
    }
    match target.storage_mode {
        ReferenceStorageMode::HotMvcc => metrics.hot_edges = metrics.hot_edges.saturating_add(1),
        ReferenceStorageMode::ColdArtifact => {
            metrics.cold_edges = metrics.cold_edges.saturating_add(1)
        }
        ReferenceStorageMode::HybridArtifactHot => {
            metrics.hybrid_edges = metrics.hybrid_edges.saturating_add(1)
        }
    }
    metrics.edge_executions.push(ReferenceEdgeExecution {
        edge_index,
        strategy: target.strategy,
        storage_mode: target.storage_mode,
        direction: target.direction,
        distinct_keys,
        projected_columns: target.projected_columns,
        reverse_source_index_eligible: target.reverse_source_index_eligible,
        target_predicate_pushdown,
        left_to_inner: target_predicate_pushdown,
        rejected_distinct_keys,
    });
}

fn lookup_target_batch(
    table: &dyn Table,
    edge: &ReferenceExpandEdge,
    keys: &[Value],
    ctx: &ExecutionContext,
    retained: &mut RetainedRowsBudget,
) -> Result<TargetBatch> {
    debug_assert!(!keys.is_empty());
    let key_ordinal = edge.target_key_column.ordinal();
    let mut projection = vec![key_ordinal];
    let mut column_positions = FxHashMap::default();
    column_positions.insert(key_ordinal, 0);
    for required in &edge.required_columns {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            column_positions.entry(required.ordinal())
        {
            let position = projection.len();
            projection.push(required.ordinal());
            entry.insert(position);
        }
    }

    let key_name = table
        .schema()
        .get_column(key_ordinal)
        .map(|column| column.name.as_str())
        .ok_or_else(|| Error::navigation(NavigationErrorCode::SchemaChanged, "target key moved"))?;
    // One key table owns admission, candidate filtering, uniqueness
    // verification, and final source-row assembly for the whole edge. Keeping
    // separate `requested`/verifier sets here used to hash every distinct key
    // three extra times on transitive document-to-dictionary paths.
    let requested_keys = UniqueLookupRows::new(keys);
    let storage_mode = reference_storage_mode(table);
    let target_rows_hint = table.row_count_hint();
    let prefer_target_hash = keys.len() >= TARGET_HASH_MIN_KEYS
        && target_rows_hint != 0
        && keys.len().saturating_mul(TARGET_HASH_DENSITY_DENOMINATOR) >= target_rows_hint;
    let merge_candidates = if prefer_target_hash
        && edge.target_key() == ReferenceTargetKey::PrimaryKey
        && projection.len() == table.schema().columns.len()
        && keys.windows(2).all(|pair| pair[0] <= pair[1])
    {
        ordered_target_candidates(table, key_name, &projection, keys, target_rows_hint, ctx)?
    } else {
        None
    };

    let (batch, strategy, direction) = if let Some(candidates) = merge_candidates {
        (
            materialize_unique_lookup_candidates(
                keys,
                candidates,
                0,
                LookupEdgeCardinality::ExactlyOne,
                |values| retained.admit_values(values),
                || ctx.check_cancelled(),
            )?,
            ReferenceLookupStrategy::MergeJoin,
            ReferenceExecutionDirection::TargetFirst,
        )
    } else if prefer_target_hash {
        (
            materialize_unique_lookup_candidates(
                keys,
                scan_target_candidates(table, &projection, &requested_keys, ctx)?,
                0,
                LookupEdgeCardinality::ExactlyOne,
                |values| retained.admit_values(values),
                || ctx.check_cancelled(),
            )?,
            ReferenceLookupStrategy::TargetHashScan,
            ReferenceExecutionDirection::TargetFirst,
        )
    } else {
        match execute_unique_lookup_join_batch(
            table,
            key_name,
            keys,
            Some(&projection),
            0,
            LookupEdgeCardinality::ExactlyOne,
            LookupEdgeFallback::None,
            |values| retained.admit_values(values),
            || ctx.check_cancelled(),
        )? {
            Some(batch) => {
                let strategy = if keys.len() == 1 {
                    ReferenceLookupStrategy::DirectUnique
                } else if keys.len() <= INDEX_NESTED_LOOP_MAX_KEYS {
                    ReferenceLookupStrategy::IndexNestedLoop
                } else {
                    ReferenceLookupStrategy::UniqueBatch
                };
                (batch, strategy, ReferenceExecutionDirection::SourceFirst)
            }
            None => {
                let candidates = scan_target_candidates(table, &projection, &requested_keys, ctx)?;
                (
                    materialize_unique_lookup_candidates(
                        keys,
                        candidates,
                        0,
                        LookupEdgeCardinality::ExactlyOne,
                        |values| retained.admit_values(values),
                        || ctx.check_cancelled(),
                    )?,
                    ReferenceLookupStrategy::SnapshotScanFallback,
                    ReferenceExecutionDirection::TargetFirst,
                )
            }
        }
    };
    ctx.check_cancelled()?;
    verify_unique_lookup_integrity(edge, batch.integrity)?;

    Ok(TargetBatch {
        rows_by_key: batch.rows_by_key,
        column_positions,
        strategy,
        storage_mode,
        direction,
        projected_columns: projection.len(),
        reverse_source_index_eligible: false,
    })
}

pub fn verify_unique_lookup_integrity(
    edge: &ReferenceExpandEdge,
    integrity: UniqueLookupIntegrity,
) -> Result<()> {
    match integrity {
        UniqueLookupIntegrity::Complete => Ok(()),
        UniqueLookupIntegrity::Missing => Err(missing_target_error(edge)),
        UniqueLookupIntegrity::NotUnique => Err(not_unique_target_error(edge)),
    }
}

fn source_index_is_snapshot_eligible(source_table: &dyn Table, edge: &ReferenceExpandEdge) -> bool {
    if source_table.has_local_changes() {
        return false;
    }
    let Some(column) = source_table
        .schema()
        .get_column(edge.source_column.ordinal())
    else {
        return false;
    };
    matches!(
        source_table.collect_row_ids_by_index_values(&column.name, &[]),
        Some(Ok(_))
    )
}

fn ordered_target_candidates(
    table: &dyn Table,
    key_name: &str,
    projection: &[usize],
    sorted_keys: &[Value],
    target_rows_hint: usize,
    ctx: &ExecutionContext,
) -> Result<Option<RowVec>> {
    let Some(rows) = table.collect_rows_ordered_by_index(key_name, true, target_rows_hint, 0)
    else {
        return Ok(None);
    };
    let mut candidates = RowVec::new();
    let mut key_index = 0usize;
    for (index, (row_id, row)) in rows.into_iter().enumerate() {
        if index & 0xff == 0 {
            ctx.check_cancelled()?;
        }
        let projected = row.take_columns(projection)?;
        let key = projected
            .get(0)
            .ok_or_else(|| Error::internal("ordered target projection omitted its key"))?;
        while key_index < sorted_keys.len() && sorted_keys[key_index] < *key {
            key_index += 1;
        }
        if key_index == sorted_keys.len() {
            break;
        }
        if sorted_keys[key_index] == *key {
            candidates.push((row_id, projected));
        }
    }
    Ok(Some(candidates))
}

fn scan_target_candidates(
    table: &dyn Table,
    projection: &[usize],
    requested: &UniqueLookupRows,
    ctx: &ExecutionContext,
) -> Result<RowVec> {
    let mut scanner = table.scan_exact_projection(projection, None)?;
    let mut rows = RowVec::new();
    let mut scanned = 0usize;
    while scanner.next() {
        if scanned & 0xff == 0 {
            ctx.check_cancelled()?;
        }
        let (row_id, row) = scanner.take_row_with_id()?;
        if row.get(0).is_some_and(|key| requested.contains_key(key)) {
            rows.push((row_id, row));
        }
        scanned += 1;
    }
    if let Some(error) = scanner.err().cloned() {
        let _ = scanner.close();
        return Err(error);
    }
    scanner.close()?;
    Ok(rows)
}

fn reference_storage_mode(table: &dyn Table) -> ReferenceStorageMode {
    match table.explain_scan(None) {
        ScanPlan::SegmentedScan { hot_rows_hint, .. }
        | ScanPlan::SegmentedMultiIndexScan { hot_rows_hint, .. }
        | ScanPlan::SegmentedCompositeIndexScan { hot_rows_hint, .. } => {
            if hot_rows_hint == 0 {
                ReferenceStorageMode::ColdArtifact
            } else {
                ReferenceStorageMode::HybridArtifactHot
            }
        }
        _ if table.has_cold_segments() => ReferenceStorageMode::HybridArtifactHot,
        _ => ReferenceStorageMode::HotMvcc,
    }
}

fn missing_target_error(edge: &ReferenceExpandEdge) -> Error {
    radixdb_storage::instrumentation::record_navigation_integrity_failure();
    Error::navigation(
        NavigationErrorCode::TargetMissing,
        format!(
            "non-null reference {}.{} has no target in {}.{}",
            edge.source_column.table().table_name(),
            edge.source_column.ordinal(),
            edge.target_key_column.table().table_name(),
            edge.target_key_column.ordinal()
        ),
    )
}

fn not_unique_target_error(edge: &ReferenceExpandEdge) -> Error {
    radixdb_storage::instrumentation::record_navigation_integrity_failure();
    Error::navigation(
        NavigationErrorCode::TargetNotUnique,
        format!(
            "reference target {}.{} returned more than one visible row",
            edge.target_key_column.table().table_name(),
            edge.target_key_column.ordinal()
        ),
    )
}

pub(super) fn column_name(engine: &dyn Engine, column: &SchemaColumnId) -> Result<String> {
    let schema = engine.get_table_schema(column.table().table_name())?;
    schema
        .get_column(column.ordinal())
        .map(|column| column.name.clone())
        .ok_or_else(|| {
            Error::navigation(
                NavigationErrorCode::SchemaChanged,
                format!(
                    "column ordinal {} no longer exists in '{}'",
                    column.ordinal(),
                    column.table().table_name()
                ),
            )
        })
}
