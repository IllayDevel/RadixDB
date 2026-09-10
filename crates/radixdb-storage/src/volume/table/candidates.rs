//! Cold candidate discovery and immutable index access paths.

use super::*;

impl SegmentedTable {
    pub(super) fn rollback_statement_to(&self, timestamp: i64) {
        self.hot.rollback_to_timestamp(timestamp);
        self.segment_mgr
            .rollback_cold_index_removals_to_timestamp(self.txn_id(), timestamp);
        self.segment_mgr
            .rollback_pending_tombstones_to_timestamp(self.txn_id(), timestamp);
    }

    /// Create a segmented table from a hot buffer and a segment manager.
    pub fn new(hot: Box<dyn Table>, segment_mgr: Arc<SegmentManager>) -> Self {
        Self {
            hot,
            segment_mgr,
            snapshot_seq: None,
        }
    }

    /// Create a segmented table with a snapshot sequence for snapshot isolation.
    /// Only tombstones with commit_seq <= snapshot_seq are visible to this table.
    pub fn with_snapshot_seq(
        hot: Box<dyn Table>,
        segment_mgr: Arc<SegmentManager>,
        snapshot_seq: u64,
    ) -> Self {
        Self {
            hot,
            segment_mgr,
            snapshot_seq: Some(snapshot_seq),
        }
    }

    /// Create a segmented table with no segments (equivalent to plain MVCCTable).
    pub fn hot_only(hot: Box<dyn Table>) -> Self {
        Self {
            segment_mgr: Arc::new(SegmentManager::new("", None)),
            hot,
            snapshot_seq: None,
        }
    }

    /// Get the transaction ID for per-txn tombstone tracking.
    pub(super) fn txn_id(&self) -> i64 {
        self.hot.txn_id()
    }

    /// Check if a tombstone is visible to this table's snapshot.
    /// For auto-commit (snapshot_seq=None), all tombstones are visible.
    /// For snapshot isolation (snapshot_seq=Some(seq)), only tombstones
    /// with commit_seq <= seq are visible — newer tombstones are invisible,
    /// so the original cold row remains visible to the older snapshot.
    #[inline]
    pub(super) fn is_tombstone_visible(&self, commit_seq: u64) -> bool {
        self.snapshot_seq.is_none_or(|ss| commit_seq <= ss)
    }

    /// Check if a row_id is tombstoned and visible to this snapshot.
    #[inline]
    pub(super) fn is_row_tombstoned(&self, tombstones: &FxHashMap<i64, u64>, row_id: i64) -> bool {
        tombstones
            .get(&row_id)
            .is_some_and(|&seq| self.is_tombstone_visible(seq))
    }

    /// Count the visible snapshot through a zero-width lazy scan.
    ///
    /// Snapshot-aware metadata cannot subtract tombstones newer than the
    /// reader without inspecting visibility. Keep that inspection streaming:
    /// the previous `collect_all_rows(None).len()` retained every full Row and
    /// made COUNT(*) memory proportional to the complete cold table.
    pub(super) fn count_visible_rows_streaming(&self) -> Result<usize> {
        let mut scanner = <Self as Table>::scan_exact_projection(self, &[], None)?;
        let mut count = 0usize;
        while scanner.next() {
            count = count.saturating_add(1);
        }
        let scan_error = scanner.err().cloned();
        scanner.close()?;
        if let Some(error) = scan_error {
            return Err(error);
        }
        Ok(count)
    }

    /// Get the schema default for a column, or typed NULL if no default.
    #[inline]
    pub(super) fn column_default(&self, col_idx: usize) -> Value {
        let schema = self.hot.schema();
        if col_idx < schema.columns.len() {
            let col = &schema.columns[col_idx];
            col.default_value
                .clone()
                .unwrap_or_else(|| Value::null(col.data_type))
        } else {
            Value::Null(radixdb_core::DataType::Null)
        }
    }

    /// Get the number of frozen segments.
    pub fn segment_count(&self) -> usize {
        self.segment_mgr.segment_count()
    }

    /// Get the segment manager.
    pub fn segment_manager(&self) -> &Arc<SegmentManager> {
        &self.segment_mgr
    }

    pub(super) fn full_schema_column_indices(&self) -> Vec<usize> {
        (0..self.hot.schema().columns.len()).collect()
    }

    pub(super) fn cold_scan_skip_set(&self) -> FxHashSet<i64> {
        // Build skip set: hot row_ids + pending tombstones for this transaction.
        // Hot row_ids shadow older cold copies; pending tombstones hide cold rows
        // already deleted/updated by the current transaction.
        let mut hot_skip: FxHashSet<i64> =
            FxHashSet::with_capacity_and_hasher(10_000, Default::default());
        self.hot.collect_hot_row_ids_into(&mut hot_skip);
        self.segment_mgr
            .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);
        hot_skip
    }

    pub(super) fn zone_map_row_id_ranges(
        &self,
        where_expr: Option<&dyn Expression>,
    ) -> Option<Arc<Vec<(i64, i64)>>> {
        if self.snapshot_seq.is_some() || self.hot.has_local_changes() {
            return None;
        }
        let expr = where_expr?;
        let comparisons = expr.collect_comparisons();
        if comparisons.len() != 1 {
            return None;
        }
        let zone_maps = self.hot.get_zone_maps()?;
        if zone_maps.is_stale() {
            return None;
        }
        let (column, operator, value) = comparisons[0];
        let segments = zone_maps.get_segments_to_scan(column, operator, value)?;
        zone_maps
            .row_id_ranges_for_segments(&segments)
            .map(Arc::new)
    }

    /// Select a persisted cold exact index for the longest available leading
    /// equality prefix. Range-only predicates remain on the ordinary artifact-backed scan
    /// until they have their own physical postings/operator.
    pub(super) fn plan_cold_composite_exact(
        &self,
        expr: &dyn Expression,
    ) -> Option<ColdCompositeExactPlan> {
        let comparisons = expr.collect_comparisons();
        if comparisons.is_empty() {
            return None;
        }

        let schema = self.hot.schema();
        let mut best: Option<ColdCompositeExactPlan> = None;
        for index in self.hot.get_indexes() {
            let columns = index.column_names();
            if columns.is_empty()
                || index.partial_predicate().is_some()
                || index.index_type() == IndexType::Hnsw
            {
                continue;
            }

            let mut column_indices = Vec::with_capacity(columns.len());
            let mut values = Vec::with_capacity(columns.len());
            let mut conditions = Vec::with_capacity(columns.len());
            for column in columns {
                let Some((column_idx, schema_column)) = schema.find_column(column) else {
                    break;
                };
                let Some((_, _, value)) = comparisons.iter().find(|(name, operator, _)| {
                    name.eq_ignore_ascii_case(column) && *operator == radixdb_core::Operator::Eq
                }) else {
                    break;
                };
                let value = value.coerce_to_type(schema_column.data_type);
                if value.is_null() {
                    break;
                }
                column_indices.push(column_idx);
                conditions.push(format!("= {}", value));
                values.push(value);
            }

            if column_indices.is_empty()
                || !self.segment_mgr.has_exact_index_columns(&column_indices)
            {
                continue;
            }

            let candidate = ColdCompositeExactPlan {
                index_name: index.name().to_string(),
                declared_column_count: columns.len(),
                columns: columns[..column_indices.len()].to_vec(),
                column_indices,
                values,
                conditions,
            };
            if best.as_ref().is_none_or(|current| {
                candidate.columns.len() > current.columns.len()
                    || (candidate.columns.len() == current.columns.len()
                        && candidate.declared_column_count < current.declared_column_count)
            }) {
                best = Some(candidate);
            }
        }
        best
    }

    pub(super) fn plan_cold_exact_set(&self, expr: &dyn Expression) -> Option<ColdExactSetPlan> {
        let infos = expr.collect_in_list_infos();
        let [(column, values, false, _)] = infos.as_slice() else {
            return None;
        };
        let (column_index, schema_column) = self.hot.schema().find_column(column)?;
        if crate::expression::in_list::has_cross_numeric_physical_variant(
            schema_column.data_type,
            values,
        ) {
            return None;
        }
        let values: Vec<Value> = values
            .iter()
            .map(|value| value.coerce_to_type(schema_column.data_type))
            .filter(|value| !value.is_null())
            .collect();
        if values.is_empty() || !self.segment_mgr.has_exact_index_columns(&[column_index]) {
            return None;
        }

        self.hot
            .get_indexes()
            .into_iter()
            .filter(|index| {
                index.partial_predicate().is_none()
                    && index.index_type() != IndexType::Hnsw
                    && index
                        .column_names()
                        .first()
                        .is_some_and(|name| name.eq_ignore_ascii_case(column))
            })
            .min_by_key(|index| index.column_names().len())
            .map(|index| ColdExactSetPlan {
                index_name: index.name().to_string(),
                declared_column_count: index.column_names().len(),
                column: schema_column.name.clone(),
                values,
            })
    }

    pub(super) fn plan_cold_multi_index_or(
        &self,
        expr: &dyn Expression,
    ) -> Option<Vec<ColdCompositeOrderedPlan>> {
        let operands = expr.get_or_operands()?;
        if operands.len() < 2 {
            return None;
        }
        operands
            .iter()
            .map(|operand| self.plan_cold_composite_ordered(operand.as_ref()))
            .collect()
    }

    /// Select an immutable artifact-backed posting set for an equality prefix followed by
    /// one INTEGER or TIMESTAMP range column. The equality prefix may be empty
    /// for a single-column index, and the effective column list may be a
    /// prefix of a wider declared composite index.
    pub(super) fn plan_cold_composite_ordered(
        &self,
        expr: &dyn Expression,
    ) -> Option<ColdCompositeOrderedPlan> {
        use radixdb_core::Operator;

        let comparisons = expr.collect_comparisons();
        if comparisons.is_empty() {
            return None;
        }
        let schema = self.hot.schema();
        let mut best: Option<ColdCompositeOrderedPlan> = None;

        for index in self.hot.get_indexes() {
            let declared_columns = index.column_names();
            if declared_columns.is_empty()
                || index.partial_predicate().is_some()
                || index.index_type() == IndexType::Hnsw
            {
                continue;
            }

            let mut column_indices = Vec::new();
            let mut equality_values = Vec::new();
            let mut conditions = Vec::new();
            let mut covered_columns = FxHashSet::default();
            let mut min = None;
            let mut max = None;
            let mut range_found = false;

            for column in declared_columns {
                let Some((column_idx, schema_column)) = schema.find_column(column) else {
                    break;
                };
                let matching: Vec<_> = comparisons
                    .iter()
                    .filter(|(name, _, _)| name.eq_ignore_ascii_case(column))
                    .collect();
                if matching.is_empty() {
                    break;
                }

                if !range_found {
                    if let Some((_, _, value)) = matching
                        .iter()
                        .find(|(_, operator, _)| *operator == Operator::Eq)
                    {
                        let value = value.coerce_to_type(schema_column.data_type);
                        if value.is_null() {
                            break;
                        }
                        column_indices.push(column_idx);
                        equality_values.push(value.clone());
                        conditions.push(format!("= {}", value));
                        covered_columns.insert(schema_column.name.clone());
                        continue;
                    }
                }

                if !matches!(
                    schema_column.data_type,
                    DataType::Integer | DataType::Timestamp
                ) {
                    break;
                }
                let mut range_conditions = Vec::new();
                let mut unsupported_range_bound = false;
                for (_, operator, value) in matching {
                    if !matches!(
                        operator,
                        Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte
                    ) {
                        continue;
                    }

                    // Persisted ordered postings are representation-specific.
                    // Coercing a Float/Decimal range bound to i64 truncates or
                    // saturates it and can remove valid candidates before the
                    // residual predicate runs (for example `id < 0.5`). Keep
                    // such predicates on the complete scan path instead.
                    let bound_has_exact_physical_domain = match schema_column.data_type {
                        DataType::Integer => matches!(value, Value::Integer(_)),
                        DataType::Timestamp => {
                            matches!(value, Value::Timestamp(_) | Value::Integer(_))
                        }
                        _ => false,
                    };
                    if !bound_has_exact_physical_domain {
                        unsupported_range_bound = true;
                        break;
                    }

                    let typed_bound = value.coerce_to_type(schema_column.data_type);
                    let bound = match &typed_bound {
                        Value::Integer(value) => *value,
                        Value::Timestamp(value) => {
                            value.timestamp_nanos_opt().unwrap_or_else(|| {
                                value
                                    .timestamp()
                                    .wrapping_mul(1_000_000_000)
                                    .wrapping_add(value.timestamp_subsec_nanos() as i64)
                            })
                        }
                        _ => continue,
                    };
                    if typed_bound.is_null() {
                        continue;
                    }
                    match operator {
                        Operator::Gt | Operator::Gte => {
                            let inclusive = *operator == Operator::Gte;
                            if min.is_none_or(|(current, current_inclusive)| {
                                bound > current
                                    || (bound == current && current_inclusive && !inclusive)
                            }) {
                                min = Some((bound, inclusive));
                            }
                        }
                        Operator::Lt | Operator::Lte => {
                            let inclusive = *operator == Operator::Lte;
                            if max.is_none_or(|(current, current_inclusive)| {
                                bound < current
                                    || (bound == current && current_inclusive && !inclusive)
                            }) {
                                max = Some((bound, inclusive));
                            }
                        }
                        _ => continue,
                    }
                    let operator = match operator {
                        Operator::Gt => ">",
                        Operator::Gte => ">=",
                        Operator::Lt => "<",
                        Operator::Lte => "<=",
                        _ => unreachable!("non-range operator was filtered above"),
                    };
                    range_conditions.push(format!("{} {}", operator, typed_bound));
                }
                if unsupported_range_bound {
                    break;
                }
                if min.is_none() && max.is_none() {
                    break;
                }
                column_indices.push(column_idx);
                conditions.push(range_conditions.join(" AND "));
                covered_columns.insert(schema_column.name.clone());
                range_found = true;
                break;
            }

            if !range_found || !self.segment_mgr.has_ordered_index_columns(&column_indices) {
                continue;
            }
            let columns = declared_columns[..column_indices.len()].to_vec();
            let candidate = ColdCompositeOrderedPlan {
                index_name: index.name().to_string(),
                declared_column_count: declared_columns.len(),
                columns,
                column_indices,
                equality_values,
                min,
                max,
                conditions,
                covered_columns,
            };
            if best.as_ref().is_none_or(|current| {
                candidate.column_indices.len() > current.column_indices.len()
                    || (candidate.column_indices.len() == current.column_indices.len()
                        && candidate.declared_column_count < current.declared_column_count)
            }) {
                best = Some(candidate);
            }
        }
        best
    }

    pub(super) fn scan_cold_composite_exact(
        &self,
        column_indices: &[usize],
        expr: &dyn Expression,
        exact_projection: bool,
    ) -> Option<Result<Box<dyn Scanner>>> {
        let plan = self.plan_cold_composite_exact(expr)?;
        let values: Vec<&Value> = plan.values.iter().collect();
        let row_ids = match self.segment_mgr.find_candidate_row_ids_by_exact_index(
            &plan.column_indices,
            &values,
            self.snapshot_seq,
            Some(COLD_INDEX_CURSOR_CANDIDATE_LIMIT),
        ) {
            Ok(Some(row_ids)) => row_ids,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };

        // Materialize only bounded candidates and always re-evaluate the whole
        // predicate. This verifies hash collisions and preserves residual
        // predicates as well as hot rows shadowing older cold copies.
        let candidate_rows = match self.collect_rows_by_ids_grouped_unfenced(&row_ids, None) {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        let mut rows = RowVec::with_capacity(candidate_rows.len());
        for (row_id, row) in candidate_rows {
            match expr.evaluate(&row) {
                Ok(true) => rows.push((row_id, row)),
                Ok(false) => {}
                Err(error) => return Some(Err(error)),
            }
        }

        // New/unsealed rows are owned by the hot index. Add them separately and
        // deduplicate row IDs that were already reached through a cold posting
        // but resolved to their authoritative hot shadow.
        let mut seen: FxHashSet<i64> = rows.iter().map(|(row_id, _)| *row_id).collect();
        let hot_rows = match self.collect_hot_index_rows(expr) {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        rows.extend(
            hot_rows
                .into_iter()
                .filter(|(row_id, _)| seen.insert(*row_id)),
        );

        let schema = CompactArc::new(self.hot.schema().clone());
        let scanner: Box<dyn Scanner> = if exact_projection {
            Box::new(crate::traits::MVCCScanner::from_rows_exact_projection(
                rows,
                schema,
                column_indices.to_vec(),
            ))
        } else {
            Box::new(crate::traits::MVCCScanner::from_rows(
                rows,
                schema,
                column_indices.to_vec(),
            ))
        };
        Some(Ok(scanner))
    }

    pub(super) fn scan_cold_exact_set(
        &self,
        column_indices: &[usize],
        expr: &dyn Expression,
        exact_projection: bool,
    ) -> Option<Result<Box<dyn Scanner>>> {
        let plan = self.plan_cold_exact_set(expr)?;
        let row_ids = match self.collect_row_ids_by_index_values(&plan.column, &plan.values) {
            Some(Ok(row_ids)) => row_ids,
            Some(Err(error)) => return Some(Err(error)),
            None => return None,
        };
        let candidates = match self.collect_rows_by_ids_grouped_unfenced(&row_ids, None) {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        let mut rows = RowVec::with_capacity(candidates.len());
        for (row_id, row) in candidates {
            match expr.evaluate(&row) {
                Ok(true) => rows.push((row_id, row)),
                Ok(false) => {}
                Err(error) => return Some(Err(error)),
            }
        }

        let schema = CompactArc::new(self.hot.schema().clone());
        let scanner: Box<dyn Scanner> = if exact_projection {
            Box::new(crate::traits::MVCCScanner::from_rows_exact_projection(
                rows,
                schema,
                column_indices.to_vec(),
            ))
        } else {
            Box::new(crate::traits::MVCCScanner::from_rows(
                rows,
                schema,
                column_indices.to_vec(),
            ))
        };
        Some(Ok(scanner))
    }

    pub(super) fn scan_cold_multi_index_or(
        &self,
        column_indices: &[usize],
        expr: &dyn Expression,
        exact_projection: bool,
    ) -> Option<Result<Box<dyn Scanner>>> {
        let plans = self.plan_cold_multi_index_or(expr)?;
        let target = self.segment_physical_row_count_hint();
        if target > COLD_INDEX_CURSOR_CANDIDATE_LIMIT {
            return None;
        }
        let operands = expr
            .get_or_operands()
            .expect("cold multi-index plan requires OR operands");
        let mut rows = RowVec::new();
        for (plan, operand) in plans.iter().zip(operands) {
            match self.collect_cold_composite_ordered_rows(plan, operand.as_ref(), true, target) {
                Ok(Some(branch_rows)) => rows.extend(branch_rows),
                Ok(None) => return None,
                Err(error) => return Some(Err(error)),
            }
        }

        let hot_rows = match self.hot.collect_all_rows(Some(expr)) {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        rows.extend(hot_rows);
        rows.sort_unstable_by_key(|(row_id, _)| *row_id);
        rows.dedup_by_key(|(row_id, _)| *row_id);
        rows.retain(|(_, row)| expr.evaluate_fast(row));

        let schema = CompactArc::new(self.hot.schema().clone());
        let scanner: Box<dyn Scanner> = if exact_projection {
            Box::new(crate::traits::MVCCScanner::from_rows_exact_projection(
                rows,
                schema,
                column_indices.to_vec(),
            ))
        } else {
            Box::new(crate::traits::MVCCScanner::from_rows(
                rows,
                schema,
                column_indices.to_vec(),
            ))
        };
        Some(Ok(scanner))
    }

    /// Collect the hot half through its normal planner/index path instead of
    /// scanning every hot row while serving a bounded cold exact lookup.
    pub(super) fn collect_hot_index_rows(&self, expr: &dyn Expression) -> Result<RowVec> {
        let projection = self.full_schema_column_indices();
        let mut scanner = self.hot.scan(&projection, Some(expr))?;
        let mut rows = RowVec::with_capacity(scanner.estimated_count().unwrap_or(0));
        while scanner.next() {
            rows.push(scanner.take_row_with_id()?);
        }
        let scan_error = scanner.err().cloned();
        scanner.close()?;
        if let Some(error) = scan_error {
            return Err(error);
        }
        Ok(rows)
    }

    pub(super) fn collect_cold_composite_ordered_rows(
        &self,
        plan: &ColdCompositeOrderedPlan,
        expr: &dyn Expression,
        ascending: bool,
        limit: usize,
    ) -> Result<Option<RowVec>> {
        let hot_skip = self.cold_scan_skip_set();
        let prefix_values: Vec<&Value> = plan.equality_values.iter().collect();
        let Some(row_ids) = self.segment_mgr.find_row_ids_by_ordered_index(
            &plan.column_indices,
            &prefix_values,
            plan.min,
            plan.max,
            ascending,
            limit,
            self.snapshot_seq,
            &hot_skip,
        )?
        else {
            return Ok(None);
        };

        let mut rows = self.collect_rows_by_ids_grouped_unfenced(&row_ids, None)?;
        rows.retain(|(_, row)| expr.evaluate_fast(row));
        let order_idx = *plan
            .column_indices
            .last()
            .expect("ordered plan always has a range column");
        rows.sort_unstable_by(|left, right| {
            let ordering = left
                .1
                .get(order_idx)
                .cmp(&right.1.get(order_idx))
                .then_with(|| left.0.cmp(&right.0));
            if ascending {
                ordering
            } else {
                ordering.reverse()
            }
        });
        rows.truncate(limit);
        Ok(Some(rows))
    }

    pub(super) fn scan_cold_composite_ordered(
        &self,
        column_indices: &[usize],
        expr: &dyn Expression,
        exact_projection: bool,
    ) -> Option<Result<Box<dyn Scanner>>> {
        let plan = self.plan_cold_composite_ordered(expr)?;
        // The limit is consumed by physical postings before tombstone and hot
        // shadow filtering.  A live-row hint would truncate the source early
        // and could omit valid rows that sort after removed candidates.
        let target = self.segment_physical_row_count_hint();
        if target > COLD_INDEX_CURSOR_CANDIDATE_LIMIT {
            return None;
        }
        let cold_rows = match self.collect_cold_composite_ordered_rows(&plan, expr, true, target) {
            Ok(Some(rows)) => rows,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        let mut rows = cold_rows;
        let mut seen: FxHashSet<i64> = rows.iter().map(|(row_id, _)| *row_id).collect();
        let hot_rows = match self.hot.collect_all_rows(Some(expr)) {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        rows.extend(
            hot_rows
                .into_iter()
                .filter(|(row_id, _)| seen.insert(*row_id)),
        );

        let schema = CompactArc::new(self.hot.schema().clone());
        let scanner: Box<dyn Scanner> = if exact_projection {
            Box::new(crate::traits::MVCCScanner::from_rows_exact_projection(
                rows,
                schema,
                column_indices.to_vec(),
            ))
        } else {
            Box::new(crate::traits::MVCCScanner::from_rows(
                rows,
                schema,
                column_indices.to_vec(),
            ))
        };
        Some(Ok(scanner))
    }
}
