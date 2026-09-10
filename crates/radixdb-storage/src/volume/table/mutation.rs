//! Cross-tier mutation staging and ordered row-id fetch.

use super::*;

impl SegmentedTable {
    /// Acquire the row claim for the common single INTEGER primary-key UPDATE
    /// before entering the segment topology fence.
    ///
    /// `MVCCTable::update` acquires this same claim while evaluating its hot
    /// fast path. Doing that while `SegmentedTable` owns `seal_fence` can turn
    /// ordinary row contention into a maintenance convoy: a waiter keeps the
    /// shared segment fence, seal/compaction queue for the exclusive side, and
    /// parking-lot writer priority then stops unrelated FK probes. Re-acquiring
    /// an already owned claim inside `MVCCTable` is an immediate no-op.
    pub(super) fn preclaim_point_update(&self, where_expr: Option<&dyn Expression>) -> Result<()> {
        let Some(expression) = where_expr else {
            return Ok(());
        };
        let schema = self.hot.schema();
        let [primary_key_index] = schema.primary_key_indices() else {
            return Ok(());
        };
        let Some(primary_key) = schema.columns.get(*primary_key_index) else {
            return Ok(());
        };
        if primary_key.data_type != DataType::Integer {
            return Ok(());
        }
        let Some((column_name, operator, value)) = expression.get_comparison_info() else {
            return Ok(());
        };
        if operator != radixdb_core::Operator::Eq
            || !column_name.eq_ignore_ascii_case(&primary_key.name)
        {
            return Ok(());
        }
        let Value::Integer(row_id) = value else {
            return Ok(());
        };
        self.hot.try_claim_row(*row_id)
    }

    /// Claims, revalidates and stages one bounded set of DELETE candidates.
    ///
    /// Candidate discovery may race with a concurrent writer that promotes an
    /// immutable cold row into hot MVCC. Claims are therefore acquired in one
    /// deterministic batch and every candidate is classified again afterwards:
    /// current hot rows are deleted through MVCCTable, current cold rows receive
    /// exactly one segment tombstone, and vanished/non-matching rows are skipped.
    /// Cold rows never receive a phantom hot delete version, so WAL ownership is
    /// unambiguous.
    pub(super) fn stage_delete_candidates(
        &mut self,
        row_ids: &[i64],
        recheck_expr: Option<&dyn Expression>,
        mut deleted_row_ids: Option<&mut Vec<i64>>,
    ) -> Result<i32> {
        if row_ids.is_empty() {
            return Ok(0);
        }
        let statement_boundary = get_fast_timestamp();
        let initial_deleted_len = deleted_row_ids.as_ref().map_or(0, |rows| rows.len());
        let result = (|| {
            let index_undo =
                ColdIndexRemovalStatementGuard::new(Arc::clone(&self.segment_mgr), self.txn_id());

            let mut candidates = row_ids.to_vec();
            candidates.sort_unstable();
            candidates.dedup();
            self.hot.try_claim_rows_for_delete(&candidates)?;

            // Row ownership can wait on a conflicting transaction and must not
            // retain a shared maintenance fence while it does so. Once every
            // candidate is owned, freeze hot/cold membership for the short
            // classification and staging section below.
            let _seal_guard = self.segment_mgr.acquire_seal_read();

            // A candidate may have moved from cold to hot while claim acquisition
            // waited. Read the authoritative hot view after all claims are held.
            let hot_rows = match self.hot.collect_rows_by_ids(&candidates) {
                Ok(rows) => rows,
                Err(radixdb_core::Error::NotSupported(_)) => RowVec::new(),
                Err(error) => return Err(error),
            };
            let mut hot_present = FxHashSet::with_capacity_and_hasher(
                hot_rows.len().saturating_add(candidates.len() / 4).max(1),
                Default::default(),
            );
            let mut hot_delete_ids = Vec::with_capacity(hot_rows.len());
            for (row_id, row) in hot_rows {
                hot_present.insert(row_id);
                let matches = match recheck_expr {
                    Some(expr) => expr.evaluate(&row)?,
                    None => true,
                };
                if matches {
                    hot_delete_ids.push(row_id);
                }
            }
            // Minimal/mock Table implementations may not expose row collection.
            // Their has_row_id contract is still sufficient when no predicate must
            // be re-evaluated. Production MVCCTable takes the typed branch above.
            for &row_id in &candidates {
                if !hot_present.contains(&row_id) && self.hot.has_row_id(row_id) {
                    if recheck_expr.is_some() {
                        return Err(radixdb_core::Error::NotSupported(
                            "hot DELETE predicate recheck requires collect_rows_by_ids".to_string(),
                        ));
                    }
                    hot_present.insert(row_id);
                    hot_delete_ids.push(row_id);
                }
            }

            let cleanup_populated_indexes = self.has_populated_cold_index_entries();
            let schema = cleanup_populated_indexes.then(|| self.hot.schema().clone());
            let mut cold_delete_ids = Vec::new();
            let mut cold_rows_for_cleanup = Vec::new();

            for row_id in candidates {
                if hot_present.contains(&row_id) {
                    continue;
                }
                let Some((_segment_id, cold, row_index)) = self.find_segment_row_for_read(row_id)
                else {
                    continue;
                };
                if let Some(schema) = schema.as_ref() {
                    let mapping = self.segment_mgr.get_cold_segment_mapping(&cold, schema);
                    let row =
                        self.materialize_volume_row_for_read(&cold.volume, row_index, &mapping)?;
                    // The immutable payload cannot change while the claim is held,
                    // but applying the predicate again keeps direct and scanned
                    // candidate paths under one explicit contract.
                    if let Some(expr) = recheck_expr {
                        if !expr.evaluate(&row)? {
                            continue;
                        }
                    }
                    cold_rows_for_cleanup.push((row_id, row));
                }
                cold_delete_ids.push(row_id);
            }

            for (row_id, row) in &cold_rows_for_cleanup {
                self.stage_populated_cold_index_entries_for_row(*row_id, row)?;
            }

            let mut exact_hot_deleted_ids = Vec::with_capacity(hot_delete_ids.len());
            let hot_deleted = if hot_delete_ids.is_empty() {
                0
            } else {
                self.hot.delete_candidate_row_ids_collect(
                    &hot_delete_ids,
                    None,
                    &mut exact_hot_deleted_ids,
                )?
            };
            self.segment_mgr
                .add_pending_tombstones(self.txn_id(), &cold_delete_ids);

            if let Some(deleted_row_ids) = deleted_row_ids.as_deref_mut() {
                deleted_row_ids.extend(exact_hot_deleted_ids);
                deleted_row_ids.extend(cold_delete_ids.iter().copied());
            }

            index_undo.finish();
            Ok(hot_deleted + cold_delete_ids.len() as i32)
        })();
        if result.is_err() {
            self.rollback_statement_to(statement_boundary);
            if let Some(deleted_row_ids) = deleted_row_ids {
                deleted_row_ids.truncate(initial_deleted_len);
            }
        }
        result
    }

    pub(super) fn typed_adapter_schema(
        &self,
        column_indices: &[usize],
        empty_projection_means_all: bool,
    ) -> Option<Schema> {
        let logical_columns: Vec<usize> = if column_indices.is_empty() {
            if !empty_projection_means_all {
                return None;
            }
            (0..self.hot.schema().columns.len()).collect()
        } else {
            column_indices.to_vec()
        };
        let mut builder = radixdb_core::SchemaBuilder::new("__hybrid_hot_typed");
        for (output_index, logical_index) in logical_columns.into_iter().enumerate() {
            let column = self.hot.schema().columns.get(logical_index)?;
            builder = builder.column(format!("c{output_index}"), column.data_type, true, false);
        }
        Some(builder.build())
    }

    /// Resolve bounded hot+cold posting candidates without materializing the
    /// indexed column. The caller chooses whether it needs row IDs only or can
    /// fuse the authoritative key recheck with its final row projection.
    pub(super) fn exact_index_candidates(
        &self,
        column_name: &str,
        values: &[Value],
    ) -> Option<Result<ExactIndexCandidates>> {
        let (column_index, column) = self.hot.schema().find_column(column_name)?;
        if crate::expression::in_list::has_cross_numeric_physical_variant(column.data_type, values)
        {
            return None;
        }

        // INTEGER PRIMARY KEY is already the physical row id. Visibility is
        // authoritative here, so no value recheck is necessary.
        if self.hot.schema().pk_column_index() == Some(column_index)
            && column.data_type == DataType::Integer
        {
            let mut row_ids = Vec::with_capacity(values.len());
            for value in values {
                match value.coerce_to_type(DataType::Integer) {
                    Value::Integer(row_id) => row_ids.push(row_id),
                    value if value.is_null() => {}
                    _ => return None,
                }
                if row_ids.len() > COLD_INDEX_CURSOR_CANDIDATE_LIMIT {
                    return None;
                }
            }
            row_ids.sort_unstable();
            row_ids.dedup();
            let mut matches = vec![false; row_ids.len()];
            if let Err(error) = self.probe_visible_row_ids(&row_ids, &mut matches) {
                return Some(Err(error));
            }
            let row_ids = row_ids
                .into_iter()
                .zip(matches)
                .filter_map(|(row_id, visible)| visible.then_some(row_id))
                .collect();
            return Some(Ok(ExactIndexCandidates {
                column_index,
                requested_values: ValueSet::default(),
                row_ids,
                requires_recheck: false,
            }));
        }

        let index = self.hot.get_index_on_column(column_name)?;
        if !self.segment_mgr.has_segments() {
            return None;
        }
        if index.partial_predicate().is_some()
            || index.index_type() == IndexType::Hnsw
            || !self.segment_mgr.has_exact_index_columns(&[column_index])
        {
            return None;
        }

        let mut coerced_values = Vec::with_capacity(values.len());
        for value in values {
            let value = value.coerce_to_type(column.data_type);
            if value.is_null() {
                continue;
            }
            coerced_values.push(value);
        }

        // Shared indexes contain committed keys only. Keep the direct physical
        // lookup for the common cold-only path, but resolve the mutable side
        // through its transaction-aware contract whenever this transaction has
        // private changes. The fused row fetch below rechecks the authoritative
        // key and removes unrelated local rows, deletes and stale old-key
        // entries.
        let mut row_ids = if self.hot.has_local_changes() {
            match self
                .hot
                .collect_row_ids_by_index_values(column_name, &coerced_values)
            {
                Some(Ok(row_ids)) => row_ids,
                Some(Err(error)) => return Some(Err(error)),
                None => return None,
            }
        } else {
            let mut row_ids = Vec::new();
            for value in &coerced_values {
                if let Err(error) =
                    index.get_row_ids_equal_into(std::slice::from_ref(value), &mut row_ids)
                {
                    return Some(Err(error));
                }
                if row_ids.len() > COLD_INDEX_CURSOR_CANDIDATE_LIMIT {
                    return None;
                }
            }
            row_ids
        };
        if row_ids.len() > COLD_INDEX_CURSOR_CANDIDATE_LIMIT {
            return None;
        }

        let cold = match self
            .segment_mgr
            .find_candidate_row_ids_by_exact_index_values(
                column_index,
                &coerced_values,
                self.snapshot_seq,
                Some(COLD_INDEX_CURSOR_CANDIDATE_LIMIT.saturating_sub(row_ids.len())),
            ) {
            Ok(Some(row_ids)) => row_ids,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        row_ids.extend(cold);
        if row_ids.len() > COLD_INDEX_CURSOR_CANDIDATE_LIMIT {
            return None;
        }
        row_ids.sort_unstable();
        row_ids.dedup();

        Some(Ok(ExactIndexCandidates {
            column_index,
            requested_values: coerced_values.into_iter().collect(),
            row_ids,
            requires_recheck: true,
        }))
    }

    /// Materialize a bounded row-id set while the caller owns the table
    /// membership and segment-publication fences.
    ///
    /// This helper must not acquire `seal_fence` itself.  The scan planner
    /// calls it after taking the shared fence; recursively taking the same
    /// reader lock can deadlock as soon as checkpoint or compaction queues an
    /// exclusive waiter.
    pub(super) fn collect_rows_by_ids_grouped_unfenced(
        &self,
        row_ids: &[i64],
        projection: Option<&[usize]>,
    ) -> Result<RowVec> {
        let schema = self.hot.schema().clone();
        let mut rows_by_position = vec![None; row_ids.len()];
        let hot_result = match projection {
            Some(columns) => self.hot.collect_rows_by_ids_projected(row_ids, columns)?,
            None => self.hot.collect_rows_by_ids(row_ids)?,
        };
        let hot_by_id: FxHashMap<i64, Row> = hot_result.into_iter().collect();
        for (position, row_id) in row_ids.iter().enumerate() {
            rows_by_position[position] = hot_by_id.get(row_id).cloned();
        }

        let snapshot = self.segment_mgr.cold_snapshot();
        let mut cold_work = Vec::new();
        for (position, &row_id) in row_ids.iter().enumerate() {
            if rows_by_position[position].is_none() {
                if let Some((segment_id, cold, row_index)) =
                    self.find_segment_row_in_snapshot(&snapshot, row_id)
                {
                    let group_index = match cold.volume.artifact_source() {
                        Some(source) => source.row_group_for_row(row_index)?,
                        None => 0,
                    };
                    cold_work.push((segment_id, group_index, row_index, position, cold));
                }
            }
        }
        cold_work
            .sort_unstable_by_key(|(segment_id, group, row, _, _)| (*segment_id, *group, *row));

        let mut mappings: FxHashMap<u64, crate::volume::writer::ColumnMapping> =
            FxHashMap::default();
        let mut work_start = 0;
        while work_start < cold_work.len() {
            let (segment_id, group_index, _, _, cold) = &cold_work[work_start];
            let mut work_end = work_start + 1;
            while work_end < cold_work.len()
                && cold_work[work_end].0 == *segment_id
                && cold_work[work_end].1 == *group_index
            {
                work_end += 1;
            }

            let mapping = mappings
                .entry(*segment_id)
                .or_insert_with(|| self.segment_mgr.get_cold_segment_mapping(cold, &schema));
            if let Some(source) = cold.volume.artifact_source() {
                // Decode the projected physical DATA columns once for the
                // complete selected row group, then materialize requested rows.
                let logical_columns: Vec<usize> = projection.map_or_else(
                    || (0..mapping.sources.len()).collect(),
                    |columns| columns.to_vec(),
                );
                let group_work = &cold_work[work_start..work_end];
                let mut row_values: Vec<Vec<Value>> = (0..group_work.len())
                    .map(|_| Vec::with_capacity(logical_columns.len()))
                    .collect();
                let mut physical_columns = logical_columns
                    .iter()
                    .filter_map(|logical_idx| match mapping.sources.get(*logical_idx) {
                        Some(ColSource::Volume(volume_idx)) => Some(*volume_idx),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                physical_columns.sort_unstable();
                physical_columns.dedup();
                let batch = source.read_columns(*group_index, &physical_columns)?;
                let group_start = batch.row_range().start;

                for logical_idx in &logical_columns {
                    let col_source = mapping.sources.get(*logical_idx).ok_or_else(|| {
                        radixdb_core::Error::internal(format!(
                            "projected logical column index {logical_idx} exceeds mapping width {}",
                            mapping.sources.len()
                        ))
                    })?;
                    match col_source {
                        ColSource::Volume(volume_idx) => {
                            let column = batch
                                .columns()
                                .iter()
                                .find_map(|(actual_idx, column)| {
                                    (*actual_idx == *volume_idx).then_some(column)
                                })
                                .ok_or_else(|| {
                                    radixdb_core::Error::internal(format!(
                                        "DATA projected group read produced no column {volume_idx}"
                                    ))
                                })?;
                            for (values, (_, _, row_index, _, _)) in
                                row_values.iter_mut().zip(group_work)
                            {
                                let local_idx =
                                    row_index.checked_sub(group_start).ok_or_else(|| {
                                        radixdb_core::Error::internal(
                                            "DATA grouped row index precedes its row group",
                                        )
                                    })?;
                                values.push(column.get_value(local_idx));
                            }
                        }
                        ColSource::Default(value) => {
                            for values in &mut row_values {
                                values.push(value.clone());
                            }
                        }
                    }
                }

                crate::instrumentation::record_row_materialization_count(
                    group_work.len() as u64,
                    group_work.len().saturating_mul(logical_columns.len()) as u64,
                );
                for (values, (_, _, _, position, _)) in row_values.into_iter().zip(group_work) {
                    rows_by_position[*position] = Some(Row::from_values(values));
                }
            } else {
                // Transient eager volumes keep the in-memory mapping path.
                for (_, _, row_index, position, cold) in &cold_work[work_start..work_end] {
                    let row = match projection {
                        Some(columns) => {
                            if mapping.is_identity {
                                cold.volume.get_row_projected(*row_index, columns)
                            } else {
                                cold.volume
                                    .get_row_mapped_projected(*row_index, mapping, columns)
                            }
                        }
                        None => {
                            if mapping.is_identity {
                                cold.volume.get_row(*row_index)
                            } else {
                                cold.volume.get_row_mapped(*row_index, mapping)
                            }
                        }
                    };
                    rows_by_position[*position] = Some(row);
                }
            }
            work_start = work_end;
        }

        let mut result = RowVec::with_capacity(row_ids.len());
        for (&row_id, row) in row_ids.iter().zip(rows_by_position) {
            if let Some(row) = row {
                result.push((row_id, row));
            }
        }
        Ok(result)
    }
}
