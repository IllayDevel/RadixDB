//! Cross-tier index maintenance and constraint validation.

use super::*;

impl SegmentedTable {
    pub(super) fn coerce_row_to_schema(&self, row: &Row) -> Row {
        let schema = self.hot.schema();
        let values = schema
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                row.get(idx)
                    .cloned()
                    .unwrap_or(Value::Null(col.data_type))
                    .coerce_to_type(col.data_type)
            })
            .collect();
        Row::from_values(values)
    }

    pub(super) fn cold_index_values_for_row(
        &self,
        index: &dyn Index,
        row: &Row,
    ) -> Result<Option<Vec<Value>>> {
        let coerced = self.coerce_row_to_schema(row);
        index_values_for_row(index, &coerced)
    }

    pub(super) fn unique_key_has_null(values: &[Value]) -> bool {
        values.iter().any(|v| v.is_null())
    }

    pub(super) fn unique_constraint_error(
        index: &dyn Index,
        values: &[Value],
        row_id: i64,
    ) -> radixdb_core::Error {
        radixdb_core::Error::UniqueConstraint {
            index: index.name().to_string(),
            column: index.column_names().join(", "),
            value: values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            row_id,
        }
    }

    pub(super) fn find_partial_segment_unique_conflict(
        &self,
        index: &dyn Index,
        target_values: &[Value],
        exclude_row_id: Option<i64>,
    ) -> Result<Option<i64>> {
        debug_assert!(index.partial_predicate().is_some());
        let full_column_indices = self.full_schema_column_indices();
        let mut scanners = self.create_segment_scanners_filtered(
            &full_column_indices,
            None,
            self.cold_scan_skip_set(),
        );
        for scanner in scanners.iter_mut() {
            while scanner.next() {
                let rid = scanner.current_row_id()?;
                if exclude_row_id == Some(rid) {
                    continue;
                }
                let row = scanner.row();
                let Some(values) = self.cold_index_values_for_row(index, row)? else {
                    continue;
                };
                if Self::unique_key_has_null(&values) {
                    continue;
                }
                if values == target_values {
                    return Ok(Some(rid));
                }
            }
            if let Some(err) = scanner.err() {
                return Err(err.clone());
            }
            scanner.close()?;
        }
        Ok(None)
    }

    pub(super) fn stage_populated_cold_index_entries_for_row(
        &self,
        row_id: i64,
        old_row: &Row,
    ) -> Result<()> {
        for index in self.hot.get_indexes() {
            if index.index_type() == IndexType::PrimaryKey {
                continue;
            }
            if index.index_type() != IndexType::Hnsw
                && !self.segment_mgr.is_cold_populated_index(index.name())
            {
                continue;
            }
            let Some(values) = self.cold_index_values_for_row(index.as_ref(), old_row)? else {
                continue;
            };
            if Self::unique_key_has_null(&values) {
                continue;
            }
            self.segment_mgr.record_cold_index_removal(
                self.txn_id(),
                Arc::clone(&index),
                values,
                row_id,
            );
        }
        Ok(())
    }

    pub(super) fn has_populated_cold_index_entries(&self) -> bool {
        self.hot.get_indexes().iter().any(|index| {
            index.index_type() == IndexType::Hnsw
                || self.segment_mgr.is_cold_populated_index(index.name())
        })
    }

    /// Validate that cold segment data has no duplicate values for a unique index.
    /// Called before CREATE UNIQUE INDEX to prevent certifying already-invalid data.
    pub(super) fn validate_cold_unique(&self, index_name: &str, columns: &[&str]) -> Result<()> {
        let schema = self.hot.schema();
        let col_indices: Vec<usize> = columns
            .iter()
            .filter_map(|c| {
                schema
                    .columns
                    .iter()
                    .position(|sc| sc.name_lower == c.to_lowercase())
            })
            .collect();
        if col_indices.len() != columns.len() {
            return Ok(());
        }

        let mut seen_values: ahash::AHashMap<Vec<Value>, i64> = ahash::AHashMap::new();

        // Skip hot row_ids and pending tombstones: hot rows shadow older cold
        // copies, and current-transaction tombstones hide rows being changed.
        let hot_skip = self.cold_scan_skip_set();

        let mut scanners = self.create_segment_scanners_filtered(&col_indices, None, hot_skip);
        for scanner in scanners.iter_mut() {
            while scanner.next() {
                let rid = scanner.current_row_id()?;
                let values: Vec<Value> = scanner.row().iter().cloned().collect();
                if values.iter().any(|v| v.is_null()) {
                    continue;
                }
                if let Some(&existing_rid) = seen_values.get(&values) {
                    return Err(radixdb_core::Error::UniqueConstraint {
                        index: index_name.to_string(),
                        column: columns.join(", "),
                        value: values
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                        row_id: existing_rid,
                    });
                }
                seen_values.insert(values, rid);
            }
            if let Some(err) = scanner.err() {
                return Err(err.clone());
            }
            scanner.close()?;
        }

        // Certify the complete transaction-visible domain. Cold rows shadowed
        // by hot IDs were skipped above, so adding the hot/local side here is
        // both complete and duplicate-free by row identity.
        for (rid, row) in self.hot.collect_all_rows(None)? {
            let values: Vec<Value> = col_indices
                .iter()
                .filter_map(|&idx| row.get(idx).cloned())
                .collect();
            if values.len() != col_indices.len() || values.iter().any(Value::is_null) {
                continue;
            }
            if let Some(&existing_rid) = seen_values.get(&values) {
                return Err(radixdb_core::Error::UniqueConstraint {
                    index: index_name.to_string(),
                    column: columns.join(", "),
                    value: values
                        .iter()
                        .map(Value::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    row_id: existing_rid,
                });
            }
            seen_values.insert(values, rid);
        }
        Ok(())
    }

    /// Populate an index from cold segment data.
    /// Called after index creation on the hot store.
    /// Propagates errors so unique-constraint violations are not swallowed.
    pub(super) fn populate_detached_index_from_cold(
        &self,
        index: &Arc<dyn Index>,
        columns: &[&str],
    ) -> Result<()> {
        let schema = self.hot.schema();
        let col_indices: Vec<usize> = columns
            .iter()
            .filter_map(|c| {
                schema
                    .columns
                    .iter()
                    .position(|sc| sc.name_lower == c.to_lowercase())
            })
            .collect();
        if col_indices.is_empty() {
            return Ok(());
        }

        if index.partial_predicate().is_some() {
            let full_column_indices = self.full_schema_column_indices();
            let mut scanners = self.create_segment_scanners_filtered(
                &full_column_indices,
                None,
                self.cold_scan_skip_set(),
            );
            for scanner in scanners.iter_mut() {
                while scanner.next() {
                    let rid = scanner.current_row_id()?;
                    let row = scanner.row();
                    let Some(values) = self.cold_index_values_for_row(index.as_ref(), row)? else {
                        continue;
                    };
                    if !Self::unique_key_has_null(&values) {
                        index.add(&values, rid, rid)?;
                    }
                }
                if let Some(err) = scanner.err() {
                    return Err(err.clone());
                }
                scanner.close()?;
            }
            return Ok(());
        }

        let mut scanners =
            self.create_segment_scanners_filtered(&col_indices, None, self.cold_scan_skip_set());
        for scanner in scanners.iter_mut() {
            while scanner.next() {
                let rid = scanner.current_row_id()?;
                let values: Vec<Value> = scanner.row().iter().cloned().collect();
                if !values.iter().any(|v| v.is_null()) {
                    index.add(&values, rid, rid)?;
                }
            }
            if let Some(err) = scanner.err() {
                return Err(err.clone());
            }
            scanner.close()?;
        }
        Ok(())
    }

    /// Get a fast approximate row count across all segments.
    /// Does NOT deduplicate overlapping row_ids. Use for hints only.
    pub(super) fn segment_row_count_hint(&self) -> usize {
        self.segment_mgr.total_row_count()
    }

    /// Get the physical posting domain across immutable segments.
    ///
    /// Unlike `segment_row_count_hint`, this includes tombstoned ordinals and
    /// is therefore safe as a limit applied before visibility filtering.
    pub(super) fn segment_physical_row_count_hint(&self) -> usize {
        self.segment_mgr.total_physical_row_count()
    }

    /// Find the first visible cold row matching a set of equality predicates.
    ///
    /// This is used by both unique constraint checks and ON CONFLICT probing so
    /// volume-backed upserts can find the conflicting cold row_id directly.
    /// Delegates to SegmentManager which handles tombstone filtering internally.
    pub(super) fn find_segment_row_id_by_values(
        &self,
        col_indices: &[usize],
        values: &[&Value],
    ) -> Result<Option<i64>> {
        let snapshot = self.segment_mgr.cold_snapshot();
        self.find_segment_row_id_by_values_with_snapshot(&snapshot, col_indices, values)
    }

    pub(super) fn find_segment_row_id_by_values_with_snapshot(
        &self,
        snapshot: &crate::volume::manifest::ColdSnapshot,
        col_indices: &[usize],
        values: &[&Value],
    ) -> Result<Option<i64>> {
        if col_indices.is_empty() || col_indices.len() != values.len() {
            return Ok(None);
        }

        let defaults: smallvec::SmallVec<[Value; 4]> = col_indices
            .iter()
            .map(|&ci| self.column_default(ci))
            .collect();
        let result = self.segment_mgr.find_row_id_by_values_with_snapshot(
            snapshot,
            col_indices,
            values,
            &defaults,
        )?;
        let Some(rid) = result else {
            return Ok(None);
        };

        if self.segment_mgr.is_pending_tombstone(self.txn_id(), rid) {
            return Ok(None);
        }

        Ok(Some(rid))
    }

    /// Check PK and UNIQUE constraints against segment data before INSERT.
    ///
    /// Uses zone maps, bloom filters, dictionary pre-filters, and binary search
    /// on sorted columns for fast rejection. No index population needed.
    /// Check cold unique constraints for UPDATE, excluding the row being updated.
    pub(super) fn check_cold_unique_for_update(
        &self,
        new_row: &Row,
        exclude_row_id: i64,
    ) -> Result<()> {
        let schema = self.hot.schema();
        for index in self.hot.get_unique_non_pk_indexes() {
            if index.partial_predicate().is_some() {
                let Some(values) = self.cold_index_values_for_row(index.as_ref(), new_row)? else {
                    continue;
                };
                if Self::unique_key_has_null(&values) {
                    continue;
                }
                if let Some(found_id) = self.find_partial_segment_unique_conflict(
                    index.as_ref(),
                    &values,
                    Some(exclude_row_id),
                )? {
                    return Err(Self::unique_constraint_error(
                        index.as_ref(),
                        &values,
                        found_id,
                    ));
                }
                continue;
            }

            let col_names = index.column_names();
            let col_indices: Vec<usize> = col_names
                .iter()
                .filter_map(|name| schema.columns.iter().position(|c| c.name_lower == *name))
                .collect();
            if col_indices.len() != col_names.len() {
                continue;
            }
            let coerced: Vec<Value> = col_indices
                .iter()
                .filter_map(|&idx| {
                    let val = new_row.get(idx)?;
                    let target_type = schema.columns[idx].data_type;
                    Some(val.coerce_to_type(target_type))
                })
                .collect();
            if coerced.len() != col_indices.len() || coerced.iter().any(|v| v.is_null()) {
                continue;
            }
            let values: Vec<&Value> = coerced.iter().collect();

            if let Some(found_id) = self.find_segment_row_id_by_values(&col_indices, &values)? {
                if found_id != exclude_row_id {
                    return Err(radixdb_core::Error::UniqueConstraint {
                        index: index.name().to_string(),
                        column: col_names.join(", "),
                        value: values
                            .iter()
                            .map(|v| format!("{}", v))
                            .collect::<Vec<_>>()
                            .join(", "),
                        row_id: found_id,
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) fn check_segment_constraints(&self, row: &Row) -> Result<()> {
        let snapshot = self.segment_mgr.cold_snapshot();
        self.check_segment_constraints_with_snapshot(&snapshot, row)
    }

    pub(super) fn check_segment_constraints_with_snapshot(
        &self,
        snapshot: &crate::volume::manifest::ColdSnapshot,
        row: &Row,
    ) -> Result<()> {
        self.check_segment_constraints_with_snapshot_mode(snapshot, row, true)
    }

    pub(super) fn check_segment_constraints_with_snapshot_mode(
        &self,
        snapshot: &crate::volume::manifest::ColdSnapshot,
        row: &Row,
        check_primary_key: bool,
    ) -> Result<()> {
        let schema = self.hot.schema();

        // 1. PK constraint: binary search on sorted INT column
        if check_primary_key {
            if let Some(pk_idx) = schema.pk_column_index() {
                if let Some(pk_value) = row.get(pk_idx) {
                    if !pk_value.is_null() {
                        if let Some(row_id) = self
                            .segment_mgr
                            .check_value_exists_with_snapshot(snapshot, pk_idx, pk_value)?
                        {
                            // check_value_exists_in_segments filters committed tombstones.
                            // Check pending tombstones (no Vec clone).
                            if !self.segment_mgr.is_pending_tombstone(self.txn_id(), row_id) {
                                return Err(radixdb_core::Error::PrimaryKeyConstraint { row_id });
                            }
                            // This txn deleted this cold row, so the PK can be
                            // replaced; non-PK UNIQUE constraints must still be
                            // checked below.
                        }
                    }
                }
            }
        }

        // 2. UNIQUE constraints: scan cold segments with pruning
        if !self.hot.has_unique_non_pk_indexes() {
            return Ok(());
        }
        for index in self.hot.get_unique_non_pk_indexes() {
            if index.partial_predicate().is_some() {
                let Some(values) = self.cold_index_values_for_row(index.as_ref(), row)? else {
                    continue;
                };
                if Self::unique_key_has_null(&values) {
                    continue;
                }
                if let Some(conflict_row_id) =
                    self.find_partial_segment_unique_conflict(index.as_ref(), &values, None)?
                {
                    return Err(Self::unique_constraint_error(
                        index.as_ref(),
                        &values,
                        conflict_row_id,
                    ));
                }
                continue;
            }

            let col_names = index.column_names();
            let col_indices: Vec<usize> = col_names
                .iter()
                .filter_map(|name| schema.columns.iter().position(|c| c.name_lower == *name))
                .collect();
            if col_indices.len() != col_names.len() {
                continue;
            }
            // Coerce values to schema column types before comparing with cold segments.
            // Prepared statements may pass TEXT for TIMESTAMP columns — the row is
            // coerced later in prepare_insert, but we need the correct types NOW for
            // zone map / bloom / dict pruning and value comparison to work.
            let coerced: Vec<Value> = col_indices
                .iter()
                .filter_map(|&idx| {
                    let val = row.get(idx)?;
                    let target_type = schema.columns[idx].data_type;
                    Some(val.coerce_to_type(target_type))
                })
                .collect();
            if coerced.len() != col_indices.len() || coerced.iter().any(|v| v.is_null()) {
                continue;
            }
            let values: Vec<&Value> = coerced.iter().collect();

            if let Some(conflict_row_id) =
                self.find_segment_row_id_by_values_with_snapshot(snapshot, &col_indices, &values)?
            {
                return Err(radixdb_core::Error::UniqueConstraint {
                    index: index.name().to_string(),
                    column: col_names.join(", "),
                    value: values
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    row_id: conflict_row_id,
                });
            }
        }
        Ok(())
    }

    /// Check a complete INSERT batch against cold INTEGER PRIMARY KEY rows by
    /// intersecting resident row-id metadata.  Returning `false` asks the
    /// caller to use the generic value path (non-integer/malformed PK shape).
    pub(super) fn check_integer_primary_key_batch_with_snapshot(
        &self,
        snapshot: &crate::volume::manifest::ColdSnapshot,
        rows: &[Row],
    ) -> Result<bool> {
        let schema = self.hot.schema();
        let Some(pk_idx) = schema.pk_column_index() else {
            return Ok(false);
        };
        if schema.columns[pk_idx].data_type != DataType::Integer {
            return Ok(false);
        }

        let mut row_ids = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(Value::Integer(row_id)) = row.get(pk_idx) else {
                return Ok(false);
            };
            row_ids.push(*row_id);
        }
        row_ids.sort_unstable();
        row_ids.dedup();

        let mut skipped = FxHashSet::default();
        self.segment_mgr
            .insert_pending_tombstones_into(self.txn_id(), &mut skipped);
        if let Some(row_id) = self.segment_mgr.first_visible_row_id_in_sorted_batch(
            snapshot,
            row_ids.as_slice(),
            &skipped,
        ) {
            return Err(radixdb_core::Error::PrimaryKeyConstraint { row_id });
        }
        Ok(true)
    }
}
