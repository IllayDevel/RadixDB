macro_rules! segmented_table_mutation_methods {
    () => {
        // =========================================================================
        // DML — writes go to hot buffer, constraints checked against segments
        // =========================================================================

        fn insert(&mut self, row: Row) -> Result<Row> {
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if self.segment_mgr.has_segments() {
                self.check_segment_constraints(&row)?;
            }
            let result = self.hot.insert(row)?;
            if self.segment_mgr.has_segments() {
                self.segment_mgr.record_txn_seal_generation(self.txn_id());
            }
            Ok(result)
        }

        fn insert_discard(&mut self, row: Row) -> Result<()> {
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            if self.segment_mgr.has_segments() {
                self.check_segment_constraints(&row)?;
            }
            self.hot.insert_discard(row)?;
            if self.segment_mgr.has_segments() {
                self.segment_mgr.record_txn_seal_generation(self.txn_id());
            }
            Ok(())
        }

        fn insert_batch(&mut self, rows: Vec<Row>) -> Result<()> {
            let statement_boundary = get_fast_timestamp();
            let result = (|| {
                let _seal_guard = self.segment_mgr.acquire_seal_read();
                if self.segment_mgr.has_segments() {
                    // Snapshot once for the entire batch — eliminates 3 lock reads per row.
                    let snapshot = self.segment_mgr.cold_snapshot();
                    let constraint_started = Instant::now();
                    let primary_key_started = Instant::now();
                    let primary_key_result =
                        self.check_integer_primary_key_batch_with_snapshot(&snapshot, &rows);
                    let primary_key_elapsed = primary_key_started.elapsed();
                    let constraint_result: Result<()> = (|| {
                        let primary_key_checked = primary_key_result?;
                        for row in &rows {
                            self.check_segment_constraints_with_snapshot_mode(
                                &snapshot,
                                row,
                                !primary_key_checked,
                            )?;
                        }
                        Ok(())
                    })();
                    crate::instrumentation::record_cold_constraint_batch(
                        rows.len(),
                        snapshot.seg_ids.len(),
                        primary_key_elapsed,
                        constraint_started.elapsed(),
                    );
                    constraint_result?;
                }
                self.hot.insert_batch(rows)?;
                if self.segment_mgr.has_segments() {
                    self.segment_mgr.record_txn_seal_generation(self.txn_id());
                }
                Ok(())
            })();
            if result.is_err() {
                self.rollback_statement_to(statement_boundary);
            }
            result
        }

        fn update(
            &mut self,
            where_expr: Option<&dyn Expression>,
            setter: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
        ) -> Result<i32> {
            let statement_boundary = get_fast_timestamp();
            let result = (|| {
                self.preclaim_point_update(where_expr)?;
                let _seal_guard = self.segment_mgr.acquire_seal_read();
                let index_undo = ColdIndexRemovalStatementGuard::new(
                    Arc::clone(&self.segment_mgr),
                    self.txn_id(),
                );
                let mut count = self.hot.update(where_expr, setter)?;

                let has_int_pk = self
                    .hot
                    .schema()
                    .columns
                    .iter()
                    .any(|c| c.primary_key && c.data_type == DataType::Integer);

                let mut hot_skip: FxHashSet<i64> =
                    FxHashSet::with_capacity_and_hasher(10_000, Default::default());
                self.hot.collect_hot_row_ids_into(&mut hot_skip);
                self.segment_mgr
                    .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);
                let schema_clone = self.hot.schema().clone();

                let column_indices: Vec<usize> = (0..schema_clone.columns.len()).collect();
                let mut scanners =
                    self.create_segment_scanners_filtered(&column_indices, where_expr, hot_skip);

                for scanner in scanners.iter_mut() {
                    while scanner.next() {
                        let row_id = scanner.current_row_id()?;
                        // Claim before reading/evaluating. A previous cold-row writer
                        // promotes its committed version into hot; after waiting we
                        // must prefer that hot shadow over the scanner's old payload.
                        self.hot.try_claim_row(row_id)?;
                        if self.hot.has_row_id(row_id) {
                            let mut apply_if_still_matching = |row: Row| {
                                if let Some(expr) = where_expr {
                                    if !expr.evaluate(&row)? {
                                        return Ok((row, false));
                                    }
                                }
                                setter(row)
                            };
                            let hot_count = self
                                .hot
                                .update_by_row_ids(&[row_id], &mut apply_if_still_matching)?;
                            count += hot_count;
                            continue;
                        }
                        // A writer may have deleted this row while we waited. Re-check
                        // cold membership after publication rather than applying the
                        // scanner payload captured before the wait.
                        if self.find_segment_row_for_read(row_id).is_none() {
                            continue;
                        }

                        let row = scanner.row().clone();
                        let old_row = row.clone();
                        let (new_row, changed) = setter(row)?;
                        if changed {
                            // Check unique constraints against cold segments.
                            if self.hot.has_unique_non_pk_indexes() {
                                self.check_cold_unique_for_update(&new_row, row_id)?;
                            }

                            // Keep the immutable predecessor visible to other
                            // transactions. Its index entries join the hot shadow's
                            // commit-time index transition and are never removed early.
                            self.stage_populated_cold_index_entries_for_row(row_id, &old_row)?;

                            // Insert the NEW row into hot. For int PK tables, first
                            // mirror the old row (so UPDATE can find it), then update.
                            // If any step fails, clean up to avoid phantoms.
                            if has_int_pk {
                                match self.hot.insert_discard(old_row.clone()) {
                                    Ok(())
                                    | Err(radixdb_core::Error::PrimaryKeyConstraint { .. })
                                    | Err(radixdb_core::Error::UniqueConstraint { .. }) => {}
                                    Err(e) => {
                                        return Err(e);
                                    }
                                }
                                let mut new_row_opt = Some(new_row);
                                let update_result =
                                    self.hot.update_by_row_ids(&[row_id], &mut |_| {
                                        Ok((new_row_opt.take().unwrap_or_else(Row::new), true))
                                    });
                                if let Err(e) = update_result {
                                    let _ = self.hot.delete_by_row_ids(&[row_id]);
                                    return Err(e);
                                }
                            } else {
                                self.hot.insert_discard(new_row)?;
                            }
                            // Add tombstone so row_count() doesn't double-count.
                            // The hot version now shadows the cold version via skip set.
                            self.segment_mgr
                                .add_pending_tombstone(self.txn_id(), row_id);
                            count += 1;
                        }
                    }
                    if let Some(err) = scanner.err() {
                        return Err(err.clone());
                    }
                    scanner.close()?;
                }
                if count > 0 {
                    self.segment_mgr.record_txn_seal_generation(self.txn_id());
                }
                index_undo.finish();
                Ok(count)
            })();
            if result.is_err() {
                self.rollback_statement_to(statement_boundary);
            }
            result
        }

        fn update_by_row_ids(
            &mut self,
            row_ids: &[i64],
            setter: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
        ) -> Result<i32> {
            let statement_boundary = get_fast_timestamp();
            let result = (|| {
                self.hot.try_claim_rows(row_ids)?;
                let _seal_guard = self.segment_mgr.acquire_seal_read();
                let index_undo = ColdIndexRemovalStatementGuard::new(
                    Arc::clone(&self.segment_mgr),
                    self.txn_id(),
                );
                let mut count = 0i32;
                let mut hot_ids = Vec::new();
                let schema = self.hot.schema().clone();
                let has_int_pk = schema
                    .columns
                    .iter()
                    .any(|c| c.primary_key && c.data_type == DataType::Integer);

                for &row_id in row_ids {
                    if let Some((_seg_id, cold, idx)) = self.find_segment_row_for_read(row_id) {
                        self.hot.try_claim_row(row_id)?;
                        if self.hot.has_row_id(row_id) {
                            let hot_count = self.hot.update_by_row_ids(&[row_id], setter)?;
                            count += hot_count;
                            continue;
                        }
                        if self.find_segment_row_for_read(row_id).is_none() {
                            continue;
                        }
                        let mapping = self.segment_mgr.get_cold_segment_mapping(&cold, &schema);
                        let row =
                            self.materialize_volume_row_for_read(&cold.volume, idx, &mapping)?;
                        let old_row = row.clone();
                        let (new_row, changed) = setter(row)?;
                        if changed {
                            if self.hot.has_unique_non_pk_indexes() {
                                self.check_cold_unique_for_update(&new_row, row_id)?;
                            }
                            self.stage_populated_cold_index_entries_for_row(row_id, &old_row)?;
                            let result = if has_int_pk {
                                let insert_ok = match self.hot.insert_discard(old_row.clone()) {
                                    Ok(()) => true,
                                    Err(radixdb_core::Error::PrimaryKeyConstraint { .. }) => true,
                                    Err(radixdb_core::Error::UniqueConstraint { .. }) => true,
                                    Err(e) => return Err(e),
                                };
                                if insert_ok {
                                    let mut new_row_opt = Some(new_row);
                                    self.hot
                                        .update_by_row_ids(&[row_id], &mut |_| {
                                            Ok((new_row_opt.take().unwrap_or_else(Row::new), true))
                                        })
                                        .map(|_| ())
                                } else {
                                    Ok(())
                                }
                            } else {
                                self.hot.insert_discard(new_row)
                            };
                            result?;
                            // Add tombstone so row_count() doesn't double-count.
                            // The hot version now shadows the cold version via skip set.
                            self.segment_mgr
                                .add_pending_tombstone(self.txn_id(), row_id);
                            count += 1;
                        }
                    } else {
                        hot_ids.push(row_id);
                    }
                }
                if !hot_ids.is_empty() {
                    // Same reasoning as update(): skip cold unique check for hot path
                    // to avoid false violations during ON CONFLICT DO UPDATE.
                    count += self.hot.update_by_row_ids(&hot_ids, setter)?;
                }
                if count > 0 {
                    self.segment_mgr.record_txn_seal_generation(self.txn_id());
                }
                index_undo.finish();
                Ok(count)
            })();
            if result.is_err() {
                self.rollback_statement_to(statement_boundary);
            }
            result
        }

        fn delete_by_row_ids(&mut self, row_ids: &[i64]) -> Result<i32> {
            self.stage_delete_candidates(row_ids, None, None)
        }

        fn collect_delete_candidate_row_ids(
            &self,
            where_expr: Option<&dyn Expression>,
        ) -> Result<Vec<i64>> {
            // Fence the topology before testing the hot-only shortcut.  The first
            // seal changes `has_segments` from false to true and removes the same
            // rows from hot storage.  Checking the flag before taking this guard
            // allowed DELETE discovery to scan a hot store while its rows were
            // being published cold, silently returning an incomplete candidate
            // set.
            let segment_mgr = Arc::clone(&self.segment_mgr);
            let _seal_guard = segment_mgr.acquire_seal_read();
            if !self.segment_mgr.has_segments() {
                return self.hot.collect_delete_candidate_row_ids(where_expr);
            }

            // Hot row IDs shadow older cold copies even when the current hot value
            // no longer matches the DELETE predicate. Keep one authoritative hot
            // snapshot for both shadowing and candidate discovery.
            let hot_snapshot = self.hot.collect_all_rows(None)?;
            let mut hot_skip = FxHashSet::with_capacity_and_hasher(
                hot_snapshot.len().saturating_mul(2).max(1),
                Default::default(),
            );
            let mut candidates = Vec::new();
            for (row_id, row) in hot_snapshot {
                hot_skip.insert(row_id);
                let matches = match where_expr {
                    Some(expr) => expr.evaluate(&row)?,
                    None => true,
                };
                if matches {
                    candidates.push(row_id);
                }
            }
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut scanners =
                self.create_segment_scanners_filtered_exact_projection(&[], where_expr, hot_skip);
            for scanner in scanners.iter_mut() {
                if !scanner.collect_remaining_row_ids(&mut candidates)? {
                    while scanner.next() {
                        candidates.push(scanner.current_row_id()?);
                    }
                }
                if let Some(error) = scanner.err() {
                    return Err(error.clone());
                }
                scanner.close()?;
            }

            candidates.sort_unstable();
            candidates.dedup();
            Ok(candidates)
        }

        fn delete_candidate_row_ids(
            &mut self,
            row_ids: &[i64],
            recheck_expr: Option<&dyn Expression>,
        ) -> Result<i32> {
            self.stage_delete_candidates(row_ids, recheck_expr, None)
        }

        fn delete_candidate_row_ids_collect(
            &mut self,
            row_ids: &[i64],
            recheck_expr: Option<&dyn Expression>,
            deleted_row_ids: &mut Vec<i64>,
        ) -> Result<i32> {
            self.stage_delete_candidates(row_ids, recheck_expr, Some(deleted_row_ids))
        }

        fn get_active_row_ids(&self) -> Vec<i64> {
            let hot_ids = self.hot.get_active_row_ids();

            if !self.segment_mgr.has_segments() {
                return hot_ids;
            }

            // Build hot_skip from hot row_ids + pending tombstones.
            // Committed tombstones are kept as a shared Arc (no clone).
            let volumes = self.segment_mgr.get_volumes_newest_first_lazy();
            let tombstones_arc = self.segment_mgr.tombstone_set_arc();
            let mut hot_skip: FxHashSet<i64> =
                FxHashSet::with_capacity_and_hasher(10_000, Default::default());
            for id in &hot_ids {
                hot_skip.insert(*id);
            }
            self.segment_mgr
                .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);

            let mut ids = Vec::new();
            for (_, cs) in volumes.iter() {
                let vol = &cs.volume;
                for i in 0..vol.meta.row_count {
                    if !cs.is_visible(i) {
                        continue;
                    }
                    let id = vol.meta.row_ids.at(i);
                    if !self.is_row_tombstoned(&tombstones_arc, id) && !hot_skip.contains(&id) {
                        ids.push(id);
                    }
                }
            }
            ids.extend(hot_ids);
            ids
        }

        fn delete(&mut self, where_expr: Option<&dyn Expression>) -> Result<i32> {
            let mut count = self.hot.delete(where_expr)?;

            // Candidate discovery and mutation are deliberately separate. The
            // initial hot pass handles the snapshot visible at statement start;
            // stage_delete_candidates revalidates cold candidates after acquiring
            // claims and catches rows concurrently promoted into hot MVCC. Keep
            // the discovery fence in this scope only: stage_delete_candidates
            // takes the same read fence after claim acquisition. Retaining this
            // guard across that call deadlocks when a queued compaction writer
            // gives parking_lot writer priority to the nested read acquisition.
            let candidates = {
                let segment_mgr = Arc::clone(&self.segment_mgr);
                let _seal_guard = segment_mgr.acquire_seal_read();
                let mut hot_skip: FxHashSet<i64> =
                    FxHashSet::with_capacity_and_hasher(10_000, Default::default());
                self.hot.collect_hot_row_ids_into(&mut hot_skip);
                self.segment_mgr
                    .insert_pending_tombstones_into(self.txn_id(), &mut hot_skip);
                let mut candidates = Vec::new();

                if let Some(expr) = where_expr {
                    // The scanner adds predicate columns to its internal needed set;
                    // exact-empty output therefore discovers row IDs without unrelated
                    // payload materialization. Populated partial/HNSW cleanup is done
                    // only after claims, against the authoritative current cold row.
                    let mut scanners = self.create_segment_scanners_filtered_exact_projection(
                        &[],
                        Some(expr),
                        hot_skip,
                    );
                    for scanner in scanners.iter_mut() {
                        if !scanner.collect_remaining_row_ids(&mut candidates)? {
                            while scanner.next() {
                                candidates.push(scanner.current_row_id()?);
                            }
                        }
                        if let Some(err) = scanner.err() {
                            return Err(err.clone());
                        }
                        scanner.close()?;
                    }
                } else {
                    let volumes = self.segment_mgr.get_volumes_newest_first_lazy();
                    let tombstones_arc = self.segment_mgr.tombstone_set_arc();
                    for (_, cs) in volumes.iter() {
                        let vol = &cs.volume;
                        for i in 0..vol.meta.row_count {
                            if !cs.is_visible(i) {
                                continue;
                            }
                            let row_id = vol.meta.row_ids.at(i);
                            if self.is_row_tombstoned(&tombstones_arc, row_id)
                                || hot_skip.contains(&row_id)
                            {
                                continue;
                            }
                            candidates.push(row_id);
                        }
                    }
                }
                candidates
            };

            count += self.stage_delete_candidates(&candidates, where_expr, None)?;
            Ok(count)
        }

        fn truncate(&mut self) -> Result<i32> {
            let _seal_guard = self.segment_mgr.acquire_seal_read();
            let seg_rows = self.segment_mgr.total_row_count() as i32;
            let txn_id = self.txn_id();
            let manager = Arc::clone(&self.segment_mgr);
            let mut clear_cold = move || {
                // This callback runs only after the hot store has proved there are
                // no active claims and while new hot commits remain excluded.
                manager.rollback_pending_tombstones(txn_id);
                manager.clear();
                Ok(())
            };
            let hot_rows = self.hot.truncate_after(&mut clear_cold)?;
            Ok(hot_rows.saturating_add(seg_rows))
        }
    };
}

pub(super) use segmented_table_mutation_methods;
