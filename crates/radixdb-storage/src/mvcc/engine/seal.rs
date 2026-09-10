use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct SealOutcome {
    pub(super) stale_publication: bool,
}

impl MVCCEngine {
    /// Seal using a catalog-generation fence already owned by the caller.
    /// This avoids recursive shared locking in the checkpoint coordinator.
    pub(super) fn seal_hot_buffers_under_catalog_generation(&self) -> Result<SealOutcome> {
        self.seal_hot_buffers_under_catalog_generation_limited(None)
    }

    /// Drain at most one pressure candidate so foreground admission is never
    /// coupled to a whole-database maintenance pass.
    pub(super) fn seal_next_hot_buffer_under_catalog_generation(&self) -> Result<SealOutcome> {
        self.seal_hot_buffers_under_catalog_generation_limited(Some(1))
    }

    fn seal_hot_buffers_under_catalog_generation_limited(
        &self,
        max_tables: Option<usize>,
    ) -> Result<SealOutcome> {
        // Seal publication turns versioned hot rows into immutable rows which
        // no longer retain their originating transaction identity. Exclude a
        // concurrent Snapshot begin from the entire rewrite and defer if a
        // previously admitted Snapshot still needs that provenance.
        let _snapshot_maintenance = self.snapshot_maintenance_fence.read();
        if self.registry.get_min_snapshot_begin_seq().is_some() {
            return Ok(SealOutcome::default());
        }

        let seal_row_threshold = SEAL_ROW_THRESHOLD;
        let seal_incremental_threshold = SEAL_INCREMENTAL_THRESHOLD;
        let (target_volume_rows, seal_hot_bytes_threshold, seal_incremental_hot_bytes_threshold) =
            self.config
                .read()
                .map(|c| {
                    (
                        c.persistence.target_volume_rows,
                        c.persistence.seal_hot_bytes_threshold,
                        c.persistence.seal_incremental_hot_bytes_threshold,
                    )
                })
                .unwrap_or((1_048_576, 64 * 1024 * 1024, 16 * 1024 * 1024));

        match self.persistence() {
            Some(pm) if pm.is_enabled() => {}
            _ => return Ok(SealOutcome::default()),
        }
        let seal_runtime = self.runtime_maintenance.seal.start();
        let mut seal_had_failure = false;
        let mut stale_publication = false;

        // Snapshot-safe seal: cutoff is computed per-table (below) to minimize
        // the TOCTOU window between the check and extraction.

        // Collect table names that might need sealing.
        // Acquire each lock separately and drop before the next to avoid
        // holding multiple RwLocks simultaneously (deadlock prevention).
        let force_seal = self.force_seal_all.load(Ordering::Acquire);

        // Step 1: Collect candidates from version_stores (row count check)
        let candidates: Vec<(String, Arc<VersionStore>)> = {
            let stores = self.version_stores.read().unwrap();
            stores
                .iter()
                .filter_map(|(table_name, store)| {
                    let row_count = store.committed_row_count();
                    if force_seal {
                        if row_count == 0 {
                            return None;
                        }
                    } else if row_count == 0 {
                        return None;
                    }
                    Some((table_name.clone(), Arc::clone(store)))
                })
                .collect()
        };

        // Aggregate pressure is evaluated only for the one-table foreground
        // pressure cycle. Periodic maintenance keeps its established per-table
        // layout thresholds, while admission can still bound a database made
        // of many individually small hot owners.
        let aggregate_hot_bytes = candidates.iter().fold(0usize, |total, (_, store)| {
            total.saturating_add(store.committed_hot_bytes())
        });
        let aggregate_pressure = max_tables.is_some()
            && aggregate_hot_bytes >= total_hot_soft_threshold(seal_hot_bytes_threshold);

        // Step 2: Filter by threshold using segment_managers.
        // Cache has_segments per table to avoid re-acquiring the lock later.
        let candidates: Vec<(String, Arc<VersionStore>, bool, usize)> = if force_seal {
            let mgrs = self.segment_managers.read().unwrap();
            candidates
                .into_iter()
                .map(|(table_name, store)| {
                    let has_seg = mgrs
                        .get(&table_name)
                        .map(|m| m.has_segments())
                        .unwrap_or(false);
                    (table_name, store, has_seg, 1)
                })
                .collect()
        } else {
            let mgrs = self.segment_managers.read().unwrap();
            candidates
                .into_iter()
                .filter_map(|(table_name, store)| {
                    let row_count = store.committed_row_count();
                    let hot_bytes = store.committed_hot_bytes();
                    let has_seg = mgrs
                        .get(&table_name)
                        .map(|m| m.has_segments())
                        .unwrap_or(false);
                    let threshold = if has_seg {
                        seal_incremental_threshold
                    } else {
                        seal_row_threshold
                    };
                    let bytes_threshold = if has_seg {
                        seal_incremental_hot_bytes_threshold
                    } else {
                        seal_hot_bytes_threshold
                    };
                    if aggregate_pressure {
                        Some((table_name, store, has_seg, 1))
                    } else if row_count >= threshold {
                        Some((table_name, store, has_seg, threshold))
                    } else if hot_bytes >= bytes_threshold {
                        Some((table_name, store, has_seg, 1))
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Step 3: Look up schemas (separate lock acquisition)
        let mut table_names: Vec<CheckpointCandidate> = {
            let schema_version = self.schema_epoch.load(Ordering::Acquire);
            let schemas = self.schemas.read().unwrap();
            candidates
                .into_iter()
                .filter_map(|(table_name, store, has_seg, min_rows)| {
                    let schema = schemas.get(&table_name)?.clone();
                    Some((table_name, schema, store, has_seg, min_rows, schema_version))
                })
                .collect()
        };

        if let Some(max_tables) = max_tables {
            // Drain the hottest owner first. Once it falls below pressure the
            // next cycle naturally advances to another table; the stable name
            // tie-breaker keeps this choice deterministic for equal budgets.
            table_names.sort_unstable_by(|left, right| {
                right
                    .2
                    .committed_hot_bytes()
                    .cmp(&left.2.committed_hot_bytes())
                    .then_with(|| left.0.cmp(&right.0))
            });
            table_names.truncate(max_tables);
        }

        // Batch size for hot removal only. Volume is built once per table.
        // Smaller batches = shorter write lock hold time per batch.
        const REMOVE_BATCH_SIZE: usize = 50_000;

        for (table_name, _schema, store, has_segments, candidate_min_rows, schema_version) in
            table_names
        {
            // Extract rows in bounded chunks AND keep the CowBTree snapshot
            // (O(1) Arc clone) used by hot cleanup.
            // The snapshot records each row's txn_id at extraction time.
            // remove_sealed_rows compares against it to detect concurrent
            // commits that modified a row after extraction.
            // The outer snapshot-maintenance fence closes the TOCTOU window
            // between this cutoff and physical publication.
            let per_table_cutoff = self.registry.get_min_snapshot_begin_seq();
            let force_seal_now = self.force_seal_all.load(Ordering::Acquire);
            let seal_reason = if force_seal_now {
                "forced_checkpoint"
            } else if has_segments {
                "incremental_hot_pressure"
            } else {
                "initial_hot_pressure"
            };
            seal_runtime.set_detail_with_reason(&table_name, &[], seal_reason);
            let threshold = if has_segments {
                seal_incremental_threshold
            } else {
                seal_row_threshold
            };
            let min_rows = if force_seal_now {
                1
            } else {
                candidate_min_rows.min(threshold).max(1)
            };

            // Build volumes from rows, splitting at target_volume_rows boundary.
            let compress = self
                .config
                .read()
                .map(|c| c.persistence.volume_compression)
                .unwrap_or(true);
            let row_group_size = crate::volume::column::ROW_GROUP_SIZE;
            let chunk_size = if target_volume_rows == 0 {
                usize::MAX
            } else {
                (target_volume_rows / row_group_size).max(1) * row_group_size
            };
            let read_txn_id = INVALID_TRANSACTION_ID + 1;
            let mut sealed_volumes = Vec::new();
            let mut seal_error: Option<Error> = None;
            let mgr = self.get_or_create_segment_manager(&table_name);
            let candidate_hot_bytes = store.committed_hot_bytes() as u64;
            // Tombstones are an independently published part of table state.
            // Capture their exact pre-extraction generation so cleanup cannot
            // consume a cold-row mutation committed while the DATA artifact is
            // being built. A conservative snapshot taken just before row
            // extraction may retain an extra tombstone for one cycle, but can
            // never resurrect an obsolete physical owner.
            let extraction_tombstones = mgr.tombstone_set_arc();

            let (total_rows, extraction_snapshot, completed) = store.extract_for_seal_chunks(
                read_txn_id,
                per_table_cutoff,
                min_rows,
                chunk_size,
                |chunk| {
                    let runtime_segment_id = mgr.reserve_runtime_segment_id();
                    match self.publish_data_segment(&table_name, chunk, compress) {
                        Ok(published) => {
                            sealed_volumes.push((
                                published.volume,
                                published.path,
                                runtime_segment_id,
                            ));
                            true
                        }
                        Err(error) => {
                            stale_publication |= error.is_stale_source();
                            seal_error = Some(error.into_error());
                            false
                        }
                    }
                },
            );

            if (!completed || seal_error.is_some()) && sealed_volumes.is_empty() {
                let message = seal_error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "chunked extraction stopped".to_string());
                eprintln!(
                    "Warning: Failed to seal hot buffer for {}: {}",
                    table_name, message
                );
                seal_had_failure = true;
                continue;
            }
            if sealed_volumes.is_empty() {
                continue;
            }
            if !completed || seal_error.is_some() {
                // Earlier chunks may already be reachable through CONTROL and
                // cannot be rolled back. Install and reconcile those exact
                // rows; the failed suffix remains hot for the next cycle.
                seal_had_failure = true;
            }

            let sealed_row_count = sealed_volumes
                .iter()
                .map(|(volume, _, _)| volume.meta.row_count)
                .sum::<usize>();

            let sealed_segment_ids = sealed_volumes
                .iter()
                .map(|(_, _, segment_id)| *segment_id)
                .collect::<Vec<_>>();
            let sealed_output_bytes = sealed_volumes.iter().fold(0_u64, |total, (volume, _, _)| {
                total.saturating_add(frozen_volume_physical_bytes(volume))
            });
            seal_runtime.set_detail_with_reason(&table_name, &sealed_segment_ids, seal_reason);
            seal_runtime.add_input(sealed_row_count as u64, candidate_hot_bytes);
            seal_runtime.add_output(sealed_row_count as u64, sealed_output_bytes);

            let max_sealed_row_id = sealed_volumes
                .iter()
                .filter_map(|(vol, _, _)| vol.meta.row_ids.last())
                .max();

            // Seal critical section under exclusive fence: register cold
            // segments + remove hot rows + remove hot index entries.
            // DML operations hold the shared fence, so they cannot race
            // between cold constraint checks and hot publication.
            {
                // Match the read-path order (membership before segment seal)
                // and wait for any commit that changed an extracted hot head
                // to publish its registry outcome before assigning a skip
                // tombstone. A scalar tombstone sequence cannot represent an
                // in-flight transaction excluded by a Snapshot boundary.
                let membership_fence = store.membership_fence();
                let _membership_guard = membership_fence.write();
                let _seal_guard = mgr.acquire_seal_write();

                mgr.set_seal_overlap(total_rows);

                // Stamp seal_seq to reflect what data the volume contains:
                // - With cutoff: volume has rows committed before cutoff, so use cutoff
                // - Without cutoff: all committed rows, use current sequence
                // Compaction skips volumes with seal_seq >= min_snap_begin_seq.
                let current_seal_seq = per_table_cutoff
                    .map(|s| s as u64)
                    .unwrap_or_else(|| self.registry.get_current_sequence() as u64);
                let current_schema_version = self.schema_epoch.load(Ordering::Acquire);
                if current_schema_version != schema_version {
                    mgr.clear_seal_overlap();
                    eprintln!(
                        "Warning: Cannot install published DATA segments for {}: schema generation changed from {} to {}",
                        table_name, schema_version, current_schema_version
                    );
                    seal_had_failure = true;
                    continue;
                }
                let registrations = sealed_volumes
                    .iter()
                    .map(|(volume, _path, volume_id)| {
                        self.segment_registration(
                            &table_name,
                            Arc::clone(volume),
                            *volume_id,
                            current_seal_seq,
                            schema_version,
                        )
                    })
                    .collect();
                if let Err(error) =
                    mgr.register_segments_atomic(registrations, None, Some(schema_version))
                {
                    mgr.clear_seal_overlap();
                    eprintln!(
                        "Warning: Failed to install published DATA segments for {}: {}",
                        table_name, error
                    );
                    seal_had_failure = true;
                    continue;
                }

                let mut all_skipped_inner: Vec<i64> = Vec::new();
                let cold_populated_indexes = mgr.cold_populated_index_names();
                for (volume, _, _) in &sealed_volumes {
                    for start in (0..volume.meta.row_ids.len()).step_by(REMOVE_BATCH_SIZE) {
                        let end = (start + REMOVE_BATCH_SIZE).min(volume.meta.row_ids.len());
                        let batch: Vec<i64> = (start..end)
                            .map(|index| volume.meta.row_ids.at(index))
                            .collect();
                        let (removed, cleanup, skipped) = store
                            .remove_sealed_rows(&batch, &extraction_snapshot)
                            .map_err(|error| {
                                Error::internal(format!(
                                    "failed to remove sealed hot rows for table '{}': {}",
                                    table_name, error
                                ))
                            })?;
                        store.subtract_committed_row_count(removed);
                        if let Err(error) = store
                            .remove_sealed_index_entries_except(cleanup, &cold_populated_indexes)
                        {
                            mgr.clear_seal_overlap();
                            return Err(Error::internal(format!(
                                "failed to remove sealed hot index entries for table '{}': {}",
                                table_name, error
                            )));
                        }
                        all_skipped_inner.extend(skipped);
                    }
                }

                if !all_skipped_inner.is_empty() {
                    // Publish the maintenance tombstone at its own visibility
                    // point. Snapshots that began while the newer hot commit
                    // was still in flight must continue to see the sealed old
                    // row; later snapshots see the tombstone/new hot state.
                    let seal_seq = self.registry.reserve_visibility_sequence()? as u64;
                    mgr.add_tombstones(&all_skipped_inner, seal_seq);
                }

                if let Some(max_id) = max_sealed_row_id {
                    let current = store.get_auto_increment_counter();
                    if max_id > current {
                        store.set_auto_increment_counter(max_id);
                    }
                }

                mgr.clear_seal_overlap();

                // Resolve only tombstones that belonged to this exact seal
                // source snapshot. A transaction can commit a newer tombstone
                // while immutable DATA is built outside the fence; clearing by
                // row_id alone would lose that mutation and resurrect the old
                // physical owner.
                {
                    let skip_set: FxHashSet<i64> = all_skipped_inner.iter().copied().collect();
                    if !extraction_tombstones.is_empty() {
                        let mut sealed_ids: FxHashSet<i64> = FxHashSet::default();
                        for (vol, _, _) in &sealed_volumes {
                            for rid in vol.meta.row_ids.iter() {
                                if extraction_tombstones.contains_key(&rid)
                                    && !skip_set.contains(&rid)
                                {
                                    sealed_ids.insert(rid);
                                }
                            }
                        }
                        if !sealed_ids.is_empty() {
                            mgr.remove_tombstones_matching_snapshot_where(
                                &extraction_tombstones,
                                |row_id| sealed_ids.contains(&row_id),
                            );
                        }
                    }
                }

                // _seal_guard dropped here — DML unblocked
            }
        }

        if seal_had_failure {
            seal_runtime.failure();
        } else {
            seal_runtime.success();
        }

        Ok(SealOutcome { stale_publication })
    }
}
