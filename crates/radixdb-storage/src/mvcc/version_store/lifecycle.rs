use super::*;

impl VersionStore {
    // Cleanup Functions
    // =========================================================================

    /// Remove sealed rows from the hot version store (phase 1 of seal).
    /// Removes version data and arena slots but KEEPS hot index entries.
    /// The stale index entries act as a safety net: unique constraint checks
    /// still find them, preventing duplicate inserts during the seal window
    /// when the row has moved to cold but might not yet be visible to a
    /// cold check that took a snapshot before register_volume.
    ///
    /// Callers MUST also call `subtract_committed_row_count(n)` with the
    /// returned count to keep the committed row count accurate.
    ///
    /// `extraction_snapshot` is an O(1) CowBTree clone taken at the moment
    /// rows were extracted. For each row_id, the removal compares the
    /// current head version's `txn_id` against the extraction snapshot's
    /// `txn_id`. If they differ, a concurrent commit published a newer
    /// version after extraction — that row is skipped (stays in hot,
    /// sealed next cycle). This prevents discarding concurrent updates
    /// that the sealed volume doesn't contain.
    /// Returns `(removed_count, index_cleanup, skipped_row_ids)`.
    /// Skipped row_ids are rows that were modified after extraction — the
    /// caller must tombstone them so recovery and row_count are correct.
    pub fn remove_sealed_rows(
        &self,
        row_ids: &[i64],
        extraction_snapshot: &ExtractionSnapshot,
    ) -> Result<(usize, SealedIndexCleanup, Vec<i64>), Error> {
        let _mutation = self.mutation_guard()?;
        if row_ids.is_empty() {
            return Ok((0, SealedIndexCleanup::default(), Vec::new()));
        }

        // Snapshot version data BEFORE removal only when hot index cleanup
        // actually needs row values. Tables without secondary hot-only indexes
        // must not keep an extra CowBTree snapshot alive during seal cleanup.
        let needs_index_cleanup = self
            .indexes
            .read()
            .values()
            .any(|idx| idx.index_type() != radixdb_core::IndexType::Hnsw);
        let snapshot = if needs_index_cleanup {
            Some(self.versions.read().clone())
        } else {
            None
        };

        // Remove rows in small sub-batches to reduce write lock hold time.
        // Each sub-batch acquires versions.write() briefly, then releases it,
        // giving concurrent commits a chance to proceed between sub-batches.
        // Without this, a 50K-row seal batch holds the write lock for the
        // entire removal, blocking all commits on this table for ~100ms+.
        const SUB_BATCH_SIZE: usize = 2_000;
        let mut removed_ids: Vec<i64> = Vec::with_capacity(row_ids.len());
        let mut skipped_ids: Vec<i64> = Vec::new();
        let mut arena_indices_to_clear: Vec<usize> = Vec::new();
        let mut removed_hot_bytes = 0usize;

        for chunk in row_ids.chunks(SUB_BATCH_SIZE) {
            let uncommitted = self.uncommitted_writes.read();
            let mut versions = self.versions.write();
            for &row_id in chunk {
                if uncommitted.contains_key(row_id) {
                    skipped_ids.push(row_id);
                    continue;
                }
                if let Some(entry) = versions.get(row_id) {
                    // Compare current txn_id against extraction-time txn_id.
                    // If they differ, a concurrent commit changed this row
                    // after we extracted it — the sealed volume has stale data
                    // for this row. Keep the newer version in hot.
                    let extracted_txn_id = extraction_snapshot
                        .inner
                        .get(row_id)
                        .map(|e| e.version.txn_id)
                        .unwrap_or(0);
                    if entry.version.txn_id != extracted_txn_id {
                        skipped_ids.push(row_id);
                        continue;
                    }
                    if let Some(idx) = unpack_arena_idx(entry.arena_idx) {
                        arena_indices_to_clear.push(idx);
                    }
                    if entry.version.deleted_at_txn_id == 0 {
                        removed_hot_bytes = removed_hot_bytes
                            .saturating_add(estimate_row_hot_bytes(&entry.version.data));
                    }
                    versions.remove(row_id);
                    removed_ids.push(row_id);
                }
            }
            // Locks released here — concurrent commits can proceed
        }

        self.subtract_committed_hot_bytes(removed_hot_bytes);

        // Invalidate sealed arena slots so speculative probes don't return
        // stale data. This sets row_id=0 in the meta, making the probe fail.
        if !arena_indices_to_clear.is_empty() {
            self.arena.clear_batch(&arena_indices_to_clear);
        }

        let count = removed_ids.len();
        Ok((
            count,
            SealedIndexCleanup {
                removed_ids,
                snapshot,
            },
            skipped_ids,
        ))
    }

    /// Remove stale hot index entries for sealed rows (phase 2 of seal).
    /// Called while the table's seal fence is still held so INSERT cannot race
    /// between cold constraint checks and hot-index cleanup.
    pub fn remove_sealed_index_entries(&self, cleanup: SealedIndexCleanup) -> Result<(), Error> {
        self.remove_sealed_index_entries_except(cleanup, &FxHashSet::default())
    }

    /// Remove sealed entries from hot-only indexes while retaining entries in
    /// explicitly cold-populated runtime indexes. Those indexes were published
    /// only after a complete cold backfill and remain complete across later
    /// seals without requiring a synchronous sidecar rebuild.
    pub fn remove_sealed_index_entries_except(
        &self,
        cleanup: SealedIndexCleanup,
        preserve_indexes: &FxHashSet<SmartString>,
    ) -> Result<(), Error> {
        let _mutation = self.mutation_guard()?;
        if cleanup.removed_ids.is_empty() {
            return Ok(());
        }

        let Some(ref snap) = cleanup.snapshot else {
            self.release_empty_hot_storage(preserve_indexes);
            return Ok(());
        };

        let indexes = self.indexes.read();
        let mut hot_only_indexes: Vec<_> = indexes
            .values()
            .filter(|idx| {
                idx.index_type() != radixdb_core::IndexType::Hnsw
                    && !preserve_indexes.contains(idx.name())
            })
            .cloned()
            .collect();
        drop(indexes);
        hot_only_indexes.sort_by(|left, right| left.name().cmp(right.name()));

        if hot_only_indexes.is_empty() {
            self.release_empty_hot_storage(preserve_indexes);
            return Ok(());
        }

        let mut index_batches = Vec::with_capacity(hot_only_indexes.len());
        for index in hot_only_indexes {
            let mut owned_entries: Vec<(i64, Vec<radixdb_core::Value>)> =
                Vec::with_capacity(cleanup.removed_ids.len());

            for &row_id in &cleanup.removed_ids {
                let Some(entry) = snap.get(row_id) else {
                    continue;
                };
                if let Some(values) = index_values_for_row(index.as_ref(), &entry.version.data)? {
                    owned_entries.push((row_id, values));
                }
            }

            index_batches.push((index, owned_entries));
        }

        let mut completed = Vec::with_capacity(index_batches.len());
        for (batch_index, (index, owned_entries)) in index_batches.iter().enumerate() {
            if owned_entries.is_empty() {
                completed.push(batch_index);
                continue;
            }
            let borrowed_entries: Vec<(i64, &[radixdb_core::Value])> = owned_entries
                .iter()
                .map(|(row_id, values)| (*row_id, values.as_slice()))
                .collect();
            if let Err(error) = index.remove_batch_slice(&borrowed_entries) {
                let mut rollback_failures = Vec::new();
                for &completed_index in completed.iter().rev() {
                    let (completed_index, completed_entries) = &index_batches[completed_index];
                    if completed_entries.is_empty() {
                        continue;
                    }
                    let restore_entries: Vec<(i64, &[Value])> = completed_entries
                        .iter()
                        .map(|(row_id, values)| (*row_id, values.as_slice()))
                        .collect();
                    if let Err(rollback_error) = completed_index.add_batch_slice(&restore_entries) {
                        rollback_failures.push(format!(
                            "restore sealed entries in '{}': {}",
                            completed_index.name(),
                            rollback_error
                        ));
                    }
                }
                if rollback_failures.is_empty() {
                    return Err(error);
                }
                return Err(Error::internal(format!(
                    "sealed index cleanup failed: {}; rollback also failed: {}",
                    error,
                    rollback_failures.join("; ")
                )));
            }
            completed.push(batch_index);
        }

        self.release_empty_hot_storage(preserve_indexes);
        Ok(())
    }

    /// Release historical peak capacities once immutable artifacts own the
    /// complete table. Partial seals retain holes for reuse; a fully empty hot
    /// layer must not scale with the table's historical row count.
    fn release_empty_hot_storage(&self, preserve_indexes: &FxHashSet<SmartString>) {
        if self.row_count() != 0 {
            return;
        }

        self.arena.clear_all();

        for index in self.indexes.read().values() {
            if index.index_type() != radixdb_core::IndexType::Hnsw
                && !preserve_indexes.contains(index.name())
            {
                index.clear();
            }
        }

        let mut uncommitted_writes = self.uncommitted_writes.write();
        if uncommitted_writes.is_empty() {
            *uncommitted_writes = new_i64_map();
        }
        drop(uncommitted_writes);

        let mut unique_key_claims = self.unique_key_claims.lock();
        if unique_key_claims.is_empty() {
            *unique_key_claims = Default::default();
        }
    }

    /// Cleanup deleted rows that are older than the retention period.
    ///
    /// This removes soft-deleted rows that are no longer visible to any active
    /// transaction and are older than the specified retention period.
    /// Also clears the arena slots to release memory.
    pub fn cleanup_deleted_rows(&self, retention_period: std::time::Duration) -> i32 {
        self.cleanup_deleted_rows_with_between_passes(retention_period, || {})
    }

    pub(super) fn cleanup_deleted_rows_with_between_passes<F>(
        &self,
        retention_period: std::time::Duration,
        between_passes: F,
    ) -> i32
    where
        F: FnOnce(),
    {
        if self.closed.load(Ordering::Acquire) {
            return 0;
        }

        let now = get_fast_timestamp();
        let cutoff_time = retention_cutoff(now, retention_period);

        let mut rows_to_delete = Vec::new();

        // Clone CowBTree once, reuse for index cleanup (O(1) Arc clone)
        let versions = self.versions.read().clone();

        // First pass: identify deleted rows older than retention period
        for (&row_id, chain) in versions.iter() {
            let version = &chain.version;
            // Only process rows that are actually deleted and old enough
            if version.is_deleted() && version.create_time < cutoff_time {
                // Check if safe to remove (no active transaction can see it)
                if self.can_safely_remove(chain) {
                    rows_to_delete.push((
                        row_id,
                        version.txn_id,
                        version.deleted_at_txn_id,
                        version.create_time,
                        chain.arena_idx,
                    ));
                }
            }
        }

        if rows_to_delete.is_empty() {
            return 0;
        }

        between_passes();

        // Second pass: acquire write lock and re-validate before removing.
        // Between the snapshot (first pass) and now, a concurrent transaction may have
        // committed a new version for a row_id we marked for deletion (e.g., re-INSERT
        // with the same PK value). We must re-check that each row is still deleted
        // before removing it to prevent data loss.
        // Arena indices are read directly from the live entry under the write lock
        // to avoid index misalignment with the rows_to_delete vector.
        let mut actually_deleted = Vec::with_capacity(rows_to_delete.len());
        let mut actual_arena_indices = Vec::with_capacity(rows_to_delete.len());
        {
            let mut versions = self.versions.write();
            for &(row_id, txn_id, deleted_at_txn_id, create_time, arena_idx) in &rows_to_delete {
                // Re-check the exact immutable candidate, not merely the row ID
                // and deleted flag. A newer tombstone for the same PK is a
                // distinct committed state and must survive this cleanup pass.
                if let Some(entry) = versions.get(row_id) {
                    let version = &entry.version;
                    if version.is_deleted()
                        && version.txn_id == txn_id
                        && version.deleted_at_txn_id == deleted_at_txn_id
                        && version.create_time == create_time
                        && entry.arena_idx == arena_idx
                        && version.create_time < cutoff_time
                        && self.can_safely_remove(entry)
                    {
                        if let Some(idx) = unpack_arena_idx(entry.arena_idx) {
                            actual_arena_indices.push(idx);
                        }
                        versions.remove(row_id);
                        actually_deleted.push(row_id);
                    }
                }
            }
        }

        if actually_deleted.is_empty() {
            return 0;
        }

        // Index entries were removed by the DELETE commit/recovery transition.
        // Garbage collection only performs optional index-specific maintenance;
        // replaying removal here could race a reinserted row with the same ID.
        {
            let indexes = self.indexes.read();

            for index in indexes.values() {
                // Let index-specific maintenance run (e.g., HNSW graph compaction)
                if let Err(error) = index.cleanup() {
                    eprintln!(
                        "Warning: post-delete maintenance failed for index '{}': {}",
                        index.name(),
                        error
                    );
                }
            }
        }

        // Clear arena slots only for rows we actually removed
        self.arena.clear_batch(&actual_arena_indices);

        actually_deleted.len() as i32
    }

    /// Check if a version can be safely removed (not visible to any active transaction)
    pub(super) fn can_safely_remove(&self, chain: &VersionChainEntry) -> bool {
        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return true, // No checker, assume safe
        };

        // Get all active transaction IDs
        let active_txns = checker.get_active_transaction_ids();

        // If no active transactions, safe to remove
        if active_txns.is_empty() {
            return true;
        }

        // A snapshot older than the DELETE cannot see the tombstone itself but
        // may still need an older live member of this same chain. Prove safety
        // over the complete row history rather than tombstone visibility.
        for txn_id in active_txns {
            let mut current = Some(chain);
            while let Some(entry) = current {
                if !entry.version.is_deleted() && checker.is_visible(entry.version.txn_id, txn_id) {
                    return false;
                }
                current = entry.prev.as_deref();
            }
        }

        true
    }

    /// Cleanup old previous versions that are no longer needed
    ///
    /// This prunes old version chains, keeping only versions that are:
    /// 1. Needed by active transactions
    /// 2. Within the retention period (for AS OF TIMESTAMP queries)
    pub fn cleanup_old_previous_versions(&self) -> i32 {
        // Default 24-hour retention for background cleanup
        self.cleanup_old_previous_versions_with_retention(std::time::Duration::from_secs(
            24 * 60 * 60,
        ))
    }

    pub fn cleanup_old_previous_versions_with_retention(
        &self,
        retention_period: std::time::Duration,
    ) -> i32 {
        if self.closed.load(Ordering::Acquire) {
            return 0;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return 0, // Need visibility checker for cleanup
        };

        let now = get_fast_timestamp();
        let retention_cutoff = retention_cutoff(now, retention_period);

        // Get active transaction IDs
        let active_txns = checker.get_active_transaction_ids();

        // First pass (read lock): identify row_ids that MAY need pruning
        let mut candidate_row_ids: Vec<i64> = Vec::new();
        {
            let versions = self.versions.read().clone();
            for (&row_id, chain_entry) in versions.iter() {
                // Quick check: does this entry have any prev versions?
                if chain_entry.prev.is_none() {
                    continue;
                }
                candidate_row_ids.push(row_id);
            }
        }

        if candidate_row_ids.is_empty() {
            return 0;
        }

        // Second pass (write lock): re-read each entry from live data and prune in-place.
        // This prevents the race where a concurrent commit adds a new HEAD between
        // the snapshot read and the write — we always work on the current live entry.
        let mut cleaned = 0;
        let mut versions = self.versions.write();

        for row_id in candidate_row_ids {
            let Some(chain_entry) = versions.get(row_id) else {
                continue; // Row was removed between passes
            };

            // Collect previous versions from the LIVE entry
            let mut prev_versions: Vec<Arc<VersionChainEntry>> = Vec::new();
            let mut current = chain_entry.prev.as_ref();
            while let Some(prev_entry) = current {
                prev_versions.push(prev_entry.clone());
                current = prev_entry.prev.as_ref();
            }

            if prev_versions.is_empty() {
                continue;
            }

            // Check each previous version independently — do NOT assume monotonic
            // visibility. With rapid updates, a newer prev version may be invisible
            // to an active txn while an older one IS visible (e.g., HEAD seq=120,
            // prev_0 seq=110, prev_1 seq=80, active txn snapshot at seq=100 needs prev_1).
            let mut keep_count = 0;
            for (i, prev_entry) in prev_versions.iter().enumerate() {
                let mut keep = false;

                // Rule 1: Keep if needed by any active transaction
                for &txn_id in &active_txns {
                    if checker.is_visible(prev_entry.version.txn_id, txn_id) {
                        keep = true;
                        break;
                    }
                }

                // Rule 2: Keep if within retention period
                if !keep && prev_entry.version.create_time >= retention_cutoff {
                    keep = true;
                }

                if keep {
                    // Keep this version and all newer ones (indices 0..=i)
                    keep_count = i + 1;
                }
            }

            // If we need to prune some versions, modify the live entry
            if keep_count < prev_versions.len() {
                let to_remove = prev_versions.len() - keep_count;
                cleaned += to_remove as i32;

                // Clone the LIVE entry (not stale snapshot) and modify
                let mut modified_entry = chain_entry.clone();

                if keep_count == 0 {
                    modified_entry.prev = None;
                } else {
                    // Rebuild chain with only kept versions
                    let kept_versions: Vec<_> =
                        prev_versions.into_iter().take(keep_count).collect();

                    // Build chain from oldest to newest (reversed)
                    let mut new_prev: Option<Arc<VersionChainEntry>> = None;
                    for entry in kept_versions.into_iter().rev() {
                        let mut cloned = (*entry).clone();
                        cloned.prev = new_prev;
                        new_prev = Some(Arc::new(cloned));
                    }
                    modified_entry.prev = new_prev;
                }

                versions.insert(row_id, modified_entry);
            }
        }

        cleaned
    }

    /// Iterate over all committed (non-deleted) versions for snapshot creation
    ///
    /// This method iterates over all rows that are visible to a snapshot transaction
    /// (i.e., all committed, non-deleted rows). The callback receives the row_id and
    /// a reference to the RowVersion. Return false from the callback to stop iteration.
    ///
    /// This is designed for creating point-in-time snapshots to disk.
    pub fn for_each_committed_version<F>(&self, callback: F)
    where
        F: FnMut(i64, &RowVersion) -> bool,
    {
        // Delegate to the cutoff version with no cutoff (0 means no filtering)
        self.for_each_committed_version_with_cutoff(callback, 0);
    }

    /// Iterate over committed versions with a commit sequence cutoff for consistent snapshots
    ///
    /// This is the same as `for_each_committed_version` but only includes transactions
    /// that were committed before the given `commit_seq_cutoff`. This ensures consistent
    /// point-in-time snapshots even when new transactions commit during iteration.
    ///
    /// # Arguments
    /// * `callback` - Called for each visible, non-deleted version
    /// * `commit_seq_cutoff` - Only include transactions with commit_seq < cutoff (0 = no filter)
    pub fn for_each_committed_version_with_cutoff<F>(&self, mut callback: F, commit_seq_cutoff: i64)
    where
        F: FnMut(i64, &RowVersion) -> bool,
    {
        if self.closed.load(Ordering::Acquire) {
            return;
        }

        // Get visibility checker for determining committed status
        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return,
        };

        // Use a very high txn_id to see all committed rows
        let snapshot_txn_id = i64::MAX;
        let use_cutoff = commit_seq_cutoff > 0;

        // Iterate all versions
        let versions = self.versions.read().clone();
        for (&row_id, chain_entry) in versions.iter() {
            // Walk the version chain to find the visible version
            let mut current: Option<&VersionChainEntry> = Some(chain_entry);

            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                // Check if this version is visible
                if checker.is_visible(version_txn_id, snapshot_txn_id) {
                    // When cutoff is specified, only include versions from transactions
                    // that committed before the cutoff to ensure snapshot consistency.
                    // CRITICAL: Do NOT walk the prev chain. The extraction snapshot
                    // captures the HEAD txn_id, and remove_sealed_rows compares against
                    // it. If we extract an older version but the HEAD txn_id matches,
                    // the HEAD is removed from hot, permanently losing the newer version.
                    // The row stays entirely in hot where MVCC handles visibility.
                    if use_cutoff && !checker.is_committed_before(version_txn_id, commit_seq_cutoff)
                    {
                        break;
                    }

                    // Skip if deleted and deletion is visible (and within cutoff if specified)
                    if deleted_at_txn_id != 0
                        && checker.is_visible(deleted_at_txn_id, snapshot_txn_id)
                        && (!use_cutoff
                            || checker.is_committed_before(deleted_at_txn_id, commit_seq_cutoff))
                    {
                        break; // Row is deleted, skip
                    }

                    // Found visible, non-deleted version
                    if !callback(row_id, &e.version) {
                        return; // Callback wants to stop
                    }
                    break;
                }

                // Try older version
                current = e.prev.as_ref().map(|arc| arc.as_ref());
            }
        }
    }

    /// Get the count of committed (non-deleted) versions for statistics
    pub fn count_committed_versions(&self) -> usize {
        if self.closed.load(Ordering::Acquire) {
            return 0;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return 0,
        };

        let snapshot_txn_id = i64::MAX;
        let mut count = 0;

        let versions = self.versions.read().clone();
        for (_, chain_entry) in versions.iter() {
            let mut current: Option<&VersionChainEntry> = Some(chain_entry);

            while let Some(e) = current {
                if checker.is_visible(e.version.txn_id, snapshot_txn_id) {
                    if e.version.deleted_at_txn_id == 0
                        || !checker.is_visible(e.version.deleted_at_txn_id, snapshot_txn_id)
                    {
                        count += 1;
                    }
                    break;
                }
                current = e.prev.as_ref().map(|arc| arc.as_ref());
            }
        }

        count
    }

    /// Get the transaction ID of the latest committed version for a row.
    ///
    /// This retrieves the "head" of the version chain, effectively checking
    /// the most recently committed change. This is critical for conflict
    /// detection (First-Committer-Wins).
    ///
    /// Returns:
    /// - Some(txn_id) if the row exists
    /// - None if the row does not exist
    pub fn get_latest_version_id(&self, row_id: i64) -> Option<i64> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }

        let versions = self.versions.read();
        versions.get(row_id).map(|entry| entry.version.txn_id)
    }

    /// Compute grouped aggregates directly from arena storage.
    ///
    /// This method performs GROUP BY aggregation at the storage level without
    /// materializing Row objects. It uses Arc::clone for group keys (O(1))
    /// instead of Value::clone (deep copy), significantly reducing allocations.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction ID for visibility checks
    /// * `group_by_indices` - Column indices to group by
    /// * `aggregates` - List of (operation, column_index) pairs
    ///
    /// # Returns
    /// Vector of grouped aggregate results, or empty if optimization not possible
    pub fn compute_grouped_aggregates(
        &self,
        txn_id: i64,
        group_by_indices: &[usize],
        aggregates: &[(AggregateOp, usize)],
    ) -> Option<Vec<GroupedAggregateResult>> {
        if self.closed.load(Ordering::Acquire) {
            return Some(Vec::new());
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Some(Vec::new()),
        };

        // Guard: arena-only path requires ReadCommitted and no uncommitted writes.
        // Under snapshot isolation, HEAD versions in the arena may have been committed
        // after the viewer's snapshot. The correct behavior requires walking version
        // chains to find older visible versions, which the arena path doesn't support.
        // Return None to let the caller fall back to regular GROUP BY aggregation.
        let uncommitted_empty = self.uncommitted_writes.read().is_empty();
        if !uncommitted_empty || checker.needs_snapshot_isolation(txn_id) {
            return None;
        }

        // Accumulator for each group: (count, sum, min, max) per aggregate
        #[derive(Clone)]
        struct Accum {
            count: i64,
            int_sum: i128,
            float_sum: f64,
            has_float: bool,
            overflowed: bool,
            min: Option<Value>,
            max: Option<Value>,
        }

        impl Default for Accum {
            fn default() -> Self {
                Self {
                    count: 0,
                    int_sum: 0,
                    float_sum: 0.0,
                    has_float: false,
                    overflowed: false,
                    min: None,
                    max: None,
                }
            }
        }

        // Pre-acquire arena lock ONCE
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();
        let arena_meta = arena_guard.meta();

        // Helper to update accumulators
        #[inline(always)]
        fn update_accums(
            accums: &mut [Accum],
            aggregates: &[(AggregateOp, usize)],
            row_data: &[Value],
        ) {
            for (agg_idx, (op, col_idx)) in aggregates.iter().enumerate() {
                let accum = &mut accums[agg_idx];

                match op {
                    AggregateOp::CountStar => {
                        accum.count += 1;
                    }
                    AggregateOp::Count => {
                        if *col_idx < row_data.len() && !row_data[*col_idx].is_null() {
                            accum.count += 1;
                        }
                    }
                    AggregateOp::Sum | AggregateOp::Avg => {
                        if *col_idx < row_data.len() {
                            match &row_data[*col_idx] {
                                Value::Integer(i) => {
                                    match accum.int_sum.checked_add(*i as i128) {
                                        Some(sum) => accum.int_sum = sum,
                                        None => accum.overflowed = true,
                                    }
                                    accum.count += 1;
                                }
                                Value::Float(f) => {
                                    accum.float_sum += *f;
                                    accum.has_float = true;
                                    accum.count += 1;
                                }
                                _ => {}
                            }
                        }
                    }
                    AggregateOp::Min => {
                        if *col_idx < row_data.len() {
                            let val = &row_data[*col_idx];
                            if !val.is_null() {
                                match &accum.min {
                                    None => accum.min = Some(val.clone()),
                                    Some(current) => {
                                        if val < current {
                                            accum.min = Some(val.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    AggregateOp::Max => {
                        if *col_idx < row_data.len() {
                            let val = &row_data[*col_idx];
                            if !val.is_null() {
                                match &accum.max {
                                    None => accum.max = Some(val.clone()),
                                    Some(current) => {
                                        if val > current {
                                            accum.max = Some(val.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Helper to compute final aggregate values
        fn compute_aggregate_values(
            aggregates: &[(AggregateOp, usize)],
            accums: &[Accum],
        ) -> Option<Vec<Value>> {
            aggregates
                .iter()
                .zip(accums.iter())
                .map(|((op, _), accum)| {
                    Some(match op {
                        AggregateOp::Count | AggregateOp::CountStar => Value::Integer(accum.count),
                        AggregateOp::Sum => {
                            if accum.overflowed {
                                return None;
                            } else if accum.count == 0 {
                                Value::Null(DataType::Float)
                            } else if accum.has_float {
                                Value::Float(accum.int_sum as f64 + accum.float_sum)
                            } else {
                                exact_integer_sum_value(accum.int_sum)?
                            }
                        }
                        AggregateOp::Avg => {
                            if accum.count > 0 {
                                Value::Float(
                                    (accum.int_sum as f64 + accum.float_sum) / accum.count as f64,
                                )
                            } else {
                                Value::Null(DataType::Float)
                            }
                        }
                        AggregateOp::Min => {
                            accum.min.clone().unwrap_or(Value::Null(DataType::Null))
                        }
                        AggregateOp::Max => {
                            accum.max.clone().unwrap_or(Value::Null(DataType::Null))
                        }
                    })
                })
                .collect()
        }

        fn exact_integer_sum_value(sum: i128) -> Option<Value> {
            if let Ok(integer) = i64::try_from(sum) {
                return Some(Value::Integer(integer));
            }
            let precision = u8::try_from(sum.to_string().trim_start_matches('-').len()).ok()?;
            Value::try_decimal(sum, precision, 0).ok()
        }

        // FAST PATH: Single-column GROUP BY with homogeneous Integer or Boolean keys.
        // Uses I64Map directly for ~3x faster hashing and comparison. If a value
        // from another identity domain appears, migrate the accumulated groups
        // once to the canonical GroupKey map. Float keys always use GroupKey so
        // signed zero and canonical NaN semantics remain consistent with Value.
        if group_by_indices.len() == 1 {
            let col_idx = group_by_indices[0];

            // Track key type: 0=Integer, 2=Boolean, 3=Canonical/Mixed.
            let mut key_type: u8 = 255; // uninitialized
            let mut int_groups: I64Map<Vec<Accum>> = I64Map::new();
            // Separate NULL accumulator - avoids creating GroupKey for NULL
            let mut null_accums: Option<Vec<Accum>> = None;
            // Only used for String/other types that can't be mapped to i64
            let mut other_groups: GroupKeyMap<Vec<Accum>> = GroupKeyMap::default();

            for (idx, meta) in arena_meta.iter().enumerate() {
                // Visibility check (standard pattern: check creation, then deletion)
                if meta.txn_id == 0 || !checker.is_visible(meta.txn_id, txn_id) {
                    continue;
                }
                if meta.deleted_at_txn_id != 0 && checker.is_visible(meta.deleted_at_txn_id, txn_id)
                {
                    continue;
                }

                let row_data = match arena_data.get(idx) {
                    Some(data) => data,
                    None => continue,
                };

                let row_slice = row_data.as_ref();
                let val = if col_idx < row_slice.len() {
                    &row_slice[col_idx]
                } else {
                    // NULL - track separately without GroupKey allocation
                    let accums =
                        null_accums.get_or_insert_with(|| vec![Accum::default(); aggregates.len()]);
                    update_accums(accums, aggregates, row_slice);
                    continue;
                };

                // Keep the compact map only while the non-NULL keys are
                // homogeneous Integer or Boolean values. A later value from a
                // different domain triggers a one-time migration; otherwise
                // canonical-equal Integer/Float or signed-zero keys could be
                // split between two maps and never reconciled.
                let i64_key = match val {
                    Value::Integer(i) => {
                        if key_type == 255 {
                            key_type = 0;
                        }
                        if key_type == 0 && *i != i64::MIN {
                            Some(*i)
                        } else {
                            None
                        }
                    }
                    Value::Float(_) => None,
                    Value::Boolean(b) => {
                        if key_type == 255 {
                            key_type = 2;
                        }
                        if key_type == 2 {
                            Some(if *b { 1 } else { 0 })
                        } else {
                            None // Type mismatch → other_groups
                        }
                    }
                    Value::Null(_) => {
                        // NULL value in the column - track separately
                        let accums = null_accums
                            .get_or_insert_with(|| vec![Accum::default(); aggregates.len()]);
                        update_accums(accums, aggregates, row_slice);
                        continue;
                    }
                    _ => None,
                };

                if let Some(key) = i64_key {
                    let accums = int_groups
                        .entry(key)
                        .or_insert_with(|| vec![Accum::default(); aggregates.len()]);
                    update_accums(accums, aggregates, row_slice);
                } else {
                    if key_type != 3 {
                        let previous_key_type = key_type;
                        for (key, accums) in int_groups.drain() {
                            let group_value = match previous_key_type {
                                0 => Value::Integer(key),
                                2 => Value::Boolean(key != 0),
                                _ => unreachable!("only primitive fast paths can be migrated"),
                            };
                            other_groups
                                .insert(GroupKey::Single(CompactArc::new(group_value)), accums);
                        }
                        key_type = 3;
                    }
                    let accums = other_groups
                        .entry(GroupKey::Single(CompactArc::new(val.clone())))
                        .or_insert_with(|| vec![Accum::default(); aggregates.len()]);
                    update_accums(accums, aggregates, row_slice);
                }
            }

            // Convert to results
            let has_null = null_accums.is_some();
            let mut results: Vec<GroupedAggregateResult> = Vec::with_capacity(
                int_groups.len() + other_groups.len() + if has_null { 1 } else { 0 },
            );

            // Convert the remaining homogeneous primitive groups.
            for (key, accums) in int_groups.iter() {
                let group_value = match key_type {
                    0 => Value::Integer(key),
                    2 => Value::Boolean(key != 0),
                    _ => unreachable!("canonical groups must not remain in I64Map"),
                };
                results.push(GroupedAggregateResult {
                    group_values: vec![group_value],
                    aggregate_values: compute_aggregate_values(aggregates, accums)?,
                });
            }

            // Add NULL group if present
            if let Some(accums) = null_accums {
                results.push(GroupedAggregateResult {
                    group_values: vec![Value::Null(DataType::Null)],
                    aggregate_values: compute_aggregate_values(aggregates, &accums)?,
                });
            }

            // Convert other_groups (strings, etc.)
            for (group_key, accums) in other_groups {
                let group_values = match group_key {
                    GroupKey::Single(v) => vec![(*v).clone()],
                    GroupKey::Multi(vs) => vs.iter().map(|v| (**v).clone()).collect(),
                };
                results.push(GroupedAggregateResult {
                    group_values,
                    aggregate_values: compute_aggregate_values(aggregates, &accums)?,
                });
            }

            return Some(results);
        }

        // SLOW PATH: Multi-column GROUP BY (currently not used from try_storage_aggregation)
        let mut groups: GroupKeyMap<Vec<Accum>> = GroupKeyMap::default();

        for (idx, meta) in arena_meta.iter().enumerate() {
            // Visibility check (standard pattern: check creation, then deletion)
            if meta.txn_id == 0 || !checker.is_visible(meta.txn_id, txn_id) {
                continue;
            }
            if meta.deleted_at_txn_id != 0 && checker.is_visible(meta.deleted_at_txn_id, txn_id) {
                continue;
            }

            let row_data = match arena_data.get(idx) {
                Some(data) => data,
                None => continue,
            };
            let row_slice = row_data.as_ref();

            let key_values: Vec<CompactArc<Value>> = group_by_indices
                .iter()
                .map(|&col_idx| {
                    if col_idx < row_slice.len() {
                        CompactArc::new(row_slice[col_idx].clone())
                    } else {
                        CompactArc::new(Value::Null(DataType::Null))
                    }
                })
                .collect();
            let group_key = GroupKey::Multi(key_values);

            let accums = groups
                .entry(group_key)
                .or_insert_with(|| vec![Accum::default(); aggregates.len()]);
            update_accums(accums, aggregates, row_slice);
        }

        // Convert to results
        let mut results: Vec<GroupedAggregateResult> = Vec::with_capacity(groups.len());

        for (group_key, accums) in groups {
            let group_values = match group_key {
                GroupKey::Single(v) => vec![(*v).clone()],
                GroupKey::Multi(vs) => vs.iter().map(|v| (**v).clone()).collect(),
            };
            results.push(GroupedAggregateResult {
                group_values,
                aggregate_values: compute_aggregate_values(aggregates, &accums)?,
            });
        }

        Some(results)
    }
}
