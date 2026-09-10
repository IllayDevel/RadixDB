use super::*;

impl VersionStore {
    /// Returns the count of rows
    pub fn row_count(&self) -> usize {
        self.versions.read().len()
    }
    /// Claims a row for update with bounded waiting and deadlock prevention.
    ///
    /// The shared TransactionRegistry records wait-for edges across every
    /// table. A request that would close a cycle fails with a retryable
    /// serialization conflict; ordinary same-row writers queue regardless of
    /// transaction start order.
    pub fn try_claim_row(&self, row_id: i64, txn_id: i64) -> Result<(), Error> {
        use radixdb_core::i64_map::Entry;

        let mut wait_budget = ClaimOwnerWaitBudget::new();
        let mut wait_guard = self.claim_wait_mutex.lock();

        loop {
            let existing_owner = {
                let mut map = self.uncommitted_writes.write();
                match map.entry(row_id) {
                    Entry::Occupied(e) => {
                        let existing_txn = *e.get();
                        if existing_txn == txn_id {
                            return Ok(());
                        }
                        existing_txn
                    }
                    Entry::Vacant(e) => {
                        e.insert(txn_id);
                        return Ok(());
                    }
                }
            };

            let wait_registered = self
                .visibility_checker
                .as_ref()
                .is_none_or(|checker| checker.register_row_wait(txn_id, existing_owner));
            if !wait_registered {
                return Err(Error::TransactionSerializationConflict { row_id });
            }

            let Some(remaining) = wait_budget.remaining(existing_owner) else {
                if let Some(checker) = &self.visibility_checker {
                    checker.clear_row_wait(txn_id);
                }
                return Err(Error::RowLockTimeout {
                    row_id,
                    timeout_ms: ROW_CLAIM_WAIT_TIMEOUT.as_millis() as u64,
                });
            };
            self.claim_changed.wait_for(&mut wait_guard, remaining);
            if let Some(checker) = &self.visibility_checker {
                checker.clear_row_wait(txn_id);
            }
        }
    }

    /// Releases a row claim
    pub fn release_row_claim(&self, row_id: i64, txn_id: i64) {
        let _wait_guard = self.claim_wait_mutex.lock();
        let mut map = self.uncommitted_writes.write();
        let mut released = false;
        if let Some(&v) = map.get(row_id) {
            if v == txn_id {
                map.remove(row_id);
                released = true;
            }
        }
        drop(map);
        if released {
            self.claim_changed.notify_all();
        }
    }

    /// Releases multiple row claims in batch
    /// OPTIMIZATION: Single lock acquisition for all removals
    #[inline]
    pub fn release_row_claims_batch(&self, row_ids: &[i64], txn_id: i64) {
        let _wait_guard = self.claim_wait_mutex.lock();
        let mut map = self.uncommitted_writes.write();
        let mut released = false;
        for &row_id in row_ids {
            if let Some(&v) = map.get(row_id) {
                if v == txn_id {
                    map.remove(row_id);
                    released = true;
                }
            }
        }
        drop(map);
        if released {
            self.claim_changed.notify_all();
        }
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        let indexes = self.indexes.read();
        indexes.contains_key(index_name)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.read();
        indexes
            .iter()
            .filter(|(_, idx)| idx.index_type() != radixdb_core::IndexType::PrimaryKey)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Check if there are any indexes
    #[inline]
    pub fn has_indexes(&self) -> bool {
        let indexes = self.indexes.read();
        !indexes.is_empty()
    }

    /// Iterate over all indexes, calling the provided function for each
    /// OPTIMIZATION: Avoids collecting index names and allows early exit on error
    pub fn for_each_index<F>(&self, mut f: F) -> radixdb_core::Result<()>
    where
        F: FnMut(&Arc<dyn Index>) -> radixdb_core::Result<()>,
    {
        let indexes = self.indexes.read();
        for index in indexes.values() {
            f(index)?;
        }
        Ok(())
    }

    /// Iterate over unique indexes only, calling the provided function for each
    /// OPTIMIZATION: Avoids collecting index names and allows early exit on error
    pub fn for_each_unique_index<F>(&self, mut f: F) -> radixdb_core::Result<()>
    where
        F: FnMut(&str, &Arc<dyn Index>) -> radixdb_core::Result<()>,
    {
        let indexes = self.indexes.read();
        for (name, index) in indexes.iter() {
            if index.is_unique() {
                f(name, index)?;
            }
        }
        Ok(())
    }

    /// Validate and atomically publish one index authority.
    pub fn add_index(&self, name: String, index: Arc<dyn Index>) -> Result<(), Error> {
        self.add_index_for_schema(name, index, &self.schema())
    }

    /// Validate and atomically publish an index against the schema visible to
    /// the owning transaction. Transactional ALTER may build the index before
    /// its schema overlay becomes the shared VersionStore schema.
    pub fn add_index_for_schema(
        &self,
        name: String,
        index: Arc<dyn Index>,
        schema: &Schema,
    ) -> Result<(), Error> {
        if name != index.name() {
            return Err(Error::invalid_argument(format!(
                "index registry name '{}' does not match index identity '{}'",
                name,
                index.name()
            )));
        }
        if !index.table_name().eq_ignore_ascii_case(&schema.table_name) {
            return Err(Error::invalid_argument(format!(
                "index '{}' belongs to table '{}', expected '{}'",
                name,
                index.table_name(),
                schema.table_name
            )));
        }
        let column_names = index.column_names();
        let column_ids = index.column_ids();
        let data_types = index.data_types();
        if column_names.is_empty()
            || column_names.len() != column_ids.len()
            || column_names.len() != data_types.len()
        {
            return Err(Error::invalid_argument(format!(
                "index '{}' metadata arity mismatch: names={}, ids={}, types={}",
                name,
                column_names.len(),
                column_ids.len(),
                data_types.len()
            )));
        }
        for ((column_name, column_id), data_type) in
            column_names.iter().zip(column_ids).zip(data_types)
        {
            let Some(column) = schema.columns.iter().find(|column| {
                column.id as i32 == *column_id
                    && column.name.eq_ignore_ascii_case(column_name.as_str())
            }) else {
                return Err(Error::invalid_argument(format!(
                    "index '{}' column '{}'/{} does not exist in table '{}'",
                    name, column_name, column_id, schema.table_name
                )));
            };
            if column.data_type != *data_type {
                return Err(Error::invalid_argument(format!(
                    "index '{}' column '{}' type {:?} does not match schema {:?}",
                    name, column_name, data_type, column.data_type
                )));
            }
        }

        let mut indexes = self.indexes.write();
        if let Some(existing) = indexes.get(&name) {
            if Arc::ptr_eq(existing, &index) {
                return Ok(());
            }
            return Err(Error::IndexAlreadyExists(name));
        }
        indexes.insert(name, index);
        Ok(())
    }

    /// Remove an index
    pub fn remove_index(&self, name: &str) -> Option<Arc<dyn Index>> {
        let mut indexes = self.indexes.write();
        indexes.remove(name)
    }

    /// Rename an index without rebuilding its data.
    pub fn rename_index(&self, old_name: &str, new_name: &str) -> radixdb_core::Result<()> {
        let mut indexes = self.indexes.write();
        if indexes.contains_key(new_name) {
            return Err(radixdb_core::Error::internal(format!(
                "index already exists: {}",
                new_name
            )));
        }
        let Some(index) = indexes.remove(old_name) else {
            return Err(radixdb_core::Error::IndexNotFound(old_name.to_string()));
        };
        let renamed = Arc::new(crate::index::RenamedIndex::new(new_name.to_string(), index));
        indexes.insert(new_name.to_string(), renamed);
        Ok(())
    }

    /// Publish index metadata after removing one schema column. Index payloads
    /// are keyed by values and remain valid; only ordinals after the removed
    /// slot shift. Partial predicates are admitted only when neither their key
    /// nor any compiled referenced column crosses the shifted boundary.
    pub fn remap_indexes_after_column_drop(
        &self,
        dropped_column: usize,
    ) -> radixdb_core::Result<()> {
        let schema = self.schema();
        let mut indexes = self.indexes.write();
        let mut replacements = Vec::new();

        for (map_name, index) in indexes.iter() {
            if index
                .column_ids()
                .iter()
                .any(|&column_id| column_id as usize == dropped_column)
            {
                return Err(radixdb_core::Error::NotSupported(format!(
                    "DROP COLUMN cannot remove an indexed key while index '{}' exists",
                    index.name()
                )));
            }

            if let Some(predicate) = index.partial_predicate() {
                let key_crosses_boundary = index
                    .column_ids()
                    .iter()
                    .any(|&column_id| column_id as usize > dropped_column);
                let predicate_crosses_boundary = predicate
                    .referenced_column_names()
                    .iter()
                    .filter_map(|name| schema.find_column(name).map(|(idx, _)| idx))
                    .any(|idx| idx >= dropped_column);
                if key_crosses_boundary || predicate_crosses_boundary {
                    return Err(radixdb_core::Error::NotSupported(format!(
                        "DROP COLUMN would invalidate partial index '{}'",
                        index.name()
                    )));
                }
            }

            let mut column_ids = index.column_ids().to_vec();
            let mut changed = false;
            for column_id in &mut column_ids {
                if *column_id as usize > dropped_column {
                    *column_id -= 1;
                    changed = true;
                }
            }
            if changed {
                replacements.push((
                    map_name.clone(),
                    Arc::new(crate::index::RenamedIndex::with_columns(
                        index.name().to_string(),
                        Arc::clone(index),
                        column_ids,
                        index.column_names().to_vec(),
                    )) as Arc<dyn Index>,
                ));
            }
        }

        for (name, index) in replacements {
            indexes.insert(name, index);
        }
        Ok(())
    }

    /// Publish an indexed column's new logical name without rebuilding its
    /// value payload. Partial predicates that mention the column are rejected
    /// by the DDL preflight because their persisted SQL/compiled owner must be
    /// rewritten as one separate operation.
    pub fn rename_indexed_column_metadata(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> radixdb_core::Result<()> {
        let mut indexes = self.indexes.write();
        let mut replacements = Vec::new();

        for (map_name, index) in indexes.iter() {
            let owns_key = index
                .column_names()
                .iter()
                .any(|name| name.eq_ignore_ascii_case(old_name));
            let owns_predicate = index.partial_predicate().is_some_and(|predicate| {
                predicate
                    .referenced_column_names()
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(old_name))
            });
            if owns_predicate {
                return Err(radixdb_core::Error::NotSupported(format!(
                    "RENAME COLUMN would invalidate partial index '{}'",
                    index.name()
                )));
            }
            if owns_key {
                let column_names = index
                    .column_names()
                    .iter()
                    .map(|name| {
                        if name.eq_ignore_ascii_case(old_name) {
                            new_name.to_string()
                        } else {
                            name.clone()
                        }
                    })
                    .collect();
                replacements.push((
                    map_name.clone(),
                    Arc::new(crate::index::RenamedIndex::with_columns(
                        index.name().to_string(),
                        Arc::clone(index),
                        index.column_ids().to_vec(),
                        column_names,
                    )) as Arc<dyn Index>,
                ));
            }
        }

        for (name, index) in replacements {
            indexes.insert(name, index);
        }
        Ok(())
    }

    /// Get an index by name
    pub fn get_index(&self, name: &str) -> Option<Arc<dyn Index>> {
        let indexes = self.indexes.read();
        indexes.get(name).cloned()
    }

    /// Get an index by column name (single-column indexes only)
    pub fn get_index_by_column(&self, column_name: &str) -> Option<Arc<dyn Index>> {
        let indexes = self.indexes.read();
        for index in indexes.values() {
            let column_names = index.column_names();
            if column_names.len() == 1 && column_names[0] == column_name {
                return Some(index.clone());
            }
        }
        None
    }

    /// Find the best multi-column index that matches a set of predicate columns.
    /// Returns the index if predicate columns cover a prefix of the index columns (leftmost prefix rule).
    /// For example, an index on (a, b, c) can be used for queries that include (a), (a, b), or (a, b, c).
    /// The predicate columns don't need to be in the same order as the index columns.
    pub fn get_multi_column_index(
        &self,
        predicate_columns: &[&str],
    ) -> Option<(Arc<dyn Index>, usize)> {
        if predicate_columns.is_empty() {
            return None;
        }

        let indexes = self.indexes.read();
        let mut best_match: Option<(Arc<dyn Index>, usize)> = None;

        // Create a set of predicate columns for O(1) lookup
        let pred_set: FxHashSet<&str> = predicate_columns.iter().copied().collect();

        for index in indexes.values() {
            let index_columns = index.column_names();
            if index_columns.len() < 2 {
                continue; // Skip single-column indexes
            }

            // Count how many of the leading index columns are in the predicate set.
            // This implements the leftmost prefix rule: we can only use the index
            // if we have predicates on a contiguous prefix of the index columns.
            let mut matched = 0;
            for idx_col in index_columns.iter() {
                if pred_set.contains(idx_col.as_str()) {
                    matched += 1;
                } else {
                    // Stop at the first index column not in predicates
                    break;
                }
            }

            // Use composite index if predicate covers a leftmost prefix
            if matched >= 1 {
                // Prefer index with more matching columns
                if best_match.is_none() || matched > best_match.as_ref().unwrap().1 {
                    best_match = Some((index.clone(), matched));
                }
            }
        }

        best_match
    }

    /// Get all indexes as Arc clones - avoids String allocation and repeated lookups
    /// OPTIMIZATION: Arc clones are cheap (atomic increment), uses SmallVec for ≤4 indexes
    #[inline]
    pub fn get_all_indexes(&self) -> SmallVec<[Arc<dyn Index>; 4]> {
        let indexes = self.indexes.read();
        indexes.values().cloned().collect()
    }

    /// Replace complete cold-backed runtime indexes with fresh hot-only
    /// generations after their immutable INDEX packs become authoritative.
    ///
    /// Every replacement is fully built before the registry write lock is
    /// taken. Readers therefore observe either the old complete in-memory
    /// generation or the new hot-only generation; they never observe a
    /// cleared or partially populated index.
    pub fn replace_cold_backfills_with_hot_only(
        &self,
        names: &FxHashSet<SmartString>,
    ) -> Result<(), Error> {
        if names.is_empty() {
            return Ok(());
        }
        let _mutation = self.mutation_guard()?;
        let versions = self.versions.read().clone();
        let expected_rows = versions.len();
        let originals = {
            let indexes = self.indexes.read();
            let mut originals = names
                .iter()
                .map(|name| {
                    indexes
                        .get(name.as_str())
                        .cloned()
                        .map(|index| (name.to_string(), index))
                        .ok_or_else(|| Error::IndexNotFound(name.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            originals.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            originals
        };

        let mut replacements = Vec::with_capacity(originals.len());
        for (name, original) in &originals {
            let replacement = empty_hot_index_like(original.as_ref(), expected_rows)?;
            let mut owned_entries = Vec::new();
            for (&row_id, chain) in versions.iter() {
                if chain.version.is_deleted() {
                    continue;
                }
                if let Some(values) =
                    index_values_for_row(replacement.as_ref(), &chain.version.data)?
                {
                    owned_entries.push((row_id, values));
                }
            }
            if !owned_entries.is_empty() {
                let borrowed = owned_entries
                    .iter()
                    .map(|(row_id, values)| (*row_id, values.as_slice()))
                    .collect::<Vec<_>>();
                replacement.add_batch_slice(&borrowed)?;
            }
            replacements.push((name.clone(), Arc::clone(original), replacement));
        }

        let mut indexes = self.indexes.write();
        for (name, expected, _) in &replacements {
            if !indexes
                .get(name)
                .is_some_and(|current| Arc::ptr_eq(current, expected))
            {
                return Err(Error::internal(format!(
                    "index '{}' changed while replacing its cold runtime backfill",
                    name
                )));
            }
        }
        for (name, _, replacement) in replacements {
            indexes.insert(name, replacement);
        }
        Ok(())
    }

    /// Borrow the indexes map under a read lock — no Arc clones, no allocations.
    #[inline]
    pub fn indexes_read(
        &self,
    ) -> parking_lot::RwLockReadGuard<'_, FxHashMap<String, Arc<dyn Index>>> {
        self.indexes.read()
    }

    /// Get column indices and names for all non-PK unique indexes.
    /// Used by commit-time cold revalidation.
    pub fn get_unique_non_pk_index_columns(&self) -> Vec<(Vec<usize>, Vec<String>)> {
        let schema = self.schema();
        let pk_col = schema
            .pk_column_index()
            .map(|i| &schema.columns[i].name_lower);
        let indexes = self.indexes.read();
        let mut result = Vec::new();
        for idx in indexes.values() {
            if !idx.is_unique() {
                continue;
            }
            let names = idx.column_names();
            if names.len() == 1 {
                if let Some(pk) = pk_col {
                    if names[0].eq_ignore_ascii_case(pk) {
                        continue;
                    }
                }
            }
            let col_indices: Vec<usize> = names
                .iter()
                .filter_map(|name| schema.columns.iter().position(|c| c.name_lower == *name))
                .collect();
            if col_indices.len() == names.len() {
                result.push((col_indices, names.to_vec()));
            }
        }
        result
    }

    /// Column sets that must be written into each immutable artifact-backed volume as an
    /// exact-equality lookup table.
    ///
    /// UNIQUE indexes need the metadata for constraint checks. Every full,
    /// non-HNSW index also contributes each non-empty leading prefix so an
    /// equality lookup retains the normal index contract after rows become
    /// cold. This includes non-unique single-column indexes and UUID primary
    /// keys. INTEGER primary keys keep their dedicated row-id metadata path and
    /// do not need a duplicate posting list.
    pub fn get_cold_exact_index_columns(&self) -> Vec<Vec<usize>> {
        let schema = self.schema();
        let integer_pk_col = schema.pk_column_index().and_then(|index| {
            (schema.columns[index].data_type == radixdb_core::DataType::Integer)
                .then_some(&schema.columns[index].name_lower)
        });
        let indexes = self.indexes.read();
        let mut result = Vec::new();

        for index in indexes.values() {
            if index.partial_predicate().is_some()
                || index.index_type() == radixdb_core::IndexType::Hnsw
            {
                continue;
            }

            let names = index.column_names();
            let is_integer_primary_key_alias = names.len() == 1
                && integer_pk_col.is_some_and(|pk| names[0].eq_ignore_ascii_case(pk));
            if is_integer_primary_key_alias {
                continue;
            }

            let col_indices: Vec<usize> = names
                .iter()
                .filter_map(|name| schema.columns.iter().position(|c| c.name_lower == *name))
                .collect();
            if col_indices.len() != names.len() || col_indices.is_empty() {
                continue;
            }
            for prefix_len in 1..=col_indices.len() {
                let prefix = col_indices[..prefix_len].to_vec();
                if !result.contains(&prefix) {
                    result.push(prefix);
                }
            }
        }

        result.sort();
        result
    }

    // =========================================================================
    // Zone Map Operations (Statistics for Segment Pruning)
    // =========================================================================

    /// Sets the zone maps for this table
    ///
    /// Zone maps contain min/max statistics per segment, enabling the query
    /// executor to skip entire segments when predicates fall outside the range.
    pub fn set_zone_maps(&self, mut zone_maps: crate::volume::zonemap::TableZoneMap) {
        let mut guard = self.zone_maps.write();
        let current_generation = self.zone_map_generation.load(Ordering::Acquire);
        let schema = self.schema();
        if !zone_maps.validate_for_schema(&schema) {
            if let Some(existing) = guard.as_ref() {
                existing.mark_stale();
            }
            return;
        }
        match zone_maps.source_generation() {
            Some(source_generation) if source_generation != current_generation => {
                if let Some(existing) = guard.as_ref() {
                    existing.mark_stale();
                }
                return;
            }
            None => zone_maps.stamp_source_generation(current_generation),
            Some(_) => {}
        }
        *guard = Some(Arc::new(zone_maps));
    }

    /// Current data/schema generation used to bind an ANALYZE build.
    pub fn zone_map_generation(&self) -> u64 {
        self.zone_map_generation.load(Ordering::Acquire)
    }

    /// Gets the zone maps for this table
    ///
    /// Returns None if zone maps have not been built (ANALYZE not run)
    /// Uses Arc to avoid expensive cloning on high QPS workloads
    pub fn get_zone_maps(&self) -> Option<Arc<crate::volume::zonemap::TableZoneMap>> {
        let guard = self.zone_maps.read();
        guard.clone()
    }

    /// Gets the segments that need to be scanned for a given predicate
    ///
    /// Uses zone maps to determine which segments can be pruned (skipped)
    pub fn get_segments_to_scan(
        &self,
        column: &str,
        operator: radixdb_core::Operator,
        value: &radixdb_core::Value,
    ) -> Option<Vec<u32>> {
        let guard = self.zone_maps.read();
        guard
            .as_ref()
            .and_then(|zm| zm.get_segments_to_scan(column, operator, value))
    }

    /// Gets prune statistics for a single-column predicate
    pub fn get_prune_stats(
        &self,
        column: &str,
        operator: radixdb_core::Operator,
        value: &radixdb_core::Value,
    ) -> Option<crate::volume::zonemap::PruneStats> {
        let guard = self.zone_maps.read();
        guard
            .as_ref()
            .and_then(|zm| zm.get_prune_stats(column, operator, value))
    }

    /// Marks zone maps as stale (needing rebuild after data changes)
    pub fn mark_zone_maps_stale(&self) {
        self.zone_map_generation.fetch_add(1, Ordering::AcqRel);
        let guard = self.zone_maps.read();
        if let Some(ref zm) = *guard {
            zm.mark_stale();
        }
    }

    /// Close the version store
    pub fn close(&self) {
        let _mutation = self.mutation_gate.write();
        self.closed.store(true, Ordering::Release);
    }

    /// Check if the version store is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    // =========================================================================
    // Recovery Functions (for WAL replay)
    // =========================================================================

    /// Apply a recovered row version during WAL replay
    ///
    /// This is used during database recovery to apply row versions from the WAL.
    /// Unlike normal operations, this directly adds the version without visibility checks.
    /// Also updates any existing indexes with the new row data.
    /// Also updates the auto_increment counter if row_id is higher than current.
    ///
    /// Duplicate Detection: If the row already exists with identical data (same values),
    /// the version is skipped to avoid duplicate entries in the version chain. This can
    /// occur when snapshot and WAL both contain the same committed data due to race
    /// conditions during snapshot creation.
    pub fn apply_recovered_version(&self, row_id: i64, version: RowVersion) -> Result<(), Error> {
        let _mutation = self.mutation_guard()?;
        let is_deleted = version.is_deleted();
        let row_data = version.data.clone();

        // Check for duplicate: if row already exists with identical data, skip adding
        // This prevents duplicate version chain entries when both snapshot and WAL
        // contain the same row data (can happen due to race conditions during snapshot)
        // No need to clone tree for single-row lookup - just hold read guard
        let existing_version = {
            let versions = self.versions.read();
            if let Some(existing_entry) = versions.get(row_id) {
                let existing = &existing_entry.version;
                // Check if data is identical (both deleted status and row data)
                if existing.is_deleted() == is_deleted && existing.data == row_data {
                    // Identical data already exists, skip to avoid duplicate
                    // Still update auto_increment counter
                    if row_id > 0 {
                        self.set_auto_increment_counter(row_id);
                    }
                    return Ok(());
                }
            }
            versions.get(row_id).map(|entry| entry.version.clone())
        };

        // Recovery must not publish the row version until every fallible index
        // transition succeeds. A later failure restores all earlier indexes.
        let old_row = existing_version
            .as_ref()
            .filter(|existing| !existing.is_deleted())
            .map(|existing| &existing.data);
        let new_row = (!is_deleted).then_some(&row_data);
        self.apply_recovery_index_transitions(row_id, old_row, new_row)?;

        // Indexes now describe the new row state, so publishing the version is
        // the final infallible step.
        self.add_version_inner(row_id, version);

        // Update auto_increment counter if this row_id is higher
        // This ensures the counter is restored to at least the max seen row_id
        if row_id > 0 {
            self.set_auto_increment_counter(row_id);
        }

        Ok(())
    }

    /// Mark a row as deleted during WAL replay
    ///
    /// This creates a deleted version for the row during recovery.
    /// Also removes the row from any existing indexes.
    pub fn mark_deleted(&self, row_id: i64, txn_id: i64) -> Result<(), Error> {
        self.mark_deleted_at(row_id, txn_id, get_fast_timestamp())
    }

    pub fn mark_deleted_at(&self, row_id: i64, txn_id: i64, create_time: i64) -> Result<(), Error> {
        let _mutation = self.mutation_guard()?;
        // Get the old row data for index removal BEFORE creating the deleted version
        let old_row = self
            .versions
            .read()
            .get(row_id)
            .filter(|entry| !entry.version.is_deleted())
            .map(|entry| entry.version.data.clone());

        // Remove every index entry before publishing the tombstone. The helper
        // restores earlier removals if any later index reports an error.
        self.apply_recovery_index_transitions(row_id, old_row.as_ref(), None)?;

        // Create a deleted version (empty data with deleted flag)
        let deleted_version = RowVersion {
            txn_id,
            deleted_at_txn_id: txn_id,
            data: Row::new(),
            create_time,
        };
        self.add_version_inner(row_id, deleted_version);

        Ok(())
    }

    /// Apply one recovered row's index transition before publishing its row
    /// version. Predicate evaluation is completed up front, and a failure
    /// restores every index already changed by this call.
    pub(super) fn apply_recovery_index_transitions(
        &self,
        row_id: i64,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<(), Error> {
        let mut indexes: Vec<Arc<dyn Index>> = self.indexes.read().values().cloned().collect();
        indexes.sort_by(|left, right| left.name().cmp(right.name()));

        let mut transitions = Vec::with_capacity(indexes.len());
        for index in indexes {
            let old_values = old_row
                .map(|row| index_values_for_row(index.as_ref(), row))
                .transpose()?
                .flatten();
            let new_values = new_row
                .map(|row| index_values_for_row(index.as_ref(), row))
                .transpose()?
                .flatten();
            if old_values != new_values {
                transitions.push((index, old_values, new_values));
            }
        }

        let mut completed = Vec::with_capacity(transitions.len());
        for (transition_index, (index, old_values, new_values)) in transitions.iter().enumerate() {
            if let Some(values) = old_values {
                if let Err(error) = index.remove(values, row_id, row_id) {
                    let rollback =
                        Self::rollback_recovery_index_transitions(row_id, &transitions, &completed);
                    return Err(Self::recovery_index_error(error, rollback));
                }
            }

            if let Some(values) = new_values {
                if let Err(error) = index.add(values, row_id, row_id) {
                    let mut rollback_failures = Vec::new();
                    if let Some(old_values) = old_values {
                        if let Err(restore_error) = index.add(old_values, row_id, row_id) {
                            rollback_failures.push(format!(
                                "restore current index '{}': {}",
                                index.name(),
                                restore_error
                            ));
                        }
                    }
                    if let Err(previous_error) =
                        Self::rollback_recovery_index_transitions(row_id, &transitions, &completed)
                    {
                        rollback_failures.push(previous_error.to_string());
                    }
                    let rollback = if rollback_failures.is_empty() {
                        Ok(())
                    } else {
                        Err(Error::internal(rollback_failures.join("; ")))
                    };
                    return Err(Self::recovery_index_error(error, rollback));
                }
            }

            completed.push(transition_index);
        }

        Ok(())
    }

    pub(super) fn rollback_recovery_index_transitions(
        row_id: i64,
        transitions: &[RecoveryIndexTransition],
        completed: &[usize],
    ) -> Result<(), Error> {
        let mut failures = Vec::new();
        for &transition_index in completed.iter().rev() {
            let (index, old_values, new_values) = &transitions[transition_index];
            if let Some(values) = new_values {
                if let Err(error) = index.remove(values, row_id, row_id) {
                    failures.push(format!(
                        "remove recovered entry from '{}': {}",
                        index.name(),
                        error
                    ));
                }
            }
            if let Some(values) = old_values {
                if let Err(error) = index.add(values, row_id, row_id) {
                    failures.push(format!(
                        "restore previous entry in '{}': {}",
                        index.name(),
                        error
                    ));
                }
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::internal(failures.join("; ")))
        }
    }

    pub(super) fn recovery_index_error(error: Error, rollback: Result<(), Error>) -> Error {
        match rollback {
            Ok(()) => error,
            Err(rollback_error) => Error::internal(format!(
                "recovery index transition failed: {}; rollback also failed: {}",
                error, rollback_error
            )),
        }
    }

    /// Drop an index by name (alias for remove_index)
    pub fn drop_index(&self, name: &str) -> Option<Arc<dyn Index>> {
        self.remove_index(name)
    }

    /// Create an index from persistence metadata during WAL replay
    ///
    /// This recreates an index from its persisted metadata.
    ///
    /// # Arguments
    /// * `meta` - Index metadata from WAL
    /// * `skip_population` - If true, creates the index structure without populating it.
    ///   This is used during batch recovery to defer population to a single-pass scan.
    pub fn create_index_from_metadata(
        &self,
        meta: &IndexDefinition,
        skip_population: bool,
    ) -> radixdb_core::Result<()> {
        self.create_index_from_metadata_with_predicate(meta, skip_population, None)
    }

    /// Create an index from durable metadata and an optional predicate already
    /// bound by the composition layer. Storage deliberately does not parse the
    /// persisted SQL text itself.
    pub fn create_index_from_metadata_with_predicate(
        &self,
        meta: &IndexDefinition,
        skip_population: bool,
        partial_predicate: Option<crate::index::PartialIndexPredicate>,
    ) -> radixdb_core::Result<()> {
        use crate::index::{BitmapIndex, EncodedIndex, HashIndex, PartialIndex};

        // Check if we have the required column information
        if meta.column_names.is_empty() {
            return Err(radixdb_core::Error::internal(
                "index metadata must have at least one column",
            ));
        }
        crate::index::validate_index_metadata_shape(
            &meta.name,
            &meta.table_name,
            &meta.column_names,
            &meta.column_ids,
            &meta.data_types,
        )?;

        // Recovery is idempotent only for the exact same authority. A name
        // collision with different columns/type/constraint metadata is an
        // artifact error, never permission to keep whichever object won first.
        if let Some(existing) = self.get_index(&meta.name) {
            let existing_predicate = existing
                .partial_predicate()
                .map(|predicate| predicate.canonical_sql());
            let expected_predicate = meta
                .partial_predicate
                .as_ref()
                .map(|predicate| predicate.canonical_sql());
            let same = existing.table_name().eq_ignore_ascii_case(&meta.table_name)
                && existing.column_names() == meta.column_names
                && existing.column_ids() == meta.column_ids
                && existing.data_types() == meta.data_types
                && existing.is_unique() == meta.is_unique
                && existing.index_type() == meta.index_type
                && existing_predicate == expected_predicate
                && existing.hnsw_m() == meta.hnsw_m
                && existing.hnsw_ef_construction() == meta.hnsw_ef_construction
                && existing
                    .default_ef_search()
                    .and_then(|value| u16::try_from(value).ok())
                    == meta.hnsw_ef_search
                && existing.hnsw_distance_metric() == meta.hnsw_distance_metric
                && existing.prepared_key_encoder() == meta.key_encoder;
            if same {
                return Ok(());
            }
            return Err(Error::invalid_argument(format!(
                "recovered index '{}' conflicts with the existing definition",
                meta.name
            )));
        }

        // Skip if a PkIndex already covers this single column
        if meta.column_names.len() == 1 {
            if let Some(existing) = self.get_index_by_column(&meta.column_names[0]) {
                if existing.index_type() == IndexType::PrimaryKey {
                    return Ok(()); // PK column covered by PkIndex
                }
            }
        }

        // Get row count for capacity hint
        // No need to clone tree just for len()
        let expected_rows = self.versions.read().len();

        let wrap_partial_index =
            |inner_index: Arc<dyn crate::Index>| -> radixdb_core::Result<Arc<dyn crate::Index>> {
                let Some(predicate_meta) = &meta.partial_predicate else {
                    return Ok(inner_index);
                };

                if meta.index_type == IndexType::Hnsw {
                    return Err(radixdb_core::Error::invalid_argument(
                        "partial HNSW indexes are not supported",
                    ));
                }

                let predicate = partial_predicate.clone().ok_or_else(|| {
                    radixdb_core::Error::invalid_argument(format!(
                        "partial index '{}' requires a prepared predicate during recovery",
                        meta.name
                    ))
                })?;
                if predicate.canonical_sql() != predicate_meta.canonical_sql() {
                    return Err(radixdb_core::Error::invalid_argument(format!(
                        "partial index '{}' predicate authority mismatch",
                        meta.name
                    )));
                }
                Ok(Arc::new(PartialIndex::new(inner_index, predicate)))
            };

        if meta.column_names.len() == 1 {
            // Single-column index
            let column_name = &meta.column_names[0];
            let column_id = meta.column_ids[0];
            let data_type = meta
                .key_encoder
                .as_ref()
                .map_or(meta.data_types[0], |encoder| encoder.physical_data_type());

            // Create index based on stored index_type
            let inner_index: Arc<dyn crate::Index> = match meta.index_type {
                IndexType::Hash => {
                    let idx = HashIndex::try_new(
                        meta.name.clone(),
                        meta.table_name.clone(),
                        vec![column_name.clone()],
                        vec![column_id],
                        vec![data_type],
                        meta.is_unique,
                        expected_rows,
                    )?;
                    Arc::new(idx)
                }
                IndexType::Bitmap => {
                    let idx = BitmapIndex::try_new(
                        meta.name.clone(),
                        meta.table_name.clone(),
                        vec![column_name.clone()],
                        vec![column_id],
                        vec![data_type],
                        meta.is_unique,
                        expected_rows,
                    )?;
                    Arc::new(idx)
                }
                IndexType::BTree => {
                    // BTree uses BTreeIndex implementation
                    let idx = crate::index::BTreeIndex::new(
                        meta.name.clone(),
                        meta.table_name.clone(),
                        column_id,
                        column_name.clone(),
                        data_type,
                        meta.is_unique,
                        expected_rows,
                    );
                    Arc::new(idx)
                }
                IndexType::MultiColumn => {
                    // MultiColumn uses MultiColumnIndex implementation
                    let idx = crate::index::MultiColumnIndex::try_new(
                        meta.name.clone(),
                        meta.table_name.clone(),
                        meta.column_names.clone(),
                        meta.column_ids.clone(),
                        meta.data_types.clone(),
                        meta.is_unique,
                        expected_rows,
                    )?;
                    Arc::new(idx)
                }
                IndexType::PrimaryKey => {
                    // PrimaryKey indexes are auto-created, never persisted via CREATE INDEX
                    return Ok(());
                }
                IndexType::Hnsw => {
                    // Get vector dimensions from schema
                    let schema = self.schema();
                    let dims = schema
                        .find_column(column_name)
                        .map(|(_, col)| col.vector_dimensions as usize)
                        .unwrap_or(0);
                    if dims == 0 {
                        return Ok(()); // Cannot rebuild without dimension info
                    }
                    let m = meta
                        .hnsw_m
                        .map(|v| v as usize)
                        .unwrap_or_else(|| crate::index::default_m_for_dims(dims));
                    let ef_construction = meta
                        .hnsw_ef_construction
                        .map(|v| v as usize)
                        .unwrap_or_else(|| crate::index::default_ef_construction(m));
                    let ef_search = meta
                        .hnsw_ef_search
                        .map(|v| v as usize)
                        .unwrap_or_else(|| crate::index::default_ef_search(m));

                    let mut idx = crate::index::HnswIndex::new(
                        meta.name.clone(),
                        meta.table_name.clone(),
                        column_name.clone(),
                        column_id,
                        dims,
                        m,
                        ef_construction,
                        ef_search,
                        crate::index::HnswDistanceMetric::from_u8(
                            meta.hnsw_distance_metric.unwrap_or(0),
                        )
                        .unwrap_or(crate::index::HnswDistanceMetric::L2),
                    )?;
                    idx.set_unique(meta.is_unique)?;
                    Arc::new(idx)
                }
            };
            let inner_index = if let Some(encoder) = &meta.key_encoder {
                Arc::new(EncodedIndex::new(
                    inner_index,
                    meta.data_types.clone(),
                    encoder.clone(),
                )?) as Arc<dyn crate::Index>
            } else {
                inner_index
            };
            let index = wrap_partial_index(inner_index)?;

            // Populate the index with existing data unless deferred
            // Uses batch_slice for better performance
            if !skip_population {
                let versions = self.versions.read().clone();
                let mut entries: Vec<(i64, Vec<radixdb_core::Value>)> = Vec::new();
                for (&row_id, version_chain) in versions.iter() {
                    let version = &version_chain.version;
                    if !version.is_deleted() {
                        if let Some(values) = index_values_for_row(index.as_ref(), &version.data)? {
                            entries.push((row_id, values));
                        }
                    }
                }
                if !entries.is_empty() {
                    let entry_refs: Vec<(i64, &[radixdb_core::Value])> = entries
                        .iter()
                        .map(|(row_id, values)| (*row_id, values.as_slice()))
                        .collect();
                    index.add_batch_slice(&entry_refs)?;
                }
            }

            self.add_index(meta.name.clone(), index)?;
        } else {
            if meta.key_encoder.is_some() {
                return Err(Error::invalid_argument(
                    "encoded operator-class indexes require one column",
                ));
            }
            // Multi-column index: use MultiColumnIndex
            let inner_index: Arc<dyn crate::Index> =
                Arc::new(crate::index::MultiColumnIndex::try_new(
                    meta.name.clone(),
                    meta.table_name.clone(),
                    meta.column_names.clone(),
                    meta.column_ids.clone(),
                    meta.data_types.clone(),
                    meta.is_unique,
                    expected_rows,
                )?);
            let index = wrap_partial_index(inner_index)?;

            // Populate the index with existing data unless deferred
            // Uses batch_slice for better performance
            if !skip_population {
                let versions = self.versions.read().clone();
                let mut entries: Vec<(i64, Vec<radixdb_core::Value>)> = Vec::new();
                for (&row_id, version_chain) in versions.iter() {
                    let version = &version_chain.version;
                    if !version.is_deleted() {
                        if let Some(values) = index_values_for_row(index.as_ref(), &version.data)? {
                            entries.push((row_id, values));
                        }
                    }
                }
                if !entries.is_empty() {
                    let entry_refs: Vec<(i64, &[radixdb_core::Value])> = entries
                        .iter()
                        .map(|(row_id, values)| (*row_id, values.as_slice()))
                        .collect();
                    index.add_batch_slice(&entry_refs)?;
                }
            }

            self.add_index(meta.name.clone(), index)?;
        }

        Ok(())
    }

    /// Populate all indexes in a single pass over the version store
    ///
    /// This is O(N + M) where N = number of rows and M = number of indexes,
    /// compared to O(N * M) when populating each index separately.
    ///
    /// OPTIMIZATION: Uses batch_slice operations to reduce lock acquisitions
    /// from O(rows × indexes) to O(indexes).
    ///
    /// Call this after WAL replay completes with skip_population=true.
    pub fn populate_all_indexes(&self) -> Result<(), Error> {
        let indexes = self.indexes.read();
        if indexes.is_empty() {
            return Ok(());
        }

        // Collect index info. HNSW indexes loaded from graph are included —
        // HnswInner::insert skips duplicate row_ids. Partial indexes must be
        // populated through index_values_for_row so predicate membership is
        // evaluated against the complete row, not only index-key columns.
        let index_infos: Vec<Arc<dyn Index>> = indexes
            .values()
            .filter(|idx| !idx.column_ids().is_empty())
            .cloned()
            .collect();

        drop(indexes); // Release lock before iteration

        if index_infos.is_empty() {
            return Ok(());
        }

        // Pre-allocate per-index batch vectors
        let num_indexes = index_infos.len();
        let mut batches: Vec<Vec<(i64, Vec<radixdb_core::Value>)>> =
            (0..num_indexes).map(|_| Vec::new()).collect();

        // First pass: Collect all entries per index
        let versions = self.versions.read().clone();
        for (&row_id, version_chain) in versions.iter() {
            let version = &version_chain.version;

            if version.is_deleted() {
                continue;
            }

            for (idx, index) in index_infos.iter().enumerate() {
                if let Some(values) = index_values_for_row(index.as_ref(), &version.data)? {
                    batches[idx].push((row_id, values));
                }
            }
        }

        // Second pass: Apply batch operations per index
        // This reduces lock acquisitions from O(rows × indexes) to O(indexes)
        for (idx, index) in index_infos.iter().enumerate() {
            if !batches[idx].is_empty() {
                let entry_refs: Vec<(i64, &[radixdb_core::Value])> = batches[idx]
                    .iter()
                    .map(|(row_id, values)| (*row_id, values.as_slice()))
                    .collect();
                index.add_batch_slice(&entry_refs)?;
            }
        }

        Ok(())
    }

    /// Populate HNSW indexes from external rows (e.g., cold segment data).
    ///
    /// Regular indexes (B-tree, Hash, Bitmap) are hot-only by design and use
    /// zone maps/bloom filters for cold data. HNSW indexes are different:
    /// vector similarity search cannot fall back to zone maps, so HNSW must
    /// contain all rows (hot + cold) to return correct results.
    ///
    /// HnswInner::insert skips duplicate row_ids, so calling this after
    /// populate_all_indexes() is safe (hot rows already in the index).
    pub fn populate_hnsw_from_rows(&self, rows: &[(i64, radixdb_core::Row)]) -> Result<(), Error> {
        let indexes = self.indexes.read();
        if indexes.is_empty() || rows.is_empty() {
            return Ok(());
        }

        // Collect only HNSW indexes with their column indices
        let hnsw_infos: Vec<(Vec<usize>, Arc<dyn Index>)> = indexes
            .values()
            .filter(|idx| idx.index_type() == radixdb_core::IndexType::Hnsw)
            .filter_map(|idx| {
                let col_ids = idx.column_ids();
                if col_ids.is_empty() {
                    return None;
                }
                let col_indices: Vec<usize> = col_ids.iter().map(|&id| id as usize).collect();
                Some((col_indices, Arc::clone(idx)))
            })
            .collect();

        drop(indexes);

        if hnsw_infos.is_empty() {
            return Ok(());
        }

        // Pre-allocate per-index batch vectors
        let mut batches: Vec<Vec<(i64, Vec<radixdb_core::Value>)>> = (0..hnsw_infos.len())
            .map(|_| Vec::with_capacity(rows.len()))
            .collect();

        for &(row_id, ref row) in rows {
            for (idx, (col_indices, _)) in hnsw_infos.iter().enumerate() {
                if col_indices.len() == 1 {
                    if let Some(value) = row.get(col_indices[0]) {
                        batches[idx].push((row_id, vec![value.clone()]));
                    }
                } else {
                    let values: Vec<radixdb_core::Value> = col_indices
                        .iter()
                        .map(|&col_idx| {
                            row.get(col_idx)
                                .cloned()
                                .unwrap_or(radixdb_core::Value::Null(radixdb_core::DataType::Null))
                        })
                        .collect();
                    batches[idx].push((row_id, values));
                }
            }
        }

        for (idx, (_, index)) in hnsw_infos.iter().enumerate() {
            if !batches[idx].is_empty() {
                let entry_refs: Vec<(i64, &[radixdb_core::Value])> = batches[idx]
                    .iter()
                    .map(|(row_id, values)| (*row_id, values.as_slice()))
                    .collect();
                index.add_batch_slice(&entry_refs)?;
            }
        }

        Ok(())
    }

    // =========================================================================
}

fn empty_hot_index_like(index: &dyn Index, expected_rows: usize) -> Result<Arc<dyn Index>, Error> {
    if index.partial_predicate().is_some() {
        return Err(Error::NotSupported(format!(
            "partial index '{}' cannot be detached from cold runtime coverage",
            index.name()
        )));
    }
    let name = index.name().to_owned();
    let table_name = index.table_name().to_owned();
    let column_names = index.column_names().to_vec();
    let column_ids = index.column_ids().to_vec();
    let logical_data_types = index.data_types().to_vec();
    let key_encoder = index.prepared_key_encoder();
    let data_types = key_encoder.as_ref().map_or_else(
        || logical_data_types.clone(),
        |encoder| vec![encoder.physical_data_type()],
    );
    let unique = index.is_unique();
    let replacement: Arc<dyn Index> = match index.index_type() {
        IndexType::Hash => Arc::new(crate::index::HashIndex::try_new(
            name,
            table_name,
            column_names,
            column_ids,
            data_types,
            unique,
            expected_rows,
        )?),
        IndexType::Bitmap => Arc::new(crate::index::BitmapIndex::try_new(
            name,
            table_name,
            column_names,
            column_ids,
            data_types,
            unique,
            expected_rows,
        )?),
        IndexType::BTree => {
            if column_names.len() != 1 || column_ids.len() != 1 || data_types.len() != 1 {
                return Err(Error::internal(format!(
                    "B-tree index '{}' has non-canonical key metadata",
                    index.name()
                )));
            }
            Arc::new(crate::index::BTreeIndex::new(
                name,
                table_name,
                column_ids[0],
                column_names[0].clone(),
                data_types[0],
                unique,
                expected_rows,
            ))
        }
        IndexType::MultiColumn => Arc::new(crate::index::MultiColumnIndex::try_new(
            name,
            table_name,
            column_names,
            column_ids,
            data_types,
            unique,
            expected_rows,
        )?),
        IndexType::PrimaryKey | IndexType::Hnsw => {
            return Err(Error::NotSupported(format!(
                "index '{}' has no immutable cold accelerator replacement",
                index.name()
            )));
        }
    };
    if let Some(encoder) = key_encoder {
        Ok(Arc::new(crate::index::EncodedIndex::new(
            replacement,
            logical_data_types,
            encoder,
        )?))
    } else {
        Ok(replacement)
    }
}
