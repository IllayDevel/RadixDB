use super::*;

impl VersionStore {
    /// Creates a new version store
    pub fn new(table_name: impl Into<SmartString>, schema: Schema) -> Self {
        Self::with_capacity(table_name, schema, None, 0)
    }

    /// Creates a new version store with pre-allocated capacity
    ///
    /// Pre-allocating capacity avoids hash map resizing during bulk inserts.
    /// Use this when the expected row count is known (e.g., during recovery).
    pub fn with_capacity(
        table_name: impl Into<SmartString>,
        schema: Schema,
        checker: Option<Arc<dyn VisibilityChecker>>,
        expected_rows: usize,
    ) -> Self {
        let versions = new_cow_btree_map();
        let arena = if expected_rows == 0 {
            RowArena::new()
        } else {
            RowArena::with_capacity(expected_rows)
        };

        Self {
            versions,
            table_name: table_name.into(),
            schema: RwLock::new(CompactArc::new(schema)),
            requires_row_normalization: AtomicBool::new(false),
            indexes: RwLock::new(FxHashMap::default()),
            closed: AtomicBool::new(false),
            mutation_gate: RwLock::new(()),
            auto_increment_counter: AtomicI64::new(0),
            uncommitted_writes: RwLock::new(new_i64_map()),
            unique_key_claims: Mutex::new(AHashMap::new()),
            claim_wait_mutex: Mutex::new(()),
            claim_changed: parking_lot::Condvar::new(),
            visibility_checker: checker.map(VisibilityOwner::Custom),
            arena,
            zone_maps: RwLock::new(None),
            zone_map_generation: AtomicU64::new(0),
            max_version_history: 0,
            committed_row_count: AtomicUsize::new(0),
            committed_hot_bytes: AtomicUsize::new(0),
            membership_fence: Arc::new(RwLock::new(())),
        }
    }

    /// Returns the table-local publication fence used by membership probes.
    #[doc(hidden)]
    pub fn membership_fence(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.membership_fence)
    }

    /// Configures the legacy per-row history limit.
    ///
    /// A non-zero hard cap can discard a version still visible to Snapshot or
    /// AS OF readers, so it is rejected. History is reclaimed only by the
    /// visibility/retention-aware cleanup owner.
    pub fn set_max_version_history(&mut self, limit: usize) -> Result<(), Error> {
        if limit != 0 {
            return Err(Error::NotSupported(
                "non-zero version history caps are not visibility-safe".to_string(),
            ));
        }
        self.max_version_history = 0;
        Ok(())
    }

    /// Gets the current max version history limit
    pub fn max_version_history(&self) -> usize {
        self.max_version_history
    }

    /// Creates a new version store with the concrete production registry path.
    pub fn with_transaction_registry(
        table_name: impl Into<SmartString>,
        schema: Schema,
        checker: Arc<TransactionRegistry>,
    ) -> Self {
        let mut store = Self::with_capacity(table_name, schema, None, 0);
        store.visibility_checker = Some(VisibilityOwner::Registry(checker));
        store
    }

    /// Creates a version store with an alternate checker for focused tests.
    pub fn with_visibility_checker(
        table_name: impl Into<SmartString>,
        schema: Schema,
        checker: Arc<dyn VisibilityChecker>,
    ) -> Self {
        Self::with_capacity(table_name, schema, Some(checker), 0)
    }

    /// Replaces the checker with the concrete production registry path.
    pub fn set_transaction_registry(&mut self, checker: Arc<TransactionRegistry>) {
        self.visibility_checker = Some(VisibilityOwner::Registry(checker));
    }

    /// Replaces the checker with an alternate implementation for focused tests.
    pub fn set_visibility_checker(&mut self, checker: Arc<dyn VisibilityChecker>) {
        self.visibility_checker = Some(VisibilityOwner::Custom(checker));
    }

    /// Returns the table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns the schema (cheap CompactArc clone)
    pub fn schema(&self) -> CompactArc<Schema> {
        self.schema.read().clone()
    }

    /// Returns a mutable reference to the schema (for modifications)
    /// Callers must use CompactArc::make_mut() to get &mut Schema
    pub fn schema_mut(&self) -> parking_lot::RwLockWriteGuard<'_, CompactArc<Schema>> {
        self.schema.write()
    }

    /// Mark that old committed rows must be normalized before evaluating a
    /// predicate against the current schema.
    pub fn require_row_normalization(&self) {
        self.requires_row_normalization
            .store(true, Ordering::Release);
        self.mark_zone_maps_stale();
    }

    /// Whether predicate fast paths must normalize logical row width first.
    pub fn requires_row_normalization(&self) -> bool {
        self.requires_row_normalization.load(Ordering::Acquire)
    }

    /// Physically remove a column from every retained hot row version.
    ///
    /// The caller must own the exclusive DDL publication fence. That excludes
    /// statement readers and transaction commit while both the MVCC chains and
    /// their zero-copy arena heads are replaced. Rows predating an appended
    /// column may be shorter; those rows legitimately have no value to remove.
    pub fn remove_column_from_hot_versions(&self, column_index: usize) {
        let mut versions = self.versions.write();
        let row_ids: Vec<i64> = versions.keys().collect();
        let mut committed_hot_bytes = 0usize;

        for row_id in row_ids {
            let Some(entry) = versions.get_mut(row_id) else {
                continue;
            };
            remove_column_from_version_chain(entry, column_index);

            if !entry.version.is_deleted() {
                committed_hot_bytes =
                    committed_hot_bytes.saturating_add(estimate_row_hot_bytes(&entry.version.data));
            }

            if let Some(arena_index) = unpack_arena_idx(entry.arena_idx) {
                let arc_data = entry.version.data.clone().into_arc();
                entry.version.data = Row::from_arc(CompactArc::clone(&arc_data));
                self.arena
                    .update_at(arena_index, row_id, entry.version.txn_id, arc_data);
                if entry.version.is_deleted() {
                    self.arena
                        .mark_deleted(arena_index, entry.version.deleted_at_txn_id);
                }
            }
        }

        self.committed_hot_bytes
            .store(committed_hot_bytes, Ordering::Release);
    }

    /// Returns the current auto-increment counter value
    pub fn get_auto_increment_counter(&self) -> i64 {
        self.auto_increment_counter.load(Ordering::Acquire)
    }

    /// Returns the next available auto-increment ID
    pub fn get_next_auto_increment_id(&self) -> Result<i64, Error> {
        self.auto_increment_counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1).filter(|next| *next > 0)
            })
            .map(|previous| previous + 1)
            .map_err(|_| Error::internal("AUTO_INCREMENT INTEGER domain exhausted"))
    }

    /// Sets the auto-increment counter to a specific value (only if current is lower)
    ///
    /// Returns true if the value was updated, false if no update was needed
    pub fn set_auto_increment_counter(&self, value: i64) -> bool {
        loop {
            let current = self.auto_increment_counter.load(Ordering::Acquire);
            if current >= value {
                return false;
            }

            if self
                .auto_increment_counter
                .compare_exchange(current, value, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Returns the current auto-increment value without incrementing
    pub fn get_current_auto_increment_value(&self) -> i64 {
        self.auto_increment_counter.load(Ordering::Acquire)
    }

    pub(super) fn mutation_guard(&self) -> Result<parking_lot::RwLockReadGuard<'_, ()>, Error> {
        let guard = self.mutation_gate.read();
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::TableClosed);
        }
        Ok(guard)
    }

    /// Adds a new version for a row.
    pub fn add_version(&self, row_id: i64, version: RowVersion) -> Result<(), Error> {
        let _mutation = self.mutation_guard()?;
        self.add_version_inner(row_id, version);
        Ok(())
    }

    pub(super) fn add_version_inner(&self, row_id: i64, version: RowVersion) {
        let is_new_version_deleted = version.deleted_at_txn_id != 0;

        // Use write lock for the entire operation (MVCC single-writer semantics)
        let mut versions = self.versions.write();

        // Use entry API to avoid double traversal
        match versions.entry(row_id) {
            radixdb_core::cow_btree::Entry::Occupied(mut occupied) => {
                // Extract existing data from the entry
                let existing = occupied.get();
                let existing_arena_idx = existing.arena_idx;
                let was_deleted = existing.version.deleted_at_txn_id != 0;
                let old_hot_bytes = if was_deleted {
                    0
                } else {
                    estimate_row_hot_bytes(&existing.version.data)
                };
                let new_hot_bytes = if is_new_version_deleted {
                    0
                } else {
                    estimate_row_hot_bytes(&version.data)
                };

                // Update committed row count based on delete state transitions
                if was_deleted && !is_new_version_deleted {
                    // Row was deleted, now being re-inserted -> increment
                    self.committed_row_count.fetch_add(1, Ordering::Relaxed);
                } else if !was_deleted && is_new_version_deleted {
                    // Row was visible, now being deleted -> decrement
                    self.committed_row_count.fetch_sub(1, Ordering::Relaxed);
                }
                self.adjust_committed_hot_bytes(old_hot_bytes, new_hot_bytes);

                // O(k) chain management - depth computed by traversal
                // When limit exceeded: drop old chain AND reuse arena slot
                let existing_depth = count_chain_depth(existing);
                let new_depth = existing_depth + 1;
                let can_reuse_arena =
                    self.max_version_history > 0 && new_depth > self.max_version_history;

                // Only clone existing version data when needed:
                // 1. For delete operations that need to preserve data
                // 2. When keeping version history (not pruning)
                let mut new_version = version;
                if new_version.deleted_at_txn_id != 0 && new_version.data.is_empty() {
                    // For deletes, preserve data from current version
                    new_version.data = existing.version.data.clone();
                }

                // Store in arena (only for non-deleted versions)
                // OPTIMIZATION: Always reuse arena slot - historical data is in prev_chain
                let arena_idx = if new_version.deleted_at_txn_id == 0 {
                    // Convert Row to Arc once (takes ownership, no copy if already Arc)
                    let arc_data = std::mem::take(&mut new_version.data).into_arc();

                    // Always reuse existing arena slot if available
                    // Historical versions are stored in prev_chain.version.data
                    let idx = if let Some(old_idx) = unpack_arena_idx(existing_arena_idx) {
                        // Reuse the arena slot - prevents unbounded growth
                        self.arena.update_at(
                            old_idx,
                            row_id,
                            new_version.txn_id,
                            CompactArc::clone(&arc_data),
                        );
                        old_idx
                    } else {
                        // No existing slot (shouldn't happen for updates), append
                        self.arena.insert_arc(
                            row_id,
                            new_version.txn_id,
                            CompactArc::clone(&arc_data),
                        )
                    };

                    // Reuse the Arc for the version's data - enables O(1) clone on read
                    new_version.data = Row::from_arc(arc_data);

                    pack_arena_idx(idx)
                } else {
                    // Deleted version - mark arena as deleted for visibility
                    if let Some(old_arena_idx) = unpack_arena_idx(existing_arena_idx) {
                        self.arena.mark_deleted(old_arena_idx, new_version.txn_id);
                    }
                    existing_arena_idx
                };

                // Build version chain entry
                // When limit exceeded: drop entire history (no prev_chain allocation)
                // When under limit: create prev_chain with existing version
                let final_prev = if can_reuse_arena {
                    // Exceeded limit - drop all history, no allocation
                    None
                } else {
                    // Under limit - clone existing version and create chain
                    let existing_version = existing.version.clone();
                    let existing_prev = existing.prev.clone();
                    Some(Arc::new(VersionChainEntry {
                        version: existing_version,
                        prev: existing_prev,
                        // Historical versions don't use arena (slot reused by new HEAD)
                        arena_idx: None,
                    }))
                };

                let new_entry = VersionChainEntry {
                    version: new_version,
                    prev: final_prev,
                    arena_idx,
                };

                // Replace entry in-place (no additional tree traversal)
                occupied.insert(new_entry);
            }
            radixdb_core::cow_btree::Entry::Vacant(vacant) => {
                // First version for this row - store in arena
                // OPTIMIZATION: Convert Row to Arc once, then just clone Arc (no data copy)
                let (arena_idx, final_version) = if version.deleted_at_txn_id == 0 {
                    // New non-deleted row -> increment counter
                    self.committed_row_count.fetch_add(1, Ordering::Relaxed);
                    self.add_committed_hot_bytes(estimate_row_hot_bytes(&version.data));

                    let mut v = version;
                    // Convert Row to Arc once (takes ownership, no copy if already Arc)
                    let arc_data = std::mem::take(&mut v.data).into_arc();
                    // Insert Arc into arena (just Arc::clone, no data copy)
                    let idx = self
                        .arena
                        .insert_arc(row_id, v.txn_id, CompactArc::clone(&arc_data));
                    // Create version with Arc-backed data for O(1) clone
                    v.data = Row::from_arc(arc_data);
                    (pack_arena_idx(idx), v)
                } else {
                    (None, version)
                };

                let new_entry = VersionChainEntry {
                    version: final_version,
                    prev: None,
                    arena_idx,
                };

                // Insert into vacant slot (no additional traversal)
                vacant.insert(new_entry);
            }
        }
    }

    /// Adds multiple versions in batch - used by commit
    ///
    /// Updates both the version store and the arena atomically per row.
    /// Arena updates are O(1) per row (just an insert), so this is efficient.
    /// Row arena index updates are batched under a single lock acquisition.
    /// Version data uses Arc-backed storage for O(1) clones on read.
    #[inline]
    pub fn add_versions_batch(&self, batch: Vec<(i64, RowVersion)>) -> Result<(), Error> {
        let _mutation = self.mutation_guard()?;
        self.add_versions_batch_inner(batch);
        Ok(())
    }

    pub(super) fn add_versions_batch_inner(&self, batch: Vec<(i64, RowVersion)>) {
        if batch.is_empty() {
            return;
        }

        // Use write lock for the entire batch operation (MVCC single-writer semantics)
        let mut versions = self.versions.write();

        // Track row count delta: positive for inserts, negative for deletes
        let mut count_delta: isize = 0;
        let mut hot_bytes_add: usize = 0;
        let mut hot_bytes_sub: usize = 0;

        for (row_id, version) in batch {
            let is_new_version_deleted = version.deleted_at_txn_id != 0;

            // Use entry API to avoid double traversal
            match versions.entry(row_id) {
                radixdb_core::cow_btree::Entry::Occupied(mut occupied) => {
                    // Extract existing data from the entry
                    let existing = occupied.get();
                    let existing_arena_idx = existing.arena_idx;
                    let was_deleted = existing.version.deleted_at_txn_id != 0;
                    let old_hot_bytes = if was_deleted {
                        0
                    } else {
                        estimate_row_hot_bytes(&existing.version.data)
                    };
                    let new_hot_bytes = if is_new_version_deleted {
                        0
                    } else {
                        estimate_row_hot_bytes(&version.data)
                    };

                    // Track row count changes based on delete state transitions
                    if was_deleted && !is_new_version_deleted {
                        count_delta += 1; // Row re-inserted
                    } else if !was_deleted && is_new_version_deleted {
                        count_delta -= 1; // Row deleted
                    }
                    accumulate_hot_bytes_delta(
                        old_hot_bytes,
                        new_hot_bytes,
                        &mut hot_bytes_add,
                        &mut hot_bytes_sub,
                    );

                    // O(k) chain management - depth computed by traversal
                    // When limit exceeded: drop old chain AND reuse arena slot
                    let existing_depth = count_chain_depth(existing);
                    let new_depth = existing_depth + 1;
                    let can_reuse_arena =
                        self.max_version_history > 0 && new_depth > self.max_version_history;

                    // Only clone existing version data when needed:
                    // 1. For delete operations that need to preserve data
                    // 2. When keeping version history (not pruning)
                    let mut new_version = version;
                    if new_version.deleted_at_txn_id != 0 && new_version.data.is_empty() {
                        // For deletes, preserve data from current version
                        new_version.data = existing.version.data.clone();
                    }

                    // Update arena with Arc reuse for O(1) clones on read
                    // OPTIMIZATION: Always reuse arena slot - historical data is in prev_chain
                    let arena_idx = if new_version.deleted_at_txn_id == 0 {
                        // Convert Row to Arc once (takes ownership, no copy if already Arc)
                        let arc_data = std::mem::take(&mut new_version.data).into_arc();

                        // Always reuse existing arena slot if available
                        // Historical versions are stored in prev_chain.version.data
                        let idx = if let Some(old_idx) = unpack_arena_idx(existing_arena_idx) {
                            // Reuse the arena slot - prevents unbounded growth
                            self.arena.update_at(
                                old_idx,
                                row_id,
                                new_version.txn_id,
                                CompactArc::clone(&arc_data),
                            );
                            old_idx
                        } else {
                            // No existing slot (shouldn't happen for updates), append
                            self.arena.insert_arc(
                                row_id,
                                new_version.txn_id,
                                CompactArc::clone(&arc_data),
                            )
                        };

                        // Reuse the Arc for the version's data
                        new_version.data = Row::from_arc(arc_data);

                        pack_arena_idx(idx)
                    } else {
                        // Deleted version - mark arena as deleted for visibility
                        if let Some(old_arena_idx) = unpack_arena_idx(existing_arena_idx) {
                            self.arena.mark_deleted(old_arena_idx, new_version.txn_id);
                        }
                        existing_arena_idx
                    };

                    // Build version chain entry
                    // When limit exceeded: drop entire history (no prev_chain allocation)
                    // When under limit: create prev_chain with existing version
                    let final_prev = if can_reuse_arena {
                        // Exceeded limit - drop all history, no allocation
                        None
                    } else {
                        // Under limit - clone existing version and create chain
                        let existing_version = existing.version.clone();
                        let existing_prev = existing.prev.clone();
                        Some(Arc::new(VersionChainEntry {
                            version: existing_version,
                            prev: existing_prev,
                            // Historical versions don't use arena (slot reused by new HEAD)
                            arena_idx: None,
                        }))
                    };

                    let new_entry = VersionChainEntry {
                        version: new_version,
                        prev: final_prev,
                        arena_idx,
                    };

                    // Replace entry in-place (no additional tree traversal)
                    occupied.insert(new_entry);
                }
                radixdb_core::cow_btree::Entry::Vacant(vacant) => {
                    // First version for this row - store in arena with Arc reuse
                    // OPTIMIZATION: Convert Row to Arc once, then just clone Arc (no data copy)
                    let (arena_idx, final_version) = if version.deleted_at_txn_id == 0 {
                        // New non-deleted row -> will increment counter
                        count_delta += 1;
                        hot_bytes_add =
                            hot_bytes_add.saturating_add(estimate_row_hot_bytes(&version.data));

                        let mut v = version;
                        // Convert Row to Arc once (takes ownership, no copy if already Arc)
                        let arc_data = std::mem::take(&mut v.data).into_arc();
                        // Insert Arc into arena (just Arc::clone, no data copy)
                        let idx =
                            self.arena
                                .insert_arc(row_id, v.txn_id, CompactArc::clone(&arc_data));
                        // Create version with Arc-backed data for O(1) clone
                        v.data = Row::from_arc(arc_data);
                        (pack_arena_idx(idx), v)
                    } else {
                        (None, version)
                    };

                    let new_entry = VersionChainEntry {
                        version: final_version,
                        prev: None,
                        arena_idx,
                    };

                    // Insert into vacant slot (no additional traversal)
                    vacant.insert(new_entry);
                }
            }
        }

        // Apply count delta in a single atomic operation
        if count_delta > 0 {
            self.committed_row_count
                .fetch_add(count_delta as usize, Ordering::Relaxed);
        } else if count_delta < 0 {
            self.committed_row_count
                .fetch_sub((-count_delta) as usize, Ordering::Relaxed);
        }

        if hot_bytes_add != 0 {
            self.committed_hot_bytes
                .fetch_add(hot_bytes_add, Ordering::Relaxed);
        }
        if hot_bytes_sub != 0 {
            self.subtract_committed_hot_bytes(hot_bytes_sub);
        }
    }

    /// Add a single version to the store (optimized for auto-commit single-row inserts)
    ///
    /// This avoids Vec allocation for the common single-row commit case.
    #[inline]
    pub fn add_version_single(&self, row_id: i64, version: RowVersion) -> Result<(), Error> {
        let _mutation = self.mutation_guard()?;
        self.add_version_single_inner(row_id, version);
        Ok(())
    }

    pub(super) fn add_version_single_inner(&self, row_id: i64, version: RowVersion) {
        let is_new_version_deleted = version.deleted_at_txn_id != 0;
        let mut versions = self.versions.write();

        match versions.entry(row_id) {
            radixdb_core::cow_btree::Entry::Occupied(mut occupied) => {
                // Extract existing data from the entry
                let existing = occupied.get();
                let existing_arena_idx = existing.arena_idx;
                let was_deleted = existing.version.deleted_at_txn_id != 0;
                let old_hot_bytes = if was_deleted {
                    0
                } else {
                    estimate_row_hot_bytes(&existing.version.data)
                };
                let new_hot_bytes = if is_new_version_deleted {
                    0
                } else {
                    estimate_row_hot_bytes(&version.data)
                };

                // Update committed row count based on delete state transitions
                if was_deleted && !is_new_version_deleted {
                    // Row was deleted, now being re-inserted -> increment
                    self.committed_row_count.fetch_add(1, Ordering::Relaxed);
                } else if !was_deleted && is_new_version_deleted {
                    // Row was visible, now being deleted -> decrement
                    self.committed_row_count.fetch_sub(1, Ordering::Relaxed);
                }
                self.adjust_committed_hot_bytes(old_hot_bytes, new_hot_bytes);

                // O(k) chain management - depth computed by traversal
                // When limit exceeded: drop old chain AND reuse arena slot
                let existing_depth = count_chain_depth(existing);
                let new_depth = existing_depth + 1;
                let can_reuse_arena =
                    self.max_version_history > 0 && new_depth > self.max_version_history;

                // Only clone existing version data when needed:
                // 1. For delete operations that need to preserve data
                // 2. When keeping version history (not pruning)
                let mut new_version = version;
                if new_version.deleted_at_txn_id != 0 && new_version.data.is_empty() {
                    // For deletes, preserve data from current version
                    new_version.data = existing.version.data.clone();
                }

                // OPTIMIZATION: Always reuse arena slot - historical data is in prev_chain
                let arena_idx = if new_version.deleted_at_txn_id == 0 {
                    let arc_data = std::mem::take(&mut new_version.data).into_arc();

                    // Always reuse existing arena slot if available
                    // Historical versions are stored in prev_chain.version.data
                    let idx = if let Some(old_idx) = unpack_arena_idx(existing_arena_idx) {
                        // Reuse the arena slot - prevents unbounded growth
                        self.arena.update_at(
                            old_idx,
                            row_id,
                            new_version.txn_id,
                            CompactArc::clone(&arc_data),
                        );
                        old_idx
                    } else {
                        // No existing slot (shouldn't happen for updates), append
                        self.arena.insert_arc(
                            row_id,
                            new_version.txn_id,
                            CompactArc::clone(&arc_data),
                        )
                    };

                    new_version.data = Row::from_arc(arc_data);

                    pack_arena_idx(idx)
                } else {
                    // Deleted version - mark arena as deleted for visibility
                    if let Some(old_arena_idx) = unpack_arena_idx(existing_arena_idx) {
                        self.arena.mark_deleted(old_arena_idx, new_version.txn_id);
                    }
                    existing_arena_idx
                };

                // Build version chain entry
                // When limit exceeded: drop entire history (no prev_chain allocation)
                // When under limit: create prev_chain with existing version
                let final_prev = if can_reuse_arena {
                    // Exceeded limit - drop all history, no allocation
                    None
                } else {
                    // Under limit - clone existing version and create chain
                    let existing_version = existing.version.clone();
                    let existing_prev = existing.prev.clone();
                    Some(Arc::new(VersionChainEntry {
                        version: existing_version,
                        prev: existing_prev,
                        // Historical versions don't use arena (slot reused by new HEAD)
                        arena_idx: None,
                    }))
                };

                let new_entry = VersionChainEntry {
                    version: new_version,
                    prev: final_prev,
                    arena_idx,
                };

                occupied.insert(new_entry);
            }
            radixdb_core::cow_btree::Entry::Vacant(vacant) => {
                let (arena_idx, final_version) = if version.deleted_at_txn_id == 0 {
                    // New non-deleted row -> increment counter
                    self.committed_row_count.fetch_add(1, Ordering::Relaxed);
                    self.add_committed_hot_bytes(estimate_row_hot_bytes(&version.data));

                    let mut v = version;
                    let arc_data = std::mem::take(&mut v.data).into_arc();
                    let idx = self
                        .arena
                        .insert_arc(row_id, v.txn_id, CompactArc::clone(&arc_data));
                    v.data = Row::from_arc(arc_data);
                    (pack_arena_idx(idx), v)
                } else {
                    (None, version)
                };

                let new_entry = VersionChainEntry {
                    version: final_version,
                    prev: None,
                    arena_idx,
                };

                vacant.insert(new_entry);
            }
        }
    }

    /// Quick check if a row might exist
    pub fn quick_check_row_existence(&self, row_id: i64) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }

        self.versions.read().contains_key(row_id)
    }

    /// Gets the latest visible version of a row
    pub fn get_visible_version(&self, row_id: i64, txn_id: i64) -> Option<RowVersion> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }

        let checker = self.visibility_checker.as_ref()?;

        // Phase 1: Speculative arena probe (arena lock only, self-contained).
        // For row_id N, probe arena slot N-1. Verify meta.row_id == row_id.
        // Hit rate ~100% for sequential-insert tables (arena slot reused on UPDATE).
        // Arena guard is acquired and dropped here to avoid lock ordering inversion
        // with the commit path (which holds versions.write → arena.write).
        if row_id > 0 {
            let probe_idx = (row_id - 1) as usize;
            let arena_guard = self.arena.read_guard();
            let arena_meta = arena_guard.meta();
            if probe_idx < arena_meta.len() {
                let meta = arena_meta[probe_idx];
                if meta.row_id == row_id && checker.is_visible(meta.txn_id, txn_id) {
                    if meta.deleted_at_txn_id != 0
                        && checker.is_visible(meta.deleted_at_txn_id, txn_id)
                    {
                        return None;
                    }
                    let data = Row::from_arc(CompactArc::clone(&arena_guard.data()[probe_idx]));
                    return Some(RowVersion {
                        txn_id: meta.txn_id,
                        deleted_at_txn_id: meta.deleted_at_txn_id,
                        data,
                        // Arena metadata doesn't store create_time (saves 8 bytes/row).
                        // All callers of get_visible_version() only use .data, .is_deleted(),
                        // and .txn_id. AS OF queries use get_visible_version_as_of_timestamp()
                        // which goes through CowBTree and has the real create_time.
                        create_time: 0,
                    });
                }
            }
            // arena_guard dropped here before acquiring versions lock
        }

        // Phase 2: CowBTree fallback (correct lock ordering: versions first, then arena)
        let versions = self.versions.read();
        let chain = versions.get(row_id)?;

        // Check HEAD visibility (most common case)
        let head_txn_id = chain.version.txn_id;
        let head_deleted_at = chain.version.deleted_at_txn_id;

        if checker.is_visible(head_txn_id, txn_id) {
            if head_deleted_at != 0 && checker.is_visible(head_deleted_at, txn_id) {
                return None;
            }
            // Use arena data via chain.arena_idx for O(1) Arc clone
            let row = if let Some(idx) = unpack_arena_idx(chain.arena_idx) {
                let arena_guard = self.arena.read_guard();
                if let Some(arc_row) = arena_guard.data().get(idx) {
                    Row::from_arc(CompactArc::clone(arc_row))
                } else {
                    chain.version.data.clone()
                }
            } else {
                chain.version.data.clone()
            };
            return Some(RowVersion {
                txn_id: head_txn_id,
                deleted_at_txn_id: head_deleted_at,
                data: row,
                create_time: chain.version.create_time,
            });
        }

        // Traverse version chain for older visible versions
        let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());

        while let Some(e) = current {
            let version_txn_id = e.version.txn_id;
            let deleted_at_txn_id = e.version.deleted_at_txn_id;

            if checker.is_visible(version_txn_id, txn_id) {
                if deleted_at_txn_id != 0 && checker.is_visible(deleted_at_txn_id, txn_id) {
                    return None;
                }
                return Some(e.version.clone());
            }
            current = e.prev.as_ref().map(|b| b.as_ref());
        }

        None
    }

    /// Check if any of the given row_ids have a visible version
    /// Returns the first row_id that has a visible version, or None if none exist
    /// OPTIMIZATION: Used for conflict detection - stops at first hit, no data fetch
    #[inline]
    pub fn has_any_visible_version(&self, row_ids: &[i64], txn_id: i64) -> Option<i64> {
        if self.closed.load(Ordering::Acquire) || row_ids.is_empty() {
            return None;
        }

        let checker = self.visibility_checker.as_ref()?;

        // Lock ordering: versions first, then arena (matches commit path)
        let versions = self.versions.read();
        let arena_guard = self.arena.read_guard();
        let arena_meta = arena_guard.meta();

        for &row_id in row_ids {
            // Speculative arena probe: O(1) for auto-increment PKs
            if row_id > 0 {
                let probe_idx = (row_id - 1) as usize;
                if probe_idx < arena_meta.len() {
                    let meta = arena_meta[probe_idx];
                    if meta.row_id == row_id && checker.is_visible(meta.txn_id, txn_id) {
                        if meta.deleted_at_txn_id == 0
                            || !checker.is_visible(meta.deleted_at_txn_id, txn_id)
                        {
                            return Some(row_id); // Conflict!
                        }
                        continue; // Deleted, check next
                    }
                }
            }

            // CowBTree lookup: O(log n)
            if let Some(chain) = versions.get(row_id) {
                let head_txn_id = chain.version.txn_id;
                let head_deleted_at = chain.version.deleted_at_txn_id;

                if checker.is_visible(head_txn_id, txn_id) {
                    if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                        return Some(row_id); // Found visible version - conflict!
                    }
                    continue; // Deleted, check next
                }

                // Check chain for older visible versions
                let mut current: Option<&VersionChainEntry> =
                    chain.prev.as_ref().map(|b| b.as_ref());

                while let Some(e) = current {
                    if checker.is_visible(e.version.txn_id, txn_id) {
                        if e.version.deleted_at_txn_id == 0
                            || !checker.is_visible(e.version.deleted_at_txn_id, txn_id)
                        {
                            return Some(row_id); // Found visible version - conflict!
                        }
                        break; // Deleted, move to next row
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
            }
        }
        None
    }

    /// Gets multiple visible versions in a single batch operation
    ///
    /// Pre-acquires all locks once, then performs per-key CowBTree lookups
    /// with visibility checking and version chain traversal as needed.
    pub fn get_visible_versions_batch(&self, row_ids: &[i64], txn_id: i64) -> RowVec {
        if self.closed.load(Ordering::Acquire) || row_ids.is_empty() {
            return RowVec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return RowVec::new(),
        };

        // Lock ordering: versions first, then arena (matches commit path)
        let versions = self.versions.read();
        let arena_guard = self.arena.read_guard();
        let arena_meta = arena_guard.meta();
        let arena_data = arena_guard.data();

        // Fast path: if both arena and version tree are empty, no rows can match.
        // This avoids iterating millions of phantom row_ids from volume-populated indexes.
        if arena_meta.is_empty() && versions.is_empty() {
            return RowVec::new();
        }

        // Don't over-allocate: most row_ids may be phantom (from volume indexes)
        let mut results = RowVec::with_capacity(row_ids.len().min(4096));

        for &row_id in row_ids {
            // Speculative arena probe: O(1) for auto-increment PKs
            if row_id > 0 {
                let probe_idx = (row_id - 1) as usize;
                if probe_idx < arena_meta.len() {
                    let meta = arena_meta[probe_idx];
                    if meta.row_id == row_id && checker.is_visible(meta.txn_id, txn_id) {
                        if meta.deleted_at_txn_id == 0
                            || !checker.is_visible(meta.deleted_at_txn_id, txn_id)
                        {
                            let row = Row::from_arc(CompactArc::clone(&arena_data[probe_idx]));
                            results.push((row_id, row));
                        }
                        continue; // Skip CowBTree lookup
                    }
                }
            }

            // CowBTree lookup: O(log n)
            if let Some(chain) = versions.get(row_id) {
                let head_txn_id = chain.version.txn_id;
                let head_deleted_at = chain.version.deleted_at_txn_id;

                if checker.is_visible(head_txn_id, txn_id) {
                    if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                        let row = if let Some(idx) = unpack_arena_idx(chain.arena_idx) {
                            if let Some(arc_row) = arena_data.get(idx) {
                                Row::from_arc(CompactArc::clone(arc_row))
                            } else {
                                chain.version.data.clone()
                            }
                        } else {
                            chain.version.data.clone()
                        };
                        results.push((row_id, row));
                    }
                    continue;
                }

                // Traverse version chain for older visible versions
                let mut current: Option<&VersionChainEntry> =
                    chain.prev.as_ref().map(|b| b.as_ref());
                while let Some(e) = current {
                    if checker.is_visible(e.version.txn_id, txn_id) {
                        if e.version.deleted_at_txn_id == 0
                            || !checker.is_visible(e.version.deleted_at_txn_id, txn_id)
                        {
                            let row = if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                                if let Some(arc_row) = arena_data.get(idx) {
                                    Row::from_arc(CompactArc::clone(arc_row))
                                } else {
                                    e.version.data.clone()
                                }
                            } else {
                                e.version.data.clone()
                            };
                            results.push((row_id, row));
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
            }
        }
        results
    }

    /// Iterates visible versions for given row_ids with early termination support.
    ///
    /// Pre-acquires all locks ONCE, then performs per-key CowBTree lookups.
    /// Calls `callback(row_id, row_data)` for each visible row.
    /// Stops iteration if callback returns `false` (used for LIMIT).
    ///
    /// This is more efficient than calling get_visible_version() per row_id because:
    /// - Single lock acquisition for CowBTree + arena (not per-row)
    /// - Supports early termination (unlike get_visible_versions_batch which collects all)
    pub fn for_each_visible<F>(&self, row_ids: &[i64], txn_id: i64, mut callback: F)
    where
        F: FnMut(i64, Row) -> bool,
    {
        if self.closed.load(Ordering::Acquire) || row_ids.is_empty() {
            return;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return,
        };

        // Lock ordering: versions first, then arena (matches commit path)
        let versions = self.versions.read();
        let arena_guard = self.arena.read_guard();
        let arena_meta = arena_guard.meta();
        let arena_data = arena_guard.data();

        for &row_id in row_ids {
            // Speculative arena probe: O(1) for auto-increment PKs
            if row_id > 0 {
                let probe_idx = (row_id - 1) as usize;
                if probe_idx < arena_meta.len() {
                    let meta = arena_meta[probe_idx];
                    if meta.row_id == row_id && checker.is_visible(meta.txn_id, txn_id) {
                        if meta.deleted_at_txn_id == 0
                            || !checker.is_visible(meta.deleted_at_txn_id, txn_id)
                        {
                            let row = Row::from_arc(CompactArc::clone(&arena_data[probe_idx]));
                            if !callback(row_id, row) {
                                return;
                            }
                        }
                        continue; // Skip CowBTree lookup
                    }
                }
            }

            // CowBTree lookup: O(log n)
            if let Some(chain) = versions.get(row_id) {
                let head_txn_id = chain.version.txn_id;
                let head_deleted_at = chain.version.deleted_at_txn_id;

                if checker.is_visible(head_txn_id, txn_id) {
                    if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                        let row = if let Some(idx) = unpack_arena_idx(chain.arena_idx) {
                            if let Some(arc_row) = arena_data.get(idx) {
                                Row::from_arc(CompactArc::clone(arc_row))
                            } else {
                                chain.version.data.clone()
                            }
                        } else {
                            chain.version.data.clone()
                        };
                        if !callback(row_id, row) {
                            return;
                        }
                    }
                    continue;
                }

                // Traverse version chain for older visible versions
                let mut current: Option<&VersionChainEntry> =
                    chain.prev.as_ref().map(|b| b.as_ref());
                while let Some(e) = current {
                    if checker.is_visible(e.version.txn_id, txn_id) {
                        if e.version.deleted_at_txn_id == 0
                            || !checker.is_visible(e.version.deleted_at_txn_id, txn_id)
                        {
                            let row = if let Some(idx) = unpack_arena_idx(e.arena_idx) {
                                if let Some(arc_row) = arena_data.get(idx) {
                                    Row::from_arc(CompactArc::clone(arc_row))
                                } else {
                                    e.version.data.clone()
                                }
                            } else {
                                e.version.data.clone()
                            };
                            if !callback(row_id, row) {
                                return;
                            }
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
            }
        }
    }

    /// Probe visible row IDs without reading or cloning row payloads.
    ///
    /// The output is aligned with `row_ids`: duplicate IDs therefore produce
    /// duplicate output positions and contribute separately to the returned
    /// count. The caller is responsible for validating equal slice lengths.
    ///
    /// This acquires the version-tree and arena metadata locks once for the
    /// entire batch. Only MVCC metadata and version-chain metadata are read;
    /// arena row data is never accessed.
    pub fn probe_visible_row_ids_batch(
        &self,
        row_ids: &[i64],
        txn_id: i64,
        matches: &mut [bool],
    ) -> usize {
        assert_eq!(
            row_ids.len(),
            matches.len(),
            "row ID probe output must align with input"
        );
        matches.fill(false);

        if self.closed.load(Ordering::Acquire) || row_ids.is_empty() {
            return 0;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return 0,
        };

        // Lock ordering: versions first, then arena (matches commit path).
        // Only arena metadata is borrowed: no Row/CompactArc clone occurs.
        let versions = self.versions.read();
        let arena_guard = self.arena.read_guard();
        let arena_meta = arena_guard.meta();

        if arena_meta.is_empty() && versions.is_empty() {
            return 0;
        }

        let mut count = 0usize;
        for (position, &row_id) in row_ids.iter().enumerate() {
            // Speculative arena metadata probe for sequential positive PKs.
            if row_id > 0 {
                let probe_idx = (row_id - 1) as usize;
                if probe_idx < arena_meta.len() {
                    let meta = arena_meta[probe_idx];
                    if meta.row_id == row_id && checker.is_visible(meta.txn_id, txn_id) {
                        let visible = meta.deleted_at_txn_id == 0
                            || !checker.is_visible(meta.deleted_at_txn_id, txn_id);
                        matches[position] = visible;
                        count += usize::from(visible);
                        continue;
                    }
                }
            }

            let Some(chain) = versions.get(row_id) else {
                continue;
            };

            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;
            if checker.is_visible(head_txn_id, txn_id) {
                let visible = head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id);
                matches[position] = visible;
                count += usize::from(visible);
                continue;
            }

            // HEAD is newer than this snapshot. Walk back to the first visible
            // version; its deletion marker decides membership at that snapshot.
            let mut current = chain.prev.as_deref();
            while let Some(entry) = current {
                if checker.is_visible(entry.version.txn_id, txn_id) {
                    let deleted_at = entry.version.deleted_at_txn_id;
                    let visible = deleted_at == 0 || !checker.is_visible(deleted_at, txn_id);
                    matches[position] = visible;
                    count += usize::from(visible);
                    break;
                }
                current = entry.prev.as_deref();
            }
        }

        count
    }

    /// Counts visible versions for batch operations (COUNT optimization)
    ///
    /// This is an optimized version of get_visible_versions_batch that only counts
    /// visible rows without cloning their data. Used for COUNT(*) subqueries.
    ///
    /// Uses parallel processing for large batches (>1000 row_ids) to leverage
    /// multiple CPU cores for visibility checking.
    pub fn count_visible_versions_batch(&self, row_ids: &[i64], txn_id: i64) -> usize {
        if self.closed.load(Ordering::Acquire) || row_ids.is_empty() {
            return 0;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return 0,
        };

        /// Minimum batch size before enabling parallel processing
        #[cfg(feature = "parallel")]
        const PARALLEL_THRESHOLD: usize = 1000;
        /// Chunk size for parallel processing
        #[cfg(feature = "parallel")]
        const PARALLEL_CHUNK_SIZE: usize = 512;

        // Lock ordering: versions first, then arena (matches commit path)
        // Clone CowBTree for parallel path (O(1) Arc clone of root node)
        let versions = self.versions.read().clone();
        let arena_guard = self.arena.read_guard();
        let arena_meta = arena_guard.meta();

        #[cfg(feature = "parallel")]
        if row_ids.len() >= PARALLEL_THRESHOLD {
            return row_ids
                .par_chunks(PARALLEL_CHUNK_SIZE)
                .map(|chunk| {
                    let mut chunk_count = 0;
                    for &row_id in chunk {
                        // Speculative arena probe: O(1) for auto-increment PKs
                        if row_id > 0 {
                            let probe_idx = (row_id - 1) as usize;
                            if probe_idx < arena_meta.len() {
                                let meta = arena_meta[probe_idx];
                                if meta.row_id == row_id && checker.is_visible(meta.txn_id, txn_id)
                                {
                                    if meta.deleted_at_txn_id == 0
                                        || !checker.is_visible(meta.deleted_at_txn_id, txn_id)
                                    {
                                        chunk_count += 1;
                                    }
                                    continue;
                                }
                            }
                        }

                        // CowBTree fallback: O(log n)
                        if let Some(chain) = versions.get(row_id) {
                            let head_txn_id = chain.version.txn_id;
                            let head_deleted_at = chain.version.deleted_at_txn_id;

                            if checker.is_visible(head_txn_id, txn_id) {
                                if head_deleted_at == 0
                                    || !checker.is_visible(head_deleted_at, txn_id)
                                {
                                    chunk_count += 1;
                                }
                                continue;
                            }

                            let mut current: Option<&VersionChainEntry> =
                                chain.prev.as_ref().map(|b| b.as_ref());
                            while let Some(e) = current {
                                if checker.is_visible(e.version.txn_id, txn_id) {
                                    if e.version.deleted_at_txn_id == 0
                                        || !checker.is_visible(e.version.deleted_at_txn_id, txn_id)
                                    {
                                        chunk_count += 1;
                                    }
                                    break;
                                }
                                current = e.prev.as_ref().map(|b| b.as_ref());
                            }
                        }
                    }
                    chunk_count
                })
                .sum();
        }

        // Sequential path for small batches (or when parallel feature is disabled)
        let mut count = 0;
        for &row_id in row_ids {
            // Speculative arena probe: O(1) for auto-increment PKs
            if row_id > 0 {
                let probe_idx = (row_id - 1) as usize;
                if probe_idx < arena_meta.len() {
                    let meta = arena_meta[probe_idx];
                    if meta.row_id == row_id && checker.is_visible(meta.txn_id, txn_id) {
                        if meta.deleted_at_txn_id == 0
                            || !checker.is_visible(meta.deleted_at_txn_id, txn_id)
                        {
                            count += 1;
                        }
                        continue;
                    }
                }
            }

            // CowBTree fallback: O(log n)
            if let Some(chain) = versions.get(row_id) {
                let head_txn_id = chain.version.txn_id;
                let head_deleted_at = chain.version.deleted_at_txn_id;

                if checker.is_visible(head_txn_id, txn_id) {
                    if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                        count += 1;
                    }
                    continue;
                }

                let mut current: Option<&VersionChainEntry> =
                    chain.prev.as_ref().map(|b| b.as_ref());
                while let Some(e) = current {
                    if checker.is_visible(e.version.txn_id, txn_id) {
                        if e.version.deleted_at_txn_id == 0
                            || !checker.is_visible(e.version.deleted_at_txn_id, txn_id)
                        {
                            count += 1;
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
            }
        }
        count
    }

    /// Gets visible versions for batch update operations
    ///
    /// Returns (row_id, row_data, original_version) for each visible row.
    /// The original_version is used for write-set tracking to avoid redundant lookups.
    ///
    /// This is optimized for UPDATE operations where we need to:
    /// 1. Read the current row data
    /// 2. Track the original version for conflict detection
    /// 3. Skip redundant lookups during put
    pub fn get_visible_versions_for_update(
        &self,
        row_ids: &[i64],
        txn_id: i64,
    ) -> Vec<(i64, Row, RowVersion)> {
        if self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Vec::new(),
        };

        let current_seq = checker.get_current_sequence();
        let mut results = Vec::with_capacity(row_ids.len());

        // Clone CowBTree to release read lock early, allowing concurrent commits
        let versions = self.versions.read().clone();

        // Pre-acquire arena lock for O(1) Arc clones
        let arena_guard = self.arena.read_guard();
        let arena_data = arena_guard.data();

        // Helper: get row from arena (O(1)) or version (O(n) clone)
        let get_row = |entry: &VersionChainEntry| -> Row {
            if let Some(idx) = unpack_arena_idx(entry.arena_idx) {
                if let Some(arc_row) = arena_data.get(idx) {
                    return Row::from_arc(CompactArc::clone(arc_row));
                }
            }
            entry.version.data.clone()
        };
        for &row_id in row_ids {
            if let Some(chain) = versions.get(row_id) {
                // FAST PATH: Check HEAD version first - O(1) for common case
                let head_txn_id = chain.version.txn_id;
                let head_deleted_at = chain.version.deleted_at_txn_id;

                if checker.is_visible(head_txn_id, txn_id) {
                    // HEAD is visible - check if deleted
                    if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                        let mut version_copy = chain.version.clone();
                        version_copy.create_time = current_seq;
                        results.push((row_id, get_row(chain), version_copy));
                    }
                    continue;
                }

                // SLOW PATH: HEAD not visible - traverse chain for older versions
                let mut current: Option<&VersionChainEntry> =
                    chain.prev.as_ref().map(|b| b.as_ref());
                while let Some(e) = current {
                    let version_txn_id = e.version.txn_id;
                    let deleted_at_txn_id = e.version.deleted_at_txn_id;

                    if checker.is_visible(version_txn_id, txn_id) {
                        if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id)
                        {
                            // Preserve the visibility boundary on the returned copy.
                            let mut version_copy = e.version.clone();
                            // Store the current sequence in create_time for later retrieval
                            // (This is a bit of a hack, but avoids changing the struct)
                            version_copy.create_time = current_seq;
                            results.push((row_id, get_row(e), version_copy));
                        }
                        break;
                    }
                    current = e.prev.as_ref().map(|b| b.as_ref());
                }
            }
        }

        results
    }

    /// Gets the visible version as of a specific transaction
    pub fn get_visible_version_as_of_transaction(
        &self,
        row_id: i64,
        as_of_txn_id: i64,
    ) -> Option<RowVersion> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }

        // No need to clone tree for single-row lookup - just hold read guard
        let versions = self.versions.read();
        let chain = versions.get(row_id)?;

        // Traverse version chain from newest to oldest
        let mut current: Option<&VersionChainEntry> = Some(chain);
        while let Some(e) = current {
            // Check if this version was created before or at the asOf transaction
            if e.version.txn_id <= as_of_txn_id {
                // Check if deleted before or at asOfTxnID
                if e.version.deleted_at_txn_id != 0 && e.version.deleted_at_txn_id <= as_of_txn_id {
                    return None;
                }
                return Some(e.version.clone());
            }
            current = e.prev.as_ref().map(|b| b.as_ref());
        }

        None
    }

    /// Gets the visible version as of a specific timestamp
    pub fn get_visible_version_as_of_timestamp(
        &self,
        row_id: i64,
        as_of_timestamp: i64,
    ) -> Option<RowVersion> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }

        // No need to clone tree for single-row lookup - just hold read guard
        let versions = self.versions.read();
        let chain = versions.get(row_id)?;

        // Traverse version chain from newest to oldest
        let mut current: Option<&VersionChainEntry> = Some(chain);
        while let Some(e) = current {
            // Check if this version was created before or at the asOf timestamp
            if e.version.create_time <= as_of_timestamp {
                // Check if deleted (we can't easily determine timestamp of deletion in this model)
                // For now, check if DeletedAtTxnID is set
                if e.version.deleted_at_txn_id != 0 {
                    return None;
                }
                return Some(e.version.clone());
            }
            current = e.prev.as_ref().map(|b| b.as_ref());
        }

        None
    }

    /// Returns all row IDs in the version store (sorted)
    pub fn get_all_row_ids(&self) -> Vec<i64> {
        if self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }

        // Clone tree to avoid holding lock for the duration of iteration
        let versions = self.versions.read().clone();
        versions.keys().collect()
    }

    /// Populate a FxHashSet with all hot row_ids. Iterates the B-tree under
    /// read lock without cloning — much cheaper than get_all_row_ids() + collect()
    /// for skip-set construction.
    pub fn collect_row_ids_into(&self, dest: &mut rustc_hash::FxHashSet<i64>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let versions = self.versions.read();
        for key in versions.keys() {
            dest.insert(key);
        }
    }

    /// Populate a caller-owned set with logical row IDs visible to `txn_id`
    /// without cloning row payloads or allocating an intermediate ID vector.
    pub fn collect_visible_row_ids_into(&self, txn_id: i64, dest: &mut rustc_hash::FxHashSet<i64>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }

        let Some(checker) = self.visibility_checker.as_ref() else {
            return;
        };

        let uncommitted_empty = self.uncommitted_writes.read().is_empty();
        if uncommitted_empty && !checker.needs_snapshot_isolation(txn_id) {
            let arena_guard = self.arena.read_guard();
            if !arena_guard.is_empty() {
                for meta in arena_guard.meta() {
                    if meta.txn_id != 0
                        && meta.deleted_at_txn_id == 0
                        && checker.is_visible(meta.txn_id, txn_id)
                    {
                        dest.insert(meta.row_id);
                    }
                }
                return;
            }
        }

        let versions = self.versions.read();
        for (&row_id, chain) in versions.iter() {
            if checker.is_visible(chain.version.txn_id, txn_id) {
                if chain.version.deleted_at_txn_id == 0
                    || !checker.is_visible(chain.version.deleted_at_txn_id, txn_id)
                {
                    dest.insert(row_id);
                }
                continue;
            }

            let mut current = chain.prev.as_deref();
            while let Some(entry) = current {
                if checker.is_visible(entry.version.txn_id, txn_id) {
                    if entry.version.deleted_at_txn_id == 0
                        || !checker.is_visible(entry.version.deleted_at_txn_id, txn_id)
                    {
                        dest.insert(row_id);
                    }
                    break;
                }
                current = entry.prev.as_deref();
            }
        }
    }

    /// Returns all row IDs that are visible to the given transaction
    ///
    /// OPTIMIZATION: Single-pass iteration with O(1) lock acquisition instead of O(N).
    /// Uses the same HEAD-first visibility pattern as count_visible_rows.
    pub fn get_all_visible_row_ids(&self, txn_id: i64) -> Vec<i64> {
        if self.closed.load(Ordering::Acquire) {
            return Vec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Vec::new(),
        };

        // FAST PATH: Scan arena directly when no uncommitted writes.
        // SAFETY: Only valid under ReadCommitted (see get_visible_row_indices).
        {
            let uncommitted_empty = self.uncommitted_writes.read().is_empty();
            if uncommitted_empty && !checker.needs_snapshot_isolation(txn_id) {
                let arena_guard = self.arena.read_guard();
                let arena_meta = arena_guard.meta();
                let arena_len = arena_guard.len();

                if arena_len > 0 {
                    let mut visible_row_ids = Vec::with_capacity(arena_len);
                    for meta in arena_meta {
                        if meta.txn_id != 0
                            && meta.deleted_at_txn_id == 0
                            && checker.is_visible(meta.txn_id, txn_id)
                        {
                            visible_row_ids.push(meta.row_id);
                        }
                    }
                    return visible_row_ids;
                }
            }
        }

        // SLOW PATH: Full CowBTree iteration
        let versions = self.versions.read().clone();
        let mut visible_row_ids = Vec::with_capacity(versions.len());

        for (&row_id, chain) in versions.iter() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            if checker.is_visible(head_txn_id, txn_id) {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    visible_row_ids.push(row_id);
                }
                continue;
            }

            // SLOW PATH: HEAD not visible - traverse chain for older versions
            let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        visible_row_ids.push(row_id);
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        visible_row_ids
    }

    /// Count visible non-deleted rows in a single pass (optimized for row_count)
    ///
    /// OPTIMIZATION: This method counts rows in O(1) lock acquisition instead of O(N)
    /// by iterating through versions once without cloning any row data.
    pub fn count_visible_rows(&self, txn_id: i64) -> usize {
        if self.closed.load(Ordering::Acquire) {
            return 0;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return 0,
        };

        // FAST PATH: Scan arena directly when no uncommitted writes.
        // SAFETY: Only valid under ReadCommitted (see get_visible_row_indices).
        {
            let uncommitted_empty = self.uncommitted_writes.read().is_empty();
            if uncommitted_empty && !checker.needs_snapshot_isolation(txn_id) {
                let arena_guard = self.arena.read_guard();
                let arena_meta = arena_guard.meta();
                let arena_len = arena_guard.len();

                if arena_len > 0 {
                    let mut count = 0usize;
                    for meta in arena_meta {
                        if meta.txn_id != 0
                            && meta.deleted_at_txn_id == 0
                            && checker.is_visible(meta.txn_id, txn_id)
                        {
                            count += 1;
                        }
                    }
                    return count;
                }
            }
        }

        // SLOW PATH: Full CowBTree iteration
        let mut count = 0;
        let versions = self.versions.read().clone();
        for chain in versions.values() {
            // FAST PATH: Check HEAD version first - O(1) for common case
            let head_txn_id = chain.version.txn_id;
            let head_deleted_at = chain.version.deleted_at_txn_id;

            if checker.is_visible(head_txn_id, txn_id) {
                // HEAD is visible - check if deleted
                if head_deleted_at == 0 || !checker.is_visible(head_deleted_at, txn_id) {
                    count += 1;
                }
                continue;
            }

            // SLOW PATH: HEAD not visible - traverse chain for older versions
            let mut current: Option<&VersionChainEntry> = chain.prev.as_ref().map(|b| b.as_ref());
            while let Some(e) = current {
                let version_txn_id = e.version.txn_id;
                let deleted_at_txn_id = e.version.deleted_at_txn_id;

                if checker.is_visible(version_txn_id, txn_id) {
                    if deleted_at_txn_id == 0 || !checker.is_visible(deleted_at_txn_id, txn_id) {
                        count += 1;
                    }
                    break;
                }
                current = e.prev.as_ref().map(|b| b.as_ref());
            }
        }

        count
    }

    /// Returns the count of committed non-deleted rows (O(1) operation)
    ///
    /// This is the fast path for COUNT(*) queries without WHERE clause.
    /// The counter is updated atomically on INSERT commit (+1) and DELETE commit (-1).
    ///
    /// # Important
    ///
    /// This count does NOT include uncommitted changes from the current transaction.
    /// Use this only for queries that see committed data (e.g., autocommit queries).
    #[inline]
    pub fn committed_row_count(&self) -> usize {
        self.committed_row_count.load(Ordering::Relaxed)
    }

    /// Returns the approximate byte budget of committed non-deleted rows that
    /// still live in the hot MVCC store.
    #[inline]
    pub fn committed_hot_bytes(&self) -> usize {
        self.committed_hot_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub(super) fn add_committed_hot_bytes(&self, bytes: usize) {
        if bytes != 0 {
            self.committed_hot_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(super) fn adjust_committed_hot_bytes(&self, old_bytes: usize, new_bytes: usize) {
        if new_bytes > old_bytes {
            self.add_committed_hot_bytes(new_bytes - old_bytes);
        } else if old_bytes > new_bytes {
            self.subtract_committed_hot_bytes(old_bytes - new_bytes);
        }
    }

    /// Atomically subtract bytes from `committed_hot_bytes` without wrapping.
    pub fn subtract_committed_hot_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        loop {
            let current = self.committed_hot_bytes.load(Ordering::Relaxed);
            let new_val = current.saturating_sub(bytes);
            match self.committed_hot_bytes.compare_exchange_weak(
                current,
                new_val,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
    }

    /// Check if a row_id exists in the committed version store (B-tree).
    /// Used during WAL replay to distinguish sealed INSERTs from post-seal UPDATEs.
    pub fn has_committed_row(&self, row_id: i64) -> bool {
        self.versions.read().contains_key(row_id)
    }

    /// Atomically subtract sealed rows from committed_row_count.
    ///
    /// After seal batch-removes rows from the B-tree, committed_row_count must
    /// decrease by the sealed amount. Using fetch_sub (not store) preserves
    /// concurrent fetch_add/fetch_sub from other threads committing INSERTs or
    /// DELETEs during the seal window. A plain store() would race with those
    /// concurrent updates, causing the counter to drift by ~N (where N is the
    /// number of rows committed during the seal operation).
    pub fn subtract_committed_row_count(&self, sealed: usize) {
        // Use saturating arithmetic to avoid wrapping if concurrent deletes
        // already reduced the counter below the sealed amount.
        loop {
            let current = self.committed_row_count.load(Ordering::Relaxed);
            let new_val = current.saturating_sub(sealed);
            match self.committed_row_count.compare_exchange_weak(
                current,
                new_val,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
    }

    /// Check if a transaction requires snapshot isolation visibility checks.
    /// When true, the O(1) committed_row_count is inaccurate because it includes
    /// rows committed after this transaction's snapshot point.
    #[inline]
    pub fn needs_snapshot_isolation(&self, txn_id: i64) -> bool {
        self.visibility_checker
            .as_ref()
            .is_some_and(|c| c.needs_snapshot_isolation(txn_id))
    }

    /// Clears all data in O(1) for TRUNCATE.
    /// Drops all versions, arena data, indexes, and resets row count.
    /// Returns the number of rows that were truncated.
    ///
    /// **Non-rollbackable**: Like MySQL/Oracle, data is physically destroyed
    /// immediately with no undo log. This is O(1) vs O(N) for DELETE.
    /// PostgreSQL/SQL Server support rollbackable TRUNCATE by deferring the
    /// physical destruction to commit time, but that adds complexity to every
    /// read path (13+ code locations). Use `DELETE FROM table` for rollback.
    ///
    /// Fails with `TableHasActiveTransactions` if another transaction holds
    /// uncommitted UPDATE/DELETE claims on this table.
    pub fn truncate_all(&self) -> radixdb_core::Result<i32> {
        self.truncate_all_after(|| Ok(()))
    }

    /// Execute a durable TRUNCATE publication callback inside the same
    /// check-and-clear critical section. A callback failure leaves hot state
    /// untouched; once it succeeds, no concurrent commit can repopulate the
    /// version map before it is cleared.
    pub fn truncate_all_after<F>(&self, before_clear: F) -> radixdb_core::Result<i32>
    where
        F: FnOnce() -> radixdb_core::Result<()>,
    {
        let _mutation = self.mutation_guard()?;

        // Hold uncommitted_writes(W) for the ENTIRE check-and-clear sequence
        // to prevent TOCTOU race: without this, a concurrent try_claim_row()
        // could add a claim between the check and the clear, and truncate would
        // silently destroy it — causing a ghost row when the UPDATE commits.
        //
        // Lock ordering: uncommitted_writes(W) → versions(W) → arena(W).
        // This is safe because all read methods that use uncommitted_writes
        // check it BEFORE acquiring arena(R), so there is no circular dependency:
        //   - Read paths: uncommitted_writes(R) [brief, dropped] → arena(R)
        //   - Commit path: versions(W) → arena(W) (never uncommitted_writes(W))
        //   - try_claim_row: uncommitted_writes(W) only — blocked by our lock
        let mut uncommitted = self.uncommitted_writes.write();
        if !uncommitted.is_empty() {
            return Err(radixdb_core::Error::TableHasActiveTransactions);
        }

        // Hold versions(W) before the durable callback and while clearing BOTH
        // versions and arena atomically.
        // INSERT commits acquire versions(W) → arena(W) in the same order,
        // so this blocks them and prevents a race where a commit inserts a
        // version pointing to an arena slot that we then clear.
        // Reset committed_row_count inside the lock so COUNT(*) never sees 0
        // while data still exists.
        let mut versions = self.versions.write();
        before_clear()?;
        let count = self.committed_row_count.swap(0, Ordering::SeqCst) as i32;
        self.committed_hot_bytes.store(0, Ordering::SeqCst);
        versions.clear();
        self.arena.clear_all();
        drop(versions);

        // Clear uncommitted_writes (already held as write lock)
        uncommitted.clear();
        drop(uncommitted);

        // 5. Clear all indexes
        let indexes = self.indexes.read();
        for index in indexes.values() {
            index.clear();
        }

        // 6. Reset auto-increment counter so new rows start at 1
        //    and the speculative arena probe ((row_id - 1) as usize) stays valid
        self.auto_increment_counter.store(0, Ordering::Release);

        // 7. Invalidate zone maps (stale after truncate)
        self.zone_map_generation.fetch_add(1, Ordering::AcqRel);
        *self.zone_maps.write() = None;

        Ok(count)
    }
}
