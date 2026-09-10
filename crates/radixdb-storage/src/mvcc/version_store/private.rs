use super::unique_claims::UniqueClaimKey;
use super::*;

/// Transaction-local version store for uncommitted changes
pub struct TransactionVersionStore {
    /// Local versions for this transaction - stores version history per row for savepoint support
    /// The list is ordered by create_time (oldest first, newest last)
    /// Lazily allocated on first write to avoid allocation overhead for read-only queries.
    /// Uses SmallVec<[RowVersion; 1]> to avoid heap allocation for single-version rows.
    local_versions: Option<I64Map<VersionList>>,
    /// Parent (shared) version store
    pub(super) parent_store: Arc<VersionStore>,
    /// This transaction's ID
    pub(super) txn_id: i64,
    /// Write set for conflict detection
    /// Lazily allocated on first write to avoid allocation overhead for read-only queries
    write_set: Option<I64Map<WriteSetEntry>>,
    /// UNIQUE keys owned by the transaction's current final row view.
    /// This is separate from the retained set so savepoint rollback can restore
    /// local semantics without reopening a key to another transaction.
    pub(super) current_unique_keys: AHashMap<UniqueClaimKey, i64>,
    /// First acquisition timestamp for every UNIQUE key retained by this
    /// transaction. Superseded keys remain claimed across later savepoints,
    /// while a failed statement can release keys it acquired after its own
    /// rollback boundary.
    pub(super) retained_unique_keys: AHashMap<UniqueClaimKey, i64>,
    /// Claims acquired directly for cold-only rows, ordered by first claim
    /// timestamp. Unlike ordinary writes, these may have no local version, so
    /// savepoint rollback cannot infer their lifetime from `local_versions`.
    external_claim_journal: Vec<(i64, i64)>,
    /// Index entries owned by immutable cold rows and scheduled for removal at
    /// commit. Keeping these transaction-private prevents uncommitted
    /// UPDATE/DELETE from creating false negatives for concurrent readers.
    external_index_removals: Vec<ExternalIndexRemoval>,
    /// Transaction-private schema used by staged ALTER TABLE operations.
    /// Other transactions keep reading `parent_store.schema()` until commit.
    schema_override: Option<CompactArc<Schema>>,
    /// Set only while EngineOperations owns the exclusive transactional DDL
    /// fence and is preparing indexes for commit publication.
    index_ddl_authorized: bool,
    /// Indexes built during transactional DDL preparation from this
    /// transaction's complete final row view.
    ///
    /// They are already populated with every local INSERT/UPDATE/DELETE, so
    /// replaying this transaction's ordinary old-to-new index delta into them
    /// would apply the same mutation twice. Keep the exact Arc identities to
    /// distinguish these prepared generations from pre-existing indexes with
    /// the same logical metadata.
    prepared_final_view_indexes: Vec<Arc<dyn Index>>,
    /// Parent indexes staged for DROP by this transaction. They remain visible
    /// to other transactions until commit, but cannot enforce or receive this
    /// transaction's final row deltas.
    disabled_parent_indexes: FxHashSet<String>,
    /// Commit or rollback is terminal for this transaction-local authority.
    terminal: bool,
}

impl TransactionVersionStore {
    /// Creates a new transaction-local version store
    ///
    /// Uses lazy allocation for local_versions and write_set maps to avoid
    /// allocation overhead for read-only queries. These are only allocated
    /// when the first write operation occurs.
    pub fn new(parent_store: Arc<VersionStore>, txn_id: i64) -> Self {
        Self {
            // Lazy allocation - maps are created on first write
            local_versions: None,
            parent_store,
            txn_id,
            write_set: None,
            current_unique_keys: AHashMap::new(),
            retained_unique_keys: AHashMap::new(),
            external_claim_journal: Vec::new(),
            external_index_removals: Vec::new(),
            schema_override: None,
            index_ddl_authorized: false,
            prepared_final_view_indexes: Vec::new(),
            disabled_parent_indexes: FxHashSet::default(),
            terminal: false,
        }
    }

    #[inline]
    fn ensure_active(&self) -> Result<(), Error> {
        if self.terminal {
            Err(Error::TransactionClosed)
        } else {
            Ok(())
        }
    }

    /// Return the transaction-private schema, if ALTER TABLE has staged one.
    pub fn schema_override(&self) -> Option<CompactArc<Schema>> {
        self.schema_override.clone()
    }

    /// Replace the transaction-private schema overlay.
    pub fn set_schema_override(&mut self, schema: CompactArc<Schema>) {
        self.schema_override = Some(schema);
    }

    /// Drop the schema overlay when savepoint rollback removes all staged ALTERs.
    pub fn clear_schema_override(&mut self) {
        self.schema_override = None;
    }

    pub fn set_index_ddl_authorized(&mut self, authorized: bool) {
        self.index_ddl_authorized = authorized;
    }

    pub fn index_ddl_authorized(&self) -> bool {
        self.index_ddl_authorized
    }

    /// Register an index generation that was populated from the complete
    /// transaction-visible row set during commit preparation.
    pub fn stage_prepared_final_view_index(&mut self, index: Arc<dyn Index>) -> Result<(), Error> {
        self.ensure_active()?;
        if let Some(prepared) = self
            .prepared_final_view_indexes
            .iter_mut()
            .find(|prepared| prepared.name().eq_ignore_ascii_case(index.name()))
        {
            // Commit validation is deliberately retryable. A failed attempt
            // leaves the transaction active, so later DML must rebuild this
            // detached generation from the new final view and replace the
            // prior attempt rather than accumulating two same-name Arcs.
            *prepared = index;
        } else {
            self.prepared_final_view_indexes.push(index);
        }
        Ok(())
    }

    pub fn prepared_final_view_index(&self, name: &str) -> Option<Arc<dyn Index>> {
        self.prepared_final_view_indexes
            .iter()
            .find(|index| index.name().eq_ignore_ascii_case(name))
            .cloned()
    }

    pub fn prepared_final_view_indexes(&self) -> Vec<Arc<dyn Index>> {
        self.prepared_final_view_indexes.clone()
    }

    /// Publish every fully built index generation while the transaction owns
    /// the DDL and commit-visibility fences. Validation has already succeeded,
    /// but registry insertion remains fallible, so compensate any prefix before
    /// returning an ordinary pre-commit error.
    pub fn activate_prepared_final_view_indexes(&mut self) -> Result<(), Error> {
        self.ensure_active()?;
        let schema = self
            .schema_override
            .clone()
            .unwrap_or_else(|| self.parent_store.schema());
        let mut activated: Vec<String> = Vec::new();
        for index in &self.prepared_final_view_indexes {
            if let Some(existing) = self.parent_store.get_index(index.name()) {
                if Arc::ptr_eq(&existing, index) {
                    continue;
                }
                for name in activated.iter().rev() {
                    self.parent_store.remove_index(name);
                }
                return Err(Error::IndexAlreadyExists(index.name().to_string()));
            }
            if let Err(error) = self.parent_store.add_index_for_schema(
                index.name().to_string(),
                Arc::clone(index),
                &schema,
            ) {
                for name in activated.iter().rev() {
                    self.parent_store.remove_index(name);
                }
                return Err(error);
            }
            activated.push(index.name().to_string());
        }
        Ok(())
    }

    pub fn deactivate_prepared_final_view_indexes(&mut self) {
        for index in &self.prepared_final_view_indexes {
            if self
                .parent_store
                .get_index(index.name())
                .is_some_and(|registered| Arc::ptr_eq(&registered, index))
            {
                self.parent_store.remove_index(index.name());
            }
        }
    }

    pub fn disable_parent_index(&mut self, name: &str) -> Result<(), Error> {
        self.ensure_active()?;
        self.disabled_parent_indexes.insert(name.to_lowercase());
        self.rebuild_current_unique_keys()
    }

    pub fn reset_disabled_parent_indexes<'a>(
        &mut self,
        names: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), Error> {
        self.ensure_active()?;
        self.disabled_parent_indexes.clear();
        self.disabled_parent_indexes
            .extend(names.into_iter().map(str::to_lowercase));
        self.rebuild_current_unique_keys()
    }

    #[inline]
    pub(super) fn is_parent_index_disabled(&self, index: &Arc<dyn Index>) -> bool {
        self.disabled_parent_indexes
            .contains(&index.name().to_lowercase())
    }

    pub fn disabled_parent_index_names(&self) -> FxHashSet<String> {
        self.disabled_parent_indexes.clone()
    }

    #[inline]
    pub(super) fn is_prepared_final_view_index(&self, index: &Arc<dyn Index>) -> bool {
        self.prepared_final_view_indexes
            .iter()
            .any(|prepared| Arc::ptr_eq(prepared, index))
    }

    /// Rewrite every transaction-local row snapshot after a physical column
    /// drop. The engine calls this for all active stores while holding the
    /// exclusive DDL fence, so a later commit cannot republish the old layout.
    pub fn remove_column_from_local_versions(&mut self, column_index: usize) {
        if let Some(local_versions) = &mut self.local_versions {
            for (_, history) in local_versions.iter_mut() {
                for version in history {
                    version.data.remove_column(column_index);
                }
            }
        }
        if let Some(write_set) = &mut self.write_set {
            for (_, entry) in write_set.iter_mut() {
                if let Some(version) = &mut entry.read_version {
                    version.data.remove_column(column_index);
                }
            }
        }
    }

    /// Returns the transaction ID
    pub fn txn_id(&self) -> i64 {
        self.txn_id
    }

    /// Read-only access to local versions for rollback cleanup.
    pub fn local_versions_ref(&self) -> Option<&I64Map<VersionList>> {
        self.local_versions.as_ref()
    }

    /// Read-only access to write set for commit-time cold revalidation.
    pub fn write_set_ref(&self) -> Option<&I64Map<WriteSetEntry>> {
        self.write_set.as_ref()
    }

    /// Ensures local_versions map is allocated, returning a mutable reference.
    /// Uses pooled maps when available to reduce allocation overhead.
    #[inline]
    fn ensure_local_versions(&mut self) -> &mut I64Map<VersionList> {
        self.local_versions.get_or_insert_with(get_version_list_map)
    }

    /// Ensures write_set map is allocated, returning a mutable reference.
    /// Uses pooled maps when available to reduce allocation overhead.
    #[inline]
    fn ensure_write_set(&mut self) -> &mut I64Map<WriteSetEntry> {
        self.write_set.get_or_insert_with(get_write_set_map)
    }

    /// Put adds or updates a row in the transaction's local store
    pub fn put(&mut self, row_id: i64, data: Row, is_delete: bool) -> Result<(), Error> {
        self.put_internal(row_id, data, is_delete, true)
    }

    fn put_internal(
        &mut self,
        row_id: i64,
        data: Row,
        is_delete: bool,
        reserve_unique_keys: bool,
    ) -> Result<(), Error> {
        self.ensure_active()?;
        // Convert to Shared (Arc) storage immediately for efficient Arc sharing:
        // - get_arc() will return cheap Arc clones (no value cloning)
        // - into_arc() at commit time returns the existing Arc (no clone)
        let data = Row::from_arc(data.into_arc());

        // Get timestamp once at the start (avoids calling SystemTime::now() inside RowVersion::new)
        let timestamp = get_fast_timestamp();

        // Create the row version with pre-computed timestamp
        let mut rv = RowVersion::new_with_timestamp(self.txn_id, data, timestamp);
        if is_delete {
            rv.deleted_at_txn_id = self.txn_id;
        }

        // Check if we already have a local version for this row
        let has_local = self
            .local_versions
            .as_ref()
            .is_some_and(|lv| lv.contains_key(row_id));

        if has_local {
            if reserve_unique_keys {
                let proposed = (!is_delete).then(|| rv.data.clone());
                self.reserve_unique_keys_for_rows(&[(row_id, proposed)])?;
            }
            // Already have local version - just append
            self.ensure_local_versions()
                .get_mut(row_id)
                .unwrap()
                .push(rv);
        } else {
            // New row - need to check write-set and parent store
            let needs_write_set_entry = self
                .write_set
                .as_ref()
                .is_none_or(|ws| !ws.contains_key(row_id));

            if needs_write_set_entry {
                let read_version = self.parent_store.get_visible_version(row_id, self.txn_id);
                let row_exists = read_version.is_some();

                // A failed claim must not leave a write-set entry that later
                // commit/rollback mistakes for an owned row.
                if row_exists {
                    self.parent_store.try_claim_row(row_id, self.txn_id)?;
                }
                if reserve_unique_keys {
                    let proposed = (!is_delete).then(|| rv.data.clone());
                    if let Err(error) = self.reserve_unique_keys_for_rows(&[(row_id, proposed)]) {
                        if row_exists {
                            self.parent_store.release_row_claim(row_id, self.txn_id);
                        }
                        return Err(error);
                    }
                }
                self.ensure_write_set()
                    .insert(row_id, WriteSetEntry::from_read(read_version));
            } else if reserve_unique_keys {
                let proposed = (!is_delete).then(|| rv.data.clone());
                self.reserve_unique_keys_for_rows(&[(row_id, proposed)])?;
            }
            self.ensure_local_versions().insert(row_id, smallvec![rv]);
        }
        Ok(())
    }

    /// Batch put for UPDATE operations where we already have the row data
    ///
    /// This is used for rows that are already tracked in local_versions (updates within same txn)
    /// or when we don't have pre-fetched original versions.
    pub fn put_batch_for_update(&mut self, rows: RowVec) -> Result<(), Error> {
        self.ensure_active()?;
        let statement_boundary = get_fast_timestamp();
        let proposed: Vec<_> = rows
            .iter()
            .map(|(row_id, row)| (*row_id, Some(row.clone())))
            .collect();
        self.reserve_unique_keys_for_rows(&proposed)?;
        for (row_id, data) in rows {
            if let Err(error) = self.put_internal(row_id, data, false, false) {
                self.rollback_to_timestamp(statement_boundary);
                return Err(error);
            }
        }
        Ok(())
    }

    /// Optimized single-row put with pre-fetched original version
    ///
    /// This avoids redundant get_visible_version() calls by accepting the original
    /// version that was already fetched during the read phase.
    /// Used for PK-based UPDATE operations.
    #[inline]
    pub fn put_with_original(
        &mut self,
        row_id: i64,
        data: Row,
        original_version: RowVersion,
        is_delete: bool,
    ) -> Result<(), Error> {
        self.put_with_original_internal(row_id, data, original_version, is_delete, true)
    }

    fn put_with_original_internal(
        &mut self,
        row_id: i64,
        data: Row,
        original_version: RowVersion,
        is_delete: bool,
        reserve_unique_keys: bool,
    ) -> Result<(), Error> {
        self.ensure_active()?;
        // Convert to Shared (Arc) storage immediately for efficient Arc sharing
        let data = Row::from_arc(data.into_arc());

        // Get timestamp once at the start (avoids calling SystemTime::now() inside RowVersion::new)
        let timestamp = get_fast_timestamp();

        // Create the new row version with pre-computed timestamp
        let mut rv = RowVersion::new_with_timestamp(self.txn_id, data, timestamp);
        if is_delete {
            rv.deleted_at_txn_id = self.txn_id;
        }

        // Check if we already have a local version for this row
        let has_local = self
            .local_versions
            .as_ref()
            .is_some_and(|lv| lv.contains_key(row_id));

        if has_local {
            if reserve_unique_keys {
                let proposed = (!is_delete).then(|| rv.data.clone());
                self.reserve_unique_keys_for_rows(&[(row_id, proposed)])?;
            }
            // Already have local version - just append
            self.ensure_local_versions()
                .get_mut(row_id)
                .unwrap()
                .push(rv);
        } else {
            // Track in write-set using the pre-fetched original version
            let needs_write_set_entry = self
                .write_set
                .as_ref()
                .is_none_or(|ws| !ws.contains_key(row_id));

            if needs_write_set_entry {
                // Claim first; journal mutation is published only after
                // ownership succeeds.
                self.parent_store.try_claim_row(row_id, self.txn_id)?;
                if reserve_unique_keys {
                    let proposed = (!is_delete).then(|| rv.data.clone());
                    if let Err(error) = self.reserve_unique_keys_for_rows(&[(row_id, proposed)]) {
                        self.parent_store.release_row_claim(row_id, self.txn_id);
                        return Err(error);
                    }
                }
                self.ensure_write_set()
                    .insert(row_id, WriteSetEntry::from_read(Some(original_version)));
            } else if let Some(entry) = self
                .write_set
                .as_mut()
                .and_then(|write_set| write_set.get_mut(row_id))
            {
                // A statement may claim candidates before reading them so it
                // can re-read the winner's committed value after waiting. Fill
                // the previously empty external-claim placeholder with that
                // post-claim original version for OCC/index bookkeeping.
                if entry.read_version.is_none() {
                    entry.read_version = Some(original_version);
                    entry.observation = WriteObservation::HotVersion;
                }
                if reserve_unique_keys {
                    let proposed = (!is_delete).then(|| rv.data.clone());
                    self.reserve_unique_keys_for_rows(&[(row_id, proposed)])?;
                }
            }
            self.ensure_local_versions().insert(row_id, smallvec![rv]);
        }
        Ok(())
    }

    /// Optimized batch put for UPDATE operations with pre-fetched original versions
    ///
    /// This avoids redundant get_visible_version() calls by accepting the original
    /// versions that were already fetched during the batch read.
    ///
    /// Parameters:
    /// - rows: Vec of (row_id, new_row_data, original_version)
    pub fn put_batch_with_originals(
        &mut self,
        rows: Vec<(i64, Row, RowVersion)>,
    ) -> Result<(), Error> {
        self.ensure_active()?;
        let statement_boundary = get_fast_timestamp();
        let now = get_fast_timestamp();
        let proposed: Vec<_> = rows
            .iter()
            .map(|(row_id, row, _)| (*row_id, Some(row.clone())))
            .collect();
        self.reserve_unique_keys_for_rows(&proposed)?;

        for (row_id, data, original_version) in rows {
            // Convert to Shared (Arc) storage immediately for efficient Arc sharing
            let data = Row::from_arc(data.into_arc());

            // Create the new row version with pre-computed timestamp (avoids wasteful
            // get_fast_timestamp() call inside RowVersion::new that would be overwritten)
            let rv = RowVersion::new_with_timestamp(self.txn_id, data, now);

            // Check if already in local versions (already processed in this transaction)
            if let Some(versions) = self.ensure_local_versions().get_mut(row_id) {
                // Append new version to history
                versions.push(rv);
                continue;
            }

            // Track in write-set using the pre-fetched original version
            let needs_write_set_entry = self
                .write_set
                .as_ref()
                .is_none_or(|ws| !ws.contains_key(row_id));

            if needs_write_set_entry {
                if let Err(error) = self.parent_store.try_claim_row(row_id, self.txn_id) {
                    self.rollback_to_timestamp(statement_boundary);
                    return Err(error);
                }
                self.ensure_write_set()
                    .insert(row_id, WriteSetEntry::from_read(Some(original_version)));
            } else if let Some(entry) = self
                .write_set
                .as_mut()
                .and_then(|write_set| write_set.get_mut(row_id))
            {
                if entry.read_version.is_none() {
                    entry.read_version = Some(original_version);
                    entry.observation = WriteObservation::HotVersion;
                }
            }

            // Insert new version history for this row
            self.ensure_local_versions().insert(row_id, smallvec![rv]);
        }
        Ok(())
    }

    /// Optimized batch delete for DELETE operations
    ///
    /// This marks multiple rows as deleted in a single operation, avoiding
    /// the overhead of individual put() calls with lock acquisitions per row.
    ///
    /// Parameters:
    /// - rows: RowVec of (row_id, row_data) to mark as deleted
    pub fn put_batch_deleted(&mut self, rows: RowVec) -> Result<(), Error> {
        self.ensure_active()?;
        let statement_boundary = get_fast_timestamp();
        // Get timestamp once for all rows in the batch
        let timestamp = get_fast_timestamp();
        let proposed: Vec<_> = rows.iter().map(|(row_id, _)| (*row_id, None)).collect();
        self.reserve_unique_keys_for_rows(&proposed)?;

        for (row_id, data) in rows {
            // Check if we already have a local version
            let has_local = self
                .local_versions
                .as_ref()
                .is_some_and(|lv| lv.contains_key(row_id));

            if !has_local {
                // Check if this row exists in parent store and track in write-set
                let needs_write_set_entry = self
                    .write_set
                    .as_ref()
                    .is_none_or(|ws| !ws.contains_key(row_id));

                if needs_write_set_entry {
                    let read_version = self.parent_store.get_visible_version(row_id, self.txn_id);
                    let row_exists = read_version.is_some();

                    if row_exists {
                        if let Err(error) = self.parent_store.try_claim_row(row_id, self.txn_id) {
                            self.rollback_to_timestamp(statement_boundary);
                            return Err(error);
                        }
                    }
                    self.ensure_write_set()
                        .insert(row_id, WriteSetEntry::from_read(read_version));
                }
            }

            // Create deleted row version with pre-computed timestamp
            let mut rv = RowVersion::new_with_timestamp(self.txn_id, data, timestamp);
            rv.deleted_at_txn_id = self.txn_id;

            // Append to version history for this row
            let local_versions = self.ensure_local_versions();
            if let Some(versions) = local_versions.get_mut(row_id) {
                versions.push(rv);
            } else {
                local_versions.insert(row_id, smallvec![rv]);
            }
        }
        Ok(())
    }

    /// Optimized batch delete with pre-fetched original versions
    ///
    /// This avoids redundant get_visible_version() calls by accepting the original
    /// versions that were already fetched during the read phase.
    /// Used for PK range DELETE operations.
    pub fn put_batch_deleted_with_originals(
        &mut self,
        rows: Vec<(i64, Row, RowVersion)>,
    ) -> Result<(), Error> {
        self.ensure_active()?;
        let statement_boundary = get_fast_timestamp();
        // Get timestamp once for all rows in the batch
        let timestamp = get_fast_timestamp();
        let proposed: Vec<_> = rows.iter().map(|(row_id, _, _)| (*row_id, None)).collect();
        self.reserve_unique_keys_for_rows(&proposed)?;

        for (row_id, data, original_version) in rows {
            // Create deleted row version with pre-computed timestamp
            let mut rv = RowVersion::new_with_timestamp(self.txn_id, data, timestamp);
            rv.deleted_at_txn_id = self.txn_id;

            // Check if already in local versions (already processed in this transaction)
            if let Some(versions) = self.ensure_local_versions().get_mut(row_id) {
                versions.push(rv);
                continue;
            }

            // Track in write-set using the pre-fetched original version
            let needs_write_set_entry = self
                .write_set
                .as_ref()
                .is_none_or(|ws| !ws.contains_key(row_id));

            if needs_write_set_entry {
                if let Err(error) = self.parent_store.try_claim_row(row_id, self.txn_id) {
                    self.rollback_to_timestamp(statement_boundary);
                    return Err(error);
                }
                self.ensure_write_set()
                    .insert(row_id, WriteSetEntry::from_read(Some(original_version)));
            } else if let Some(entry) = self
                .write_set
                .as_mut()
                .and_then(|write_set| write_set.get_mut(row_id))
            {
                if entry.read_version.is_none() {
                    entry.read_version = Some(original_version);
                    entry.observation = WriteObservation::HotVersion;
                }
            }

            // Insert deleted version for this row
            self.ensure_local_versions().insert(row_id, smallvec![rv]);
        }
        Ok(())
    }

    /// Check if we have local changes for a row
    pub fn has_locally_seen(&self, row_id: i64) -> bool {
        self.local_versions
            .as_ref()
            .is_some_and(|lv| lv.contains_key(row_id))
    }

    /// Returns true if this transaction has any uncommitted local changes
    pub fn has_local_changes(&self) -> bool {
        self.local_versions
            .as_ref()
            .is_some_and(|lv| !lv.is_empty())
            || !self.external_index_removals.is_empty()
    }

    /// Stage an index removal for a cold row without mutating shared index
    /// state. The ordinary commit transition applies it together with all hot
    /// index additions/removals and uses the same compensation path on error.
    pub fn stage_external_index_removal(
        &mut self,
        index: Arc<dyn Index>,
        values: Vec<Value>,
        row_id: i64,
    ) -> Result<(), Error> {
        self.ensure_active()?;
        if self.external_index_removals.iter().any(|entry| {
            entry.row_id == row_id && entry.index.name() == index.name() && entry.values == values
        }) {
            return Ok(());
        }
        self.external_index_removals.push(ExternalIndexRemoval {
            index,
            values,
            row_id,
        });
        Ok(())
    }

    /// Returns the number of local changes (distinct row IDs)
    pub fn local_count(&self) -> usize {
        self.local_versions.as_ref().map_or(0, |lv| lv.len())
    }

    /// Get the latest local version for a specific row, if any.
    #[inline]
    pub fn get_latest_local(&self, row_id: i64) -> Option<&RowVersion> {
        self.local_versions
            .as_ref()
            .and_then(|lv| lv.get(row_id))
            .and_then(|versions| versions.last())
    }

    /// Iterate over local versions (returns most recent version per row)
    pub fn iter_local(&self) -> impl Iterator<Item = (i64, &RowVersion)> {
        self.local_versions
            .iter()
            .flat_map(|lv| lv.iter())
            .filter_map(|(k, versions)| versions.last().map(|v| (k, v)))
    }

    /// Iterate over local versions with their original (old) versions for index updates
    /// Returns (row_id, new_version, old_row_option)
    pub fn iter_local_with_old(&self) -> impl Iterator<Item = (i64, &RowVersion, Option<&Row>)> {
        let write_set_ref = self.write_set.as_ref();
        self.local_versions
            .iter()
            .flat_map(|lv| lv.iter())
            .filter_map(move |(row_id, versions)| {
                versions.last().map(|version| {
                    let old_row = write_set_ref
                        .and_then(|ws| ws.get(row_id))
                        .and_then(|entry| entry.read_version.as_ref())
                        .filter(|v| !v.is_deleted())
                        .map(|v| &v.data);
                    (row_id, version, old_row)
                })
            })
    }

    /// Get the local version for a row (without checking parent)
    /// Returns the most recent version in the transaction's history
    pub fn get_local_version(&self, row_id: i64) -> Option<&RowVersion> {
        self.local_versions
            .as_ref()
            .and_then(|lv| lv.get(row_id))
            .and_then(|versions| versions.last())
    }

    /// Get a row, checking local versions first then parent store
    pub fn get(&self, row_id: i64) -> Option<Row> {
        if self.terminal {
            return None;
        }
        // Check local versions first (get most recent)
        if let Some(lv) = self.local_versions.as_ref() {
            if let Some(versions) = lv.get(row_id) {
                if let Some(local_version) = versions.last() {
                    if local_version.is_deleted() {
                        return None;
                    }
                    return Some(local_version.data.clone());
                }
            }
        }

        // Check parent store
        self.parent_store
            .get_visible_version(row_id, self.txn_id)
            .map(|v| v.data.clone())
    }

    /// OCC validation that tolerates seal-removed rows.
    ///
    /// If a row is missing from the hot B-tree, it was moved to cold by seal.
    /// This is not a conflict for READ COMMITTED transactions. The transaction
    /// read the row data into its local txn_versions before seal removed it.
    /// On commit, the updated version is re-inserted into hot, and the
    /// skip-set dedup mechanism ensures the stale cold version is shadowed.
    /// Seal also skips rows claimed by active transactions (uncommitted_writes).
    ///
    /// INSERT provenance is explicit and remains `Absent` even if the same
    /// transaction subsequently updates/deletes its new row. A missing
    /// read_version alone is not enough: cold and residual candidates can be
    /// claimed as existing without materializing a hot version.
    pub fn detect_conflicts_safe(&self) -> Result<(), Error> {
        let Some(write_set) = self.write_set.as_ref() else {
            return Ok(());
        };

        for (row_id, write_entry) in write_set.iter() {
            match write_entry.observation {
                WriteObservation::HotVersion => {
                    let Some(read_version) = &write_entry.read_version else {
                        return Err(Error::internal(format!(
                            "invalid write-set provenance: row {} has HotVersion without payload",
                            row_id
                        )));
                    };
                    // UPDATE path: check that the row hasn't been modified concurrently
                    match self.parent_store.get_latest_version_id(row_id) {
                        Some(latest_txn_id) if latest_txn_id != read_version.txn_id => {
                            return Err(Error::internal(format!(
                                "write conflict: row {} was modified by another transaction",
                                row_id
                            )));
                        }
                        Some(_) => {}
                        None => {
                            // Seal moved the row from hot to immutable cold
                            // storage. The claim still serializes its writer.
                        }
                    }
                }
                WriteObservation::Absent => {
                    // INSERT path: guard TOCTOU races for explicit PK values.
                    if self
                        .parent_store
                        .has_any_visible_version(&[row_id], self.txn_id)
                        .is_some()
                    {
                        return Err(Error::internal(format!(
                            "write conflict: row {} was concurrently inserted by another transaction",
                            row_id
                        )));
                    }
                }
                WriteObservation::ClaimedExisting => {
                    // Claim acquisition serialized this previously visible
                    // candidate before residual recheck. No local version is
                    // required when it did not match or remained cold.
                }
            }
        }

        Ok(())
    }

    /// Prepare commit - returns list of versions to commit (most recent per row)
    pub fn prepare_commit(&self) -> Vec<(i64, RowVersion)> {
        let Some(local_versions) = self.local_versions.as_ref() else {
            return Vec::new();
        };

        let mut versions = Vec::new();
        for (row_id, version_history) in local_versions.iter() {
            // Only commit the most recent version per row
            if let Some(version) = version_history.last() {
                versions.push((row_id, version.clone()));
            }
        }
        versions
    }

    /// Validate every fallible MVCC/index constraint without publishing data.
    ///
    /// The engine calls this for all touched tables while holding their ordered
    /// commit membership fences. Only after every table passes may the first
    /// table mutate an index or publish a row version. This is the cross-table
    /// atomicity boundary that prevents a late UNIQUE conflict from leaving an
    /// earlier table committed.
    pub fn validate_commit(&self) -> Result<(), Error> {
        self.detect_conflicts_safe()?;
        self.validate_unique_indexes_on_commit()
    }

    fn validate_unique_indexes_on_commit(&self) -> Result<(), Error> {
        let local_versions = self.local_versions.as_ref();
        if local_versions.is_none_or(I64Map::is_empty) && self.external_index_removals.is_empty() {
            return Ok(());
        }

        let mut indexes: Vec<Arc<dyn Index>> = self
            .parent_store
            .get_all_indexes()
            .into_iter()
            .filter(|index| {
                index.is_unique()
                    && !self.is_prepared_final_view_index(index)
                    && !self.is_parent_index_disabled(index)
            })
            .collect();
        indexes.sort_by(|left, right| left.name().cmp(right.name()));

        for index in indexes {
            let mut removals = I64Set::new();
            let mut additions: Vec<(i64, Vec<radixdb_core::Value>)> = Vec::new();

            for removal in &self.external_index_removals {
                if removal.index.name() == index.name() {
                    removals.insert(removal.row_id);
                }
            }

            for (row_id, versions) in local_versions.into_iter().flat_map(|rows| rows.iter()) {
                let Some(new_version) = versions.last() else {
                    continue;
                };
                let old_row = self
                    .write_set
                    .as_ref()
                    .and_then(|write_set| write_set.get(row_id))
                    .and_then(|entry| entry.read_version.as_ref())
                    .map(|version| &version.data);

                if let Some(old_row) = old_row {
                    if !new_version.is_deleted()
                        && !index_affecting_columns_changed(
                            index.as_ref(),
                            old_row,
                            &new_version.data,
                        )
                    {
                        continue;
                    }
                    if index_values_for_row(index.as_ref(), old_row)?.is_some() {
                        removals.insert(row_id);
                    }
                }

                if !new_version.is_deleted() {
                    if let Some(values) = index_values_for_row(index.as_ref(), &new_version.data)? {
                        if !values.iter().any(|value| value.is_null()) {
                            additions.push((row_id, values));
                        }
                    }
                }
            }

            let mut seen: ahash::AHashMap<Vec<radixdb_core::Value>, i64> =
                ahash::AHashMap::with_capacity(additions.len());
            let is_hnsw = index.index_type() == radixdb_core::IndexType::Hnsw;

            for (row_id, values) in additions {
                if let Some(existing_row_id) = seen.insert(values.clone(), row_id) {
                    if existing_row_id != row_id {
                        return Err(Error::unique_constraint(
                            index.name(),
                            index.column_names().join(", "),
                            format!("{:?}", values),
                        ));
                    }
                }

                if is_hnsw {
                    let Some(hnsw) = index.as_any().downcast_ref::<crate::index::HnswIndex>()
                    else {
                        return Err(Error::internal(format!(
                            "index '{}' advertised HNSW type but cannot be downcast",
                            index.name()
                        )));
                    };
                    if let Some(value) = values.first() {
                        if let Some(conflict) =
                            hnsw.find_exact_duplicate(value, row_id, Some(&removals))
                        {
                            return Err(Error::unique_constraint(
                                index.name(),
                                index.column_names().join(", "),
                                format!("{:?} conflicts with row_id {}", values, conflict),
                            ));
                        }
                    }
                } else if index
                    .get_row_ids_equal(&values)?
                    .iter()
                    .any(|conflict| *conflict != row_id && !removals.contains(*conflict))
                {
                    return Err(Error::unique_constraint(
                        index.name(),
                        index.column_names().join(", "),
                        format!("{:?}", values),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Commit local changes to parent store
    ///
    /// Performance: This method drains local_versions to take ownership of
    /// RowVersion values, avoiding expensive clones. The transaction is
    /// consumed after commit anyway, so this is safe.
    pub fn commit(&mut self) -> Result<(), Error> {
        self.commit_retaining_claims()?;
        self.release_committed_claims();
        Ok(())
    }

    /// Commit local versions while retaining row claims until the transaction
    /// registry publishes the commit.
    ///
    /// The engine uses this variant to make version publication and waiter
    /// wake-up one ordered boundary. Direct `MVCCTable` users keep the historic
    /// `commit()` contract, which releases claims immediately because they do
    /// not have an external transaction-registry finalization phase.
    pub fn commit_retaining_claims(&mut self) -> Result<(), Error> {
        self.ensure_active()?;
        let _mutation = self.parent_store.mutation_guard()?;
        // OCC validation: detect concurrent write conflicts.
        // Rows removed by seal (missing from hot B-tree) are not conflicts —
        // they were moved to cold segments, not modified by another transaction.
        self.detect_conflicts_safe()?;

        // Update indexes BEFORE committing versions
        self.update_indexes_on_commit()?;
        self.external_index_removals.clear();
        self.prepared_final_view_indexes.clear();
        self.disabled_parent_indexes.clear();

        // Commit local versions to parent store
        if let Some(local_versions) = self.local_versions.as_mut() {
            if local_versions.len() == 1 {
                // Single-row fast path: avoid Vec allocation
                if let Some((row_id, mut versions)) = local_versions.drain().next() {
                    if let Some(version) = versions.pop() {
                        self.parent_store.add_version_single_inner(row_id, version);
                    }
                }
            } else {
                // Multi-row path: collect into Vec
                let mut batch: Vec<(i64, RowVersion)> = local_versions
                    .drain()
                    .filter_map(|(row_id, mut versions)| versions.pop().map(|v| (row_id, v)))
                    .collect();

                // Sort by row_id to ensure deterministic locking order
                batch.sort_by_key(|(row_id, _)| *row_id);

                self.parent_store.add_versions_batch_inner(batch);
            }
        }

        // Claims deliberately remain owned here. The registry has not yet made
        // this transaction visible; waking a waiter now would let it re-read
        // the previous committed version. EngineOperations drops the retained
        // TransactionVersionStore only after `complete_commit`, and Drop then
        // releases/notifies all claims as one publication boundary.

        self.terminal = true;
        Ok(())
    }

    fn release_committed_claims(&mut self) {
        if let Some(write_set) = self.write_set.as_mut() {
            if write_set.len() == 1 {
                if let Some((row_id, _)) = write_set.drain().next() {
                    self.parent_store.release_row_claim(row_id, self.txn_id);
                }
            } else {
                let mut row_ids: Vec<i64> = write_set.drain().map(|(row_id, _)| row_id).collect();
                row_ids.sort_unstable();
                self.parent_store
                    .release_row_claims_batch(&row_ids, self.txn_id);
            }
        }
        self.external_claim_journal.clear();
        self.release_retained_unique_keys();
    }

    /// Update indexes during commit
    ///
    /// This method updates all indexes with the changes from this transaction.
    /// For each row being committed:
    /// - If there's an old version (UPDATE/DELETE), remove the old indexed values
    /// - If the new version is not deleted (INSERT/UPDATE), add the new indexed values
    ///
    /// Uses two paths:
    /// - Single-row fast path: SmallVec + immediate apply (zero heap allocation for 1-2 column indexes)
    /// - Multi-row batch path: Collects changes, then applies in batch (reduces lock acquisitions)
    ///
    /// Returns an error if a unique constraint is violated.
    fn update_indexes_on_commit(&self) -> Result<(), Error> {
        #[cfg(feature = "test-mutations")]
        if crate::test_mutations::index_publication_disabled() {
            return Ok(());
        }

        let local_versions = self.local_versions.as_ref();
        if local_versions.is_none_or(I64Map::is_empty) && self.external_index_removals.is_empty() {
            return Ok(());
        }

        // Get all indexes - early exit if none
        let indexes: SmallVec<[Arc<dyn Index>; 4]> = self
            .parent_store
            .get_all_indexes()
            .into_iter()
            .filter(|index| {
                !self.is_prepared_final_view_index(index) && !self.is_parent_index_disabled(index)
            })
            .collect();
        if indexes.is_empty() {
            return Ok(());
        }

        // FAST PATH: Single-row commit (most common case for auto-commit INSERT/UPDATE/DELETE)
        // Uses SmallVec to avoid heap allocation for 1-2 column indexes
        if self.external_index_removals.is_empty()
            && local_versions.is_some_and(|versions| versions.len() == 1)
        {
            return self.update_indexes_single_row(&indexes);
        }

        // BATCH PATH: Multi-row commit
        // Sort indexes by name for deterministic lock ordering (prevents deadlocks)
        let mut indexes: Vec<_> = indexes.into_vec();
        indexes.sort_by(|a, b| a.name().cmp(b.name()));

        let num_indexes = indexes.len();

        // Pre-allocate per-index batch vectors
        let mut add_batches: Vec<Vec<(i64, Vec<radixdb_core::Value>)>> =
            (0..num_indexes).map(|_| Vec::new()).collect();
        let mut remove_batches: Vec<Vec<(i64, Vec<radixdb_core::Value>)>> =
            (0..num_indexes).map(|_| Vec::new()).collect();

        // Collect index updates for each row
        for (row_id, versions) in local_versions.into_iter().flat_map(|rows| rows.iter()) {
            // Get the latest version for this row (last in the list)
            let Some(new_version) = versions.last() else {
                continue;
            };

            let is_deleted = new_version.is_deleted();
            let new_row = &new_version.data;

            // Get old version from write_set (if exists)
            let old_row: Option<&radixdb_core::Row> = self
                .write_set
                .as_ref()
                .and_then(|ws| ws.get(row_id))
                .and_then(|entry| entry.read_version.as_ref())
                .map(|rv| &rv.data);

            for (idx, index) in indexes.iter().enumerate() {
                // OPTIMIZATION: For UPDATEs, check if any indexed column changed
                // BEFORE allocating Vecs
                if let Some(old_r) = old_row {
                    if !is_deleted {
                        // UPDATE case: check if indexed columns differ
                        if !index_affecting_columns_changed(index.as_ref(), old_r, new_row) {
                            // Indexed and predicate columns unchanged - skip this index
                            continue;
                        }

                        if let Some(old_values) = index_values_for_row(index.as_ref(), old_r)? {
                            remove_batches[idx].push((row_id, old_values));
                        }
                        if let Some(new_values) = index_values_for_row(index.as_ref(), new_row)? {
                            add_batches[idx].push((row_id, new_values));
                        }
                        continue;
                    }
                }

                if is_deleted {
                    // DELETE: collect values to remove from index
                    // Use old_row if available, otherwise fall back to new_row
                    let source_row = old_row.unwrap_or(new_row);
                    if let Some(values_to_remove) =
                        index_values_for_row(index.as_ref(), source_row)?
                    {
                        remove_batches[idx].push((row_id, values_to_remove));
                    }
                } else {
                    // INSERT: collect values to add to index
                    // (old_row.is_some() cases with !is_deleted are handled above with continue)
                    if let Some(new_values) = index_values_for_row(index.as_ref(), new_row)? {
                        add_batches[idx].push((row_id, new_values));
                    }
                }
            }
        }

        for removal in &self.external_index_removals {
            let Some(index_idx) = indexes
                .iter()
                .position(|index| index.name() == removal.index.name())
            else {
                return Err(Error::internal(format!(
                    "staged cold index removal references missing index '{}'",
                    removal.index.name()
                )));
            };
            remove_batches[index_idx].push((removal.row_id, removal.values.clone()));
        }

        // PHASE 1: Pre-validate ALL unique indexes before modifying ANY index
        // This prevents index pollution when a later index fails
        for (idx, index) in indexes.iter().enumerate() {
            if !index.is_unique() || add_batches[idx].is_empty() {
                continue;
            }

            // Build a set of row_ids being removed for O(1) conflict resolution
            // For unique indexes, if a row_id is being removed, its current value is being removed.
            // We don't need to track values - just knowing the row_id is sufficient.
            let removals_set: I64Set = remove_batches[idx]
                .iter()
                .map(|(row_id, _)| *row_id)
                .collect();

            // Check for intra-batch duplicates first
            let mut seen: ahash::AHashMap<&[radixdb_core::Value], i64> =
                ahash::AHashMap::with_capacity(add_batches[idx].len());

            let is_hnsw = index.index_type() == radixdb_core::IndexType::Hnsw;

            for (row_id, values) in &add_batches[idx] {
                // Skip NULLs - they don't violate uniqueness
                if values.iter().any(|v| v.is_null()) {
                    continue;
                }

                // Check intra-batch duplicates (applies to all index types, including HNSW)
                if let Some(&existing_row_id) = seen.get(values.as_slice()) {
                    if existing_row_id != *row_id {
                        let values_str: Vec<String> =
                            values.iter().map(|v| format!("{:?}", v)).collect();
                        return Err(Error::unique_constraint(
                            index.name(),
                            index.column_names().join(", "),
                            format!("[{}]", values_str.join(", ")),
                        ));
                    }
                }
                seen.insert(values.as_slice(), *row_id);

                if is_hnsw {
                    // HNSW uniqueness: use exact vector-byte duplicate check.
                    // This is metric-independent and avoids threshold-based false negatives.
                    let Some(hnsw_index) = index.as_any().downcast_ref::<crate::index::HnswIndex>()
                    else {
                        return Err(Error::internal(format!(
                            "index '{}' advertised HNSW type but cannot be downcast",
                            index.name()
                        )));
                    };

                    if let Some(value) = values.first() {
                        if let Some(existing_row_id) =
                            hnsw_index.find_exact_duplicate(value, *row_id, Some(&removals_set))
                        {
                            let dims = value.as_vector_f32().map_or(0, |v| v.len());
                            return Err(Error::unique_constraint(
                                index.name(),
                                index.column_names().join(", "),
                                format!(
                                    "<vector({} dims)> conflicts with row_id {}",
                                    dims, existing_row_id
                                ),
                            ));
                        }
                    }
                } else {
                    // Non-HNSW: standard equality-based uniqueness check
                    // Check against existing index entries
                    let existing = index.get_row_ids_equal(values)?;

                    // Identify potential conflicts (exclude self and rows being removed)
                    // O(1) lookup using removals_set instead of O(N) scan
                    let has_real_conflict = existing.iter().any(|&conflict_id| {
                        conflict_id != *row_id && !removals_set.contains(conflict_id)
                    });

                    if has_real_conflict {
                        let values_str: Vec<String> =
                            values.iter().map(|v| format!("{:?}", v)).collect();
                        return Err(Error::unique_constraint(
                            index.name(),
                            index.column_names().join(", "),
                            format!("[{}]", values_str.join(", ")),
                        ));
                    }
                }
            }
        }

        // PHASE 2: All validations passed - now modify all indexes
        // Track modifications for rollback if a later index fails (race condition protection)
        // Between Phase 1 validation and Phase 2 modification, another transaction could commit,
        // causing a unique constraint violation that wasn't detected in Phase 1.
        let mut completed_removals: Vec<usize> = Vec::new();
        let mut completed_additions: Vec<usize> = Vec::new();

        for (idx, index) in indexes.iter().enumerate() {
            // Remove old entries first (for UPDATE correctness)
            if !remove_batches[idx].is_empty() {
                let batch: Vec<(i64, &[radixdb_core::Value])> = remove_batches[idx]
                    .iter()
                    .map(|(row_id, values)| (*row_id, values.as_slice()))
                    .collect();
                if let Err(error) = index.remove_batch_slice(&batch) {
                    let rollback = Self::rollback_index_batches(
                        &indexes,
                        &add_batches,
                        &remove_batches,
                        &completed_additions,
                        &completed_removals,
                    );
                    return Err(Self::index_error_with_rollback(error, rollback));
                }
                completed_removals.push(idx);
            }

            // Add new entries
            if !add_batches[idx].is_empty() {
                let batch: Vec<(i64, &[radixdb_core::Value])> = add_batches[idx]
                    .iter()
                    .map(|(row_id, values)| (*row_id, values.as_slice()))
                    .collect();

                if let Err(e) = index.add_batch_slice(&batch) {
                    let rollback = Self::rollback_index_batches(
                        &indexes,
                        &add_batches,
                        &remove_batches,
                        &completed_additions,
                        &completed_removals,
                    );
                    return Err(Self::index_error_with_rollback(e, rollback));
                }
                completed_additions.push(idx);
            }
        }

        Ok(())
    }

    fn rollback_index_batches(
        indexes: &[Arc<dyn Index>],
        add_batches: &[Vec<(i64, Vec<Value>)>],
        remove_batches: &[Vec<(i64, Vec<Value>)>],
        completed_additions: &[usize],
        completed_removals: &[usize],
    ) -> Result<(), Error> {
        let mut failures = Vec::new();

        for &rollback_idx in completed_additions.iter().rev() {
            let rollback_batch: Vec<(i64, &[Value])> = add_batches[rollback_idx]
                .iter()
                .map(|(row_id, values)| (*row_id, values.as_slice()))
                .collect();
            if let Err(error) = indexes[rollback_idx].remove_batch_slice(&rollback_batch) {
                failures.push(format!(
                    "remove additions from '{}': {}",
                    indexes[rollback_idx].name(),
                    error
                ));
            }
        }

        for &rollback_idx in completed_removals.iter().rev() {
            let rollback_batch: Vec<(i64, &[Value])> = remove_batches[rollback_idx]
                .iter()
                .map(|(row_id, values)| (*row_id, values.as_slice()))
                .collect();
            if let Err(error) = indexes[rollback_idx].add_batch_slice(&rollback_batch) {
                failures.push(format!(
                    "restore removals to '{}': {}",
                    indexes[rollback_idx].name(),
                    error
                ));
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::internal(failures.join("; ")))
        }
    }

    fn index_error_with_rollback(error: Error, rollback: Result<(), Error>) -> Error {
        match rollback {
            Ok(()) => error,
            Err(rollback_error) => Error::internal(format!(
                "index transition failed: {}; compensation failed: {}",
                error, rollback_error
            )),
        }
    }

    /// Fast path for single-row index updates
    ///
    /// Uses SmallVec to avoid heap allocation for indexes with 1-2 columns (most common case).
    /// Applies changes immediately without batching overhead.
    fn update_indexes_single_row(&self, indexes: &[Arc<dyn Index>]) -> Result<(), Error> {
        let local_versions = self.local_versions.as_ref().unwrap();

        // Get the single row entry
        let Some((row_id, versions)) = local_versions.iter().next() else {
            return Ok(());
        };

        let Some(new_version) = versions.last() else {
            return Ok(());
        };

        let is_deleted = new_version.is_deleted();
        let new_row = &new_version.data;

        // Get old version from write_set (if exists)
        let old_row: Option<&Row> = self
            .write_set
            .as_ref()
            .and_then(|ws| ws.get(row_id))
            .and_then(|entry| entry.read_version.as_ref())
            .map(|rv| &rv.data);

        // Track what we've done for rollback on error
        let mut completed_ops: SmallVec<[(usize, bool); 4]> = SmallVec::new(); // (index_idx, is_add)

        for (idx, index) in indexes.iter().enumerate() {
            // OPTIMIZATION: For UPDATEs, check if any indexed column changed BEFORE allocating
            if let Some(old_r) = old_row {
                if !is_deleted {
                    // UPDATE case: check if indexed columns differ (no allocation)
                    if !index_affecting_columns_changed(index.as_ref(), old_r, new_row) {
                        // Indexed and predicate columns unchanged - skip this index entirely
                        continue;
                    }

                    let old_values = index_values_for_row(index.as_ref(), old_r)?;
                    let new_values = index_values_for_row(index.as_ref(), new_row)?;

                    // Remove old, add new
                    if let Some(old_values) = &old_values {
                        if let Err(error) = index.remove(old_values, row_id, row_id) {
                            let rollback = self.rollback_single_row_ops(
                                &completed_ops,
                                indexes,
                                row_id,
                                old_row,
                                new_row,
                            );
                            return Err(Self::index_error_with_rollback(error, rollback));
                        }
                        completed_ops.push((idx, false)); // false = removal
                    }

                    if let Some(new_values) = &new_values {
                        if let Err(e) = index.add(new_values, row_id, row_id) {
                            let rollback = self.rollback_single_row_ops(
                                &completed_ops,
                                indexes,
                                row_id,
                                old_row,
                                new_row,
                            );
                            return Err(Self::index_error_with_rollback(e, rollback));
                        }
                        completed_ops.push((idx, true)); // true = addition
                    }
                    continue;
                }
            }

            if is_deleted {
                // DELETE: remove from index
                let source_row = old_row.unwrap_or(new_row);
                if let Some(values_to_remove) = index_values_for_row(index.as_ref(), source_row)? {
                    if let Err(error) = index.remove(&values_to_remove, row_id, row_id) {
                        let rollback = self.rollback_single_row_ops(
                            &completed_ops,
                            indexes,
                            row_id,
                            old_row,
                            new_row,
                        );
                        return Err(Self::index_error_with_rollback(error, rollback));
                    }
                    completed_ops.push((idx, false));
                }
            } else {
                // INSERT: add values to index
                if let Some(new_values) = index_values_for_row(index.as_ref(), new_row)? {
                    if let Err(e) = index.add(&new_values, row_id, row_id) {
                        let rollback = self.rollback_single_row_ops(
                            &completed_ops,
                            indexes,
                            row_id,
                            old_row,
                            new_row,
                        );
                        return Err(Self::index_error_with_rollback(e, rollback));
                    }
                    completed_ops.push((idx, true));
                }
            }
        }

        Ok(())
    }

    /// Rollback helper for single-row operations
    #[inline]
    fn rollback_single_row_ops(
        &self,
        completed_ops: &[(usize, bool)],
        indexes: &[Arc<dyn Index>],
        row_id: i64,
        old_row: Option<&Row>,
        new_row: &Row,
    ) -> Result<(), Error> {
        for &(idx, is_add) in completed_ops.iter().rev() {
            let index = &indexes[idx];

            if is_add {
                // We added new values - remove them
                if let Some(values) = index_values_for_row(index.as_ref(), new_row)? {
                    index.remove(&values, row_id, row_id)?;
                }
            } else {
                // We removed old values - re-add them
                let source = old_row.unwrap_or(new_row);
                if let Some(values) = index_values_for_row(index.as_ref(), source)? {
                    index.add(&values, row_id, row_id)?;
                }
            }
        }
        Ok(())
    }

    /// Rollback - discard local changes and release claims
    pub fn rollback(&mut self) {
        if self.terminal {
            return;
        }
        self.release_all_claims();
        if let Some(map) = self.local_versions.take() {
            return_version_list_map(map);
        }
        if let Some(map) = self.write_set.take() {
            return_write_set_map(map);
        }
        self.external_claim_journal.clear();
        self.external_index_removals.clear();
        self.prepared_final_view_indexes.clear();
        self.disabled_parent_indexes.clear();
        self.schema_override = None;
        self.index_ddl_authorized = false;
        self.terminal = true;
    }

    /// Rollback to a specific timestamp (for savepoint support)
    ///
    /// Discards all local changes that were made after the given timestamp.
    /// For rows with version history, keeps versions at or before the timestamp.
    /// Row claims are released only if all versions for that row are discarded.
    pub fn rollback_to_timestamp(&mut self, timestamp: i64) {
        if self.terminal {
            return;
        }
        let mut rows_to_remove_completely: Vec<i64> = Vec::new();

        if let Some(local_versions) = self.local_versions.as_mut() {
            // For each row, remove versions with create_time > timestamp
            for (row_id, versions) in local_versions.iter_mut() {
                // Keep only versions at or before the timestamp
                versions.retain(|v| v.create_time <= timestamp);

                // If all versions are removed, mark for complete removal
                if versions.is_empty() {
                    rows_to_remove_completely.push(row_id);
                }
            }

            // Remove rows with no remaining versions and release their claims
            for row_id in &rows_to_remove_completely {
                local_versions.remove(*row_id);
                self.parent_store.release_row_claim(*row_id, self.txn_id);
                if let Some(write_set) = self.write_set.as_mut() {
                    write_set.remove(*row_id);
                }
            }
        }

        // Cold non-INTEGER-PK DELETE can own a claim without creating a hot
        // local version. Roll those direct claims back by their own journal.
        while self
            .external_claim_journal
            .last()
            .is_some_and(|(_, claimed_at)| *claimed_at > timestamp)
        {
            let Some((row_id, _)) = self.external_claim_journal.pop() else {
                break;
            };
            let has_local_version = self
                .local_versions
                .as_ref()
                .is_some_and(|versions| versions.contains_key(row_id));
            if !has_local_version {
                self.parent_store.release_row_claim(row_id, self.txn_id);
                if let Some(write_set) = self.write_set.as_mut() {
                    write_set.remove(row_id);
                }
            }
        }
        // Superseded claims acquired before this boundary stay retained so an
        // older row view can be restored without a claim gap. Claims first
        // acquired by the rolled-back statement are released atomically with
        // rebuilding the transaction-local final view.
        if let Err(error) = self.rollback_unique_keys_to(timestamp) {
            // Do not let a secondary index-expression failure turn rollback
            // into a panic. Shared claims remain conservative until the
            // transaction terminates; clearing only this local accelerator
            // defers duplicate detection to the existing commit validator.
            self.current_unique_keys.clear();
            debug_assert!(false, "failed to rebuild UNIQUE claim view: {error}");
        }
    }

    /// Track a claim made directly on the parent VersionStore (not through put()).
    /// Used by SegmentedTable for cold row UPDATE/DELETE claims that bypass
    /// TransactionVersionStore's put methods. Without tracking, these claims
    /// leak because commit() only releases claims found in write_set.
    pub fn track_external_claim(&mut self, row_id: i64) -> Result<(), Error> {
        self.ensure_active()?;
        // Only add if not already tracked (idempotent).
        // The claim proves this is a previously visible candidate, not an
        // INSERT. A concrete read_version may be filled later if the candidate
        // is found in hot MVCC after waiting.
        use radixdb_core::i64_map::Entry;
        let inserted = match self.ensure_write_set().entry(row_id) {
            Entry::Vacant(e) => {
                e.insert(WriteSetEntry::claimed());
                true
            }
            Entry::Occupied(_) => false,
        };
        if inserted {
            self.external_claim_journal
                .push((row_id, get_fast_timestamp()));
        }
        Ok(())
    }

    /// Claim committed candidate rows before evaluating UPDATE expressions.
    ///
    /// Sorting gives a stable acquisition order inside one statement. The
    /// VersionStore wait-die policy covers cycles across statements/tables.
    /// Placeholders are later completed by `put_with_original` after the
    /// caller re-reads the now-current committed row.
    pub fn claim_rows_for_update(&mut self, row_ids: &[i64]) -> Result<(), Error> {
        self.claim_rows_for_operation(row_ids)
    }

    /// Claim committed candidate rows for DELETE while preserving their
    /// existing-row provenance even when residual recheck produces no local
    /// delete version.
    pub fn claim_rows_for_delete(&mut self, row_ids: &[i64]) -> Result<(), Error> {
        self.claim_rows_for_operation(row_ids)
    }

    fn claim_rows_for_operation(&mut self, row_ids: &[i64]) -> Result<(), Error> {
        self.ensure_active()?;
        let mut ordered = row_ids.to_vec();
        ordered.sort_unstable();
        ordered.dedup();

        let mut claimed_here = Vec::new();
        for row_id in ordered {
            if self
                .write_set
                .as_ref()
                .is_some_and(|write_set| write_set.contains_key(row_id))
            {
                continue;
            }
            if let Err(error) = self.parent_store.try_claim_row(row_id, self.txn_id) {
                for claimed_row_id in claimed_here.into_iter().rev() {
                    self.parent_store
                        .release_row_claim(claimed_row_id, self.txn_id);
                    if let Some(write_set) = self.write_set.as_mut() {
                        write_set.remove(claimed_row_id);
                    }
                    self.external_claim_journal
                        .retain(|(journal_row_id, _)| *journal_row_id != claimed_row_id);
                }
                return Err(error);
            }
            self.track_external_claim(row_id)?;
            claimed_here.push(row_id);
        }
        Ok(())
    }

    /// Release all row claims held by this transaction
    fn release_all_claims(&mut self) {
        if let Some(write_set) = self.write_set.as_ref() {
            // OPTIMIZATION: Collect row_ids first, then batch release
            // Avoids holding write_set iterator while accessing parent_store
            let mut row_ids: Vec<i64> = write_set.keys().collect();
            // Sort by row_id to ensure deterministic locking order
            row_ids.sort_unstable();
            self.parent_store
                .release_row_claims_batch(&row_ids, self.txn_id);
        }
        self.release_retained_unique_keys();
    }
}

impl Drop for TransactionVersionStore {
    fn drop(&mut self) {
        // Release any row claims still held by this transaction.
        // This is a safety net for cases where drop happens without explicit
        // commit/rollback (e.g., transaction panics, implicit drop on scope exit).
        // Without this, claims in uncommitted_writes would leak permanently,
        // blocking future UPDATE/DELETE on those rows.
        self.release_all_claims();

        // Return maps to the pool for reuse by future transactions.
        // This reduces allocation overhead from ~5.5KB per transaction to near zero
        // for bulk insert workloads where many short-lived transactions are created.
        if let Some(map) = self.local_versions.take() {
            return_version_list_map(map);
        }
        if let Some(map) = self.write_set.take() {
            return_write_set_map(map);
        }
    }
}

impl fmt::Debug for TransactionVersionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionVersionStore")
            .field("txn_id", &self.txn_id)
            .field(
                "local_version_count",
                &self.local_versions.as_ref().map_or(0, |lv| lv.len()),
            )
            .field(
                "write_set_count",
                &self.write_set.as_ref().map_or(0, |ws| ws.len()),
            )
            .finish()
    }
}
