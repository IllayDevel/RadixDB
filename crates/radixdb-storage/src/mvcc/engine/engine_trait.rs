use super::*;

use crate::PhysicalSnapshotIdentity;

impl Engine for MVCCEngine {
    fn open(&mut self) -> Result<()> {
        MVCCEngine::open_engine(self)
    }

    fn close(&mut self) -> Result<()> {
        MVCCEngine::close_engine(self)
    }

    fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        self.begin_transaction_with_level(self.get_isolation_level())
    }

    fn begin_transaction_with_level(&self, level: IsolationLevel) -> Result<Box<dyn Transaction>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        // Physical maintenance discards row-level MVCC provenance when it
        // publishes immutable artifacts. Linearize Snapshot admission with
        // that publication lifecycle: maintenance that entered first finishes
        // before this boundary is captured, while maintenance entering later
        // observes this active snapshot and defers its rewrite.
        let _snapshot_admission = (level == IsolationLevel::SnapshotIsolation)
            .then(|| self.snapshot_maintenance_fence.write());

        // Begin transaction in registry.
        let (txn_id, begin_seq) = self.registry.begin_transaction_with_isolation(level);
        if txn_id == INVALID_TRANSACTION_ID {
            return Err(Error::internal(
                "transaction registry is not accepting new transactions",
            ));
        }

        // Create transaction
        let mut txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&self.registry));

        // Set engine operations
        let engine_ops = self.create_engine_operations();
        txn.set_engine_operations(engine_ops);

        Ok(Box::new(txn))
    }

    fn path(&self) -> Option<&str> {
        if self.path == "memory://" {
            None
        } else {
            Some(&self.path)
        }
    }

    fn table_exists(&self, table_name: &str) -> Result<bool> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let name = to_lowercase_cow(table_name);
        let schemas = self.schemas.read().unwrap();
        Ok(schemas.contains_key(name.as_ref()))
    }

    fn index_exists(&self, index_name: &str, table_name: &str) -> Result<bool> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let store = self.get_version_store(table_name)?;
        Ok(store.index_exists(index_name))
    }

    fn get_index(&self, table_name: &str, index_name: &str) -> Result<Arc<dyn Index>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let store = self.get_version_store(table_name)?;
        store
            .get_index(index_name)
            .ok_or_else(|| Error::IndexNotFound(format!("{table_name}.{index_name}")))
    }

    fn get_table_schema(&self, table_name: &str) -> Result<CompactArc<Schema>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let name = to_lowercase_cow(table_name);
        let schemas = self.schemas.read().unwrap();
        schemas
            .get(name.as_ref())
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.as_ref().to_string()))
    }

    #[inline]
    fn schema_epoch(&self) -> u64 {
        self.schema_epoch.load(Ordering::Acquire)
    }

    #[inline]
    fn schema_scope_id(&self) -> u64 {
        self.schema_scope_id
    }

    fn list_table_indexes(&self, table_name: &str) -> Result<FxHashMap<String, String>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let store = self.get_version_store(table_name)?;
        let mut result = FxHashMap::default();
        for index in store.get_all_indexes() {
            if is_schema_derived_pk_index(&store.schema(), index.as_ref()) {
                continue;
            }
            result.insert(
                index.name().to_string(),
                match index.index_type() {
                    radixdb_core::IndexType::BTree => "BTree",
                    radixdb_core::IndexType::Hash => "Hash",
                    radixdb_core::IndexType::Bitmap => "Bitmap",
                    radixdb_core::IndexType::Hnsw => "HNSW",
                    radixdb_core::IndexType::MultiColumn => "MultiColumn",
                    radixdb_core::IndexType::PrimaryKey => unreachable!(),
                }
                .to_string(),
            );
        }
        Ok(result)
    }

    fn get_all_indexes(&self, table_name: &str) -> Result<Vec<std::sync::Arc<dyn Index>>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let store = self.get_version_store(table_name)?;

        // Get all indexes from the version store (convert SmallVec to Vec for trait compatibility)
        Ok(store.get_all_indexes().into_vec())
    }

    fn get_isolation_level(&self) -> IsolationLevel {
        self.registry.get_global_isolation_level()
    }

    fn set_isolation_level(&mut self, level: IsolationLevel) -> Result<()> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        self.registry.set_global_isolation_level(level);
        Ok(())
    }

    fn get_config(&self) -> Config {
        self.config.read().expect("config lock poisoned").clone()
    }

    fn update_config(&mut self, config: Config) -> Result<()> {
        self.update_engine_config(config)
    }

    fn checkpoint_cycle(&self) -> Result<()> {
        MVCCEngine::checkpoint_cycle(self)
    }

    fn force_checkpoint_cycle(&self) -> Result<()> {
        if !self.persistence().is_some_and(|pm| pm.is_enabled()) {
            return Err(Error::invalid_argument(
                "CHECKPOINT requires a persistent database",
            ));
        }
        match MVCCEngine::checkpoint_cycle_inner(self, true) {
            Ok(()) => {
                self.compaction_requested.store(true, Ordering::Release);
                Ok(())
            }
            Err(error)
                if matches!(
                    &error,
                    Error::Internal { message }
                        if message == FORCED_CHECKPOINT_HOT_ROWS_UNSEALED
                ) =>
            {
                // A partial seal is still durable and safe. Queue ordinary
                // bounded maintenance, but preserve the explicit busy outcome;
                // retrying durability must never synchronously rewrite cold
                // history as flow control.
                self.compaction_requested.store(true, Ordering::Release);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    fn create_snapshot(&self) -> Result<PhysicalSnapshotIdentity> {
        MVCCEngine::create_physical_snapshot(self)
    }

    fn create_snapshot_cancellable(
        &self,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<PhysicalSnapshotIdentity> {
        MVCCEngine::create_physical_snapshot_cancellable(self, is_cancelled)
    }

    fn restore_snapshot(&self, snapshot_id: Option<&str>) -> Result<String> {
        MVCCEngine::restore_physical_snapshot(self, snapshot_id)
    }

    fn record_truncate_table(&self, table_name: &str) -> Result<()> {
        let table_lower = table_name.to_lowercase();

        // WAL FIRST: record the truncate before deleting segment files.
        // If crash happens after WAL but before file deletion, WAL replay
        // will re-execute the truncate. Orphan files are harmless.
        if !self.should_skip_wal() {
            let schema = self
                .schemas
                .read()
                .unwrap()
                .get(&table_lower)
                .cloned()
                .ok_or_else(|| Error::TableNotFound(table_lower.clone()))?;
            let table_id = ObjectId::from_user_bytes(schema.catalog_id()).map_err(|error| {
                Error::internal(format!(
                    "table '{}' has invalid stable WAL identity: {error}",
                    schema.table_name
                ))
            })?;
            if let Some(pm) = self.persistence() {
                if pm.is_enabled() {
                    pm.record_table_operation(table_id, WALOperationType::TruncateTable, &[])?;
                }
            }
        }

        // Remove process-local membership only after the WAL record is durable.
        // The next checkpoint compares runtime membership with the selected
        // physical manifest and publishes an empty table-manifest generation
        // before it may retire this WAL prefix. Immutable DATA/INDEX bytes are
        // never copied or deleted here; reachability-aware GC owns reclamation.
        {
            let mgrs = self.segment_managers.read().unwrap();
            if let Some(mgr) = mgrs.get(&table_lower) {
                mgr.truncate_persisted_segments().map_err(|error| {
                    Error::WalDurabilityUncertain {
                        detail: format!(
                            "TRUNCATE '{}' is durably committed but persisted segment publication requires recovery: {}",
                            table_name, error
                        ),
                    }
                })?;
            }
        }

        // Legacy snapshot tombstones are not part of the committed volume
        // generation, but must not survive an explicit TRUNCATE.
        if let Some(pm) = self.persistence() {
            if pm.is_enabled() {
                let tombstones_path = pm
                    .path()
                    .join("snapshots")
                    .join(&table_lower)
                    .join("tombstones.dat");
                match std::fs::remove_file(&tombstones_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => eprintln!(
                        "Warning: failed to delete legacy TRUNCATE tombstones '{}': {error}",
                        tombstones_path.display()
                    ),
                }
            }
        }

        Ok(())
    }

    fn fetch_rows_by_ids(&self, table_name: &str, row_ids: &[i64]) -> Result<radixdb_core::RowVec> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let store = self.get_version_store(table_name)?;
        let read_txn_id = INVALID_TRANSACTION_ID + 1;
        let mut result = store.get_visible_versions_batch(row_ids, read_txn_id);

        // Fall back to cold segments for rows not found in hot
        if result.len() < row_ids.len() {
            let mgr = self.get_or_create_segment_manager(table_name);
            if mgr.has_segments() {
                let schema = store.schema().clone();
                let found_ids: rustc_hash::FxHashSet<i64> =
                    result.iter().map(|(id, _)| *id).collect();
                for &rid in row_ids {
                    if !found_ids.contains(&rid) {
                        if let Some(row) = mgr.get_cold_row_normalized(rid, &schema)? {
                            result.push((rid, row));
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    fn get_row_fetcher(
        &self,
        table_name: &str,
    ) -> Result<Box<dyn Fn(&[i64]) -> Result<radixdb_core::RowVec> + Send + Sync>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let store = self.get_version_store(table_name)?;
        let mgr = self.get_or_create_segment_manager(table_name);
        let schema = store.schema().clone();
        let read_txn_id = INVALID_TRANSACTION_ID + 1;

        Ok(Box::new(move |row_ids: &[i64]| {
            let mut result = store.get_visible_versions_batch(row_ids, read_txn_id);
            if result.len() < row_ids.len() && mgr.has_segments() {
                let found_ids: rustc_hash::FxHashSet<i64> =
                    result.iter().map(|(id, _)| *id).collect();
                for &rid in row_ids {
                    if !found_ids.contains(&rid) {
                        if let Some(row) = mgr.get_cold_row_normalized(rid, &schema)? {
                            result.push((rid, row));
                        }
                    }
                }
            }
            Ok(result)
        }))
    }

    /// Get a count-only function for counting visible rows by their IDs.
    /// This is optimized for COUNT(*) subqueries where we don't need the actual row data.
    fn get_row_counter(
        &self,
        table_name: &str,
    ) -> Result<Box<dyn Fn(&[i64]) -> usize + Send + Sync>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let store = self.get_version_store(table_name)?;
        let mgr = self.get_or_create_segment_manager(table_name);
        let read_txn_id = INVALID_TRANSACTION_ID + 1;

        Ok(Box::new(move |row_ids: &[i64]| {
            let mut count = store.count_visible_versions_batch(row_ids, read_txn_id);
            if count < row_ids.len() && mgr.has_segments() {
                for &rid in row_ids {
                    if !store.has_committed_row(rid) && mgr.row_exists(rid) {
                        count += 1;
                    }
                }
            }
            count
        }))
    }
}

// =============================================================================
// Cleanup Functions
// =============================================================================
