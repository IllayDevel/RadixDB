use super::*;
use radixdb_catalog::{CatalogMutationSet, CatalogPackMeta, CatalogPublisher};

#[derive(Debug)]
struct PreparedCatalogCommit {
    mutation: CatalogMutationSet,
    next_catalog_id: [u8; 16],
    created_unix_ns: u64,
    schemas: Vec<Schema>,
    views: Vec<ViewDefinition>,
}

/// Engine operations for transaction callbacks.
///
/// Holds Arc references to shared engine state, allowing safe access
/// from transactions without raw pointers.
pub(super) struct EngineOperations {
    /// Stable owner identity used to isolate deterministic interleave tests
    /// from other engines running in the same test process.
    #[cfg(any(test, feature = "test-failpoints"))]
    schema_scope_id: u64,
    /// Shared reference to schemas (each schema is Arc-wrapped to avoid cloning on lookup)
    schemas: Arc<RwLock<FxHashMap<String, CompactArc<Schema>>>>,
    /// Shared reference to version stores
    version_stores: Arc<RwLock<FxHashMap<String, Arc<VersionStore>>>>,
    /// Transaction-local unpublished tables.
    pending_tables: Arc<RwLock<FxHashMap<String, PendingTable>>>,
    /// Shared view namespace paired with table reservations.
    views: Arc<RwLock<FxHashMap<String, Arc<ViewDefinition>>>>,
    /// Single logical-catalog publication owner installed by database open.
    catalog_publisher: Arc<ArcSwap<CatalogPublisher>>,
    /// CONTROL-selected physical generation owner used by post-commit DDL
    /// accelerator publication.
    physical_generation: Arc<ArcSwapOption<crate::v6::PhysicalGenerationPublisher>>,
    /// Canonical database root paired with `physical_generation`.
    database_root: PathBuf,
    catalog_runtime_binder: CatalogRuntimeBinder,
    /// Successor validated before the commit marker and consumed exactly once
    /// after that marker becomes durable.
    prepared_catalog: Mutex<Option<PreparedCatalogCommit>>,
    /// Shared reference to registry
    registry: Arc<TransactionRegistry>,
    /// Shared reference to transaction version stores cache
    txn_version_stores: Arc<RwLock<TxnVersionStoreMap>>,
    /// Shared deferred persistence owner.
    persistence: Arc<ArcSwapOption<PersistenceManager>>,
    /// Shared reference to loading_from_disk flag
    loading_from_disk: Arc<AtomicBool>,
    /// Shared reference to segment managers
    segment_managers: Arc<RwLock<FxHashMap<String, Arc<crate::volume::manifest::SegmentManager>>>>,
    /// Shared CPU admission for transaction WAL serialization/framing and
    /// physical seal/compaction work.
    storage_cpu_runtime: Arc<crate::cpu_runtime::StorageCpuRuntime>,
    /// Seal fence for WAL truncation safety
    seal_fence: Arc<parking_lot::RwLock<()>>,
    /// Catalog publication fence shared with executors.
    ddl_fence: Arc<parking_lot::RwLock<()>>,
    /// Logical commit visibility fence shared with executors.
    visibility_fence: Arc<parking_lot::RwLock<()>>,
    /// Shared catalog epoch incremented only when DDL becomes visible.
    schema_epoch: Arc<AtomicU64>,
    /// Memory-pressure seal coordinator shared with the background worker.
    pressure_seal: Arc<PressureSealControl>,
    /// Cooperative compaction admission shared with the background owner.
    compaction_requested: Arc<AtomicBool>,
    compaction_soft_backpressure_waits: Arc<AtomicU64>,
    compaction_soft_backpressure_wait_millis: Arc<AtomicU64>,
    compaction_hard_backpressure_rejections: Arc<AtomicU64>,
    /// Admission policy snapshot installed when this transaction began.
    l0_pressure_limits: L0PressureLimits,
    /// Threshold snapshot installed when this transaction began. Runtime
    /// configuration changes apply to subsequently created transactions.
    row_validator_binder: crate::validation::RowValidatorBinder,
    seal_hot_bytes_threshold: usize,
    seal_incremental_hot_bytes_threshold: usize,
}

// EngineOperations is Send + Sync because all fields are Arc-wrapped thread-safe types

impl EngineOperations {
    pub(super) fn new(engine: &MVCCEngine) -> Self {
        let (seal_hot_bytes_threshold, seal_incremental_hot_bytes_threshold) = engine
            .config
            .read()
            .map(|config| {
                (
                    config.persistence.seal_hot_bytes_threshold,
                    config.persistence.seal_incremental_hot_bytes_threshold,
                )
            })
            .unwrap_or((64 * 1024 * 1024, 16 * 1024 * 1024));
        Self {
            #[cfg(any(test, feature = "test-failpoints"))]
            schema_scope_id: engine.schema_scope_id,
            schemas: Arc::clone(&engine.schemas),
            version_stores: Arc::clone(&engine.version_stores),
            pending_tables: Arc::clone(&engine.pending_tables),
            views: Arc::clone(&engine.views),
            catalog_publisher: Arc::clone(&engine.catalog_publisher),
            physical_generation: Arc::clone(&engine.physical_generation),
            database_root: PathBuf::from(&engine.path),
            catalog_runtime_binder: engine.catalog_runtime_binder(),
            prepared_catalog: Mutex::new(None),
            registry: Arc::clone(&engine.registry),
            txn_version_stores: Arc::clone(&engine.txn_version_stores),
            persistence: Arc::clone(&engine.persistence),
            loading_from_disk: Arc::clone(&engine.loading_from_disk),
            segment_managers: Arc::clone(&engine.segment_managers),
            storage_cpu_runtime: Arc::clone(&engine.storage_cpu_runtime),
            seal_fence: Arc::clone(&engine.seal_fence),
            ddl_fence: Arc::clone(&engine.ddl_fence),
            visibility_fence: Arc::clone(&engine.visibility_fence),
            schema_epoch: Arc::clone(&engine.schema_epoch),
            pressure_seal: Arc::clone(&engine.pressure_seal),
            compaction_requested: Arc::clone(&engine.compaction_requested),
            compaction_soft_backpressure_waits: Arc::clone(
                &engine.compaction_soft_backpressure_waits,
            ),
            compaction_soft_backpressure_wait_millis: Arc::clone(
                &engine.compaction_soft_backpressure_wait_millis,
            ),
            compaction_hard_backpressure_rejections: Arc::clone(
                &engine.compaction_hard_backpressure_rejections,
            ),
            l0_pressure_limits: engine.l0_pressure_limits(),
            row_validator_binder: engine
                .row_validator_binder
                .get()
                .copied()
                .unwrap_or(crate::validation::bind_schema_only_row_validator),
            seal_hot_bytes_threshold,
            seal_incremental_hot_bytes_threshold,
        }
    }

    fn bind_row_validator(
        &self,
        schema: &Schema,
    ) -> Result<Box<dyn crate::validation::PreparedRowValidator>> {
        (self.row_validator_binder)(schema)
    }

    /// Ordinary non-unique indexes can let the immutable artifact publisher
    /// own their cold population. UNIQUE needs pre-commit validation, while
    /// partial and HNSW indexes still require their dedicated runtime owners.
    fn can_defer_cold_index_backfill(
        &self,
        definition: &crate::traits::PendingIndexDefinition,
    ) -> bool {
        if definition.is_unique
            || definition.partial_predicate.is_some()
            || definition.key_encoder.is_some()
            || self.physical_generation.load_full().is_none()
        {
            return false;
        }
        let table_name = definition.table_name.to_lowercase();
        if !self
            .segment_managers
            .read()
            .unwrap()
            .get(&table_name)
            .is_some_and(|manager| manager.has_segments())
        {
            return false;
        }
        if definition.columns.len() > 1 {
            return definition.index_type != Some(radixdb_core::IndexType::Hnsw);
        }
        let Some(column_name) = definition.columns.first() else {
            return false;
        };
        let schemas = self.schemas.read().unwrap();
        let Some(schema) = schemas.get(&table_name) else {
            return false;
        };
        let Some((_, column)) = schema.find_column(column_name) else {
            return false;
        };
        let resolved = definition.index_type.unwrap_or(match column.data_type {
            radixdb_core::DataType::Vector => radixdb_core::IndexType::Hnsw,
            radixdb_core::DataType::Text
            | radixdb_core::DataType::Json
            | radixdb_core::DataType::Bytes => radixdb_core::IndexType::Hash,
            radixdb_core::DataType::Boolean => radixdb_core::IndexType::Bitmap,
            _ => radixdb_core::IndexType::BTree,
        });
        !matches!(
            resolved,
            radixdb_core::IndexType::Hnsw | radixdb_core::IndexType::PrimaryKey
        )
    }

    fn touched_l0_pressure(
        &self,
        txn_id: i64,
    ) -> Option<(
        String,
        crate::volume::manifest::L0DebtSnapshot,
        L0PressureLevel,
    )> {
        let touched: SmallVec<[SmartString; 4]> = self
            .txn_version_stores
            .read()
            .unwrap()
            .get(txn_id)
            .map(|tables| tables.iter().map(|(name, _)| name.clone()).collect())
            .unwrap_or_default();
        if touched.is_empty() {
            return None;
        }

        let managers = self.segment_managers.read().unwrap();
        let mut soft = None;
        for table_name in touched {
            let Some(manager) = managers.get(table_name.as_str()) else {
                continue;
            };
            let debt = manager.l0_debt_snapshot();
            match classify_l0_pressure(debt, self.l0_pressure_limits) {
                L0PressureLevel::Hard => {
                    return Some((table_name.to_string(), debt, L0PressureLevel::Hard));
                }
                L0PressureLevel::Soft if soft.is_none() => {
                    soft = Some((table_name.to_string(), debt, L0PressureLevel::Soft));
                }
                L0PressureLevel::Normal | L0PressureLevel::Soft => {}
            }
        }
        soft
    }

    fn wait_for_compaction_pressure(&self, txn_id: i64) -> Result<()> {
        let started = Instant::now();
        let mut soft_wait_recorded = false;
        loop {
            let Some((table, debt, level)) = self.touched_l0_pressure(txn_id) else {
                if soft_wait_recorded {
                    self.compaction_soft_backpressure_wait_millis.fetch_add(
                        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                }
                return Ok(());
            };
            self.compaction_requested.store(true, Ordering::Release);
            if level == L0PressureLevel::Hard {
                if soft_wait_recorded {
                    self.compaction_soft_backpressure_wait_millis.fetch_add(
                        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                }
                self.compaction_hard_backpressure_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Error::CompactionBackpressure {
                    table,
                    segments: debt.segments,
                    physical_bytes: debt.physical_bytes,
                    hard_segments: self.l0_pressure_limits.hard_segments,
                    hard_bytes: self.l0_pressure_limits.hard_bytes,
                });
            }
            if !soft_wait_recorded {
                self.compaction_soft_backpressure_waits
                    .fetch_add(1, Ordering::Relaxed);
                soft_wait_recorded = true;
            }
            if self.l0_pressure_limits.soft_wait.is_zero()
                || started.elapsed() >= self.l0_pressure_limits.soft_wait
            {
                self.compaction_soft_backpressure_wait_millis.fetch_add(
                    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    Ordering::Relaxed,
                );
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn touched_tables_exceed_seal_pressure(&self, table_names: &[SmartString]) -> bool {
        if table_names.is_empty() {
            return false;
        }
        let stores = self.version_stores.read().unwrap();
        let total_hot_bytes = stores.values().fold(0usize, |total, store| {
            total.saturating_add(store.committed_hot_bytes())
        });
        let candidates: SmallVec<[(SmartString, Arc<VersionStore>); 4]> = table_names
            .iter()
            .filter_map(|name| {
                stores
                    .get(name.as_str())
                    .map(|store| (name.clone(), Arc::clone(store)))
            })
            .collect();
        drop(stores);

        // Per-table thresholds alone do not bound a database with many small
        // hot tables.  The aggregate soft watermark starts the independent
        // worker before producers reach the hard admission watermark.
        if total_hot_bytes >= total_hot_soft_threshold(self.seal_hot_bytes_threshold) {
            return true;
        }

        let managers = self.segment_managers.read().unwrap();
        candidates.into_iter().any(|(name, store)| {
            let incremental = managers
                .get(name.as_str())
                .is_some_and(|manager| manager.has_segments());
            let byte_threshold = if incremental {
                self.seal_incremental_hot_bytes_threshold
            } else {
                self.seal_hot_bytes_threshold
            };
            store.committed_hot_bytes() >= byte_threshold
        })
    }

    /// Soft aggregate pressure runs in the background.  Foreground commits
    /// wait only when a table crossed its configured owner budget or the whole
    /// database reached the larger hard watermark; otherwise a small-table
    /// workload would pay one artifact publication and fsync on every commit.
    fn hot_storage_requires_backpressure(&self) -> bool {
        let stores = self.version_stores.read().unwrap();
        let total_hot_bytes = stores.values().fold(0usize, |total, store| {
            total.saturating_add(store.committed_hot_bytes())
        });
        if total_hot_bytes >= total_hot_hard_threshold(self.seal_hot_bytes_threshold) {
            return true;
        }
        let candidates = stores
            .iter()
            .map(|(name, store)| (name.clone(), Arc::clone(store)))
            .collect::<Vec<_>>();
        drop(stores);

        let managers = self.segment_managers.read().unwrap();
        candidates.into_iter().any(|(name, store)| {
            let incremental = managers
                .get(name.as_str())
                .is_some_and(|manager| manager.has_segments());
            let byte_threshold = if incremental {
                self.seal_incremental_hot_bytes_threshold
            } else {
                self.seal_hot_bytes_threshold
            };
            store.committed_hot_bytes() >= byte_threshold
        })
    }

    fn schemas(&self) -> &RwLock<FxHashMap<String, CompactArc<Schema>>> {
        &self.schemas
    }

    fn version_stores(&self) -> &RwLock<FxHashMap<String, Arc<VersionStore>>> {
        &self.version_stores
    }

    fn txn_version_stores(&self) -> &RwLock<TxnVersionStoreMap> {
        &self.txn_version_stores
    }

    fn persistence(&self) -> Option<Arc<PersistenceManager>> {
        self.persistence.load_full()
    }

    fn should_skip_wal(&self) -> bool {
        self.loading_from_disk.load(Ordering::Acquire)
    }

    fn get_or_create_segment_manager(
        &self,
        table_name: &str,
    ) -> Arc<crate::volume::manifest::SegmentManager> {
        if let Some(manager) = self.segment_managers.read().unwrap().get(table_name) {
            return Arc::clone(manager);
        }

        let mut managers = self.segment_managers.write().unwrap();
        let manager = managers
            .entry(table_name.to_string())
            .or_insert_with(|| {
                // SegmentManager is a process-local topology/cache owner in
                // the catalog-owned format. Durable membership is published
                // only through DatabaseManifest/TableManifest + CONTROL; a
                // second table-local durable authority would reintroduce the
                // retired dual-publication path.
                Arc::new(crate::volume::manifest::SegmentManager::new(
                    table_name, None,
                ))
            })
            .clone();
        drop(managers);
        manager
    }

    fn validate_schema(&self, schema: &Schema) -> Result<()> {
        schema.validate_structural_invariants()?;
        if schema.table_name.is_empty() {
            return Err(Error::internal("schema missing table name"));
        }
        if schema.primary_key_indices().len() > 1 {
            return Err(Error::NotSupported(
                "ALTER TABLE supports exactly one PRIMARY KEY column".to_string(),
            ));
        }
        let mut seen_names = FxHashSet::default();
        for column in &schema.columns {
            if column.name.is_empty() {
                return Err(Error::internal("column name cannot be empty"));
            }
            if column.auto_increment
                && !matches!(column.data_type, DataType::Integer | DataType::Uuid)
            {
                return Err(Error::invalid_argument(format!(
                    "auto-increment column {} must be INTEGER or UUID",
                    column.name
                )));
            }
            if !seen_names.insert(column.name_lower.clone()) {
                return Err(Error::DuplicateColumn);
            }
        }
        schema.validate_foreign_key_invariants()?;
        crate::mvcc::persistence::validate_schema_persistence(schema)
    }

    fn version_store_for_transaction(
        &self,
        txn_id: i64,
        table_name: &str,
    ) -> Option<Arc<VersionStore>> {
        if let Some(store) = self
            .pending_tables
            .read()
            .unwrap()
            .get(table_name)
            .and_then(|pending| {
                (pending.owner_txn_id == txn_id).then(|| Arc::clone(&pending.version_store))
            })
        {
            return Some(store);
        }
        self.version_stores.read().unwrap().get(table_name).cloned()
    }

    /// Validate pending INSERT/UPDATE rows against cold segments at commit time.
    /// Called when seal_generation changed since the transaction's INSERT,
    /// meaning a seal may have moved conflicting rows from hot to cold.
    /// The seal read fence is held by the caller.
    fn validate_pending_against_cold(
        &self,
        txn_id: i64,
        txn_store: &Arc<RwLock<TransactionVersionStore>>,
        version_store: &Arc<VersionStore>,
        mgr: &Arc<crate::volume::manifest::SegmentManager>,
    ) -> Result<()> {
        let store = txn_store.read().unwrap();
        let Some(local) = store.local_versions_ref() else {
            return Ok(());
        };
        let validation_started = Instant::now();
        let pending_rows = local.len();

        let schema_arc = version_store.schema();
        let schema = &*schema_arc;
        let pk_idx = schema.pk_column_index();
        // The caller owns the segment seal-read fence, so one immutable cold
        // snapshot is sufficient for the complete transaction batch. Taking a
        // fresh snapshot for every pending COPY row made the safety recheck
        // O(rows * (manifest lock + ArcSwap load + segment list allocation))
        // and turned a concurrent seal into a multi-minute commit on HDD.
        let cold_snapshot = mgr.cold_snapshot();

        // INTEGER PRIMARY KEY is the physical row identity.  Validate the
        // complete INSERT set against resident cold row-id metadata once,
        // instead of decoding/probing every immutable segment for every row.
        // A malformed/non-canonical row shape falls back to the generic path.
        let primary_key_started = Instant::now();
        let integer_primary_key_batch_checked = if let Some(pk_col) = pk_idx {
            if schema.columns[pk_col].data_type == DataType::Integer {
                let mut row_ids = Vec::new();
                let mut canonical = true;
                for (row_id, versions) in local.iter() {
                    let Some(version) = versions.last() else {
                        continue;
                    };
                    if version.is_deleted() {
                        continue;
                    }
                    let is_insert = store
                        .write_set_ref()
                        .and_then(|write_set| write_set.get(row_id))
                        .is_some_and(|entry| {
                            entry.observation
                                == crate::mvcc::version_store::WriteObservation::Absent
                        });
                    if !is_insert {
                        continue;
                    }
                    match version.data.get(pk_col) {
                        Some(Value::Integer(primary_key)) if *primary_key == row_id => {
                            row_ids.push(*primary_key);
                        }
                        _ => {
                            canonical = false;
                            break;
                        }
                    }
                }
                if canonical {
                    row_ids.sort_unstable();
                    row_ids.dedup();
                    let mut pending_tombstones = FxHashSet::default();
                    mgr.insert_pending_tombstones_into(txn_id, &mut pending_tombstones);
                    if let Some(row_id) = mgr.first_visible_row_id_in_sorted_batch(
                        &cold_snapshot,
                        row_ids.as_slice(),
                        &pending_tombstones,
                    ) {
                        return Err(radixdb_core::Error::PrimaryKeyConstraint { row_id });
                    }
                }
                canonical
            } else {
                false
            }
        } else {
            false
        };
        let primary_key_elapsed = primary_key_started.elapsed();

        // Collect unique index column info + precompute defaults once.
        let unique_indexes: Vec<(Vec<usize>, Vec<String>, Vec<radixdb_core::Value>)> =
            version_store
                .get_unique_non_pk_index_columns()
                .into_iter()
                .map(|(col_indices, col_names)| {
                    let defaults: Vec<radixdb_core::Value> = col_indices
                        .iter()
                        .map(|&ci| {
                            schema.columns[ci]
                                .default_value
                                .clone()
                                .unwrap_or(radixdb_core::Value::null(schema.columns[ci].data_type))
                        })
                        .collect();
                    (col_indices, col_names, defaults)
                })
                .collect();

        for (row_id, versions) in local.iter() {
            let Some(version) = versions.last() else {
                continue;
            };
            if version.is_deleted() {
                continue;
            }
            let row = &version.data;

            // Write-set provenance is authoritative. A missing read_version
            // can mean either a true INSERT or a claimed cold/residual
            // candidate; inferring from Option<RowVersion> caused RDB-0008.
            let is_insert = store
                .write_set_ref()
                .and_then(|ws| ws.get(row_id))
                .is_some_and(|entry| {
                    entry.observation == crate::mvcc::version_store::WriteObservation::Absent
                });

            if is_insert {
                // PK check against cold
                if !integer_primary_key_batch_checked {
                    if let Some(pk_col) = pk_idx {
                        if let Some(pk_val) = row.get(pk_col) {
                            if !pk_val.is_null() {
                                if let Some(cold_rid) = mgr.check_value_exists_with_snapshot(
                                    &cold_snapshot,
                                    pk_col,
                                    pk_val,
                                )? {
                                    if !mgr.is_pending_tombstone(txn_id, cold_rid) {
                                        return Err(radixdb_core::Error::PrimaryKeyConstraint {
                                            row_id: cold_rid,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                // UNIQUE constraint checks against cold
                for (col_indices, col_names, defaults) in &unique_indexes {
                    let coerced: Vec<radixdb_core::Value> = col_indices
                        .iter()
                        .filter_map(|&idx| {
                            let val = row.get(idx)?;
                            Some(val.coerce_to_type(schema.columns[idx].data_type))
                        })
                        .collect();
                    if coerced.len() != col_indices.len() || coerced.iter().any(|v| v.is_null()) {
                        continue;
                    }
                    let values: Vec<&radixdb_core::Value> = coerced.iter().collect();
                    if let Some(cold_rid) = mgr.find_row_id_by_values_with_snapshot(
                        &cold_snapshot,
                        col_indices,
                        &values,
                        defaults,
                    )? {
                        if !mgr.is_pending_tombstone(txn_id, cold_rid) {
                            return Err(radixdb_core::Error::UniqueConstraint {
                                index: col_names.join("_"),
                                column: col_names.join(", "),
                                value: values
                                    .iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", "),
                                row_id: cold_rid,
                            });
                        }
                    }
                }
            } else {
                // UPDATE: check PK if it changed
                if let Some(pk_col) = pk_idx {
                    let new_pk = row.get(pk_col);
                    let old_pk = store
                        .write_set_ref()
                        .and_then(|ws| ws.get(row_id))
                        .and_then(|entry| entry.read_version.as_ref())
                        .and_then(|rv| rv.data.get(pk_col));
                    if new_pk != old_pk {
                        if let Some(pk_val) = new_pk {
                            if !pk_val.is_null() {
                                if let Some(cold_rid) = mgr.check_value_exists_with_snapshot(
                                    &cold_snapshot,
                                    pk_col,
                                    pk_val,
                                )? {
                                    if cold_rid != row_id
                                        && !mgr.is_pending_tombstone(txn_id, cold_rid)
                                    {
                                        return Err(radixdb_core::Error::PrimaryKeyConstraint {
                                            row_id: cold_rid,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                // UPDATE: check unique constraints excluding the row being updated.
                // Skip when the indexed columns haven't changed.
                let old_row = store
                    .write_set_ref()
                    .and_then(|ws| ws.get(row_id))
                    .and_then(|entry| entry.read_version.as_ref())
                    .map(|rv| &rv.data);

                for (col_indices, col_names, defaults) in &unique_indexes {
                    // Skip if none of the indexed columns changed
                    if let Some(old_r) = old_row {
                        let any_changed = col_indices
                            .iter()
                            .any(|&idx| row.get(idx) != old_r.get(idx));
                        if !any_changed {
                            continue;
                        }
                    }

                    let coerced: Vec<radixdb_core::Value> = col_indices
                        .iter()
                        .filter_map(|&idx| {
                            let val = row.get(idx)?;
                            Some(val.coerce_to_type(schema.columns[idx].data_type))
                        })
                        .collect();
                    if coerced.len() != col_indices.len() || coerced.iter().any(|v| v.is_null()) {
                        continue;
                    }
                    let values: Vec<&radixdb_core::Value> = coerced.iter().collect();
                    if let Some(cold_rid) = mgr.find_row_id_by_values_with_snapshot(
                        &cold_snapshot,
                        col_indices,
                        &values,
                        defaults,
                    )? {
                        if cold_rid != row_id && !mgr.is_pending_tombstone(txn_id, cold_rid) {
                            return Err(radixdb_core::Error::UniqueConstraint {
                                index: col_names.join("_"),
                                column: col_names.join(", "),
                                value: values
                                    .iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", "),
                                row_id: cold_rid,
                            });
                        }
                    }
                }
            }
        }
        instrumentation::record_cold_constraint_batch(
            pending_rows,
            cold_snapshot.seg_ids.len(),
            primary_key_elapsed,
            validation_started.elapsed(),
        );
        Ok(())
    }

    /// Materialize pending versions for one table before the ordered WAL batch.
    fn collect_table_wal_operations(
        &self,
        table: &MVCCTable,
        operations: &mut Vec<PendingDmlWalOperation>,
    ) -> Result<()> {
        let table_id = radixdb_catalog::ObjectId::from_user_bytes(table.schema().catalog_id())
            .map_err(|error| {
                Error::internal(format!(
                    "table '{}' has invalid catalog identity for WAL: {error}",
                    table.name()
                ))
            })?;
        let pending = table.get_pending_versions();

        for (row_id, row_data, is_deleted, version_txn_id, create_time) in pending {
            let version = RowVersion {
                txn_id: version_txn_id,
                deleted_at_txn_id: if is_deleted { version_txn_id } else { 0 },
                data: row_data,
                create_time,
            };
            let operation = if is_deleted {
                WALOperationType::Delete
            } else {
                WALOperationType::Insert
            };
            operations.push(PendingDmlWalOperation {
                table_id,
                row_id,
                operation,
                version,
            });
        }
        Ok(())
    }

    fn transaction_value_exists(
        &self,
        txn_id: i64,
        table_name: &str,
        column_name: &str,
        value: &Value,
    ) -> Result<bool> {
        let table = self.get_table_for_transaction(txn_id, table_name)?;
        let (_, column) = table
            .schema()
            .find_column(column_name)
            .ok_or_else(|| Error::ColumnNotFound(column_name.to_string()))?;
        if column.primary_key && column.data_type == DataType::Integer {
            if let Value::Integer(row_id) = value {
                let mut matches = [false];
                // Commit preflight serializes writers but deliberately leaves
                // readers and table membership available. Use the ordinary
                // fenced probe while validating the current committed parent.
                let hits = table.probe_visible_row_ids(&[*row_id], &mut matches)?;
                return Ok(hits == 1 && matches[0]);
            }
        }
        let mut filter = crate::expression::ComparisonExpr::new(
            column.name.as_str(),
            radixdb_core::Operator::Eq,
            value.clone(),
        );
        filter.prepare_for_schema(table.schema());
        Ok(!table
            .collect_rows_with_limit_unordered(Some(&filter), 1, 0)?
            .is_empty())
    }

    /// Revalidate ordinary DML constraints at the global commit-visibility
    /// boundary. Statement checks remain useful diagnostics, but only this
    /// serialized pass can close child-insert/parent-delete write skew.
    fn validate_pending_dml_constraints(&self, txn_id: i64) -> Result<()> {
        struct PendingTableRows {
            name: String,
            schema: CompactArc<Schema>,
            rows: FxHashMap<i64, (Option<Row>, Option<Row>)>,
        }

        let touched: Vec<_> = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .map(|tables| {
                tables
                    .iter()
                    .map(|(name, store)| (name.clone(), Arc::clone(store)))
                    .collect()
            })
            .unwrap_or_default();

        let mut pending_tables = Vec::with_capacity(touched.len());
        for (table_name, store) in touched {
            let version_store = self
                .version_store_for_transaction(txn_id, table_name.as_str())
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
            let (schema, mut rows) = {
                let store = store.read().unwrap();
                let schema = store
                    .schema_override()
                    .unwrap_or_else(|| version_store.schema().clone());
                let rows = store
                    .iter_local_with_old()
                    .map(|(row_id, version, old)| {
                        (
                            row_id,
                            (
                                old.cloned(),
                                (!version.is_deleted()).then(|| version.data.clone()),
                            ),
                        )
                    })
                    .collect::<FxHashMap<_, _>>();
                (schema, rows)
            };

            let manager = self
                .segment_managers
                .read()
                .unwrap()
                .get(table_name.as_str())
                .cloned();
            if let Some(manager) = manager {
                for row_id in manager.get_pending_tombstones(txn_id) {
                    if rows.get(&row_id).is_some_and(|(old, _)| old.is_some()) {
                        continue;
                    }
                    if let Some(old) = manager.get_cold_row_normalized(row_id, &schema)? {
                        rows.entry(row_id)
                            .and_modify(|entry| entry.0 = Some(old.clone()))
                            .or_insert((Some(old), None));
                    }
                }
            }

            if !rows.is_empty() {
                pending_tables.push(PendingTableRows {
                    name: table_name.to_string(),
                    schema,
                    rows,
                });
            }
        }

        for table in &pending_tables {
            let mut row_validator = self.bind_row_validator(&table.schema)?;
            for (_, new_row) in table.rows.values() {
                let Some(new_row) = new_row else {
                    continue;
                };
                row_validator.validate(new_row)?;
                for foreign_key in table.schema.foreign_keys() {
                    let Some(value) = new_row.get(foreign_key.column_index) else {
                        return Err(Error::internal(format!(
                            "row in '{}' is missing foreign-key column '{}'",
                            table.name, foreign_key.column_name
                        )));
                    };
                    if value.is_null() {
                        continue;
                    }
                    if !self.transaction_value_exists(
                        txn_id,
                        &foreign_key.referenced_table,
                        &foreign_key.referenced_column,
                        value,
                    )? {
                        return Err(Error::foreign_key_violation(
                            &table.name,
                            &foreign_key.column_name,
                            &foreign_key.referenced_table,
                            &foreign_key.referenced_column,
                            format!(
                                "referenced row with {} = {} does not exist at commit",
                                foreign_key.referenced_column, value
                            ),
                        ));
                    }
                }
            }
        }

        let schemas: Vec<_> = self.schemas.read().unwrap().values().cloned().collect();
        for parent in &pending_tables {
            for child_schema in &schemas {
                for foreign_key in child_schema.foreign_keys() {
                    if !foreign_key
                        .referenced_table
                        .eq_ignore_ascii_case(&parent.name)
                    {
                        continue;
                    }
                    let parent_column = parent
                        .schema
                        .get_column_index(&foreign_key.referenced_column)
                        .ok_or_else(|| {
                            Error::ColumnNotFound(foreign_key.referenced_column.clone())
                        })?;
                    for (old_row, new_row) in parent.rows.values() {
                        let Some(old_value) = old_row
                            .as_ref()
                            .and_then(|row| row.get(parent_column))
                            .filter(|value| !value.is_null())
                        else {
                            continue;
                        };
                        if new_row.as_ref().and_then(|row| row.get(parent_column))
                            == Some(old_value)
                        {
                            continue;
                        }
                        // A cold-row UPDATE is represented by a tombstone for
                        // the immutable row plus a new hot row. Those two
                        // physical mutations can have different row IDs, so
                        // this entry alone may look like removal even though
                        // the transaction-final parent view still contains the
                        // exact same referenced identity. Referential actions
                        // apply to key disappearance, not row relocation.
                        if self.transaction_value_exists(
                            txn_id,
                            &parent.name,
                            &foreign_key.referenced_column,
                            old_value,
                        )? {
                            continue;
                        }
                        if self.transaction_value_exists(
                            txn_id,
                            &child_schema.table_name,
                            &foreign_key.column_name,
                            old_value,
                        )? {
                            return Err(Error::foreign_key_violation(
                                &child_schema.table_name,
                                &foreign_key.column_name,
                                &parent.name,
                                &foreign_key.referenced_column,
                                format!(
                                    "referencing rows still exist for {} = {} at commit",
                                    foreign_key.referenced_column, old_value
                                ),
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl TransactionEngineOperations for EngineOperations {
    fn stage_schema_change(&self, txn_id: i64, change: &PendingSchemaChange) -> Result<()> {
        // Ensure the transaction-local store exists, then replace its complete
        // schema overlay. This supports every ALTER shape without publishing a
        // partially mutated shared catalog.
        let _ = self.get_table_for_transaction(txn_id, &change.table_name)?;
        let store = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .and_then(|tables| {
                tables
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(&change.table_name))
                    .map(|(_, store)| Arc::clone(store))
            })
            .ok_or_else(|| Error::internal("transaction schema store was not initialized"))?;

        store
            .write()
            .unwrap()
            .set_schema_override(CompactArc::new(change.schema.clone()));
        Ok(())
    }

    fn reset_schema_changes(&self, txn_id: i64, changes: &[PendingSchemaChange]) -> Result<()> {
        let stores: Vec<(String, Arc<RwLock<TransactionVersionStore>>)> = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .map(|tables| {
                tables
                    .iter()
                    .map(|(name, store)| (name.to_string(), Arc::clone(store)))
                    .collect()
            })
            .unwrap_or_default();

        for (table_name, store) in stores {
            let last_change = changes
                .iter()
                .rev()
                .find(|change| change.table_name.eq_ignore_ascii_case(&table_name));
            let mut guard = store.write().unwrap();
            let Some(change) = last_change else {
                guard.clear_schema_override();
                continue;
            };
            guard.set_schema_override(CompactArc::new(change.schema.clone()));
        }
        Ok(())
    }

    fn stage_index_drop(&self, txn_id: i64, drop: &PendingIndexDrop) -> Result<()> {
        let _ = self.get_table_for_transaction(txn_id, &drop.table_name)?;
        let store = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .and_then(|tables| {
                tables
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(&drop.table_name))
                    .map(|(_, store)| Arc::clone(store))
            })
            .ok_or_else(|| Error::internal("transaction index-drop store was not initialized"))?;
        let result = store
            .write()
            .unwrap()
            .disable_parent_index(&drop.index_name);
        result
    }

    fn reset_index_drops(&self, txn_id: i64, drops: &[PendingIndexDrop]) -> Result<()> {
        let stores: Vec<(String, Arc<RwLock<TransactionVersionStore>>)> = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .map(|tables| {
                tables
                    .iter()
                    .map(|(name, store)| (name.to_string(), Arc::clone(store)))
                    .collect()
            })
            .unwrap_or_default();
        for (table_name, store) in stores {
            store.write().unwrap().reset_disabled_parent_indexes(
                drops
                    .iter()
                    .filter(|drop| drop.table_name.eq_ignore_ascii_case(&table_name))
                    .map(|drop| drop.index_name.as_str()),
            )?;
        }
        Ok(())
    }

    fn get_table_for_transaction(&self, txn_id: i64, table_name: &str) -> Result<Box<dyn Table>> {
        // Use Cow to avoid allocation when table_name is already lowercase (common case)
        let table_name_lower = to_lowercase_cow(table_name);

        // Resolve an unpublished table only for its owner. Other transactions
        // must not observe CREATE TABLE before the catalog publication point.
        let pending_store = {
            let pending = self.pending_tables.read().unwrap();
            pending.get(&*table_name_lower).and_then(|table| {
                (table.owner_txn_id == txn_id).then(|| Arc::clone(&table.version_store))
            })
        };
        let unpublished_table = pending_store.is_some();
        let version_store = if let Some(store) = pending_store {
            store
        } else {
            let stores = self.version_stores().read().unwrap();
            stores
                .get(&*table_name_lower)
                .cloned()
                .ok_or_else(|| Error::TableNotFound(table_name_lower.to_string()))?
        };

        // Check if we have a cached transaction version store for this (txn_id, table_name)
        let txn_versions = {
            let cache = self.txn_version_stores().read().unwrap();
            let found = if let Some(txn_tables) = cache.get(txn_id) {
                // Linear search on SmallVec (fast for 1-2 tables)
                txn_tables
                    .iter()
                    .find(|(name, _)| name == &*table_name_lower)
                    .map(|(_, cached)| Arc::clone(cached))
            } else {
                None
            };
            drop(cache);

            if let Some(cached) = found {
                cached
            } else {
                // Upgrade to write lock and re-check (another thread may have inserted)
                let mut cache = self.txn_version_stores().write().unwrap();
                let txn_tables = cache.entry(txn_id).or_default();
                if let Some((_, cached)) = txn_tables
                    .iter()
                    .find(|(name, _)| name == &*table_name_lower)
                {
                    Arc::clone(cached)
                } else {
                    let new_store = Arc::new(RwLock::new(TransactionVersionStore::new(
                        Arc::clone(&version_store),
                        txn_id,
                    )));
                    txn_tables.push((
                        table_name_lower.clone().into_owned().into(),
                        Arc::clone(&new_store),
                    ));
                    new_store
                }
            }
        };

        let schema_override = txn_versions.read().unwrap().schema_override();
        // Create MVCC table with the transaction-private ALTER overlay, if any.
        let table = MVCCTable::new_with_shared_store_and_schema(
            txn_id,
            version_store,
            txn_versions,
            schema_override,
        );

        // A handle must retain the table's future cold owner even before the
        // first segment exists. Otherwise a checkpoint can move rows from hot
        // to cold after planning while this long-lived handle keeps probing
        // only the now-empty VersionStore. This was observable as transient
        // orphan rows in an indexed LEFT anti-join during concurrent CHECKPOINT.
        // Private CREATE TABLE state cannot be sealed before publication and
        // therefore does not acquire a durable manager yet.
        let existing_manager = self
            .segment_managers
            .read()
            .unwrap()
            .get(&*table_name_lower)
            .cloned();
        let manager = match existing_manager {
            Some(manager) => Some(manager),
            None if !unpublished_table
                && self.persistence().is_some_and(|owner| owner.is_enabled()) =>
            {
                Some(self.get_or_create_segment_manager(&table_name_lower))
            }
            None => None,
        };
        if let Some(manager) = manager {
            if self.registry.get_isolation_level(txn_id) == IsolationLevel::SnapshotIsolation {
                let begin_seq = self.registry.get_transaction_begin_sequence(txn_id) as u64;
                return Ok(Box::new(
                    crate::volume::table::SegmentedTable::with_snapshot_seq(
                        Box::new(table),
                        manager,
                        begin_seq,
                    ),
                ));
            }
            return Ok(Box::new(crate::volume::table::SegmentedTable::new(
                Box::new(table),
                manager,
            )));
        }

        Ok(Box::new(table))
    }

    fn create_table(&self, txn_id: i64, name: &str, mut schema: Schema) -> Result<Box<dyn Table>> {
        let _ddl_guard = DdlFenceGuard::exclusive(Arc::clone(&self.ddl_fence));
        let table_name = name.to_lowercase();
        if schema.table_name_lower != table_name {
            return Err(Error::invalid_argument(format!(
                "transactional CREATE TABLE name '{}' does not match schema name '{}'",
                name, schema.table_name
            )));
        }
        schema.ensure_catalog_identity();
        schema.ensure_constraint_catalog()?;
        self.validate_schema(&schema)?;

        // Create version store for this table (before acquiring locks)
        let version_store = Arc::new(VersionStore::with_transaction_registry(
            schema.table_name.clone(),
            schema.clone(),
            registry_as_visibility_checker(&self.registry),
        ));

        // Register PkIndex if table has a primary key
        register_pk_index(&schema, &version_store)?;

        // Reserve the catalog name without publishing it. The global schema map
        // remains unchanged until this transaction commits.
        {
            let schemas = self.schemas().read().unwrap();
            let mut pending = self.pending_tables.write().unwrap();
            let views = self.views.read().unwrap();
            if pending.contains_key(&table_name) {
                return Err(Error::TableAlreadyExists(table_name.clone()));
            }
            if schemas.contains_key(&table_name) {
                return Err(Error::TableAlreadyExists(table_name));
            }
            if views.contains_key(&table_name) {
                return Err(Error::ViewAlreadyExists(name.to_string()));
            }
            pending.insert(
                table_name.clone(),
                PendingTable {
                    owner_txn_id: txn_id,
                    version_store: Arc::clone(&version_store),
                },
            );
        }

        // A table created inside a transaction must use the same registered
        // store as every existing-table handle.  The commit protocol discovers
        // DML exclusively through txn_version_stores; returning an unregistered
        // store here would let INSERTs succeed on the private handle while the
        // record/publish phases durably commit only the CREATE TABLE entry.
        let txn_versions = Arc::new(RwLock::new(TransactionVersionStore::new(
            Arc::clone(&version_store),
            txn_id,
        )));
        self.txn_version_stores()
            .write()
            .unwrap()
            .entry(txn_id)
            .or_default()
            .push((table_name.clone().into(), Arc::clone(&txn_versions)));

        let table = MVCCTable::new_with_shared_store(txn_id, version_store, txn_versions);

        Ok(Box::new(table))
    }

    fn restore_table(&self, name: &str, schema: Schema) -> Result<()> {
        let table_name = name.to_lowercase();
        let version_store = Arc::new(VersionStore::with_transaction_registry(
            schema.table_name.clone(),
            schema.clone(),
            registry_as_visibility_checker(&self.registry),
        ));
        register_pk_index(&schema, &version_store)?;
        {
            let mut schemas = self.schemas().write().unwrap();
            if schemas.contains_key(&table_name) {
                return Err(Error::TableAlreadyExists(table_name));
            }
            schemas.insert(table_name.clone(), CompactArc::new(schema));
        }
        self.version_stores
            .write()
            .unwrap()
            .insert(table_name, version_store);
        self.schema_epoch.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        let table_name_lower = name.to_lowercase();

        // This callback is now only the cancellation/rollback owner for an
        // unpublished CREATE TABLE reservation. Existing-table DROP is staged
        // by MvccTransaction and published after its shared commit marker.
        self.pending_tables
            .write()
            .unwrap()
            .remove(&table_name_lower)
            .map(|_| ())
            .ok_or(Error::TableNotFound(table_name_lower))
    }

    fn list_tables(&self) -> Result<Vec<String>> {
        let schemas = self.schemas().read().unwrap();
        Ok(schemas.keys().cloned().collect())
    }

    fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        Err(Error::NotSupported(format!(
            "RENAME TABLE '{old_name}' TO '{new_name}' is not supported inside an explicit transaction"
        )))
    }

    fn record_commit(&self, txn_id: i64) -> Result<u64> {
        // Skip WAL writes during recovery replay
        if self.should_skip_wal() {
            return Ok(0);
        }

        // Record commit in WAL — propagate errors since missing commit records
        // means crash recovery won't replay this transaction's changes
        if let Some(pm) = self.persistence() {
            if pm.is_enabled() {
                if let Some(prepared) = self.prepared_catalog.lock().unwrap().as_ref() {
                    return pm.record_catalog_commit(
                        txn_id,
                        prepared.next_catalog_id,
                        prepared.created_unix_ns,
                        &prepared.mutation,
                    );
                }
                return pm.record_commit(txn_id);
            }
        }
        Ok(0)
    }

    fn record_rollback(&self, txn_id: i64) -> Result<()> {
        // Skip WAL writes during recovery replay
        if self.should_skip_wal() {
            return Ok(());
        }

        // Record rollback in WAL
        if let Some(pm) = self.persistence() {
            if pm.is_enabled() {
                pm.record_rollback(txn_id)?;
            }
        }
        Ok(())
    }

    fn get_tables_with_pending_changes(&self, txn_id: i64) -> Result<Vec<Box<dyn Table>>> {
        let touched: Vec<(radixdb_core::SmartString, bool)> = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .map(|txn_tables| {
                txn_tables
                    .iter()
                    .map(|(table_name, txn_store)| {
                        (
                            table_name.clone(),
                            txn_store.read().unwrap().has_local_changes(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let managers = self.segment_managers.read().unwrap();
        let pending_names: Vec<_> = touched
            .into_iter()
            .filter_map(|(table_name, has_hot)| {
                let has_cold = managers.get(table_name.as_str()).is_some_and(|manager| {
                    manager.has_pending_tombstones(txn_id)
                        || manager.has_pending_cold_index_removals(txn_id)
                });
                (has_hot || has_cold).then_some(table_name)
            })
            .collect();
        drop(managers);

        pending_names
            .into_iter()
            .map(|table_name| self.get_table_for_transaction(txn_id, table_name.as_str()))
            .collect()
    }

    fn has_pending_dml_changes(&self, txn_id: i64) -> bool {
        // Check hot-side DML changes
        let cache = self.txn_version_stores().read().unwrap();
        let txn_tables = match cache.get(txn_id) {
            Some(tables) if !tables.is_empty() => tables,
            _ => {
                // No txn_version_store entry means this txn never accessed any table
                // for DML, so no pending tombstones can exist either.
                return false;
            }
        };

        // Phase 1: Check hot mutations. Return immediately if any found.
        // Do NOT collect table names here — avoids SmartString clones
        // on the common early-return path.
        for (_, txn_store) in txn_tables.iter() {
            if txn_store.read().unwrap().has_local_changes() {
                return true;
            }
        }

        // Phase 2: No hot changes found. Now collect table names for cold check.
        let touched_tables: smallvec::SmallVec<[radixdb_core::SmartString; 4]> =
            txn_tables.iter().map(|(name, _)| name.clone()).collect();
        drop(cache);

        // Check cold-side pending tombstones only for tables this txn touched.
        // Cold DELETE/UPDATE on non-int-pk tables may create tombstones without
        // hot mutations, but they always go through get_table_for_transaction first.
        let mgrs = self.segment_managers.read().unwrap();
        for table_name in &touched_tables {
            if let Some(mgr) = mgrs.get(table_name.as_str()) {
                if mgr.has_pending_tombstones(txn_id) || mgr.has_pending_cold_index_removals(txn_id)
                {
                    return true;
                }
            }
        }
        false
    }

    fn validate_transaction_commit(&self, txn_id: i64) -> Result<()> {
        // Cold rows keep their shared index postings until this commit
        // boundary. Move the transaction-private removal plan into the same
        // MVCC store that will atomically apply hot index additions/removals.
        {
            let touched: Vec<_> = self
                .txn_version_stores()
                .read()
                .unwrap()
                .get(txn_id)
                .map(|entries| {
                    entries
                        .iter()
                        .map(|(name, store)| (name.clone(), Arc::clone(store)))
                        .collect()
                })
                .unwrap_or_default();
            for (table_name, txn_store) in touched {
                let manager = self
                    .segment_managers
                    .read()
                    .unwrap()
                    .get(table_name.as_str())
                    .cloned();
                let Some(manager) = manager else {
                    continue;
                };
                let removals = manager.pending_cold_index_removals(txn_id);
                if removals.is_empty() {
                    continue;
                }
                let mut txn_store = txn_store.write().unwrap();
                for (index, values, row_id) in removals {
                    txn_store.stage_external_index_removal(index, values, row_id)?;
                }
            }
        }

        let tables: Vec<(
            radixdb_core::SmartString,
            Arc<RwLock<TransactionVersionStore>>,
            Arc<VersionStore>,
        )> = {
            let cache = self.txn_version_stores().read().unwrap();
            cache
                .get(txn_id)
                .map(|entries| {
                    entries
                        .iter()
                        .filter(|(_, store)| store.read().unwrap().has_local_changes())
                        .filter_map(|(table_name, store)| {
                            self.version_store_for_transaction(txn_id, table_name.as_str())
                                .map(|version_store| {
                                    (table_name.clone(), Arc::clone(store), version_store)
                                })
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

        for (table_name, txn_store, version_store) in tables {
            let manager = self
                .segment_managers
                .read()
                .unwrap()
                .get(table_name.as_str())
                .cloned();
            let _segment_guard = manager.as_ref().map(|manager| manager.acquire_seal_read());
            if let Some(manager) = manager.as_ref() {
                let recorded = manager.get_txn_seal_generation(txn_id);
                let needs_recheck = match recorded {
                    Some(generation) => generation != manager.seal_generation(),
                    None => manager.has_segments(),
                };
                if needs_recheck {
                    self.validate_pending_against_cold(
                        txn_id,
                        &txn_store,
                        &version_store,
                        manager,
                    )?;
                    manager.refresh_txn_seal_generation_after_validation(txn_id);
                }
            }
            txn_store.read().unwrap().validate_commit()?;
        }
        self.validate_pending_dml_constraints(txn_id)?;
        Ok(())
    }

    fn record_transaction_dml(&self, txn_id: i64) -> Result<()> {
        if self.should_skip_wal() {
            return Ok(());
        }
        let Some(pm) = self.persistence().filter(|pm| pm.is_enabled()) else {
            return Ok(());
        };

        let (mut hot_tables, mut touched_names) = {
            let cache = self.txn_version_stores().read().unwrap();
            let Some(txn_tables) = cache.get(txn_id) else {
                return Ok(());
            };
            let mut hot_tables = Vec::new();
            let mut touched_names = Vec::with_capacity(txn_tables.len());
            for (table_name, txn_store) in txn_tables.iter() {
                touched_names.push(table_name.to_string());
                if txn_store.read().unwrap().has_local_changes() {
                    if let Some(version_store) =
                        self.version_store_for_transaction(txn_id, table_name.as_str())
                    {
                        hot_tables.push((table_name.clone(), Arc::clone(txn_store), version_store));
                    }
                }
            }
            (hot_tables, touched_names)
        };
        hot_tables.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        touched_names.sort_unstable();
        touched_names.dedup();

        let managers: ahash::AHashMap<String, Arc<crate::volume::manifest::SegmentManager>> = {
            let managers = self.segment_managers.read().unwrap();
            touched_names
                .iter()
                .filter_map(|name| {
                    managers
                        .get(name)
                        .map(|manager| (name.clone(), Arc::clone(manager)))
                })
                .collect()
        };

        let mut operations = Vec::new();
        let mut tombstones_recorded = rustc_hash::FxHashSet::default();
        for (table_name, txn_store, version_store) in &hot_tables {
            let table_id =
                radixdb_catalog::ObjectId::from_user_bytes(version_store.schema().catalog_id())
                    .map_err(|error| {
                        Error::internal(format!(
                            "table '{}' has invalid catalog identity for WAL: {error}",
                            table_name
                        ))
                    })?;
            if let Some(manager) = managers.get(table_name.as_str()) {
                for row_id in manager.get_pending_tombstones(txn_id) {
                    let version = RowVersion {
                        txn_id,
                        deleted_at_txn_id: txn_id,
                        data: Row::new(),
                        create_time: get_fast_timestamp(),
                    };
                    operations.push(PendingDmlWalOperation {
                        table_id,
                        row_id,
                        operation: WALOperationType::Delete,
                        version,
                    });
                }
                tombstones_recorded.insert(table_name.to_string());
            }

            let table = MVCCTable::new_with_shared_store(
                txn_id,
                Arc::clone(version_store),
                Arc::clone(txn_store),
            );
            self.collect_table_wal_operations(&table, &mut operations)?;
        }

        for table_name in touched_names {
            if tombstones_recorded.contains(&table_name) {
                continue;
            }
            let Some(manager) = managers.get(&table_name) else {
                continue;
            };
            let store = self
                .version_stores
                .read()
                .unwrap()
                .get(&table_name)
                .cloned()
                .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
            let table_id = radixdb_catalog::ObjectId::from_user_bytes(store.schema().catalog_id())
                .map_err(|error| {
                    Error::internal(format!(
                        "table '{}' has invalid catalog identity for WAL: {error}",
                        table_name
                    ))
                })?;
            for row_id in manager.get_pending_tombstones(txn_id) {
                let version = RowVersion {
                    txn_id,
                    deleted_at_txn_id: txn_id,
                    data: Row::new(),
                    create_time: get_fast_timestamp(),
                };
                operations.push(PendingDmlWalOperation {
                    table_id,
                    row_id,
                    operation: WALOperationType::Delete,
                    version,
                });
            }
        }
        pm.record_dml_operations(txn_id, operations, &self.storage_cpu_runtime)?;
        Ok(())
    }

    fn publish_transaction_dml(&self, txn_id: i64) -> Result<()> {
        let (mut hot_tables, mut touched_names) = {
            let cache = self.txn_version_stores().read().unwrap();
            let Some(txn_tables) = cache.get(txn_id) else {
                return Ok(());
            };
            let mut hot_tables = Vec::new();
            let mut touched_names = Vec::with_capacity(txn_tables.len());
            for (table_name, txn_store) in txn_tables.iter() {
                touched_names.push(table_name.to_string());
                if txn_store.read().unwrap().has_local_changes() {
                    if let Some(version_store) =
                        self.version_store_for_transaction(txn_id, table_name.as_str())
                    {
                        hot_tables.push((table_name.clone(), Arc::clone(txn_store), version_store));
                    }
                }
            }
            (hot_tables, touched_names)
        };
        hot_tables.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        touched_names.sort_unstable();
        touched_names.dedup();

        let managers: ahash::AHashMap<String, Arc<crate::volume::manifest::SegmentManager>> = {
            let managers = self.segment_managers.read().unwrap();
            touched_names
                .iter()
                .filter_map(|name| {
                    managers
                        .get(name)
                        .map(|manager| (name.clone(), Arc::clone(manager)))
                })
                .collect()
        };

        let mut failed_tables = rustc_hash::FxHashSet::default();
        let mut first_error = None;
        for (table_name, txn_store, version_store) in hot_tables {
            let mut table = MVCCTable::new_with_shared_store(txn_id, version_store, txn_store);
            if let Err(error) = table.commit_retaining_claims() {
                failed_tables.insert(table_name.to_string());
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        for table_name in touched_names {
            let Some(manager) = managers.get(&table_name) else {
                continue;
            };
            if failed_tables.contains(&table_name) {
                manager.rollback_cold_index_removals(txn_id);
                manager.rollback_pending_tombstones(txn_id);
                manager.clear_txn_seal_generation(txn_id);
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn publish_committed_transaction_storage(&self, txn_id: i64, visibility_seq: u64) {
        #[cfg(any(test, feature = "test-failpoints"))]
        crate::test_failpoints::interleave_scoped(
            self.schema_scope_id,
            crate::test_failpoints::InterleavePoint::CommittedStorageBeforePublish,
            txn_id,
        );
        let mut touched_names: Vec<String> = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .map(|tables| {
                tables
                    .iter()
                    .map(|(table_name, _)| table_name.to_string())
                    .collect()
            })
            .unwrap_or_default();
        touched_names.sort_unstable();
        touched_names.dedup();

        let managers = self.segment_managers.read().unwrap();
        for table_name in touched_names {
            let Some(manager) = managers.get(&table_name) else {
                continue;
            };
            // Compaction owns the exclusive side while it derives, commits
            // and installs one exact physical/runtime tombstone generation.
            // A transaction may have prepared a cold mutation before that
            // boundary, so promote its pending storage state only while
            // holding the shared side. Otherwise the durable generation can
            // commit between the runtime tombstone load and replacement and
            // the transaction can mutate the map underneath that publication.
            let _segment_publication_guard = manager.acquire_seal_read();
            manager.commit_cold_index_removals(txn_id);
            manager.commit_pending_tombstones(txn_id, visibility_seq);
            manager.clear_txn_seal_generation(txn_id);
        }
    }

    fn finalize_transaction_commit(&self, txn_id: i64) {
        // Dropping each TransactionVersionStore releases its row claims and
        // wakes waiters. The caller invokes this only after TransactionRegistry
        // has published the commit, so every awakened READ COMMITTED writer
        // re-reads the new version rather than the previous one.
        let touched: SmallVec<[SmartString; 4]> = {
            let mut stores = self.txn_version_stores().write().unwrap();
            let touched = stores
                .get(txn_id)
                .map(|tables| tables.iter().map(|(name, _)| name.clone()).collect())
                .unwrap_or_default();
            stores.remove(txn_id);
            touched
        };
        if self.touched_tables_exceed_seal_pressure(&touched) {
            self.pressure_seal.request();
        }
    }

    fn rollback_all_tables(&self, txn_id: i64) {
        // Collect touched table names BEFORE removing the cache entry,
        // so we only rollback tombstones on tables this txn actually used
        // instead of iterating every segment manager (O(tables) → O(touched)).
        let mut cache = self.txn_version_stores().write().unwrap();
        let touched: smallvec::SmallVec<[radixdb_core::SmartString; 4]> = cache
            .get(txn_id)
            .map(|tables| tables.iter().map(|(name, _)| name.clone()).collect())
            .unwrap_or_default();
        cache.remove(txn_id);
        drop(cache);

        // Rollback pending tombstones and clean up seal generation records
        // only on tables this transaction touched.
        if !touched.is_empty() {
            let mgrs = self.segment_managers.read().unwrap();
            for name in &touched {
                if let Some(mgr) = mgrs.get(name.as_str()) {
                    mgr.rollback_cold_index_removals(txn_id);
                    mgr.rollback_pending_tombstones(txn_id);
                    mgr.clear_txn_seal_generation(txn_id);
                }
            }
        }
    }

    fn wait_for_storage_pressure(&self, txn_id: i64) -> Result<()> {
        if self.hot_storage_requires_backpressure() {
            self.pressure_seal.wait_before_commit();
        }
        self.wait_for_compaction_pressure(txn_id)
    }

    fn acquire_seal_fence(&self, txn_id: i64) -> Option<SealFenceGuard> {
        let _ = txn_id;
        Some(SealFenceGuard::new(Arc::clone(&self.seal_fence)))
    }

    fn lock_commit_membership_fences(&self, txn_id: i64, guard: &mut SealFenceGuard) {
        // A transaction may touch several tables. Acquire the corresponding
        // VersionStore publication fences in lexical order, otherwise two
        // multi-table commits could deadlock on inverse table order. The caller
        // already owns global seal shared + visibility exclusive.
        let mut table_names: Vec<String> = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .map(|tables| {
                tables
                    .iter()
                    .map(|(table_name, _)| table_name.to_string())
                    .collect()
            })
            .unwrap_or_default();
        table_names.sort_unstable();
        table_names.dedup();

        let membership_fences: Vec<_> = {
            let stores = self.version_stores().read().unwrap();
            table_names
                .iter()
                .filter_map(|table_name| {
                    stores.get(table_name).map(|store| store.membership_fence())
                })
                .collect()
        };
        for membership_fence in membership_fences {
            guard.lock_membership_fence(membership_fence);
        }
    }

    fn acquire_ddl_fence(&self) -> Option<DdlFenceGuard> {
        Some(DdlFenceGuard::exclusive(Arc::clone(&self.ddl_fence)))
    }

    fn acquire_commit_visibility_fence(&self) -> Option<VisibilityFenceGuard> {
        Some(VisibilityFenceGuard::exclusive(Arc::clone(
            &self.visibility_fence,
        )))
    }

    fn prepare_transactional_ddl(
        &self,
        preparation: TransactionalDdlPreparation<'_>,
    ) -> Result<()> {
        let TransactionalDdlPreparation {
            txn_id,
            created_tables,
            dropped_tables,
            pending_indexes,
            pending_index_drops,
            pending_index_renames,
            pending_table_renames,
            pending_schema_changes,
            catalog_mutation,
        } = preparation;
        let has_physical_ddl = !created_tables.is_empty()
            || !dropped_tables.is_empty()
            || !pending_indexes.is_empty()
            || !pending_index_drops.is_empty()
            || !pending_index_renames.is_empty()
            || !pending_table_renames.is_empty()
            || !pending_schema_changes.is_empty();
        if has_physical_ddl && catalog_mutation.is_none() {
            return Err(Error::NotSupported(
                "physical DDL requires one authoritative typed catalog mutation".to_owned(),
            ));
        }

        // Prove every post-marker catalog insertion while the transaction owns
        // the exclusive DDL fence. Publication below is deliberately
        // infallible; no ordinary error may be returned after the shared
        // DDL+DML commit marker is durable.
        {
            let pending = self.pending_tables.read().unwrap();
            let schemas = self.schemas.read().unwrap();
            let stores = self.version_stores.read().unwrap();
            for table_name in created_tables {
                let lower = table_name.to_lowercase();
                let table = pending.get(&lower).ok_or_else(|| {
                    Error::internal(format!(
                        "transaction {} lost pending table reservation '{}'",
                        txn_id, table_name
                    ))
                })?;
                if table.owner_txn_id != txn_id {
                    return Err(Error::internal(format!(
                        "pending table '{}' belongs to transaction {}, not {}",
                        table_name, table.owner_txn_id, txn_id
                    )));
                }
                if schemas.contains_key(&lower) || stores.contains_key(&lower) {
                    return Err(Error::TableAlreadyExists(lower));
                }
                let schema = table.version_store.schema();
                if schema.table_name_lower != lower {
                    return Err(Error::invalid_argument(format!(
                        "pending table identity '{}' does not match schema '{}'",
                        table_name, schema.table_name
                    )));
                }
                self.validate_schema(&schema)?;
            }
        }

        let prepared_catalog = if let Some(mutation) = catalog_mutation {
            let current = self
                .catalog_publisher
                .load_full()
                .pin()
                .map_err(|error| Error::internal(format!("catalog pin failed: {error}")))?;
            let graph = mutation.apply(&current).map_err(|error| {
                Error::invalid_argument(format!("catalog mutation rejected: {error}"))
            })?;
            let candidate = radixdb_catalog::CatalogGeneration::new(current.meta(), graph);
            let runtime = self.catalog_runtime_binder.bind(&candidate)?;
            let (tables, views) = runtime.into_parts();
            if let Some(view) = {
                let pending = self.pending_tables.read().unwrap();
                views
                    .iter()
                    .find(|view| pending.contains_key(&view.name))
                    .map(|view| view.original_name.clone())
            } {
                return Err(Error::TableAlreadyExists(view));
            }
            let schemas = tables
                .into_iter()
                .map(|table| table.into_parts().0)
                .collect();
            let created_unix_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64;
            Some(PreparedCatalogCommit {
                mutation: mutation.clone(),
                next_catalog_id: radixdb_core::new_durable_identity_bytes(),
                created_unix_ns,
                schemas,
                views,
            })
        } else {
            None
        };

        {
            let schemas = self.schemas.read().unwrap();
            for (table_name, staged_schema) in dropped_tables {
                let current = schemas
                    .get(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
                if current.as_ref() != staged_schema {
                    return Err(Error::invalid_argument(format!(
                        "transactional DROP TABLE conflict on '{}': catalog changed; retry the transaction",
                        table_name
                    )));
                }
            }
        }

        let dropped_table_names: FxHashSet<String> = dropped_tables
            .iter()
            .map(|(name, _)| name.to_lowercase())
            .collect();
        if let Some((dropped, dependent)) = self.views.read().unwrap().values().find_map(|view| {
            view.dependencies.iter().find_map(|dependency| {
                dropped_table_names
                    .contains(dependency)
                    .then(|| (dependency.clone(), view.original_name.clone()))
            })
        }) {
            return Err(Error::NotSupported(format!(
                "cannot drop table '{}' while view '{}' depends on it",
                dropped, dependent
            )));
        }

        // The private overlay is positional. If another transaction changed
        // the table schema since staging, committing these rows under the new
        // layout would silently bind values to the wrong columns. Reject before
        // writing any user-transaction DDL/DML or commit marker.
        let schemas = self.schemas.read().unwrap();
        let created_table_names: FxHashSet<String> = created_tables
            .iter()
            .map(|name| name.to_lowercase())
            .collect();
        let mut final_schema_changes: FxHashMap<String, &PendingSchemaChange> =
            FxHashMap::default();
        for change in pending_schema_changes {
            final_schema_changes.insert(change.table_name.to_lowercase(), change);
        }
        let mut final_schema_changes: Vec<&PendingSchemaChange> =
            final_schema_changes.into_values().collect();
        final_schema_changes.sort_unstable_by(|left, right| {
            left.table_name
                .to_lowercase()
                .cmp(&right.table_name.to_lowercase())
        });
        for change in &final_schema_changes {
            if created_table_names.contains(&change.table_name.to_lowercase()) {
                continue;
            }
            let current = schemas
                .get(&change.table_name)
                .ok_or_else(|| Error::TableNotFound(change.table_name.clone()))?;
            if current.as_ref() != &change.expected_catalog_schema {
                return Err(Error::invalid_argument(format!(
                    "transactional DDL conflict on table '{}': catalog changed; retry the transaction",
                    change.table_name
                )));
            }
            self.validate_schema(&change.schema)?;
        }
        for change in pending_schema_changes {
            let Some(transition) = &change.physical_transition else {
                continue;
            };
            let store = self
                .version_stores
                .read()
                .unwrap()
                .get(&change.table_name)
                .cloned()
                .ok_or_else(|| Error::TableNotFound(change.table_name.clone()))?;
            match transition {
                SchemaPhysicalTransition::DropColumn {
                    column_name,
                    column_index,
                } => {
                    let schema = store.schema();
                    for index in store.get_all_indexes() {
                        if index
                            .column_ids()
                            .iter()
                            .any(|&id| id as usize == *column_index)
                        {
                            return Err(Error::NotSupported(format!(
                                "DROP COLUMN cannot remove indexed column '{}.{}' while index '{}' exists; drop the index first",
                                change.table_name,
                                column_name,
                                index.name()
                            )));
                        }
                        if let Some(predicate) = index.partial_predicate() {
                            let key_crosses_boundary = index
                                .column_ids()
                                .iter()
                                .any(|&id| id as usize > *column_index);
                            let predicate_crosses_boundary = predicate
                                .referenced_column_names()
                                .iter()
                                .filter_map(|name| schema.find_column(name).map(|(idx, _)| idx))
                                .any(|idx| idx >= *column_index);
                            if key_crosses_boundary || predicate_crosses_boundary {
                                return Err(Error::NotSupported(format!(
                                    "DROP COLUMN cannot shift partial index '{}' metadata",
                                    index.name()
                                )));
                            }
                        }
                    }
                }
                SchemaPhysicalTransition::RenameColumn { old_name, .. } => {
                    for index in store.get_all_indexes() {
                        if index.partial_predicate().is_some_and(|predicate| {
                            predicate
                                .referenced_column_names()
                                .iter()
                                .any(|name| name.eq_ignore_ascii_case(old_name))
                        }) {
                            return Err(Error::NotSupported(format!(
                                "RENAME COLUMN cannot rewrite partial index '{}' predicate",
                                index.name()
                            )));
                        }
                    }
                }
            }
        }
        for drop in pending_index_drops {
            if dropped_table_names.contains(&drop.table_name.to_lowercase()) {
                continue;
            }
            let store = self
                .version_stores
                .read()
                .unwrap()
                .get(&drop.table_name.to_lowercase())
                .cloned()
                .ok_or_else(|| Error::TableNotFound(drop.table_name.clone()))?;
            if store.get_index(&drop.index_name).is_none() {
                return Err(Error::IndexNotFound(drop.index_name.clone()));
            }
        }
        for rename in pending_index_renames {
            let store = self
                .version_stores
                .read()
                .unwrap()
                .get(&rename.table_name.to_lowercase())
                .cloned()
                .ok_or_else(|| Error::TableNotFound(rename.table_name.clone()))?;
            if store.get_index(&rename.old_index_name).is_none() {
                return Err(Error::IndexNotFound(rename.old_index_name.clone()));
            }
            if store.get_index(&rename.new_index_name).is_some() {
                return Err(Error::IndexAlreadyExists(rename.new_index_name.clone()));
            }
        }
        {
            let schemas = self.schemas.read().unwrap();
            let stores = self.version_stores.read().unwrap();
            let mut names = schemas.keys().cloned().collect::<FxHashSet<_>>();
            for rename in pending_table_renames {
                if !names.remove(&rename.old_name) || !stores.contains_key(&rename.old_name) {
                    return Err(Error::TableNotFound(rename.old_name.clone()));
                }
                if !names.insert(rename.new_name.clone()) {
                    return Err(Error::TableAlreadyExists(rename.new_name.clone()));
                }
            }
        }
        drop(schemas);

        // Validate the complete catalog that this transaction would publish,
        // not only each schema in isolation. This makes transaction-private FK
        // children visible to simultaneous DROP/ALTER decisions.
        let mut final_catalog: FxHashMap<String, Schema> = self
            .schemas
            .read()
            .unwrap()
            .iter()
            .filter(|(name, _)| !dropped_table_names.contains(name.as_str()))
            .map(|(name, schema)| (name.clone(), schema.as_ref().clone()))
            .collect();
        {
            let pending = self.pending_tables.read().unwrap();
            for table_name in created_tables {
                let lower = table_name.to_lowercase();
                let schema = pending
                    .get(&lower)
                    .filter(|table| table.owner_txn_id == txn_id)
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "transaction {} lost pending table '{}' during catalog validation",
                            txn_id, table_name
                        ))
                    })?
                    .version_store
                    .schema()
                    .as_ref()
                    .clone();
                final_catalog.insert(lower, schema);
            }
        }
        for change in &final_schema_changes {
            final_catalog.insert(change.table_name.to_lowercase(), change.schema.clone());
        }
        for rename in pending_table_renames {
            let mut renamed = final_catalog
                .remove(&rename.old_name)
                .ok_or_else(|| Error::TableNotFound(rename.old_name.clone()))?;
            renamed.rename_table(&rename.new_name);
            for schema in final_catalog.values_mut() {
                rename_schema_fk_target(schema, &rename.old_name, &rename.new_name);
            }
            rename_schema_fk_target(&mut renamed, &rename.old_name, &rename.new_name);
            if final_catalog
                .insert(rename.new_name.clone(), renamed)
                .is_some()
            {
                return Err(Error::TableAlreadyExists(rename.new_name.clone()));
            }
        }
        for (table_name, schema) in &mut final_catalog {
            if !created_table_names.contains(table_name) {
                // DROP TABLE has always removed inbound FK metadata from
                // surviving catalog tables. Model that same final state here;
                // only a newly created child must fail rather than silently
                // publish without the constraint it requested.
                schema
                    .foreign_keys
                    .retain(|fk| !dropped_table_names.contains(&fk.referenced_table));
            }
        }
        for schema in final_catalog.values() {
            for foreign_key in &schema.foreign_keys {
                let parent = final_catalog.get(&foreign_key.referenced_table).ok_or_else(|| {
                    Error::invalid_argument(format!(
                        "foreign key '{}.{}' references table '{}' absent from the final transaction catalog",
                        schema.table_name, foreign_key.column_name, foreign_key.referenced_table
                    ))
                })?;
                let (_, parent_column) = parent
                    .find_column(&foreign_key.referenced_column)
                    .ok_or_else(|| {
                        Error::invalid_argument(format!(
                            "foreign key '{}.{}' references missing column '{}.{}'",
                            schema.table_name,
                            foreign_key.column_name,
                            foreign_key.referenced_table,
                            foreign_key.referenced_column
                        ))
                    })?;
                let child_column = &schema.columns[foreign_key.column_index];
                if child_column.data_type != parent_column.data_type {
                    return Err(Error::invalid_argument(format!(
                        "foreign key '{}.{}' type {:?} does not match '{}.{}' type {:?}",
                        schema.table_name,
                        child_column.name,
                        child_column.data_type,
                        parent.table_name,
                        parent_column.name,
                        parent_column.data_type
                    )));
                }
            }
        }
        if let Some(prepared) = prepared_catalog.as_ref() {
            let runtime_names = prepared
                .schemas
                .iter()
                .map(|schema| schema.table_name_lower.clone())
                .collect::<FxHashSet<_>>();
            let physical_names = final_catalog.keys().cloned().collect::<FxHashSet<_>>();
            if runtime_names != physical_names {
                return Err(Error::internal(
                    "typed catalog and physical DDL resolve to different table sets",
                ));
            }
        }

        // Revalidate the final transaction-visible rows while commit owns the
        // table membership fences. This closes the statement-to-commit window
        // for NOT NULL, CHECK and FOREIGN KEY constraints.
        let mut row_validation_schemas: FxHashMap<String, Schema> = final_schema_changes
            .iter()
            .filter(|change| {
                // A standalone DROP/RENAME COLUMN preserves the values owned
                // by every surviving column. Its positional hot-row transform
                // is published only after the commit marker, so scanning the
                // private final-schema overlay here would either truncate the
                // wrong tail value or reject mixed pre/post-ADD row widths.
                // Existing constraints were already true and dependency
                // checks above prove none references a removed identity.
                // Revalidate only when this transaction also introduced a
                // logical schema/constraint change or needs ADD normalization.
                pending_schema_changes.iter().any(|candidate| {
                    candidate
                        .table_name
                        .eq_ignore_ascii_case(&change.table_name)
                        && candidate.physical_transition.is_none()
                })
            })
            .map(|change| (change.table_name.to_lowercase(), change.schema.clone()))
            .collect();
        for table_name in created_tables {
            let table = self.get_table_for_transaction(txn_id, table_name)?;
            row_validation_schemas
                .entry(table_name.to_lowercase())
                .or_insert_with(|| table.schema().clone());
        }
        for (table_name, schema) in &mut row_validation_schemas {
            if !created_table_names.contains(table_name) {
                schema
                    .foreign_keys
                    .retain(|fk| !dropped_table_names.contains(&fk.referenced_table));
            }
        }
        for (table_name, schema) in &row_validation_schemas {
            let table = self.get_table_for_transaction(txn_id, table_name)?;
            let mut row_validator = self.bind_row_validator(schema)?;
            let mut parent_domains = Vec::with_capacity(schema.foreign_keys.len());
            for foreign_key in &schema.foreign_keys {
                let parent =
                    self.get_table_for_transaction(txn_id, &foreign_key.referenced_table)?;
                let parent_index = parent
                    .schema()
                    .get_column_index(&foreign_key.referenced_column)
                    .ok_or_else(|| Error::ColumnNotFound(foreign_key.referenced_column.clone()))?;
                let mut parent_scanner =
                    parent.scan_exact_projection_unfenced(&[parent_index], None)?;
                let mut values = ValueSet::default();
                while parent_scanner.next() {
                    let (_, parent_row) = parent_scanner.take_row_with_id()?;
                    if let Some(value) = parent_row.get(0).filter(|value| !value.is_null()) {
                        values.insert(value.clone());
                    }
                }
                if let Some(error) = parent_scanner.err().cloned() {
                    let _ = parent_scanner.close();
                    return Err(error);
                }
                parent_scanner.close()?;
                parent_domains.push(values);
            }

            let projection: Vec<usize> = (0..schema.columns.len()).collect();
            let mut scanner = table.scan_exact_projection_unfenced(&projection, None)?;
            while scanner.next() {
                let (_, row) = scanner.take_row_with_id()?;
                row_validator.validate(&row)?;
                for (foreign_key, parent_values) in schema.foreign_keys.iter().zip(&parent_domains)
                {
                    let value = row.get(foreign_key.column_index).ok_or_else(|| {
                        Error::internal(format!(
                            "row in '{}' is missing foreign-key column '{}'",
                            table_name, foreign_key.column_name
                        ))
                    })?;
                    if value.is_null() {
                        continue;
                    }
                    if !parent_values.contains(value) {
                        return Err(Error::foreign_key_violation(
                            &schema.table_name,
                            &foreign_key.column_name,
                            &foreign_key.referenced_table,
                            &foreign_key.referenced_column,
                            format!(
                                "referenced row with {} = {} does not exist",
                                foreign_key.referenced_column, value
                            ),
                        ));
                    }
                }
            }
            if let Some(error) = scanner.err().cloned() {
                let _ = scanner.close();
                return Err(error);
            }
            scanner.close()?;
        }

        // Public transaction table handles cannot publish a shared index. The
        // narrow authorization below exists only while this commit owns both
        // DDL and visibility fences, and is cleared on every build outcome.
        let authorized_index_stores: Vec<_> = {
            let stores = self.txn_version_stores().read().unwrap();
            let mut authorized = Vec::new();
            if let Some(entries) = stores.get(txn_id) {
                for definition in pending_indexes {
                    if let Some((_, store)) = entries
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(definition.table_name.as_str()))
                    {
                        if !authorized
                            .iter()
                            .any(|candidate| Arc::ptr_eq(candidate, store))
                        {
                            authorized.push(Arc::clone(store));
                        }
                    }
                }
            }
            authorized
        };
        for store in &authorized_index_stores {
            store.write().unwrap().set_index_ddl_authorized(true);
        }
        let build_indexes: Result<()> = (|| {
            // Build every requested index first. If any definition fails, the
            // transaction rollback path removes already-built indexes and private
            // tables before a commit marker can be written.
            for definition in pending_indexes {
                if definition.index_type == Some(radixdb_core::IndexType::Hnsw)
                    && (definition.hnsw_ef_construction == Some(0)
                        || definition.hnsw_ef_search == Some(0))
                {
                    return Err(Error::invalid_argument(
                        "HNSW ef_construction and ef_search must be greater than zero",
                    ));
                }
                let table = self.get_table_for_transaction(txn_id, &definition.table_name)?;
                let column_refs: Vec<&str> =
                    definition.columns.iter().map(String::as_str).collect();
                if let Some(encoder) = &definition.key_encoder {
                    if definition.partial_predicate.is_some() {
                        return Err(Error::NotSupported(
                            "partial external operator-class indexes are not supported".to_owned(),
                        ));
                    }
                    let index_type = definition.index_type.ok_or_else(|| {
                        Error::internal("operator-class index lost its access method")
                    })?;
                    table.create_index_with_key_encoder(
                        &definition.index_name,
                        &column_refs,
                        definition.is_unique,
                        index_type,
                        encoder.clone(),
                    )?;
                } else if self.can_defer_cold_index_backfill(definition) {
                    table.create_index_with_deferred_cold_backfill(
                        &definition.index_name,
                        &column_refs,
                        false,
                        definition.index_type,
                    )?;
                } else if let Some(predicate) = &definition.partial_predicate {
                    table.create_partial_index_with_type(
                        &definition.index_name,
                        &column_refs,
                        definition.is_unique,
                        definition.index_type,
                        predicate.clone(),
                    )?;
                } else if definition.index_type == Some(radixdb_core::IndexType::Hnsw) {
                    let metric = crate::index::HnswDistanceMetric::from_u8(
                        definition.hnsw_distance_metric.unwrap_or(0),
                    )
                    .unwrap_or(crate::index::HnswDistanceMetric::L2);
                    table.create_hnsw_index(
                        &definition.index_name,
                        &definition.columns[0],
                        definition.is_unique,
                        definition.hnsw_m.unwrap_or(16) as usize,
                        definition.hnsw_ef_construction.unwrap_or(200) as usize,
                        definition.hnsw_ef_search.unwrap_or(50) as usize,
                        metric,
                    )?;
                } else {
                    table.create_index_with_type(
                        &definition.index_name,
                        &column_refs,
                        definition.is_unique,
                        definition.index_type,
                    )?;
                }
            }

            // Builders use the complete transaction-visible row set, but their
            // legacy publication hook registers the result immediately. Detach
            // every newly built generation before releasing the preparation
            // phase. It is activated only after commit validation succeeds, so
            // neither another session nor ordinary DML publication can mistake
            // it for a pre-existing index generation.
            for definition in pending_indexes {
                let table = self.get_table_for_transaction(txn_id, &definition.table_name)?;
                let Some(index) = table.get_index(&definition.index_name) else {
                    // A redundant explicit index on the INTEGER primary key is
                    // the historical no-op and creates no named generation.
                    continue;
                };
                table.drop_index(&definition.index_name)?;
                let txn_store = self
                    .txn_version_stores()
                    .read()
                    .unwrap()
                    .get(txn_id)
                    .and_then(|entries| {
                        entries
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case(&definition.table_name))
                            .map(|(_, store)| Arc::clone(store))
                    })
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "transaction {} lost table '{}' while detaching index '{}'",
                            txn_id, definition.table_name, definition.index_name
                        ))
                    })?;
                txn_store
                    .write()
                    .unwrap()
                    .stage_prepared_final_view_index(index)?;
            }
            Ok(())
        })();
        for store in &authorized_index_stores {
            store.write().unwrap().set_index_ddl_authorized(false);
        }
        build_indexes?;

        // The typed catalog mutation is the only durable DDL authority. It is
        // appended by record_commit() with the shared transaction marker.
        // Re-emitting object lifecycle records into the data WAL would restore
        // the forbidden legacy authority beside the catalog generation.
        *self.prepared_catalog.lock().unwrap() = prepared_catalog;

        Ok(())
    }

    fn activate_transactional_indexes(&self, txn_id: i64) -> Result<()> {
        let mut stores: Vec<_> = self
            .txn_version_stores()
            .read()
            .unwrap()
            .get(txn_id)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|(_, store)| {
                        !store
                            .read()
                            .unwrap()
                            .prepared_final_view_indexes()
                            .is_empty()
                    })
                    .map(|(name, store)| (name.to_string(), Arc::clone(store)))
                    .collect()
            })
            .unwrap_or_default();
        stores.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        let mut activated: Vec<(String, Arc<RwLock<TransactionVersionStore>>)> = Vec::new();
        for (table_name, store) in &stores {
            if let Err(error) = store
                .write()
                .unwrap()
                .activate_prepared_final_view_indexes()
            {
                for (_, activated_store) in activated.iter().rev() {
                    activated_store
                        .write()
                        .unwrap()
                        .deactivate_prepared_final_view_indexes();
                }
                return Err(Error::internal(format!(
                    "failed to activate prepared indexes for table '{}': {}",
                    table_name, error
                )));
            }
            activated.push((table_name.clone(), Arc::clone(store)));
        }

        let managers = self.segment_managers.read().unwrap();
        for (table_name, store) in stores {
            let Some(manager) = managers.get(&table_name) else {
                continue;
            };
            if !manager.has_segments() {
                continue;
            }
            for index in store.read().unwrap().prepared_final_view_indexes() {
                if index.index_type() != radixdb_core::IndexType::PrimaryKey {
                    manager.mark_cold_populated_index(index.name());
                }
            }
        }
        Ok(())
    }

    fn publish_transactional_ddl(
        &self,
        publication: TransactionalDdlPublication<'_>,
    ) -> Result<()> {
        let TransactionalDdlPublication {
            txn_id,
            created_tables,
            dropped_tables,
            pending_indexes,
            pending_index_drops,
            pending_index_renames,
            pending_table_renames,
            pending_schema_changes,
            catalog_mutation,
            commit_lsn,
        } = publication;
        if created_tables.is_empty()
            && dropped_tables.is_empty()
            && pending_index_drops.is_empty()
            && pending_index_renames.is_empty()
            && pending_table_renames.is_empty()
            && pending_schema_changes.is_empty()
            && catalog_mutation.is_none()
        {
            return Ok(());
        }
        let mut publish = Vec::with_capacity(created_tables.len());
        {
            let mut pending = self.pending_tables.write().unwrap();
            for table_name in created_tables {
                let lower = table_name.to_lowercase();
                let table = pending
                    .remove(&lower)
                    .expect("transactional DDL publication was prevalidated");
                debug_assert_eq!(table.owner_txn_id, txn_id);
                publish.push((lower, table.version_store));
            }
        }

        {
            let mut schemas = self.schemas.write().unwrap();
            let mut stores = self.version_stores.write().unwrap();
            for (name, store) in publish {
                debug_assert!(!schemas.contains_key(&name));
                debug_assert!(!stores.contains_key(&name));
                let previous_schema = schemas.insert(name.clone(), store.schema().clone());
                let previous_store = stores.insert(name, store);
                debug_assert!(previous_schema.is_none() && previous_store.is_none());
            }
        }

        // The commit marker and row visibility point are already durable. Apply
        // the staged schema to the shared VersionStore and catalog as one DDL
        // publication while the caller still owns the exclusive DDL fence.
        let mut final_schema_changes: FxHashMap<String, &PendingSchemaChange> =
            FxHashMap::default();
        for change in pending_schema_changes {
            final_schema_changes.insert(change.table_name.to_lowercase(), change);
        }
        let mut final_schema_changes: Vec<&PendingSchemaChange> =
            final_schema_changes.into_values().collect();
        final_schema_changes.sort_unstable_by(|left, right| {
            left.table_name
                .to_lowercase()
                .cmp(&right.table_name.to_lowercase())
        });
        for change in pending_schema_changes {
            let Some(transition) = &change.physical_transition else {
                continue;
            };
            let table_name = change.table_name.to_lowercase();
            let store = self
                .version_stores
                .read()
                .unwrap()
                .get(&table_name)
                .cloned()
                .expect("transactional ALTER TABLE target was prevalidated");
            let result = match transition {
                SchemaPhysicalTransition::DropColumn { column_index, .. } => store
                    .remap_indexes_after_column_drop(*column_index)
                    .map(|()| {
                        store.remove_column_from_hot_versions(*column_index);
                        let transaction_stores = self.txn_version_stores.read().unwrap();
                        for stores in transaction_stores.values() {
                            if let Some((_, transaction_store)) = stores
                                .iter()
                                .find(|(name, _)| name.eq_ignore_ascii_case(&table_name))
                            {
                                transaction_store
                                    .write()
                                    .unwrap()
                                    .remove_column_from_local_versions(*column_index);
                            }
                        }
                    }),
                SchemaPhysicalTransition::RenameColumn { old_name, new_name } => {
                    store.rename_indexed_column_metadata(old_name, new_name)
                }
            };
            result.map_err(|error| Error::WalDurabilityUncertain {
                detail: format!(
                    "transaction {} committed ALTER TABLE '{}' but physical publication requires recovery: {}",
                    txn_id, table_name, error
                ),
            })?;
        }

        for change in final_schema_changes {
            let table_name = change.table_name.to_lowercase();
            let store = self
                .version_stores
                .read()
                .unwrap()
                .get(&table_name)
                .cloned()
                .expect("transactional ALTER TABLE target was prevalidated");
            let old_schema = store.schema().clone();
            let published_schema = CompactArc::new(change.schema.clone());
            *store.schema_mut() = published_schema.clone();
            if old_schema.primary_key_indices() != published_schema.primary_key_indices() {
                if let Some(old_index_name) = schema_derived_pk_index_name(&old_schema) {
                    store.remove_index(&old_index_name);
                }
                register_pk_index(&published_schema, &store)?;
            }
            if change.requires_row_normalization {
                store.require_row_normalization();
            }
            self.schemas
                .write()
                .unwrap()
                .insert(table_name.clone(), published_schema.clone());
            if let Some(manager) = self.segment_managers.read().unwrap().get(&table_name) {
                for staged in pending_schema_changes
                    .iter()
                    .filter(|staged| staged.table_name.eq_ignore_ascii_case(&table_name))
                {
                    match &staged.physical_transition {
                        Some(SchemaPhysicalTransition::DropColumn { column_name, .. }) => {
                            manager.record_column_drop(
                                column_name,
                                self.schema_epoch.load(Ordering::Acquire),
                            );
                        }
                        Some(SchemaPhysicalTransition::RenameColumn { old_name, new_name }) => {
                            manager.record_column_rename(old_name, new_name);
                        }
                        None => {}
                    }
                }
                manager.invalidate_mappings(&published_schema);
            }
        }

        for drop in pending_index_drops.iter().filter(|drop| !drop.schema_owned) {
            if dropped_tables
                .iter()
                .any(|(table_name, _)| table_name.eq_ignore_ascii_case(&drop.table_name))
            {
                continue;
            }
            let store = self
                .version_stores
                .read()
                .unwrap()
                .get(&drop.table_name.to_lowercase())
                .cloned()
                .expect("transactional DROP INDEX target was prevalidated");
            let removed = store.remove_index(&drop.index_name);
            debug_assert!(removed.is_some());
            if let Some(manager) = self
                .segment_managers
                .read()
                .unwrap()
                .get(&drop.table_name.to_lowercase())
            {
                manager.unmark_cold_populated_index(&drop.index_name);
            }
        }

        for rename in pending_index_renames {
            let store = self
                .version_stores
                .read()
                .unwrap()
                .get(&rename.table_name.to_lowercase())
                .cloned()
                .expect("transactional ALTER INDEX target was prevalidated");
            store
                .rename_index(&rename.old_index_name, &rename.new_index_name)
                .map_err(|error| Error::WalDurabilityUncertain {
                    detail: format!(
                        "transaction {} committed ALTER INDEX '{}.{}' but runtime publication requires recovery: {}",
                        txn_id, rename.table_name, rename.old_index_name, error
                    ),
                })?;
            if let Some(manager) = self
                .segment_managers
                .read()
                .unwrap()
                .get(&rename.table_name.to_lowercase())
            {
                manager.rename_cold_populated_index(&rename.old_index_name, &rename.new_index_name);
            }
        }

        for rename in pending_table_renames {
            let mut stores = self.version_stores.write().unwrap();
            let store = stores
                .remove(&rename.old_name)
                .expect("transactional table rename was prevalidated");
            {
                let mut schema = store.schema_mut();
                CompactArc::make_mut(&mut *schema).rename_table(&rename.new_name);
            }
            stores.insert(rename.new_name.clone(), Arc::clone(&store));
            drop(stores);

            let mut schemas = self.schemas.write().unwrap();
            let mut schema = schemas
                .remove(&rename.old_name)
                .expect("transactional table schema rename was prevalidated");
            CompactArc::make_mut(&mut schema).rename_table(&rename.new_name);
            schemas.insert(rename.new_name.clone(), schema);
            drop(schemas);

            let mut managers = self.segment_managers.write().unwrap();
            if let Some(manager) = managers.remove(&rename.old_name) {
                managers.insert(rename.new_name.clone(), manager);
            }
        }

        for (table_name, _) in dropped_tables {
            {
                let mut schemas = self.schemas.write().unwrap();
                schemas.remove(table_name);
                let stores = self.version_stores.read().unwrap();
                strip_fk_references(&mut schemas, &stores, table_name);
            }
            {
                let mut stores = self.version_stores.write().unwrap();
                if let Some(store) = stores.remove(table_name) {
                    store.close();
                }
            }
            {
                let mut managers = self.segment_managers.write().unwrap();
                if let Some(manager) = managers.get(table_name) {
                    manager.clear();
                }
                managers.remove(table_name);
            }
        }
        if catalog_mutation.is_some() {
            let prepared_catalog =
                self.prepared_catalog
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| {
                        Error::internal("catalog publication was not prepared before commit")
                    })?;
            let PreparedCatalogCommit {
                mutation,
                next_catalog_id,
                created_unix_ns,
                schemas: catalog_schemas,
                views: catalog_views,
            } = prepared_catalog;
            let publisher = self.catalog_publisher.load_full();
            let current = publisher
                .pin()
                .map_err(|error| Error::internal(format!("catalog pin failed: {error}")))?;
            let meta = current.meta();
            let next_generation = meta
                .catalog_generation()
                .checked_add(1)
                .ok_or_else(|| Error::internal("catalog generation overflow during publication"))?;
            let next_meta = CatalogPackMeta::new(
                meta.database_id(),
                next_catalog_id,
                next_generation,
                commit_lsn.max(meta.snapshot_lsn()),
                created_unix_ns,
            )
            .map_err(|error| Error::internal(format!("catalog metadata rejected: {error}")))?;
            let prepared_mutation = mutation
                .prepare(&current, next_meta)
                .map_err(|error| Error::internal(format!("catalog successor rejected: {error}")))?;
            crate::v6::publish_catalog_mutation(&publisher, prepared_mutation).map_err(
                |error| Error::WalDurabilityUncertain {
                    detail: format!("catalog runtime publication requires recovery: {error}"),
                },
            )?;
            let stores = self.version_stores.read().unwrap();
            if stores.len() != catalog_schemas.len() {
                return Err(Error::WalDurabilityUncertain {
                    detail: "committed catalog and runtime storage contain different table sets; recovery required"
                        .to_owned(),
                });
            }
            let mut schemas = FxHashMap::default();
            for schema in catalog_schemas {
                let table_name = schema.table_name_lower.clone();
                let store = stores.get(&table_name).ok_or_else(|| {
                    Error::WalDurabilityUncertain {
                        detail: format!(
                            "committed catalog table '{}' has no runtime storage; recovery required",
                            schema.table_name
                        ),
                    }
                })?;
                let schema = CompactArc::new(schema);
                *store.schema_mut() = schema.clone();
                schemas.insert(table_name, schema);
            }
            drop(stores);
            *self.schemas.write().unwrap() = schemas;
            let mut views = FxHashMap::default();
            for view in catalog_views {
                let name = view.name.clone();
                if views.insert(name, Arc::new(view)).is_some() {
                    return Err(Error::WalDurabilityUncertain {
                        detail:
                            "committed catalog produced duplicate runtime views; recovery required"
                                .to_string(),
                    });
                }
            }
            *self.views.write().unwrap() = views;

            if !pending_indexes.is_empty() {
                let publication = super::physical_publication::publish_transactional_indexes(
                    &self.database_root,
                    &self.physical_generation,
                    &self.catalog_publisher,
                    &self.persistence,
                    &self.schemas,
                    &self.version_stores,
                    &self.segment_managers,
                    pending_indexes,
                );
                let (published_tables, publication_error) = match publication {
                    Ok(tables) => (tables, None),
                    Err(error) => (FxHashSet::default(), Some(error)),
                };
                let mut fallback_failures = Vec::new();
                for definition in pending_indexes {
                    let table_name = definition.table_name.to_lowercase();
                    if !self.can_defer_cold_index_backfill(definition)
                        || published_tables.contains(&table_name)
                    {
                        continue;
                    }
                    let table = self.get_table_for_transaction(txn_id, &definition.table_name)?;
                    let columns = definition
                        .columns
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>();
                    if let Err(error) =
                        table.complete_deferred_cold_index(&definition.index_name, &columns)
                    {
                        fallback_failures.push(format!(
                            "{}.{}: {error}",
                            definition.table_name, definition.index_name
                        ));
                    }
                }
                if !fallback_failures.is_empty() {
                    return Err(Error::WalDurabilityUncertain {
                        detail: format!(
                            "transaction {} committed CREATE INDEX but neither immutable nor runtime cold coverage completed for {}",
                            txn_id,
                            fallback_failures.join("; ")
                        ),
                    });
                }
                if let Some(error) = publication_error {
                    // The logical DDL and its WAL commit marker are already
                    // authoritative. INDEX artifacts are rebuildable; the
                    // complete runtime fallback above preserves query results.
                    eprintln!(
                        "Warning: committed CREATE INDEX retained complete runtime coverage because physical accelerator publication failed: {error}"
                    );
                }
            }
        }
        self.schema_epoch.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

fn rename_schema_fk_target(schema: &mut Schema, old_name: &str, new_name: &str) {
    for foreign_key in &mut schema.foreign_keys {
        if foreign_key.referenced_table.eq_ignore_ascii_case(old_name) {
            foreign_key.referenced_table = new_name.to_owned();
        }
    }
    for constraint in &mut schema.constraints {
        if let SchemaConstraintKind::ForeignKey {
            referenced_table, ..
        } = &mut constraint.kind
        {
            if referenced_table.eq_ignore_ascii_case(old_name) {
                *referenced_table = new_name.to_owned();
            }
        }
    }
}
