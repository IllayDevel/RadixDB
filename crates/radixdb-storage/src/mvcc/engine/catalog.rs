use super::*;
use radixdb_catalog::{
    CatalogGeneration as RuntimeCatalogGeneration, CatalogGraph, CatalogName, CatalogObject,
    CatalogPackMeta, CatalogPayload, NamespacePayload, ObjectId,
};

impl MVCCEngine {
    pub(super) fn bootstrap_catalog_generation() -> RuntimeCatalogGeneration {
        let namespace = CatalogObject::new(
            ObjectId::BOOTSTRAP_NAMESPACE,
            None,
            None,
            ObjectId::BOOTSTRAP_OWNER,
            CatalogName::new("public").expect("bootstrap namespace name is valid"),
            1,
            CatalogPayload::Namespace(NamespacePayload::new()),
        )
        .expect("bootstrap namespace object is valid");
        let graph = CatalogGraph::build(vec![namespace], Vec::new())
            .expect("bootstrap catalog graph is valid");
        let database_id = radixdb_core::new_durable_identity_bytes();
        let catalog_id = radixdb_core::new_durable_identity_bytes();
        let created_unix_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        let meta = CatalogPackMeta::new(database_id, catalog_id, 1, 0, created_unix_ns)
            .expect("bootstrap catalog metadata is valid");
        RuntimeCatalogGeneration::new(meta, graph)
    }

    /// Pin the complete logical catalog used to bind one statement or begin
    /// one transaction. The returned generation remains valid across later
    /// catalog publications.
    #[doc(hidden)]
    pub fn pin_catalog(&self) -> Result<Arc<RuntimeCatalogGeneration>> {
        self.catalog_publisher
            .load_full()
            .pin()
            .map_err(|error| Error::internal(format!("catalog pin failed: {error}")))
    }

    pub(super) fn install_catalog(&self, generation: Arc<RuntimeCatalogGeneration>) {
        self.catalog_publisher
            .store(Arc::new(CatalogPublisher::new(generation)));
    }

    /// Bind and install one recovered catalog as the complete runtime schema
    /// authority. Callers invoke this before DML WAL replay while admission is
    /// stopped; failure leaves the prior empty startup state untouched.
    pub(super) fn install_recovered_catalog(
        &self,
        generation: Arc<RuntimeCatalogGeneration>,
    ) -> Result<Vec<ObjectId>> {
        let runtime = self.catalog_runtime_binder().bind(generation.as_ref())?;
        let (tables, views) = runtime.into_parts();
        let mut schemas = FxHashMap::default();
        let mut stores = FxHashMap::default();
        let mut table_ids = Vec::with_capacity(tables.len());

        for table in tables {
            let (schema, indexes) = table.into_parts();
            schema.validate_structural_invariants()?;
            crate::mvcc::persistence::validate_schema_persistence(&schema)?;
            let table_id = ObjectId::from_user_bytes(schema.catalog_id).map_err(|error| {
                Error::internal(format!(
                    "runtime table '{}' has invalid catalog identity: {error}",
                    schema.table_name
                ))
            })?;
            let catalog_table = generation.object(table_id).ok_or_else(|| {
                Error::internal(format!(
                    "runtime table '{}' is absent from recovered catalog",
                    schema.table_name
                ))
            })?;
            if catalog_table.kind() != radixdb_catalog::ObjectKind::Table
                || !catalog_table
                    .name()
                    .display()
                    .as_str()
                    .eq_ignore_ascii_case(&schema.table_name)
            {
                return Err(Error::internal(format!(
                    "runtime table '{}' differs from recovered catalog",
                    schema.table_name
                )));
            }

            let table_name = schema.table_name_lower.clone();
            let store = VersionStore::with_transaction_registry(
                table_name.clone(),
                schema.clone(),
                Arc::clone(&self.registry),
            );
            let store = Arc::new(store);
            // Primary-key enforcement is schema-derived runtime state.  The
            // catalog carries the durable constraint identity (and may carry
            // its support-index identity), but the concrete hot-row owner is
            // rebuilt exactly as it is on CREATE TABLE before ordinary catalog
            // indexes are installed.  In particular, IndexType::PrimaryKey is
            // intentionally not a second independently populated index.
            register_pk_index(&schema, &store)?;
            for index in indexes {
                let predicate = index
                    .partial_predicate
                    .as_ref()
                    .map(|predicate| {
                        (self.partial_index_predicate_binder)(predicate.canonical_sql(), &schema)
                    })
                    .transpose()?;
                store.create_index_from_metadata_with_predicate(&index, true, predicate)?;
            }
            if schemas
                .insert(table_name.clone(), CompactArc::new(schema))
                .is_some()
                || stores.insert(table_name, store).is_some()
            {
                return Err(Error::internal(
                    "recovered catalog binds duplicate runtime tables",
                ));
            }
            table_ids.push(table_id);
        }

        let mut bound_views = FxHashMap::default();
        for view in views {
            if bound_views
                .insert(view.name.clone(), Arc::new(view))
                .is_some()
            {
                return Err(Error::internal(
                    "recovered catalog binds duplicate runtime views",
                ));
            }
        }

        table_ids.sort_unstable();
        *self.schemas.write().unwrap() = schemas;
        *self.version_stores.write().unwrap() = stores;
        *self.views.write().unwrap() = bound_views;
        self.install_catalog(generation);
        self.schema_epoch.fetch_add(1, Ordering::AcqRel);
        Ok(table_ids)
    }

    /// Return the names in the currently published catalog runtime.
    pub fn get_all_table_names(&self) -> Vec<String> {
        self.schemas.read().unwrap().keys().cloned().collect()
    }

    /// Returns all schemas currently in the engine (CompactArc ref-count bump only)
    pub fn get_all_schemas(&self) -> Vec<radixdb_core::CompactArc<Schema>> {
        self.schemas.read().unwrap().values().cloned().collect()
    }

    /// Get a table handle for an existing transaction by txn_id.
    /// This allows FK enforcement to participate in the caller's transaction,
    /// ensuring CASCADE effects are atomic and uncommitted rows are visible.
    pub fn get_table_for_txn(
        &self,
        txn_id: i64,
        table_name: &str,
    ) -> Result<Box<dyn crate::traits::Table>> {
        EngineOperations::new(self).get_table_for_transaction(txn_id, table_name)
    }

    /// Resolve schema through the transaction's private CREATE TABLE overlay.
    #[doc(hidden)]
    pub fn get_table_schema_for_txn(
        &self,
        txn_id: i64,
        table_name: &str,
    ) -> Result<CompactArc<Schema>> {
        let lower = to_lowercase_cow(table_name);
        if let Some(schema) = self
            .txn_version_stores
            .read()
            .unwrap()
            .get(txn_id)
            .and_then(|tables| tables.iter().find(|(name, _)| name == lower.as_ref()))
            .and_then(|(_, store)| store.read().unwrap().schema_override())
        {
            return Ok(schema);
        }
        if let Some(schema) = self
            .pending_tables
            .read()
            .unwrap()
            .get(lower.as_ref())
            .and_then(|pending| {
                (pending.owner_txn_id == txn_id).then(|| pending.version_store.schema().clone())
            })
        {
            return Ok(schema);
        }
        self.get_table_schema(table_name)
    }

    /// Find all FK constraints in other tables that reference the given parent table.
    /// Uses a cached reverse mapping that is rebuilt only when schema_epoch changes.
    /// Returns Arc-wrapped Vec for zero-copy sharing (ref-count bump only).
    /// Zero cost for databases without FK constraints.
    pub fn find_referencing_fks(
        &self,
        parent_table: &str,
    ) -> Arc<Vec<(String, ForeignKeyConstraint)>> {
        static EMPTY: std::sync::LazyLock<Arc<Vec<(String, ForeignKeyConstraint)>>> =
            std::sync::LazyLock::new(|| Arc::new(Vec::new()));

        let current_epoch = self.schema_epoch.load(Ordering::Acquire);

        // Fast path: check if cache is valid (read lock only)
        {
            let cache = self.fk_reverse_cache.read().unwrap();
            if cache.0 == current_epoch {
                return cache
                    .1
                    .get(parent_table)
                    .cloned()
                    .unwrap_or_else(|| Arc::clone(&EMPTY));
            }
        }

        // Cache miss: rebuild under write lock
        let mut cache = self.fk_reverse_cache.write().unwrap();
        // Double-check after acquiring write lock (another thread may have rebuilt)
        if cache.0 == current_epoch {
            return cache
                .1
                .get(parent_table)
                .cloned()
                .unwrap_or_else(|| Arc::clone(&EMPTY));
        }

        // Rebuild the full reverse mapping
        let schemas = self.schemas.read().unwrap();
        let mut map: StringMap<Vec<(String, ForeignKeyConstraint)>> = StringMap::default();
        for schema in schemas.values() {
            for fk in &schema.foreign_keys {
                map.entry(fk.referenced_table.clone())
                    .or_default()
                    .push((schema.table_name_lower.clone(), fk.clone()));
            }
        }
        // Wrap each Vec in Arc before storing in cache
        let arc_map: StringMap<Arc<Vec<(String, ForeignKeyConstraint)>>> =
            map.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();
        *cache = (current_epoch, arc_map);

        cache
            .1
            .get(parent_table)
            .cloned()
            .unwrap_or_else(|| Arc::clone(&EMPTY))
    }

    /// Reverse FK graph visible to one transaction, including private CREATE
    /// TABLE and staged schema replacements.
    #[doc(hidden)]
    pub fn find_referencing_fks_for_txn(
        &self,
        txn_id: i64,
        parent_table: &str,
    ) -> Arc<Vec<(String, ForeignKeyConstraint)>> {
        let parent_table = parent_table.to_lowercase();
        let mut schemas: FxHashMap<String, CompactArc<Schema>> = self
            .schemas
            .read()
            .unwrap()
            .iter()
            .map(|(name, schema)| (name.clone(), schema.clone()))
            .collect();

        if let Some(entries) = self.txn_version_stores.read().unwrap().get(txn_id) {
            for (name, store) in entries {
                if let Some(schema) = store.read().unwrap().schema_override() {
                    schemas.insert(name.to_string(), schema);
                }
            }
        }
        for (name, pending) in self.pending_tables.read().unwrap().iter() {
            if pending.owner_txn_id == txn_id {
                schemas.insert(name.clone(), pending.version_store.schema().clone());
            }
        }

        let mut referencing = Vec::new();
        for schema in schemas.values() {
            for foreign_key in &schema.foreign_keys {
                if foreign_key
                    .referenced_table
                    .eq_ignore_ascii_case(&parent_table)
                {
                    referencing.push((schema.table_name_lower.clone(), foreign_key.clone()));
                }
            }
        }
        Arc::new(referencing)
    }

    /// Look up the runtime version store selected by the published catalog.
    pub fn get_version_store(&self, name: &str) -> Result<Arc<VersionStore>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        self.get_version_store_for_recovery(name)
    }

    /// Lookup used only by the serialized startup/recovery owner before Ready
    /// publication. Public callers must use `get_version_store`, which remains
    /// fail-closed while the engine is Opening.
    pub(super) fn get_version_store_for_recovery(&self, name: &str) -> Result<Arc<VersionStore>> {
        let table_name = to_lowercase_cow(name);

        let stores = self.version_stores.read().unwrap();
        stores
            .get(table_name.as_ref())
            .cloned()
            .ok_or_else(|| Error::TableNotFound(table_name.as_ref().to_string()))
    }

    /// Return or create the runtime segment manager for a catalog-owned table.
    pub(super) fn get_or_create_segment_manager(
        &self,
        table_name: &str,
    ) -> Arc<crate::volume::manifest::SegmentManager> {
        // Fast path: read lock (common case during WAL replay — manager already exists)
        {
            let mgrs = self.segment_managers.read().unwrap();
            if let Some(mgr) = mgrs.get(table_name) {
                return Arc::clone(mgr);
            }
        }
        // Slow path: write lock (first access, creates new manager)
        let mut mgrs = self.segment_managers.write().unwrap();
        let manager = mgrs
            .entry(table_name.to_string())
            .or_insert_with(|| {
                Arc::new(crate::volume::manifest::SegmentManager::new(
                    table_name, None,
                ))
            })
            .clone();
        manager
    }

    /// Register a frozen volume into a table's segment manager.
    /// Uses a fresh auto-assigned segment ID.
    #[cfg(test)]
    pub(super) fn register_volume(
        &self,
        table_name: &str,
        volume: Arc<crate::volume::writer::FrozenVolume>,
    ) -> Result<()> {
        let mgr = self.get_or_create_segment_manager(table_name);
        let seg_id = mgr.manifest_mut().allocate_segment_id();
        self.register_volume_with_id(table_name, volume, seg_id)
    }

    /// Register a frozen volume with a specific segment ID.
    /// Used during startup to restore stable IDs from volume filenames,
    /// ensuring .dv files match their segments across restarts.
    #[cfg(test)]
    pub(super) fn register_volume_with_id(
        &self,
        table_name: &str,
        volume: Arc<crate::volume::writer::FrozenVolume>,
        seg_id: u64,
    ) -> Result<()> {
        self.register_volume_with_id_and_seal_seq(table_name, volume, seg_id, 0)
    }

    #[cfg(test)]
    pub(super) fn register_volume_with_id_and_seal_seq(
        &self,
        table_name: &str,
        volume: Arc<crate::volume::writer::FrozenVolume>,
        seg_id: u64,
        seal_seq: u64,
    ) -> Result<()> {
        let mgr = self.get_or_create_segment_manager(table_name);
        let schema_version = self.schema_epoch.load(Ordering::Acquire);
        let registration =
            self.segment_registration(table_name, volume, seg_id, seal_seq, schema_version);
        mgr.register_segments_atomic(vec![registration], None, None)
    }

    pub(super) fn segment_registration(
        &self,
        table_name: &str,
        volume: Arc<crate::volume::writer::FrozenVolume>,
        seg_id: u64,
        seal_seq: u64,
        schema_version: u64,
    ) -> crate::volume::manifest::SegmentRegistration {
        use crate::volume::manifest::{SegmentLevel, SegmentMeta, SegmentRegistration};
        let min_id = volume.meta.row_ids.first().unwrap_or(0);
        let max_id = volume.meta.row_ids.last().unwrap_or(0);
        let row_count = volume.meta.row_count;
        let file_path = self.segment_file_path_for_manifest(table_name, seg_id, volume.as_ref());
        SegmentRegistration::new(
            seg_id,
            volume,
            SegmentMeta {
                segment_id: seg_id,
                file_path,
                row_count,
                min_row_id: min_id,
                max_row_id: max_id,
                seal_seq,
                schema_version,
                level: SegmentLevel::L0,
                creation_epoch: seal_seq,
            },
        )
    }

    pub(super) fn segment_file_path_for_manifest(
        &self,
        table_name: &str,
        seg_id: u64,
        volume: &crate::volume::writer::FrozenVolume,
    ) -> PathBuf {
        if let Some(source) = volume.artifact_source() {
            return source.layout().reference().relative_path();
        }
        conventional_segment_file_path(table_name, seg_id)
    }

    /// Sync auto-increment counters from segment data.
    ///
    /// After WAL replay, ensure auto-increment counters account for
    /// the max row_id in segments (which may be higher than hot buffer).
    /// Normal secondary indexes are NOT populated from cold data.
    /// HNSW is the one cold index that needs explicit graph rebuild.
    pub(super) fn sync_auto_increment_from_segments(&self) {
        // Collect segment data under segment_managers lock, then drop it
        // before acquiring version_stores lock to avoid multi-lock deadlock.
        let segment_data: Vec<(String, i64)> = {
            let mgrs = self.segment_managers.read().unwrap();
            if mgrs.is_empty() {
                return;
            }
            mgrs.iter()
                .filter_map(|(table_name, mgr)| {
                    let segments = mgr.get_segments_ordered_meta();
                    let mut max_vol_row_id: i64 = 0;
                    for vol in &segments {
                        // Row IDs are sorted (from B-tree iteration during seal).
                        // Use last() for O(1) instead of iterating all row_ids.
                        if let Some(last_id) = vol.meta.row_ids.last() {
                            if last_id > max_vol_row_id {
                                max_vol_row_id = last_id;
                            }
                        }
                    }
                    if max_vol_row_id > 0 {
                        Some((table_name.clone(), max_vol_row_id))
                    } else {
                        None
                    }
                })
                .collect()
        };

        if segment_data.is_empty() {
            return;
        }

        let stores = self.version_stores.read().unwrap();
        for (table_name, max_vol_row_id) in &segment_data {
            if let Some(store) = stores.get(table_name) {
                store.set_auto_increment_counter(*max_vol_row_id);
            }
        }
    }

    /// Creates an engine operations wrapper for a transaction
    pub(super) fn create_engine_operations(&self) -> Arc<dyn TransactionEngineOperations> {
        Arc::new(EngineOperations::new(self))
    }
}
