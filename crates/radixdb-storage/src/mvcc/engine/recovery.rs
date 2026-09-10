use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use radixdb_catalog::{CatalogGeneration, CatalogPayload, ObjectId};
use radixdb_core::{Error, Result, Schema, SchemaColumn};
use rustc_hash::FxHashMap;

use crate::config::PersistenceConfig;
use crate::mvcc::persistence::PersistenceManager;
use crate::mvcc::wal_manager::{TwoPhaseRecoveryInfo, WALOperationType};
use crate::mvcc::FileLock;
use crate::v6::{
    ArtifactDataSource, ArtifactIndexSource, DataWalRecoveryContext, DataWalRecoveryOutcome,
    DatabaseRecovery, DatabaseRoot, FormatError, FormatResult, OpenMetadataBudget,
    PhysicalGenerationPublisher, PhysicalGenerationSnapshot, RecoveryLimits, SegmentTier,
    WalRecovery, WalReplayFloor,
};
use crate::volume::manifest::{SegmentLevel, SegmentRegistration};
use crate::volume::writer::FrozenVolume;

use super::MVCCEngine;

impl MVCCEngine {
    /// Recover one persistent runtime from CONTROL-selected catalog, physical
    /// generation and WAL authorities. Persistence is not constructed before
    /// CONTROL has selected the exact WAL generation and replay floor.
    pub(super) fn recover_persistent_runtime(
        &self,
        config: &PersistenceConfig,
        writer_lock: &FileLock,
    ) -> Result<Arc<PhysicalGenerationPublisher>> {
        let root = Path::new(&self.path);
        DatabaseRoot::new(root)
            .open_or_create(unix_time_nanos())
            .map_err(storage_format_error)?;
        let mut wal = EngineWalRecovery {
            engine: self,
            config,
        };
        let recovered = DatabaseRecovery::new(root, RecoveryLimits::default())
            .recover(&mut wal)
            .map_err(storage_format_error)?;
        let publisher = Arc::new(
            PhysicalGenerationPublisher::from_locked(
                recovered.physical().clone(),
                writer_lock.clone(),
            )
            .map_err(storage_format_error)?,
        );
        recovered.into_runtime_state();
        Ok(publisher)
    }

    fn replay_catalog_owned_wal(&self, floor_lsn: u64) -> Result<RecoveryReplay> {
        let persistence = self.persistence().ok_or(Error::WalNotInitialized)?;
        let table_names = self
            .schemas
            .read()
            .unwrap()
            .values()
            .map(|schema| {
                ObjectId::from_user_bytes(schema.catalog_id)
                    .map(|table_id| (table_id, schema.table_name_lower.clone()))
                    .map_err(|error| {
                        Error::internal(format!(
                            "runtime table '{}' has invalid WAL identity: {error}",
                            schema.table_name
                        ))
                    })
            })
            .collect::<Result<rustc_hash::FxHashMap<_, _>>>()?;
        let mut applied_data_entries = 0_u64;
        let mut catalog_or_irrelevant_entries = 0_u64;
        // WAL data records precede their commit marker. Keep only the touched
        // table names for cold tombstones so replay can publish them with the
        // marker's real durable visibility sequence instead of inventing a
        // sentinel sequence that cannot be encoded in an immutable artifact.
        let mut pending_tombstone_tables = FxHashMap::default();
        let info = persistence.replay_two_phase_with_outcome_observer(
            floor_lsn,
            |entry| match entry.operation {
                WALOperationType::CatalogMutation => {
                    catalog_or_irrelevant_entries = catalog_or_irrelevant_entries.saturating_add(1);
                    Ok(())
                }
                WALOperationType::Insert
                | WALOperationType::Update
                | WALOperationType::Delete
                | WALOperationType::TruncateTable
                    if entry
                        .table_id
                        .is_none_or(|table_id| !table_names.contains_key(&table_id)) =>
                {
                    // The final catalog can omit a table created and dropped
                    // wholly inside the replay suffix. Its historical row
                    // records have no runtime owner and are intentionally dead.
                    catalog_or_irrelevant_entries = catalog_or_irrelevant_entries.saturating_add(1);
                    Ok(())
                }
                _ => {
                    let table_name = entry
                        .table_id
                        .and_then(|table_id| table_names.get(&table_id).cloned());
                    self.apply_wal_entry_resolved(
                        entry,
                        table_name,
                        &mut pending_tombstone_tables,
                    )?;
                    applied_data_entries = applied_data_entries.saturating_add(1);
                    Ok(())
                }
            },
            |txn_id| self.registry.recover_aborted_transaction(txn_id),
        )?;
        if !pending_tombstone_tables.is_empty() {
            return Err(Error::internal(
                "committed WAL replay left cold tombstones without a commit marker",
            ));
        }
        self.registry
            .recover_transaction_high_water(info.max_transaction_id)?;
        Ok(RecoveryReplay {
            info,
            applied_data_entries,
            catalog_or_irrelevant_entries,
        })
    }

    fn install_recovered_artifacts(
        &self,
        physical: &PhysicalGenerationSnapshot,
        metadata_budget: &mut OpenMetadataBudget,
    ) -> Result<()> {
        let schemas = self.schemas.read().unwrap();
        let mut runtime_tables = schemas
            .values()
            .map(|schema| {
                ObjectId::from_user_bytes(schema.catalog_id)
                    .map(|id| (id, schema.table_name_lower.clone(), schema.clone()))
                    .map_err(|error| {
                        Error::internal(format!(
                            "runtime schema '{}' has invalid catalog identity: {error}",
                            schema.table_name
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        runtime_tables.sort_unstable_by_key(|(id, _, _)| *id);
        drop(schemas);

        for manifest in physical.table_manifests() {
            let Some((_, table_name, schema)) = runtime_tables
                .binary_search_by_key(&manifest.table_id(), |(id, _, _)| *id)
                .ok()
                .and_then(|index| runtime_tables.get(index))
            else {
                // CONTROL owns the checkpoint catalog and its physical table
                // manifests. Catalog WAL after that floor may durably drop a
                // table before the crash. The replayed catalog is the final
                // logical authority, so the checkpoint payload of such a
                // table is unreachable historical state, not a malformed
                // recovery graph. Catalog/runtime set equality is validated
                // after data replay and still catches an incomplete binder.
                continue;
            };
            if manifest.segments().is_empty() {
                continue;
            }
            let segment_count = manifest.segments().len() as u64;
            let first_runtime_id = manifest
                .next_segment_sequence()
                .checked_sub(segment_count)
                .filter(|id| *id != 0)
                .ok_or_else(|| {
                    Error::internal(format!(
                        "table '{}' next segment sequence cannot own its manifest",
                        table_name
                    ))
                })?;
            let manager = self.get_or_create_segment_manager(table_name);
            // Table manifests are canonically sorted by opaque SegmentId for
            // byte stability. Runtime segment IDs, however, define oldest to
            // newest precedence. Reconstruct that order from the durable
            // transaction ranges; never mistake canonical file order for
            // logical recency.
            let mut recovered_segments = manifest.segments().to_vec();
            recovered_segments.sort_unstable_by_key(|segment| {
                (
                    segment.max_transaction_id(),
                    segment.min_transaction_id(),
                    segment.data_artifact().creation_generation().get(),
                    segment.id(),
                )
            });
            let mut registrations = Vec::with_capacity(recovered_segments.len());
            let mut recovered_tombstones = FxHashMap::default();
            for (offset, segment) in recovered_segments.into_iter().enumerate() {
                let runtime_id = first_runtime_id
                    .checked_add(offset as u64)
                    .ok_or_else(|| Error::internal("runtime segment sequence overflows"))?;
                let source = Arc::new(
                    ArtifactDataSource::open_with_budget(
                        Path::new(&self.path).join(segment.data_artifact().relative_path()),
                        segment.data_artifact(),
                        metadata_budget,
                    )
                    .map_err(storage_format_error)?,
                );
                if source.layout().header().segment_kind() != segment.kind() {
                    return Err(Error::internal(format!(
                        "table '{}' DATA kind differs from its segment descriptor",
                        table_name
                    )));
                }
                if segment.kind() == crate::v6::SegmentKind::Tombstones {
                    if segment.index_artifact().is_some() || source.column_count() != 0 {
                        return Err(Error::internal(format!(
                            "table '{}' tombstone segment owns columns or an INDEX artifact",
                            table_name
                        )));
                    }
                    for group_index in 0..source.row_group_count() {
                        let row_ids = source.read_row_ids(group_index)?;
                        for &row_id in row_ids.row_ids() {
                            if recovered_tombstones
                                .insert(row_id, segment.max_transaction_id())
                                .is_some()
                            {
                                return Err(Error::internal(format!(
                                    "table '{}' has duplicate durable tombstone row ID {}",
                                    table_name, row_id
                                )));
                            }
                        }
                    }
                    continue;
                }
                let physical_schema = artifact_physical_schema(
                    self.pin_catalog()?.as_ref(),
                    manifest.table_id(),
                    schema,
                    source.as_ref(),
                )?;
                let mut volume =
                    FrozenVolume::from_artifact_source(&physical_schema, Arc::clone(&source))?;
                if let Some(index_reference) = segment.index_artifact() {
                    let index = Arc::new(
                        ArtifactIndexSource::open_with_budget(
                            Path::new(&self.path).join(index_reference.relative_path()),
                            index_reference,
                            source,
                            metadata_budget,
                        )
                        .map_err(storage_format_error)?,
                    );
                    volume = volume.with_artifact_index_source(index)?;
                }
                let volume = Arc::new(volume);
                let mut registration = self.segment_registration(
                    table_name,
                    volume,
                    runtime_id,
                    0,
                    self.schema_epoch.load(Ordering::Acquire),
                );
                registration.meta.level = match segment.tier() {
                    SegmentTier::L0 => SegmentLevel::L0,
                    SegmentTier::L1 => SegmentLevel::L1,
                };
                registration.meta.file_path = segment.data_artifact().relative_path();
                registrations.push(SegmentRegistration::new(
                    runtime_id,
                    registration.volume,
                    registration.meta,
                ));
            }
            manager.install_recovered_tombstones(recovered_tombstones);
            if !registrations.is_empty() {
                manager.register_segments_atomic(registrations, Some(schema), None)?;
            }
            manager.ensure_runtime_segment_sequence(manifest.next_segment_sequence());
        }
        self.sync_auto_increment_from_segments();
        Ok(())
    }

    /// Restore the MVCC admission frontiers that may no longer be present in
    /// the retained WAL. Row segment descriptors carry the transaction-ID
    /// high-water observed when they were sealed. Tombstone descriptors carry
    /// the commit-sequence frontier needed by snapshot visibility. Advancing
    /// the sequence to at least the transaction frontier is conservative and
    /// prevents identity reuse when a database has no durable tombstones.
    fn recover_physical_mvcc_high_waters(
        &self,
        physical: &PhysicalGenerationSnapshot,
    ) -> Result<()> {
        let mut transaction_high_water = physical.database_manifest().transaction_high_water();
        let mut visibility_high_water = 0_u64;
        for manifest in physical.table_manifests() {
            for segment in manifest.segments() {
                match segment.kind() {
                    crate::v6::SegmentKind::Rows => {
                        transaction_high_water =
                            transaction_high_water.max(segment.max_transaction_id());
                    }
                    crate::v6::SegmentKind::Tombstones => {
                        visibility_high_water =
                            visibility_high_water.max(segment.max_transaction_id());
                    }
                }
            }
        }
        visibility_high_water = visibility_high_water.max(transaction_high_water);

        let transaction_high_water = i64::try_from(transaction_high_water).map_err(|_| {
            Error::internal("physical transaction high-water exceeds the positive MVCC domain")
        })?;
        let visibility_high_water = i64::try_from(visibility_high_water).map_err(|_| {
            Error::internal("physical visibility high-water exceeds the packed MVCC domain")
        })?;
        self.registry
            .recover_transaction_high_water(transaction_high_water)?;
        self.registry
            .recover_visibility_high_water(visibility_high_water)?;
        Ok(())
    }
}

/// Reconstruct the schema owned by one immutable DATA artifact.
///
/// Catalog DDL is metadata-only: an older segment may have fewer columns, may
/// retain a dropped column, or may predate a column rename. Stable catalog
/// column identities let recovery name every still-live physical column from
/// the selected catalog while giving removed columns an unambiguous tombstone
/// name. `SegmentManager` then derives the current logical mapping without
/// rewriting or materializing the segment.
fn artifact_physical_schema(
    catalog: &CatalogGeneration,
    table_id: ObjectId,
    current_schema: &Schema,
    source: &ArtifactDataSource,
) -> Result<Schema> {
    let mut columns = Vec::with_capacity(source.layout().columns().len());
    for (ordinal, physical) in source.layout().columns().iter().copied().enumerate() {
        let name = match catalog.object(physical.column_id()) {
            Some(object) => {
                let CatalogPayload::Column(column) = object.payload() else {
                    return Err(Error::internal(format!(
                        "DATA artifact column identity {} resolves to a non-column catalog object",
                        physical.column_id()
                    )));
                };
                if object.parent_id() != Some(table_id) {
                    return Err(Error::internal(format!(
                        "DATA artifact column identity {} belongs to another table",
                        physical.column_id()
                    )));
                }
                if column.data_type() != physical.data_type() {
                    return Err(Error::internal(format!(
                        "DATA artifact column identity {} has a type incompatible with the selected catalog",
                        physical.column_id()
                    )));
                }
                object.name().display().as_str().to_owned()
            }
            None => format!("__dropped_{}", physical.column_id()),
        };
        let mut column = SchemaColumn::new(
            ordinal,
            name,
            physical.data_type().logical_type(),
            physical.nullable(),
            false,
        );
        if let Some(type_ref) = physical.data_type().external_type_ref() {
            let type_object = catalog
                .object(
                    physical
                        .data_type()
                        .type_object_id()
                        .ok_or_else(|| Error::internal("external DATA type lost its object ID"))?,
                )
                .ok_or_else(|| Error::internal("external DATA type object disappeared"))?;
            column = column
                .with_external_type(type_ref, type_object.name().display().as_str().to_owned());
        }
        columns.push(column);
    }
    let mut schema = Schema::new(current_schema.table_name(), columns);
    schema.install_catalog_identity(current_schema.catalog_id())?;
    Ok(schema)
}

struct EngineWalRecovery<'a> {
    engine: &'a MVCCEngine,
    config: &'a PersistenceConfig,
}

impl WalRecovery for EngineWalRecovery<'_> {
    type State = ();

    fn read_catalog_transactions(
        &mut self,
        floor: WalReplayFloor,
        byte_budget: u64,
    ) -> FormatResult<Vec<u8>> {
        PersistenceManager::read_catalog_transactions_at(
            Path::new(&self.engine.path),
            self.config,
            floor,
            byte_budget,
        )
        .map_err(recovery_format_error)
    }

    fn replay_data(
        &mut self,
        context: DataWalRecoveryContext<'_>,
    ) -> FormatResult<DataWalRecoveryOutcome<Self::State>> {
        let mut metadata_budget = OpenMetadataBudget::from_allowance(context.metadata_allowance());
        let catalog = Arc::new(CatalogGeneration::new(
            context.catalog().meta(),
            context.catalog().graph().clone(),
        ));
        let table_ids = self
            .engine
            .install_recovered_catalog(catalog)
            .map_err(recovery_format_error)?;
        self.engine
            .recover_physical_mvcc_high_waters(context.physical())
            .map_err(recovery_format_error)?;
        self.engine
            .install_recovered_artifacts(context.physical(), &mut metadata_budget)
            .map_err(recovery_format_error)?;
        let persistence = Arc::new(
            PersistenceManager::new_with_replay_floor(
                Some(context.root()),
                self.config,
                context.floor(),
            )
            .map_err(recovery_format_error)?,
        );
        persistence.start().map_err(recovery_format_error)?;
        self.engine.persistence.store(Some(persistence));
        self.engine.loading_from_disk.store(true, Ordering::Release);
        let replay = self
            .engine
            .replay_catalog_owned_wal(context.floor().lsn())
            .map_err(recovery_format_error)?;
        // Catalog binding creates empty runtime index owners before data WAL
        // replay. Populate ordinary indexes once from the recovered hot rows,
        // then complete HNSW coverage from immutable artifacts. Lifecycle DDL
        // records are deliberately absent from the catalog-owned data WAL.
        self.engine
            .populate_all_indexes()
            .map_err(recovery_format_error)?;
        self.engine
            .populate_hnsw_from_segments()
            .map_err(recovery_format_error)?;
        self.engine.populate_schema_defaults();
        self.engine
            .loading_from_disk
            .store(false, Ordering::Release);
        Ok(DataWalRecoveryOutcome::new(
            (),
            replay.info.last_lsn.max(context.floor().lsn()),
            replay.info.committed_transactions as u64,
            replay.applied_data_entries,
            replay
                .info
                .skipped_entries
                .saturating_add(replay.catalog_or_irrelevant_entries),
            table_ids,
        ))
    }
}

struct RecoveryReplay {
    info: TwoPhaseRecoveryInfo,
    applied_data_entries: u64,
    catalog_or_irrelevant_entries: u64,
}

fn unix_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn storage_format_error(error: FormatError) -> Error {
    Error::internal(format!("persistent database format error: {error}"))
}

fn recovery_format_error(error: Error) -> FormatError {
    FormatError::InvalidRecoveryGraph {
        detail: error.to_string(),
    }
}
