use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use radixdb_catalog::{
    AccessMethod, CatalogGeneration as RuntimeCatalog, CatalogPayload, CatalogPublisher,
    ConstraintPayload, EdgeKind, IndexPayload, ObjectId, ObjectKind,
};
use radixdb_core::{CompactArc, DataType, Error, Result, RowVec, Schema};
use rustc_hash::{FxHashMap, FxHashSet};
use sha2::{Digest, Sha256};

use crate::v6::{
    catalog_path, database_manifest_path, encode_catalog_artifact, encode_database_manifest,
    encode_table_manifest, table_manifest_path, wal_path, write_artifact_pair, write_data_artifact,
    write_index_replacement_reusing, AcceleratorBuildSpec, ArtifactBuildKind, ArtifactBuildLease,
    ArtifactId, ArtifactKind, ArtifactLocator, ArtifactPairBuildRequest, CatalogGeneration,
    CatalogId, CatalogRef, CatalogRootRef, ColumnBuildPolicy, ControlRecord, ControlSlotIndex,
    DataArtifactBuildRequest, DataArtifactHeader, DataColumnSpec, DataPhysicalCodec,
    DataValueEncoding, DatabaseGeneration, DatabaseId, DatabaseManifest, DatabaseManifestRootRef,
    ExactPageBuildLimits, FanoutBuildLimits, FrozenCheckpoint, FrozenMaintenance, IndexKeyColumn,
    IndexNullsOrder, IndexPageCodec, IndexRebuildRequest, IndexReplacementBuildRequest,
    IndexSortDirection, MaintenanceKind, ManifestGeneration, ManifestId, ManifestKind, ManifestRef,
    OrderedPageBuildLimits, PhysicalGenerationPublisher, PhysicalGenerationSnapshot,
    SegmentDescriptor, SegmentId, SegmentKind, SegmentTier, SourceRow, StagedArtifactSet,
    StagedMemberRole, StagingDiscoveryLimits, TableManifest, TableManifestRef, WalGeneration,
    WalReplayFloor, WalRetirementStatus, WriterInstanceId, MAX_SORT_RUN_RECORDS,
};
use crate::volume::writer::FrozenVolume;

use super::MVCCEngine;

pub(super) struct PublishedDataSegment {
    pub(super) volume: Arc<FrozenVolume>,
    pub(super) path: PathBuf,
}

#[derive(Debug)]
pub(super) struct DataSegmentPublicationError {
    error: Box<Error>,
    stale_source: bool,
}

impl DataSegmentPublicationError {
    pub(super) fn is_stale_source(&self) -> bool {
        self.stale_source
    }

    pub(super) fn into_error(self) -> Error {
        *self.error
    }
}

impl From<Error> for DataSegmentPublicationError {
    fn from(error: Error) -> Self {
        Self {
            error: Box::new(error),
            stale_source: false,
        }
    }
}

type DataSegmentPublicationResult<T> = std::result::Result<T, DataSegmentPublicationError>;

fn data_column_policy(column: DataColumnSpec, codec: DataPhysicalCodec) -> ColumnBuildPolicy {
    let encoding = match column.data_type().logical_type() {
        DataType::Text | DataType::Json | DataType::Bytes => DataValueEncoding::Adaptive,
        _ => DataValueEncoding::Plain,
    };
    ColumnBuildPolicy::new(encoding, codec, None)
}

pub(super) fn open_artifact_volume(
    database_root: &Path,
    schema: &Schema,
    descriptor: SegmentDescriptor,
) -> Result<(Arc<FrozenVolume>, PathBuf)> {
    let path = database_root.join(descriptor.data_artifact().relative_path());
    let data = Arc::new(
        crate::v6::ArtifactDataSource::open(&path, descriptor.data_artifact())
            .map_err(format_error)?,
    );
    let mut volume = FrozenVolume::from_artifact_source(schema, Arc::clone(&data))?;
    if let Some(index_reference) = descriptor.index_artifact() {
        let index = Arc::new(
            crate::v6::ArtifactIndexSource::open(
                database_root.join(index_reference.relative_path()),
                index_reference,
                data,
            )
            .map_err(format_error)?,
        );
        volume = volume.with_artifact_index_source(index)?;
    }
    Ok((Arc::new(volume), path))
}

pub(super) struct StagedCompactionSegment {
    pub(super) row_count: u64,
    pub(super) physical_bytes: u64,
    pub(super) has_index: bool,
}

struct TableSegmentChange {
    table_id: ObjectId,
    additions: Vec<SegmentDescriptor>,
    removals: Vec<SegmentId>,
    tombstone_ack: Option<TombstonePublicationAck>,
}

struct TombstonePublicationAck {
    manager: Arc<crate::volume::manifest::SegmentManager>,
    generation: u64,
}

struct StagingCleanup {
    path: PathBuf,
    armed: bool,
}

impl StagingCleanup {
    fn armed(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

impl TableSegmentChange {
    fn append(table_id: ObjectId, segment: SegmentDescriptor) -> Self {
        Self {
            table_id,
            additions: vec![segment],
            removals: Vec::new(),
            tombstone_ack: None,
        }
    }

    fn confirm_tombstone_publication(&self) {
        if let Some(ack) = self.tombstone_ack.as_ref() {
            ack.manager.confirm_tombstone_publication(ack.generation);
        }
    }
}

pub(super) struct CompactionPublication {
    database_root: PathBuf,
    publisher: Arc<PhysicalGenerationPublisher>,
    logical: Arc<RuntimeCatalog>,
    schema: CompactArc<Schema>,
    table_id: ObjectId,
    columns: Vec<DataColumnSpec>,
    accelerators: Option<Vec<AcceleratorBuildSpec>>,
    staging: StagedArtifactSet,
    build: ArtifactBuildLease,
    expected_control: ControlRecord,
    target_generation: DatabaseGeneration,
    created_unix_ns: u64,
    min_transaction_id: u64,
    max_transaction_id: u64,
    expected_inputs: Vec<SegmentDescriptor>,
    removals: Vec<SegmentId>,
    additions: Vec<SegmentDescriptor>,
    cleanup_staging_on_drop: bool,
}

impl CompactionPublication {
    pub(super) fn scratch_root(&self) -> PathBuf {
        self.staging.path().join("scratch")
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn stage_rows(
        &mut self,
        row_refs: &mut super::CompactionRowRefSpool,
        range: std::ops::Range<usize>,
        volumes: &[(u64, Arc<FrozenVolume>)],
        mappings: &[crate::volume::writer::ColumnMapping],
        cache: &mut crate::volume::writer::CompactionBlockCache,
        compress: bool,
    ) -> Result<StagedCompactionSegment> {
        let row_count = range.len();
        if row_count == 0 {
            return Err(Error::internal(
                "cannot stage an empty compaction DATA artifact",
            ));
        }
        let first_row_id = row_refs
            .ref_at(range.start)?
            .row_id
            .try_into()
            .map_err(|_| Error::internal("compaction DATA has an invalid first row ID"))?;
        let last_row_id = row_refs
            .ref_at(range.end - 1)?
            .row_id
            .try_into()
            .map_err(|_| Error::internal("compaction DATA has an invalid last row ID"))?;
        let row_count = u64::try_from(row_count)
            .map_err(|_| Error::internal("compaction DATA row count exceeds u64"))?;
        let limits = FanoutBuildLimits::default();
        let column_count = u32::try_from(self.columns.len())
            .map_err(|_| Error::internal("DATA column count exceeds u32"))?;
        let row_group_rows = limits
            .planned_row_group_rows(row_count, column_count)
            .map_err(format_error)?;
        let row_group_count = row_count.div_ceil(u64::from(row_group_rows));
        let segment_id = SegmentId::new();
        let artifact_id = ArtifactId::new();
        let header = DataArtifactHeader::new(
            artifact_id,
            self.expected_control.database_id(),
            self.table_id,
            segment_id,
            self.target_generation,
            CatalogGeneration::new(self.logical.meta().catalog_generation())
                .map_err(format_error)?,
            self.min_transaction_id,
            self.max_transaction_id,
            row_count,
            column_count,
            u32::try_from(row_group_count)
                .map_err(|_| Error::internal("DATA row-group count exceeds u32"))?,
            SegmentKind::Rows,
            self.created_unix_ns,
        )
        .map_err(format_error)?;
        let codec = if compress {
            DataPhysicalCodec::Lz4
        } else {
            DataPhysicalCodec::None
        };
        let policies = self
            .columns
            .iter()
            .copied()
            .map(|column| data_column_policy(column, codec))
            .collect::<Vec<_>>();
        let data_relative =
            ArtifactLocator::derive(artifact_id, ArtifactKind::Data).relative_path(artifact_id);
        let data_relative_text = data_relative
            .to_str()
            .ok_or_else(|| Error::internal("canonical DATA path is not UTF-8"))?;
        let source = super::CompactionSealRowSource::new_spooled_range(
            row_refs, range, volumes, mappings, cache,
        );
        let source_rows = super::CompactionArtifactRows::new(source, self.columns.len());

        let (data_reference, index_reference) = if let Some(accelerators) =
            self.accelerators.clone()
        {
            let index_artifact_id = ArtifactId::new();
            let index_relative = ArtifactLocator::derive(index_artifact_id, ArtifactKind::Index)
                .relative_path(index_artifact_id);
            let index_relative_text = index_relative
                .to_str()
                .ok_or_else(|| Error::internal("canonical INDEX path is not UTF-8"))?;
            let request = ArtifactPairBuildRequest::new(
                header,
                self.columns.clone(),
                policies,
                codec,
                index_artifact_id,
                accelerators,
                limits,
                self.staging.path(),
            )
            .map_err(format_error)?;
            let written = self
                .staging
                .write_generation_file(StagedMemberRole::Index, index_relative_text, |index_file| {
                    self.staging.write_generation_file(
                        StagedMemberRole::Data,
                        data_relative_text,
                        |data_file| {
                            write_artifact_pair(&request, source_rows, data_file, index_file)
                        },
                    )
                })
                .map_err(format_error)?;
            (written.data_reference(), Some(written.index_reference()))
        } else {
            let request = DataArtifactBuildRequest::new(
                header,
                self.columns.clone(),
                policies,
                codec,
                limits,
            )
            .map_err(format_error)?;
            let written = self
                .staging
                .write_generation_file(StagedMemberRole::Data, data_relative_text, |file| {
                    write_data_artifact(&request, source_rows, file)
                })
                .map_err(format_error)?;
            (written.data_reference(), None)
        };
        let physical_bytes = data_reference
            .byte_length()
            .saturating_add(index_reference.map_or(0, |reference| reference.byte_length()));
        self.additions.push(
            SegmentDescriptor::new_at_tier(
                segment_id,
                SegmentKind::Rows,
                SegmentTier::L1,
                self.min_transaction_id,
                self.max_transaction_id,
                row_count,
                first_row_id,
                last_row_id,
                data_reference,
                index_reference,
            )
            .map_err(format_error)?,
        );
        Ok(StagedCompactionSegment {
            row_count,
            physical_bytes,
            has_index: index_reference.is_some(),
        })
    }

    pub(super) fn publish(
        mut self,
        engine: &MVCCEngine,
        tombstone_target: Option<&FxHashMap<i64, u64>>,
    ) -> Result<Vec<PublishedDataSegment>> {
        let scratch = self.scratch_root();
        match std::fs::remove_dir_all(&scratch) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::internal(format!(
                    "failed to remove compaction scratch directory '{}': {error}",
                    scratch.display()
                )));
            }
        }

        let row_additions = std::mem::take(&mut self.additions);
        let prebuilt_artifacts = row_additions
            .iter()
            .flat_map(|descriptor| {
                std::iter::once(descriptor.data_artifact()).chain(descriptor.index_artifact())
            })
            .collect::<Vec<_>>();
        let prebuilt_complete = self
            .staging
            .mark_complete(self.created_unix_ns, StagingDiscoveryLimits::default())
            .map_err(format_error)?;

        // Rebase only the bounded manifest graph. The caller holds the
        // checkpoint and table seal-write fences, so this current generation
        // stays stable until the physical publisher takes its own fence.
        let current_lease = self.publisher.pin().map_err(format_error)?;
        let current = current_lease.snapshot();
        let current_control = current.control();
        validate_catalog_identity(self.logical.as_ref(), current_control)?;
        if self.logical.meta().catalog_generation() != current_control.catalog().generation().get()
            || self.logical.meta().catalog_id() != current_control.catalog().id().into_bytes()
        {
            return Err(super::compaction_abort(
                super::COMPACTION_STALE_PUBLICATION_REASON,
                "catalog advanced after compaction planning",
            ));
        }
        let current_table = current.table_manifest(self.table_id).ok_or_else(|| {
            super::compaction_abort(
                super::COMPACTION_STALE_PUBLICATION_REASON,
                "selected table disappeared after compaction planning",
            )
        })?;
        for expected in &self.expected_inputs {
            let actual = current_table
                .segments()
                .binary_search_by_key(&expected.id(), |segment| segment.id())
                .ok()
                .and_then(|index| current_table.segments().get(index));
            if actual != Some(expected) {
                return Err(super::compaction_abort(
                    super::COMPACTION_STALE_PUBLICATION_REASON,
                    "selected input changed after compaction planning",
                ));
            }
        }

        let target_generation = current_control
            .database_generation()
            .checked_next()
            .map_err(format_error)?;
        let rebase_created_unix_ns = unix_time_nanos();
        let rebase_writer = WriterInstanceId::new();
        let rebase_staging = StagedArtifactSet::create(
            self.database_root.join("staging"),
            rebase_writer,
            target_generation,
            process_id(),
            rebase_created_unix_ns,
        )
        .map_err(format_error)?;
        let mut rebase_cleanup = StagingCleanup::armed(rebase_staging.path().to_path_buf());
        let rebase_build = self
            .publisher
            .begin_artifact_build(
                ArtifactBuildKind::Compaction,
                &current_lease,
                rebase_staging.owner(),
            )
            .map_err(format_error)?;
        let mut segment_change = TableSegmentChange {
            table_id: self.table_id,
            additions: row_additions.clone(),
            removals: std::mem::take(&mut self.removals),
            tombstone_ack: None,
        };
        let catalog_reference = current.database_manifest().catalog();
        if let Some(tombstones) = tombstone_target {
            let tombstone_change = stage_tombstone_replacement(
                &rebase_staging,
                current,
                target_generation,
                catalog_reference.generation(),
                self.table_id,
                tombstones,
                None,
                rebase_created_unix_ns,
            )?;
            segment_change.additions.extend(tombstone_change.additions);
            segment_change.removals.extend(tombstone_change.removals);
        }
        let table_references = stage_table_manifests(
            &rebase_staging,
            self.logical.as_ref(),
            current,
            target_generation,
            catalog_reference.generation(),
            std::slice::from_ref(&segment_change),
            &engine.version_stores,
            rebase_created_unix_ns,
        )?;
        let database_manifest_id = ManifestId::new();
        let database_manifest = DatabaseManifest::new(
            current_control.database_id(),
            database_manifest_id,
            target_generation,
            catalog_reference,
            current_control.wal_replay_floor(),
            engine.publication_transaction_high_water(current),
            table_references,
            rebase_created_unix_ns,
        )
        .map_err(format_error)?;
        let database_bytes = encode_database_manifest(&database_manifest).map_err(format_error)?;
        let database_reference = DatabaseManifestRootRef::new(
            database_manifest_id,
            ManifestGeneration::new(target_generation.get()).map_err(format_error)?,
            footer_sha(&database_bytes)?,
        );
        write_staged(
            &rebase_staging,
            StagedMemberRole::DatabaseManifest,
            database_manifest_path(database_reference),
            &database_bytes,
        )?;
        let target_control = ControlRecord::new(
            inactive_slot(current_control.slot()),
            target_generation,
            current_control.database_id(),
            database_reference,
            current_control.catalog(),
            current_control.wal_replay_floor(),
            rebase_created_unix_ns,
            rebase_writer,
        )
        .map_err(format_error)?;
        let complete = rebase_staging
            .mark_complete(rebase_created_unix_ns, StagingDiscoveryLimits::default())
            .map_err(format_error)?;
        let maintenance = FrozenMaintenance::new(
            MaintenanceKind::Compaction,
            current_control,
            target_control,
            complete,
            rebase_build,
        )
        .map_err(format_error)?;
        let expected_removals = segment_change
            .removals
            .iter()
            .map(|segment_id| {
                current_table
                    .segments()
                    .binary_search_by_key(segment_id, |segment| segment.id())
                    .ok()
                    .and_then(|index| current_table.segments().get(index))
                    .copied()
                    .ok_or_else(|| {
                        super::compaction_abort(
                            super::COMPACTION_STALE_PUBLICATION_REASON,
                            "segment selected for removal changed during manifest rebase",
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        self.publisher
            .publish_rebased_compaction(
                prebuilt_complete,
                self.build.clone(),
                &prebuilt_artifacts,
                maintenance,
                self.table_id,
                &expected_removals,
                &segment_change.additions,
            )
            .map_err(compaction_publication_error)?;
        rebase_cleanup.disarm();
        self.cleanup_staging_on_drop = false;

        row_additions
            .into_iter()
            .map(|descriptor| {
                let (volume, path) =
                    open_artifact_volume(&self.database_root, &self.schema, descriptor)?;
                Ok(PublishedDataSegment { volume, path })
            })
            .collect()
    }
}

impl Drop for CompactionPublication {
    fn drop(&mut self) {
        if self.cleanup_staging_on_drop {
            let _ = std::fs::remove_dir_all(self.staging.path());
        }
    }
}

impl MVCCEngine {
    fn publication_transaction_high_water(&self, current: &PhysicalGenerationSnapshot) -> u64 {
        let persisted = current.database_manifest().transaction_high_water();
        self.persistence().map_or(persisted, |persistence| {
            persisted.max(persistence.transaction_high_water().max(0) as u64)
        })
    }

    pub(super) fn begin_compaction_publication(
        &self,
        table_name: &str,
        volumes: &[(u64, Arc<FrozenVolume>)],
    ) -> Result<CompactionPublication> {
        if volumes.is_empty() {
            return Err(Error::internal("compaction publication has no inputs"));
        }
        let publisher = self.physical_generation.load_full().ok_or_else(|| {
            Error::internal("physical generation publisher is unavailable during compaction")
        })?;
        let source = publisher.pin().map_err(format_error)?;
        let current = source.snapshot();
        let expected_control = current.control();
        let target_generation = expected_control
            .database_generation()
            .checked_next()
            .map_err(format_error)?;
        let logical = self.pin_catalog()?;
        validate_catalog_identity(logical.as_ref(), expected_control)?;
        if logical.meta().catalog_generation() != expected_control.catalog().generation().get()
            || logical.meta().catalog_id() != expected_control.catalog().id().into_bytes()
        {
            return Err(Error::internal(
                "compaction requires the CONTROL-selected catalog generation",
            ));
        }
        let schema = self
            .schemas
            .read()
            .unwrap()
            .get(&table_name.to_lowercase())
            .cloned()
            .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))?;
        let table_id = ObjectId::from_user_bytes(schema.catalog_id)
            .map_err(|error| Error::internal(format!("invalid table catalog identity: {error}")))?;
        let table_manifest = current
            .table_manifests()
            .binary_search_by_key(&table_id, TableManifest::table_id)
            .ok()
            .and_then(|index| current.table_manifests().get(index))
            .ok_or_else(|| Error::internal("compacted table is absent from physical generation"))?;
        let mut removals = Vec::with_capacity(volumes.len());
        let mut expected_inputs = Vec::with_capacity(volumes.len());
        let mut min_transaction_id = u64::MAX;
        let mut max_transaction_id = 0_u64;
        for (_, volume) in volumes {
            let artifact = volume.artifact_source().ok_or_else(|| {
                Error::internal("compaction input is not a canonical DATA artifact")
            })?;
            let segment_id = artifact.layout().header().segment_id();
            if removals.contains(&segment_id) {
                return Err(Error::internal(
                    "compaction input repeats a physical segment identity",
                ));
            }
            let descriptor = table_manifest
                .segments()
                .binary_search_by_key(&segment_id, |segment| segment.id())
                .ok()
                .and_then(|index| table_manifest.segments().get(index))
                .copied()
                .ok_or_else(|| {
                    Error::internal("compaction input is absent from physical table manifest")
                })?;
            if descriptor.data_artifact() != artifact.layout().reference() {
                return Err(Error::internal(
                    "compaction input DATA reference differs from physical table manifest",
                ));
            }
            min_transaction_id = min_transaction_id.min(descriptor.min_transaction_id());
            max_transaction_id = max_transaction_id.max(descriptor.max_transaction_id());
            expected_inputs.push(descriptor);
            removals.push(segment_id);
        }

        let created_unix_ns = unix_time_nanos();
        let writer = WriterInstanceId::new();
        let staging = StagedArtifactSet::create(
            Path::new(&self.path).join("staging"),
            writer,
            target_generation,
            process_id(),
            created_unix_ns,
        )
        .map_err(format_error)?;
        let build = publisher
            .begin_artifact_build(ArtifactBuildKind::Compaction, &source, staging.owner())
            .map_err(format_error)?;
        let columns = table_columns(logical.as_ref(), table_id)?;
        let accelerators = table_accelerators(logical.as_ref(), table_id)?;
        Ok(CompactionPublication {
            database_root: Path::new(&self.path).to_path_buf(),
            publisher,
            logical,
            schema,
            table_id,
            columns,
            accelerators,
            staging,
            build,
            expected_control,
            target_generation,
            created_unix_ns,
            min_transaction_id,
            max_transaction_id,
            expected_inputs,
            removals,
            additions: Vec::new(),
            cleanup_staging_on_drop: true,
        })
    }

    pub(super) fn publish_data_segment(
        &self,
        table_name: &str,
        rows: RowVec,
        compress: bool,
    ) -> DataSegmentPublicationResult<PublishedDataSegment> {
        if rows.is_empty() {
            return Err(Error::internal("cannot publish an empty DATA segment").into());
        }
        let publisher = self.physical_generation.load_full().ok_or_else(|| {
            Error::internal("physical generation publisher is unavailable during seal")
        })?;
        let source = publisher.pin().map_err(format_error)?;
        let current = source.snapshot();
        let control = current.control();
        let target_generation = control
            .database_generation()
            .checked_next()
            .map_err(format_error)?;
        let logical = self.pin_catalog()?;
        validate_catalog_identity(logical.as_ref(), control)?;

        let schema = self
            .schemas
            .read()
            .unwrap()
            .get(&table_name.to_lowercase())
            .cloned()
            .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))?;
        let table_id = ObjectId::from_user_bytes(schema.catalog_id)
            .map_err(|error| Error::internal(format!("invalid table catalog identity: {error}")))?;
        let columns = table_columns(logical.as_ref(), table_id)?;
        let row_count = rows.len() as u64;
        let first_row_id = rows
            .first()
            .map(|(row_id, _)| *row_id)
            .map(crate::v6::encode_runtime_row_id)
            .ok_or_else(|| Error::internal("DATA segment has no first row ID"))?;
        let last_row_id = rows
            .last()
            .map(|(row_id, _)| *row_id)
            .map(crate::v6::encode_runtime_row_id)
            .ok_or_else(|| Error::internal("DATA segment has no last row ID"))?;
        let now = unix_time_nanos();
        let writer = WriterInstanceId::new();
        let staging = StagedArtifactSet::create(
            Path::new(&self.path).join("staging"),
            writer,
            target_generation,
            process_id(),
            now,
        )
        .map_err(format_error)?;
        let build = publisher
            .begin_artifact_build(ArtifactBuildKind::Seal, &source, staging.owner())
            .map_err(format_error)?;

        let segment_id = SegmentId::new();
        let artifact_id = ArtifactId::new();
        let limits = FanoutBuildLimits::default();
        let column_count = u32::try_from(columns.len())
            .map_err(|_| Error::internal("DATA column count exceeds u32"))?;
        let row_group_rows = limits
            .planned_row_group_rows(row_count, column_count)
            .map_err(format_error)?;
        let row_group_count = row_count.div_ceil(u64::from(row_group_rows));
        let transaction_high_water = self.persistence().map_or(1, |persistence| {
            persistence.transaction_high_water().max(1) as u64
        });
        let header = DataArtifactHeader::new(
            artifact_id,
            control.database_id(),
            table_id,
            segment_id,
            target_generation,
            CatalogGeneration::new(logical.meta().catalog_generation()).map_err(format_error)?,
            1,
            transaction_high_water,
            row_count,
            column_count,
            u32::try_from(row_group_count)
                .map_err(|_| Error::internal("DATA row-group count exceeds u32"))?,
            SegmentKind::Rows,
            now,
        )
        .map_err(format_error)?;
        let codec = if compress {
            DataPhysicalCodec::Lz4
        } else {
            DataPhysicalCodec::None
        };
        let policies = columns
            .iter()
            .copied()
            .map(|column| data_column_policy(column, codec))
            .collect::<Vec<_>>();
        let relative =
            ArtifactLocator::derive(artifact_id, ArtifactKind::Data).relative_path(artifact_id);
        let relative_text = relative
            .to_str()
            .ok_or_else(|| Error::internal("canonical DATA path is not UTF-8"))?;
        let source_rows = rows.into_vec();
        let accelerators = table_accelerators(logical.as_ref(), table_id)?;
        let (data_reference, index_reference) = if let Some(accelerators) = accelerators {
            let index_artifact_id = ArtifactId::new();
            let index_relative = ArtifactLocator::derive(index_artifact_id, ArtifactKind::Index)
                .relative_path(index_artifact_id);
            let index_relative_text = index_relative
                .to_str()
                .ok_or_else(|| Error::internal("canonical INDEX path is not UTF-8"))?;
            let request = ArtifactPairBuildRequest::new(
                header,
                columns,
                policies,
                codec,
                index_artifact_id,
                accelerators,
                limits,
                staging.path(),
            )
            .map_err(format_error)?;
            let normalized_schema = schema.clone();
            let written = staging
                .write_generation_file(StagedMemberRole::Index, index_relative_text, |index_file| {
                    staging.write_generation_file(
                        StagedMemberRole::Data,
                        relative_text,
                        |data_file| {
                            write_artifact_pair(
                                &request,
                                normalized_source_rows(source_rows, &normalized_schema),
                                data_file,
                                index_file,
                            )
                        },
                    )
                })
                .map_err(format_error)?;
            if written.index_reference().relative_path() != index_relative {
                return Err(Error::internal(
                    "INDEX writer returned a non-canonical artifact locator",
                )
                .into());
            }
            (written.data_reference(), Some(written.index_reference()))
        } else {
            let request = DataArtifactBuildRequest::new(header, columns, policies, codec, limits)
                .map_err(format_error)?;
            let normalized_schema = schema.clone();
            let written = staging
                .write_generation_file(StagedMemberRole::Data, relative_text, |file| {
                    write_data_artifact(
                        &request,
                        normalized_source_rows(source_rows, &normalized_schema),
                        file,
                    )
                })
                .map_err(format_error)?;
            (written.data_reference(), None)
        };
        if data_reference.relative_path() != relative {
            return Err(
                Error::internal("DATA writer returned a non-canonical artifact locator").into(),
            );
        }
        let descriptor = SegmentDescriptor::new(
            segment_id,
            SegmentKind::Rows,
            1,
            transaction_high_water,
            row_count,
            first_row_id,
            last_row_id,
            data_reference,
            index_reference,
        )
        .map_err(format_error)?;

        let catalog_reference = stage_catalog_if_changed(
            &staging,
            logical.as_ref(),
            current.database_manifest().catalog(),
        )?;
        let segment_change = TableSegmentChange::append(table_id, descriptor);
        let table_references = stage_table_manifests(
            &staging,
            logical.as_ref(),
            current,
            target_generation,
            catalog_reference.generation(),
            std::slice::from_ref(&segment_change),
            &self.version_stores,
            now,
        )?;
        let database_manifest_id = ManifestId::new();
        let database_manifest = DatabaseManifest::new(
            control.database_id(),
            database_manifest_id,
            target_generation,
            catalog_reference,
            control.wal_replay_floor(),
            self.publication_transaction_high_water(current),
            table_references,
            now,
        )
        .map_err(format_error)?;
        let database_bytes = encode_database_manifest(&database_manifest).map_err(format_error)?;
        let database_reference = DatabaseManifestRootRef::new(
            database_manifest_id,
            ManifestGeneration::new(target_generation.get()).map_err(format_error)?,
            footer_sha(&database_bytes)?,
        );
        write_staged(
            &staging,
            StagedMemberRole::DatabaseManifest,
            database_manifest_path(database_reference),
            &database_bytes,
        )?;
        let target_control = ControlRecord::new(
            inactive_slot(control.slot()),
            target_generation,
            control.database_id(),
            database_reference,
            CatalogRootRef::new(
                catalog_reference.id(),
                catalog_reference.generation(),
                *catalog_reference.body_sha256(),
            ),
            control.wal_replay_floor(),
            now,
            writer,
        )
        .map_err(format_error)?;
        let complete = staging
            .mark_complete(now, StagingDiscoveryLimits::default())
            .map_err(format_error)?;
        #[cfg(any(test, feature = "test-failpoints"))]
        crate::test_failpoints::interleave_scoped(
            self.schema_scope_id,
            crate::test_failpoints::InterleavePoint::SealBeforePublish,
            0,
        );
        publisher
            .publish_staged_generation(build, complete, target_control)
            .map_err(data_segment_publication_error)?;

        let (volume, path) = open_artifact_volume(Path::new(&self.path), &schema, descriptor)?;
        Ok(PublishedDataSegment { volume, path })
    }

    /// Publish the durable checkpoint boundary as one physical generation.
    ///
    /// The WAL manager has already flushed the checkpoint prefix and ensured
    /// that the next replay-floor generation exists.  While the caller owns
    /// both DDL and commit fences this method freezes that WAL member together
    /// with the catalog/manifests and switches CONTROL only after every member
    /// is durable.
    pub(super) fn publish_checkpoint_generation(
        &self,
        checkpoint_lsn: u64,
        target_wal_generation: WalGeneration,
    ) -> Result<()> {
        if checkpoint_lsn == 0 {
            return Err(Error::internal(
                "physical checkpoint requires a non-zero durable WAL boundary",
            ));
        }
        let publisher = self.physical_generation.load_full().ok_or_else(|| {
            Error::internal("physical generation publisher is unavailable during checkpoint")
        })?;
        let source = publisher.pin().map_err(format_error)?;
        let current = source.snapshot();
        let control = current.control();
        let current_floor = control.wal_replay_floor();
        if checkpoint_lsn < current_floor.lsn() {
            return Err(Error::internal(
                "physical checkpoint WAL boundary regresses below CONTROL",
            ));
        }
        if checkpoint_lsn == current_floor.lsn() {
            // No committed WAL suffix exists beyond the already-published
            // replay floor. Publishing another database generation would
            // require a successor WAL that retention correctly did not create
            // and would turn an otherwise empty checkpoint into fake work.
            return Ok(());
        }
        let target_generation = control
            .database_generation()
            .checked_next()
            .map_err(format_error)?;
        if target_wal_generation <= current_floor.generation() {
            return Err(Error::internal(
                "physical checkpoint WAL generation does not advance CONTROL",
            ));
        }
        let logical = self.pin_catalog()?;
        validate_catalog_identity(logical.as_ref(), control)?;

        let now = unix_time_nanos();
        let writer = WriterInstanceId::new();
        let staging = StagedArtifactSet::create(
            Path::new(&self.path).join("staging"),
            writer,
            target_generation,
            process_id(),
            now,
        )
        .map_err(format_error)?;
        let build = publisher
            .begin_artifact_build(ArtifactBuildKind::Seal, &source, staging.owner())
            .map_err(format_error)?;

        stage_wal_generation(&staging, Path::new(&self.path), target_wal_generation)?;
        let catalog_reference = stage_catalog_if_changed(
            &staging,
            logical.as_ref(),
            current.database_manifest().catalog(),
        )?;
        let mut segment_changes = stage_checkpoint_tombstones(
            &staging,
            logical.as_ref(),
            current,
            target_generation,
            catalog_reference.generation(),
            self,
            now,
        )?;
        merge_segment_changes(
            &mut segment_changes,
            checkpoint_row_membership_changes(logical.as_ref(), current, self)?,
        )?;
        let table_references = stage_table_manifests(
            &staging,
            logical.as_ref(),
            current,
            target_generation,
            catalog_reference.generation(),
            &segment_changes,
            &self.version_stores,
            now,
        )?;
        let database_manifest_id = ManifestId::new();
        let target_floor = WalReplayFloor::new(target_wal_generation, checkpoint_lsn);
        let database_manifest = DatabaseManifest::new(
            control.database_id(),
            database_manifest_id,
            target_generation,
            catalog_reference,
            target_floor,
            self.publication_transaction_high_water(current),
            table_references,
            now,
        )
        .map_err(format_error)?;
        let database_bytes = encode_database_manifest(&database_manifest).map_err(format_error)?;
        let database_reference = DatabaseManifestRootRef::new(
            database_manifest_id,
            ManifestGeneration::new(target_generation.get()).map_err(format_error)?,
            footer_sha(&database_bytes)?,
        );
        write_staged(
            &staging,
            StagedMemberRole::DatabaseManifest,
            database_manifest_path(database_reference),
            &database_bytes,
        )?;
        let target_control = ControlRecord::new(
            inactive_slot(control.slot()),
            target_generation,
            control.database_id(),
            database_reference,
            CatalogRootRef::new(
                catalog_reference.id(),
                catalog_reference.generation(),
                *catalog_reference.body_sha256(),
            ),
            target_floor,
            now,
            writer,
        )
        .map_err(format_error)?;
        let complete = staging
            .mark_complete(now, StagingDiscoveryLimits::default())
            .map_err(format_error)?;
        let persistence = self
            .persistence()
            .ok_or_else(|| Error::internal("physical checkpoint lost its WAL persistence owner"))?;
        let obsolete_wal =
            persistence.checkpoint_retirement_candidates(current_floor.generation())?;
        let checkpoint = FrozenCheckpoint::new(
            control,
            target_control,
            complete,
            build,
            obsolete_wal.clone(),
        )
        .map_err(format_error)?;
        #[cfg(any(test, feature = "test-failpoints"))]
        crate::test_failpoints::interleave(
            crate::test_failpoints::InterleavePoint::ManifestBeforePublish,
            0,
        );
        // WAL retirement is post-CONTROL housekeeping. A deferred retirement
        // leaves a complete checkpoint and is retried by later maintenance;
        // it must not turn the committed generation into a failed checkpoint.
        let outcome = publisher
            .publish_checkpoint(checkpoint)
            .map_err(format_error)?;
        let retired = if matches!(outcome.wal_retirement(), WalRetirementStatus::Complete(_)) {
            obsolete_wal.as_slice()
        } else {
            &[]
        };
        persistence.confirm_checkpoint_publication(target_floor, retired);
        for change in &segment_changes {
            change.confirm_tombstone_publication();
        }
        Ok(())
    }
}

struct PublishedIndexTable {
    table_name: String,
    schema: CompactArc<Schema>,
    descriptors: Vec<SegmentDescriptor>,
}

/// Publish complete INDEX packs for CREATE INDEX definitions over already
/// sealed DATA, then replace their transaction-built cold runtime copies with
/// fresh hot-only generations. The caller owns the exclusive DDL and commit
/// visibility fences for the complete operation.
#[allow(clippy::too_many_arguments)]
pub(super) fn publish_transactional_indexes(
    database_root: &Path,
    physical_generation: &arc_swap::ArcSwapOption<PhysicalGenerationPublisher>,
    catalog_publisher: &arc_swap::ArcSwap<CatalogPublisher>,
    persistence: &arc_swap::ArcSwapOption<crate::mvcc::PersistenceManager>,
    schemas: &RwLock<FxHashMap<String, CompactArc<Schema>>>,
    version_stores: &RwLock<FxHashMap<String, Arc<crate::mvcc::VersionStore>>>,
    segment_managers: &RwLock<FxHashMap<String, Arc<crate::volume::manifest::SegmentManager>>>,
    pending_indexes: &[crate::traits::PendingIndexDefinition],
) -> Result<FxHashSet<String>> {
    if pending_indexes.is_empty() || database_root.as_os_str().is_empty() {
        return Ok(FxHashSet::default());
    }
    let Some(publisher) = physical_generation.load_full() else {
        return Ok(FxHashSet::default());
    };
    let source = publisher.pin().map_err(format_error)?;
    let current = source.snapshot();
    let control = current.control();
    let target_generation = control
        .database_generation()
        .checked_next()
        .map_err(format_error)?;
    let logical = catalog_publisher
        .load_full()
        .pin()
        .map_err(|error| Error::internal(format!("catalog pin failed: {error}")))?;
    validate_catalog_identity(logical.as_ref(), control)?;
    let catalog_generation =
        CatalogGeneration::new(logical.meta().catalog_generation()).map_err(format_error)?;

    let mut table_names = pending_indexes
        .iter()
        .map(|definition| definition.table_name.to_lowercase())
        .collect::<Vec<_>>();
    table_names.sort_unstable();
    table_names.dedup();

    struct Candidate {
        table_name: String,
        table_id: ObjectId,
        requested_index_id: ObjectId,
        schema: CompactArc<Schema>,
        accelerators: Vec<AcceleratorBuildSpec>,
        segments: Vec<SegmentDescriptor>,
    }

    let schema_guard = schemas.read().unwrap();
    let mut candidates = Vec::new();
    for table_name in table_names {
        let Some(schema) = schema_guard.get(&table_name).cloned() else {
            continue;
        };
        let table_id = ObjectId::from_user_bytes(schema.catalog_id)
            .map_err(|error| Error::internal(format!("invalid table catalog identity: {error}")))?;
        let Some(accelerators) = table_accelerators(logical.as_ref(), table_id)? else {
            // Partial/expression/HNSW definitions have no complete immutable
            // pack yet. Their correct transaction-built runtime coverage stays
            // live until that dedicated format owner exists.
            continue;
        };
        let mut requested_index_ids = pending_indexes
            .iter()
            .filter(|definition| definition.table_name.eq_ignore_ascii_case(&table_name))
            .map(|definition| {
                logical
                    .find_index(ObjectId::BOOTSTRAP_NAMESPACE, &definition.index_name)
                    .map_err(|error| {
                        Error::internal(format!("catalog index lookup failed: {error}"))
                    })?
                    .filter(|index| index.parent_id() == Some(table_id))
                    .map(|index| index.id())
                    .ok_or_else(|| {
                        Error::internal("pending CREATE INDEX is absent from the committed catalog")
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        requested_index_ids.sort_unstable();
        requested_index_ids.dedup();
        let requested_index_id = requested_index_ids
            .first()
            .copied()
            .ok_or_else(|| Error::internal("INDEX backfill has no pending catalog identity"))?;
        if !accelerators
            .iter()
            .any(|accelerator| accelerator.logical_index_id() == requested_index_id)
        {
            return Err(Error::internal(
                "pending CREATE INDEX is absent from the complete accelerator pack",
            ));
        }
        let segments = current
            .table_manifest(table_id)
            .map(|manifest| {
                manifest
                    .segments()
                    .iter()
                    .copied()
                    .filter(|descriptor| descriptor.kind() == SegmentKind::Rows)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if segments.is_empty() {
            continue;
        }
        candidates.push(Candidate {
            table_name,
            table_id,
            requested_index_id,
            schema,
            accelerators,
            segments,
        });
    }
    drop(schema_guard);
    if candidates.is_empty() {
        return Ok(FxHashSet::default());
    }

    let now = unix_time_nanos();
    let writer = WriterInstanceId::new();
    let staging = StagedArtifactSet::create(
        database_root.join("staging"),
        writer,
        target_generation,
        process_id(),
        now,
    )
    .map_err(format_error)?;
    let build = publisher
        .begin_artifact_build(ArtifactBuildKind::DdlPublication, &source, staging.owner())
        .map_err(format_error)?;
    let mut changes = Vec::with_capacity(candidates.len());
    let mut published_tables = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let requested = candidate
            .accelerators
            .iter()
            .find(|accelerator| accelerator.logical_index_id() == candidate.requested_index_id)
            .ok_or_else(|| Error::internal("INDEX backfill has no accelerator definition"))?;
        let mut descriptors = Vec::with_capacity(candidate.segments.len());
        for source_descriptor in candidate.segments {
            let data_path = database_root.join(source_descriptor.data_artifact().relative_path());
            let data_source =
                crate::v6::ArtifactDataSource::open(&data_path, source_descriptor.data_artifact())
                    .map_err(format_error)?;
            let data = data_source.layout();
            let existing_index = source_descriptor.index_artifact().and_then(|reference| {
                let source =
                    crate::v6::ArtifactFile::open(database_root.join(reference.relative_path()))
                        .ok()?;
                let layout = crate::v6::open_index_artifact_metadata(&source, reference, data)
                    .ok()?
                    .into_layout();
                Some((source, layout))
            });
            let rebuild = IndexRebuildRequest::for_ddl_publication(
                control.database_id(),
                candidate.table_id,
                source_descriptor.id(),
                source_descriptor.data_artifact(),
                requested.logical_index_id(),
                *requested.definition_sha256(),
            );
            let artifact_id = ArtifactId::new();
            let relative = ArtifactLocator::derive(artifact_id, ArtifactKind::Index)
                .relative_path(artifact_id);
            let relative_text = relative
                .to_str()
                .ok_or_else(|| Error::internal("canonical INDEX path is not UTF-8"))?;
            // DDL rebuild has no DATA writer buffers. Let narrow keys use the
            // already-governed rebuild memory before spilling; the byte limit
            // remains authoritative for wide/user-controlled keys.
            let ddl_limits = FanoutBuildLimits::default()
                .with_sort_run_records(MAX_SORT_RUN_RECORDS)
                .map_err(format_error)?;
            let request = IndexReplacementBuildRequest::new(
                rebuild,
                artifact_id,
                target_generation,
                catalog_generation,
                candidate.accelerators.clone(),
                ddl_limits,
                staging.path(),
            )
            .map_err(format_error)?;
            let written = staging
                .write_generation_file(StagedMemberRole::Index, relative_text, |file| {
                    let reusable = existing_index
                        .as_ref()
                        .map(|(source, layout)| (source as &dyn crate::v6::ArtifactSource, layout));
                    write_index_replacement_reusing(&request, &data_source, data, reusable, file)
                })
                .map_err(format_error)?;
            if written.reference().relative_path() != relative {
                return Err(Error::internal(
                    "INDEX rebuild writer returned a non-canonical artifact locator",
                ));
            }
            descriptors.push(
                SegmentDescriptor::new_at_tier(
                    source_descriptor.id(),
                    source_descriptor.kind(),
                    source_descriptor.tier(),
                    source_descriptor.min_transaction_id(),
                    source_descriptor.max_transaction_id(),
                    source_descriptor.row_count(),
                    source_descriptor.first_row_id(),
                    source_descriptor.last_row_id(),
                    source_descriptor.data_artifact(),
                    Some(written.reference()),
                )
                .map_err(format_error)?,
            );
        }
        changes.push(TableSegmentChange {
            table_id: candidate.table_id,
            removals: descriptors
                .iter()
                .map(|descriptor| descriptor.id())
                .collect(),
            additions: descriptors.clone(),
            tombstone_ack: None,
        });
        published_tables.push(PublishedIndexTable {
            table_name: candidate.table_name,
            schema: candidate.schema,
            descriptors,
        });
    }

    let catalog_reference = stage_catalog_if_changed(
        &staging,
        logical.as_ref(),
        current.database_manifest().catalog(),
    )?;
    if catalog_reference.generation() != catalog_generation {
        return Err(Error::internal(
            "DDL INDEX publication did not advance the physical catalog",
        ));
    }
    let table_references = stage_table_manifests(
        &staging,
        logical.as_ref(),
        current,
        target_generation,
        catalog_generation,
        &changes,
        version_stores,
        now,
    )?;
    let database_manifest_id = ManifestId::new();
    let database_manifest = DatabaseManifest::new(
        control.database_id(),
        database_manifest_id,
        target_generation,
        catalog_reference,
        control.wal_replay_floor(),
        persistence.load_full().map_or(
            current.database_manifest().transaction_high_water(),
            |owner| {
                current
                    .database_manifest()
                    .transaction_high_water()
                    .max(owner.transaction_high_water().max(0) as u64)
            },
        ),
        table_references,
        now,
    )
    .map_err(format_error)?;
    let database_bytes = encode_database_manifest(&database_manifest).map_err(format_error)?;
    let database_reference = DatabaseManifestRootRef::new(
        database_manifest_id,
        ManifestGeneration::new(target_generation.get()).map_err(format_error)?,
        footer_sha(&database_bytes)?,
    );
    write_staged(
        &staging,
        StagedMemberRole::DatabaseManifest,
        database_manifest_path(database_reference),
        &database_bytes,
    )?;
    let target_control = ControlRecord::new(
        inactive_slot(control.slot()),
        target_generation,
        control.database_id(),
        database_reference,
        CatalogRootRef::new(
            catalog_reference.id(),
            catalog_reference.generation(),
            *catalog_reference.body_sha256(),
        ),
        control.wal_replay_floor(),
        now,
        writer,
    )
    .map_err(format_error)?;
    let complete = staging
        .mark_complete(now, StagingDiscoveryLimits::default())
        .map_err(format_error)?;
    publisher
        .publish_ddl_generation(build, complete, target_control)
        .map_err(format_error)?;

    let managers = segment_managers.read().unwrap();
    let stores = version_stores.read().unwrap();
    let mut published_table_names = FxHashSet::default();
    for table in published_tables {
        published_table_names.insert(table.table_name.clone());
        let manager = managers
            .get(&table.table_name)
            .ok_or_else(|| Error::internal("published INDEX table lost its segment manager"))?;
        let mut attachments = Vec::with_capacity(table.descriptors.len());
        for descriptor in table.descriptors {
            let (volume, _) = open_artifact_volume(database_root, &table.schema, descriptor)?;
            attachments.push((descriptor.data_artifact(), volume));
        }
        manager.attach_index_artifacts(&attachments)?;
        let cold_names = manager.cold_populated_index_names();
        if cold_names.is_empty() {
            continue;
        }
        let store = stores
            .get(&table.table_name)
            .ok_or_else(|| Error::internal("published INDEX table lost its version store"))?;
        store.replace_cold_backfills_with_hot_only(&cold_names)?;
        for name in cold_names {
            manager.unmark_cold_populated_index(&name);
        }
    }
    Ok(published_table_names)
}

/// Reconcile process-local row membership with the CONTROL-selected table
/// manifests before a checkpoint is allowed to retire its WAL prefix.
///
/// Ordinary seal and compaction publish their immutable membership before
/// installing it in the runtime manager, so the two sets normally match.
/// TRUNCATE is different: its WAL record is the immediate durability owner and
/// runtime membership becomes empty without rewriting immutable DATA. This
/// comparison turns that removal into a manifest-only checkpoint change. It
/// also closes the same state after WAL recovery replays a pre-checkpoint
/// TRUNCATE.
fn checkpoint_row_membership_changes(
    logical: &RuntimeCatalog,
    current: &PhysicalGenerationSnapshot,
    engine: &MVCCEngine,
) -> Result<Vec<TableSegmentChange>> {
    let managers = engine.segment_managers.read().unwrap();
    let mut changes = Vec::new();

    for manifest in current.table_manifests() {
        let table_id = manifest.table_id();
        let Some(table) = logical.object(table_id) else {
            // A committed DROP TABLE removes this manifest when the same
            // checkpoint stages the newer catalog generation.
            continue;
        };
        let table_name = table.name().display().as_str().to_lowercase();
        let runtime_ids = managers
            .get(&table_name)
            .map(|manager| {
                manager
                    .get_segments_ordered_meta()
                    .into_iter()
                    .map(|volume| {
                        let source = volume.artifact_source().ok_or_else(|| {
                            Error::internal(format!(
                                "checkpoint table '{}' has a row segment without canonical DATA",
                                table_name
                            ))
                        })?;
                        if source.layout().header().table_id() != table_id
                            || source.layout().header().segment_kind() != SegmentKind::Rows
                        {
                            return Err(Error::internal(format!(
                                "checkpoint table '{}' has a DATA segment with mismatched identity",
                                table_name
                            )));
                        }
                        Ok(source.layout().header().segment_id())
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let runtime_ids = runtime_ids
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let durable_ids = manifest
            .segments()
            .iter()
            .filter(|segment| segment.kind() == SegmentKind::Rows)
            .map(|segment| segment.id())
            .collect::<std::collections::BTreeSet<_>>();

        if let Some(unpublished) = runtime_ids.difference(&durable_ids).next() {
            return Err(Error::internal(format!(
                "checkpoint table '{}' contains runtime segment {} absent from CONTROL",
                table_name, unpublished
            )));
        }
        let removals = durable_ids
            .difference(&runtime_ids)
            .copied()
            .collect::<Vec<_>>();
        if !removals.is_empty() {
            changes.push(TableSegmentChange {
                table_id,
                additions: Vec::new(),
                removals,
                tombstone_ack: None,
            });
        }
    }
    Ok(changes)
}

fn merge_segment_changes(
    target: &mut Vec<TableSegmentChange>,
    incoming: Vec<TableSegmentChange>,
) -> Result<()> {
    for mut change in incoming {
        if let Some(existing) = target
            .iter_mut()
            .find(|existing| existing.table_id == change.table_id)
        {
            if existing.tombstone_ack.is_some() && change.tombstone_ack.is_some() {
                return Err(Error::internal(
                    "checkpoint produced duplicate tombstone publication acknowledgements",
                ));
            }
            existing.additions.append(&mut change.additions);
            existing.removals.append(&mut change.removals);
            if existing.tombstone_ack.is_none() {
                existing.tombstone_ack = change.tombstone_ack.take();
            }
        } else {
            target.push(change);
        }
    }
    target.sort_unstable_by_key(|change| change.table_id);
    for change in target {
        change.removals.sort_unstable();
        if change.removals.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::internal(
                "checkpoint segment change removes one segment twice",
            ));
        }
    }
    Ok(())
}

fn validate_catalog_identity(logical: &RuntimeCatalog, control: ControlRecord) -> Result<()> {
    let meta = logical.meta();
    if meta.database_id() != control.database_id().into_bytes()
        || meta.catalog_generation() < control.catalog().generation().get()
        || (meta.catalog_generation() == control.catalog().generation().get()
            && meta.catalog_id() != control.catalog().id().into_bytes())
    {
        return Err(Error::internal(
            "logical catalog is not a successor of the physical CONTROL root",
        ));
    }
    Ok(())
}

fn table_columns(catalog: &RuntimeCatalog, table_id: ObjectId) -> Result<Vec<DataColumnSpec>> {
    let table = catalog
        .object(table_id)
        .ok_or_else(|| Error::internal("sealed table is absent from logical catalog"))?;
    let CatalogPayload::Table(table) = table.payload() else {
        return Err(Error::internal("sealed catalog object is not a table"));
    };
    table
        .column_ids()
        .iter()
        .map(|column_id| {
            let column = catalog
                .object(*column_id)
                .ok_or_else(|| Error::internal("table column is absent from logical catalog"))?;
            let CatalogPayload::Column(column) = column.payload() else {
                return Err(Error::internal("table column object has the wrong kind"));
            };
            Ok(DataColumnSpec::new(
                *column_id,
                column.data_type(),
                column.nullable(),
            ))
        })
        .collect()
}

/// Resolve the complete accelerator pack for a table. `None` means that the
/// generation intentionally publishes authoritative DATA without an INDEX
/// member; readers must use the existing bounded scan fallback and maintenance
/// may build the complete pack later. A partial pack is never manufactured.
fn table_accelerators(
    catalog: &RuntimeCatalog,
    table_id: ObjectId,
) -> Result<Option<Vec<AcceleratorBuildSpec>>> {
    let table = catalog
        .object(table_id)
        .ok_or_else(|| Error::internal("sealed table is absent from logical catalog"))?;
    let CatalogPayload::Table(table_payload) = table.payload() else {
        return Err(Error::internal("sealed catalog object is not a table"));
    };
    if table_payload.index_ids().is_empty() {
        return Ok(None);
    }

    let mut output = Vec::with_capacity(table_payload.index_ids().len());
    for index_id in table_payload.index_ids() {
        let index = catalog
            .object(*index_id)
            .ok_or_else(|| Error::internal("table index is absent from logical catalog"))?;
        let CatalogPayload::Index(payload) = index.payload() else {
            return Err(Error::internal("table index object has the wrong kind"));
        };
        if index.parent_id() != Some(table_id) {
            return Err(Error::internal("table index has the wrong catalog parent"));
        }

        // The initial streaming writer has no expression/predicate/INCLUDE or
        // HNSW builder. INDEX is rebuildable, so the only coherent degraded
        // state is to omit the complete pack, never to publish a subset that
        // looks authoritative for this catalog generation.
        if payload.expression_sql().is_some()
            || payload.predicate_sql().is_some()
            || !payload.include_column_ids().is_empty()
            || payload.access_method() == AccessMethod::Hnsw
            || payload.operator_class_id().is_some()
        {
            return Ok(None);
        }

        let key_columns = payload
            .key_column_ids()
            .iter()
            .map(|column_id| {
                let column = catalog.object(*column_id).ok_or_else(|| {
                    Error::internal("index key column is absent from logical catalog")
                })?;
                let CatalogPayload::Column(column) = column.payload() else {
                    return Err(Error::internal("index key object is not a column"));
                };
                Ok(IndexKeyColumn::new(
                    *column_id,
                    column.data_type().logical_type(),
                    IndexSortDirection::Ascending,
                    IndexNullsOrder::Last,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let constraint_owned = catalog.graph().outgoing_edges(*index_id).any(|edge| {
            edge.kind() == EdgeKind::DependsOn
                && catalog
                    .object(edge.target_object_id())
                    .is_some_and(|target| {
                        matches!(
                            target.payload(),
                            CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { .. })
                                | CatalogPayload::Constraint(ConstraintPayload::Unique { .. })
                        )
                    })
        });
        let definition = index_definition_sha256(catalog, *index_id, payload);
        let accelerator = match payload.access_method() {
            AccessMethod::Btree => AcceleratorBuildSpec::ordered(
                *index_id,
                payload.unique(),
                constraint_owned,
                definition,
                key_columns,
                IndexPageCodec::Lz4,
                OrderedPageBuildLimits::default(),
            ),
            AccessMethod::Hash | AccessMethod::Bitmap => AcceleratorBuildSpec::exact(
                *index_id,
                payload.unique(),
                constraint_owned,
                definition,
                key_columns,
                IndexPageCodec::Lz4,
                ExactPageBuildLimits::default(),
            ),
            AccessMethod::Hnsw => unreachable!("HNSW was handled as a degraded complete pack"),
        }
        .map_err(format_error)?;
        output.push(accelerator);
    }
    Ok(Some(output))
}

/// Stable physical binding for one logical index definition. The digest is
/// deliberately independent of serialization order and physical generations;
/// it changes with the index payload itself or any declared dependency.
fn index_definition_sha256(
    catalog: &RuntimeCatalog,
    index_id: ObjectId,
    payload: &IndexPayload,
) -> [u8; 32] {
    let index = catalog
        .object(index_id)
        .expect("validated catalog index disappeared");
    let mut digest = Sha256::new();
    digest.update(b"RADIXDB-INDEX-DEFINITION\0");
    digest.update(index.definition_revision().to_le_bytes());
    digest.update(payload.access_method().tag().to_le_bytes());
    digest.update([u8::from(payload.unique())]);
    digest_object_ids(&mut digest, payload.key_column_ids());
    digest_object_ids(&mut digest, payload.include_column_ids());
    digest_optional_text(
        &mut digest,
        payload.expression_sql().map(|sql| sql.as_str()),
    );
    digest_optional_text(&mut digest, payload.predicate_sql().map(|sql| sql.as_str()));
    if let Some(parameters) = payload.hnsw_parameters() {
        digest.update([1]);
        digest.update(parameters.m().to_le_bytes());
        digest.update(parameters.ef_construction().to_le_bytes());
        digest.update(parameters.ef_search().to_le_bytes());
        digest.update(parameters.distance_metric().tag().to_le_bytes());
    } else {
        digest.update([0]);
    }
    let dependencies = catalog
        .graph()
        .outgoing_edges(index_id)
        .filter(|edge| edge.kind().is_dependency())
        .collect::<Vec<_>>();
    digest.update((dependencies.len() as u64).to_le_bytes());
    for edge in dependencies {
        digest.update(edge.kind().tag().to_le_bytes());
        digest.update(edge.ordinal().to_le_bytes());
        digest.update(edge.target_object_id().as_bytes());
        let revision = catalog
            .object(edge.target_object_id())
            .map_or(0, |target| target.definition_revision());
        digest.update(revision.to_le_bytes());
    }
    digest.finalize().into()
}

fn digest_object_ids(digest: &mut Sha256, ids: &[ObjectId]) {
    digest.update((ids.len() as u64).to_le_bytes());
    for id in ids {
        digest.update(id.as_bytes());
    }
}

fn digest_optional_text(digest: &mut Sha256, text: Option<&str>) {
    if let Some(text) = text {
        digest.update([1]);
        digest.update((text.len() as u64).to_le_bytes());
        digest.update(text.as_bytes());
    } else {
        digest.update([0]);
    }
}

fn normalized_source_rows<'a>(
    rows: Vec<(i64, radixdb_core::Row)>,
    schema: &'a radixdb_core::Schema,
) -> impl Iterator<Item = crate::v6::FormatResult<SourceRow>> + 'a {
    rows.into_iter().map(move |(row_id, mut row)| {
        let row_id = crate::v6::encode_runtime_row_id(row_id);
        let schema_width = schema.columns.len();
        if row.len() < schema_width {
            for column in &schema.columns[row.len()..] {
                row.push(
                    column
                        .default_value
                        .clone()
                        .unwrap_or_else(|| radixdb_core::Value::null(column.data_type)),
                );
            }
        } else if row.len() > schema_width {
            row.truncate(schema_width);
        }
        Ok(SourceRow::new(row_id, row.into_values()))
    })
}

fn stage_catalog_if_changed(
    staging: &StagedArtifactSet,
    logical: &RuntimeCatalog,
    current: CatalogRef,
) -> Result<CatalogRef> {
    let meta = logical.meta();
    if meta.catalog_generation() == current.generation().get() {
        return Ok(current);
    }
    let bytes = encode_catalog_artifact(meta, logical.graph()).map_err(format_error)?;
    let reference = CatalogRef::new(
        CatalogId::from_bytes(meta.catalog_id()).map_err(format_error)?,
        CatalogGeneration::new(meta.catalog_generation()).map_err(format_error)?,
        bytes.len() as u64,
        footer_sha(&bytes)?,
    )
    .map_err(format_error)?;
    write_staged(
        staging,
        StagedMemberRole::CatalogPack,
        catalog_path(reference),
        &bytes,
    )?;
    Ok(reference)
}

/// Stage complete replacements for every committed tombstone set that has
/// changed since its last CONTROL-selected generation.
///
/// Tombstone DATA carries row IDs only.  Replacing all prior tombstone
/// descriptors for a changed table makes recovery independent from directory
/// order and prevents an obsolete delete marker from hiding a later sealed row.
/// Unchanged tables retain their existing descriptors byte-for-byte.
#[allow(clippy::too_many_arguments)]
fn stage_checkpoint_tombstones(
    staging: &StagedArtifactSet,
    logical: &RuntimeCatalog,
    current: &PhysicalGenerationSnapshot,
    target_generation: DatabaseGeneration,
    catalog_generation: CatalogGeneration,
    engine: &MVCCEngine,
    now: u64,
) -> Result<Vec<TableSegmentChange>> {
    let schemas = engine.schemas.read().unwrap();
    let managers = engine.segment_managers.read().unwrap();
    let mut dirty = Vec::new();
    for (table_name, manager) in managers.iter() {
        let Some(schema) = schemas.get(table_name) else {
            continue;
        };
        let table_id = ObjectId::from_user_bytes(schema.catalog_id).map_err(|error| {
            Error::internal(format!(
                "table '{}' has invalid catalog identity during tombstone publication: {error}",
                schema.table_name
            ))
        })?;
        if logical.object(table_id).is_none() {
            continue;
        }
        if let Some(snapshot) = manager.tombstone_publication_snapshot() {
            dirty.push((table_id, Arc::clone(manager), snapshot));
        }
    }
    drop(managers);
    drop(schemas);
    dirty.sort_unstable_by_key(|(table_id, _, _)| *table_id);

    let mut changes = Vec::with_capacity(dirty.len());
    for (table_id, manager, snapshot) in dirty {
        changes.push(stage_tombstone_replacement(
            staging,
            current,
            target_generation,
            catalog_generation,
            table_id,
            snapshot.tombstones(),
            Some(TombstonePublicationAck {
                manager,
                generation: snapshot.generation(),
            }),
            now,
        )?);
    }
    Ok(changes)
}

#[allow(clippy::too_many_arguments)]
fn stage_tombstone_replacement(
    staging: &StagedArtifactSet,
    current: &PhysicalGenerationSnapshot,
    target_generation: DatabaseGeneration,
    catalog_generation: CatalogGeneration,
    table_id: ObjectId,
    tombstone_map: &FxHashMap<i64, u64>,
    tombstone_ack: Option<TombstonePublicationAck>,
    now: u64,
) -> Result<TableSegmentChange> {
    let source = current
        .table_manifests()
        .binary_search_by_key(&table_id, TableManifest::table_id)
        .ok()
        .and_then(|index| current.table_manifests().get(index));
    let removals = source
        .into_iter()
        .flat_map(|manifest| manifest.segments())
        .filter(|segment| segment.kind() == SegmentKind::Tombstones)
        .map(|segment| segment.id())
        .collect::<Vec<_>>();

    let mut tombstones = tombstone_map
        .iter()
        .map(|(&row_id, &commit_seq)| {
            let row_id = crate::v6::encode_runtime_row_id(row_id);
            if commit_seq == 0 {
                return Err(Error::internal(
                    "committed tombstone has a zero visibility sequence",
                ));
            }
            Ok((row_id, commit_seq))
        })
        .collect::<Result<Vec<_>>>()?;
    tombstones.sort_unstable_by_key(|(row_id, _)| *row_id);

    let limits = FanoutBuildLimits::default();
    let max_rows_per_artifact = usize::try_from(u32::MAX).unwrap_or(usize::MAX);
    let mut additions = Vec::new();
    for chunk in tombstones.chunks(max_rows_per_artifact) {
        let row_count = chunk.len() as u64;
        let row_group_rows = limits
            .planned_row_group_rows(row_count, 0)
            .map_err(format_error)?;
        let row_group_count = row_count.div_ceil(u64::from(row_group_rows));
        let min_transaction_id = chunk
            .iter()
            .map(|(_, commit_seq)| *commit_seq)
            .min()
            .expect("tombstone chunks are non-empty");
        let max_transaction_id = chunk
            .iter()
            .map(|(_, commit_seq)| *commit_seq)
            .max()
            .expect("tombstone chunks are non-empty");
        let segment_id = SegmentId::new();
        let artifact_id = ArtifactId::new();
        let header = DataArtifactHeader::new(
            artifact_id,
            current.control().database_id(),
            table_id,
            segment_id,
            target_generation,
            catalog_generation,
            min_transaction_id,
            max_transaction_id,
            row_count,
            0,
            u32::try_from(row_group_count)
                .map_err(|_| Error::internal("tombstone row-group count exceeds u32"))?,
            SegmentKind::Tombstones,
            now,
        )
        .map_err(format_error)?;
        let relative =
            ArtifactLocator::derive(artifact_id, ArtifactKind::Data).relative_path(artifact_id);
        let relative_text = relative
            .to_str()
            .ok_or_else(|| Error::internal("canonical tombstone DATA path is not UTF-8"))?;
        let request = DataArtifactBuildRequest::new(
            header,
            Vec::new(),
            Vec::new(),
            DataPhysicalCodec::Lz4,
            limits,
        )
        .map_err(format_error)?;
        let written = staging
            .write_generation_file(StagedMemberRole::Data, relative_text, |file| {
                write_data_artifact(
                    &request,
                    chunk
                        .iter()
                        .map(|(row_id, _)| Ok(SourceRow::new(*row_id, Vec::new()))),
                    file,
                )
            })
            .map_err(format_error)?;
        if written.data_reference().relative_path() != relative {
            return Err(Error::internal(
                "tombstone writer returned a non-canonical DATA locator",
            ));
        }
        additions.push(
            SegmentDescriptor::new(
                segment_id,
                SegmentKind::Tombstones,
                min_transaction_id,
                max_transaction_id,
                row_count,
                chunk[0].0,
                chunk[chunk.len() - 1].0,
                written.data_reference(),
                None,
            )
            .map_err(format_error)?,
        );
    }

    Ok(TableSegmentChange {
        table_id,
        additions,
        removals,
        tombstone_ack,
    })
}

#[allow(clippy::too_many_arguments)]
fn stage_table_manifests(
    staging: &StagedArtifactSet,
    logical: &RuntimeCatalog,
    current: &PhysicalGenerationSnapshot,
    target_generation: DatabaseGeneration,
    catalog_generation: CatalogGeneration,
    segment_changes: &[TableSegmentChange],
    version_stores: &RwLock<FxHashMap<String, Arc<crate::mvcc::VersionStore>>>,
    now: u64,
) -> Result<Vec<TableManifestRef>> {
    let manifests = current.table_manifests();
    let mut tables = logical
        .objects_of_kind(ObjectKind::Table)
        .map(|object| object.id())
        .collect::<Vec<_>>();
    tables.sort_unstable();
    let mut references = Vec::with_capacity(tables.len());
    for table_id in tables {
        let source = manifests
            .binary_search_by_key(&table_id, TableManifest::table_id)
            .ok()
            .and_then(|index| manifests.get(index));
        let table_change = segment_changes
            .iter()
            .find(|change| change.table_id == table_id);
        let is_changed = table_change.is_some();
        if source.is_some() && !is_changed {
            source.ok_or_else(|| {
                Error::internal("physical generation lacks an unchanged catalog table")
            })?;
            let reference = current
                .database_manifest()
                .tables()
                .binary_search_by_key(&table_id, |reference| reference.table_id())
                .ok()
                .and_then(|index| current.database_manifest().tables().get(index))
                .copied()
                .ok_or_else(|| Error::internal("unchanged table reference is absent"))?;
            references.push(reference);
            continue;
        }
        let mut segments = source
            .map(|manifest| manifest.segments().to_vec())
            .unwrap_or_default();
        if let Some(change) = table_change {
            segments.retain(|segment| !change.removals.contains(&segment.id()));
            segments.extend(change.additions.iter().copied());
        }
        let table_name = logical
            .object(table_id)
            .map(|object| object.name().display().as_str().to_owned())
            .ok_or_else(|| Error::internal("catalog table disappeared during publication"))?;
        let runtime_high_water = version_stores
            .read()
            .unwrap()
            .get(&table_name.to_lowercase())
            .map_or(0, |store| store.get_auto_increment_counter().max(0) as u64);
        let row_id_high_water = source
            .map_or(0, TableManifest::row_id_high_water)
            .max(runtime_high_water)
            .max(
                table_change
                    .and_then(|change| {
                        change
                            .additions
                            .iter()
                            .filter(|segment| segment.kind() == SegmentKind::Rows)
                            .map(|segment| segment.last_row_id())
                            .max()
                    })
                    .unwrap_or(0),
            );
        let added_segments = table_change.map_or(0, |change| {
            change
                .additions
                .iter()
                .filter(|segment| !change.removals.contains(&segment.id()))
                .count() as u64
        });
        let next_segment_sequence = source
            .map_or(1, TableManifest::next_segment_sequence)
            .checked_add(added_segments)
            .ok_or_else(|| Error::internal("table segment sequence overflows"))?;
        let manifest_id = ManifestId::new();
        let manifest = TableManifest::new(
            DatabaseId::from_bytes(logical.meta().database_id()).map_err(format_error)?,
            table_id,
            manifest_id,
            ManifestGeneration::new(target_generation.get()).map_err(format_error)?,
            catalog_generation,
            row_id_high_water,
            next_segment_sequence,
            segments,
            now,
        )
        .map_err(format_error)?;
        let bytes = encode_table_manifest(&manifest).map_err(format_error)?;
        let reference = table_reference(&manifest, &bytes)?;
        write_staged(
            staging,
            StagedMemberRole::TableManifest,
            table_manifest_path(reference),
            &bytes,
        )?;
        references.push(reference);
    }
    Ok(references)
}

fn stage_wal_generation(
    staging: &StagedArtifactSet,
    database_root: &Path,
    generation: WalGeneration,
) -> Result<()> {
    let relative = wal_path(generation);
    let source_path = database_root.join(&relative);
    let metadata = std::fs::symlink_metadata(&source_path).map_err(|error| {
        Error::internal(format!(
            "failed to inspect checkpoint WAL generation '{}': {error}",
            source_path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::internal(format!(
            "checkpoint WAL generation '{}' is not a regular file",
            source_path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    let mut source = options.open(&source_path).map_err(|error| {
        Error::internal(format!(
            "failed to open checkpoint WAL generation '{}': {error}",
            source_path.display()
        ))
    })?;
    let relative = relative
        .to_str()
        .ok_or_else(|| Error::internal("canonical WAL path is not UTF-8"))?;
    staging
        .write_generation_file(StagedMemberRole::WalSuccessor, relative, |target| {
            std::io::copy(&mut source, target).map_err(|error| {
                crate::v6::FormatError::StagingIo {
                    operation: "copy checkpoint WAL generation",
                    kind: error.kind(),
                }
            })?;
            Ok(())
        })
        .map_err(format_error)
}

fn table_reference(manifest: &TableManifest, bytes: &[u8]) -> Result<TableManifestRef> {
    let (length, sha) = (bytes.len() as u64, footer_sha(bytes)?);
    TableManifestRef::new(
        manifest.table_id(),
        ManifestRef::new(
            manifest.manifest_id(),
            ManifestKind::Table,
            manifest.generation(),
            length,
            sha,
        )
        .map_err(format_error)?,
    )
    .map_err(format_error)
}

fn write_staged(
    staging: &StagedArtifactSet,
    role: StagedMemberRole,
    relative: PathBuf,
    bytes: &[u8],
) -> Result<()> {
    let relative = relative
        .to_str()
        .ok_or_else(|| Error::internal("canonical generation path is not UTF-8"))?;
    staging
        .write_generation_file(role, relative, |file| {
            use std::io::Write;
            file.write_all(bytes)
                .map_err(|error| crate::v6::FormatError::StagingIo {
                    operation: "write generation member",
                    kind: error.kind(),
                })?;
            Ok(())
        })
        .map_err(format_error)
}

fn footer_sha(bytes: &[u8]) -> Result<[u8; 32]> {
    bytes
        .get(bytes.len().saturating_sub(32)..)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| Error::internal("encoded generation member has no checksum footer"))
}

const fn inactive_slot(slot: ControlSlotIndex) -> ControlSlotIndex {
    match slot {
        ControlSlotIndex::Zero => ControlSlotIndex::One,
        ControlSlotIndex::One => ControlSlotIndex::Zero,
    }
}

fn unix_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(not(target_os = "wasi"))]
fn process_id() -> u64 {
    u64::from(std::process::id())
}

#[cfg(target_os = "wasi")]
fn process_id() -> u64 {
    0
}

fn format_error(error: crate::v6::FormatError) -> Error {
    Error::internal(format!("physical generation publication failed: {error}"))
}

fn data_segment_publication_error(error: crate::v6::FormatError) -> DataSegmentPublicationError {
    let stale_source = matches!(
        error,
        crate::v6::FormatError::InvalidLease {
            detail: "artifact build source differs from publication source"
        }
    );
    DataSegmentPublicationError {
        error: Box::new(format_error(error)),
        stale_source,
    }
}

fn compaction_publication_error(error: crate::v6::FormatError) -> Error {
    if matches!(
        error,
        crate::v6::FormatError::InvalidMaintenance {
            detail: "maintenance boundary is stale"
        }
    ) {
        return super::compaction_abort(
            super::COMPACTION_STALE_PUBLICATION_REASON,
            "CONTROL advanced after compaction planning",
        );
    }
    format_error(error)
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}
