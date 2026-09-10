use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(feature = "test-failpoints")]
use std::process::Command;
use std::sync::{Arc, Barrier};

#[cfg(all(target_os = "linux", feature = "test-failpoints"))]
#[path = "support/publication_retry.rs"]
mod retry;

#[cfg(feature = "test-failpoints")]
use radixdb_catalog::{decode_catalog_pack, ObjectKind};
use radixdb_catalog::{
    encode_catalog_pack, CatalogDataType, CatalogEdge, CatalogGraph, CatalogName, CatalogObject,
    CatalogPackMeta, CatalogPayload, ColumnPayload, EdgeKind, NamespacePayload, ObjectId,
    TablePayload,
};
use radixdb_core::{DataType, Value};
#[cfg(feature = "test-failpoints")]
use radixdb_storage::test_failpoints::{InterleaveGuard, InterleavePoint};
use radixdb_storage::v6::{
    build_artifact_pair, decode_control_slot, encode_control_slot, encode_database_manifest,
    encode_table_manifest, AcceleratorBuildSpec, ArtifactBuildKind, ArtifactBuildLease, ArtifactId,
    ArtifactPairBuildRequest, ArtifactRef, CatalogGeneration, CatalogId, CatalogRef,
    CatalogRootRef, ColumnBuildPolicy, CompleteStagingSet, ControlRecord, ControlSlotIndex,
    DataArtifactHeader, DataColumnSpec, DataPhysicalCodec, DatabaseGeneration, DatabaseId,
    DatabaseManifest, DatabaseManifestRootRef, ExactPageBuildLimits, FanoutBuildLimits,
    FormatError, FormatResult, FrozenCheckpoint, FrozenMaintenance, IndexKeyColumn,
    IndexNullsOrder, IndexPageCodec, IndexSortDirection, LeaseLimits, MaintenanceKind,
    ManifestGeneration, ManifestId, ManifestKind, ManifestRef, PhysicalGenerationPublisher,
    PhysicalGenerationSnapshot, SegmentDescriptor, SegmentId, SegmentKind, SourceRow,
    StagedArtifactSet, TableManifest, TableManifestRef, WalGeneration, WalReplayFloor,
    WriterInstanceId,
};
#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{
    encode_catalog_artifact, ArtifactCleanupLimits, ArtifactGarbageCollector,
    DataWalRecoveryContext, DataWalRecoveryOutcome, DatabaseRecovery, GenerationCrashPoint,
    GenerationFaultGuard, GenerationFaultMode, PhysicalGenerationExpectation, ReachabilityLimits,
    RecoveryLimits, StagedMemberRole, WalRecovery,
};
#[cfg(feature = "test-hooks")]
use radixdb_storage::v6::{publication_diagnostics, reset_publication_diagnostics};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn footer_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - 32..].try_into().unwrap()
}

fn table_manifest_path(reference: TableManifestRef) -> PathBuf {
    PathBuf::from("manifests")
        .join("tables")
        .join(reference.table_id().to_string())
        .join(format!(
            "table-{:016x}.mft",
            reference.manifest().generation().get()
        ))
}

fn database_manifest_path(reference: DatabaseManifestRootRef) -> PathBuf {
    PathBuf::from("manifests").join(format!(
        "database-{:016x}.mft",
        reference.generation().get()
    ))
}

fn catalog_path(reference: CatalogRef) -> PathBuf {
    PathBuf::from("catalog").join(format!("catalog-{:016x}.cat", reference.generation().get()))
}

fn wal_path(floor: WalReplayFloor) -> PathBuf {
    PathBuf::from("wal").join(format!("wal-{:016x}.log", floor.generation().get()))
}

fn catalog_object(
    id: ObjectId,
    namespace_id: Option<ObjectId>,
    parent_id: Option<ObjectId>,
    name: &str,
    payload: CatalogPayload,
) -> CatalogObject {
    CatalogObject::new(
        id,
        namespace_id,
        parent_id,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        1,
        payload,
    )
    .unwrap()
}

fn catalog_bytes(
    database_id: DatabaseId,
    catalog_id: CatalogId,
    generation: CatalogGeneration,
    table_id: ObjectId,
    column_id: ObjectId,
) -> Vec<u8> {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let graph = CatalogGraph::build(
        vec![
            catalog_object(
                namespace,
                None,
                None,
                "public",
                CatalogPayload::Namespace(NamespacePayload::new()),
            ),
            catalog_object(
                table_id,
                Some(namespace),
                Some(namespace),
                "messages",
                CatalogPayload::Table(
                    TablePayload::new(vec![column_id], vec![], vec![], None).unwrap(),
                ),
            ),
            catalog_object(
                column_id,
                Some(namespace),
                Some(table_id),
                "value",
                CatalogPayload::Column(
                    ColumnPayload::new(
                        0,
                        CatalogDataType::scalar(DataType::Integer).unwrap(),
                        false,
                        None,
                        None,
                    )
                    .unwrap(),
                ),
            ),
        ],
        vec![
            CatalogEdge::new(namespace, table_id, EdgeKind::Contains, 0),
            CatalogEdge::new(table_id, column_id, EdgeKind::Contains, 0),
        ],
    )
    .unwrap();
    encode_catalog_pack(
        CatalogPackMeta::new(
            database_id.into_bytes(),
            catalog_id.into_bytes(),
            generation.get(),
            900,
            123_000,
        )
        .unwrap(),
        &graph,
    )
    .unwrap()
}

fn rows() -> impl Iterator<Item = FormatResult<SourceRow>> {
    [(101, 5), (103, 1), (107, 3), (109, 2), (113, 4), (127, 6)]
        .into_iter()
        .map(|(row_id, value)| Ok(SourceRow::new(row_id, vec![Value::integer(value)])))
}

struct GenerationFiles {
    control: ControlRecord,
    snapshot: PhysicalGenerationSnapshot,
    catalog_reference: CatalogRef,
    catalog_bytes: Vec<u8>,
    table_reference: TableManifestRef,
    table_bytes: Vec<u8>,
    database_reference: DatabaseManifestRootRef,
    database_bytes: Vec<u8>,
    data_reference: ArtifactRef,
    data_bytes: Vec<u8>,
    index_reference: ArtifactRef,
    index_bytes: Vec<u8>,
    wal_floor: WalReplayFloor,
}

#[allow(clippy::too_many_arguments)]
fn generation(
    build_root: &Path,
    database_id: DatabaseId,
    table_id: ObjectId,
    column_id: ObjectId,
    database_generation: u64,
    catalog_generation: u64,
    marker: u8,
    slot: ControlSlotIndex,
    writer: WriterInstanceId,
) -> GenerationFiles {
    generation_with_group_rows(
        build_root,
        database_id,
        table_id,
        column_id,
        database_generation,
        catalog_generation,
        marker,
        slot,
        writer,
        2,
    )
}

#[allow(clippy::too_many_arguments)]
fn generation_with_group_rows(
    build_root: &Path,
    database_id: DatabaseId,
    table_id: ObjectId,
    column_id: ObjectId,
    database_generation: u64,
    catalog_generation: u64,
    marker: u8,
    slot: ControlSlotIndex,
    writer: WriterInstanceId,
    row_group_rows: u32,
) -> GenerationFiles {
    let database_generation = DatabaseGeneration::new(database_generation).unwrap();
    let catalog_generation = CatalogGeneration::new(catalog_generation).unwrap();
    let segment_id = SegmentId::from_bytes(raw(marker + 1)).unwrap();
    let columns = vec![DataColumnSpec::new(
        column_id,
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    )];
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(0x41),
        false,
        false,
        [0x51; 32],
        vec![IndexKeyColumn::new(
            column_id,
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::new(2, 1024 * 1024).unwrap(),
    )
    .unwrap();
    let request = ArtifactPairBuildRequest::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes(raw(marker + 2)).unwrap(),
            database_id,
            table_id,
            segment_id,
            database_generation,
            catalog_generation,
            501,
            509,
            6,
            1,
            6_u64.div_ceil(u64::from(row_group_rows)) as u32,
            SegmentKind::Rows,
            123_456,
        )
        .unwrap(),
        columns,
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes(raw(marker + 3)).unwrap(),
        vec![accelerator],
        FanoutBuildLimits::new(row_group_rows, 2, 1024 * 1024, 128).unwrap(),
        build_root,
    )
    .unwrap();
    let pair = build_artifact_pair(&request, rows()).unwrap();

    let catalog_id = CatalogId::from_bytes(raw(marker + 4)).unwrap();
    let catalog_bytes = catalog_bytes(
        database_id,
        catalog_id,
        catalog_generation,
        table_id,
        column_id,
    );
    let catalog_reference = CatalogRef::new(
        catalog_id,
        catalog_generation,
        catalog_bytes.len() as u64,
        footer_sha(&catalog_bytes),
    )
    .unwrap();
    let table_manifest = TableManifest::new(
        database_id,
        table_id,
        ManifestId::from_bytes(raw(marker + 5)).unwrap(),
        ManifestGeneration::new(database_generation.get()).unwrap(),
        catalog_generation,
        127,
        2,
        vec![SegmentDescriptor::new(
            segment_id,
            SegmentKind::Rows,
            501,
            509,
            6,
            101,
            127,
            pair.data_reference(),
            Some(pair.index_reference()),
        )
        .unwrap()],
        123_456,
    )
    .unwrap();
    let table_bytes = encode_table_manifest(&table_manifest).unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest.manifest_id(),
            ManifestKind::Table,
            table_manifest.generation(),
            table_bytes.len() as u64,
            footer_sha(&table_bytes),
        )
        .unwrap(),
    )
    .unwrap();
    let wal_floor =
        WalReplayFloor::new(WalGeneration::new(database_generation.get()).unwrap(), 900);
    let database_manifest = DatabaseManifest::new(
        database_id,
        ManifestId::from_bytes(raw(marker + 6)).unwrap(),
        database_generation,
        catalog_reference,
        wal_floor,
        1_000_000,
        vec![table_reference],
        123_456,
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&database_manifest).unwrap();
    let database_reference = DatabaseManifestRootRef::new(
        database_manifest.manifest_id(),
        ManifestGeneration::new(database_generation.get()).unwrap(),
        footer_sha(&database_bytes),
    );
    let control = ControlRecord::new(
        slot,
        database_generation,
        database_id,
        database_reference,
        CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog_bytes)),
        wal_floor,
        123_456,
        writer,
    )
    .unwrap();
    let snapshot =
        PhysicalGenerationSnapshot::new(control, database_manifest, vec![table_manifest]).unwrap();
    GenerationFiles {
        control,
        snapshot,
        catalog_reference,
        catalog_bytes,
        table_reference,
        table_bytes,
        database_reference,
        database_bytes,
        data_reference: pair.data_reference(),
        data_bytes: pair.data_bytes().to_vec(),
        index_reference: pair.index_reference(),
        index_bytes: pair.index_bytes().to_vec(),
        wal_floor,
    }
}

fn write_file(root: &Path, relative: &Path, bytes: &[u8]) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn install(root: &Path, files: &GenerationFiles) {
    write_file(
        root,
        &files.data_reference.relative_path(),
        &files.data_bytes,
    );
    write_file(
        root,
        &files.index_reference.relative_path(),
        &files.index_bytes,
    );
    write_file(
        root,
        &catalog_path(files.catalog_reference),
        &files.catalog_bytes,
    );
    write_file(
        root,
        &table_manifest_path(files.table_reference),
        &files.table_bytes,
    );
    write_file(
        root,
        &database_manifest_path(files.database_reference),
        &files.database_bytes,
    );
    write_file(root, &wal_path(files.wal_floor), b"");
    write_file(
        root,
        Path::new(match files.control.slot() {
            ControlSlotIndex::Zero => "CONTROL.0",
            ControlSlotIndex::One => "CONTROL.1",
        }),
        &encode_control_slot(files.control),
    );
}

fn stage(root: &Path, files: &GenerationFiles, writer: WriterInstanceId) -> CompleteStagingSet {
    stage_with_wal(root, files, writer, b"")
}

fn stage_with_wal(
    root: &Path,
    files: &GenerationFiles,
    writer: WriterInstanceId,
    wal_bytes: &[u8],
) -> CompleteStagingSet {
    stage_with_index_and_wal(root, files, writer, &files.index_bytes, wal_bytes)
}

fn stage_with_index_and_wal(
    root: &Path,
    files: &GenerationFiles,
    writer: WriterInstanceId,
    index_bytes: &[u8],
    wal_bytes: &[u8],
) -> CompleteStagingSet {
    let staging = StagedArtifactSet::create(
        root.join("staging"),
        writer,
        files.control.database_generation(),
        55,
        1_000,
    )
    .unwrap();
    for (path, bytes) in [
        (
            files.data_reference.relative_path(),
            files.data_bytes.as_slice(),
        ),
        (files.index_reference.relative_path(), index_bytes),
        (
            catalog_path(files.catalog_reference),
            files.catalog_bytes.as_slice(),
        ),
        (
            table_manifest_path(files.table_reference),
            files.table_bytes.as_slice(),
        ),
        (
            database_manifest_path(files.database_reference),
            files.database_bytes.as_slice(),
        ),
        (wal_path(files.wal_floor), wal_bytes),
    ] {
        staging
            .write_file(path.to_str().unwrap(), |file| {
                file.write_all(bytes).unwrap();
                Ok(())
            })
            .unwrap();
    }
    staging.mark_complete(2_000, Default::default()).unwrap()
}

fn stage_maintenance(
    root: &Path,
    files: &GenerationFiles,
    writer: WriterInstanceId,
) -> CompleteStagingSet {
    let staging = StagedArtifactSet::create(
        root.join("staging"),
        writer,
        files.control.database_generation(),
        55,
        1_000,
    )
    .unwrap();
    for (path, bytes) in [
        (
            files.data_reference.relative_path(),
            files.data_bytes.as_slice(),
        ),
        (
            files.index_reference.relative_path(),
            files.index_bytes.as_slice(),
        ),
        (
            table_manifest_path(files.table_reference),
            files.table_bytes.as_slice(),
        ),
        (
            database_manifest_path(files.database_reference),
            files.database_bytes.as_slice(),
        ),
    ] {
        staging
            .write_file(path.to_str().unwrap(), |file| {
                file.write_all(bytes).unwrap();
                Ok(())
            })
            .unwrap();
    }
    staging.mark_complete(2_000, Default::default()).unwrap()
}

fn begin_seal(
    publisher: &PhysicalGenerationPublisher,
    staging: &CompleteStagingSet,
) -> ArtifactBuildLease {
    let source = publisher.pin().unwrap();
    publisher
        .begin_artifact_build(ArtifactBuildKind::Seal, &source, staging.owner())
        .unwrap()
}

struct Fixture {
    root: tempfile::TempDir,
    target: GenerationFiles,
    publisher: PhysicalGenerationPublisher,
    writer: WriterInstanceId,
}

struct MaintenanceFixture {
    root: tempfile::TempDir,
    source_control: ControlRecord,
    source_data: ArtifactRef,
    source_index: ArtifactRef,
    target: GenerationFiles,
    publisher: PhysicalGenerationPublisher,
    writer: WriterInstanceId,
}

fn maintenance_fixture() -> MaintenanceFixture {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("staging")).unwrap();
    let builds = tempfile::tempdir().unwrap();
    let database_id = DatabaseId::from_bytes(raw(1)).unwrap();
    let table_id = object_id(2);
    let column_id = object_id(3);
    let writer = WriterInstanceId::from_bytes(raw(4)).unwrap();
    let source = generation(
        builds.path(),
        database_id,
        table_id,
        column_id,
        7,
        3,
        20,
        ControlSlotIndex::Zero,
        writer,
    );
    install(root.path(), &source);
    let candidate = generation(
        builds.path(),
        database_id,
        table_id,
        column_id,
        8,
        3,
        40,
        ControlSlotIndex::One,
        writer,
    );
    let target_manifest = DatabaseManifest::new(
        database_id,
        candidate.database_reference.id(),
        candidate.control.database_generation(),
        source.catalog_reference,
        source.wal_floor,
        source.snapshot.database_manifest().transaction_high_water(),
        vec![candidate.table_reference],
        223_456,
    )
    .unwrap();
    let target_database_bytes = encode_database_manifest(&target_manifest).unwrap();
    let target_database_reference = DatabaseManifestRootRef::new(
        target_manifest.manifest_id(),
        ManifestGeneration::new(target_manifest.generation().get()).unwrap(),
        footer_sha(&target_database_bytes),
    );
    let target_control = ControlRecord::new(
        ControlSlotIndex::One,
        target_manifest.generation(),
        database_id,
        target_database_reference,
        source.control.catalog(),
        source.wal_floor,
        223_456,
        writer,
    )
    .unwrap();
    let target_table = candidate.snapshot.table_manifests()[0].clone();
    let target_snapshot =
        PhysicalGenerationSnapshot::new(target_control, target_manifest, vec![target_table])
            .unwrap();
    let target = GenerationFiles {
        control: target_control,
        snapshot: target_snapshot,
        catalog_reference: source.catalog_reference,
        catalog_bytes: source.catalog_bytes.clone(),
        table_reference: candidate.table_reference,
        table_bytes: candidate.table_bytes,
        database_reference: target_database_reference,
        database_bytes: target_database_bytes,
        data_reference: candidate.data_reference,
        data_bytes: candidate.data_bytes,
        index_reference: candidate.index_reference,
        index_bytes: candidate.index_bytes,
        wal_floor: source.wal_floor,
    };
    let publisher =
        PhysicalGenerationPublisher::open(root.path(), source.snapshot.clone()).unwrap();
    MaintenanceFixture {
        root,
        source_control: source.control,
        source_data: source.data_reference,
        source_index: source.index_reference,
        target,
        publisher,
        writer,
    }
}

fn fixture() -> Fixture {
    fixture_with_group_rows(2)
}

fn fixture_with_group_rows(row_group_rows: u32) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    fixture_from_root(root, row_group_rows)
}

#[cfg(all(target_os = "linux", feature = "test-failpoints"))]
fn fixture_with_group_rows_in(parent: &Path, row_group_rows: u32) -> Fixture {
    let root = tempfile::Builder::new()
        .prefix("radixdb-publication-")
        .tempdir_in(parent)
        .unwrap();
    fixture_from_root(root, row_group_rows)
}

fn fixture_from_root(root: tempfile::TempDir, row_group_rows: u32) -> Fixture {
    std::fs::create_dir(root.path().join("staging")).unwrap();
    let builds = tempfile::tempdir().unwrap();
    let database_id = DatabaseId::from_bytes(raw(1)).unwrap();
    let table_id = object_id(2);
    let column_id = object_id(3);
    let writer = WriterInstanceId::from_bytes(raw(4)).unwrap();
    let current = generation_with_group_rows(
        builds.path(),
        database_id,
        table_id,
        column_id,
        7,
        3,
        20,
        ControlSlotIndex::Zero,
        writer,
        row_group_rows,
    );
    install(root.path(), &current);
    let target = generation_with_group_rows(
        builds.path(),
        database_id,
        table_id,
        column_id,
        8,
        4,
        40,
        ControlSlotIndex::One,
        writer,
        row_group_rows,
    );
    let publisher =
        PhysicalGenerationPublisher::open(root.path(), current.snapshot.clone()).unwrap();
    Fixture {
        root,
        target,
        publisher,
        writer,
    }
}

fn stage_unchanged_artifacts(fixture: &Fixture) -> (CompleteStagingSet, ControlRecord) {
    stage_unchanged_generation(fixture, false)
}

fn stage_unchanged_checkpoint(fixture: &Fixture) -> (CompleteStagingSet, ControlRecord) {
    stage_unchanged_generation(fixture, true)
}

fn stage_unchanged_generation(
    fixture: &Fixture,
    advance_wal: bool,
) -> (CompleteStagingSet, ControlRecord) {
    let pinned = fixture.publisher.pin().unwrap();
    let current = pinned.snapshot();
    let next = current
        .control()
        .database_generation()
        .checked_next()
        .unwrap();
    let wal_floor = if advance_wal {
        WalReplayFloor::new(
            WalGeneration::new(current.control().wal_replay_floor().generation().get() + 1)
                .unwrap(),
            current.control().wal_replay_floor().lsn(),
        )
    } else {
        current.control().wal_replay_floor()
    };
    let manifest = DatabaseManifest::new(
        current.control().database_id(),
        ManifestId::from_bytes(raw(0x71)).unwrap(),
        next,
        current.database_manifest().catalog(),
        wal_floor,
        current.database_manifest().transaction_high_water(),
        current.database_manifest().tables().to_vec(),
        223_456,
    )
    .unwrap();
    let bytes = encode_database_manifest(&manifest).unwrap();
    let reference = DatabaseManifestRootRef::new(
        manifest.manifest_id(),
        ManifestGeneration::new(next.get()).unwrap(),
        footer_sha(&bytes),
    );
    let target = ControlRecord::new(
        ControlSlotIndex::One,
        next,
        current.control().database_id(),
        reference,
        current.control().catalog(),
        wal_floor,
        223_456,
        fixture.writer,
    )
    .unwrap();
    let staged = StagedArtifactSet::create(
        fixture.root.path().join("staging"),
        fixture.writer,
        next,
        55,
        1_000,
    )
    .unwrap();
    staged
        .write_file(
            database_manifest_path(reference).to_str().unwrap(),
            |file| {
                file.write_all(&bytes).unwrap();
                Ok(())
            },
        )
        .unwrap();
    if advance_wal {
        staged
            .write_file(wal_path(wal_floor).to_str().unwrap(), |_file| Ok(()))
            .unwrap();
    }
    (
        staged.mark_complete(2_000, Default::default()).unwrap(),
        target,
    )
}

fn stage_changed_data_with_retained_index(
    fixture: &Fixture,
) -> (CompleteStagingSet, ControlRecord) {
    let pinned = fixture.publisher.pin().unwrap();
    let current = pinned.snapshot();
    let current_segment = current.table_manifests()[0].segments()[0];
    let next = current
        .control()
        .database_generation()
        .checked_next()
        .unwrap();
    let build_root = tempfile::tempdir().unwrap();
    let candidate = generation(
        build_root.path(),
        current.control().database_id(),
        current.table_manifests()[0].table_id(),
        object_id(3),
        next.get(),
        current.database_manifest().catalog().generation().get(),
        0x80,
        ControlSlotIndex::One,
        fixture.writer,
    );
    let candidate_table = &candidate.snapshot.table_manifests()[0];
    let candidate_segment = candidate_table.segments()[0];
    let table = TableManifest::new(
        current.control().database_id(),
        candidate_table.table_id(),
        candidate_table.manifest_id(),
        candidate_table.generation(),
        current.database_manifest().catalog().generation(),
        candidate_table.row_id_high_water(),
        candidate_table.next_segment_sequence(),
        vec![SegmentDescriptor::new_at_tier(
            candidate_segment.id(),
            candidate_segment.kind(),
            candidate_segment.tier(),
            candidate_segment.min_transaction_id(),
            candidate_segment.max_transaction_id(),
            candidate_segment.row_count(),
            candidate_segment.first_row_id(),
            candidate_segment.last_row_id(),
            candidate_segment.data_artifact(),
            current_segment.index_artifact(),
        )
        .unwrap()],
        223_456,
    )
    .unwrap();
    let table_bytes = encode_table_manifest(&table).unwrap();
    let table_reference = TableManifestRef::new(
        table.table_id(),
        ManifestRef::new(
            table.manifest_id(),
            ManifestKind::Table,
            table.generation(),
            table_bytes.len() as u64,
            footer_sha(&table_bytes),
        )
        .unwrap(),
    )
    .unwrap();
    let manifest = DatabaseManifest::new(
        current.control().database_id(),
        candidate.database_reference.id(),
        next,
        current.database_manifest().catalog(),
        current.control().wal_replay_floor(),
        current.database_manifest().transaction_high_water(),
        vec![table_reference],
        223_456,
    )
    .unwrap();
    let manifest_bytes = encode_database_manifest(&manifest).unwrap();
    let manifest_reference = DatabaseManifestRootRef::new(
        manifest.manifest_id(),
        ManifestGeneration::new(next.get()).unwrap(),
        footer_sha(&manifest_bytes),
    );
    let target = ControlRecord::new(
        ControlSlotIndex::One,
        next,
        current.control().database_id(),
        manifest_reference,
        current.control().catalog(),
        current.control().wal_replay_floor(),
        223_456,
        fixture.writer,
    )
    .unwrap();
    drop(pinned);

    let staging = StagedArtifactSet::create(
        fixture.root.path().join("staging"),
        fixture.writer,
        next,
        55,
        1_000,
    )
    .unwrap();
    for (path, bytes) in [
        (
            candidate.data_reference.relative_path(),
            candidate.data_bytes.as_slice(),
        ),
        (table_manifest_path(table_reference), table_bytes.as_slice()),
        (
            database_manifest_path(manifest_reference),
            manifest_bytes.as_slice(),
        ),
    ] {
        staging
            .write_file(path.to_str().unwrap(), |file| {
                file.write_all(bytes).unwrap();
                Ok(())
            })
            .unwrap();
    }
    (
        staging.mark_complete(2_000, Default::default()).unwrap(),
        target,
    )
}

#[test]
fn complete_generation_moves_every_member_then_publishes_control_and_runtime() {
    let fixture = fixture();
    let old_lease = fixture.publisher.pin().unwrap();
    let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let staged_path = staged.path().to_path_buf();
    let build = begin_seal(&fixture.publisher, &staged);
    #[cfg(feature = "test-hooks")]
    reset_publication_diagnostics();

    let new_lease = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap();

    assert_eq!(old_lease.database_generation().get(), 7);
    assert_eq!(new_lease.database_generation().get(), 8);
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        8
    );
    for path in [
        fixture.target.data_reference.relative_path(),
        fixture.target.index_reference.relative_path(),
        catalog_path(fixture.target.catalog_reference),
        table_manifest_path(fixture.target.table_reference),
        database_manifest_path(fixture.target.database_reference),
        wal_path(fixture.target.wal_floor),
    ] {
        assert!(fixture.root.path().join(path).is_file());
    }
    let control = std::fs::read(fixture.root.path().join("CONTROL.1")).unwrap();
    assert_eq!(
        decode_control_slot(&control, ControlSlotIndex::One).unwrap(),
        fixture.target.control
    );
    assert!(
        !staged_path.exists(),
        "durably published staging set must be retired"
    );
    #[cfg(feature = "test-hooks")]
    {
        let counters = publication_diagnostics();
        assert_eq!(counters.generation_fence_holds, 1);
        assert_eq!(counters.retained_artifact_identity_read_calls, 0);
        assert_eq!(counters.retained_artifact_identity_read_bytes, 0);
        assert_eq!(
            counters.new_artifact_identity_read_bytes,
            fixture.target.data_reference.byte_length()
                + fixture.target.index_reference.byte_length()
                - 96
        );
        assert!(counters.new_artifact_identity_read_calls >= 2);
        reset_publication_diagnostics();
    }
}

#[test]
fn missing_optional_retained_index_does_not_block_publication() {
    let fixture = fixture();
    let pinned = fixture.publisher.pin().unwrap();
    let index = pinned.snapshot().table_manifests()[0].segments()[0]
        .index_artifact()
        .unwrap();
    std::fs::remove_file(fixture.root.path().join(index.relative_path())).unwrap();
    drop(pinned);

    let (staged, target) = stage_unchanged_checkpoint(&fixture);
    let expected = fixture.publisher.pin().unwrap().snapshot().control();
    let build = begin_seal(&fixture.publisher, &staged);
    let checkpoint = FrozenCheckpoint::new(expected, target, staged, build, vec![]).unwrap();
    #[cfg(feature = "test-hooks")]
    reset_publication_diagnostics();
    let outcome = fixture
        .publisher
        .publish_checkpoint(checkpoint)
        .unwrap_or_else(|error| {
            panic!("missing retained optional index blocked publication: {error}")
        });
    let published = outcome.lease();
    assert_eq!(published.database_generation().get(), 8);
    #[cfg(feature = "test-hooks")]
    {
        let counters = publication_diagnostics();
        assert_eq!(counters.generation_fence_holds, 1);
        assert_eq!(counters.new_artifact_identity_read_calls, 0);
        assert_eq!(counters.new_artifact_identity_read_bytes, 0);
        assert_eq!(counters.retained_artifact_identity_read_calls, 0);
        assert_eq!(counters.retained_artifact_identity_read_bytes, 0);
        reset_publication_diagnostics();
    }

    #[cfg(feature = "test-failpoints")]
    {
        drop(outcome);
        let recovered = DatabaseRecovery::new(fixture.root.path(), RecoveryLimits::default())
            .recover(&mut ProcessRecoveryWal)
            .unwrap();
        assert_eq!(recovered.control().database_generation().get(), 8);
        assert_eq!(recovered.unavailable_indexes().len(), 1);
        assert_eq!(recovered.unavailable_indexes()[0].reference(), index);
        assert!(matches!(
            recovered.unavailable_indexes()[0].reason(),
            radixdb_storage::v6::UnavailableIndexReason::Missing
        ));
        #[cfg(feature = "test-hooks")]
        {
            let counters = publication_diagnostics();
            assert_eq!(counters.data_identity_read_calls, 0);
            assert_eq!(counters.data_identity_read_bytes, 0);
            assert_eq!(counters.index_identity_read_calls, 0);
            assert_eq!(counters.index_identity_read_bytes, 0);
            assert_eq!(counters.generation_fence_holds, 0);
            reset_publication_diagnostics();
        }
    }
}

#[test]
fn corrupt_optional_retained_index_does_not_block_publication() {
    let fixture = fixture();
    let pinned = fixture.publisher.pin().unwrap();
    let index = pinned.snapshot().table_manifests()[0].segments()[0]
        .index_artifact()
        .unwrap();
    let path = fixture.root.path().join(index.relative_path());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0x80;
    std::fs::write(path, bytes).unwrap();
    drop(pinned);

    let (staged, target) = stage_unchanged_artifacts(&fixture);
    let build = begin_seal(&fixture.publisher, &staged);
    let published = fixture
        .publisher
        .publish_staged_generation(build, staged, target)
        .unwrap_or_else(|error| {
            panic!("corrupt retained optional index blocked publication: {error}")
        });
    assert_eq!(published.database_generation().get(), 8);
}

#[test]
fn corrupt_new_index_remains_a_hard_publication_failure() {
    let fixture = fixture();
    let mut corrupt = fixture.target.index_bytes.clone();
    corrupt[0] ^= 0x80;
    let staged = stage_with_index_and_wal(
        fixture.root.path(),
        &fixture.target,
        fixture.writer,
        &corrupt,
        b"",
    );
    let build = begin_seal(&fixture.publisher, &staged);
    let error =
        match fixture
            .publisher
            .publish_staged_generation(build, staged, fixture.target.control)
        {
            Ok(_) => panic!("corrupt new index was published"),
            Err(error) => error,
        };
    assert!(matches!(
        error,
        FormatError::InvalidPublication {
            detail: "target generation contains a new unavailable index"
        }
    ));
}

#[test]
fn unavailable_index_cannot_be_retained_across_a_data_pair_change() {
    let fixture = fixture();
    let pinned = fixture.publisher.pin().unwrap();
    let index = pinned.snapshot().table_manifests()[0].segments()[0]
        .index_artifact()
        .unwrap();
    std::fs::remove_file(fixture.root.path().join(index.relative_path())).unwrap();
    drop(pinned);

    let (staged, target) = stage_changed_data_with_retained_index(&fixture);
    let build = begin_seal(&fixture.publisher, &staged);
    let error = match fixture
        .publisher
        .publish_staged_generation(build, staged, target)
    {
        Ok(_) => panic!("unavailable index was retained across a DATA pair change"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        FormatError::InvalidPublication {
            detail: "target generation changes an unavailable inherited index pair"
        }
    ));
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        7
    );
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[test]
fn publisher_rejects_another_database_root_before_mutation() {
    let fixture = fixture();
    let owned_control = std::fs::read(fixture.root.path().join("CONTROL.0")).unwrap();
    let other_root = tempfile::tempdir().unwrap();
    std::fs::create_dir(other_root.path().join("staging")).unwrap();
    let build_root = tempfile::tempdir().unwrap();
    let other = generation(
        build_root.path(),
        DatabaseId::from_bytes(raw(99)).unwrap(),
        object_id(100),
        object_id(101),
        1,
        1,
        110,
        ControlSlotIndex::Zero,
        WriterInstanceId::from_bytes(raw(102)).unwrap(),
    );
    install(other_root.path(), &other);
    let original_control = std::fs::read(other_root.path().join("CONTROL.0")).unwrap();
    let staged = stage(other_root.path(), &fixture.target, fixture.writer);
    let build = begin_seal(&fixture.publisher, &staged);
    let result = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control);
    let accepted = result.is_ok();
    drop(result);

    let original_unchanged =
        std::fs::read(other_root.path().join("CONTROL.0")).unwrap() == original_control;
    let foreign_control_created = other_root.path().join("CONTROL.1").exists();
    let owned_control_unchanged =
        std::fs::read(fixture.root.path().join("CONTROL.0")).unwrap() == owned_control;
    let owned_control_created = fixture.root.path().join("CONTROL.1").exists();
    assert!(
        !accepted
            && original_unchanged
            && !foreign_control_created
            && owned_control_unchanged
            && !owned_control_created,
        "wrong-root outcome: accepted={accepted}, original_unchanged={original_unchanged}, \
         foreign_control_created={foreign_control_created}, \
         owned_control_unchanged={owned_control_unchanged}, \
         owned_control_created={owned_control_created}"
    );
}

#[cfg(unix)]
#[test]
fn publisher_accepts_a_path_alias_to_the_same_locked_root() {
    use std::os::unix::fs::symlink;

    let fixture = fixture();
    let source = fixture.publisher.pin().unwrap().snapshot().clone();
    drop(fixture.publisher);
    let aliases = tempfile::tempdir().unwrap();
    let alias = aliases.path().join("database-alias");
    symlink(fixture.root.path(), &alias).unwrap();
    let publisher = PhysicalGenerationPublisher::open(&alias, source).unwrap();
    let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let build = begin_seal(&publisher, &staged);

    let published = publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap();

    assert_eq!(published.database_generation().get(), 8);
    assert!(fixture.root.path().join("CONTROL.1").is_file());
}

#[test]
fn publisher_rejects_a_moved_and_replaced_owned_directory() {
    let fixture = fixture();
    let original = fixture.root.path().to_path_buf();
    let moved = original.with_extension("moved-for-owner-test");
    std::fs::rename(&original, &moved).unwrap();
    std::fs::create_dir(&original).unwrap();
    std::fs::create_dir(original.join("staging")).unwrap();
    std::fs::write(
        original.join("CONTROL.0"),
        encode_control_slot(fixture.publisher.pin().unwrap().snapshot().control()),
    )
    .unwrap();
    let staged = stage(&original, &fixture.target, fixture.writer);
    let build = begin_seal(&fixture.publisher, &staged);

    let error = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidFilesystemOwner { .. }));
    assert!(!original.join("CONTROL.1").exists());
    drop(fixture.publisher);
    std::fs::remove_dir_all(&original).unwrap();
    std::fs::rename(&moved, &original).unwrap();
}

#[test]
fn publisher_rejects_a_replaced_lock_inode() {
    let fixture = fixture();
    let lock_path = fixture.root.path().join("LOCK");
    std::fs::remove_file(&lock_path).unwrap();
    std::fs::write(&lock_path, b"replacement").unwrap();
    let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let build = begin_seal(&fixture.publisher, &staged);

    let error = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidFilesystemOwner { .. }));
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[cfg(feature = "test-failpoints")]
#[test]
fn recovery_budget_accounts_for_open_artifact_metadata() {
    use radixdb_storage::v6::{open_data_artifact_metadata, open_index_artifact_metadata};

    let fixture = fixture_with_group_rows(1);
    let pinned = fixture.publisher.pin().unwrap();
    let segment = pinned.snapshot().table_manifests()[0].segments()[0];
    let data_bytes = std::fs::read(
        fixture
            .root
            .path()
            .join(segment.data_artifact().relative_path()),
    )
    .unwrap();
    let index_reference = segment.index_artifact().unwrap();
    let index_bytes =
        std::fs::read(fixture.root.path().join(index_reference.relative_path())).unwrap();
    let data = open_data_artifact_metadata(data_bytes.as_slice(), segment.data_artifact()).unwrap();
    let index =
        open_index_artifact_metadata(index_bytes.as_slice(), index_reference, data.layout())
            .unwrap();
    let artifact_metadata_bytes =
        data.metrics().accounted_allocation_bytes() + index.metrics().accounted_allocation_bytes();
    drop(pinned);

    let mut first_success = None;
    for budget in (512..=32_768).step_by(256) {
        let limits = RecoveryLimits::new(
            ReachabilityLimits::new(100, budget).unwrap(),
            Default::default(),
        );
        if DatabaseRecovery::new(fixture.root.path(), limits)
            .recover(&mut ProcessRecoveryWal)
            .is_ok()
        {
            first_success = Some(budget);
            break;
        }
    }
    let admitted = first_success.expect("fixture must fit the bounded search corridor");
    assert!(
        admitted >= artifact_metadata_bytes,
        "recovery admitted {admitted} bytes below artifact metadata {artifact_metadata_bytes}"
    );

    let rejected_limit = admitted - 256;
    let rejected = match DatabaseRecovery::new(
        fixture.root.path(),
        RecoveryLimits::new(
            ReachabilityLimits::new(100, rejected_limit).unwrap(),
            Default::default(),
        ),
    )
    .recover(&mut ProcessRecoveryWal)
    {
        Ok(_) => panic!("recovery accepted the preceding lowered aggregate budget"),
        Err(error) => error,
    };
    assert!(
        matches!(
            &rejected,
            radixdb_storage::v6::FormatError::MetadataOpenLimitExceeded {
                field: "accounted bytes",
                actual,
                limit,
            } if actual > limit && *limit == rejected_limit
        ),
        "lowered aggregate budget lost its diagnostic: {rejected}"
    );
}

#[cfg(feature = "test-failpoints")]
#[test]
fn successful_pin_cannot_return_an_artifact_already_deleted_by_gc() {
    let Fixture {
        root,
        target,
        publisher,
        writer,
    } = fixture();
    let staged_eight = stage(root.path(), &target, writer);
    let build_eight = begin_seal(&publisher, &staged_eight);
    let publisher = Arc::new(publisher);
    let old_reference =
        publisher.pin().unwrap().snapshot().table_manifests()[0].segments()[0].data_artifact();
    let schedule = InterleaveGuard::install_scoped(
        publisher.test_interleave_scope(),
        [InterleavePoint::GenerationPinLoaded],
    );
    let controller = schedule.controller();
    let reader = {
        let publisher = Arc::clone(&publisher);
        std::thread::Builder::new()
            .name("paused-generation-pin".into())
            .spawn(move || publisher.pin())
            .unwrap()
    };
    let arrival = controller
        .wait_for(
            InterleavePoint::GenerationPinLoaded,
            Some(7),
            std::time::Duration::from_secs(10),
        )
        .unwrap();
    controller.disable(InterleavePoint::GenerationPinLoaded);

    let generation_eight = publisher
        .publish_staged_generation(build_eight, staged_eight, target.control)
        .unwrap();
    let fallback = generation_eight.snapshot().clone();

    let build_root = tempfile::tempdir().unwrap();
    let next = generation(
        build_root.path(),
        target.control.database_id(),
        object_id(2),
        object_id(3),
        9,
        5,
        60,
        ControlSlotIndex::Zero,
        writer,
    );
    let staged = stage(root.path(), &next, writer);
    let build = publisher
        .begin_artifact_build(ArtifactBuildKind::Seal, &generation_eight, staged.owner())
        .unwrap();
    drop(
        publisher
            .publish_staged_generation(build, staged, next.control)
            .unwrap(),
    );
    drop(generation_eight);

    let limits = ArtifactCleanupLimits::new(
        1_000,
        1024 * 1024,
        1_000,
        1_000,
        1024 * 1024,
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(5),
    )
    .unwrap();
    let collector = ArtifactGarbageCollector::new(root.path(), limits);
    let cleanup = {
        let publisher = Arc::clone(&publisher);
        std::thread::Builder::new()
            .name("generation-cleanup-behind-pin".into())
            .spawn(move || {
                publisher.collect_unreachable_artifacts(
                    &collector,
                    std::slice::from_ref(&fallback),
                    &[],
                    u64::MAX,
                    ReachabilityLimits::default(),
                )?;
                publisher.collect_unreachable_artifacts(
                    &collector,
                    &[fallback],
                    &[],
                    u64::MAX,
                    ReachabilityLimits::default(),
                )?;
                FormatResult::Ok(())
            })
            .unwrap()
    };

    controller.release(arrival);
    let observed = reader.join().unwrap().unwrap();
    cleanup.join().unwrap().unwrap();
    assert_eq!(observed.database_generation().get(), 7);
    assert!(
        root.path().join(old_reference.relative_path()).is_file(),
        "successful pin refers to an artifact already deleted by GC"
    );
}

#[test]
fn partial_final_moves_are_resumed_from_the_complete_member_identity() {
    let fixture = fixture();
    let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let build = begin_seal(&fixture.publisher, &staged);
    let relative = fixture.target.data_reference.relative_path();
    let final_path = fixture.root.path().join(&relative);
    std::fs::create_dir_all(final_path.parent().unwrap()).unwrap();
    std::fs::rename(staged.path().join(&relative), &final_path).unwrap();

    let lease = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap();

    assert_eq!(lease.database_generation().get(), 8);
    assert_eq!(
        std::fs::read(final_path).unwrap(),
        fixture.target.data_bytes
    );
}

#[test]
fn checkpoint_accepts_an_append_only_successor_wal_that_grew_after_staging() {
    let fixture = fixture();
    let staged_prefix = b"durable successor prefix";
    let staged = stage_with_wal(
        fixture.root.path(),
        &fixture.target,
        fixture.writer,
        staged_prefix,
    );
    let build = begin_seal(&fixture.publisher, &staged);
    let wal = wal_path(fixture.target.wal_floor);
    let mut live_wal = staged_prefix.to_vec();
    live_wal.extend_from_slice(b" and concurrently committed suffix");
    write_file(fixture.root.path(), &wal, &live_wal);

    let lease = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap();

    assert_eq!(lease.database_generation().get(), 8);
    assert_eq!(
        std::fs::read(fixture.root.path().join(wal)).unwrap(),
        live_wal
    );
}

#[test]
fn checkpoint_rejects_a_successor_wal_with_a_different_prefix() {
    let fixture = fixture();
    let staged = stage_with_wal(
        fixture.root.path(),
        &fixture.target,
        fixture.writer,
        b"expected prefix",
    );
    let build = begin_seal(&fixture.publisher, &staged);
    write_file(
        fixture.root.path(),
        &wal_path(fixture.target.wal_floor),
        b"different prefix and suffix",
    );

    let error = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidPublication { .. }));
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        7
    );
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[test]
fn checkpoint_rejects_a_truncated_live_successor_wal() {
    let fixture = fixture();
    let staged = stage_with_wal(
        fixture.root.path(),
        &fixture.target,
        fixture.writer,
        b"durable prefix and staged suffix",
    );
    let build = begin_seal(&fixture.publisher, &staged);
    write_file(
        fixture.root.path(),
        &wal_path(fixture.target.wal_floor),
        b"durable prefix",
    );

    let error = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidPublication { .. }));
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        7
    );
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[test]
fn extra_completed_member_is_rejected_before_control() {
    let fixture = fixture();
    let staging = StagedArtifactSet::create(
        fixture.root.path().join("staging"),
        fixture.writer,
        fixture.target.control.database_generation(),
        55,
        1_000,
    )
    .unwrap();
    for (path, bytes) in [
        (
            fixture.target.data_reference.relative_path(),
            fixture.target.data_bytes.as_slice(),
        ),
        (
            fixture.target.index_reference.relative_path(),
            fixture.target.index_bytes.as_slice(),
        ),
        (
            catalog_path(fixture.target.catalog_reference),
            fixture.target.catalog_bytes.as_slice(),
        ),
        (
            table_manifest_path(fixture.target.table_reference),
            fixture.target.table_bytes.as_slice(),
        ),
        (
            database_manifest_path(fixture.target.database_reference),
            fixture.target.database_bytes.as_slice(),
        ),
        (
            wal_path(fixture.target.wal_floor),
            b"successor WAL".as_slice(),
        ),
        (PathBuf::from("unexpected.bin"), b"noise".as_slice()),
    ] {
        staging
            .write_file(path.to_str().unwrap(), |file| {
                file.write_all(bytes).unwrap();
                Ok(())
            })
            .unwrap();
    }
    let staged = staging.mark_complete(2_000, Default::default()).unwrap();
    let build = begin_seal(&fixture.publisher, &staged);

    let error = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidStagingRecord { .. }));
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        7
    );
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[test]
fn immutable_locator_collision_is_rejected_before_control() {
    let fixture = fixture();
    let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let build = begin_seal(&fixture.publisher, &staged);
    write_file(
        fixture.root.path(),
        &fixture.target.data_reference.relative_path(),
        b"different immutable bytes",
    );

    let error = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidPublication { .. }));
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        7
    );
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[test]
fn invalid_staged_graph_never_reaches_control() {
    let fixture = fixture();
    let staging = StagedArtifactSet::create(
        fixture.root.path().join("staging"),
        fixture.writer,
        fixture.target.control.database_generation(),
        55,
        1_000,
    )
    .unwrap();
    let mut bad_index = fixture.target.index_bytes.clone();
    bad_index[260] ^= 0x80;
    for (path, bytes) in [
        (
            fixture.target.data_reference.relative_path(),
            fixture.target.data_bytes.as_slice(),
        ),
        (
            fixture.target.index_reference.relative_path(),
            bad_index.as_slice(),
        ),
        (
            catalog_path(fixture.target.catalog_reference),
            fixture.target.catalog_bytes.as_slice(),
        ),
        (
            table_manifest_path(fixture.target.table_reference),
            fixture.target.table_bytes.as_slice(),
        ),
        (
            database_manifest_path(fixture.target.database_reference),
            fixture.target.database_bytes.as_slice(),
        ),
        (
            wal_path(fixture.target.wal_floor),
            b"successor WAL".as_slice(),
        ),
    ] {
        staging
            .write_file(path.to_str().unwrap(), |file| {
                file.write_all(bytes).unwrap();
                Ok(())
            })
            .unwrap();
    }
    let staged = staging.mark_complete(2_000, Default::default()).unwrap();
    let build = begin_seal(&fixture.publisher, &staged);

    let error = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidPublication { .. }));
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        7
    );
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[test]
fn compaction_publishes_independent_generation_without_checkpoint_authority() {
    let fixture = maintenance_fixture();
    let old_reader = fixture.publisher.pin().unwrap();
    let staging = stage_maintenance(fixture.root.path(), &fixture.target, fixture.writer);
    let build = fixture
        .publisher
        .begin_artifact_build(ArtifactBuildKind::Compaction, &old_reader, staging.owner())
        .unwrap();
    let maintenance = FrozenMaintenance::new(
        MaintenanceKind::Compaction,
        fixture.source_control,
        fixture.target.control,
        staging,
        build,
    )
    .unwrap();

    let outcome = fixture.publisher.publish_maintenance(maintenance).unwrap();

    assert_eq!(outcome.kind(), MaintenanceKind::Compaction);
    assert_eq!(outcome.lease().database_generation().get(), 8);
    assert_eq!(
        outcome.lease().snapshot().database_manifest().catalog(),
        old_reader.snapshot().database_manifest().catalog()
    );
    assert_eq!(
        outcome
            .lease()
            .snapshot()
            .database_manifest()
            .wal_replay_floor(),
        old_reader.snapshot().database_manifest().wal_replay_floor()
    );
    assert_eq!(
        outcome.retired_artifacts(),
        &[fixture.source_data, fixture.source_index]
    );
    assert!(fixture
        .root
        .path()
        .join(fixture.source_data.relative_path())
        .exists());
    assert!(fixture
        .root
        .path()
        .join(fixture.source_index.relative_path())
        .exists());
    assert!(fixture
        .root
        .path()
        .join(fixture.target.data_reference.relative_path())
        .exists());
    assert_eq!(old_reader.database_generation().get(), 7);
}

#[test]
fn index_rebuild_classification_rejects_compaction_topology() {
    let fixture = maintenance_fixture();
    let source = fixture.publisher.pin().unwrap();
    let staging = stage_maintenance(fixture.root.path(), &fixture.target, fixture.writer);
    let build = fixture
        .publisher
        .begin_artifact_build(ArtifactBuildKind::IndexRebuild, &source, staging.owner())
        .unwrap();
    let maintenance = FrozenMaintenance::new(
        MaintenanceKind::IndexRebuild,
        fixture.source_control,
        fixture.target.control,
        staging,
        build,
    )
    .unwrap();

    let error = fixture
        .publisher
        .publish_maintenance(maintenance)
        .unwrap_err();

    assert_eq!(
        error,
        FormatError::InvalidMaintenance {
            detail: "index rebuild changes segment identity"
        }
    );
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        7
    );
    assert!(!fixture.root.path().join("CONTROL.1").exists());
    assert!(!fixture
        .root
        .path()
        .join(fixture.target.data_reference.relative_path())
        .exists());
}

#[test]
fn maintenance_cannot_advance_the_wal_replay_floor() {
    let fixture = maintenance_fixture();
    let source = fixture.publisher.pin().unwrap();
    let staging = stage_maintenance(fixture.root.path(), &fixture.target, fixture.writer);
    let build = fixture
        .publisher
        .begin_artifact_build(ArtifactBuildKind::Compaction, &source, staging.owner())
        .unwrap();
    let advanced_wal = WalReplayFloor::new(
        fixture.source_control.wal_replay_floor().generation(),
        fixture.source_control.wal_replay_floor().lsn() + 1,
    );
    let invalid_target = ControlRecord::new(
        fixture.target.control.slot(),
        fixture.target.control.database_generation(),
        fixture.target.control.database_id(),
        fixture.target.control.database_manifest(),
        fixture.target.control.catalog(),
        advanced_wal,
        fixture.target.control.published_unix_ns(),
        fixture.target.control.writer_instance_id(),
    )
    .unwrap();

    let error = FrozenMaintenance::new(
        MaintenanceKind::Compaction,
        fixture.source_control,
        invalid_target,
        staging,
        build,
    )
    .unwrap_err();
    assert_eq!(
        error,
        FormatError::InvalidMaintenance {
            detail: "maintenance publication changes WAL replay floor"
        }
    );
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[test]
fn generation_lease_capacity_is_reserved_before_any_publication_side_effect() {
    let fixture = fixture();
    let source_snapshot = fixture.publisher.pin().unwrap().snapshot().clone();
    drop(fixture.publisher);
    let publisher = PhysicalGenerationPublisher::open_with_lease_limits(
        fixture.root.path(),
        source_snapshot,
        LeaseLimits::new(1, 1).unwrap(),
    )
    .unwrap();
    let reader = publisher.pin().unwrap();
    let staging = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let staging_path = staging.path().to_path_buf();
    let build = publisher
        .begin_artifact_build(ArtifactBuildKind::Seal, &reader, staging.owner())
        .unwrap();

    let error = publisher
        .publish_staged_generation(build, staging, fixture.target.control)
        .unwrap_err();

    assert!(matches!(
        error,
        FormatError::LeaseLimitExceeded {
            field: "physical generations",
            actual: 2,
            limit: 1,
        }
    ));
    assert!(!fixture.root.path().join("CONTROL.1").exists());
    assert!(!fixture
        .root
        .path()
        .join(fixture.target.data_reference.relative_path())
        .exists());
    assert!(staging_path
        .join(fixture.target.data_reference.relative_path())
        .exists());
}

#[cfg(feature = "test-failpoints")]
#[test]
fn publication_fault_matrix_never_exposes_a_partial_runtime_generation() {
    let points = [
        GenerationCrashPoint::DataAfterFinalRenameBeforeDirSync,
        GenerationCrashPoint::DataFinalDirDurable,
        GenerationCrashPoint::IndexAfterFinalRenameBeforeDirSync,
        GenerationCrashPoint::IndexFinalDirDurable,
        GenerationCrashPoint::TableManifestAfterRenameBeforeDirSync,
        GenerationCrashPoint::TableManifestDirDurable,
        GenerationCrashPoint::CatalogPackAfterRenameBeforeDirSync,
        GenerationCrashPoint::CatalogPackDirDurable,
        GenerationCrashPoint::DatabaseManifestAfterRenameBeforeDirSync,
        GenerationCrashPoint::DatabaseManifestDirDurable,
        GenerationCrashPoint::ControlAfterPartialWrite,
        GenerationCrashPoint::ControlAfterWriteBeforeFdatasync,
        GenerationCrashPoint::ControlDurable,
        GenerationCrashPoint::ControlAfterRootDirSync,
        GenerationCrashPoint::RuntimeGenerationBeforePublish,
        GenerationCrashPoint::RuntimeGenerationPublished,
    ];

    for point in points {
        let fixture = fixture();
        let old_control = fixture.publisher.pin().unwrap().snapshot().control();
        let staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
        let build = begin_seal(&fixture.publisher, &staged);
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        let result = fixture.publisher.publish_staged_generation(
            build.clone(),
            staged.clone(),
            fixture.target.control,
        );
        assert!(
            result.is_err(),
            "{} must interrupt publication",
            point.name()
        );
        assert_eq!(guard.hit_count(), 1, "{} was not traversed", point.name());
        drop(guard);

        let observed = fixture.publisher.pin().unwrap().snapshot().control();
        if point == GenerationCrashPoint::RuntimeGenerationPublished {
            assert_eq!(observed, fixture.target.control);
        } else {
            assert_eq!(observed, old_control);
            fixture
                .publisher
                .publish_staged_generation(build, staged, fixture.target.control)
                .unwrap_or_else(|error| {
                    panic!("{} did not resume idempotently: {error}", point.name())
                });
            assert_eq!(
                fixture.publisher.pin().unwrap().snapshot().control(),
                fixture.target.control
            );
        }
    }
}

#[test]
fn concurrent_publishers_serialize_and_stale_loser_stops_before_control_write() {
    let fixture = fixture();
    let builds = tempfile::tempdir().unwrap();
    let alternate = generation(
        builds.path(),
        fixture.target.control.database_id(),
        object_id(2),
        object_id(3),
        8,
        4,
        60,
        ControlSlotIndex::One,
        fixture.writer,
    );
    let first_staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let second_staged = stage(fixture.root.path(), &alternate, fixture.writer);
    let publisher = Arc::new(fixture.publisher);
    let first_build = begin_seal(&publisher, &first_staged);
    let second_build = begin_seal(&publisher, &second_staged);
    let root = fixture.root.path().to_path_buf();
    let start = Arc::new(Barrier::new(3));

    let first = {
        let publisher = Arc::clone(&publisher);
        let start = Arc::clone(&start);
        let control = fixture.target.control;
        std::thread::spawn(move || {
            start.wait();
            publisher.publish_staged_generation(first_build, first_staged, control)
        })
    };
    let second = {
        let publisher = Arc::clone(&publisher);
        let start = Arc::clone(&start);
        let control = alternate.control;
        std::thread::spawn(move || {
            start.wait();
            publisher.publish_staged_generation(second_build, second_staged, control)
        })
    };
    start.wait();
    let first = first.join().unwrap();
    let second = second.join().unwrap();

    assert_eq!(
        [first.is_ok(), second.is_ok()]
            .into_iter()
            .filter(|ok| *ok)
            .count(),
        1
    );
    let loser = if first.is_err() { first } else { second };
    assert!(
        matches!(
            loser,
            Err(FormatError::InvalidLease {
                detail: "artifact build source differs from publication source"
            })
        ),
        "unexpected stale-publisher result: {loser:?}"
    );

    let pinned = publisher.pin().unwrap();
    let winner = pinned.snapshot().control();
    assert!(winner == fixture.target.control || winner == alternate.control);
    let (expected_catalog_path, expected_catalog) = if winner == fixture.target.control {
        (
            catalog_path(fixture.target.catalog_reference),
            &fixture.target.catalog_bytes,
        )
    } else {
        (
            catalog_path(alternate.catalog_reference),
            &alternate.catalog_bytes,
        )
    };
    assert_eq!(
        std::fs::read(root.join(expected_catalog_path)).unwrap(),
        *expected_catalog
    );
    assert_eq!(
        decode_control_slot(
            &std::fs::read(root.join("CONTROL.1")).unwrap(),
            ControlSlotIndex::One,
        )
        .unwrap(),
        winner
    );
}

#[test]
fn checkpoint_and_seal_share_one_publication_fence() {
    let fixture = fixture();
    let builds = tempfile::tempdir().unwrap();
    let alternate = generation(
        builds.path(),
        fixture.target.control.database_id(),
        object_id(2),
        object_id(3),
        8,
        4,
        90,
        ControlSlotIndex::One,
        fixture.writer,
    );
    let checkpoint_staged = stage(fixture.root.path(), &fixture.target, fixture.writer);
    let seal_staged = stage(fixture.root.path(), &alternate, fixture.writer);
    let publisher = Arc::new(fixture.publisher);
    let expected = publisher.pin().unwrap().snapshot().control();
    let checkpoint_build = begin_seal(&publisher, &checkpoint_staged);
    let seal_build = begin_seal(&publisher, &seal_staged);
    let checkpoint = FrozenCheckpoint::new(
        expected,
        fixture.target.control,
        checkpoint_staged,
        checkpoint_build,
        vec![],
    )
    .unwrap();
    let root = fixture.root.path().to_path_buf();
    let start = Arc::new(Barrier::new(3));

    let checkpoint_thread = {
        let publisher = Arc::clone(&publisher);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            publisher.publish_checkpoint(checkpoint).map(drop)
        })
    };
    let seal_thread = {
        let publisher = Arc::clone(&publisher);
        let start = Arc::clone(&start);
        let control = alternate.control;
        std::thread::spawn(move || {
            start.wait();
            publisher
                .publish_staged_generation(seal_build, seal_staged, control)
                .map(drop)
        })
    };
    start.wait();
    let results = [
        checkpoint_thread.join().unwrap(),
        seal_thread.join().unwrap(),
    ];

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert!(results
        .iter()
        .filter_map(|result| result.as_ref().err())
        .all(|error| matches!(
            error,
            FormatError::InvalidCheckpoint { .. } | FormatError::InvalidLease { .. }
        )));
    let selected = publisher.pin().unwrap().snapshot().control();
    assert!(selected == fixture.target.control || selected == alternate.control);
    assert_eq!(
        decode_control_slot(
            &std::fs::read(root.join("CONTROL.1")).unwrap(),
            ControlSlotIndex::One,
        )
        .unwrap(),
        selected
    );
}

#[cfg(feature = "test-failpoints")]
struct ProcessRecoveryWal;

#[cfg(feature = "test-failpoints")]
impl WalRecovery for ProcessRecoveryWal {
    type State = ();

    fn read_catalog_transactions(
        &mut self,
        _floor: WalReplayFloor,
        _byte_budget: u64,
    ) -> FormatResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn replay_data(
        &mut self,
        context: DataWalRecoveryContext<'_>,
    ) -> FormatResult<DataWalRecoveryOutcome<Self::State>> {
        let mut tables = context
            .catalog()
            .objects_of_kind(ObjectKind::Table)
            .map(|table| table.id())
            .collect::<Vec<_>>();
        tables.sort_unstable();
        Ok(DataWalRecoveryOutcome::new(
            (),
            context.floor().lsn(),
            0,
            0,
            0,
            tables,
        ))
    }
}

#[cfg(feature = "test-failpoints")]
fn process_fixture(root: &Path) -> FixtureParts {
    std::fs::create_dir(root.join("staging")).unwrap();
    let builds = tempfile::tempdir().unwrap();
    let database_id = DatabaseId::from_bytes(raw(1)).unwrap();
    let table_id = object_id(2);
    let column_id = object_id(3);
    let writer = WriterInstanceId::from_bytes(raw(4)).unwrap();
    let source = generation(
        builds.path(),
        database_id,
        table_id,
        column_id,
        7,
        3,
        20,
        ControlSlotIndex::Zero,
        writer,
    );
    install(root, &source);
    let target = generation(
        builds.path(),
        database_id,
        table_id,
        column_id,
        8,
        4,
        40,
        ControlSlotIndex::One,
        writer,
    );
    let publisher = PhysicalGenerationPublisher::open(root, source.snapshot).unwrap();
    FixtureParts {
        target,
        publisher,
        writer,
    }
}

#[cfg(feature = "test-failpoints")]
struct FixtureParts {
    target: GenerationFiles,
    publisher: PhysicalGenerationPublisher,
    writer: WriterInstanceId,
}

#[cfg(feature = "test-failpoints")]
#[test]
#[ignore = "isolated child entrypoint; executed by process_abort_publication_matrix"]
fn process_abort_publication_child() {
    let root = PathBuf::from(std::env::var_os("RADIXDB_PROCESS_TEST_ROOT").unwrap());
    let fixture = process_fixture(&root);
    let staged = stage(&root, &fixture.target, fixture.writer);
    let build = begin_seal(&fixture.publisher, &staged);
    let result = fixture
        .publisher
        .publish_staged_generation(build, staged, fixture.target.control);
    panic!("child reached the end instead of stopping: {result:?}");
}

#[cfg(feature = "test-failpoints")]
#[test]
fn process_abort_publication_matrix_recovers_only_complete_generations() {
    let points = [
        GenerationCrashPoint::DataAfterFinalRenameBeforeDirSync,
        GenerationCrashPoint::DataFinalDirDurable,
        GenerationCrashPoint::IndexAfterFinalRenameBeforeDirSync,
        GenerationCrashPoint::IndexFinalDirDurable,
        GenerationCrashPoint::TableManifestAfterRenameBeforeDirSync,
        GenerationCrashPoint::TableManifestDirDurable,
        GenerationCrashPoint::CatalogPackAfterRenameBeforeDirSync,
        GenerationCrashPoint::CatalogPackDirDurable,
        GenerationCrashPoint::DatabaseManifestAfterRenameBeforeDirSync,
        GenerationCrashPoint::DatabaseManifestDirDurable,
        GenerationCrashPoint::ControlAfterPartialWrite,
        GenerationCrashPoint::ControlAfterWriteBeforeFdatasync,
        GenerationCrashPoint::ControlDurable,
        GenerationCrashPoint::ControlAfterRootDirSync,
        GenerationCrashPoint::RuntimeGenerationBeforePublish,
        GenerationCrashPoint::RuntimeGenerationPublished,
    ];

    for point in points {
        let root = tempfile::tempdir().unwrap();
        let evidence = root.path().join("boundary.hit");
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("process_abort_publication_child")
            .arg("--ignored")
            .env("RADIXDB_PROCESS_TEST_ROOT", root.path())
            .env("RADIXDB_GENERATION_FAULT_POINT", point.name())
            .env("RADIXDB_GENERATION_FAULT_READY", &evidence)
            .status()
            .unwrap();
        assert!(!status.success(), "{} did not stop the child", point.name());
        assert_eq!(
            std::fs::read_to_string(&evidence).unwrap().trim(),
            point.name()
        );

        let mut first_wal = ProcessRecoveryWal;
        let first = DatabaseRecovery::new(root.path(), RecoveryLimits::default())
            .recover(&mut first_wal)
            .unwrap_or_else(|error| panic!("{} first reopen failed: {error}", point.name()));
        let selected = first.control();
        match point.physical_expectation() {
            PhysicalGenerationExpectation::Old => {
                assert_eq!(selected.database_generation().get(), 7)
            }
            PhysicalGenerationExpectation::New => {
                assert_eq!(selected.database_generation().get(), 8)
            }
            PhysicalGenerationExpectation::OldOrNew => {
                assert!(matches!(selected.database_generation().get(), 7 | 8))
            }
        }
        assert_eq!(
            first
                .catalog()
                .object(object_id(2))
                .unwrap()
                .name()
                .display()
                .as_str(),
            "messages"
        );

        let mut second_wal = ProcessRecoveryWal;
        let second = DatabaseRecovery::new(root.path(), RecoveryLimits::default())
            .recover(&mut second_wal)
            .unwrap_or_else(|error| panic!("{} second reopen failed: {error}", point.name()));
        assert_eq!(second.control(), selected);
    }
}

#[cfg(feature = "test-failpoints")]
fn requested_process_point() -> GenerationCrashPoint {
    let requested = std::env::var("RADIXDB_PROCESS_TEST_POINT").unwrap();
    GenerationCrashPoint::LIFECYCLE_PUBLICATION_POINTS
        .into_iter()
        .find(|point| point.name() == requested)
        .unwrap_or_else(|| panic!("unknown process test point {requested}"))
}

#[cfg(feature = "test-failpoints")]
fn activate_process_fault(point: GenerationCrashPoint) {
    let ready = std::env::var_os("RADIXDB_PROCESS_TEST_READY").unwrap();
    std::env::set_var("RADIXDB_GENERATION_FAULT_POINT", point.name());
    std::env::set_var("RADIXDB_GENERATION_FAULT_READY", ready);
}

#[cfg(feature = "test-failpoints")]
#[test]
#[ignore = "isolated child entrypoint; executed by process_abort_build_matrix"]
fn process_abort_build_child() {
    let root = PathBuf::from(std::env::var_os("RADIXDB_PROCESS_TEST_ROOT").unwrap());
    let fixture = process_fixture(&root);
    let point = requested_process_point();
    activate_process_fault(point);

    let outcome: FormatResult<()> = match point {
        GenerationCrashPoint::StageOwnerBeforeWrite | GenerationCrashPoint::StageOwnerAfterSync => {
            StagedArtifactSet::create(
                root.join("staging"),
                fixture.writer,
                fixture.target.control.database_generation(),
                77,
                3_000,
            )
            .map(drop)
        }
        GenerationCrashPoint::DataAfterBodyBeforeFooter
        | GenerationCrashPoint::DataAfterFooterBeforeSync
        | GenerationCrashPoint::IndexAfterBodyBeforeFooter
        | GenerationCrashPoint::IndexAfterFooterBeforeSync
        | GenerationCrashPoint::TableManifestAfterBodyBeforeFooter
        | GenerationCrashPoint::DatabaseManifestAfterBodyBeforeFooter => {
            let builds = tempfile::tempdir().unwrap();
            let _ = generation(
                builds.path(),
                fixture.target.control.database_id(),
                object_id(2),
                object_id(3),
                9,
                5,
                80,
                ControlSlotIndex::One,
                fixture.writer,
            );
            Ok(())
        }
        GenerationCrashPoint::CatalogPackAfterBodyBeforeFooter => {
            let catalog = decode_catalog_pack(&fixture.target.catalog_bytes).unwrap();
            encode_catalog_artifact(catalog.meta(), catalog.graph()).map(drop)
        }
        GenerationCrashPoint::DataAfterFileSync
        | GenerationCrashPoint::IndexAfterFileSync
        | GenerationCrashPoint::TableManifestAfterFileSync
        | GenerationCrashPoint::CatalogPackAfterFileSync
        | GenerationCrashPoint::DatabaseManifestAfterFileSync
        | GenerationCrashPoint::WalSuccessorAfterCreateBeforeSync
        | GenerationCrashPoint::WalSuccessorDurable => {
            let staging = StagedArtifactSet::create(
                root.join("staging"),
                fixture.writer,
                fixture.target.control.database_generation(),
                78,
                3_001,
            )
            .unwrap();
            let (role, path, bytes) = match point {
                GenerationCrashPoint::DataAfterFileSync => (
                    StagedMemberRole::Data,
                    fixture.target.data_reference.relative_path(),
                    fixture.target.data_bytes.as_slice(),
                ),
                GenerationCrashPoint::IndexAfterFileSync => (
                    StagedMemberRole::Index,
                    fixture.target.index_reference.relative_path(),
                    fixture.target.index_bytes.as_slice(),
                ),
                GenerationCrashPoint::TableManifestAfterFileSync => (
                    StagedMemberRole::TableManifest,
                    table_manifest_path(fixture.target.table_reference),
                    fixture.target.table_bytes.as_slice(),
                ),
                GenerationCrashPoint::CatalogPackAfterFileSync => (
                    StagedMemberRole::CatalogPack,
                    catalog_path(fixture.target.catalog_reference),
                    fixture.target.catalog_bytes.as_slice(),
                ),
                GenerationCrashPoint::DatabaseManifestAfterFileSync => (
                    StagedMemberRole::DatabaseManifest,
                    database_manifest_path(fixture.target.database_reference),
                    fixture.target.database_bytes.as_slice(),
                ),
                GenerationCrashPoint::WalSuccessorAfterCreateBeforeSync
                | GenerationCrashPoint::WalSuccessorDurable => (
                    StagedMemberRole::WalSuccessor,
                    wal_path(fixture.target.wal_floor),
                    &[][..],
                ),
                _ => unreachable!(),
            };
            staging
                .write_generation_file(role, path.to_str().unwrap(), |file| {
                    file.write_all(bytes).unwrap();
                    Ok(())
                })
                .map(drop)
        }
        _ => panic!("{} is not a build-boundary point", point.name()),
    };
    panic!("child reached the end instead of stopping: {outcome:?}");
}

#[cfg(feature = "test-failpoints")]
#[test]
fn process_abort_build_matrix_preserves_the_complete_source_root() {
    let points = [
        GenerationCrashPoint::StageOwnerBeforeWrite,
        GenerationCrashPoint::StageOwnerAfterSync,
        GenerationCrashPoint::DataAfterBodyBeforeFooter,
        GenerationCrashPoint::DataAfterFooterBeforeSync,
        GenerationCrashPoint::DataAfterFileSync,
        GenerationCrashPoint::IndexAfterBodyBeforeFooter,
        GenerationCrashPoint::IndexAfterFooterBeforeSync,
        GenerationCrashPoint::IndexAfterFileSync,
        GenerationCrashPoint::TableManifestAfterBodyBeforeFooter,
        GenerationCrashPoint::TableManifestAfterFileSync,
        GenerationCrashPoint::CatalogPackAfterBodyBeforeFooter,
        GenerationCrashPoint::CatalogPackAfterFileSync,
        GenerationCrashPoint::WalSuccessorAfterCreateBeforeSync,
        GenerationCrashPoint::WalSuccessorDurable,
        GenerationCrashPoint::DatabaseManifestAfterBodyBeforeFooter,
        GenerationCrashPoint::DatabaseManifestAfterFileSync,
    ];

    for point in points {
        let root = tempfile::tempdir().unwrap();
        let evidence = root.path().join("boundary.hit");
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("process_abort_build_child")
            .arg("--ignored")
            .env("RADIXDB_PROCESS_TEST_ROOT", root.path())
            .env("RADIXDB_PROCESS_TEST_POINT", point.name())
            .env("RADIXDB_PROCESS_TEST_READY", &evidence)
            .status()
            .unwrap();
        assert!(!status.success(), "{} did not stop the child", point.name());
        assert_eq!(
            std::fs::read_to_string(&evidence).unwrap().trim(),
            point.name()
        );

        for attempt in 0..2 {
            let mut wal = ProcessRecoveryWal;
            let recovered = DatabaseRecovery::new(root.path(), RecoveryLimits::default())
                .recover(&mut wal)
                .unwrap_or_else(|error| {
                    panic!("{} reopen {attempt} failed: {error}", point.name())
                });
            assert_eq!(recovered.control().database_generation().get(), 7);
            assert_eq!(
                recovered
                    .catalog()
                    .object(object_id(2))
                    .unwrap()
                    .name()
                    .display()
                    .as_str(),
                "messages"
            );
        }
    }
}
