#![cfg(feature = "test-hooks")]

use std::io::Write;
use std::path::{Path, PathBuf};

use radixdb_catalog::{
    encode_catalog_pack, CatalogDataType, CatalogEdge, CatalogGraph, CatalogName, CatalogObject,
    CatalogPackMeta, CatalogPayload, ColumnPayload, EdgeKind, NamespacePayload, ObjectId,
    TablePayload,
};
use radixdb_core::DataType;
use radixdb_storage::v6::{
    encode_control_slot, encode_data_artifact, encode_database_manifest, encode_table_manifest,
    publication_diagnostics, reset_publication_diagnostics, ArtifactBuildKind, ArtifactId,
    ArtifactRef, CatalogGeneration, CatalogId, CatalogRef, CatalogRootRef, ControlRecord,
    ControlSlotIndex, DataArtifactHeader, DataArtifactInput, DataBlockSpec, DataPhysicalCodec,
    DatabaseGeneration, DatabaseId, DatabaseManifest, DatabaseManifestRootRef, FrozenCheckpoint,
    ManifestGeneration, ManifestId, ManifestKind, ManifestRef, PhysicalGenerationPublisher,
    PhysicalGenerationSnapshot, SegmentDescriptor, SegmentId, SegmentKind, StagedArtifactSet,
    TableManifest, TableManifestRef, WalGeneration, WalReplayFloor, WriterInstanceId,
    MAX_ROWS_PER_GROUP,
};

const ARTIFACT_FOOTER_BYTES: u64 = 48;
const DELTA_ROW_ID: u64 = 1_000_000_000;

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

fn control_path(slot: ControlSlotIndex) -> &'static Path {
    Path::new(match slot {
        ControlSlotIndex::Zero => "CONTROL.0",
        ControlSlotIndex::One => "CONTROL.1",
    })
}

fn write_file(root: &Path, relative: &Path, bytes: &[u8]) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
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

fn encode_catalog(
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
                "events",
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
            1,
            1,
        )
        .unwrap(),
        &graph,
    )
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn tombstone_artifact(
    database_id: DatabaseId,
    table_id: ObjectId,
    database_generation: DatabaseGeneration,
    catalog_generation: CatalogGeneration,
    artifact_marker: u8,
    segment_marker: u8,
    first_row_id: u64,
    row_count: usize,
) -> (Vec<u8>, ArtifactRef, SegmentDescriptor) {
    let segment_id = SegmentId::from_bytes(raw(segment_marker)).unwrap();
    let row_group_count = row_count.div_ceil(MAX_ROWS_PER_GROUP as usize) as u32;
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(artifact_marker)).unwrap(),
        database_id,
        table_id,
        segment_id,
        database_generation,
        catalog_generation,
        1,
        1,
        row_count as u64,
        0,
        row_group_count,
        SegmentKind::Tombstones,
        1,
    )
    .unwrap();
    // Deliberately use a non-dense retained sequence. Dense row IDs have a
    // constant-size arithmetic encoding, so increasing their count no longer
    // grows the immutable body and cannot exercise the delta-scaling gate.
    let row_ids = (0..row_count as u64)
        .map(|offset| first_row_id + offset.saturating_mul(2))
        .collect::<Vec<_>>();
    let blocks = row_ids
        .chunks(MAX_ROWS_PER_GROUP as usize)
        .enumerate()
        .map(|(group, rows)| {
            DataBlockSpec::row_ids(group as u32, rows, DataPhysicalCodec::None).unwrap()
        })
        .collect();
    let (bytes, reference) =
        encode_data_artifact(&DataArtifactInput::new(header, vec![], vec![], blocks).unwrap())
            .unwrap();
    let last_row_id = *row_ids.last().unwrap();
    let descriptor = SegmentDescriptor::new(
        segment_id,
        SegmentKind::Tombstones,
        1,
        1,
        row_count as u64,
        first_row_id,
        last_row_id,
        reference,
        None,
    )
    .unwrap();
    (bytes, reference, descriptor)
}

fn encode_table(
    database_id: DatabaseId,
    table_id: ObjectId,
    generation: DatabaseGeneration,
    catalog_generation: CatalogGeneration,
    manifest_marker: u8,
    row_id_high_water: u64,
    segments: Vec<SegmentDescriptor>,
) -> (TableManifest, Vec<u8>, TableManifestRef) {
    let manifest = TableManifest::new(
        database_id,
        table_id,
        ManifestId::from_bytes(raw(manifest_marker)).unwrap(),
        ManifestGeneration::new(generation.get()).unwrap(),
        catalog_generation,
        row_id_high_water,
        segments.len() as u64 + 1,
        segments,
        1,
    )
    .unwrap();
    let bytes = encode_table_manifest(&manifest).unwrap();
    let reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            manifest.manifest_id(),
            ManifestKind::Table,
            manifest.generation(),
            bytes.len() as u64,
            footer_sha(&bytes),
        )
        .unwrap(),
    )
    .unwrap();
    (manifest, bytes, reference)
}

#[allow(clippy::too_many_arguments)]
fn encode_database(
    database_id: DatabaseId,
    generation: DatabaseGeneration,
    manifest_marker: u8,
    catalog: CatalogRef,
    wal_floor: WalReplayFloor,
    table: TableManifestRef,
) -> (DatabaseManifest, Vec<u8>, DatabaseManifestRootRef) {
    let manifest = DatabaseManifest::new(
        database_id,
        ManifestId::from_bytes(raw(manifest_marker)).unwrap(),
        generation,
        catalog,
        wal_floor,
        1,
        vec![table],
        1,
    )
    .unwrap();
    let bytes = encode_database_manifest(&manifest).unwrap();
    let reference = DatabaseManifestRootRef::new(
        manifest.manifest_id(),
        ManifestGeneration::new(generation.get()).unwrap(),
        footer_sha(&bytes),
    );
    (manifest, bytes, reference)
}

struct ScaleFixture {
    _root: tempfile::TempDir,
    publisher: PhysicalGenerationPublisher,
    checkpoint: FrozenCheckpoint,
    retained_body_bytes: u64,
    delta_body_bytes: u64,
}

fn scale_fixture(retained_rows: usize) -> ScaleFixture {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("staging")).unwrap();
    let database_id = DatabaseId::from_bytes(raw(0x10)).unwrap();
    let table_id = object_id(0x11);
    let column_id = object_id(0x12);
    let writer = WriterInstanceId::from_bytes(raw(0x13)).unwrap();
    let catalog_generation = CatalogGeneration::new(3).unwrap();
    let source_generation = DatabaseGeneration::new(7).unwrap();
    let target_generation = DatabaseGeneration::new(8).unwrap();

    let catalog_id = CatalogId::from_bytes(raw(0x14)).unwrap();
    let catalog_bytes = encode_catalog(
        database_id,
        catalog_id,
        catalog_generation,
        table_id,
        column_id,
    );
    let catalog = CatalogRef::new(
        catalog_id,
        catalog_generation,
        catalog_bytes.len() as u64,
        footer_sha(&catalog_bytes),
    )
    .unwrap();

    let (retained_bytes, retained, retained_segment) = tombstone_artifact(
        database_id,
        table_id,
        source_generation,
        catalog_generation,
        0x20,
        0x21,
        1,
        retained_rows,
    );
    let (delta_bytes, delta, delta_segment) = tombstone_artifact(
        database_id,
        table_id,
        target_generation,
        catalog_generation,
        0x22,
        0x23,
        DELTA_ROW_ID,
        1,
    );

    let (source_table, source_table_bytes, source_table_ref) = encode_table(
        database_id,
        table_id,
        source_generation,
        catalog_generation,
        0x30,
        retained_segment.last_row_id(),
        vec![retained_segment],
    );
    let (target_table, target_table_bytes, target_table_ref) = encode_table(
        database_id,
        table_id,
        target_generation,
        catalog_generation,
        0x31,
        DELTA_ROW_ID,
        vec![retained_segment, delta_segment],
    );

    let source_floor = WalReplayFloor::new(WalGeneration::new(7).unwrap(), 0);
    let target_floor = WalReplayFloor::new(WalGeneration::new(8).unwrap(), 0);
    let (source_database, source_database_bytes, source_database_ref) = encode_database(
        database_id,
        source_generation,
        0x32,
        catalog,
        source_floor,
        source_table_ref,
    );
    let (target_database, target_database_bytes, target_database_ref) = encode_database(
        database_id,
        target_generation,
        0x33,
        catalog,
        target_floor,
        target_table_ref,
    );
    let source_control = ControlRecord::new(
        ControlSlotIndex::Zero,
        source_generation,
        database_id,
        source_database_ref,
        CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog_bytes)),
        source_floor,
        1,
        writer,
    )
    .unwrap();
    let target_control = ControlRecord::new(
        ControlSlotIndex::One,
        target_generation,
        database_id,
        target_database_ref,
        CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog_bytes)),
        target_floor,
        2,
        writer,
    )
    .unwrap();
    let source_snapshot =
        PhysicalGenerationSnapshot::new(source_control, source_database, vec![source_table])
            .unwrap();
    let _target_snapshot =
        PhysicalGenerationSnapshot::new(target_control, target_database, vec![target_table])
            .unwrap();

    for (path, bytes) in [
        (retained.relative_path(), retained_bytes.as_slice()),
        (catalog_path(catalog), catalog_bytes.as_slice()),
        (
            table_manifest_path(source_table_ref),
            source_table_bytes.as_slice(),
        ),
        (
            database_manifest_path(source_database_ref),
            source_database_bytes.as_slice(),
        ),
        (wal_path(source_floor), &[][..]),
        (
            control_path(ControlSlotIndex::Zero).to_path_buf(),
            encode_control_slot(source_control).as_slice(),
        ),
    ] {
        write_file(root.path(), &path, bytes);
    }

    let staged =
        StagedArtifactSet::create(root.path().join("staging"), writer, target_generation, 1, 1)
            .unwrap();
    for (path, bytes) in [
        (delta.relative_path(), delta_bytes.as_slice()),
        (
            table_manifest_path(target_table_ref),
            target_table_bytes.as_slice(),
        ),
        (
            database_manifest_path(target_database_ref),
            target_database_bytes.as_slice(),
        ),
        (wal_path(target_floor), &[][..]),
    ] {
        staged
            .write_file(path.to_str().unwrap(), |file| {
                file.write_all(bytes).unwrap();
                Ok(())
            })
            .unwrap();
    }
    let staged = staged.mark_complete(2, Default::default()).unwrap();
    let publisher = PhysicalGenerationPublisher::open(root.path(), source_snapshot).unwrap();
    let source = publisher.pin().unwrap();
    let build = publisher
        .begin_artifact_build(ArtifactBuildKind::Seal, &source, staged.owner())
        .unwrap();
    drop(source);
    let checkpoint =
        FrozenCheckpoint::new(source_control, target_control, staged, build, vec![]).unwrap();
    ScaleFixture {
        _root: root,
        publisher,
        checkpoint,
        retained_body_bytes: retained.byte_length() - ARTIFACT_FOOTER_BYTES,
        delta_body_bytes: delta.byte_length() - ARTIFACT_FOOTER_BYTES,
    }
}

#[test]
fn publication_identity_reads_follow_delta_not_retained_body_scale() {
    let mut previous_retained_bytes = 0_u64;
    let mut delta_bytes = None;

    for retained_rows in [8_192, 81_920, 819_200] {
        let fixture = scale_fixture(retained_rows);
        assert!(
            fixture.retained_body_bytes > previous_retained_bytes.saturating_mul(8),
            "retained body did not grow by the intended scale: previous={previous_retained_bytes}, current={}",
            fixture.retained_body_bytes
        );
        previous_retained_bytes = fixture.retained_body_bytes;
        assert_eq!(
            *delta_bytes.get_or_insert(fixture.delta_body_bytes),
            fixture.delta_body_bytes
        );

        reset_publication_diagnostics();
        let outcome = fixture
            .publisher
            .publish_checkpoint(fixture.checkpoint)
            .unwrap();
        assert_eq!(outcome.lease().database_generation().get(), 8);

        let counters = publication_diagnostics();
        assert_eq!(counters.generation_fence_holds, 1);
        assert_eq!(counters.retained_artifact_identity_read_calls, 0);
        assert_eq!(counters.retained_artifact_identity_read_bytes, 0);
        assert_eq!(
            counters.new_artifact_identity_read_bytes,
            fixture.delta_body_bytes
        );
        assert!(counters.new_artifact_identity_read_calls >= 1);
        println!(
            "retained_rows={retained_rows} retained_body_bytes={} delta_body_bytes={} \
             retained_identity_read_bytes={} new_identity_read_bytes={} fence_ns={}",
            fixture.retained_body_bytes,
            fixture.delta_body_bytes,
            counters.retained_artifact_identity_read_bytes,
            counters.new_artifact_identity_read_bytes,
            counters.generation_fence_hold_nanoseconds,
        );
    }
}
