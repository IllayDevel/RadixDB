use std::collections::HashMap;
use std::path::Path;

use radixdb_catalog::{
    encode_catalog_pack, CatalogDataType, CatalogEdge, CatalogGeneration as RuntimeCatalog,
    CatalogGraph, CatalogMutation, CatalogMutationSet, CatalogName, CatalogObject, CatalogPackMeta,
    CatalogPayload, ColumnPayload, EdgeKind, ObjectId, ObjectKind, ObjectPrecondition,
    TablePayload,
};
use radixdb_core::DataType;
use radixdb_storage::v6::{
    encode_catalog_wal_transaction, encode_control_slot, encode_data_artifact,
    encode_database_manifest, encode_table_manifest, ArtifactId, ArtifactKind, ArtifactRef,
    CatalogGeneration, CatalogId, CatalogRef, CatalogRootRef, CatalogWalTransaction,
    CatalogWalTransactionId, ControlRecord, ControlSlotIndex, DataArtifactHeader,
    DataArtifactInput, DataBlockSpec, DataPhysicalCodec, DataWalRecoveryContext,
    DataWalRecoveryOutcome, DatabaseGeneration, DatabaseId, DatabaseManifest,
    DatabaseManifestRootRef, DatabaseRecovery, FormatError, FormatResult, ManifestGeneration,
    ManifestId, ManifestKind, ManifestRef, ReachabilityLimits, RecoveryLimits, SegmentDescriptor,
    SegmentId, SegmentKind, TableManifest, TableManifestRef, UnavailableIndexReason, WalGeneration,
    WalRecovery, WalReplayFloor, WriterInstanceId,
};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn footer_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - 32..].try_into().unwrap()
}

fn catalog_object(
    id: ObjectId,
    namespace: Option<ObjectId>,
    parent: Option<ObjectId>,
    name: &str,
    payload: CatalogPayload,
) -> CatalogObject {
    CatalogObject::new(
        id,
        namespace,
        parent,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        1,
        payload,
    )
    .unwrap()
}

fn catalog(
    database_id: DatabaseId,
    catalog_id: CatalogId,
    generation: u64,
    snapshot_lsn: u64,
    table_id: ObjectId,
) -> RuntimeCatalog {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let column_id = object_id(11);
    let graph = CatalogGraph::build(
        vec![
            catalog_object(
                namespace,
                None,
                None,
                "public",
                CatalogPayload::Namespace(radixdb_catalog::NamespacePayload::new()),
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
                "id",
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
    RuntimeCatalog::new(
        CatalogPackMeta::new(
            database_id.into_bytes(),
            catalog_id.into_bytes(),
            generation,
            snapshot_lsn,
            snapshot_lsn * 1_000,
        )
        .unwrap(),
        graph,
    )
}

struct GenerationFixture {
    control: ControlRecord,
    catalog_bytes: Vec<u8>,
    table_bytes: Vec<u8>,
    database_bytes: Vec<u8>,
    wal_bytes: Vec<u8>,
    recovered_name: &'static str,
}

#[allow(clippy::too_many_arguments)]
fn generation(
    database_id: DatabaseId,
    table_id: ObjectId,
    database_generation: u64,
    catalog_marker: u8,
    catalog_generation: u64,
    manifest_marker: u8,
    floor_generation: u64,
    floor_lsn: u64,
    slot: ControlSlotIndex,
    recovered_name: &'static str,
) -> GenerationFixture {
    let catalog_id = CatalogId::from_bytes(raw(catalog_marker)).unwrap();
    let base = catalog(
        database_id,
        catalog_id,
        catalog_generation,
        floor_lsn,
        table_id,
    );
    let catalog_bytes = encode_catalog_pack(base.meta(), base.graph()).unwrap();
    let catalog_reference = CatalogRef::new(
        catalog_id,
        CatalogGeneration::new(catalog_generation).unwrap(),
        catalog_bytes.len() as u64,
        footer_sha(&catalog_bytes),
    )
    .unwrap();

    let manifest_generation = ManifestGeneration::new(database_generation).unwrap();
    let table_manifest_id = ManifestId::from_bytes(raw(manifest_marker)).unwrap();
    let table = TableManifest::new(
        database_id,
        table_id,
        table_manifest_id,
        manifest_generation,
        catalog_reference.generation(),
        0,
        1,
        vec![],
        floor_lsn * 1_000,
    )
    .unwrap();
    let table_bytes = encode_table_manifest(&table).unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest_id,
            ManifestKind::Table,
            manifest_generation,
            table_bytes.len() as u64,
            footer_sha(&table_bytes),
        )
        .unwrap(),
    )
    .unwrap();

    let floor = WalReplayFloor::new(WalGeneration::new(floor_generation).unwrap(), floor_lsn);
    let database_manifest_id = ManifestId::from_bytes(raw(manifest_marker + 1)).unwrap();
    let database = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        DatabaseGeneration::new(database_generation).unwrap(),
        catalog_reference,
        floor,
        10_000,
        vec![table_reference],
        floor_lsn * 1_000,
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&database).unwrap();
    let control = ControlRecord::new(
        slot,
        DatabaseGeneration::new(database_generation).unwrap(),
        database_id,
        DatabaseManifestRootRef::new(
            database_manifest_id,
            manifest_generation,
            footer_sha(&database_bytes),
        ),
        CatalogRootRef::new(
            catalog_id,
            catalog_reference.generation(),
            footer_sha(&catalog_bytes),
        ),
        floor,
        floor_lsn * 1_000,
        WriterInstanceId::from_bytes(raw(30)).unwrap(),
    )
    .unwrap();

    let mutation = CatalogMutationSet::new(
        database_id.into_bytes(),
        catalog_id.into_bytes(),
        catalog_generation,
        vec![CatalogMutation::rename(
            ObjectPrecondition::new(table_id, ObjectKind::Table, 1).unwrap(),
            CatalogName::new(recovered_name).unwrap(),
        )],
        vec![],
        vec![],
    )
    .unwrap();
    let transaction = CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes(raw(catalog_marker + 40)).unwrap(),
        raw(catalog_marker + 20),
        floor_lsn + 1,
        (floor_lsn + 1) * 1_000,
        mutation,
    )
    .unwrap();
    let wal_bytes = encode_catalog_wal_transaction(&transaction).unwrap();

    GenerationFixture {
        control,
        catalog_bytes,
        table_bytes,
        database_bytes,
        wal_bytes,
        recovered_name,
    }
}

fn write_file(root: &Path, relative: impl AsRef<Path>, bytes: &[u8]) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn install_generation(root: &Path, generation: &GenerationFixture) {
    let control = generation.control;
    let control_name = match control.slot() {
        ControlSlotIndex::Zero => "CONTROL.0",
        ControlSlotIndex::One => "CONTROL.1",
    };
    write_file(root, control_name, &encode_control_slot(control));
    write_file(
        root,
        format!(
            "catalog/catalog-{:016x}.cat",
            control.catalog().generation().get()
        ),
        &generation.catalog_bytes,
    );
    let table_id = object_id(10);
    write_file(
        root,
        format!(
            "manifests/tables/{table_id}/table-{:016x}.mft",
            control.database_generation().get()
        ),
        &generation.table_bytes,
    );
    write_file(
        root,
        format!(
            "manifests/database-{:016x}.mft",
            control.database_generation().get()
        ),
        &generation.database_bytes,
    );
}

struct Fixture {
    root: tempfile::TempDir,
    old: GenerationFixture,
    new: GenerationFixture,
    table_id: ObjectId,
}

fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let database_id = DatabaseId::from_bytes(raw(1)).unwrap();
    let table_id = object_id(10);
    let old = generation(
        database_id,
        table_id,
        7,
        2,
        3,
        4,
        5,
        500,
        ControlSlotIndex::Zero,
        "old_recovered",
    );
    let new = generation(
        database_id,
        table_id,
        8,
        3,
        4,
        6,
        6,
        600,
        ControlSlotIndex::One,
        "new_recovered",
    );
    install_generation(root.path(), &old);
    install_generation(root.path(), &new);
    Fixture {
        root,
        old,
        new,
        table_id,
    }
}

#[derive(Default)]
struct WalDriver {
    bytes: HashMap<WalReplayFloor, Vec<u8>>,
    catalog_calls: Vec<WalReplayFloor>,
    data_calls: Vec<(WalReplayFloor, u64, String)>,
    fail_data: bool,
    wrong_tables: bool,
}

impl WalRecovery for WalDriver {
    type State = &'static str;

    fn read_catalog_transactions(
        &mut self,
        floor: WalReplayFloor,
        byte_budget: u64,
    ) -> FormatResult<Vec<u8>> {
        self.catalog_calls.push(floor);
        let bytes = self.bytes.get(&floor).ok_or(FormatError::InvalidRecovery {
            detail: "catalog WAL fixture is absent",
        })?;
        assert!(bytes.len() as u64 <= byte_budget);
        Ok(bytes.clone())
    }

    fn replay_data(
        &mut self,
        context: DataWalRecoveryContext<'_>,
    ) -> FormatResult<DataWalRecoveryOutcome<Self::State>> {
        let table = context
            .catalog()
            .objects_of_kind(ObjectKind::Table)
            .next()
            .unwrap();
        self.data_calls.push((
            context.floor(),
            context.physical().control().database_generation().get(),
            table.name().display().as_str().to_owned(),
        ));
        if self.fail_data {
            return Err(FormatError::InvalidRecovery {
                detail: "injected data WAL failure",
            });
        }
        let mut tables = context
            .catalog()
            .objects_of_kind(ObjectKind::Table)
            .map(|table| table.id())
            .collect::<Vec<_>>();
        tables.sort_unstable();
        if self.wrong_tables {
            tables.clear();
        }
        Ok(DataWalRecoveryOutcome::new(
            "runtime",
            context.floor().lsn() + 1,
            1,
            2,
            0,
            tables,
        ))
    }
}

impl WalDriver {
    fn for_fixture(fixture: &Fixture) -> Self {
        Self {
            bytes: HashMap::from([
                (
                    fixture.old.control.wal_replay_floor(),
                    fixture.old.wal_bytes.clone(),
                ),
                (
                    fixture.new.control.wal_replay_floor(),
                    fixture.new.wal_bytes.clone(),
                ),
            ]),
            ..Self::default()
        }
    }
}

#[test]
fn newest_complete_generation_replays_catalog_then_data_exactly_once() {
    let fixture = fixture();
    let mut wal = WalDriver::for_fixture(&fixture);
    let recovered = DatabaseRecovery::new(fixture.root.path(), RecoveryLimits::default())
        .recover(&mut wal)
        .unwrap();

    assert_eq!(recovered.control(), fixture.new.control);
    assert_eq!(wal.catalog_calls, [fixture.new.control.wal_replay_floor()]);
    assert_eq!(wal.data_calls.len(), 1);
    assert_eq!(wal.data_calls[0].1, 8);
    assert_eq!(wal.data_calls[0].2, fixture.new.recovered_name);
    assert_eq!(recovered.catalog_report().replayed_transactions(), 1);
    assert_eq!(recovered.catalog_report().base_generation().get(), 4);
    assert_eq!(recovered.catalog_report().final_generation().get(), 5);
    assert_eq!(recovered.data_report().applied_transactions(), 1);
    assert_eq!(recovered.into_runtime_state(), "runtime");
}

#[test]
fn incomplete_newer_graph_falls_back_before_any_data_replay() {
    let fixture = fixture();
    std::fs::remove_file(fixture.root.path().join(format!(
        "manifests/database-{:016x}.mft",
        fixture.new.control.database_generation().get()
    )))
    .unwrap();
    let mut wal = WalDriver::for_fixture(&fixture);
    let recovered = DatabaseRecovery::new(fixture.root.path(), RecoveryLimits::default())
        .recover(&mut wal)
        .unwrap();

    assert_eq!(recovered.control(), fixture.old.control);
    assert_eq!(wal.catalog_calls, [fixture.old.control.wal_replay_floor()]);
    assert_eq!(wal.data_calls.len(), 1);
    assert_eq!(wal.data_calls[0].1, 7);
    assert_eq!(wal.data_calls[0].2, fixture.old.recovered_name);
}

#[test]
fn metadata_budget_exhaustion_never_selects_an_older_control() {
    let fixture = fixture();
    let mut wal = WalDriver::for_fixture(&fixture);
    let limits = RecoveryLimits::new(
        ReachabilityLimits::new(100, 512).unwrap(),
        Default::default(),
    );
    let result = DatabaseRecovery::new(fixture.root.path(), limits).recover(&mut wal);

    assert!(matches!(
        result,
        Err(FormatError::MetadataOpenLimitExceeded {
            field: "accounted bytes",
            actual,
            limit: 512,
        }) if actual > 512
    ));
    assert!(
        wal.catalog_calls.is_empty(),
        "resource exhaustion must stop before fallback or WAL replay"
    );
    assert!(wal.data_calls.is_empty());
}

#[test]
fn missing_newer_catalog_wal_falls_back_without_partial_data_replay() {
    let fixture = fixture();
    let mut wal = WalDriver::for_fixture(&fixture);
    wal.bytes.remove(&fixture.new.control.wal_replay_floor());
    let recovered = DatabaseRecovery::new(fixture.root.path(), RecoveryLimits::default())
        .recover(&mut wal)
        .unwrap();

    assert_eq!(recovered.control(), fixture.old.control);
    assert_eq!(
        wal.catalog_calls,
        [
            fixture.new.control.wal_replay_floor(),
            fixture.old.control.wal_replay_floor()
        ]
    );
    assert_eq!(wal.data_calls.len(), 1);
}

#[test]
fn corrupt_newer_catalog_wal_falls_back_before_data_replay() {
    let fixture = fixture();
    let mut wal = WalDriver::for_fixture(&fixture);
    wal.bytes
        .get_mut(&fixture.new.control.wal_replay_floor())
        .unwrap()[160] ^= 1;
    let recovered = DatabaseRecovery::new(fixture.root.path(), RecoveryLimits::default())
        .recover(&mut wal)
        .unwrap();

    assert_eq!(recovered.control(), fixture.old.control);
    assert_eq!(
        wal.catalog_calls,
        [
            fixture.new.control.wal_replay_floor(),
            fixture.old.control.wal_replay_floor()
        ]
    );
    assert_eq!(wal.data_calls.len(), 1);
    assert_eq!(wal.data_calls[0].1, 7);
}

#[test]
fn data_replay_failure_never_retries_an_older_control() {
    let fixture = fixture();
    let mut wal = WalDriver {
        fail_data: true,
        ..WalDriver::for_fixture(&fixture)
    };
    let result =
        DatabaseRecovery::new(fixture.root.path(), RecoveryLimits::default()).recover(&mut wal);

    assert!(matches!(result, Err(FormatError::InvalidRecovery { .. })));
    assert_eq!(wal.catalog_calls, [fixture.new.control.wal_replay_floor()]);
    assert_eq!(wal.data_calls.len(), 1);
}

#[test]
fn final_runtime_table_set_must_equal_the_recovered_catalog() {
    let fixture = fixture();
    let mut wal = WalDriver {
        wrong_tables: true,
        ..WalDriver::for_fixture(&fixture)
    };
    let result =
        DatabaseRecovery::new(fixture.root.path(), RecoveryLimits::default()).recover(&mut wal);

    assert!(matches!(result, Err(FormatError::InvalidRecovery { .. })));
    assert_eq!(wal.data_calls.len(), 1);
    assert_eq!(fixture.table_id, object_id(10));
}

#[test]
fn missing_optional_index_is_explicitly_unavailable_without_hiding_data() {
    let root = tempfile::tempdir().unwrap();
    let database_id = DatabaseId::from_bytes(raw(71)).unwrap();
    let table_id = object_id(10);
    let catalog_id = CatalogId::from_bytes(raw(72)).unwrap();
    let catalog_generation = CatalogGeneration::new(3).unwrap();
    let catalog = catalog(database_id, catalog_id, 3, 500, table_id);
    let catalog_bytes = encode_catalog_pack(catalog.meta(), catalog.graph()).unwrap();
    let catalog_reference = CatalogRef::new(
        catalog_id,
        catalog_generation,
        catalog_bytes.len() as u64,
        footer_sha(&catalog_bytes),
    )
    .unwrap();

    let database_generation = DatabaseGeneration::new(7).unwrap();
    let segment_id = SegmentId::from_bytes(raw(73)).unwrap();
    let data_header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(74)).unwrap(),
        database_id,
        table_id,
        segment_id,
        database_generation,
        catalog_generation,
        1,
        1,
        1,
        0,
        1,
        SegmentKind::Tombstones,
        500_000,
    )
    .unwrap();
    let data_block = DataBlockSpec::row_ids(0, &[101], DataPhysicalCodec::None).unwrap();
    let (data_bytes, data_reference) = encode_data_artifact(
        &DataArtifactInput::new(data_header, vec![], vec![], vec![data_block]).unwrap(),
    )
    .unwrap();
    let index_reference = ArtifactRef::new(
        ArtifactId::from_bytes(raw(75)).unwrap(),
        ArtifactKind::Index,
        database_generation,
        4_096,
        [76; 32],
    )
    .unwrap();

    let manifest_generation = ManifestGeneration::new(7).unwrap();
    let table_manifest_id = ManifestId::from_bytes(raw(77)).unwrap();
    let table = TableManifest::new(
        database_id,
        table_id,
        table_manifest_id,
        manifest_generation,
        catalog_generation,
        1,
        2,
        vec![SegmentDescriptor::new(
            segment_id,
            SegmentKind::Tombstones,
            1,
            1,
            1,
            1,
            1,
            data_reference,
            Some(index_reference),
        )
        .unwrap()],
        500_000,
    )
    .unwrap();
    let table_bytes = encode_table_manifest(&table).unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest_id,
            ManifestKind::Table,
            manifest_generation,
            table_bytes.len() as u64,
            footer_sha(&table_bytes),
        )
        .unwrap(),
    )
    .unwrap();
    let floor = WalReplayFloor::new(WalGeneration::new(5).unwrap(), 500);
    let database_manifest_id = ManifestId::from_bytes(raw(78)).unwrap();
    let database = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        database_generation,
        catalog_reference,
        floor,
        10_000,
        vec![table_reference],
        500_000,
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&database).unwrap();
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        database_generation,
        database_id,
        DatabaseManifestRootRef::new(
            database_manifest_id,
            manifest_generation,
            footer_sha(&database_bytes),
        ),
        CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog_bytes)),
        floor,
        500_000,
        WriterInstanceId::from_bytes(raw(79)).unwrap(),
    )
    .unwrap();

    write_file(root.path(), "CONTROL.0", &encode_control_slot(control));
    write_file(
        root.path(),
        "catalog/catalog-0000000000000003.cat",
        &catalog_bytes,
    );
    write_file(
        root.path(),
        format!("manifests/tables/{table_id}/table-0000000000000007.mft"),
        &table_bytes,
    );
    write_file(
        root.path(),
        "manifests/database-0000000000000007.mft",
        &database_bytes,
    );
    write_file(root.path(), data_reference.relative_path(), &data_bytes);

    let mut wal = WalDriver {
        bytes: HashMap::from([(floor, Vec::new())]),
        ..WalDriver::default()
    };
    let recovered = DatabaseRecovery::new(root.path(), RecoveryLimits::default())
        .recover(&mut wal)
        .unwrap();

    assert_eq!(recovered.control(), control);
    assert_eq!(recovered.physical().artifact_references().len(), 2);
    assert_eq!(recovered.unavailable_indexes().len(), 1);
    assert_eq!(
        recovered.unavailable_indexes()[0].reference(),
        index_reference
    );
    assert!(matches!(
        recovered.unavailable_indexes()[0].reason(),
        UnavailableIndexReason::Missing
    ));
}
