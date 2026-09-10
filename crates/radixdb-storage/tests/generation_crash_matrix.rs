use std::collections::{HashMap, HashSet};

use radixdb_catalog::{
    encode_catalog_pack, CatalogDataType, CatalogEdge, CatalogGeneration as RuntimeCatalog,
    CatalogGraph, CatalogMutation, CatalogMutationSet, CatalogName, CatalogObject, CatalogPackMeta,
    CatalogPayload, ColumnPayload, EdgeKind, ObjectId, ObjectKind, ObjectPrecondition,
    TablePayload,
};
use radixdb_core::DataType;
use radixdb_storage::v6::{
    decode_catalog_wal, encode_catalog_wal_transaction, encode_control_slot,
    encode_database_manifest, encode_table_manifest, replay_catalog_wal, select_control_slots,
    validate_control_generation, ArtifactInspection, ArtifactRef, CatalogGeneration, CatalogId,
    CatalogRef, CatalogRootRef, CatalogWalReplayLimits, CatalogWalTransaction,
    CatalogWalTransactionId, ControlRecord, ControlSlotIndex, DatabaseGeneration, DatabaseId,
    DatabaseManifest, DatabaseManifestRootRef, GenerationCrashPoint, ManifestGeneration,
    ManifestId, ManifestKind, ManifestRef, PhysicalGenerationExpectation, ReachabilityResult,
    ReachabilitySource, SemanticExpectation, TableManifest, TableManifestRef, WalGeneration,
    WalReplayFloor, WriterInstanceId,
};

const SOURCE_NAME: &str = "messages";
const COMMITTED_NAME: &str = "events";

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn footer_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - 32..].try_into().unwrap()
}

fn record_length(bytes: &[u8]) -> usize {
    u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize
}

fn catalog_object(
    id: ObjectId,
    namespace: Option<ObjectId>,
    parent: Option<ObjectId>,
    revision: u64,
    name: &str,
    payload: CatalogPayload,
) -> CatalogObject {
    CatalogObject::new(
        id,
        namespace,
        parent,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        revision,
        payload,
    )
    .unwrap()
}

fn source_catalog(database_id: DatabaseId, catalog_id: CatalogId) -> RuntimeCatalog {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let table = object_id(10);
    let column = object_id(11);
    let graph = CatalogGraph::build(
        vec![
            catalog_object(
                namespace,
                None,
                None,
                1,
                "public",
                CatalogPayload::Namespace(radixdb_catalog::NamespacePayload::new()),
            ),
            catalog_object(
                table,
                Some(namespace),
                Some(namespace),
                1,
                SOURCE_NAME,
                CatalogPayload::Table(
                    TablePayload::new(vec![column], vec![], vec![], None).unwrap(),
                ),
            ),
            catalog_object(
                column,
                Some(namespace),
                Some(table),
                1,
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
            CatalogEdge::new(namespace, table, EdgeKind::Contains, 0),
            CatalogEdge::new(table, column, EdgeKind::Contains, 0),
        ],
    )
    .unwrap();
    RuntimeCatalog::new(
        CatalogPackMeta::new(
            database_id.into_bytes(),
            catalog_id.into_bytes(),
            3,
            500,
            500_000,
        )
        .unwrap(),
        graph,
    )
}

struct GenerationBytes {
    catalog_id: CatalogId,
    catalog: Vec<u8>,
    table_manifest_id: ManifestId,
    table_manifest: Vec<u8>,
    database_manifest_id: ManifestId,
    database_manifest: Vec<u8>,
    control: Vec<u8>,
}

struct Fixture {
    source: GenerationBytes,
    successor: GenerationBytes,
    wal_mutation: Vec<u8>,
    wal_transaction: Vec<u8>,
}

fn fixture() -> Fixture {
    let database_id = DatabaseId::from_bytes(raw(1)).unwrap();
    let source_catalog_id = CatalogId::from_bytes(raw(2)).unwrap();
    let successor_catalog_id = CatalogId::from_bytes(raw(3)).unwrap();
    let source_runtime = source_catalog(database_id, source_catalog_id);
    let mutation_set = CatalogMutationSet::new(
        database_id.into_bytes(),
        source_catalog_id.into_bytes(),
        3,
        vec![CatalogMutation::rename(
            ObjectPrecondition::new(object_id(10), ObjectKind::Table, 1).unwrap(),
            CatalogName::new(COMMITTED_NAME).unwrap(),
        )],
        vec![],
        vec![],
    )
    .unwrap();
    let successor_graph = mutation_set.apply(&source_runtime).unwrap();
    let source_catalog =
        encode_catalog_pack(source_runtime.meta(), source_runtime.graph()).unwrap();
    let successor_meta = CatalogPackMeta::new(
        database_id.into_bytes(),
        successor_catalog_id.into_bytes(),
        4,
        600,
        600_000,
    )
    .unwrap();
    let successor_catalog = encode_catalog_pack(successor_meta, &successor_graph).unwrap();

    let source = generation_bytes(
        database_id,
        source_catalog_id,
        3,
        &source_catalog,
        4,
        7,
        6,
        7,
        ControlSlotIndex::Zero,
        WalReplayFloor::new(WalGeneration::new(5).unwrap(), 500),
    );
    let successor = generation_bytes(
        database_id,
        successor_catalog_id,
        4,
        &successor_catalog,
        5,
        8,
        7,
        8,
        ControlSlotIndex::One,
        WalReplayFloor::new(WalGeneration::new(6).unwrap(), 600),
    );
    let transaction = CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes(raw(20)).unwrap(),
        successor_catalog_id.into_bytes(),
        600,
        600_000,
        mutation_set,
    )
    .unwrap();
    let wal_transaction = encode_catalog_wal_transaction(&transaction).unwrap();
    let mutation_length = record_length(&wal_transaction);
    let wal_mutation = wal_transaction[..mutation_length].to_vec();
    Fixture {
        source,
        successor,
        wal_mutation,
        wal_transaction,
    }
}

#[allow(clippy::too_many_arguments)]
fn generation_bytes(
    database_id: DatabaseId,
    catalog_id: CatalogId,
    catalog_generation: u64,
    catalog: &[u8],
    table_manifest_marker: u8,
    manifest_generation: u64,
    database_manifest_marker: u8,
    database_generation: u64,
    slot: ControlSlotIndex,
    wal_floor: WalReplayFloor,
) -> GenerationBytes {
    let table_id = object_id(10);
    let table_manifest_id = ManifestId::from_bytes(raw(table_manifest_marker)).unwrap();
    let table_manifest = TableManifest::new(
        database_id,
        table_id,
        table_manifest_id,
        ManifestGeneration::new(manifest_generation).unwrap(),
        CatalogGeneration::new(catalog_generation).unwrap(),
        0,
        1,
        vec![],
        database_generation * 100_000,
    )
    .unwrap();
    let table_manifest = encode_table_manifest(&table_manifest).unwrap();
    let table_ref = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest_id,
            ManifestKind::Table,
            ManifestGeneration::new(manifest_generation).unwrap(),
            table_manifest.len() as u64,
            footer_sha(&table_manifest),
        )
        .unwrap(),
    )
    .unwrap();
    let catalog_ref = CatalogRef::new(
        catalog_id,
        CatalogGeneration::new(catalog_generation).unwrap(),
        catalog.len() as u64,
        footer_sha(catalog),
    )
    .unwrap();
    let database_manifest_id = ManifestId::from_bytes(raw(database_manifest_marker)).unwrap();
    let database_manifest = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        DatabaseGeneration::new(database_generation).unwrap(),
        catalog_ref,
        wal_floor,
        database_generation * 100_000,
        vec![table_ref],
        database_generation * 100_000,
    )
    .unwrap();
    let database_manifest = encode_database_manifest(&database_manifest).unwrap();
    let control = ControlRecord::new(
        slot,
        DatabaseGeneration::new(database_generation).unwrap(),
        database_id,
        DatabaseManifestRootRef::new(
            database_manifest_id,
            ManifestGeneration::new(database_generation).unwrap(),
            footer_sha(&database_manifest),
        ),
        CatalogRootRef::new(
            catalog_id,
            CatalogGeneration::new(catalog_generation).unwrap(),
            footer_sha(catalog),
        ),
        wal_floor,
        database_generation * 100_000,
        WriterInstanceId::from_bytes(raw(30)).unwrap(),
    )
    .unwrap();
    GenerationBytes {
        catalog_id,
        catalog: catalog.to_vec(),
        table_manifest_id,
        table_manifest,
        database_manifest_id,
        database_manifest,
        control: encode_control_slot(control).to_vec(),
    }
}

#[derive(Clone)]
struct DurableWorld {
    controls: [Vec<u8>; 2],
    catalogs: HashMap<CatalogId, Vec<u8>>,
    table_manifests: HashMap<ManifestId, Vec<u8>>,
    database_manifests: HashMap<ManifestId, Vec<u8>>,
    wal: Vec<u8>,
}

impl DurableWorld {
    fn source(fixture: &Fixture) -> Self {
        Self {
            controls: [fixture.source.control.clone(), Vec::new()],
            catalogs: HashMap::from([(fixture.source.catalog_id, fixture.source.catalog.clone())]),
            table_manifests: HashMap::from([(
                fixture.source.table_manifest_id,
                fixture.source.table_manifest.clone(),
            )]),
            database_manifests: HashMap::from([(
                fixture.source.database_manifest_id,
                fixture.source.database_manifest.clone(),
            )]),
            wal: Vec::new(),
        }
    }
}

impl ReachabilitySource for DurableWorld {
    fn read_database_manifest(
        &mut self,
        reference: DatabaseManifestRootRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        let bytes = self.database_manifests.get(&reference.id());
        assert!(bytes.is_none_or(|bytes| bytes.len() as u64 <= byte_budget));
        Ok(bytes.cloned())
    }

    fn read_catalog(
        &mut self,
        reference: CatalogRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        let bytes = self.catalogs.get(&reference.id());
        assert!(bytes.is_none_or(|bytes| bytes.len() as u64 <= byte_budget));
        Ok(bytes.cloned())
    }

    fn read_table_manifest(
        &mut self,
        reference: TableManifestRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        let bytes = self.table_manifests.get(&reference.manifest().id());
        assert!(bytes.is_none_or(|bytes| bytes.len() as u64 <= byte_budget));
        Ok(bytes.cloned())
    }

    fn inspect_artifact(
        &mut self,
        _reference: ArtifactRef,
        _allowance: radixdb_storage::v6::ReachabilityAllowance,
    ) -> ReachabilityResult<ArtifactInspection> {
        panic!("empty table manifest cannot request an artifact")
    }
}

struct Injector {
    armed: GenerationCrashPoint,
    hits: u64,
}

impl Injector {
    fn hit(&mut self, point: GenerationCrashPoint) -> Result<(), ()> {
        if point == self.armed {
            self.hits += 1;
            Err(())
        } else {
            Ok(())
        }
    }
}

fn publish_until(
    fixture: &Fixture,
    armed: GenerationCrashPoint,
    uncertain_bytes_survive: bool,
) -> (DurableWorld, u64) {
    use GenerationCrashPoint as P;

    let mut world = DurableWorld::source(fixture);
    let mut injector = Injector { armed, hits: 0 };
    macro_rules! stop {
        ($point:expr) => {
            if injector.hit($point).is_err() {
                return (world, injector.hits);
            }
        };
    }

    stop!(P::WalBeforeRecordWrite);
    world.wal = fixture.wal_mutation.clone();
    stop!(P::WalAfterRecordWriteBeforeSync);
    stop!(P::WalBeforeCommitMarker);
    if uncertain_bytes_survive {
        world.wal = fixture.wal_transaction.clone();
    }
    stop!(P::WalAfterCommitMarkerWriteBeforeSync);
    world.wal = fixture.wal_transaction.clone();
    stop!(P::WalCommitMarkerDurable);
    stop!(P::CatalogRuntimeBeforePublish);
    stop!(P::CatalogRuntimePublished);

    stop!(P::TableManifestAfterBodyBeforeFooter);
    stop!(P::TableManifestAfterFileSync);
    stop!(P::TableManifestAfterRenameBeforeDirSync);
    world.table_manifests.insert(
        fixture.successor.table_manifest_id,
        fixture.successor.table_manifest.clone(),
    );
    stop!(P::TableManifestDirDurable);

    stop!(P::CatalogPackAfterBodyBeforeFooter);
    stop!(P::CatalogPackAfterFileSync);
    stop!(P::CatalogPackAfterRenameBeforeDirSync);
    world.catalogs.insert(
        fixture.successor.catalog_id,
        fixture.successor.catalog.clone(),
    );
    stop!(P::CatalogPackDirDurable);

    stop!(P::WalSuccessorAfterCreateBeforeSync);
    stop!(P::WalSuccessorDurable);

    stop!(P::DatabaseManifestAfterBodyBeforeFooter);
    stop!(P::DatabaseManifestAfterFileSync);
    stop!(P::DatabaseManifestAfterRenameBeforeDirSync);
    world.database_manifests.insert(
        fixture.successor.database_manifest_id,
        fixture.successor.database_manifest.clone(),
    );
    stop!(P::DatabaseManifestDirDurable);

    world.controls[1] = fixture.successor.control[..913].to_vec();
    stop!(P::ControlAfterPartialWrite);
    if uncertain_bytes_survive {
        world.controls[1] = fixture.successor.control.clone();
    }
    stop!(P::ControlAfterWriteBeforeFdatasync);
    world.controls[1] = fixture.successor.control.clone();
    stop!(P::ControlDurable);
    stop!(P::ControlAfterRootDirSync);
    stop!(P::RuntimeGenerationBeforePublish);
    stop!(P::RuntimeGenerationPublished);
    stop!(P::WalBeforeTruncate);
    stop!(P::WalAfterRenameToRetired);
    world.wal.clear();
    stop!(P::WalAfterUnlinkBeforeDirSync);
    stop!(P::WalTruncateDirDurable);
    unreachable!("every CA-20 point must stop the structural publisher")
}

struct Reopened {
    database_generation: u64,
    relation_name: String,
}

fn reopen(world: &DurableWorld) -> Reopened {
    let selected = select_control_slots(&world.controls[0], &world.controls[1], |candidate| {
        let mut source = world.clone();
        validate_control_generation(*candidate, &mut source).is_ok()
    })
    .unwrap();
    let mut source = world.clone();
    let validated = validate_control_generation(selected, &mut source).unwrap();
    let generation = RuntimeCatalog::from_pack(validated.catalog().clone());
    let replay = decode_catalog_wal(&world.wal, CatalogWalReplayLimits::hard()).unwrap();
    let generation = if replay.transactions().iter().any(|transaction| {
        selected
            .wal_replay_floor()
            .starts_after(transaction.commit_lsn())
    }) {
        replay_catalog_wal(&generation, &replay).unwrap()
    } else {
        generation
    };
    Reopened {
        database_generation: selected.database_generation().get(),
        relation_name: generation
            .object(object_id(10))
            .unwrap()
            .name()
            .display()
            .as_str()
            .to_owned(),
    }
}

fn assert_outcome(point: GenerationCrashPoint, uncertain_survives: bool, reopened: &Reopened) {
    match point.physical_expectation() {
        PhysicalGenerationExpectation::Old => assert_eq!(reopened.database_generation, 7),
        PhysicalGenerationExpectation::New => assert_eq!(reopened.database_generation, 8),
        PhysicalGenerationExpectation::OldOrNew => assert_eq!(
            reopened.database_generation,
            if uncertain_survives { 8 } else { 7 }
        ),
    }
    match point.semantic_expectation() {
        SemanticExpectation::Source => assert_eq!(reopened.relation_name, SOURCE_NAME),
        SemanticExpectation::Committed => assert_eq!(reopened.relation_name, COMMITTED_NAME),
        SemanticExpectation::SourceOrCommitted => assert_eq!(
            reopened.relation_name,
            if uncertain_survives {
                COMMITTED_NAME
            } else {
                SOURCE_NAME
            }
        ),
    }
}

#[test]
fn ca20_stable_point_names_are_complete_unique_and_ordered() {
    let expected = [
        "V6_WAL_BEFORE_RECORD_WRITE",
        "V6_WAL_AFTER_RECORD_WRITE_BEFORE_SYNC",
        "V6_WAL_BEFORE_COMMIT_MARKER",
        "V6_WAL_AFTER_COMMIT_MARKER_WRITE_BEFORE_SYNC",
        "V6_WAL_COMMIT_MARKER_DURABLE",
        "V6_CATALOG_RUNTIME_BEFORE_PUBLISH",
        "V6_CATALOG_RUNTIME_PUBLISHED",
        "V6_TABLE_MANIFEST_AFTER_BODY_BEFORE_FOOTER",
        "V6_TABLE_MANIFEST_AFTER_FILE_SYNC",
        "V6_TABLE_MANIFEST_AFTER_RENAME_BEFORE_DIR_SYNC",
        "V6_TABLE_MANIFEST_DIR_DURABLE",
        "V6_CATALOG_PACK_AFTER_BODY_BEFORE_FOOTER",
        "V6_CATALOG_PACK_AFTER_FILE_SYNC",
        "V6_CATALOG_PACK_AFTER_RENAME_BEFORE_DIR_SYNC",
        "V6_CATALOG_PACK_DIR_DURABLE",
        "V6_WAL_SUCCESSOR_AFTER_CREATE_BEFORE_SYNC",
        "V6_WAL_SUCCESSOR_DURABLE",
        "V6_DATABASE_MANIFEST_AFTER_BODY_BEFORE_FOOTER",
        "V6_DATABASE_MANIFEST_AFTER_FILE_SYNC",
        "V6_DATABASE_MANIFEST_AFTER_RENAME_BEFORE_DIR_SYNC",
        "V6_DATABASE_MANIFEST_DIR_DURABLE",
        "V6_CONTROL_AFTER_PARTIAL_WRITE",
        "V6_CONTROL_AFTER_WRITE_BEFORE_FDATASYNC",
        "V6_CONTROL_DURABLE",
        "V6_CONTROL_AFTER_ROOT_DIR_SYNC",
        "V6_RUNTIME_GENERATION_BEFORE_PUBLISH",
        "V6_RUNTIME_GENERATION_PUBLISHED",
        "V6_WAL_BEFORE_TRUNCATE",
        "V6_WAL_AFTER_RENAME_TO_RETIRED",
        "V6_WAL_AFTER_UNLINK_BEFORE_DIR_SYNC",
        "V6_WAL_TRUNCATE_DIR_DURABLE",
    ];
    let actual =
        GenerationCrashPoint::STRUCTURAL_PUBLICATION_POINTS.map(GenerationCrashPoint::name);
    assert_eq!(actual, expected);
    assert_eq!(actual.into_iter().collect::<HashSet<_>>().len(), 31);
}

#[test]
fn lifecycle_publication_point_names_are_complete_unique_and_ordered() {
    let expected = [
        "V6_STAGE_OWNER_BEFORE_WRITE",
        "V6_STAGE_OWNER_AFTER_SYNC",
        "V6_DATA_AFTER_BODY_BEFORE_FOOTER",
        "V6_DATA_AFTER_FOOTER_BEFORE_SYNC",
        "V6_DATA_AFTER_FILE_SYNC",
        "V6_DATA_AFTER_FINAL_RENAME_BEFORE_DIR_SYNC",
        "V6_DATA_FINAL_DIR_DURABLE",
        "V6_INDEX_AFTER_BODY_BEFORE_FOOTER",
        "V6_INDEX_AFTER_FOOTER_BEFORE_SYNC",
        "V6_INDEX_AFTER_FILE_SYNC",
        "V6_INDEX_AFTER_FINAL_RENAME_BEFORE_DIR_SYNC",
        "V6_INDEX_FINAL_DIR_DURABLE",
        "V6_WAL_BEFORE_RECORD_WRITE",
        "V6_WAL_AFTER_RECORD_WRITE_BEFORE_SYNC",
        "V6_WAL_BEFORE_COMMIT_MARKER",
        "V6_WAL_AFTER_COMMIT_MARKER_WRITE_BEFORE_SYNC",
        "V6_WAL_COMMIT_MARKER_DURABLE",
        "V6_CATALOG_RUNTIME_BEFORE_PUBLISH",
        "V6_CATALOG_RUNTIME_PUBLISHED",
        "V6_TABLE_MANIFEST_AFTER_BODY_BEFORE_FOOTER",
        "V6_TABLE_MANIFEST_AFTER_FILE_SYNC",
        "V6_TABLE_MANIFEST_AFTER_RENAME_BEFORE_DIR_SYNC",
        "V6_TABLE_MANIFEST_DIR_DURABLE",
        "V6_CATALOG_PACK_AFTER_BODY_BEFORE_FOOTER",
        "V6_CATALOG_PACK_AFTER_FILE_SYNC",
        "V6_CATALOG_PACK_AFTER_RENAME_BEFORE_DIR_SYNC",
        "V6_CATALOG_PACK_DIR_DURABLE",
        "V6_WAL_SUCCESSOR_AFTER_CREATE_BEFORE_SYNC",
        "V6_WAL_SUCCESSOR_DURABLE",
        "V6_DATABASE_MANIFEST_AFTER_BODY_BEFORE_FOOTER",
        "V6_DATABASE_MANIFEST_AFTER_FILE_SYNC",
        "V6_DATABASE_MANIFEST_AFTER_RENAME_BEFORE_DIR_SYNC",
        "V6_DATABASE_MANIFEST_DIR_DURABLE",
        "V6_CONTROL_AFTER_PARTIAL_WRITE",
        "V6_CONTROL_AFTER_WRITE_BEFORE_FDATASYNC",
        "V6_CONTROL_DURABLE",
        "V6_CONTROL_AFTER_ROOT_DIR_SYNC",
        "V6_RUNTIME_GENERATION_BEFORE_PUBLISH",
        "V6_RUNTIME_GENERATION_PUBLISHED",
        "V6_WAL_BEFORE_TRUNCATE",
        "V6_WAL_AFTER_RENAME_TO_RETIRED",
        "V6_WAL_AFTER_UNLINK_BEFORE_DIR_SYNC",
        "V6_WAL_TRUNCATE_DIR_DURABLE",
    ];
    let actual = GenerationCrashPoint::LIFECYCLE_PUBLICATION_POINTS.map(GenerationCrashPoint::name);
    assert_eq!(actual, expected);
    assert_eq!(actual.into_iter().collect::<HashSet<_>>().len(), 43);

    let gc = GenerationCrashPoint::LIFECYCLE_GC_POINTS.map(GenerationCrashPoint::name);
    assert_eq!(gc.into_iter().collect::<HashSet<_>>().len(), 10);
    assert!(actual.into_iter().all(|name| !gc.contains(&name)));
}

#[test]
fn snapshot_publication_point_names_are_complete_unique_and_disjoint() {
    let expected = [
        "V6_SNAPSHOT_MEMBER_AFTER_WRITE_BEFORE_SYNC",
        "V6_SNAPSHOT_MEMBER_DURABLE",
        "V6_SNAPSHOT_MANIFEST_AFTER_SYNC_BEFORE_RENAME",
        "V6_SNAPSHOT_MANIFEST_AFTER_RENAME_BEFORE_DIR_SYNC",
        "V6_SNAPSHOT_MANIFEST_DIR_DURABLE",
    ];
    let snapshot =
        GenerationCrashPoint::SNAPSHOT_PUBLICATION_POINTS.map(GenerationCrashPoint::name);
    assert_eq!(snapshot, expected);
    assert_eq!(snapshot.into_iter().collect::<HashSet<_>>().len(), 5);
    assert!(GenerationCrashPoint::LIFECYCLE_PUBLICATION_POINTS
        .into_iter()
        .chain(GenerationCrashPoint::LIFECYCLE_GC_POINTS)
        .map(GenerationCrashPoint::name)
        .all(|name| !snapshot.contains(&name)));

    let expected_restore = [
        "V6_RESTORE_STAGE_VALIDATED",
        "V6_RESTORE_AFTER_ROOT_RENAME_BEFORE_PARENT_SYNC",
        "V6_RESTORE_PARENT_DIR_DURABLE",
    ];
    let restore = GenerationCrashPoint::RESTORE_PUBLICATION_POINTS.map(GenerationCrashPoint::name);
    assert_eq!(restore, expected_restore);
    assert_eq!(restore.into_iter().collect::<HashSet<_>>().len(), 3);
    assert!(GenerationCrashPoint::LIFECYCLE_PUBLICATION_POINTS
        .into_iter()
        .chain(GenerationCrashPoint::LIFECYCLE_GC_POINTS)
        .chain(GenerationCrashPoint::SNAPSHOT_PUBLICATION_POINTS)
        .map(GenerationCrashPoint::name)
        .all(|name| !restore.contains(&name)));
}

#[test]
fn every_ca20_boundary_reopens_only_documented_complete_state() {
    let fixture = fixture();
    for point in GenerationCrashPoint::STRUCTURAL_PUBLICATION_POINTS {
        let durability_variants: &[bool] =
            match (point.physical_expectation(), point.semantic_expectation()) {
                (PhysicalGenerationExpectation::OldOrNew, _)
                | (_, SemanticExpectation::SourceOrCommitted) => &[false, true],
                _ => &[false],
            };
        for uncertain_survives in durability_variants {
            // Structural ReturnIoError and abrupt-stop modes share the same
            // durable-byte oracle at CA-20. CA-60 repeats this with real child
            // processes and filesystem barriers.
            for _mode in ["return-error", "abrupt-stop"] {
                let (world, hit_count) = publish_until(&fixture, point, *uncertain_survives);
                assert_eq!(hit_count, 1, "{} was not hit exactly once", point.name());
                let first = reopen(&world);
                assert_outcome(point, *uncertain_survives, &first);
                let second = reopen(&world);
                assert_eq!(first.database_generation, second.database_generation);
                assert_eq!(first.relation_name, second.relation_name);
            }
        }
    }
}

#[test]
fn every_control_truncation_and_single_byte_flip_falls_back_with_wal_replay() {
    let fixture = fixture();
    let (complete, hit_count) =
        publish_until(&fixture, GenerationCrashPoint::WalBeforeTruncate, false);
    assert_eq!(hit_count, 1);

    for length in 0..fixture.successor.control.len() {
        let mut world = complete.clone();
        world.controls[1].truncate(length);
        let reopened = reopen(&world);
        assert_eq!(reopened.database_generation, 7, "truncate={length}");
        assert_eq!(reopened.relation_name, COMMITTED_NAME, "truncate={length}");
    }
    for offset in 0..fixture.successor.control.len() {
        let mut world = complete.clone();
        world.controls[1][offset] ^= 1;
        let reopened = reopen(&world);
        assert_eq!(reopened.database_generation, 7, "flip={offset}");
        assert_eq!(reopened.relation_name, COMMITTED_NAME, "flip={offset}");
    }
}

#[test]
fn missing_or_corrupt_new_generation_member_falls_back_to_complete_old_root() {
    let fixture = fixture();
    let (complete, hit_count) =
        publish_until(&fixture, GenerationCrashPoint::WalBeforeTruncate, false);
    assert_eq!(hit_count, 1);

    let mut variants = Vec::new();
    let mut missing_database = complete.clone();
    missing_database
        .database_manifests
        .remove(&fixture.successor.database_manifest_id);
    variants.push(("missing database manifest", missing_database));

    let mut corrupt_database = complete.clone();
    corrupt_database
        .database_manifests
        .get_mut(&fixture.successor.database_manifest_id)
        .unwrap()[256] ^= 1;
    variants.push(("corrupt database manifest", corrupt_database));

    let mut missing_catalog = complete.clone();
    missing_catalog
        .catalogs
        .remove(&fixture.successor.catalog_id);
    variants.push(("missing catalog", missing_catalog));

    let mut corrupt_catalog = complete.clone();
    corrupt_catalog
        .catalogs
        .get_mut(&fixture.successor.catalog_id)
        .unwrap()[256] ^= 1;
    variants.push(("corrupt catalog", corrupt_catalog));

    let mut missing_table = complete.clone();
    missing_table
        .table_manifests
        .remove(&fixture.successor.table_manifest_id);
    variants.push(("missing table manifest", missing_table));

    let mut corrupt_table = complete;
    corrupt_table
        .table_manifests
        .get_mut(&fixture.successor.table_manifest_id)
        .unwrap()[256] ^= 1;
    variants.push(("corrupt table manifest", corrupt_table));

    for (case, world) in variants {
        let reopened = reopen(&world);
        assert_eq!(reopened.database_generation, 7, "{case}");
        assert_eq!(reopened.relation_name, COMMITTED_NAME, "{case}");
    }
}

#[test]
fn every_transaction_prefix_before_complete_marker_is_invisible() {
    let fixture = fixture();
    for length in 0..fixture.wal_transaction.len() {
        let replay = decode_catalog_wal(
            &fixture.wal_transaction[..length],
            CatalogWalReplayLimits::hard(),
        )
        .unwrap();
        assert!(replay.transactions().is_empty(), "prefix={length}");
        assert_eq!(replay.committed_bytes(), 0, "prefix={length}");
        assert_eq!(replay.incomplete_tail_bytes(), length, "prefix={length}");
    }
}
