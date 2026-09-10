use std::path::{Path, PathBuf};
#[cfg(feature = "test-failpoints")]
use std::process::{Command, Stdio};

use radixdb_catalog::{
    encode_catalog_pack, CatalogDataType, CatalogEdge,
    CatalogGeneration as RuntimeCatalogGeneration, CatalogGraph, CatalogName, CatalogObject,
    CatalogPackMeta, CatalogPayload, ColumnPayload, EdgeKind, ObjectId, TablePayload,
};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    commit_snapshot_manifest, decode_data_artifact_layout, encode_data_artifact,
    encode_database_manifest, encode_index_artifact, encode_table_manifest, restore_snapshot,
    ArtifactId, CatalogGeneration, CatalogId, CatalogRef, CatalogRootRef, ControlRecord,
    ControlSlotIndex, DataArtifactHeader, DataArtifactInput, DataBlockSpec, DataColumnSpec,
    DataPhysicalCodec, DataValueEncoding, DataWalRecoveryContext, DataWalRecoveryOutcome,
    DatabaseGeneration, DatabaseId, DatabaseManifest, DatabaseManifestRootRef, DatabaseRecovery,
    FormatResult, IndexAcceleratorKind, IndexAcceleratorSpec, IndexArtifactHeader,
    IndexArtifactInput, IndexKeyColumn, IndexNullsOrder, IndexPageCodec, IndexPageSpec,
    IndexSectionKind, IndexSectionSpec, IndexSortDirection, ManifestGeneration, ManifestId,
    ManifestKind, ManifestRef, PhysicalGenerationSnapshot, RecoveryLimits, SegmentDescriptor,
    SegmentId, SegmentKind, SnapshotId, SnapshotIndexPolicy, SnapshotManifest, SnapshotMember,
    TableManifest, TableManifestRef, UnavailableIndexReason, WalGeneration, WalRecovery,
    WalReplayFloor, WriterInstanceId,
};
#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode};

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

struct RestoreFixture {
    _root: tempfile::TempDir,
    snapshot: PathBuf,
    target: PathBuf,
    manifest: SnapshotManifest,
    table_id: ObjectId,
    index_reference: radixdb_storage::v6::ArtifactRef,
}

impl RestoreFixture {
    fn new(index_policy: SnapshotIndexPolicy) -> Self {
        Self::with_table_catalog_generation(index_policy, CatalogGeneration::new(4).unwrap())
    }

    fn with_table_catalog_generation(
        index_policy: SnapshotIndexPolicy,
        table_catalog_generation: CatalogGeneration,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let snapshot_id = SnapshotId::from_bytes(raw(0x11)).unwrap();
        let snapshot = root.path().join(snapshot_id.to_string());
        let target = root.path().join("restored-database");
        std::fs::create_dir(&snapshot).unwrap();

        let database_id = DatabaseId::from_bytes(raw(0x12)).unwrap();
        let database_generation = DatabaseGeneration::new(9).unwrap();
        let catalog_generation = CatalogGeneration::new(4).unwrap();
        let catalog_id = CatalogId::from_bytes(raw(0x13)).unwrap();
        let table_id = object_id(0x14);
        let column_id = object_id(0x15);
        let logical_index_id = object_id(0x16);
        let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
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
        let catalog = RuntimeCatalogGeneration::new(
            CatalogPackMeta::new(
                database_id.into_bytes(),
                catalog_id.into_bytes(),
                catalog_generation.get(),
                700,
                700_000,
            )
            .unwrap(),
            graph,
        );
        let catalog_bytes = encode_catalog_pack(catalog.meta(), catalog.graph()).unwrap();
        let catalog_reference = CatalogRef::new(
            catalog_id,
            catalog_generation,
            catalog_bytes.len() as u64,
            footer_sha(&catalog_bytes),
        )
        .unwrap();

        let segment_id = SegmentId::from_bytes(raw(0x17)).unwrap();
        let column = DataColumnSpec::new(
            column_id,
            CatalogDataType::scalar(DataType::Integer).unwrap(),
            false,
        );
        let data_header = DataArtifactHeader::new(
            ArtifactId::from_bytes(raw(0x18)).unwrap(),
            database_id,
            table_id,
            segment_id,
            database_generation,
            table_catalog_generation,
            1,
            3,
            3,
            1,
            1,
            SegmentKind::Rows,
            700_000,
        )
        .unwrap();
        let data_input = DataArtifactInput::new(
            data_header,
            vec![column],
            vec![],
            vec![
                DataBlockSpec::row_ids(0, &[101, 102, 103], DataPhysicalCodec::None).unwrap(),
                DataBlockSpec::column(
                    0,
                    0,
                    column,
                    &[Value::integer(1), Value::integer(2), Value::integer(3)],
                    DataValueEncoding::Plain,
                    DataPhysicalCodec::None,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let (data_bytes, data_reference) = encode_data_artifact(&data_input).unwrap();
        let data_layout = decode_data_artifact_layout(&data_bytes, data_reference).unwrap();

        let key = IndexKeyColumn::new(
            column_id,
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        );
        let accelerator = IndexAcceleratorSpec::new(
            logical_index_id,
            IndexAcceleratorKind::Exact,
            false,
            false,
            [0x19; 32],
            vec![key],
            3,
            vec![IndexSectionSpec::pages(
                IndexSectionKind::ExactPages,
                vec![IndexPageSpec::new(
                    b"three exact postings".to_vec(),
                    IndexPageCodec::None,
                    3,
                    1,
                    3,
                )
                .unwrap()],
            )
            .unwrap()],
        )
        .unwrap();
        let index_header = IndexArtifactHeader::for_data(
            ArtifactId::from_bytes(raw(0x1a)).unwrap(),
            database_generation,
            table_catalog_generation,
            &data_layout,
        )
        .unwrap();
        let (index_bytes, index_reference) = encode_index_artifact(
            &IndexArtifactInput::new(index_header, &data_layout, vec![accelerator]).unwrap(),
        )
        .unwrap();

        let table_manifest_id = ManifestId::from_bytes(raw(0x1b)).unwrap();
        let manifest_generation = ManifestGeneration::new(9).unwrap();
        let table = TableManifest::new(
            database_id,
            table_id,
            table_manifest_id,
            manifest_generation,
            table_catalog_generation,
            103,
            2,
            vec![SegmentDescriptor::new(
                segment_id,
                SegmentKind::Rows,
                1,
                3,
                3,
                101,
                103,
                data_reference,
                Some(index_reference),
            )
            .unwrap()],
            700_000,
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

        let floor = WalReplayFloor::new(WalGeneration::new(7).unwrap(), 700);
        let database_manifest_id = ManifestId::from_bytes(raw(0x1c)).unwrap();
        let database = DatabaseManifest::new(
            database_id,
            database_manifest_id,
            database_generation,
            catalog_reference,
            floor,
            10_000,
            vec![table_reference],
            700_000,
        )
        .unwrap();
        let database_bytes = encode_database_manifest(&database).unwrap();
        let database_root = DatabaseManifestRootRef::new(
            database_manifest_id,
            manifest_generation,
            footer_sha(&database_bytes),
        );
        let catalog_root =
            CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog_bytes));
        let control = ControlRecord::new(
            ControlSlotIndex::Zero,
            database_generation,
            database_id,
            database_root,
            catalog_root,
            floor,
            700_000,
            WriterInstanceId::from_bytes(raw(0x1d)).unwrap(),
        )
        .unwrap();
        let generation = PhysicalGenerationSnapshot::new(control, database, vec![table]).unwrap();
        let wal_bytes = vec![0_u8];
        let wal = SnapshotMember::wal(
            database_id,
            floor.generation(),
            wal_bytes.len() as u64,
            radixdb_core::sha256_digest(&wal_bytes),
        )
        .unwrap();
        let manifest = SnapshotManifest::from_generation(
            snapshot_id,
            &generation,
            database_bytes.len() as u64,
            vec![wal],
            index_policy,
            800_000,
        )
        .unwrap();

        write_snapshot_member(
            &snapshot,
            manifest.database_manifest().id().into_bytes(),
            &manifest,
            &database_bytes,
        );
        write_snapshot_member(
            &snapshot,
            catalog_id.into_bytes(),
            &manifest,
            &catalog_bytes,
        );
        write_snapshot_member(
            &snapshot,
            table_manifest_id.into_bytes(),
            &manifest,
            &table_bytes,
        );
        write_snapshot_member(
            &snapshot,
            data_reference.id().into_bytes(),
            &manifest,
            &data_bytes,
        );
        if index_policy == SnapshotIndexPolicy::Include {
            write_snapshot_member(
                &snapshot,
                index_reference.id().into_bytes(),
                &manifest,
                &index_bytes,
            );
        }
        write_snapshot_member(&snapshot, wal.id(), &manifest, &wal_bytes);
        commit_snapshot_manifest(&snapshot, &manifest).unwrap();

        Self {
            _root: root,
            snapshot,
            target,
            manifest,
            table_id,
            index_reference,
        }
    }

    fn source_bytes(&self) -> Vec<(PathBuf, Vec<u8>)> {
        let mut paths = self
            .manifest
            .members()
            .iter()
            .map(|member| member.relative_path())
            .collect::<Vec<_>>();
        paths.push(PathBuf::from("SNAPSHOT.mft"));
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let bytes = std::fs::read(self.snapshot.join(&path)).unwrap();
                (path, bytes)
            })
            .collect()
    }
}

fn write_snapshot_member(
    snapshot: &Path,
    identity: [u8; 16],
    manifest: &SnapshotManifest,
    bytes: &[u8],
) {
    let member = manifest
        .members()
        .iter()
        .find(|member| member.id() == identity)
        .unwrap();
    let path = snapshot.join(member.relative_path());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

struct WalDriver {
    root: PathBuf,
    table_id: ObjectId,
}

impl WalRecovery for WalDriver {
    type State = ();

    fn read_catalog_transactions(
        &mut self,
        floor: WalReplayFloor,
        byte_budget: u64,
    ) -> FormatResult<Vec<u8>> {
        let bytes = std::fs::read(
            self.root
                .join("wal")
                .join(format!("wal-{:016x}.log", floor.generation().get())),
        )
        .unwrap();
        assert!(bytes.len() as u64 <= byte_budget);
        Ok(bytes)
    }

    fn replay_data(
        &mut self,
        context: DataWalRecoveryContext<'_>,
    ) -> FormatResult<DataWalRecoveryOutcome<Self::State>> {
        assert_eq!(context.root(), self.root);
        Ok(DataWalRecoveryOutcome::new(
            (),
            context.floor().lsn(),
            0,
            0,
            0,
            vec![self.table_id],
        ))
    }
}

fn recover(fixture: &RestoreFixture) -> usize {
    let mut wal = WalDriver {
        root: fixture.target.clone(),
        table_id: fixture.table_id,
    };
    DatabaseRecovery::new(&fixture.target, RecoveryLimits::default())
        .recover(&mut wal)
        .unwrap()
        .unavailable_indexes()
        .len()
}

#[test]
fn restore_publishes_one_complete_generation_without_mutating_snapshot() {
    for (policy, unavailable) in [
        (SnapshotIndexPolicy::Include, 0),
        (SnapshotIndexPolicy::OmitRebuildable, 1),
    ] {
        let fixture = RestoreFixture::new(policy);
        let source_before = fixture.source_bytes();
        let outcome = restore_snapshot(
            &fixture.snapshot,
            &fixture.target,
            WriterInstanceId::from_bytes(raw(0x31)).unwrap(),
            radixdb_storage::v6::ReachabilityLimits::default(),
        )
        .unwrap();
        assert_eq!(outcome.target_root(), fixture.target);
        assert_eq!(outcome.snapshot_id(), fixture.manifest.snapshot_id());
        assert_eq!(outcome.control().database_generation().get(), 9);
        assert_eq!(outcome.unavailable_indexes().len(), unavailable);
        assert_eq!(recover(&fixture), unavailable);
        if unavailable == 1 {
            assert_eq!(
                outcome.unavailable_indexes()[0].reference(),
                fixture.index_reference
            );
            assert!(matches!(
                outcome.unavailable_indexes()[0].reason(),
                UnavailableIndexReason::Missing
            ));
        }
        assert_eq!(source_before, fixture.source_bytes());
    }
}

#[test]
fn restore_accepts_a_table_local_manifest_older_than_the_selected_catalog() {
    let fixture = RestoreFixture::with_table_catalog_generation(
        SnapshotIndexPolicy::Include,
        CatalogGeneration::new(3).unwrap(),
    );
    let outcome = restore_snapshot(
        &fixture.snapshot,
        &fixture.target,
        WriterInstanceId::from_bytes(raw(0x47)).unwrap(),
        radixdb_storage::v6::ReachabilityLimits::default(),
    )
    .unwrap();

    assert_eq!(outcome.control().catalog().generation().get(), 4);
    assert_eq!(recover(&fixture), 0);
}

#[test]
fn existing_target_is_rejected_before_any_restore_mutation() {
    let fixture = RestoreFixture::new(SnapshotIndexPolicy::Include);
    std::fs::create_dir(&fixture.target).unwrap();
    std::fs::write(fixture.target.join("sentinel"), b"keep").unwrap();

    assert!(restore_snapshot(
        &fixture.snapshot,
        &fixture.target,
        WriterInstanceId::from_bytes(raw(0x32)).unwrap(),
        radixdb_storage::v6::ReachabilityLimits::default(),
    )
    .is_err());
    assert_eq!(
        std::fs::read(fixture.target.join("sentinel")).unwrap(),
        b"keep"
    );
    assert!(!fixture
        .target
        .parent()
        .unwrap()
        .join(format!(".restore-{}", fixture.manifest.snapshot_id()))
        .exists());
}

#[test]
fn missing_required_member_never_creates_a_restore_target() {
    let fixture = RestoreFixture::new(SnapshotIndexPolicy::OmitRebuildable);
    let data = fixture
        .manifest
        .members()
        .iter()
        .find(|member| member.kind() == radixdb_storage::v6::SnapshotMemberKind::Data)
        .unwrap();
    std::fs::remove_file(fixture.snapshot.join(data.relative_path())).unwrap();

    assert!(restore_snapshot(
        &fixture.snapshot,
        &fixture.target,
        WriterInstanceId::from_bytes(raw(0x33)).unwrap(),
        radixdb_storage::v6::ReachabilityLimits::default(),
    )
    .is_err());
    assert!(!fixture.target.exists());
}

#[test]
fn metadata_budget_exhaustion_never_publishes_a_restore_target() {
    let fixture = RestoreFixture::new(SnapshotIndexPolicy::Include);
    let result = restore_snapshot(
        &fixture.snapshot,
        &fixture.target,
        WriterInstanceId::from_bytes(raw(0x46)).unwrap(),
        radixdb_storage::v6::ReachabilityLimits::new(100, 512).unwrap(),
    );

    assert!(matches!(
        result,
        Err(radixdb_storage::v6::FormatError::MetadataOpenLimitExceeded {
            field: "accounted bytes",
            actual,
            limit: 512,
        }) if actual > 512
    ));
    assert!(!fixture.target.exists());
    assert!(!fixture
        .target
        .parent()
        .unwrap()
        .join(format!(".restore-{}", fixture.manifest.snapshot_id()))
        .exists());
}

#[test]
fn member_outside_the_reachable_graph_is_rejected_before_staging() {
    let mut fixture = RestoreFixture::new(SnapshotIndexPolicy::OmitRebuildable);
    let data = fixture
        .manifest
        .members()
        .iter()
        .find(|member| member.kind() == radixdb_storage::v6::SnapshotMemberKind::Data)
        .copied()
        .unwrap();
    let data_bytes = std::fs::read(fixture.snapshot.join(data.relative_path())).unwrap();
    let extra = SnapshotMember::from_persisted(
        radixdb_storage::v6::SnapshotMemberKind::Data,
        data.format_version(),
        0,
        raw(0x44),
        data.generation(),
        data.byte_length(),
        data.body_sha256(),
        0x44,
        1,
    )
    .unwrap();
    let mut members = fixture.manifest.members().to_vec();
    members.push(extra);
    fixture.manifest = SnapshotManifest::new(
        fixture.manifest.snapshot_id(),
        fixture.manifest.database_id(),
        fixture.manifest.database_generation(),
        fixture.manifest.database_manifest(),
        fixture.manifest.catalog(),
        members,
        fixture.manifest.created_unix_ns(),
    )
    .unwrap();
    std::fs::remove_file(fixture.snapshot.join("SNAPSHOT.mft")).unwrap();
    write_snapshot_member(
        &fixture.snapshot,
        extra.id(),
        &fixture.manifest,
        &data_bytes,
    );
    commit_snapshot_manifest(&fixture.snapshot, &fixture.manifest).unwrap();

    assert!(restore_snapshot(
        &fixture.snapshot,
        &fixture.target,
        WriterInstanceId::from_bytes(raw(0x45)).unwrap(),
        radixdb_storage::v6::ReachabilityLimits::default(),
    )
    .is_err());
    assert!(!fixture.target.exists());
    assert!(!fixture
        .target
        .parent()
        .unwrap()
        .join(format!(".restore-{}", fixture.manifest.snapshot_id()))
        .exists());
}

#[cfg(feature = "test-failpoints")]
#[test]
fn restore_io_error_matrix_exposes_only_absent_or_complete_targets() {
    for point in GenerationCrashPoint::RESTORE_PUBLICATION_POINTS {
        let fixture = RestoreFixture::new(SnapshotIndexPolicy::OmitRebuildable);
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        assert!(restore_snapshot(
            &fixture.snapshot,
            &fixture.target,
            WriterInstanceId::from_bytes(raw(0x34)).unwrap(),
            radixdb_storage::v6::ReachabilityLimits::default(),
        )
        .is_err());
        assert_eq!(guard.hit_count(), 1, "point={}", point.name());
        drop(guard);
        if fixture.target.exists() {
            assert_eq!(recover(&fixture), 1);
        } else {
            assert_eq!(point, GenerationCrashPoint::RestoreStageValidated);
        }
    }
}

#[cfg(feature = "test-failpoints")]
#[test]
#[ignore = "child process entrypoint for the physical restore abort matrix"]
fn snapshot_restore_process_abort_child() {
    let (Some(snapshot), Some(target)) = (
        std::env::var_os("RADIXDB_RESTORE_SNAPSHOT"),
        std::env::var_os("RADIXDB_RESTORE_TARGET"),
    ) else {
        return;
    };
    restore_snapshot(
        snapshot,
        target,
        WriterInstanceId::from_bytes(raw(0x35)).unwrap(),
        radixdb_storage::v6::ReachabilityLimits::default(),
    )
    .unwrap();
}

#[cfg(feature = "test-failpoints")]
#[test]
fn restore_process_abort_matrix_exposes_only_absent_or_recoverable_targets() {
    let executable = std::env::current_exe().unwrap();
    for point in GenerationCrashPoint::RESTORE_PUBLICATION_POINTS {
        let fixture = RestoreFixture::new(SnapshotIndexPolicy::OmitRebuildable);
        let evidence = fixture.snapshot.join("restore-fault.ready");
        let status = Command::new(&executable)
            .arg("--ignored")
            .arg("--exact")
            .arg("snapshot_restore_process_abort_child")
            .env("RADIXDB_RESTORE_SNAPSHOT", &fixture.snapshot)
            .env("RADIXDB_RESTORE_TARGET", &fixture.target)
            .env("RADIXDB_GENERATION_FAULT_POINT", point.name())
            .env("RADIXDB_GENERATION_FAULT_READY", &evidence)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success(), "point={}", point.name());
        assert_eq!(
            std::fs::read_to_string(&evidence).unwrap().trim(),
            point.name()
        );
        if fixture.target.exists() {
            assert_eq!(recover(&fixture), 1);
        } else {
            assert_eq!(point, GenerationCrashPoint::RestoreStageValidated);
        }
    }
}
