use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(feature = "test-failpoints")]
use std::process::Command;

use radixdb_catalog::{
    encode_catalog_pack, CatalogGraph, CatalogName, CatalogObject, CatalogPackMeta, CatalogPayload,
    NamespacePayload,
};
use radixdb_storage::v6::{
    decode_control_slot, encode_control_slot, encode_database_manifest, ArtifactBuildKind,
    ArtifactBuildLease, CatalogGeneration, CatalogId, CatalogRef, CatalogRootRef, ControlRecord,
    ControlSlotIndex, DatabaseGeneration, DatabaseId, DatabaseManifest, DatabaseManifestRootRef,
    FormatError, FrozenCheckpoint, ManifestGeneration, ManifestId, PhysicalGenerationPublisher,
    PhysicalGenerationSnapshot, StagedArtifactSet, StagingDiscoveryLimits, WalGeneration,
    WalReplayFloor, WalRetirementStatus, WriterInstanceId,
};
#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{
    DataWalRecoveryContext, DataWalRecoveryOutcome, DatabaseRecovery, FormatResult,
    GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode, RecoveryLimits, WalRecovery,
};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn footer_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - 32..].try_into().unwrap()
}

fn catalog_bytes(
    database_id: DatabaseId,
    catalog_id: CatalogId,
    generation: CatalogGeneration,
) -> Vec<u8> {
    let namespace = CatalogObject::new(
        radixdb_catalog::ObjectId::BOOTSTRAP_NAMESPACE,
        None,
        None,
        radixdb_catalog::ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("public").unwrap(),
        1,
        CatalogPayload::Namespace(NamespacePayload::new()),
    )
    .unwrap();
    let graph = CatalogGraph::build(vec![namespace], vec![]).unwrap();
    encode_catalog_pack(
        CatalogPackMeta::new(
            database_id.into_bytes(),
            catalog_id.into_bytes(),
            generation.get(),
            100,
            1_000,
        )
        .unwrap(),
        &graph,
    )
    .unwrap()
}

fn catalog_path(reference: CatalogRef) -> PathBuf {
    PathBuf::from("catalog").join(format!("catalog-{:016x}.cat", reference.generation().get()))
}

fn database_manifest_path(reference: DatabaseManifestRootRef) -> PathBuf {
    PathBuf::from("manifests").join(format!(
        "database-{:016x}.mft",
        reference.generation().get()
    ))
}

fn wal_path(generation: WalGeneration) -> PathBuf {
    PathBuf::from("wal").join(format!("wal-{:016x}.log", generation.get()))
}

fn write_file(root: &Path, relative: &Path, bytes: &[u8]) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

struct CheckpointFixture {
    root: tempfile::TempDir,
    publisher: PhysicalGenerationPublisher,
    expected_control: ControlRecord,
    target_control: ControlRecord,
    target_database_path: PathBuf,
    staging: radixdb_storage::v6::CompleteStagingSet,
    build: ArtifactBuildLease,
    obsolete_wal: WalGeneration,
    retained_wal: WalGeneration,
    successor_wal: WalGeneration,
}

impl CheckpointFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        for directory in ["wal", "catalog", "manifests", "staging"] {
            std::fs::create_dir(root.path().join(directory)).unwrap();
        }

        let database_id = DatabaseId::from_bytes(raw(0x11)).unwrap();
        let catalog_id = CatalogId::from_bytes(raw(0x12)).unwrap();
        let catalog_generation = CatalogGeneration::new(1).unwrap();
        let catalog_bytes = catalog_bytes(database_id, catalog_id, catalog_generation);
        let catalog_reference = CatalogRef::new(
            catalog_id,
            catalog_generation,
            catalog_bytes.len() as u64,
            footer_sha(&catalog_bytes),
        )
        .unwrap();
        write_file(
            root.path(),
            &catalog_path(catalog_reference),
            &catalog_bytes,
        );

        let expected_generation = DatabaseGeneration::new(1).unwrap();
        let retained_wal = WalGeneration::new(2).unwrap();
        let expected_floor = WalReplayFloor::new(retained_wal, 100);
        let expected_manifest = DatabaseManifest::new(
            database_id,
            ManifestId::from_bytes(raw(0x13)).unwrap(),
            expected_generation,
            catalog_reference,
            expected_floor,
            0,
            vec![],
            1_000,
        )
        .unwrap();
        let expected_database_bytes = encode_database_manifest(&expected_manifest).unwrap();
        let expected_database_reference = DatabaseManifestRootRef::new(
            expected_manifest.manifest_id(),
            ManifestGeneration::new(expected_generation.get()).unwrap(),
            footer_sha(&expected_database_bytes),
        );
        let expected_control = ControlRecord::new(
            ControlSlotIndex::Zero,
            expected_generation,
            database_id,
            expected_database_reference,
            CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog_bytes)),
            expected_floor,
            1_000,
            WriterInstanceId::from_bytes(raw(0x14)).unwrap(),
        )
        .unwrap();
        write_file(
            root.path(),
            &database_manifest_path(expected_database_reference),
            &expected_database_bytes,
        );
        write_file(
            root.path(),
            Path::new("CONTROL.0"),
            &encode_control_slot(expected_control),
        );
        write_file(root.path(), &wal_path(retained_wal), b"retained WAL");
        let obsolete_wal = WalGeneration::new(1).unwrap();
        write_file(root.path(), &wal_path(obsolete_wal), b"obsolete WAL");

        let target_generation = expected_generation.checked_next().unwrap();
        let successor_wal = retained_wal.checked_next().unwrap();
        let target_floor = WalReplayFloor::new(successor_wal, 200);
        let target_manifest = DatabaseManifest::new(
            database_id,
            ManifestId::from_bytes(raw(0x15)).unwrap(),
            target_generation,
            catalog_reference,
            target_floor,
            0,
            vec![],
            2_000,
        )
        .unwrap();
        let target_database_bytes = encode_database_manifest(&target_manifest).unwrap();
        let target_database_reference = DatabaseManifestRootRef::new(
            target_manifest.manifest_id(),
            ManifestGeneration::new(target_generation.get()).unwrap(),
            footer_sha(&target_database_bytes),
        );
        let writer = WriterInstanceId::from_bytes(raw(0x16)).unwrap();
        let target_control = ControlRecord::new(
            ControlSlotIndex::One,
            target_generation,
            database_id,
            target_database_reference,
            CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog_bytes)),
            target_floor,
            2_000,
            writer,
        )
        .unwrap();
        let staged = StagedArtifactSet::create(
            root.path().join("staging"),
            writer,
            target_generation,
            55,
            1_500,
        )
        .unwrap();
        let target_database_path = database_manifest_path(target_database_reference);
        staged
            .write_file(target_database_path.to_str().unwrap(), |file| {
                file.write_all(&target_database_bytes).unwrap();
                Ok(())
            })
            .unwrap();
        staged
            .write_file(wal_path(successor_wal).to_str().unwrap(), |file| {
                file.write_all(b"successor WAL").unwrap();
                Ok(())
            })
            .unwrap();
        let staging = staged
            .mark_complete(1_600, StagingDiscoveryLimits::default())
            .unwrap();
        let snapshot =
            PhysicalGenerationSnapshot::new(expected_control, expected_manifest, vec![]).unwrap();
        let publisher = PhysicalGenerationPublisher::open(root.path(), snapshot).unwrap();
        let source = publisher.pin().unwrap();
        let build = publisher
            .begin_artifact_build(ArtifactBuildKind::Seal, &source, staging.owner())
            .unwrap();

        Self {
            root,
            publisher,
            expected_control,
            target_control,
            target_database_path,
            staging,
            build,
            obsolete_wal,
            retained_wal,
            successor_wal,
        }
    }

    fn freeze(self, obsolete_wal: Vec<WalGeneration>) -> (Self, FrozenCheckpoint) {
        let checkpoint = FrozenCheckpoint::new(
            self.expected_control,
            self.target_control,
            self.staging.clone(),
            self.build.clone(),
            obsolete_wal,
        )
        .unwrap();
        (self, checkpoint)
    }
}

#[test]
fn checkpoint_publishes_before_retiring_only_wal_excluded_by_both_controls() {
    let fixture = CheckpointFixture::new();
    let obsolete = fixture.obsolete_wal;
    let (fixture, checkpoint) = fixture.freeze(vec![obsolete]);

    let outcome = fixture.publisher.publish_checkpoint(checkpoint).unwrap();

    assert_eq!(
        outcome.lease().database_generation(),
        fixture.target_control.database_generation()
    );
    let WalRetirementStatus::Complete(report) = outcome.wal_retirement() else {
        panic!("WAL retirement did not complete");
    };
    assert_eq!(report.requested(), 1);
    assert_eq!(report.renamed(), 1);
    assert_eq!(report.unlinked(), 1);
    assert_eq!(report.already_absent(), 0);
    assert!(!fixture.root.path().join(wal_path(obsolete)).exists());
    assert!(fixture
        .root
        .path()
        .join(wal_path(fixture.retained_wal))
        .exists());
    assert!(fixture
        .root
        .path()
        .join(wal_path(fixture.successor_wal))
        .exists());
    let control_bytes = std::fs::read(fixture.root.path().join("CONTROL.1")).unwrap();
    assert_eq!(
        decode_control_slot(&control_bytes, ControlSlotIndex::One).unwrap(),
        fixture.target_control
    );
}

#[test]
fn checkpoint_rejects_wal_still_reachable_from_retained_control() {
    let fixture = CheckpointFixture::new();
    let error = FrozenCheckpoint::new(
        fixture.expected_control,
        fixture.target_control,
        fixture.staging,
        fixture.build,
        vec![fixture.retained_wal],
    )
    .unwrap_err();
    assert_eq!(
        error,
        FormatError::InvalidCheckpoint {
            detail: "WAL generation is still required by retained CONTROL"
        }
    );
    assert!(fixture
        .root
        .path()
        .join(wal_path(fixture.retained_wal))
        .exists());
    assert!(!fixture.root.path().join("CONTROL.1").exists());
}

#[test]
fn failed_checkpoint_publication_never_starts_wal_retirement() {
    let fixture = CheckpointFixture::new();
    let obsolete = fixture.obsolete_wal;
    let (fixture, checkpoint) = fixture.freeze(vec![obsolete]);
    std::fs::remove_file(fixture.staging.path().join(&fixture.target_database_path)).unwrap();

    fixture
        .publisher
        .publish_checkpoint(checkpoint)
        .unwrap_err();

    assert!(fixture.root.path().join(wal_path(obsolete)).exists());
    assert!(!fixture.root.path().join("CONTROL.1").exists());
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation(),
        fixture.expected_control.database_generation()
    );
}

#[test]
fn post_commit_wal_failure_is_deferred_and_idempotently_retryable() {
    let fixture = CheckpointFixture::new();
    let obsolete = fixture.obsolete_wal;
    let (fixture, checkpoint) = fixture.freeze(vec![obsolete]);
    std::fs::write(fixture.root.path().join("wal/retired"), b"collision").unwrap();

    let mut outcome = fixture.publisher.publish_checkpoint(checkpoint).unwrap();

    assert!(matches!(
        outcome.wal_retirement(),
        WalRetirementStatus::Deferred(FormatError::InvalidCheckpoint { .. })
    ));
    assert_eq!(
        outcome.lease().database_generation(),
        fixture.target_control.database_generation()
    );
    assert!(fixture.root.path().join("CONTROL.1").exists());
    assert!(fixture.root.path().join(wal_path(obsolete)).exists());

    std::fs::remove_file(fixture.root.path().join("wal/retired")).unwrap();
    let report = outcome.retry_wal_retirement().unwrap();
    assert_eq!(report.requested(), 1);
    assert_eq!(report.renamed(), 1);
    assert_eq!(report.unlinked(), 1);
    assert_eq!(report.already_absent(), 0);
    assert!(!fixture.root.path().join(wal_path(obsolete)).exists());
}

#[test]
fn retry_resumes_from_a_wal_already_moved_to_retired() {
    let fixture = CheckpointFixture::new();
    let obsolete = fixture.obsolete_wal;
    let (fixture, checkpoint) = fixture.freeze(vec![obsolete]);
    std::fs::create_dir(fixture.root.path().join("wal/retired")).unwrap();
    std::fs::rename(
        fixture.root.path().join(wal_path(obsolete)),
        fixture
            .root
            .path()
            .join("wal/retired")
            .join(format!("wal-{:016x}.log", obsolete.get())),
    )
    .unwrap();

    let outcome = fixture.publisher.publish_checkpoint(checkpoint).unwrap();

    let WalRetirementStatus::Complete(report) = outcome.wal_retirement() else {
        panic!("WAL retirement did not complete");
    };
    assert_eq!(report.renamed(), 0);
    assert_eq!(report.unlinked(), 1);
    assert_eq!(report.already_absent(), 0);
    assert!(!fixture.root.path().join(wal_path(obsolete)).exists());
}

#[test]
fn retry_repairs_an_identical_hard_link_fallback_residue() {
    let fixture = CheckpointFixture::new();
    let obsolete = fixture.obsolete_wal;
    let (fixture, checkpoint) = fixture.freeze(vec![obsolete]);
    let retired_root = fixture.root.path().join("wal/retired");
    std::fs::create_dir(&retired_root).unwrap();
    std::fs::hard_link(
        fixture.root.path().join(wal_path(obsolete)),
        retired_root.join(format!("wal-{:016x}.log", obsolete.get())),
    )
    .unwrap();

    let outcome = fixture.publisher.publish_checkpoint(checkpoint).unwrap();

    let WalRetirementStatus::Complete(report) = outcome.wal_retirement() else {
        panic!("WAL retirement did not complete");
    };
    assert_eq!(report.renamed(), 0);
    assert_eq!(report.unlinked(), 1);
    assert!(!fixture.root.path().join(wal_path(obsolete)).exists());
    assert!(!retired_root
        .join(format!("wal-{:016x}.log", obsolete.get()))
        .exists());
}

#[cfg(feature = "test-failpoints")]
#[test]
fn wal_retirement_faults_leave_the_checkpoint_committed_and_retryable() {
    for point in [
        GenerationCrashPoint::WalBeforeTruncate,
        GenerationCrashPoint::WalAfterRenameToRetired,
        GenerationCrashPoint::WalAfterUnlinkBeforeDirSync,
        GenerationCrashPoint::WalTruncateDirDurable,
    ] {
        let fixture = CheckpointFixture::new();
        let obsolete = fixture.obsolete_wal;
        let (fixture, checkpoint) = fixture.freeze(vec![obsolete]);
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        let mut outcome = fixture.publisher.publish_checkpoint(checkpoint).unwrap();
        assert!(
            matches!(outcome.wal_retirement(), WalRetirementStatus::Deferred(_)),
            "{} must defer only retirement",
            point.name()
        );
        assert_eq!(guard.hit_count(), 1, "{} was not traversed", point.name());
        assert_eq!(
            fixture.publisher.pin().unwrap().database_generation(),
            fixture.target_control.database_generation()
        );
        drop(guard);
        let report = outcome.retry_wal_retirement().unwrap_or_else(|error| {
            panic!("{} did not resume idempotently: {error}", point.name())
        });
        assert_eq!(report.requested(), 1);
        assert!(!fixture.root.path().join(wal_path(obsolete)).exists());
    }
}

#[cfg(feature = "test-failpoints")]
fn prepare_process_checkpoint(
    root: &Path,
) -> (PhysicalGenerationPublisher, FrozenCheckpoint, WalGeneration) {
    for directory in ["wal", "catalog", "manifests", "staging"] {
        std::fs::create_dir(root.join(directory)).unwrap();
    }
    let database_id = DatabaseId::from_bytes(raw(0x31)).unwrap();
    let catalog_id = CatalogId::from_bytes(raw(0x32)).unwrap();
    let catalog_generation = CatalogGeneration::new(1).unwrap();
    let catalog = catalog_bytes(database_id, catalog_id, catalog_generation);
    let catalog_reference = CatalogRef::new(
        catalog_id,
        catalog_generation,
        catalog.len() as u64,
        footer_sha(&catalog),
    )
    .unwrap();
    write_file(root, &catalog_path(catalog_reference), &catalog);

    let source_generation = DatabaseGeneration::new(1).unwrap();
    let retained_wal = WalGeneration::new(2).unwrap();
    let source_floor = WalReplayFloor::new(retained_wal, 100);
    let source_manifest = DatabaseManifest::new(
        database_id,
        ManifestId::from_bytes(raw(0x33)).unwrap(),
        source_generation,
        catalog_reference,
        source_floor,
        0,
        vec![],
        1_000,
    )
    .unwrap();
    let source_bytes = encode_database_manifest(&source_manifest).unwrap();
    let source_reference = DatabaseManifestRootRef::new(
        source_manifest.manifest_id(),
        ManifestGeneration::new(source_generation.get()).unwrap(),
        footer_sha(&source_bytes),
    );
    let writer = WriterInstanceId::from_bytes(raw(0x34)).unwrap();
    let source_control = ControlRecord::new(
        ControlSlotIndex::Zero,
        source_generation,
        database_id,
        source_reference,
        CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog)),
        source_floor,
        1_000,
        writer,
    )
    .unwrap();
    write_file(
        root,
        &database_manifest_path(source_reference),
        &source_bytes,
    );
    write_file(
        root,
        Path::new("CONTROL.0"),
        &encode_control_slot(source_control),
    );
    write_file(root, &wal_path(retained_wal), b"");
    let obsolete_wal = WalGeneration::new(1).unwrap();
    write_file(root, &wal_path(obsolete_wal), b"obsolete");

    let target_generation = source_generation.checked_next().unwrap();
    let successor_wal = retained_wal.checked_next().unwrap();
    let target_floor = WalReplayFloor::new(successor_wal, 200);
    let target_manifest = DatabaseManifest::new(
        database_id,
        ManifestId::from_bytes(raw(0x35)).unwrap(),
        target_generation,
        catalog_reference,
        target_floor,
        0,
        vec![],
        2_000,
    )
    .unwrap();
    let target_bytes = encode_database_manifest(&target_manifest).unwrap();
    let target_reference = DatabaseManifestRootRef::new(
        target_manifest.manifest_id(),
        ManifestGeneration::new(target_generation.get()).unwrap(),
        footer_sha(&target_bytes),
    );
    let target_control = ControlRecord::new(
        ControlSlotIndex::One,
        target_generation,
        database_id,
        target_reference,
        CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog)),
        target_floor,
        2_000,
        writer,
    )
    .unwrap();
    let staged =
        StagedArtifactSet::create(root.join("staging"), writer, target_generation, 55, 1_500)
            .unwrap();
    staged
        .write_file(
            database_manifest_path(target_reference).to_str().unwrap(),
            |file| {
                file.write_all(&target_bytes).unwrap();
                Ok(())
            },
        )
        .unwrap();
    staged
        .write_file(wal_path(successor_wal).to_str().unwrap(), |_file| Ok(()))
        .unwrap();
    let staging = staged
        .mark_complete(1_600, StagingDiscoveryLimits::default())
        .unwrap();
    let source_snapshot =
        PhysicalGenerationSnapshot::new(source_control, source_manifest, vec![]).unwrap();
    let publisher = PhysicalGenerationPublisher::open(root, source_snapshot).unwrap();
    let source = publisher.pin().unwrap();
    let build = publisher
        .begin_artifact_build(ArtifactBuildKind::Seal, &source, staging.owner())
        .unwrap();
    let checkpoint = FrozenCheckpoint::new(
        source_control,
        target_control,
        staging,
        build,
        vec![obsolete_wal],
    )
    .unwrap();
    (publisher, checkpoint, obsolete_wal)
}

#[cfg(feature = "test-failpoints")]
struct ProcessCheckpointWal;

#[cfg(feature = "test-failpoints")]
impl WalRecovery for ProcessCheckpointWal {
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
        Ok(DataWalRecoveryOutcome::new(
            (),
            context.floor().lsn(),
            0,
            0,
            0,
            vec![],
        ))
    }
}

#[cfg(feature = "test-failpoints")]
#[test]
#[ignore = "isolated child entrypoint; executed by process_abort_wal_retirement_matrix"]
fn process_abort_wal_retirement_child() {
    let root = PathBuf::from(std::env::var_os("RADIXDB_PROCESS_TEST_ROOT").unwrap());
    let (publisher, checkpoint, _) = prepare_process_checkpoint(&root);
    let result = publisher.publish_checkpoint(checkpoint);
    panic!("child reached the end instead of stopping: {result:?}");
}

#[cfg(feature = "test-failpoints")]
#[test]
fn process_abort_wal_retirement_matrix_keeps_the_checkpoint_recoverable() {
    for point in [
        GenerationCrashPoint::WalBeforeTruncate,
        GenerationCrashPoint::WalAfterRenameToRetired,
        GenerationCrashPoint::WalAfterUnlinkBeforeDirSync,
        GenerationCrashPoint::WalTruncateDirDurable,
    ] {
        let root = tempfile::tempdir().unwrap();
        let evidence = root.path().join("boundary.hit");
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("process_abort_wal_retirement_child")
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

        for attempt in 0..2 {
            let mut wal = ProcessCheckpointWal;
            let recovered = DatabaseRecovery::new(root.path(), RecoveryLimits::default())
                .recover(&mut wal)
                .unwrap_or_else(|error| {
                    panic!("{} reopen {attempt} failed: {error}", point.name())
                });
            assert_eq!(recovered.control().database_generation().get(), 2);
        }
    }
}
