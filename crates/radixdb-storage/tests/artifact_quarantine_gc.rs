use std::fs;
#[cfg(feature = "test-failpoints")]
use std::path::{Path, PathBuf};
#[cfg(feature = "test-failpoints")]
use std::process::Command;
use std::time::Duration;

use radixdb_catalog::ObjectId;
use radixdb_storage::mvcc::FileLock;
use radixdb_storage::v6::{
    encode_control_slot, encode_data_artifact, ArtifactCleanupLimits, ArtifactGarbageCollector,
    ArtifactId, ArtifactRef, CatalogGeneration, CatalogId, CatalogRef, CatalogRootRef,
    ControlRecord, ControlSlotIndex, DataArtifactHeader, DataArtifactInput, DataBlockSpec,
    DataPhysicalCodec, DatabaseGeneration, DatabaseId, DatabaseManifest, DatabaseManifestRootRef,
    ManifestGeneration, ManifestId, ManifestKind, ManifestRef, PhysicalGenerationPublisher,
    PhysicalGenerationSnapshot, ReachabilityLimits, SegmentDescriptor, SegmentId, SegmentKind,
    TableManifest, TableManifestRef, WalGeneration, WalReplayFloor, WriterInstanceId,
};
#[cfg(feature = "test-hooks")]
use radixdb_storage::v6::{publication_diagnostics, reset_publication_diagnostics};
#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn artifact(marker: u8) -> (Vec<u8>, ArtifactRef) {
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(marker)).unwrap(),
        DatabaseId::from_bytes(raw(0x11)).unwrap(),
        ObjectId::from_user_bytes(raw(0x12)).unwrap(),
        SegmentId::from_bytes(raw(marker.wrapping_add(1))).unwrap(),
        DatabaseGeneration::new(9).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        1,
        1,
        1,
        0,
        1,
        SegmentKind::Tombstones,
        10,
    )
    .unwrap();
    let block = DataBlockSpec::row_ids(0, &[u64::from(marker)], DataPhysicalCodec::None).unwrap();
    encode_data_artifact(&DataArtifactInput::new(header, vec![], vec![], vec![block]).unwrap())
        .unwrap()
}

fn snapshot(
    artifact: ArtifactRef,
    database_generation: u64,
    marker: u8,
    slot: ControlSlotIndex,
) -> PhysicalGenerationSnapshot {
    let database_id = DatabaseId::from_bytes(raw(0x11)).unwrap();
    let database_generation = DatabaseGeneration::new(database_generation).unwrap();
    let manifest_generation = ManifestGeneration::new(database_generation.get()).unwrap();
    let catalog_generation = CatalogGeneration::new(3).unwrap();
    let catalog_id = CatalogId::from_bytes(raw(0x13)).unwrap();
    let catalog_sha = [0x14; 32];
    let catalog = CatalogRef::new(catalog_id, catalog_generation, 512, catalog_sha).unwrap();
    let table_id = ObjectId::from_user_bytes(raw(0x12)).unwrap();
    let table = TableManifest::new(
        database_id,
        table_id,
        ManifestId::from_bytes(raw(marker)).unwrap(),
        manifest_generation,
        catalog_generation,
        1,
        2,
        vec![SegmentDescriptor::new(
            SegmentId::from_bytes(raw(marker.wrapping_add(1))).unwrap(),
            SegmentKind::Rows,
            1,
            1,
            1,
            1,
            1,
            artifact,
            None,
        )
        .unwrap()],
        10,
    )
    .unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table.manifest_id(),
            ManifestKind::Table,
            manifest_generation,
            512,
            [marker.wrapping_add(2); 32],
        )
        .unwrap(),
    )
    .unwrap();
    let database_manifest_id = ManifestId::from_bytes(raw(marker.wrapping_add(3))).unwrap();
    let wal_floor = WalReplayFloor::new(WalGeneration::new(database_generation.get()).unwrap(), 1);
    let database_manifest = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        database_generation,
        catalog,
        wal_floor,
        10_000,
        vec![table_reference],
        10,
    )
    .unwrap();
    let control = ControlRecord::new(
        slot,
        database_generation,
        database_id,
        DatabaseManifestRootRef::new(
            database_manifest_id,
            manifest_generation,
            [marker.wrapping_add(4); 32],
        ),
        CatalogRootRef::new(catalog_id, catalog_generation, catalog_sha),
        wal_floor,
        10,
        WriterInstanceId::from_bytes(raw(0x15)).unwrap(),
    )
    .unwrap();
    PhysicalGenerationSnapshot::new(control, database_manifest, vec![table]).unwrap()
}

fn write(root: &std::path::Path, bytes: &[u8], reference: ArtifactRef) {
    let path = root.join(reference.relative_path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
}

fn install_control(root: &std::path::Path, snapshot: &PhysicalGenerationSnapshot) {
    let control = snapshot.control();
    let name = match control.slot() {
        ControlSlotIndex::Zero => "CONTROL.0",
        ControlSlotIndex::One => "CONTROL.1",
    };
    fs::write(root.join(name), encode_control_slot(control)).unwrap();
}

fn limits() -> ArtifactCleanupLimits {
    ArtifactCleanupLimits::new(
        100,
        1024 * 1024 * 1024,
        100,
        100,
        1024 * 1024 * 1024,
        Duration::ZERO,
        Duration::from_secs(60),
    )
    .unwrap()
}

#[test]
fn publisher_fences_cleanup_with_current_fallback_and_fresh_root_proofs() {
    let root = tempfile::tempdir().unwrap();
    let (current_bytes, current_artifact) = artifact(0x21);
    let (fallback_bytes, fallback_artifact) = artifact(0x31);
    write(root.path(), &current_bytes, current_artifact);
    write(root.path(), &fallback_bytes, fallback_artifact);

    let current = snapshot(current_artifact, 10, 0x41, ControlSlotIndex::One);
    let fallback = snapshot(fallback_artifact, 9, 0x51, ControlSlotIndex::Zero);
    install_control(root.path(), &current);
    let publisher = PhysicalGenerationPublisher::open(root.path(), current).unwrap();
    let collector = ArtifactGarbageCollector::new(root.path(), limits());
    #[cfg(feature = "test-hooks")]
    reset_publication_diagnostics();

    let first = publisher
        .collect_unreachable_artifacts(
            &collector,
            &[],
            &[],
            u64::MAX,
            ReachabilityLimits::default(),
        )
        .unwrap();
    assert_eq!(first.quarantined(), 1);
    assert!(root.path().join(current_artifact.relative_path()).exists());
    assert!(!root.path().join(fallback_artifact.relative_path()).exists());
    #[cfg(feature = "test-hooks")]
    {
        let counters = publication_diagnostics();
        assert_eq!(counters.cleanup_fence_holds, 1);
        assert_eq!(counters.generation_fence_holds, 0);
        reset_publication_diagnostics();
    }

    let second = publisher
        .collect_unreachable_artifacts(
            &collector,
            &[fallback],
            &[],
            u64::MAX,
            ReachabilityLimits::default(),
        )
        .unwrap();
    assert_eq!(second.restored(), 1);
    assert_eq!(second.deleted(), 0);
    assert!(root.path().join(fallback_artifact.relative_path()).exists());
}

#[test]
fn publisher_rejects_collector_for_a_cloned_database_root_before_mutation() {
    let owned_root = tempfile::tempdir().unwrap();
    let clone_root = tempfile::tempdir().unwrap();
    let (current_bytes, current_artifact) = artifact(0x25);
    let (orphan_bytes, orphan_artifact) = artifact(0x35);
    let current = snapshot(current_artifact, 10, 0x45, ControlSlotIndex::One);
    for root in [owned_root.path(), clone_root.path()] {
        write(root, &current_bytes, current_artifact);
        write(root, &orphan_bytes, orphan_artifact);
        install_control(root, &current);
    }
    let publisher = PhysicalGenerationPublisher::open(owned_root.path(), current).unwrap();
    let _clone_lock = FileLock::acquire(clone_root.path()).unwrap();
    let collector = ArtifactGarbageCollector::new(clone_root.path(), limits());
    let control_before = fs::read(clone_root.path().join("CONTROL.1")).unwrap();

    let error = publisher
        .collect_unreachable_artifacts(
            &collector,
            &[],
            &[],
            u64::MAX,
            ReachabilityLimits::default(),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        radixdb_storage::v6::FormatError::InvalidFilesystemOwner { .. }
    ));
    assert_eq!(
        fs::read(clone_root.path().join("CONTROL.1")).unwrap(),
        control_before
    );
    assert!(clone_root
        .path()
        .join(orphan_artifact.relative_path())
        .exists());
    assert!(!clone_root.path().join("quarantine").exists());
}

#[cfg(feature = "test-failpoints")]
fn cleanup_fixture() -> (
    tempfile::TempDir,
    PhysicalGenerationPublisher,
    ArtifactGarbageCollector,
    ArtifactRef,
) {
    let root = tempfile::tempdir().unwrap();
    let (current_bytes, current_artifact) = artifact(0x61);
    let (orphan_bytes, orphan_artifact) = artifact(0x71);
    write(root.path(), &current_bytes, current_artifact);
    write(root.path(), &orphan_bytes, orphan_artifact);
    let current = snapshot(current_artifact, 10, 0x81, ControlSlotIndex::One);
    install_control(root.path(), &current);
    let publisher = PhysicalGenerationPublisher::open(root.path(), current).unwrap();
    let collector = ArtifactGarbageCollector::new(root.path(), limits());
    (root, publisher, collector, orphan_artifact)
}

#[cfg(feature = "test-failpoints")]
#[test]
fn cleanup_fault_matrix_preserves_two_proofs_and_idempotent_recovery() {
    for point in [
        GenerationCrashPoint::GcAfterRootSnapshot,
        GenerationCrashPoint::GcEnumerationError,
        GenerationCrashPoint::GcBeforeQuarantineRename,
        GenerationCrashPoint::GcAfterQuarantineRenameBeforeSync,
        GenerationCrashPoint::GcQuarantineDirDurable,
    ] {
        let (root, publisher, collector, orphan) = cleanup_fixture();
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        assert!(publisher
            .collect_unreachable_artifacts(
                &collector,
                &[],
                &[],
                u64::MAX,
                ReachabilityLimits::default(),
            )
            .is_err());
        assert_eq!(guard.hit_count(), 1, "{} was not traversed", point.name());
        drop(guard);

        let report = publisher
            .collect_unreachable_artifacts(
                &collector,
                &[],
                &[],
                u64::MAX,
                ReachabilityLimits::default(),
            )
            .unwrap_or_else(|error| {
                panic!("{} did not resume idempotently: {error}", point.name())
            });
        assert!(report.quarantined() + report.deleted() >= 1);
        assert!(!root.path().join(orphan.relative_path()).exists());
    }

    for point in [
        GenerationCrashPoint::GcAfterSecondRootProof,
        GenerationCrashPoint::GcBeforeUnlink,
        GenerationCrashPoint::GcAfterUnlinkBeforeDirSync,
        GenerationCrashPoint::GcDeleteDirDurable,
    ] {
        let (root, publisher, collector, orphan) = cleanup_fixture();
        let first = publisher
            .collect_unreachable_artifacts(
                &collector,
                &[],
                &[],
                u64::MAX,
                ReachabilityLimits::default(),
            )
            .unwrap();
        assert_eq!(first.quarantined(), 1);

        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        assert!(publisher
            .collect_unreachable_artifacts(
                &collector,
                &[],
                &[],
                u64::MAX,
                ReachabilityLimits::default(),
            )
            .is_err());
        assert_eq!(guard.hit_count(), 1, "{} was not traversed", point.name());
        drop(guard);

        publisher
            .collect_unreachable_artifacts(
                &collector,
                &[],
                &[],
                u64::MAX,
                ReachabilityLimits::default(),
            )
            .unwrap_or_else(|error| {
                panic!("{} did not resume idempotently: {error}", point.name())
            });
        assert!(!root.path().join(orphan.relative_path()).exists());
    }
}

#[cfg(feature = "test-failpoints")]
#[test]
fn cleanup_root_reappearance_preempts_deletion() {
    let (root, publisher, collector, orphan) = cleanup_fixture();
    publisher
        .collect_unreachable_artifacts(
            &collector,
            &[],
            &[],
            u64::MAX,
            ReachabilityLimits::default(),
        )
        .unwrap();
    let guard = GenerationFaultGuard::arm(
        GenerationCrashPoint::GcLeaseAppeared,
        GenerationFaultMode::ReturnIoError,
    );
    assert!(publisher
        .collect_unreachable_artifacts(
            &collector,
            &[],
            &[orphan],
            u64::MAX,
            ReachabilityLimits::default(),
        )
        .is_err());
    assert_eq!(guard.hit_count(), 1);
    drop(guard);
    let report = publisher
        .collect_unreachable_artifacts(
            &collector,
            &[],
            &[orphan],
            u64::MAX,
            ReachabilityLimits::default(),
        )
        .unwrap();
    assert_eq!(report.restored(), 1);
    assert!(root.path().join(orphan.relative_path()).exists());
}

#[cfg(feature = "test-failpoints")]
fn process_cleanup_state(
    root: &Path,
    initialize: bool,
) -> (
    PhysicalGenerationPublisher,
    ArtifactGarbageCollector,
    ArtifactRef,
    ArtifactRef,
) {
    let (current_bytes, current_artifact) = artifact(0x91);
    let (orphan_bytes, orphan_artifact) = artifact(0xA1);
    if initialize {
        write(root, &current_bytes, current_artifact);
        write(root, &orphan_bytes, orphan_artifact);
    }
    let current = snapshot(current_artifact, 10, 0xB1, ControlSlotIndex::One);
    if initialize {
        install_control(root, &current);
    }
    (
        PhysicalGenerationPublisher::open(root, current).unwrap(),
        ArtifactGarbageCollector::new(root, limits()),
        current_artifact,
        orphan_artifact,
    )
}

#[cfg(feature = "test-failpoints")]
#[test]
#[ignore = "isolated child entrypoint; executed by process_abort_cleanup_matrix"]
fn process_abort_cleanup_child() {
    let root = PathBuf::from(std::env::var_os("RADIXDB_PROCESS_TEST_ROOT").unwrap());
    let requested = std::env::var("RADIXDB_GENERATION_FAULT_POINT").unwrap();
    let point = GenerationCrashPoint::LIFECYCLE_GC_POINTS
        .into_iter()
        .find(|point| point.name() == requested)
        .unwrap();
    let (publisher, collector, _, orphan) = process_cleanup_state(&root, true);
    if matches!(
        point,
        GenerationCrashPoint::GcAfterSecondRootProof
            | GenerationCrashPoint::GcBeforeUnlink
            | GenerationCrashPoint::GcAfterUnlinkBeforeDirSync
            | GenerationCrashPoint::GcDeleteDirDurable
            | GenerationCrashPoint::GcLeaseAppeared
    ) {
        publisher
            .collect_unreachable_artifacts(
                &collector,
                &[],
                &[],
                u64::MAX,
                ReachabilityLimits::default(),
            )
            .unwrap();
    }
    let retained = if point == GenerationCrashPoint::GcLeaseAppeared {
        std::slice::from_ref(&orphan)
    } else {
        &[]
    };
    let result = publisher.collect_unreachable_artifacts(
        &collector,
        &[],
        retained,
        u64::MAX,
        ReachabilityLimits::default(),
    );
    panic!("child reached the end instead of stopping: {result:?}");
}

#[cfg(feature = "test-failpoints")]
#[test]
fn process_abort_cleanup_matrix_never_deletes_a_reachable_artifact() {
    for point in GenerationCrashPoint::LIFECYCLE_GC_POINTS {
        let root = tempfile::tempdir().unwrap();
        let evidence = root.path().join("boundary.hit");
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("process_abort_cleanup_child")
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

        let (publisher, collector, current, orphan) = process_cleanup_state(root.path(), false);
        assert!(root.path().join(current.relative_path()).is_file());
        if point == GenerationCrashPoint::GcLeaseAppeared {
            publisher
                .collect_unreachable_artifacts(
                    &collector,
                    &[],
                    &[orphan],
                    u64::MAX,
                    ReachabilityLimits::default(),
                )
                .unwrap();
            assert!(root.path().join(orphan.relative_path()).is_file());
        } else {
            for _ in 0..2 {
                publisher
                    .collect_unreachable_artifacts(
                        &collector,
                        &[],
                        &[],
                        u64::MAX,
                        ReachabilityLimits::default(),
                    )
                    .unwrap();
            }
            assert!(!root.path().join(orphan.relative_path()).exists());
        }
        assert!(root.path().join(current.relative_path()).is_file());
    }
}
