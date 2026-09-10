use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use radixdb_catalog::ObjectId;

use crate::v6::{
    ArtifactId, ArtifactKind, ArtifactRef, CatalogGeneration, CatalogId, CatalogRef,
    CatalogRootRef, ControlRecord, ControlSlotIndex, DatabaseGeneration, DatabaseId,
    DatabaseManifest, DatabaseManifestRootRef, FormatError, ManifestGeneration, ManifestId,
    ManifestKind, ManifestRef, SegmentDescriptor, SegmentId, SegmentKind, StagedArtifactSet,
    TableManifest, TableManifestRef, WalGeneration, WalReplayFloor, WriterInstanceId,
};

use super::{ArtifactBuildKind, LeaseLimits};
use crate::v6::{PhysicalGenerationPublisher, PhysicalGenerationSnapshot};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn snapshot(generation: u64, marker: u8) -> PhysicalGenerationSnapshot {
    let generation = DatabaseGeneration::new(generation).unwrap();
    let manifest_generation = ManifestGeneration::new(generation.get()).unwrap();
    let database_id = DatabaseId::from_bytes(raw(1)).unwrap();
    let table_id = object_id(2);
    let catalog_generation = CatalogGeneration::new(3).unwrap();
    let catalog_id = CatalogId::from_bytes(raw(4)).unwrap();
    let catalog_sha = [5; 32];
    let catalog = CatalogRef::new(catalog_id, catalog_generation, 512, catalog_sha).unwrap();
    let data = ArtifactRef::new(
        ArtifactId::from_bytes(raw(marker)).unwrap(),
        ArtifactKind::Data,
        generation,
        512,
        [marker; 32],
    )
    .unwrap();
    let index = ArtifactRef::new(
        ArtifactId::from_bytes(raw(marker + 1)).unwrap(),
        ArtifactKind::Index,
        generation,
        512,
        [marker + 1; 32],
    )
    .unwrap();
    let table_manifest = TableManifest::new(
        database_id,
        table_id,
        ManifestId::from_bytes(raw(marker + 2)).unwrap(),
        manifest_generation,
        catalog_generation,
        10,
        2,
        vec![SegmentDescriptor::new(
            SegmentId::from_bytes(raw(marker + 3)).unwrap(),
            SegmentKind::Rows,
            1,
            1,
            10,
            1,
            10,
            data,
            Some(index),
        )
        .unwrap()],
        10,
    )
    .unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest.manifest_id(),
            ManifestKind::Table,
            manifest_generation,
            512,
            [marker + 4; 32],
        )
        .unwrap(),
    )
    .unwrap();
    let database_manifest_id = ManifestId::from_bytes(raw(marker + 5)).unwrap();
    let wal_floor = WalReplayFloor::new(WalGeneration::new(generation.get()).unwrap(), 50);
    let database_manifest = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        generation,
        catalog,
        wal_floor,
        10_000,
        vec![table_reference],
        10,
    )
    .unwrap();
    let database_root =
        DatabaseManifestRootRef::new(database_manifest_id, manifest_generation, [marker + 6; 32]);
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        generation,
        database_id,
        database_root,
        CatalogRootRef::new(catalog_id, catalog_generation, catalog_sha),
        wal_floor,
        10,
        WriterInstanceId::from_bytes(raw(9)).unwrap(),
    )
    .unwrap();
    PhysicalGenerationSnapshot::new(control, database_manifest, vec![table_manifest]).unwrap()
}

fn staging(root: &Path, writer_marker: u8, intended_generation: u64) -> StagedArtifactSet {
    StagedArtifactSet::create(
        root,
        WriterInstanceId::from_bytes(raw(writer_marker)).unwrap(),
        DatabaseGeneration::new(intended_generation).unwrap(),
        42,
        1_000,
    )
    .unwrap()
}

#[test]
fn reader_and_build_leases_are_exact_deduplicated_gc_roots() {
    let root = tempfile::tempdir().unwrap();
    let publisher = PhysicalGenerationPublisher::for_runtime_tests(snapshot(7, 20));
    let first_reader = publisher.pin().unwrap();
    let second_reader = publisher.pin().unwrap();
    assert_eq!(publisher.active_leases().generations().len(), 1);

    let seal_staging = staging(root.path(), 31, 8);
    let compaction_staging = staging(root.path(), 32, 8);
    let snapshot_staging = staging(root.path(), 33, 7);
    let rebuild_staging = staging(root.path(), 34, 8);
    let builds = [
        publisher
            .begin_artifact_build(ArtifactBuildKind::Seal, &first_reader, seal_staging.owner())
            .unwrap(),
        publisher
            .begin_artifact_build(
                ArtifactBuildKind::Compaction,
                &first_reader,
                compaction_staging.owner(),
            )
            .unwrap(),
        publisher
            .begin_artifact_build(
                ArtifactBuildKind::Snapshot,
                &first_reader,
                snapshot_staging.owner(),
            )
            .unwrap(),
        publisher
            .begin_artifact_build(
                ArtifactBuildKind::IndexRebuild,
                &first_reader,
                rebuild_staging.owner(),
            )
            .unwrap(),
    ];

    drop(first_reader);
    drop(second_reader);
    let roots = publisher.active_leases();
    assert_eq!(roots.generations().len(), 1);
    assert_eq!(roots.artifact_builds().len(), 4);
    assert_eq!(roots.pinned_artifacts().len(), 2);
    assert_eq!(roots.staging_owners().len(), 4);
    for build in &builds {
        assert_eq!(build.source().database_generation().get(), 7);
        assert_eq!(build.source_artifacts().len(), 2);
    }

    drop(builds);
    assert_eq!(publisher.active_leases().artifact_builds().len(), 4);
    drop(roots);
    let released = publisher.active_leases();
    assert!(released.generations().is_empty());
    assert!(released.artifact_builds().is_empty());
}

#[test]
fn build_admission_rejects_wrong_generation_foreign_source_and_duplicate_owner() {
    let root = tempfile::tempdir().unwrap();
    let publisher = PhysicalGenerationPublisher::for_runtime_tests(snapshot(7, 20));
    let foreign = PhysicalGenerationPublisher::for_runtime_tests(snapshot(7, 60));
    let source = publisher.pin().unwrap();
    let foreign_source = foreign.pin().unwrap();
    let wrong = staging(root.path(), 41, 7);
    let valid = staging(root.path(), 42, 8);

    assert!(matches!(
        publisher.begin_artifact_build(ArtifactBuildKind::Compaction, &source, wrong.owner()),
        Err(FormatError::InvalidLease { .. })
    ));
    assert!(matches!(
        publisher.begin_artifact_build(ArtifactBuildKind::Seal, &foreign_source, valid.owner()),
        Err(FormatError::InvalidLease { .. })
    ));
    let lease = publisher
        .begin_artifact_build(ArtifactBuildKind::IndexRebuild, &source, valid.owner())
        .unwrap();
    assert!(matches!(
        publisher.begin_artifact_build(ArtifactBuildKind::IndexRebuild, &source, valid.owner()),
        Err(FormatError::InvalidLease { .. })
    ));
    assert_eq!(lease.staging_owner(), valid.owner());

    let complete = valid.mark_complete(2_000, Default::default()).unwrap();
    assert!(matches!(
        lease.validate_publication(
            source.snapshot(),
            &complete,
            Some(ArtifactBuildKind::Compaction)
        ),
        Err(FormatError::InvalidLease { .. })
    ));
    assert!(matches!(
        lease.validate_publication(
            foreign_source.snapshot(),
            &complete,
            Some(ArtifactBuildKind::IndexRebuild)
        ),
        Err(FormatError::InvalidLease { .. })
    ));
}

#[test]
fn lease_limits_fail_closed_and_dead_entries_release_capacity() {
    assert!(matches!(
        LeaseLimits::new(0, 1),
        Err(FormatError::InvalidLeaseLimit { .. })
    ));
    let root = tempfile::tempdir().unwrap();
    let limits = LeaseLimits::new(2, 1).unwrap();
    let publisher =
        PhysicalGenerationPublisher::for_runtime_tests_with_lease_limits(snapshot(7, 20), limits);
    let source = publisher.pin().unwrap();
    let first = staging(root.path(), 51, 8);
    let second = staging(root.path(), 52, 8);
    let lease = publisher
        .begin_artifact_build(ArtifactBuildKind::Compaction, &source, first.owner())
        .unwrap();
    assert!(matches!(
        publisher.begin_artifact_build(ArtifactBuildKind::Compaction, &source, second.owner()),
        Err(FormatError::LeaseLimitExceeded {
            field: "artifact builds",
            actual: 2,
            limit: 1,
        })
    ));
    drop(lease);
    publisher
        .begin_artifact_build(ArtifactBuildKind::Compaction, &source, second.owner())
        .unwrap();
}

#[test]
#[ignore = "release-only RV6-10 concurrent pin latency evidence"]
fn concurrent_pin_latency_evidence() {
    const SAMPLES_PER_THREAD: usize = 5_000;

    for thread_count in [1_usize, 8, 32] {
        let publisher = Arc::new(PhysicalGenerationPublisher::for_runtime_tests(snapshot(
            7, 20,
        )));
        for _ in 0..1_000 {
            drop(publisher.pin().unwrap());
        }

        let barrier = Arc::new(Barrier::new(thread_count + 1));
        let mut workers = Vec::with_capacity(thread_count);
        for worker_index in 0..thread_count {
            let publisher = Arc::clone(&publisher);
            let barrier = Arc::clone(&barrier);
            workers.push(
                std::thread::Builder::new()
                    .name(format!("pin-latency-{worker_index}"))
                    .spawn(move || {
                        let mut samples = Vec::with_capacity(SAMPLES_PER_THREAD);
                        barrier.wait();
                        for _ in 0..SAMPLES_PER_THREAD {
                            let started = Instant::now();
                            let lease = publisher.pin().unwrap();
                            samples.push(started.elapsed().as_nanos());
                            drop(lease);
                        }
                        samples
                    })
                    .unwrap(),
            );
        }

        let wall_started = Instant::now();
        barrier.wait();
        let mut samples = Vec::with_capacity(thread_count * SAMPLES_PER_THREAD);
        for worker in workers {
            samples.extend(worker.join().unwrap());
        }
        let wall_ns = wall_started.elapsed().as_nanos();
        samples.sort_unstable();
        let percentile = |percent: usize| samples[(samples.len() - 1) * percent / 100];
        let throughput = (samples.len() as u128 * 1_000_000_000_u128) / wall_ns.max(1);

        println!(
            "{{\"threads\":{thread_count},\"samples\":{},\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"max_ns\":{},\"wall_ns\":{wall_ns},\"pins_per_second\":{throughput}}}",
            samples.len(),
            percentile(50),
            percentile(95),
            percentile(99),
            samples[samples.len() - 1],
        );
    }
}
