use std::fs;
use std::time::Duration;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use tempfile::TempDir;

use crate::v6::{
    build_artifact_pair, encode_data_artifact, AcceleratorBuildSpec, ArtifactCleanupLimits,
    ArtifactGarbageCollector, ArtifactId, ArtifactPairBuildRequest, ArtifactReachability,
    CatalogGeneration, CatalogId, CatalogRef, CatalogRootRef, ColumnBuildPolicy, ControlRecord,
    ControlSlotIndex, DataArtifactHeader, DataArtifactInput, DataBlockSpec, DataColumnSpec,
    DataPhysicalCodec, DatabaseGeneration, DatabaseId, DatabaseManifest, DatabaseManifestRootRef,
    ExactPageBuildLimits, FanoutBuildLimits, FormatError, ImmutableMemberRef, IndexKeyColumn,
    IndexNullsOrder, IndexPageCodec, IndexSortDirection, ManifestGeneration, ManifestId,
    ManifestKind, ManifestRef, PhysicalGenerationPublisher, PhysicalGenerationSnapshot,
    ReachabilityLimits, SegmentDescriptor, SegmentId, SegmentKind, SourceRow, TableManifest,
    TableManifestRef, WalGeneration, WalReplayFloor, WriterInstanceId,
};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn artifact(marker: u8) -> (Vec<u8>, crate::v6::ArtifactRef) {
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(marker)).unwrap(),
        DatabaseId::from_bytes(raw(0x22)).unwrap(),
        ObjectId::from_user_bytes(raw(0x33)).unwrap(),
        SegmentId::from_bytes(raw(marker.wrapping_add(1))).unwrap(),
        DatabaseGeneration::new(9).unwrap(),
        CatalogGeneration::new(7).unwrap(),
        101,
        105,
        1,
        0,
        1,
        SegmentKind::Tombstones,
        123_456,
    )
    .unwrap();
    let block = DataBlockSpec::row_ids(0, &[u64::from(marker)], DataPhysicalCodec::None).unwrap();
    encode_data_artifact(&DataArtifactInput::new(header, vec![], vec![], vec![block]).unwrap())
        .unwrap()
}

fn write_artifact(root: &TempDir, marker: u8) -> crate::v6::ArtifactRef {
    let (bytes, reference) = artifact(marker);
    let path = root.path().join(reference.relative_path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
    reference
}

fn write_artifact_pair(root: &TempDir) -> [crate::v6::ArtifactRef; 2] {
    fs::create_dir(root.path().join("build")).unwrap();
    let column_id = ObjectId::from_user_bytes(raw(0xd1)).unwrap();
    let column = DataColumnSpec::new(
        column_id,
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let accelerator = AcceleratorBuildSpec::exact(
        ObjectId::from_user_bytes(raw(0xd2)).unwrap(),
        false,
        false,
        [0xd3; 32],
        vec![IndexKeyColumn::new(
            column_id,
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::new(16, 1024 * 1024).unwrap(),
    )
    .unwrap();
    let request = ArtifactPairBuildRequest::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes(raw(0xd4)).unwrap(),
            DatabaseId::from_bytes(raw(0xd5)).unwrap(),
            ObjectId::from_user_bytes(raw(0xd6)).unwrap(),
            SegmentId::from_bytes(raw(0xd7)).unwrap(),
            DatabaseGeneration::new(12).unwrap(),
            CatalogGeneration::new(8).unwrap(),
            1,
            1,
            1,
            1,
            1,
            SegmentKind::Rows,
            10,
        )
        .unwrap(),
        vec![column],
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes(raw(0xd8)).unwrap(),
        vec![accelerator],
        FanoutBuildLimits::new(16, 16, 1024 * 1024, 16).unwrap(),
        root.path().join("build"),
    )
    .unwrap();
    let pair =
        build_artifact_pair(&request, [Ok(SourceRow::new(1, vec![Value::integer(42)]))]).unwrap();
    for (reference, bytes) in [
        (pair.data_reference(), pair.data_bytes()),
        (pair.index_reference(), pair.index_bytes()),
    ] {
        let path = root.path().join(reference.relative_path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    [pair.data_reference(), pair.index_reference()]
}

fn write_metadata_shell(
    root: &TempDir,
    relative_path: &str,
    magic: &[u8; 8],
    format_minor: u16,
    fields: &[(usize, &[u8])],
) -> Vec<u8> {
    let mut bytes = vec![0_u8; 256];
    bytes[..8].copy_from_slice(magic);
    bytes[8..10].copy_from_slice(&6_u16.to_le_bytes());
    bytes[10..12].copy_from_slice(&format_minor.to_le_bytes());
    bytes[12..16].copy_from_slice(&256_u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&304_u64.to_le_bytes());
    for (offset, value) in fields {
        bytes[*offset..*offset + value.len()].copy_from_slice(value);
    }
    let crc = radixdb_core::crc32_ieee(&bytes[..248]);
    bytes[248..252].copy_from_slice(&crc.to_le_bytes());
    let body_sha = radixdb_core::sha256_digest(&bytes);
    bytes.extend_from_slice(b"RDX6END\0");
    bytes.extend_from_slice(&304_u64.to_le_bytes());
    bytes.extend_from_slice(&body_sha);
    let path = root.path().join(relative_path);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, &bytes).unwrap();
    bytes
}

fn write_metadata_members(root: &TempDir) -> Vec<ImmutableMemberRef> {
    let catalog_id = CatalogId::from_bytes(raw(0xe1)).unwrap();
    let catalog_generation = CatalogGeneration::new(11).unwrap();
    let catalog_bytes = write_metadata_shell(
        root,
        "catalog/catalog-000000000000000b.cat",
        b"RDX6CAT\0",
        radixdb_catalog::LATEST_CATALOG_MINOR,
        &[
            (40, catalog_id.as_bytes()),
            (56, &catalog_generation.get().to_le_bytes()),
        ],
    );
    let catalog = CatalogRef::new(
        catalog_id,
        catalog_generation,
        catalog_bytes.len() as u64,
        catalog_bytes[catalog_bytes.len() - 32..]
            .try_into()
            .unwrap(),
    )
    .unwrap();

    let database_manifest_id = ManifestId::from_bytes(raw(0xe2)).unwrap();
    let database_generation = ManifestGeneration::new(12).unwrap();
    let database_bytes = write_metadata_shell(
        root,
        "manifests/database-000000000000000c.mft",
        b"RDX6DBM\0",
        0,
        &[
            (40, database_manifest_id.as_bytes()),
            (56, &database_generation.get().to_le_bytes()),
        ],
    );
    let database = ImmutableMemberRef::DatabaseManifest(
        ManifestRef::new(
            database_manifest_id,
            ManifestKind::Database,
            database_generation,
            database_bytes.len() as u64,
            database_bytes[database_bytes.len() - 32..]
                .try_into()
                .unwrap(),
        )
        .unwrap(),
    );

    let table_id = ObjectId::from_user_bytes(raw(0xe3)).unwrap();
    let table_manifest_id = ManifestId::from_bytes(raw(0xe4)).unwrap();
    let table_generation = ManifestGeneration::new(9).unwrap();
    let table_bytes = write_metadata_shell(
        root,
        &format!("manifests/tables/{table_id}/table-0000000000000009.mft"),
        b"RDX6TBM\0",
        0,
        &[
            (40, table_id.as_bytes()),
            (56, table_manifest_id.as_bytes()),
            (72, &table_generation.get().to_le_bytes()),
        ],
    );
    let table = ImmutableMemberRef::TableManifest(
        TableManifestRef::new(
            table_id,
            ManifestRef::new(
                table_manifest_id,
                ManifestKind::Table,
                table_generation,
                table_bytes.len() as u64,
                table_bytes[table_bytes.len() - 32..].try_into().unwrap(),
            )
            .unwrap(),
        )
        .unwrap(),
    );
    vec![ImmutableMemberRef::Catalog(catalog), database, table]
}

fn limits(max_renames: u64, max_unlinks: u64) -> ArtifactCleanupLimits {
    ArtifactCleanupLimits::new(
        100,
        1024 * 1024 * 1024,
        max_renames,
        max_unlinks,
        1024 * 1024 * 1024,
        Duration::ZERO,
        Duration::from_secs(60),
    )
    .unwrap()
}

fn reachability(
    references: impl IntoIterator<Item = crate::v6::ArtifactRef>,
) -> ArtifactReachability {
    ArtifactReachability::from_artifacts(references, ReachabilityLimits::default()).unwrap()
}

fn snapshot(reference: crate::v6::ArtifactRef) -> PhysicalGenerationSnapshot {
    snapshot_with_index(reference, None)
}

fn snapshot_with_index(
    reference: crate::v6::ArtifactRef,
    index: Option<crate::v6::ArtifactRef>,
) -> PhysicalGenerationSnapshot {
    let database_id = DatabaseId::from_bytes(raw(0xb1)).unwrap();
    let generation = reference.creation_generation();
    let manifest_generation = ManifestGeneration::new(generation.get()).unwrap();
    let catalog_generation = CatalogGeneration::new(2).unwrap();
    let catalog_id = CatalogId::from_bytes(raw(0xb2)).unwrap();
    let catalog_sha = [0xb3; 32];
    let catalog = CatalogRef::new(catalog_id, catalog_generation, 512, catalog_sha).unwrap();
    let table_id = ObjectId::from_user_bytes(raw(0xb4)).unwrap();
    let table = TableManifest::new(
        database_id,
        table_id,
        ManifestId::from_bytes(raw(0xb5)).unwrap(),
        manifest_generation,
        catalog_generation,
        1,
        2,
        vec![SegmentDescriptor::new(
            SegmentId::from_bytes(raw(0xb6)).unwrap(),
            SegmentKind::Rows,
            1,
            1,
            1,
            1,
            1,
            reference,
            index,
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
            [0xb7; 32],
        )
        .unwrap(),
    )
    .unwrap();
    let database_manifest_id = ManifestId::from_bytes(raw(0xb8)).unwrap();
    let wal_floor = WalReplayFloor::new(WalGeneration::new(generation.get()).unwrap(), 10);
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
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        generation,
        database_id,
        DatabaseManifestRootRef::new(database_manifest_id, manifest_generation, [0xb9; 32]),
        CatalogRootRef::new(catalog_id, catalog_generation, catalog_sha),
        wal_floor,
        10,
        WriterInstanceId::from_bytes(raw(0xba)).unwrap(),
    )
    .unwrap();
    PhysicalGenerationSnapshot::new(control, database_manifest, vec![table]).unwrap()
}

#[test]
fn unreachable_artifact_is_quarantined_then_deleted_after_fresh_cycle() {
    let root = TempDir::new().unwrap();
    let reference = write_artifact(&root, 0x11);
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));
    let empty = reachability([]);

    let first = collector.run_cycle(&empty, u64::MAX).unwrap();
    assert_eq!(first.generation().get(), 1);
    assert_eq!(first.quarantined(), 1);
    assert_eq!(first.deleted(), 0);
    assert!(!root.path().join(reference.relative_path()).exists());
    let quarantined = root
        .path()
        .join("quarantine/q-0000000000000001")
        .join(reference.relative_path());
    assert!(quarantined.exists());

    let second = collector.run_cycle(&empty, u64::MAX).unwrap();
    assert_eq!(second.generation().get(), 2);
    assert_eq!(second.quarantined(), 0);
    assert_eq!(second.deleted(), 1);
    assert!(!quarantined.exists());
}

#[test]
fn data_and_index_artifacts_share_the_same_two_cycle_rule() {
    let root = TempDir::new().unwrap();
    let references = write_artifact_pair(&root);
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));
    let empty = reachability([]);

    let first = collector.run_cycle(&empty, u64::MAX).unwrap();
    assert_eq!(first.quarantined(), 2);
    for reference in references {
        assert!(!root.path().join(reference.relative_path()).exists());
    }
    let second = collector.run_cycle(&empty, u64::MAX).unwrap();
    assert_eq!(second.deleted(), 2);
}

#[test]
fn latest_supported_catalog_minor_shares_the_metadata_two_cycle_rule() {
    let root = TempDir::new().unwrap();
    let references = write_metadata_members(&root);
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));
    let empty = ArtifactReachability::from_members([], ReachabilityLimits::default()).unwrap();

    let first = collector.run_cycle(&empty, u64::MAX).unwrap();
    assert_eq!(first.quarantined(), 3, "{first:?}");
    for reference in &references {
        assert!(!root.path().join(reference.relative_path()).exists());
    }
    let second = collector.run_cycle(&empty, u64::MAX).unwrap();
    assert_eq!(second.deleted(), 3);
    assert!(!root.path().join("quarantine/q-0000000000000001").exists());
}

#[test]
fn future_catalog_minor_fails_closed_before_gc_mutates_any_member() {
    let root = TempDir::new().unwrap();
    let references = write_metadata_members(&root);
    let catalog_path = root.path().join(references[0].relative_path());
    let mut bytes = fs::read(&catalog_path).unwrap();
    bytes[10..12].copy_from_slice(
        &radixdb_catalog::LATEST_CATALOG_MINOR
            .checked_add(1)
            .unwrap()
            .to_le_bytes(),
    );
    let crc = radixdb_core::crc32_ieee(&bytes[..248]);
    bytes[248..252].copy_from_slice(&crc.to_le_bytes());
    fs::write(&catalog_path, bytes).unwrap();

    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));
    let empty = ArtifactReachability::from_members([], ReachabilityLimits::default()).unwrap();
    assert!(matches!(
        collector.run_cycle(&empty, u64::MAX),
        Err(FormatError::InvalidCleanup { .. })
    ));
    for reference in references {
        assert!(root.path().join(reference.relative_path()).exists());
    }
    assert!(!root.path().join("quarantine").exists());
}

#[test]
fn nonzero_manifest_minor_fails_closed_before_gc_mutates_any_member() {
    let root = TempDir::new().unwrap();
    let references = write_metadata_members(&root);
    let manifest_path = root.path().join(references[1].relative_path());
    let mut bytes = fs::read(&manifest_path).unwrap();
    bytes[10..12].copy_from_slice(&1_u16.to_le_bytes());
    let crc = radixdb_core::crc32_ieee(&bytes[..248]);
    bytes[248..252].copy_from_slice(&crc.to_le_bytes());
    fs::write(&manifest_path, bytes).unwrap();

    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));
    let empty = ArtifactReachability::from_members([], ReachabilityLimits::default()).unwrap();
    assert!(matches!(
        collector.run_cycle(&empty, u64::MAX),
        Err(FormatError::InvalidCleanup { .. })
    ));
    for reference in references {
        assert!(root.path().join(reference.relative_path()).exists());
    }
    assert!(!root.path().join("quarantine").exists());
}

#[test]
fn reachable_catalog_and_manifests_are_never_moved() {
    let root = TempDir::new().unwrap();
    let references = write_metadata_members(&root);
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));
    let reachable = ArtifactReachability::from_members(
        references.iter().copied(),
        ReachabilityLimits::default(),
    )
    .unwrap();

    let report = collector.run_cycle(&reachable, u64::MAX).unwrap();
    assert_eq!(report.reachable(), 3);
    assert_eq!(report.quarantined(), 0);
    for reference in references {
        assert!(root.path().join(reference.relative_path()).exists());
    }
}

#[test]
fn reachable_artifact_is_never_moved() {
    let root = TempDir::new().unwrap();
    let reference = write_artifact(&root, 0x21);
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));

    let report = collector
        .run_cycle(&reachability([reference]), u64::MAX)
        .unwrap();
    assert_eq!(report.reachable(), 1);
    assert_eq!(report.quarantined(), 0);
    assert_eq!(report.deleted(), 0);
    assert!(root.path().join(reference.relative_path()).exists());
}

#[test]
fn artifact_that_reappears_in_roots_is_restored_before_other_cleanup() {
    let root = TempDir::new().unwrap();
    let reference = write_artifact(&root, 0x31);
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));

    collector.run_cycle(&reachability([]), u64::MAX).unwrap();
    assert!(!root.path().join(reference.relative_path()).exists());

    let report = collector
        .run_cycle(&reachability([reference]), u64::MAX)
        .unwrap();
    assert_eq!(report.restored(), 1);
    assert_eq!(report.deleted(), 0);
    assert!(root.path().join(reference.relative_path()).exists());
}

#[test]
fn metadata_that_reappears_in_roots_is_restored_before_deletion() {
    let root = TempDir::new().unwrap();
    let references = write_metadata_members(&root);
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));
    let empty = ArtifactReachability::from_members([], ReachabilityLimits::default()).unwrap();

    let first = collector.run_cycle(&empty, u64::MAX).unwrap();
    assert_eq!(first.quarantined(), 3, "{first:?}");
    for reference in &references {
        assert!(!root.path().join(reference.relative_path()).exists());
    }

    let reachable = ArtifactReachability::from_members(
        references.iter().copied(),
        ReachabilityLimits::default(),
    )
    .unwrap();
    let second = collector.run_cycle(&reachable, u64::MAX).unwrap();
    assert_eq!(second.restored(), 3, "{second:?}");
    assert_eq!(second.deleted(), 0, "{second:?}");
    for reference in references {
        assert!(root.path().join(reference.relative_path()).exists());
    }
}

#[test]
fn complete_enumeration_failure_prevents_any_mutation() {
    let root = TempDir::new().unwrap();
    let reference = write_artifact(&root, 0x41);
    fs::write(root.path().join("artifacts/unknown"), b"noise").unwrap();
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));

    assert!(matches!(
        collector.run_cycle(&reachability([]), u64::MAX),
        Err(FormatError::InvalidCleanup { .. })
    ));
    assert!(root.path().join(reference.relative_path()).exists());
    assert!(!root.path().join("quarantine").exists());
}

#[test]
fn corrupt_candidate_prevents_any_mutation() {
    let root = TempDir::new().unwrap();
    let first = write_artifact(&root, 0x51);
    let second = write_artifact(&root, 0x61);
    let second_path = root.path().join(second.relative_path());
    let mut bytes = fs::read(&second_path).unwrap();
    bytes[200] ^= 1;
    fs::write(&second_path, bytes).unwrap();
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));

    assert!(matches!(
        collector.run_cycle(&reachability([]), u64::MAX),
        Err(FormatError::InvalidCleanup { .. })
    ));
    assert!(root.path().join(first.relative_path()).exists());
    assert!(second_path.exists());
    assert!(!root.path().join("quarantine").exists());
}

#[test]
fn corrupt_metadata_candidate_prevents_any_mutation() {
    let root = TempDir::new().unwrap();
    let references = write_metadata_members(&root);
    let corrupt_path = root.path().join(references[2].relative_path());
    let mut bytes = fs::read(&corrupt_path).unwrap();
    bytes[100] ^= 1;
    fs::write(&corrupt_path, bytes).unwrap();
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));
    let empty = ArtifactReachability::from_members([], ReachabilityLimits::default()).unwrap();

    assert!(matches!(
        collector.run_cycle(&empty, u64::MAX),
        Err(FormatError::InvalidCleanup { .. })
    ));
    for reference in references {
        assert!(root.path().join(reference.relative_path()).exists());
    }
    assert!(!root.path().join("quarantine").exists());
}

#[test]
fn payload_corruption_does_not_turn_gc_into_an_implicit_full_scrub() {
    let root = TempDir::new().unwrap();
    let reference = write_artifact(&root, 0x62);
    let path = root.path().join(reference.relative_path());
    let mut bytes = fs::read(&path).unwrap();
    bytes[300] ^= 1;
    fs::write(&path, bytes).unwrap();
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));

    let report = collector.run_cycle(&reachability([]), u64::MAX).unwrap();

    assert_eq!(report.quarantined(), 1);
    assert!(!path.exists());
}

#[test]
fn explicit_scrub_recomputes_the_whole_artifact_identity() {
    let root = TempDir::new().unwrap();
    let reference = write_artifact(&root, 0x63);
    let generation = snapshot(reference);

    let report = crate::v6::scrub_generation_artifacts(root.path(), &generation).unwrap();
    assert_eq!(report.data_artifacts(), 1);
    assert_eq!(report.index_artifacts(), 0);
    assert_eq!(report.body_bytes(), reference.byte_length() - 48);

    let path = root.path().join(reference.relative_path());
    let mut bytes = fs::read(&path).unwrap();
    bytes[300] ^= 1;
    fs::write(path, bytes).unwrap();

    assert!(matches!(
        crate::v6::scrub_generation_artifacts(root.path(), &generation),
        Err(FormatError::DataArtifactChecksumMismatch {
            scope: "whole-file identity"
        })
    ));
}

#[test]
fn explicit_scrub_covers_data_and_index_members() {
    let root = TempDir::new().unwrap();
    let [data, index] = write_artifact_pair(&root);
    let generation = snapshot_with_index(data, Some(index));

    let report = crate::v6::scrub_generation_artifacts(root.path(), &generation).unwrap();
    assert_eq!(report.data_artifacts(), 1);
    assert_eq!(report.index_artifacts(), 1);
    assert_eq!(
        report.body_bytes(),
        data.byte_length() + index.byte_length() - 96
    );

    let path = root.path().join(index.relative_path());
    let mut bytes = fs::read(&path).unwrap();
    let last_body_byte = bytes.len() - 49;
    bytes[last_body_byte] ^= 1;
    fs::write(path, bytes).unwrap();

    assert!(matches!(
        crate::v6::scrub_generation_artifacts(root.path(), &generation),
        Err(FormatError::IndexArtifactChecksumMismatch {
            scope: "whole-file identity"
        })
    ));
}

#[test]
fn soft_rename_budget_defers_work_without_losing_candidates() {
    let root = TempDir::new().unwrap();
    let first = write_artifact(&root, 0x71);
    let second = write_artifact(&root, 0x81);
    let collector = ArtifactGarbageCollector::new(root.path(), limits(1, 10));

    let report = collector.run_cycle(&reachability([]), u64::MAX).unwrap();
    assert_eq!(report.quarantined(), 1);
    assert_eq!(report.deferred(), 1);
    let remaining = [first, second]
        .into_iter()
        .filter(|reference| root.path().join(reference.relative_path()).exists())
        .count();
    assert_eq!(remaining, 1);
}

#[cfg(unix)]
#[test]
fn symlink_candidate_fails_closed_before_cycle_creation() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().unwrap();
    let (_, reference) = artifact(0x91);
    let path = root.path().join(reference.relative_path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    symlink("/dev/null", &path).unwrap();
    let collector = ArtifactGarbageCollector::new(root.path(), limits(10, 10));

    assert!(matches!(
        collector.run_cycle(&reachability([]), u64::MAX),
        Err(FormatError::InvalidCleanup { .. })
    ));
    assert!(path.symlink_metadata().unwrap().file_type().is_symlink());
    assert!(!root.path().join("quarantine").exists());
}

#[test]
fn reachability_rejects_one_id_with_different_exact_identity() {
    let (_, reference) = artifact(0xa1);
    let changed = crate::v6::ArtifactRef::new(
        reference.id(),
        reference.kind(),
        reference.creation_generation(),
        reference.byte_length(),
        [0x55; 32],
    )
    .unwrap();
    assert!(matches!(
        ArtifactReachability::from_artifacts([reference, changed], ReachabilityLimits::default()),
        Err(FormatError::InvalidCleanup { .. })
    ));
}

#[test]
fn control_and_strong_lease_roots_feed_the_same_exact_reachability_set() {
    let (_, reference) = artifact(0xc1);
    let root = snapshot(reference);
    let publisher = PhysicalGenerationPublisher::for_runtime_tests(root.clone());
    let lease = publisher.pin().unwrap();
    let leases = publisher.active_leases();

    let reachable =
        ArtifactReachability::build(&[root], &[], &leases, ReachabilityLimits::default()).unwrap();
    assert!(reachable.contains_artifact(reference));
    assert_eq!(reachable.len(), 4);

    drop(lease);
    assert!(publisher.active_leases().generations().len() == 1);
    drop(leases);
    assert!(publisher.active_leases().generations().is_empty());
}
