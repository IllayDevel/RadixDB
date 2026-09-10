use std::path::{Path, PathBuf};
#[cfg(feature = "test-failpoints")]
use std::process::{Command, Stdio};

use radixdb_catalog::ObjectId;
use radixdb_storage::v6::{
    commit_snapshot_manifest, decode_snapshot_manifest, encode_snapshot_manifest,
    open_snapshot_manifest, ArtifactId, ArtifactKind, ArtifactRef, CatalogGeneration, CatalogId,
    CatalogRef, CatalogRootRef, ControlRecord, ControlSlotIndex, DatabaseGeneration, DatabaseId,
    DatabaseManifest, DatabaseManifestRootRef, FormatError, ManifestGeneration, ManifestId,
    ManifestKind, ManifestRef, PhysicalGenerationSnapshot, SegmentDescriptor, SegmentId,
    SegmentKind, SnapshotId, SnapshotIndexPolicy, SnapshotManifest, SnapshotMember,
    SnapshotMemberKind, TableManifest, TableManifestRef, WalGeneration, WalReplayFloor,
    WriterInstanceId, SNAPSHOT_MANIFEST_FILE,
};
#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn common_member_bytes(marker: u8) -> (Vec<u8>, [u8; 32]) {
    let body = vec![marker; 256];
    let body_sha256 = radixdb_core::sha256_digest(&body);
    let mut bytes = body;
    let file_length = bytes.len() as u64 + 48;
    bytes.extend_from_slice(b"RDX6END\0");
    bytes.extend_from_slice(&file_length.to_le_bytes());
    bytes.extend_from_slice(&body_sha256);
    (bytes, body_sha256)
}

fn member(
    kind: SnapshotMemberKind,
    marker: u8,
    generation: u64,
    optional: bool,
    bytes: &[u8],
    body_sha256: [u8; 32],
) -> SnapshotMember {
    let suffix = match kind {
        SnapshotMemberKind::Data => 1,
        SnapshotMemberKind::Index => 2,
        SnapshotMemberKind::DatabaseManifest | SnapshotMemberKind::TableManifest => 3,
        SnapshotMemberKind::Catalog => 4,
        SnapshotMemberKind::Wal => 5,
    };
    let shard = if matches!(kind, SnapshotMemberKind::Data | SnapshotMemberKind::Index) {
        u16::from(marker)
    } else {
        0
    };
    SnapshotMember::from_persisted(
        kind,
        if matches!(kind, SnapshotMemberKind::Data | SnapshotMemberKind::Index) {
            1
        } else {
            6
        },
        u32::from(optional),
        raw(marker),
        generation,
        bytes.len() as u64,
        body_sha256,
        shard,
        suffix,
    )
    .unwrap()
}

struct SnapshotFixture {
    _root: tempfile::TempDir,
    directory: PathBuf,
    manifest: SnapshotManifest,
}

impl SnapshotFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let snapshot_id = SnapshotId::from_bytes(raw(0x11)).unwrap();
        let directory = root.path().join(snapshot_id.to_string());
        std::fs::create_dir(&directory).unwrap();

        let (database_bytes, database_sha) = common_member_bytes(0x21);
        let (catalog_bytes, catalog_sha) = common_member_bytes(0x22);
        let (data_bytes, data_sha) = common_member_bytes(0x31);
        let (index_bytes, index_sha) = common_member_bytes(0x41);
        let wal_bytes = b"complete WAL member".to_vec();
        let wal_sha = radixdb_core::sha256_digest(&wal_bytes);
        let database_id = DatabaseId::from_bytes(raw(0x12)).unwrap();
        let generation = DatabaseGeneration::new(9).unwrap();
        let database_id_ref = ManifestId::from_bytes(raw(0x21)).unwrap();
        let catalog_id = CatalogId::from_bytes(raw(0x22)).unwrap();
        let wal = SnapshotMember::wal(
            database_id,
            WalGeneration::new(9).unwrap(),
            wal_bytes.len() as u64,
            wal_sha,
        )
        .unwrap();
        let members = vec![
            wal,
            member(
                SnapshotMemberKind::Index,
                0x41,
                8,
                true,
                &index_bytes,
                index_sha,
            ),
            member(
                SnapshotMemberKind::Data,
                0x31,
                8,
                false,
                &data_bytes,
                data_sha,
            ),
            member(
                SnapshotMemberKind::Catalog,
                0x22,
                4,
                false,
                &catalog_bytes,
                catalog_sha,
            ),
            member(
                SnapshotMemberKind::DatabaseManifest,
                0x21,
                9,
                false,
                &database_bytes,
                database_sha,
            ),
        ];
        let manifest = SnapshotManifest::new(
            snapshot_id,
            database_id,
            generation,
            DatabaseManifestRootRef::new(
                database_id_ref,
                ManifestGeneration::new(9).unwrap(),
                database_sha,
            ),
            CatalogRootRef::new(catalog_id, CatalogGeneration::new(4).unwrap(), catalog_sha),
            members,
            1_000,
        )
        .unwrap();
        for (member, bytes) in manifest.members().iter().copied().zip([
            database_bytes,
            catalog_bytes,
            data_bytes,
            index_bytes,
            wal_bytes,
        ]) {
            write_member(&directory, member, &bytes);
        }
        Self {
            _root: root,
            directory,
            manifest,
        }
    }
}

fn write_member(root: &Path, member: SnapshotMember, bytes: &[u8]) {
    let path = root.join(member.relative_path());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn repair_checksums(bytes: &mut [u8]) {
    let header_crc = radixdb_core::crc32_ieee(&bytes[..248]);
    bytes[248..252].copy_from_slice(&header_crc.to_le_bytes());
    let footer = bytes.len() - 48;
    let body_sha256 = radixdb_core::sha256_digest(&bytes[..footer]);
    bytes[footer + 16..].copy_from_slice(&body_sha256);
}

#[test]
fn snapshot_manifest_round_trip_is_canonical_and_prefix_closed() {
    let fixture = SnapshotFixture::new();
    let bytes = encode_snapshot_manifest(&fixture.manifest).unwrap();
    assert_eq!(decode_snapshot_manifest(&bytes).unwrap(), fixture.manifest);
    assert_eq!(
        encode_snapshot_manifest(&decode_snapshot_manifest(&bytes).unwrap()).unwrap(),
        bytes
    );
    for cut in 0..bytes.len() {
        assert!(
            decode_snapshot_manifest(&bytes[..cut]).is_err(),
            "cut={cut}"
        );
    }
}

#[test]
fn snapshot_manifest_rejects_unknown_flags_locators_reserved_bytes_and_corruption() {
    let fixture = SnapshotFixture::new();
    let canonical = encode_snapshot_manifest(&fixture.manifest).unwrap();

    let mut corrupt = canonical.clone();
    corrupt[300] ^= 0x80;
    assert!(matches!(
        decode_snapshot_manifest(&corrupt),
        Err(FormatError::SnapshotChecksumMismatch { .. })
    ));

    let mut header_flags = canonical.clone();
    header_flags[128..136].copy_from_slice(&3_u64.to_le_bytes());
    repair_checksums(&mut header_flags);
    assert!(matches!(
        decode_snapshot_manifest(&header_flags),
        Err(FormatError::InvalidSnapshot { .. })
    ));

    let mut reserved = canonical.clone();
    reserved[200] = 1;
    repair_checksums(&mut reserved);
    assert!(matches!(
        decode_snapshot_manifest(&reserved),
        Err(FormatError::InvalidSnapshot { .. })
    ));

    let mut bad_locator = canonical;
    bad_locator[256 + 74..256 + 76].copy_from_slice(&5_u16.to_le_bytes());
    repair_checksums(&mut bad_locator);
    assert!(matches!(
        decode_snapshot_manifest(&bad_locator),
        Err(FormatError::InvalidSnapshot { .. })
    ));

    let mut unsorted = encode_snapshot_manifest(&fixture.manifest).unwrap();
    let first = unsorted[256..352].to_vec();
    let second = unsorted[352..448].to_vec();
    unsorted[256..352].copy_from_slice(&second);
    unsorted[352..448].copy_from_slice(&first);
    repair_checksums(&mut unsorted);
    assert!(matches!(
        decode_snapshot_manifest(&unsorted),
        Err(FormatError::InvalidSnapshot { .. })
    ));
}

#[test]
fn generation_builder_requires_a_contiguous_wal_range_from_the_replay_floor() {
    let database_id = DatabaseId::from_bytes(raw(0x51)).unwrap();
    let database_generation = DatabaseGeneration::new(7).unwrap();
    let catalog_id = CatalogId::from_bytes(raw(0x52)).unwrap();
    let catalog = CatalogRef::new(
        catalog_id,
        CatalogGeneration::new(3).unwrap(),
        304,
        [0x53; 32],
    )
    .unwrap();
    let floor = WalReplayFloor::new(WalGeneration::new(5).unwrap(), 10);
    let database_manifest = DatabaseManifest::new(
        database_id,
        ManifestId::from_bytes(raw(0x54)).unwrap(),
        database_generation,
        catalog,
        floor,
        0,
        vec![],
        1,
    )
    .unwrap();
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        database_generation,
        database_id,
        DatabaseManifestRootRef::new(
            database_manifest.manifest_id(),
            ManifestGeneration::new(7).unwrap(),
            [0x55; 32],
        ),
        CatalogRootRef::new(catalog_id, CatalogGeneration::new(3).unwrap(), [0x53; 32]),
        floor,
        1,
        WriterInstanceId::from_bytes(raw(0x56)).unwrap(),
    )
    .unwrap();
    let generation = PhysicalGenerationSnapshot::new(control, database_manifest, vec![]).unwrap();
    let wal = |number| {
        SnapshotMember::wal(
            database_id,
            WalGeneration::new(number).unwrap(),
            16,
            [number as u8; 32],
        )
        .unwrap()
    };
    let manifest = SnapshotManifest::from_generation(
        SnapshotId::from_bytes(raw(0x57)).unwrap(),
        &generation,
        304,
        vec![wal(6), wal(5)],
        SnapshotIndexPolicy::OmitRebuildable,
        2,
    )
    .unwrap();
    assert_eq!(manifest.members().len(), 4);
    assert_eq!(
        manifest
            .members()
            .iter()
            .filter(|member| member.kind() == SnapshotMemberKind::Wal)
            .map(|member| member.generation())
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
    assert!(SnapshotManifest::from_generation(
        SnapshotId::from_bytes(raw(0x58)).unwrap(),
        &generation,
        304,
        vec![wal(5), wal(7)],
        SnapshotIndexPolicy::Include,
        2,
    )
    .is_err());
}

#[test]
fn generation_builder_owns_data_once_and_makes_index_exclusion_explicit() {
    let database_id = DatabaseId::from_bytes(raw(0x61)).unwrap();
    let database_generation = DatabaseGeneration::new(7).unwrap();
    let catalog_generation = CatalogGeneration::new(3).unwrap();
    let catalog_id = CatalogId::from_bytes(raw(0x62)).unwrap();
    let catalog = CatalogRef::new(catalog_id, catalog_generation, 304, [0x63; 32]).unwrap();
    let data = ArtifactRef::new(
        ArtifactId::from_bytes(raw(0x64)).unwrap(),
        ArtifactKind::Data,
        database_generation,
        304,
        [0x65; 32],
    )
    .unwrap();
    let index = ArtifactRef::new(
        ArtifactId::from_bytes(raw(0x66)).unwrap(),
        ArtifactKind::Index,
        database_generation,
        304,
        [0x67; 32],
    )
    .unwrap();
    let table_id = ObjectId::from_user_bytes(raw(0x68)).unwrap();
    let table_manifest_id = ManifestId::from_bytes(raw(0x69)).unwrap();
    let manifest_generation = ManifestGeneration::new(7).unwrap();
    let table = TableManifest::new(
        database_id,
        table_id,
        table_manifest_id,
        manifest_generation,
        catalog_generation,
        1,
        2,
        vec![SegmentDescriptor::new(
            SegmentId::from_bytes(raw(0x6a)).unwrap(),
            SegmentKind::Rows,
            1,
            1,
            1,
            1,
            1,
            data,
            Some(index),
        )
        .unwrap()],
        1,
    )
    .unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest_id,
            ManifestKind::Table,
            manifest_generation,
            304,
            [0x6b; 32],
        )
        .unwrap(),
    )
    .unwrap();
    let floor = WalReplayFloor::new(WalGeneration::new(5).unwrap(), 10);
    let database_manifest_id = ManifestId::from_bytes(raw(0x6c)).unwrap();
    let database_manifest = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        database_generation,
        catalog,
        floor,
        10_000,
        vec![table_reference],
        1,
    )
    .unwrap();
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        database_generation,
        database_id,
        DatabaseManifestRootRef::new(database_manifest_id, manifest_generation, [0x6d; 32]),
        CatalogRootRef::new(catalog_id, catalog_generation, [0x63; 32]),
        floor,
        1,
        WriterInstanceId::from_bytes(raw(0x6e)).unwrap(),
    )
    .unwrap();
    let generation =
        PhysicalGenerationSnapshot::new(control, database_manifest, vec![table]).unwrap();
    let wal = || SnapshotMember::wal(database_id, floor.generation(), 16, [0x6f; 32]).unwrap();

    let included = SnapshotManifest::from_generation(
        SnapshotId::from_bytes(raw(0x70)).unwrap(),
        &generation,
        304,
        vec![wal()],
        SnapshotIndexPolicy::Include,
        2,
    )
    .unwrap();
    assert_eq!(
        included
            .members()
            .iter()
            .filter(|member| member.kind() == SnapshotMemberKind::Data)
            .count(),
        1
    );
    assert!(included.members().iter().any(|member| {
        member.kind() == SnapshotMemberKind::Index && member.optional_rebuildable()
    }));

    let omitted = SnapshotManifest::from_generation(
        SnapshotId::from_bytes(raw(0x71)).unwrap(),
        &generation,
        304,
        vec![wal()],
        SnapshotIndexPolicy::OmitRebuildable,
        2,
    )
    .unwrap();
    assert!(omitted
        .members()
        .iter()
        .all(|member| member.kind() != SnapshotMemberKind::Index));
    assert!(omitted
        .members()
        .iter()
        .any(|member| member.kind() == SnapshotMemberKind::Data));
}

#[test]
fn manifest_is_published_last_after_every_typed_member_is_durable() {
    let fixture = SnapshotFixture::new();
    let final_path = fixture.directory.join(SNAPSHOT_MANIFEST_FILE);
    assert!(!final_path.exists());
    commit_snapshot_manifest(&fixture.directory, &fixture.manifest).unwrap();
    assert!(final_path.exists());
    assert_eq!(
        open_snapshot_manifest(&fixture.directory).unwrap(),
        fixture.manifest
    );
    assert!(commit_snapshot_manifest(&fixture.directory, &fixture.manifest).is_err());
}

#[test]
fn absent_or_corrupt_required_member_never_publishes_a_snapshot_root() {
    let missing = SnapshotFixture::new();
    let member_path = missing
        .directory
        .join(missing.manifest.members()[0].relative_path());
    std::fs::remove_file(member_path).unwrap();
    assert!(commit_snapshot_manifest(&missing.directory, &missing.manifest).is_err());
    assert!(!missing.directory.join(SNAPSHOT_MANIFEST_FILE).exists());

    let corrupt = SnapshotFixture::new();
    let member_path = corrupt
        .directory
        .join(corrupt.manifest.members()[1].relative_path());
    let mut bytes = std::fs::read(&member_path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(member_path, bytes).unwrap();
    assert!(matches!(
        commit_snapshot_manifest(&corrupt.directory, &corrupt.manifest),
        Err(FormatError::SnapshotChecksumMismatch { .. })
    ));
    assert!(!corrupt.directory.join(SNAPSHOT_MANIFEST_FILE).exists());
}

#[cfg(feature = "test-failpoints")]
#[test]
fn io_error_matrix_never_exposes_a_partial_snapshot() {
    for point in GenerationCrashPoint::SNAPSHOT_PUBLICATION_POINTS {
        let fixture = SnapshotFixture::new();
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        assert!(commit_snapshot_manifest(&fixture.directory, &fixture.manifest).is_err());
        assert_eq!(guard.hit_count(), 1, "point={}", point.name());
        drop(guard);
        let final_path = fixture.directory.join(SNAPSHOT_MANIFEST_FILE);
        if final_path.exists() {
            assert_eq!(
                open_snapshot_manifest(&fixture.directory).unwrap(),
                fixture.manifest
            );
        } else {
            assert!(matches!(
                point,
                GenerationCrashPoint::SnapshotMemberAfterWriteBeforeSync
                    | GenerationCrashPoint::SnapshotMemberDurable
                    | GenerationCrashPoint::SnapshotManifestAfterSyncBeforeRename
            ));
        }
    }
}

#[cfg(feature = "test-failpoints")]
#[test]
#[ignore = "child process entrypoint for the physical snapshot abort matrix"]
fn snapshot_process_abort_child() {
    let Some(directory) = std::env::var_os("RADIXDB_SNAPSHOT_ABORT_ROOT") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let bytes = std::fs::read(directory.join("INPUT.mft")).unwrap();
    let manifest = decode_snapshot_manifest(&bytes).unwrap();
    commit_snapshot_manifest(directory, &manifest).unwrap();
}

#[cfg(feature = "test-failpoints")]
#[test]
fn process_abort_matrix_exposes_only_absent_or_complete_snapshot_roots() {
    let executable = std::env::current_exe().unwrap();
    for point in GenerationCrashPoint::SNAPSHOT_PUBLICATION_POINTS {
        let fixture = SnapshotFixture::new();
        std::fs::write(
            fixture.directory.join("INPUT.mft"),
            encode_snapshot_manifest(&fixture.manifest).unwrap(),
        )
        .unwrap();
        let evidence = fixture.directory.join("fault.ready");
        let status = Command::new(&executable)
            .arg("--ignored")
            .arg("--exact")
            .arg("snapshot_process_abort_child")
            .env("RADIXDB_SNAPSHOT_ABORT_ROOT", &fixture.directory)
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
        let final_path = fixture.directory.join(SNAPSHOT_MANIFEST_FILE);
        if final_path.exists() {
            assert_eq!(
                open_snapshot_manifest(&fixture.directory).unwrap(),
                fixture.manifest
            );
        } else {
            assert!(matches!(
                point,
                GenerationCrashPoint::SnapshotMemberAfterWriteBeforeSync
                    | GenerationCrashPoint::SnapshotMemberDurable
                    | GenerationCrashPoint::SnapshotManifestAfterSyncBeforeRename
            ));
        }
    }
}
