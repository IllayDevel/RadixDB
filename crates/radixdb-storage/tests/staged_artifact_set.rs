use std::io::Write;

use radixdb_storage::v6::{
    decode_staging_complete, decode_staging_owner, discover_staging_publications,
    encode_staging_complete, encode_staging_owner, DatabaseGeneration, FormatError, PublicationId,
    StagedArtifactSet, StagingComplete, StagingCompletion, StagingDiscoveryLimits,
    StagingDisposition, StagingOwner, WriterInstanceId, STAGING_COMPLETE_BYTES,
    STAGING_OWNER_BYTES,
};

#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{
    GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode, StagedMemberRole,
};

fn identity(marker: u8) -> [u8; 16] {
    let mut bytes = [marker; 16];
    bytes[15] = marker.wrapping_add(1);
    bytes
}

fn writer(marker: u8) -> WriterInstanceId {
    WriterInstanceId::from_bytes(identity(marker)).unwrap()
}

fn publication(marker: u8) -> PublicationId {
    PublicationId::from_bytes(identity(marker)).unwrap()
}

fn generation(value: u64) -> DatabaseGeneration {
    DatabaseGeneration::new(value).unwrap()
}

fn owner() -> StagingOwner {
    StagingOwner::new(writer(1), publication(2), 41, 1_000, 1_010, generation(7)).unwrap()
}

fn create_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("staging")).unwrap();
    root
}

fn create_set(
    root: &tempfile::TempDir,
    writer_id: WriterInstanceId,
    publication_id: PublicationId,
    created: u64,
) -> StagedArtifactSet {
    StagedArtifactSet::create_with_publication_id(
        root.path().join("staging"),
        writer_id,
        publication_id,
        generation(7),
        41,
        created,
    )
    .unwrap()
}

#[test]
fn owner_and_complete_records_are_exact_and_checksummed() {
    let owner = owner();
    let bytes = encode_staging_owner(owner);
    assert_eq!(bytes.len(), STAGING_OWNER_BYTES);
    assert_eq!(decode_staging_owner(&bytes).unwrap(), owner);

    let complete = StagingComplete::new(owner, 3, [0x55; 32], 1_020).unwrap();
    let complete_bytes = encode_staging_complete(complete);
    assert_eq!(complete_bytes.len(), STAGING_COMPLETE_BYTES);
    assert_eq!(decode_staging_complete(&complete_bytes).unwrap(), complete);

    let mut corrupt = bytes;
    corrupt[64] ^= 1;
    assert!(matches!(
        decode_staging_owner(&corrupt),
        Err(FormatError::StagingChecksumMismatch { record: "OWNER" })
    ));
    assert!(decode_staging_complete(&complete_bytes[..127]).is_err());

    let mut reserved = complete_bytes;
    reserved[112] = 1;
    let crc = radixdb_core::crc32_ieee(&reserved[..124]);
    reserved[124..].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(
        decode_staging_complete(&reserved),
        Err(FormatError::InvalidStagingRecord {
            record: "COMPLETE",
            ..
        })
    ));
}

#[test]
fn identities_accept_only_canonical_lowercase_hex() {
    let expected = publication(0xab);
    let canonical = expected.to_string();
    assert_eq!(canonical.parse::<PublicationId>().unwrap(), expected);
    assert!(canonical
        .to_ascii_uppercase()
        .parse::<PublicationId>()
        .is_err());
}

#[test]
fn complete_set_binds_members_and_discovery_is_idempotent() {
    let root = create_root();
    let staged = create_set(&root, writer(4), publication(5), 1_000);
    staged
        .write_file("artifacts/data/05/example.data", |file| {
            file.write_all(b"authoritative-data")
                .map_err(|error| FormatError::StagingIo {
                    operation: "test write",
                    kind: error.kind(),
                })?;
            Ok(())
        })
        .unwrap();
    staged
        .write_file("artifacts/index/05/example.idx", |file| {
            file.write_all(b"derived-index")
                .map_err(|error| FormatError::StagingIo {
                    operation: "test write",
                    kind: error.kind(),
                })?;
            Ok(())
        })
        .unwrap();
    let complete = staged
        .mark_complete(1_020, StagingDiscoveryLimits::default())
        .unwrap();
    assert_eq!(complete.complete().member_count(), 2);
    assert!(complete.path().join("OWNER").is_file());
    assert!(complete.path().join("COMPLETE").is_file());

    let first = discover_staging_publications(
        root.path().join("staging"),
        1_050,
        100,
        StagingDiscoveryLimits::default(),
    )
    .unwrap();
    let second = discover_staging_publications(
        root.path().join("staging"),
        1_050,
        100,
        StagingDiscoveryLimits::default(),
    )
    .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.publications().len(), 1);
    assert!(matches!(
        first.publications()[0].completion(),
        StagingCompletion::Complete(marker) if marker.member_count() == 2
    ));
    assert_eq!(
        first.publications()[0].disposition(),
        StagingDisposition::Active
    );

    let aged = discover_staging_publications(
        root.path().join("staging"),
        1_200,
        100,
        StagingDiscoveryLimits::default(),
    )
    .unwrap();
    assert_eq!(
        aged.publications()[0].disposition(),
        StagingDisposition::AgedOrphanCandidate
    );
    assert!(complete.path().exists(), "discovery must remain read-only");
}

#[test]
fn completion_is_idempotent_and_closes_member_admission() {
    let root = create_root();
    let staged = create_set(&root, writer(6), publication(7), 1_000);
    staged
        .write_file("artifacts/data/07/member.data", |file| {
            file.write_all(b"bytes")
                .map_err(|error| FormatError::StagingIo {
                    operation: "test write",
                    kind: error.kind(),
                })?;
            Ok(())
        })
        .unwrap();
    let first = staged
        .mark_complete(1_020, StagingDiscoveryLimits::default())
        .unwrap();
    let second = staged
        .mark_complete(1_030, StagingDiscoveryLimits::default())
        .unwrap();
    assert_eq!(first, second);
    assert!(staged
        .write_file("late.data", |_| Ok::<_, FormatError>(()))
        .is_err());
}

#[test]
fn incomplete_old_owner_is_only_an_orphan_candidate() {
    let root = create_root();
    let staged = create_set(&root, writer(8), publication(9), 1_000);
    let discovery = discover_staging_publications(
        root.path().join("staging"),
        2_000,
        500,
        StagingDiscoveryLimits::default(),
    )
    .unwrap();
    let candidate = &discovery.publications()[0];
    assert_eq!(candidate.completion(), StagingCompletion::Incomplete);
    assert_eq!(
        candidate.disposition(),
        StagingDisposition::AgedOrphanCandidate
    );
    assert!(staged.path().exists(), "discovery cannot delete candidates");
}

#[test]
fn invalid_or_missing_owner_requires_quarantine() {
    let root = create_root();
    let writer_path = root.path().join("staging").join(writer(10).to_string());
    let missing_owner = writer_path.join(publication(11).to_string());
    std::fs::create_dir_all(&missing_owner).unwrap();
    let invalid_owner = writer_path.join(publication(12).to_string());
    std::fs::create_dir(&invalid_owner).unwrap();
    std::fs::write(invalid_owner.join("OWNER"), b"torn").unwrap();

    let discovery = discover_staging_publications(
        root.path().join("staging"),
        2_000,
        500,
        StagingDiscoveryLimits::default(),
    )
    .unwrap();
    assert_eq!(discovery.publications().len(), 2);
    assert!(discovery.publications().iter().all(|entry| {
        entry.disposition() == StagingDisposition::QuarantineRequired && entry.owner().is_none()
    }));
}

#[test]
fn member_mutation_after_complete_invalidates_marker() {
    let root = create_root();
    let staged = create_set(&root, writer(13), publication(14), 1_000);
    staged
        .write_file("artifacts/data/0e/member.data", |file| {
            file.write_all(b"before")
                .map_err(|error| FormatError::StagingIo {
                    operation: "test write",
                    kind: error.kind(),
                })?;
            Ok(())
        })
        .unwrap();
    staged
        .mark_complete(1_010, StagingDiscoveryLimits::default())
        .unwrap();
    std::fs::write(
        staged.path().join("artifacts/data/0e/member.data"),
        b"after",
    )
    .unwrap();

    let discovery = discover_staging_publications(
        root.path().join("staging"),
        1_020,
        100,
        StagingDiscoveryLimits::default(),
    )
    .unwrap();
    assert_eq!(
        discovery.publications()[0].completion(),
        StagingCompletion::Invalid
    );
    assert_eq!(
        discovery.publications()[0].disposition(),
        StagingDisposition::QuarantineRequired
    );
}

#[test]
fn failed_writer_removes_partial_member_and_unsafe_paths_are_rejected() {
    let root = create_root();
    let staged = create_set(&root, writer(15), publication(16), 1_000);
    let error = staged.write_file("artifacts/data/10/partial.data", |file| {
        file.write_all(b"partial")
            .map_err(|error| FormatError::StagingIo {
                operation: "test write",
                kind: error.kind(),
            })?;
        Err::<(), _>(FormatError::InvalidStagingRecord {
            record: "test",
            detail: "injected",
        })
    });
    assert!(error.is_err());
    assert!(!staged
        .path()
        .join("artifacts/data/10/partial.data")
        .exists());
    for path in ["../escape", "/absolute", "OWNER", "a\\b", "a//b"] {
        assert!(staged
            .write_file(path, |_| Ok::<_, FormatError>(()))
            .is_err());
    }
}

#[test]
fn discovery_limits_abort_without_partial_success() {
    let root = create_root();
    let writer_id = writer(17);
    let _first = create_set(&root, writer_id, publication(18), 1_000);
    let _second = create_set(&root, writer_id, publication(19), 1_000);
    let limits = StagingDiscoveryLimits::new(1, 10, 4_096, 16, 4_096).unwrap();
    assert!(matches!(
        discover_staging_publications(root.path().join("staging"), 1_000, 100, limits),
        Err(FormatError::StagingLimitExceeded {
            field: "publication count",
            ..
        })
    ));
}

#[test]
fn member_limit_is_checked_before_complete_marker_publication() {
    let root = create_root();
    let staged = create_set(&root, writer(23), publication(24), 1_000);
    staged
        .write_file("first.data", |file| {
            file.write_all(b"first")
                .map_err(|error| FormatError::StagingIo {
                    operation: "test write",
                    kind: error.kind(),
                })?;
            Ok(())
        })
        .unwrap();
    staged
        .write_file("second.data", |file| {
            file.write_all(b"second")
                .map_err(|error| FormatError::StagingIo {
                    operation: "test write",
                    kind: error.kind(),
                })?;
            Ok(())
        })
        .unwrap();
    let limits = StagingDiscoveryLimits::new(10, 1, 4_096, 16, 4_096).unwrap();
    assert!(matches!(
        staged.mark_complete(1_010, limits),
        Err(FormatError::StagingLimitExceeded {
            field: "filesystem entries per publication",
            actual: 2,
            limit: 1,
        })
    ));
    assert!(!staged.path().join("COMPLETE").exists());
}

#[cfg(unix)]
#[test]
fn symlinked_member_cannot_validate_as_complete() {
    use std::os::unix::fs::symlink;

    let root = create_root();
    let staged = create_set(&root, writer(20), publication(21), 1_000);
    let outside = root.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    std::fs::create_dir_all(staged.path().join("artifacts/data/15")).unwrap();
    symlink(outside, staged.path().join("artifacts/data/15/member.data")).unwrap();
    assert!(matches!(
        staged.mark_complete(1_010, StagingDiscoveryLimits::default()),
        Err(FormatError::InvalidStagingRecord { .. })
    ));
}

#[test]
fn generated_publication_ids_are_distinct() {
    let root = create_root();
    let writer_id = writer(22);
    let first = StagedArtifactSet::create(
        root.path().join("staging"),
        writer_id,
        generation(7),
        41,
        1_000,
    )
    .unwrap();
    let second = StagedArtifactSet::create(
        root.path().join("staging"),
        writer_id,
        generation(8),
        41,
        1_001,
    )
    .unwrap();
    assert_ne!(
        first.owner().publication_id(),
        second.owner().publication_id()
    );
    assert_ne!(first.path(), second.path());
}

#[cfg(feature = "test-failpoints")]
#[test]
fn lifecycle_faults_hit_exact_typed_staging_boundaries_once() {
    for (index, point) in [
        GenerationCrashPoint::StageOwnerBeforeWrite,
        GenerationCrashPoint::StageOwnerAfterSync,
    ]
    .into_iter()
    .enumerate()
    {
        let root = create_root();
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        let result = StagedArtifactSet::create_with_publication_id(
            root.path().join("staging"),
            writer(30 + index as u8),
            publication(40 + index as u8),
            generation(7),
            41,
            1_000,
        );
        assert!(result.is_err(), "{} must interrupt staging", point.name());
        assert_eq!(guard.hit_count(), 1, "{} was not traversed", point.name());
    }

    let cases = [
        (
            StagedMemberRole::Data,
            GenerationCrashPoint::DataAfterFileSync,
            "artifacts/data/32/member.data",
        ),
        (
            StagedMemberRole::Index,
            GenerationCrashPoint::IndexAfterFileSync,
            "artifacts/index/34/member.idx",
        ),
        (
            StagedMemberRole::TableManifest,
            GenerationCrashPoint::TableManifestAfterFileSync,
            "manifests/tables/35/table.mft",
        ),
        (
            StagedMemberRole::CatalogPack,
            GenerationCrashPoint::CatalogPackAfterFileSync,
            "catalog/catalog-36.cat",
        ),
        (
            StagedMemberRole::WalSuccessor,
            GenerationCrashPoint::WalSuccessorAfterCreateBeforeSync,
            "wal/wal-37.log",
        ),
        (
            StagedMemberRole::WalSuccessor,
            GenerationCrashPoint::WalSuccessorDurable,
            "wal/wal-38.log",
        ),
        (
            StagedMemberRole::DatabaseManifest,
            GenerationCrashPoint::DatabaseManifestAfterFileSync,
            "manifests/database-39.mft",
        ),
    ];
    for (index, (role, point, path)) in cases.into_iter().enumerate() {
        let root = create_root();
        let staged = create_set(
            &root,
            writer(50 + index as u8),
            publication(70 + index as u8),
            1_000,
        );
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        let result = staged.write_generation_file(role, path, |file| {
            file.write_all(b"complete member")
                .map_err(|error| FormatError::StagingIo {
                    operation: "test typed write",
                    kind: error.kind(),
                })
        });
        assert!(
            result.is_err(),
            "{} must interrupt member write",
            point.name()
        );
        assert_eq!(guard.hit_count(), 1, "{} was not traversed", point.name());
        assert!(!staged.path().join(path).exists());
    }
}
