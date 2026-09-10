use std::collections::BTreeSet;

use radixdb_catalog::ObjectId;
use radixdb_storage::v6::{
    ArtifactId, ArtifactKind, ArtifactRef, ArtifactSliceRef, CatalogGeneration, CatalogId,
    CatalogRef, DatabaseGeneration, DatabaseId, FormatError, IndexSectionKind, IndexSectionRef,
    ManifestGeneration, ManifestId, ManifestKind, ManifestRef, SegmentId, WalGeneration,
    WalReplayFloor, ARTIFACT_CODEC_VERSION, FORMAT_VERSION, MAX_ARTIFACT_FILE_BYTES,
    MAX_CATALOG_FILE_BYTES, MAX_MANIFEST_FILE_BYTES,
};

fn bytes(first: u8, last: u8) -> [u8; 16] {
    let mut value = [first; 16];
    value[15] = last;
    value
}

#[test]
fn durable_identity_types_are_distinct_nonzero_and_canonical() {
    let database = DatabaseId::from_bytes(bytes(0x11, 0x12)).unwrap();
    let catalog = CatalogId::from_bytes(bytes(0x21, 0x22)).unwrap();
    let manifest = ManifestId::from_bytes(bytes(0x31, 0x32)).unwrap();
    let artifact = ArtifactId::from_bytes(bytes(0x41, 0x42)).unwrap();
    let segment = SegmentId::from_bytes(bytes(0x51, 0x52)).unwrap();

    assert_eq!(database.to_string(), "11111111111111111111111111111112");
    assert_eq!(catalog.to_string(), "21212121212121212121212121212122");
    assert_eq!(manifest.to_string(), "31313131313131313131313131313132");
    assert_eq!(artifact.to_string(), "41414141414141414141414141414142");
    assert_eq!(segment.to_string(), "51515151515151515151515151515152");
    assert_eq!(
        database.to_string().parse::<DatabaseId>().unwrap(),
        database
    );
    assert_eq!(catalog.to_string().parse::<CatalogId>().unwrap(), catalog);
    assert_eq!(
        manifest.to_string().parse::<ManifestId>().unwrap(),
        manifest
    );
    assert_eq!(
        artifact.to_string().parse::<ArtifactId>().unwrap(),
        artifact
    );
    assert_eq!(segment.to_string().parse::<SegmentId>().unwrap(), segment);

    assert!(matches!(
        DatabaseId::from_bytes([0; 16]),
        Err(FormatError::ZeroIdentity { .. })
    ));
    assert!("00".parse::<ArtifactId>().is_err());

    let generated = (0..256).map(|_| ArtifactId::new()).collect::<BTreeSet<_>>();
    assert_eq!(generated.len(), 256);
    assert!(generated.iter().all(|id| id.as_bytes() != &[0; 16]));
}

#[test]
fn generation_types_are_nonzero_monotonic_and_do_not_alias() {
    let database = DatabaseGeneration::new(7).unwrap();
    let catalog = CatalogGeneration::new(11).unwrap();
    let manifest = ManifestGeneration::new(13).unwrap();
    let wal = WalGeneration::new(17).unwrap();

    assert_eq!(database.checked_next().unwrap().get(), 8);
    assert_eq!(catalog.get(), 11);
    assert_eq!(manifest.get(), 13);
    assert_eq!(wal.get(), 17);
    assert!(DatabaseGeneration::new(0).is_err());
    assert!(matches!(
        DatabaseGeneration::new(u64::MAX).unwrap().checked_next(),
        Err(FormatError::GenerationOverflow { .. })
    ));

    let floor = WalReplayFloor::new(wal, 200);
    assert_eq!(floor.generation(), wal);
    assert_eq!(floor.lsn(), 200);
    assert!(!floor.starts_after(200));
    assert!(floor.starts_after(201));
}

#[test]
fn metadata_references_bind_kind_version_length_and_hash() {
    let catalog = CatalogRef::new(
        CatalogId::from_bytes(bytes(0x21, 1)).unwrap(),
        CatalogGeneration::new(2).unwrap(),
        304,
        [0xa1; 32],
    )
    .unwrap();
    assert_eq!(catalog.format(), FORMAT_VERSION);
    assert_eq!(catalog.byte_length(), 304);
    assert_eq!(catalog.body_sha256(), &[0xa1; 32]);

    let manifest = ManifestRef::new(
        ManifestId::from_bytes(bytes(0x31, 1)).unwrap(),
        ManifestKind::Table,
        ManifestGeneration::new(3).unwrap(),
        400,
        [0xb2; 32],
    )
    .unwrap();
    assert_eq!(manifest.kind(), ManifestKind::Table);
    assert_eq!(manifest.format().major(), 6);
    assert_eq!(manifest.format().minor(), 0);

    assert!(matches!(
        CatalogRef::from_persisted(
            catalog.id(),
            catalog.generation(),
            7,
            0,
            catalog.byte_length(),
            *catalog.body_sha256(),
        ),
        Err(FormatError::UnsupportedFormatVersion { .. })
    ));
    assert!(ManifestRef::new(
        manifest.id(),
        ManifestKind::Database,
        manifest.generation(),
        303,
        *manifest.body_sha256(),
    )
    .is_err());
    assert!(CatalogRef::new(
        catalog.id(),
        catalog.generation(),
        MAX_CATALOG_FILE_BYTES + 1,
        *catalog.body_sha256(),
    )
    .is_err());
    assert!(ManifestRef::new(
        manifest.id(),
        manifest.kind(),
        manifest.generation(),
        MAX_MANIFEST_FILE_BYTES + 1,
        *manifest.body_sha256(),
    )
    .is_err());
}

#[test]
fn artifact_reference_derives_locator_without_path_identity() {
    let id = ArtifactId::from_bytes(bytes(0xab, 0xcd)).unwrap();
    let generation = DatabaseGeneration::new(9).unwrap();
    let data = ArtifactRef::new(id, ArtifactKind::Data, generation, 4096, [0x44; 32]).unwrap();
    assert_eq!(data.codec_version(), ARTIFACT_CODEC_VERSION);
    assert_eq!(data.locator().shard(), 0xab);
    assert_eq!(
        data.relative_path().to_str().unwrap(),
        "artifacts/data/ab/abababababababababababababababcd.data"
    );

    let index = ArtifactRef::new(id, ArtifactKind::Index, generation, 4096, [0x55; 32]).unwrap();
    assert_eq!(
        index.relative_path().to_str().unwrap(),
        "artifacts/index/ab/abababababababababababababababcd.idx"
    );

    assert!(ArtifactRef::from_persisted(
        id,
        ArtifactKind::Data,
        2,
        0,
        generation,
        4096,
        [0; 32],
        0xab,
        1,
    )
    .is_err());
    assert!(ArtifactRef::from_persisted(
        id,
        ArtifactKind::Data,
        1,
        1,
        generation,
        4096,
        [0; 32],
        0xab,
        1,
    )
    .is_err());
    assert!(ArtifactRef::from_persisted(
        id,
        ArtifactKind::Data,
        1,
        0,
        generation,
        4096,
        [0; 32],
        0xaa,
        1,
    )
    .is_err());
    assert!(ArtifactRef::from_persisted(
        id,
        ArtifactKind::Data,
        1,
        0,
        generation,
        4096,
        [0; 32],
        0xab,
        2,
    )
    .is_err());
    assert!(ArtifactRef::new(
        id,
        ArtifactKind::Data,
        generation,
        MAX_ARTIFACT_FILE_BYTES + 1,
        [0; 32],
    )
    .is_err());
}

#[test]
fn index_section_reference_binds_logical_and_physical_identity() {
    let artifact = ArtifactRef::new(
        ArtifactId::from_bytes(bytes(0x61, 1)).unwrap(),
        ArtifactKind::Index,
        DatabaseGeneration::new(1).unwrap(),
        8192,
        [0x66; 32],
    )
    .unwrap();
    let slice = ArtifactSliceRef::new(
        artifact,
        7,
        IndexSectionKind::ExactPages,
        1,
        4096,
        1024,
        2048,
        200,
        0x1234_5678,
    )
    .unwrap();
    let logical = ObjectId::from_user_bytes(bytes(0x71, 1)).unwrap();
    let reference = IndexSectionRef::new(logical, slice);

    assert_eq!(reference.logical_index_id(), logical);
    assert_eq!(reference.slice().artifact().id(), artifact.id());
    assert_eq!(reference.slice().section_index(), 7);
    assert_eq!(
        reference.slice().section_kind(),
        IndexSectionKind::ExactPages
    );
    assert_eq!(reference.slice().stored_length(), 1024);
    assert_eq!(reference.slice().logical_length(), 2048);
    assert_eq!(reference.slice().item_count(), 200);
    assert_eq!(reference.slice().stored_crc32(), 0x1234_5678);

    let data = ArtifactRef::new(
        ArtifactId::from_bytes(bytes(0x81, 1)).unwrap(),
        ArtifactKind::Data,
        DatabaseGeneration::new(1).unwrap(),
        8192,
        [0x77; 32],
    )
    .unwrap();
    assert!(ArtifactSliceRef::new(
        data,
        0,
        IndexSectionKind::ExactPages,
        1,
        4096,
        512,
        512,
        1,
        0,
    )
    .is_err());
    assert!(ArtifactSliceRef::new(
        artifact,
        0,
        IndexSectionKind::ExactPages,
        1,
        4097,
        512,
        512,
        1,
        0,
    )
    .is_err());
    assert!(ArtifactSliceRef::new(
        artifact,
        0,
        IndexSectionKind::ExactPages,
        1,
        7936,
        512,
        512,
        1,
        0,
    )
    .is_err());
    assert_eq!(
        IndexSectionKind::from_tag(6).unwrap(),
        IndexSectionKind::HnswAdjacency
    );
    assert!(IndexSectionKind::from_tag(7).is_err());
}
