use radixdb_catalog::ObjectId;
use radixdb_storage::v6::{
    decode_database_manifest, decode_table_manifest, encode_database_manifest,
    encode_table_manifest, ArtifactId, ArtifactKind, ArtifactRef, CatalogGeneration, CatalogId,
    CatalogRef, DatabaseGeneration, DatabaseId, DatabaseManifest, FormatError, ManifestGeneration,
    ManifestId, ManifestKind, ManifestRef, SegmentDescriptor, SegmentId, SegmentKind, SegmentTier,
    TableManifest, TableManifestRef, WalGeneration, WalReplayFloor,
    MAX_SEGMENTS_PER_TABLE_MANIFEST, MAX_TABLES_PER_DATABASE,
};
#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn artifact(marker: u8, kind: ArtifactKind, generation: u64) -> ArtifactRef {
    ArtifactRef::new(
        ArtifactId::from_bytes(raw(marker)).unwrap(),
        kind,
        DatabaseGeneration::new(generation).unwrap(),
        4096,
        [marker.wrapping_add(1); 32],
    )
    .unwrap()
}

fn segment(marker: u8, generation: u64, with_index: bool) -> SegmentDescriptor {
    segment_at_tier(marker, generation, with_index, SegmentTier::L0)
}

fn segment_at_tier(
    marker: u8,
    generation: u64,
    with_index: bool,
    tier: SegmentTier,
) -> SegmentDescriptor {
    SegmentDescriptor::new_at_tier(
        SegmentId::from_bytes(raw(marker)).unwrap(),
        SegmentKind::Rows,
        tier,
        10,
        20,
        100,
        u64::from(marker) * 100 + 1,
        u64::from(marker) * 100 + 100,
        artifact(marker.wrapping_add(20), ArtifactKind::Data, generation),
        with_index.then(|| artifact(marker.wrapping_add(30), ArtifactKind::Index, generation)),
    )
    .unwrap()
}

fn table_manifest(
    table_marker: u8,
    manifest_marker: u8,
    generation: u64,
    segments: Vec<SegmentDescriptor>,
) -> TableManifest {
    TableManifest::new(
        DatabaseId::from_bytes(raw(1)).unwrap(),
        object_id(table_marker),
        ManifestId::from_bytes(raw(manifest_marker)).unwrap(),
        ManifestGeneration::new(generation).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        20_000,
        31,
        segments,
        123_456,
    )
    .unwrap()
}

fn table_reference(manifest: &TableManifest, bytes: &[u8]) -> TableManifestRef {
    let footer = bytes.len() - 48;
    let body_sha: [u8; 32] = bytes[footer + 16..].try_into().unwrap();
    TableManifestRef::new(
        manifest.table_id(),
        ManifestRef::new(
            manifest.manifest_id(),
            ManifestKind::Table,
            manifest.generation(),
            bytes.len() as u64,
            body_sha,
        )
        .unwrap(),
    )
    .unwrap()
}

fn database_manifest(tables: Vec<TableManifestRef>) -> DatabaseManifest {
    DatabaseManifest::new(
        DatabaseId::from_bytes(raw(1)).unwrap(),
        ManifestId::from_bytes(raw(2)).unwrap(),
        DatabaseGeneration::new(7).unwrap(),
        CatalogRef::new(
            CatalogId::from_bytes(raw(3)).unwrap(),
            CatalogGeneration::new(3).unwrap(),
            2048,
            [4; 32],
        )
        .unwrap(),
        WalReplayFloor::new(WalGeneration::new(5).unwrap(), 600),
        100,
        tables,
        789_012,
    )
    .unwrap()
}

fn refresh_integrity(bytes: &mut [u8]) {
    let header_crc = radixdb_core::crc32_ieee(&bytes[..248]);
    bytes[248..252].copy_from_slice(&header_crc.to_le_bytes());
    let footer = bytes.len() - 48;
    let body_sha = radixdb_core::sha256_digest(&bytes[..footer]);
    bytes[footer + 16..].copy_from_slice(&body_sha);
}

#[test]
fn table_manifest_is_canonical_and_roundtrips() {
    let first = segment(10, 2, false);
    let second = segment_at_tier(20, 4, true, SegmentTier::L1);
    let shuffled = table_manifest(10, 11, 7, vec![second, first]);
    let sorted = table_manifest(10, 11, 7, vec![first, second]);

    let bytes = encode_table_manifest(&shuffled).unwrap();
    assert_eq!(bytes, encode_table_manifest(&sorted).unwrap());
    assert_eq!(&bytes[..8], b"RDX6TBM\0");
    assert_eq!(bytes.len(), 256 + 2 * 240 + 48);
    assert_eq!(u64::from_le_bytes(bytes[104..112].try_into().unwrap()), 2);
    assert_eq!(u64::from_le_bytes(bytes[112..120].try_into().unwrap()), 256);
    assert_eq!(u64::from_le_bytes(bytes[120..128].try_into().unwrap()), 480);
    assert_eq!(&bytes[256..272], first.id().as_bytes());
    assert_eq!(u32::from_le_bytes(bytes[276..280].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(bytes[516..520].try_into().unwrap()), 1);
    assert!(bytes[256 + 152..256 + 240].iter().all(|byte| *byte == 0));
    let decoded = decode_table_manifest(&bytes).unwrap();
    assert_eq!(decoded.segments()[0].tier(), SegmentTier::L0);
    assert_eq!(decoded.segments()[1].tier(), SegmentTier::L1);
    assert_eq!(decoded, sorted);
    assert_eq!(
        encode_table_manifest(&decode_table_manifest(&bytes).unwrap()).unwrap(),
        bytes
    );
}

#[test]
fn database_manifest_is_canonical_and_roundtrips() {
    let table_a = table_manifest(10, 11, 6, vec![segment(10, 2, false)]);
    let table_b = table_manifest(20, 21, 7, vec![segment(20, 3, true)]);
    let bytes_a = encode_table_manifest(&table_a).unwrap();
    let bytes_b = encode_table_manifest(&table_b).unwrap();
    let ref_a = table_reference(&table_a, &bytes_a);
    let ref_b = table_reference(&table_b, &bytes_b);
    let shuffled = database_manifest(vec![ref_b, ref_a]);
    let sorted = database_manifest(vec![ref_a, ref_b]);

    let bytes = encode_database_manifest(&shuffled).unwrap();
    assert_eq!(bytes, encode_database_manifest(&sorted).unwrap());
    assert_eq!(&bytes[..8], b"RDX6DBM\0");
    assert_eq!(bytes.len(), 256 + 2 * 96 + 48);
    assert_eq!(u64::from_le_bytes(bytes[144..152].try_into().unwrap()), 2);
    assert_eq!(u64::from_le_bytes(bytes[152..160].try_into().unwrap()), 256);
    assert_eq!(u64::from_le_bytes(bytes[160..168].try_into().unwrap()), 192);
    assert_eq!(u64::from_le_bytes(bytes[184..192].try_into().unwrap()), 100);
    assert_eq!(&bytes[256..272], table_a.table_id().as_bytes());
    let decoded = decode_database_manifest(&bytes).unwrap();
    assert_eq!(decoded.transaction_high_water(), 100);
    assert_eq!(decoded, sorted);
    assert_eq!(
        encode_database_manifest(&decode_database_manifest(&bytes).unwrap()).unwrap(),
        bytes
    );
}

#[test]
fn empty_manifests_use_zero_directory_identity() {
    let table = table_manifest(10, 11, 1, vec![]);
    let table_bytes = encode_table_manifest(&table).unwrap();
    assert_eq!(table_bytes.len(), 304);
    assert_eq!(&table_bytes[104..128], &[0; 24]);
    assert_eq!(decode_table_manifest(&table_bytes).unwrap(), table);

    let database = database_manifest(vec![]);
    let database_bytes = encode_database_manifest(&database).unwrap();
    assert_eq!(database_bytes.len(), 304);
    assert_eq!(&database_bytes[144..168], &[0; 24]);
    assert_eq!(decode_database_manifest(&database_bytes).unwrap(), database);
}

#[test]
fn manifest_models_reject_duplicate_and_generation_mismatches() {
    let item = segment(10, 2, false);
    assert!(TableManifest::new(
        DatabaseId::from_bytes(raw(1)).unwrap(),
        object_id(10),
        ManifestId::from_bytes(raw(11)).unwrap(),
        ManifestGeneration::new(7).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        100,
        2,
        vec![item, item],
        0,
    )
    .is_err());

    let duplicate_artifact = artifact(30, ArtifactKind::Data, 2);
    let first = SegmentDescriptor::new(
        SegmentId::from_bytes(raw(10)).unwrap(),
        SegmentKind::Rows,
        10,
        20,
        100,
        1,
        100,
        duplicate_artifact,
        None,
    )
    .unwrap();
    let second = SegmentDescriptor::new(
        SegmentId::from_bytes(raw(20)).unwrap(),
        SegmentKind::Rows,
        21,
        30,
        100,
        101,
        200,
        duplicate_artifact,
        None,
    )
    .unwrap();
    assert!(TableManifest::new(
        DatabaseId::from_bytes(raw(1)).unwrap(),
        object_id(10),
        ManifestId::from_bytes(raw(11)).unwrap(),
        ManifestGeneration::new(7).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        200,
        3,
        vec![first, second],
        0,
    )
    .is_err());

    let future = segment(10, 8, false);
    assert!(TableManifest::new(
        DatabaseId::from_bytes(raw(1)).unwrap(),
        object_id(10),
        ManifestId::from_bytes(raw(11)).unwrap(),
        ManifestGeneration::new(7).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        100,
        2,
        vec![future],
        0,
    )
    .is_err());

    let table = table_manifest(10, 11, 7, vec![]);
    let bytes = encode_table_manifest(&table).unwrap();
    let reference = table_reference(&table, &bytes);
    assert!(DatabaseManifest::new(
        DatabaseId::from_bytes(raw(1)).unwrap(),
        ManifestId::from_bytes(raw(2)).unwrap(),
        DatabaseGeneration::new(6).unwrap(),
        CatalogRef::new(
            CatalogId::from_bytes(raw(3)).unwrap(),
            CatalogGeneration::new(3).unwrap(),
            2048,
            [4; 32],
        )
        .unwrap(),
        WalReplayFloor::new(WalGeneration::new(5).unwrap(), 600),
        0,
        vec![reference],
        0,
    )
    .is_err());
    assert!(DatabaseManifest::new(
        DatabaseId::from_bytes(raw(1)).unwrap(),
        ManifestId::from_bytes(raw(2)).unwrap(),
        DatabaseGeneration::new(7).unwrap(),
        CatalogRef::new(
            CatalogId::from_bytes(raw(3)).unwrap(),
            CatalogGeneration::new(3).unwrap(),
            2048,
            [4; 32],
        )
        .unwrap(),
        WalReplayFloor::new(WalGeneration::new(5).unwrap(), 600),
        0,
        vec![reference, reference],
        0,
    )
    .is_err());
    assert!(matches!(
        DatabaseManifest::new(
            DatabaseId::from_bytes(raw(1)).unwrap(),
            ManifestId::from_bytes(raw(2)).unwrap(),
            DatabaseGeneration::new(7).unwrap(),
            CatalogRef::new(
                CatalogId::from_bytes(raw(3)).unwrap(),
                CatalogGeneration::new(3).unwrap(),
                2048,
                [4; 32],
            )
            .unwrap(),
            WalReplayFloor::new(WalGeneration::new(5).unwrap(), 600),
            i64::MAX as u64,
            vec![],
            0,
        ),
        Err(FormatError::ManifestLimitExceeded {
            field: "transaction high-water",
            ..
        })
    ));
}

#[test]
fn malformed_database_manifest_corpus_fails_closed() {
    let table_a = table_manifest(10, 11, 6, vec![]);
    let table_b = table_manifest(20, 21, 6, vec![]);
    let bytes_a = encode_table_manifest(&table_a).unwrap();
    let bytes_b = encode_table_manifest(&table_b).unwrap();
    let original = encode_database_manifest(&database_manifest(vec![
        table_reference(&table_a, &bytes_a),
        table_reference(&table_b, &bytes_b),
    ]))
    .unwrap();

    let mut bad_sha = original.clone();
    bad_sha[300] ^= 1;
    assert!(matches!(
        decode_database_manifest(&bad_sha),
        Err(FormatError::ManifestChecksumMismatch { .. })
    ));

    let mut excessive = original.clone();
    excessive[144..152].copy_from_slice(&((MAX_TABLES_PER_DATABASE as u64) + 1).to_le_bytes());
    refresh_integrity(&mut excessive);
    assert!(matches!(
        decode_database_manifest(&excessive),
        Err(FormatError::ManifestLimitExceeded { .. })
    ));

    let mut unsorted = original.clone();
    let (left, right) = unsorted[256..448].split_at_mut(96);
    left.swap_with_slice(right);
    refresh_integrity(&mut unsorted);
    assert!(matches!(
        decode_database_manifest(&unsorted),
        Err(FormatError::InvalidManifest { .. })
    ));

    let mut reserved = original.clone();
    reserved[200] = 1;
    refresh_integrity(&mut reserved);
    assert!(decode_database_manifest(&reserved).is_err());
}

#[test]
fn malformed_table_manifest_corpus_fails_closed() {
    let original = encode_table_manifest(&table_manifest(
        10,
        11,
        7,
        vec![segment(10, 2, false), segment(20, 3, true)],
    ))
    .unwrap();

    let mut excessive = original.clone();
    excessive[104..112]
        .copy_from_slice(&((MAX_SEGMENTS_PER_TABLE_MANIFEST as u64) + 1).to_le_bytes());
    refresh_integrity(&mut excessive);
    assert!(matches!(
        decode_table_manifest(&excessive),
        Err(FormatError::ManifestLimitExceeded { .. })
    ));

    let mut unsorted = original.clone();
    let (left, right) = unsorted[256..736].split_at_mut(240);
    left.swap_with_slice(right);
    refresh_integrity(&mut unsorted);
    assert!(decode_table_manifest(&unsorted).is_err());

    let mut data_is_index = original.clone();
    data_is_index[256 + 64 + 16..256 + 64 + 18].copy_from_slice(&2_u16.to_le_bytes());
    data_is_index[256 + 64 + 74..256 + 64 + 76].copy_from_slice(&2_u16.to_le_bytes());
    refresh_integrity(&mut data_is_index);
    assert!(decode_table_manifest(&data_is_index).is_err());

    let mut missing_data = original.clone();
    missing_data[256 + 64..256 + 152].fill(0);
    refresh_integrity(&mut missing_data);
    assert!(decode_table_manifest(&missing_data).is_err());

    let mut partial_optional_index = original.clone();
    partial_optional_index[256 + 152] = 1;
    refresh_integrity(&mut partial_optional_index);
    assert!(decode_table_manifest(&partial_optional_index).is_err());

    let mut unknown_segment_flags = original;
    unknown_segment_flags[256 + 20..256 + 24].copy_from_slice(&2_u32.to_le_bytes());
    refresh_integrity(&mut unknown_segment_flags);
    assert!(matches!(
        decode_table_manifest(&unknown_segment_flags),
        Err(FormatError::InvalidManifest { .. })
    ));
}

#[cfg(feature = "test-failpoints")]
#[test]
fn manifest_encoders_expose_body_before_footer_boundaries() {
    let table = table_manifest(10, 11, 7, vec![segment(10, 2, true)]);
    let guard = GenerationFaultGuard::arm(
        GenerationCrashPoint::TableManifestAfterBodyBeforeFooter,
        GenerationFaultMode::ReturnIoError,
    );
    assert!(encode_table_manifest(&table).is_err());
    assert_eq!(guard.hit_count(), 1);
    drop(guard);

    let table_bytes = encode_table_manifest(&table).unwrap();
    let database = database_manifest(vec![table_reference(&table, &table_bytes)]);
    let guard = GenerationFaultGuard::arm(
        GenerationCrashPoint::DatabaseManifestAfterBodyBeforeFooter,
        GenerationFaultMode::ReturnIoError,
    );
    assert!(encode_database_manifest(&database).is_err());
    assert_eq!(guard.hit_count(), 1);
}
