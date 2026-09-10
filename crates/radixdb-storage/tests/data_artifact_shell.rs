use radixdb_catalog::ObjectId;
use radixdb_storage::v6::{
    decode_data_artifact_layout, encode_data_artifact, read_data_block, ArtifactId, ArtifactKind,
    ArtifactRef, CatalogGeneration, DataArtifactHeader, DataArtifactInput, DataBlockKind,
    DataBlockSpec, DataPhysicalCodec, DataSectionKind, DatabaseGeneration, DatabaseId, FormatError,
    SegmentId, SegmentKind, DATA_BLOCK_REF_BYTES, DATA_FOOTER_BYTES, DATA_HEADER_BYTES,
    DATA_SECTION_COUNT, DATA_SECTION_REF_BYTES, MAX_COLUMNS_PER_TABLE,
};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn fixture() -> (Vec<u8>, ArtifactRef, Vec<u8>) {
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x11)).unwrap(),
        DatabaseId::from_bytes(raw(0x22)).unwrap(),
        ObjectId::from_user_bytes(raw(0x33)).unwrap(),
        SegmentId::from_bytes(raw(0x44)).unwrap(),
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
    let block = DataBlockSpec::row_ids(0, &[7], DataPhysicalCodec::None).unwrap();
    let payload = block.stored_bytes().to_vec();
    let input = DataArtifactInput::new(header, vec![], vec![], vec![block]).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    (bytes, reference, payload)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

#[test]
fn exact_shell_roundtrips_identity_directories_footer_and_block_crc() {
    let (bytes, reference, payload) = fixture();
    assert_eq!(&bytes[..8], b"RDX6DAT\0");
    assert_eq!(read_u16(&bytes, 8), 6);
    assert_eq!(read_u16(&bytes, 10), 0);
    assert_eq!(read_u32(&bytes, 12), DATA_HEADER_BYTES as u32);
    assert_eq!(read_u64(&bytes, 16), bytes.len() as u64);
    assert_eq!(read_u32(&bytes, 136), DATA_SECTION_COUNT as u32);
    assert_eq!(read_u32(&bytes, 140), 1);
    assert_eq!(read_u64(&bytes, 144), DATA_HEADER_BYTES as u64);
    assert_eq!(
        read_u64(&bytes, 152),
        (DATA_SECTION_COUNT * DATA_SECTION_REF_BYTES) as u64
    );

    let footer = bytes.len() - DATA_FOOTER_BYTES;
    assert_eq!(&bytes[footer..footer + 8], b"RDX6END\0");
    assert_eq!(read_u64(&bytes, footer + 8), bytes.len() as u64);
    assert_eq!(&bytes[footer + 16..], reference.body_sha256());

    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    assert_eq!(layout.reference(), reference);
    assert_eq!(layout.header().segment_kind(), SegmentKind::Tombstones);
    assert_eq!(layout.sections().len(), DATA_SECTION_COUNT);
    assert_eq!(
        layout.sections()[0].kind(),
        DataSectionKind::ColumnDirectory
    );
    assert_eq!(
        layout.sections()[1].kind(),
        DataSectionKind::RowGroupDirectory
    );
    assert_eq!(layout.sections()[2].kind(), DataSectionKind::BlockDirectory);
    assert_eq!(
        layout.sections()[2].stored_length(),
        DATA_BLOCK_REF_BYTES as u64
    );
    assert_eq!(layout.blocks().len(), 1);
    assert_eq!(layout.blocks()[0].kind(), DataBlockKind::RowIds);
    assert_eq!(read_data_block(&bytes, &layout, 0).unwrap(), payload);
}

#[test]
fn metadata_checks_are_fail_closed_but_payload_crc_is_lazy() {
    let (bytes, reference, _) = fixture();

    let mut bad_header = bytes.clone();
    bad_header[168] = 1;
    assert!(matches!(
        decode_data_artifact_layout(&bad_header, reference),
        Err(FormatError::InvalidDataArtifact { .. })
            | Err(FormatError::DataArtifactChecksumMismatch { scope: "header" })
    ));

    let mut bad_section = bytes.clone();
    let row_group_offset =
        read_u64(&bad_section, DATA_HEADER_BYTES + DATA_SECTION_REF_BYTES + 8) as usize;
    bad_section[row_group_offset] ^= 1;
    assert!(matches!(
        decode_data_artifact_layout(&bad_section, reference),
        Err(FormatError::DataArtifactChecksumMismatch { scope: "section" })
    ));

    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    let mut bad_block = bytes.clone();
    bad_block[layout.blocks()[0].offset() as usize] ^= 1;
    let reopened = decode_data_artifact_layout(&bad_block, reference).unwrap();
    assert!(matches!(
        read_data_block(&bad_block, &reopened, 0),
        Err(FormatError::DataArtifactChecksumMismatch { scope: "block" })
    ));
}

#[test]
fn unsupported_version_and_reference_mismatch_are_rejected() {
    let (bytes, reference, _) = fixture();

    let mut future = bytes.clone();
    future[8..10].copy_from_slice(&7_u16.to_le_bytes());
    let crc = radixdb_core::crc32_ieee(&future[..248]);
    put_u32(&mut future, 248, crc);
    assert!(matches!(
        decode_data_artifact_layout(&future, reference),
        Err(FormatError::UnsupportedFormatVersion {
            owner: "data artifact",
            major: 7,
            minor: 0
        })
    ));

    let wrong_identity = ArtifactRef::new(
        ArtifactId::from_bytes(raw(0x55)).unwrap(),
        ArtifactKind::Data,
        reference.creation_generation(),
        reference.byte_length(),
        *reference.body_sha256(),
    )
    .unwrap();
    assert!(matches!(
        decode_data_artifact_layout(&bytes, wrong_identity),
        Err(FormatError::InvalidDataArtifact { .. })
    ));
}

#[test]
fn constructors_reject_noncanonical_shape_before_encoding() {
    let result = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x11)).unwrap(),
        DatabaseId::from_bytes(raw(0x22)).unwrap(),
        ObjectId::from_user_bytes(raw(0x33)).unwrap(),
        SegmentId::from_bytes(raw(0x44)).unwrap(),
        DatabaseGeneration::new(9).unwrap(),
        CatalogGeneration::new(7).unwrap(),
        1,
        1,
        1,
        MAX_COLUMNS_PER_TABLE + 1,
        1,
        SegmentKind::Rows,
        0,
    );
    assert!(matches!(
        result,
        Err(FormatError::DataArtifactLimitExceeded {
            field: "column count",
            ..
        })
    ));

    assert!(DataBlockSpec::row_ids(0, &[2, 2], DataPhysicalCodec::None).is_err());
}
