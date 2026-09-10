use radixdb_catalog::ObjectId;
use radixdb_storage::v6::{
    build_data_artifact, decode_data_artifact_layout, encode_data_artifact, read_data_row_ids,
    ArtifactId, CatalogGeneration, DataArtifactBuildRequest, DataArtifactHeader, DataArtifactInput,
    DataBlockSpec, DataPhysicalCodec, DatabaseGeneration, DatabaseId, FanoutBuildLimits,
    FormatError, SegmentId, SegmentKind, SourceRow, DATA_BLOCK_REF_BYTES, DATA_HEADER_BYTES,
    DATA_SECTION_REF_BYTES, MAX_ROWS_PER_GROUP,
};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn header(row_count: u64, group_count: u32) -> DataArtifactHeader {
    DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x61)).unwrap(),
        DatabaseId::from_bytes(raw(0x62)).unwrap(),
        ObjectId::from_user_bytes(raw(0x63)).unwrap(),
        SegmentId::from_bytes(raw(0x64)).unwrap(),
        DatabaseGeneration::new(11).unwrap(),
        CatalogGeneration::new(9).unwrap(),
        201,
        205,
        row_count,
        0,
        group_count,
        SegmentKind::Tombstones,
        987_654,
    )
    .unwrap()
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn refresh_section_crc(bytes: &mut [u8], section_index: usize) {
    let entry = DATA_HEADER_BYTES + section_index * DATA_SECTION_REF_BYTES;
    let offset = u64::from_le_bytes(bytes[entry + 8..entry + 16].try_into().unwrap()) as usize;
    let length = u64::from_le_bytes(bytes[entry + 16..entry + 24].try_into().unwrap()) as usize;
    let crc = radixdb_core::crc32_ieee(&bytes[offset..offset + length]);
    put_u32(bytes, entry + 40, crc);
}

fn refresh_block_crc(bytes: &mut [u8], block_index: usize) {
    let section = DATA_HEADER_BYTES + 2 * DATA_SECTION_REF_BYTES;
    let directory =
        u64::from_le_bytes(bytes[section + 8..section + 16].try_into().unwrap()) as usize;
    let entry = directory + block_index * DATA_BLOCK_REF_BYTES;
    let offset = u64::from_le_bytes(bytes[entry + 16..entry + 24].try_into().unwrap()) as usize;
    let length = u64::from_le_bytes(bytes[entry + 24..entry + 32].try_into().unwrap()) as usize;
    let crc = radixdb_core::crc32_ieee(&bytes[offset..offset + length]);
    put_u32(bytes, entry + 48, crc);
    refresh_section_crc(bytes, 2);
}

#[test]
fn independent_row_group_blocks_roundtrip_none_and_raw_lz4() {
    let first = vec![1, 2, 127, 128, 16_384];
    let second = vec![1_000_000, 1_000_009, u32::MAX as u64, u64::MAX];
    let input = DataArtifactInput::new(
        header((first.len() + second.len()) as u64, 2),
        vec![],
        vec![],
        vec![
            DataBlockSpec::row_ids(0, &first, DataPhysicalCodec::None).unwrap(),
            DataBlockSpec::row_ids(1, &second, DataPhysicalCodec::Lz4).unwrap(),
        ],
    )
    .unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();

    assert_eq!(layout.row_groups().len(), 2);
    assert_eq!(layout.row_groups()[0].first_row_ordinal(), 0);
    assert_eq!(
        layout.row_groups()[1].first_row_ordinal(),
        first.len() as u64
    );
    assert_eq!(layout.row_groups()[0].min_row_id(), 1);
    assert_eq!(layout.row_groups()[1].max_row_id(), u64::MAX);
    assert_eq!(read_data_row_ids(&bytes, &layout, 0).unwrap(), first);
    assert_eq!(read_data_row_ids(&bytes, &layout, 1).unwrap(), second);
}

#[test]
fn dense_row_ids_use_constant_size_payload_and_roundtrip() {
    let row_ids = (1_000_000..1_065_536).collect::<Vec<_>>();
    let block = DataBlockSpec::row_ids(0, &row_ids, DataPhysicalCodec::None).unwrap();
    assert_eq!(block.logical_length(), 24);

    let input =
        DataArtifactInput::new(header(row_ids.len() as u64, 1), vec![], vec![], vec![block])
            .unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    assert_eq!(read_data_row_ids(&bytes, &layout, 0).unwrap(), row_ids);
}

#[test]
fn streaming_tombstone_artifact_has_complete_canonical_byte_coverage() {
    let row_ids = [7, 19, 41];
    let request = DataArtifactBuildRequest::new(
        header(row_ids.len() as u64, 1),
        vec![],
        vec![],
        DataPhysicalCodec::Lz4,
        FanoutBuildLimits::default(),
    )
    .unwrap();
    let built = build_data_artifact(
        &request,
        row_ids
            .into_iter()
            .map(|row_id| Ok(SourceRow::new(row_id, vec![]))),
    )
    .unwrap();
    let layout = decode_data_artifact_layout(built.data_bytes(), built.data_reference()).unwrap();
    assert_eq!(layout.header().segment_kind(), SegmentKind::Tombstones);
    assert_eq!(
        read_data_row_ids(built.data_bytes(), &layout, 0).unwrap(),
        row_ids
    );
}

#[test]
fn nonminimal_and_zero_delta_encodings_fail_closed_after_valid_block_crc() {
    let input = DataArtifactInput::new(
        header(2, 1),
        vec![],
        vec![],
        vec![DataBlockSpec::row_ids(0, &[1, 129], DataPhysicalCodec::None).unwrap()],
    )
    .unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    let mut nonminimal = bytes.clone();
    let payload = layout.blocks()[0].offset() as usize;
    assert_eq!(&nonminimal[payload + 24..payload + 26], &[0x80, 0x01]);
    nonminimal[payload + 25] = 0;
    refresh_block_crc(&mut nonminimal, 0);
    let layout = decode_data_artifact_layout(&nonminimal, reference).unwrap();
    assert!(matches!(
        read_data_row_ids(&nonminimal, &layout, 0),
        Err(FormatError::InvalidDataArtifact { .. })
    ));

    let input = DataArtifactInput::new(
        header(3, 1),
        vec![],
        vec![],
        vec![DataBlockSpec::row_ids(0, &[1, 3, 4], DataPhysicalCodec::None).unwrap()],
    )
    .unwrap();
    let (mut zero_delta, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&zero_delta, reference).unwrap();
    let payload = layout.blocks()[0].offset() as usize;
    zero_delta[payload + 24] = 0;
    refresh_block_crc(&mut zero_delta, 0);
    let layout = decode_data_artifact_layout(&zero_delta, reference).unwrap();
    assert!(matches!(
        read_data_row_ids(&zero_delta, &layout, 0),
        Err(FormatError::InvalidDataArtifact { .. })
    ));
}

#[test]
fn row_group_directory_and_payload_bounds_must_agree() {
    let input = DataArtifactInput::new(
        header(2, 1),
        vec![],
        vec![],
        vec![DataBlockSpec::row_ids(0, &[10, 20], DataPhysicalCodec::None).unwrap()],
    )
    .unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();

    let mut wrong_bounds = bytes.clone();
    let group = layout.sections()[1].offset() as usize;
    wrong_bounds[group + 16..group + 24].copy_from_slice(&11_u64.to_le_bytes());
    refresh_section_crc(&mut wrong_bounds, 1);
    let layout = decode_data_artifact_layout(&wrong_bounds, reference).unwrap();
    assert!(read_data_row_ids(&wrong_bounds, &layout, 0).is_err());

    let mut wrong_block_range = bytes;
    wrong_block_range[group + 32..group + 36].copy_from_slice(&1_u32.to_le_bytes());
    refresh_section_crc(&mut wrong_block_range, 1);
    assert!(decode_data_artifact_layout(&wrong_block_range, reference).is_err());
}

#[test]
fn row_id_builder_rejects_empty_unsorted_and_oversized_groups() {
    assert!(DataBlockSpec::row_ids(0, &[], DataPhysicalCodec::None).is_err());
    assert!(DataBlockSpec::row_ids(0, &[2, 1], DataPhysicalCodec::None).is_err());
    assert!(DataBlockSpec::row_ids(0, &[2, 2], DataPhysicalCodec::None).is_err());
    let oversized = (0..=u64::from(MAX_ROWS_PER_GROUP)).collect::<Vec<_>>();
    assert!(matches!(
        DataBlockSpec::row_ids(0, &oversized, DataPhysicalCodec::None),
        Err(FormatError::DataArtifactLimitExceeded {
            field: "rows per group",
            ..
        })
    ));
}
