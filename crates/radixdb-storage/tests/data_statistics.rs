use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    decode_data_artifact_layout, encode_data_artifact, read_data_bloom, ArtifactId, ArtifactRef,
    CatalogGeneration, DataArtifactHeader, DataArtifactInput, DataBlockKind, DataBlockSpec,
    DataBloomConfig, DataColumnSpec, DataPhysicalCodec, DataStatisticsSpec, DataValueEncoding,
    DatabaseGeneration, DatabaseId, FormatError, SegmentId, SegmentKind, DATA_BLOCK_REF_BYTES,
    DATA_HEADER_BYTES, DATA_SECTION_REF_BYTES, MAX_BLOOM_BITS_PER_GROUP_COLUMN,
    MAX_STATISTIC_VALUE_BYTES,
};

const STATISTICS_ENTRY_BYTES: usize = 80;

struct Fixture {
    bytes: Vec<u8>,
    reference: ArtifactRef,
}

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn header(row_count: u64, column_count: u32, row_group_count: u32) -> DataArtifactHeader {
    DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x31)).unwrap(),
        DatabaseId::from_bytes(raw(0x32)).unwrap(),
        ObjectId::from_user_bytes(raw(0x33)).unwrap(),
        SegmentId::from_bytes(raw(0x34)).unwrap(),
        DatabaseGeneration::new(11).unwrap(),
        CatalogGeneration::new(9).unwrap(),
        201,
        205,
        row_count,
        column_count,
        row_group_count,
        SegmentKind::Rows,
        987_654,
    )
    .unwrap()
}

fn column(ordinal: u8, data_type: CatalogDataType, nullable: bool) -> DataColumnSpec {
    DataColumnSpec::new(
        ObjectId::from_user_bytes(raw(0x40 + ordinal)).unwrap(),
        data_type,
        nullable,
    )
}

fn fixture() -> Fixture {
    let integer = column(0, CatalogDataType::scalar(DataType::Integer).unwrap(), true);
    let text = column(1, CatalogDataType::scalar(DataType::Text).unwrap(), true);
    let vector = column(2, CatalogDataType::vector(2).unwrap(), false);
    let columns = vec![integer, text, vector];

    let integer_groups = [
        vec![
            Value::integer(9),
            Value::null(DataType::Integer),
            Value::integer(-7),
        ],
        vec![Value::integer(4), Value::integer(4), Value::integer(8)],
    ];
    let text_groups = [
        vec![Value::text("z"), Value::text("a"), Value::text("m")],
        vec![
            Value::text(""),
            Value::null(DataType::Text),
            Value::text("beta"),
        ],
    ];
    let vector_groups = [
        vec![
            Value::vector(vec![1.0, 2.0]),
            Value::vector(vec![3.0, 4.0]),
            Value::vector(vec![5.0, 6.0]),
        ],
        vec![
            Value::vector(vec![-1.0, -2.0]),
            Value::vector(vec![0.0, 0.0]),
            Value::vector(vec![1.0, 2.0]),
        ],
    ];

    let mut blocks = Vec::new();
    for group in 0..2_u32 {
        blocks.push(
            DataBlockSpec::row_ids(
                group,
                if group == 0 { &[1, 3, 7] } else { &[8, 11, 20] },
                DataPhysicalCodec::None,
            )
            .unwrap(),
        );
        blocks.push(
            DataBlockSpec::column(
                group,
                0,
                integer,
                &integer_groups[group as usize],
                DataValueEncoding::Plain,
                DataPhysicalCodec::None,
            )
            .unwrap(),
        );
        blocks.push(
            DataBlockSpec::column(
                group,
                1,
                text,
                &text_groups[group as usize],
                DataValueEncoding::Dictionary,
                DataPhysicalCodec::Lz4,
            )
            .unwrap(),
        );
        blocks.push(
            DataBlockSpec::column(
                group,
                2,
                vector,
                &vector_groups[group as usize],
                DataValueEncoding::Plain,
                DataPhysicalCodec::None,
            )
            .unwrap(),
        );
        if group == 0 {
            blocks.push(
                DataBlockSpec::bloom(
                    group,
                    0,
                    integer,
                    &integer_groups[0],
                    DataBloomConfig::new(257, 5, 0x1122_3344_5566_7788).unwrap(),
                    DataPhysicalCodec::None,
                )
                .unwrap(),
            );
        } else {
            blocks.push(
                DataBlockSpec::bloom(
                    group,
                    1,
                    text,
                    &text_groups[1],
                    DataBloomConfig::new(509, 7, 0x8877_6655_4433_2211).unwrap(),
                    DataPhysicalCodec::Lz4,
                )
                .unwrap(),
            );
        }
    }

    let statistics = vec![
        DataStatisticsSpec::from_values(0, 0, integer, &integer_groups[0], Some(2)).unwrap(),
        DataStatisticsSpec::from_values(0, 1, integer, &integer_groups[1], Some(2)).unwrap(),
        DataStatisticsSpec::from_values(1, 0, text, &text_groups[0], Some(3)).unwrap(),
        DataStatisticsSpec::from_values(1, 1, text, &text_groups[1], Some(2)).unwrap(),
        DataStatisticsSpec::from_values(2, 0, vector, &vector_groups[0], None).unwrap(),
        DataStatisticsSpec::from_values(2, 1, vector, &vector_groups[1], None).unwrap(),
    ];
    let input = DataArtifactInput::new(header(6, 3, 2), columns, statistics, blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    Fixture { bytes, reference }
}

fn section_offset(bytes: &[u8], section_index: usize) -> usize {
    let entry = DATA_HEADER_BYTES + section_index * DATA_SECTION_REF_BYTES;
    u64::from_le_bytes(bytes[entry + 8..entry + 16].try_into().unwrap()) as usize
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
    let directory = section_offset(bytes, 2);
    let entry = directory + block_index * DATA_BLOCK_REF_BYTES;
    let offset = u64::from_le_bytes(bytes[entry + 16..entry + 24].try_into().unwrap()) as usize;
    let length = u64::from_le_bytes(bytes[entry + 24..entry + 32].try_into().unwrap()) as usize;
    let crc = radixdb_core::crc32_ieee(&bytes[offset..offset + length]);
    put_u32(bytes, entry + 48, crc);
    refresh_section_crc(bytes, 2);
}

#[test]
fn zone_maps_statistics_and_blooms_roundtrip_without_row_scaled_postings() {
    let Fixture { bytes, reference } = fixture();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();

    assert_eq!(layout.statistics().len(), 6);
    assert_eq!(layout.columns()[0].statistics_entry_index(), 0);
    assert_eq!(layout.columns()[1].statistics_entry_index(), 2);
    assert_eq!(layout.columns()[2].statistics_entry_index(), 4);

    let integer_group = &layout.statistics()[0];
    assert_eq!(
        (
            integer_group.column_ordinal(),
            integer_group.row_group_ordinal()
        ),
        (0, 0)
    );
    assert_eq!(integer_group.null_count(), 1);
    assert_eq!(integer_group.distinct_estimate(), Some(2));
    assert_eq!(integer_group.minimum().cloned(), Some(Value::integer(-7)));
    assert_eq!(integer_group.maximum().cloned(), Some(Value::integer(9)));
    assert_eq!(integer_group.integer_sum(), Some(2));
    assert_eq!(integer_group.float_sum(), None);
    assert_eq!(integer_group.numeric_count(), 2);
    assert!(integer_group.bloom_block_index().is_some());

    let second_integer_group = &layout.statistics()[1];
    assert_eq!(second_integer_group.integer_sum(), Some(16));
    assert_eq!(second_integer_group.numeric_count(), 3);

    let empty_text_bound = &layout.statistics()[3];
    assert_eq!(empty_text_bound.minimum().cloned(), Some(Value::text("")));
    assert_eq!(
        empty_text_bound.maximum().cloned(),
        Some(Value::text("beta"))
    );
    assert_eq!(empty_text_bound.null_count(), 1);
    assert!(empty_text_bound.bloom_block_index().is_some());

    for statistics in &layout.statistics()[4..] {
        assert!(statistics.minimum().is_none());
        assert!(statistics.maximum().is_none());
        assert!(statistics.bloom_block_index().is_none());
        assert_eq!(statistics.integer_sum(), None);
        assert_eq!(statistics.float_sum(), None);
        assert_eq!(statistics.numeric_count(), 0);
    }
    let bloom_blocks = layout
        .blocks()
        .iter()
        .filter(|block| block.kind() == DataBlockKind::Bloom)
        .collect::<Vec<_>>();
    assert_eq!(bloom_blocks.len(), 2);
    assert_eq!(bloom_blocks[0].item_count(), 257);
    assert_eq!(bloom_blocks[1].item_count(), 509);

    let integer_bloom = read_data_bloom(&bytes, &layout, 0, 0).unwrap().unwrap();
    assert!(integer_bloom.might_contain(&Value::integer(-7)).unwrap());
    assert!(integer_bloom.might_contain(&Value::integer(9)).unwrap());
    assert!(integer_bloom
        .might_contain(&Value::null(DataType::Integer))
        .unwrap());
    assert!((10..10_000)
        .map(Value::integer)
        .any(|candidate| !integer_bloom.might_contain(&candidate).unwrap()));

    let text_bloom = read_data_bloom(&bytes, &layout, 1, 1).unwrap().unwrap();
    assert!(text_bloom.might_contain(&Value::text("")).unwrap());
    assert!(text_bloom.might_contain(&Value::text("beta")).unwrap());
    assert!(read_data_bloom(&bytes, &layout, 0, 1).unwrap().is_none());
    assert!(read_data_bloom(&bytes, &layout, 0, 2).unwrap().is_none());
    assert!(read_data_bloom(&bytes, &layout, 2, 0).is_err());
    assert!(read_data_bloom(&bytes, &layout, 0, 3).is_err());
}

#[test]
fn writer_rejects_noncanonical_statistics_and_unowned_or_mismatched_blooms() {
    let integer = column(
        0,
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let values = vec![Value::integer(1), Value::integer(2)];
    assert!(DataStatisticsSpec::from_values(0, 0, integer, &values, Some(0)).is_err());
    assert!(DataStatisticsSpec::from_values(0, 0, integer, &values, Some(3)).is_err());

    let base_blocks = vec![
        DataBlockSpec::row_ids(0, &[1, 2], DataPhysicalCodec::None).unwrap(),
        DataBlockSpec::column(
            0,
            0,
            integer,
            &values,
            DataValueEncoding::Plain,
            DataPhysicalCodec::None,
        )
        .unwrap(),
    ];
    let statistics = DataStatisticsSpec::from_values(0, 0, integer, &values, Some(2)).unwrap();
    assert!(DataArtifactInput::new(
        header(2, 1, 1),
        vec![integer],
        vec![statistics.clone(), statistics],
        base_blocks.clone(),
    )
    .is_err());

    let mut orphan_bloom = base_blocks.clone();
    orphan_bloom.push(
        DataBlockSpec::bloom(
            0,
            0,
            integer,
            &values,
            DataBloomConfig::new(64, 3, 7).unwrap(),
            DataPhysicalCodec::None,
        )
        .unwrap(),
    );
    assert!(DataArtifactInput::new(header(2, 1, 1), vec![integer], vec![], orphan_bloom).is_err());

    let short_values = vec![Value::integer(1)];
    let mut mismatched_bloom = base_blocks;
    mismatched_bloom.push(
        DataBlockSpec::bloom(
            0,
            0,
            integer,
            &short_values,
            DataBloomConfig::new(64, 3, 7).unwrap(),
            DataPhysicalCodec::None,
        )
        .unwrap(),
    );
    let statistics = DataStatisticsSpec::from_values(0, 0, integer, &values, Some(2)).unwrap();
    assert!(DataArtifactInput::new(
        header(2, 1, 1),
        vec![integer],
        vec![statistics],
        mismatched_bloom,
    )
    .is_err());

    assert!(DataBloomConfig::new(0, 1, 0).is_err());
    assert!(DataBloomConfig::new((MAX_BLOOM_BITS_PER_GROUP_COLUMN + 1) as u32, 1, 0).is_err());
    assert!(DataBloomConfig::new(64, 0, 0).is_err());
    assert!(DataBloomConfig::new(64, 17, 0).is_err());
}

#[test]
fn oversized_zone_map_values_are_omitted_instead_of_expanding_metadata() {
    let text = column(0, CatalogDataType::scalar(DataType::Text).unwrap(), false);
    let value = Value::text("x".repeat(MAX_STATISTIC_VALUE_BYTES as usize + 1));
    let values = vec![value];
    let blocks = vec![
        DataBlockSpec::row_ids(0, &[1], DataPhysicalCodec::None).unwrap(),
        DataBlockSpec::column(
            0,
            0,
            text,
            &values,
            DataValueEncoding::Plain,
            DataPhysicalCodec::Lz4,
        )
        .unwrap(),
    ];
    let statistics = DataStatisticsSpec::from_values(0, 0, text, &values, Some(1)).unwrap();
    let input =
        DataArtifactInput::new(header(1, 1, 1), vec![text], vec![statistics], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    assert!(layout.statistics()[0].minimum().is_none());
    assert!(layout.statistics()[0].maximum().is_none());
    assert_eq!(layout.statistics()[0].distinct_estimate(), Some(1));
}

#[test]
fn statistics_directory_and_value_corruption_fail_closed() {
    let Fixture { bytes, reference } = fixture();
    let statistics = section_offset(&bytes, 3);

    let mut unknown_flags = bytes.clone();
    put_u32(&mut unknown_flags, statistics + 20, 1 << 31);
    refresh_section_crc(&mut unknown_flags, 3);
    assert!(decode_data_artifact_layout(&unknown_flags, reference).is_err());

    let mut missing_numeric_sum = bytes.clone();
    let flags = u32::from_le_bytes(
        missing_numeric_sum[statistics + 20..statistics + 24]
            .try_into()
            .unwrap(),
    );
    put_u32(&mut missing_numeric_sum, statistics + 20, flags & !(1 << 4));
    refresh_section_crc(&mut missing_numeric_sum, 3);
    assert!(decode_data_artifact_layout(&missing_numeric_sum, reference).is_err());

    let mut wrong_numeric_sum_crc = bytes.clone();
    let encoded = u32::from_le_bytes(
        wrong_numeric_sum_crc[statistics + 76..statistics + 80]
            .try_into()
            .unwrap(),
    );
    put_u32(&mut wrong_numeric_sum_crc, statistics + 76, encoded ^ 1);
    refresh_section_crc(&mut wrong_numeric_sum_crc, 3);
    assert!(matches!(
        decode_data_artifact_layout(&wrong_numeric_sum_crc, reference),
        Err(FormatError::DataArtifactChecksumMismatch {
            scope: "statistics numeric sum"
        })
    ));

    let mut non_numeric_sum_crc = bytes.clone();
    let text_entry = statistics + 2 * STATISTICS_ENTRY_BYTES;
    put_u32(&mut non_numeric_sum_crc, text_entry + 76, 1);
    refresh_section_crc(&mut non_numeric_sum_crc, 3);
    assert!(decode_data_artifact_layout(&non_numeric_sum_crc, reference).is_err());

    let mut wrong_value_crc = bytes.clone();
    let encoded = u32::from_le_bytes(
        wrong_value_crc[statistics + 52..statistics + 56]
            .try_into()
            .unwrap(),
    );
    put_u32(&mut wrong_value_crc, statistics + 52, encoded ^ 1);
    refresh_section_crc(&mut wrong_value_crc, 3);
    assert!(matches!(
        decode_data_artifact_layout(&wrong_value_crc, reference),
        Err(FormatError::DataArtifactChecksumMismatch {
            scope: "statistics value"
        })
    ));

    let mut duplicate_key = bytes;
    let second = statistics + STATISTICS_ENTRY_BYTES;
    let first_column_id = duplicate_key[statistics..statistics + 16].to_vec();
    duplicate_key[second..second + 16].copy_from_slice(&first_column_id);
    put_u32(&mut duplicate_key, second + 16, 0);
    refresh_section_crc(&mut duplicate_key, 3);
    assert!(decode_data_artifact_layout(&duplicate_key, reference).is_err());
}

#[test]
fn bloom_payload_is_checked_lazily_and_rejects_noncanonical_bits() {
    let Fixture { bytes, reference } = fixture();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    let block_index = layout.statistics()[0].bloom_block_index().unwrap() as usize;
    let payload = layout.blocks()[block_index].offset() as usize;

    let mut invalid_hash_count = bytes.clone();
    invalid_hash_count[payload + 6..payload + 8].copy_from_slice(&0_u16.to_le_bytes());
    refresh_block_crc(&mut invalid_hash_count, block_index);
    let layout = decode_data_artifact_layout(&invalid_hash_count, reference).unwrap();
    assert!(read_data_bloom(&invalid_hash_count, &layout, 0, 0).is_err());

    let mut nonzero_unused_bits = bytes;
    let logical_length = layout.blocks()[block_index].logical_length() as usize;
    nonzero_unused_bits[payload + logical_length - 1] |= 0b1000_0000;
    refresh_block_crc(&mut nonzero_unused_bits, block_index);
    let layout = decode_data_artifact_layout(&nonzero_unused_bits, reference).unwrap();
    assert!(read_data_bloom(&nonzero_unused_bits, &layout, 0, 0).is_err());
}

#[test]
fn bloom_normalizes_float_zero_and_nan_aliases_without_false_negatives() {
    let float = column(0, CatalogDataType::scalar(DataType::Float).unwrap(), false);
    let values = vec![
        Value::float(-0.0),
        Value::float(f64::from_bits(0x7ff8_0000_0000_0001)),
    ];
    let blocks = vec![
        DataBlockSpec::row_ids(0, &[1, 2], DataPhysicalCodec::None).unwrap(),
        DataBlockSpec::column(
            0,
            0,
            float,
            &values,
            DataValueEncoding::Plain,
            DataPhysicalCodec::None,
        )
        .unwrap(),
        DataBlockSpec::bloom(
            0,
            0,
            float,
            &values,
            DataBloomConfig::new(257, 5, 42).unwrap(),
            DataPhysicalCodec::None,
        )
        .unwrap(),
    ];
    let statistics = DataStatisticsSpec::from_values(0, 0, float, &values, Some(2)).unwrap();
    let input =
        DataArtifactInput::new(header(2, 1, 1), vec![float], vec![statistics], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    let bloom = read_data_bloom(&bytes, &layout, 0, 0).unwrap().unwrap();
    let statistics = &layout.statistics()[0];
    assert_eq!(statistics.integer_sum(), None);
    assert!(statistics.float_sum().unwrap().is_nan());
    assert_eq!(statistics.numeric_count(), 2);

    assert!(bloom.might_contain(&Value::float(0.0)).unwrap());
    assert!(bloom
        .might_contain(&Value::float(f64::from_bits(0x7ff8_ffff_ffff_ffff)))
        .unwrap());
    assert!(bloom.might_contain(&Value::integer(0)).unwrap());
}
