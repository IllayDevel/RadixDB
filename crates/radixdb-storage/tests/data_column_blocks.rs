use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    decode_data_artifact_layout, encode_data_artifact, read_data_column, ArtifactId, ArtifactRef,
    CatalogGeneration, DataArtifactHeader, DataArtifactInput, DataBlockSpec, DataColumnSpec,
    DataPhysicalCodec, DataValueEncoding, DatabaseGeneration, DatabaseId, FormatError, SegmentId,
    SegmentKind, DATA_BLOCK_REF_BYTES, DATA_HEADER_BYTES, DATA_SECTION_REF_BYTES,
};

type ColumnCase = (DataColumnSpec, Vec<Value>, Vec<Value>);

struct Fixture {
    bytes: Vec<u8>,
    reference: ArtifactRef,
    cases: Vec<ColumnCase>,
}

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn column_id(ordinal: usize) -> ObjectId {
    ObjectId::from_user_bytes(raw(0x40 + ordinal as u8)).unwrap()
}

fn header(column_count: u32) -> DataArtifactHeader {
    DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x21)).unwrap(),
        DatabaseId::from_bytes(raw(0x22)).unwrap(),
        ObjectId::from_user_bytes(raw(0x23)).unwrap(),
        SegmentId::from_bytes(raw(0x24)).unwrap(),
        DatabaseGeneration::new(11).unwrap(),
        CatalogGeneration::new(9).unwrap(),
        201,
        205,
        6,
        column_count,
        2,
        SegmentKind::Rows,
        987_654,
    )
    .unwrap()
}

fn spec(ordinal: usize, data_type: CatalogDataType) -> DataColumnSpec {
    DataColumnSpec::new(column_id(ordinal), data_type, true)
}

fn cases() -> Vec<ColumnCase> {
    vec![
        (
            spec(0, CatalogDataType::scalar(DataType::Integer).unwrap()),
            vec![
                Value::integer(-7),
                Value::null(DataType::Integer),
                Value::integer(9),
            ],
            vec![
                Value::integer(i64::MIN),
                Value::integer(0),
                Value::integer(i64::MAX),
            ],
        ),
        (
            spec(1, CatalogDataType::scalar(DataType::Float).unwrap()),
            vec![
                Value::float(-1.25),
                Value::null(DataType::Float),
                Value::float(3.5),
            ],
            vec![
                Value::float(-0.0),
                Value::float(0.0),
                Value::float(f64::MAX),
            ],
        ),
        (
            spec(2, CatalogDataType::scalar(DataType::Text).unwrap()),
            vec![
                Value::text("z"),
                Value::null(DataType::Text),
                Value::text("a"),
            ],
            vec![
                Value::text("alpha"),
                Value::text("beta"),
                Value::text("alpha"),
            ],
        ),
        (
            spec(3, CatalogDataType::scalar(DataType::Boolean).unwrap()),
            vec![
                Value::boolean(true),
                Value::null(DataType::Boolean),
                Value::boolean(false),
            ],
            vec![
                Value::boolean(false),
                Value::boolean(true),
                Value::boolean(true),
            ],
        ),
        (
            spec(4, CatalogDataType::scalar(DataType::Timestamp).unwrap()),
            vec![
                Value::timestamp(chrono::DateTime::from_timestamp_nanos(-1)),
                Value::null(DataType::Timestamp),
                Value::timestamp(chrono::DateTime::from_timestamp_nanos(1)),
            ],
            vec![
                Value::timestamp(chrono::DateTime::from_timestamp_nanos(i64::MIN)),
                Value::timestamp(chrono::DateTime::from_timestamp_nanos(0)),
                Value::timestamp(chrono::DateTime::from_timestamp_nanos(i64::MAX)),
            ],
        ),
        (
            spec(5, CatalogDataType::scalar(DataType::Json).unwrap()),
            vec![
                Value::try_json("{\"z\":1}").unwrap(),
                Value::null(DataType::Json),
                Value::try_json("[1,2]").unwrap(),
            ],
            vec![
                Value::try_json("true").unwrap(),
                Value::try_json("null").unwrap(),
                Value::try_json("true").unwrap(),
            ],
        ),
        (
            spec(6, CatalogDataType::vector(2).unwrap()),
            vec![
                Value::vector(vec![1.0, -2.0]),
                Value::null(DataType::Vector),
                Value::vector(vec![3.5, 4.5]),
            ],
            vec![
                Value::vector(vec![0.0, 1.0]),
                Value::vector(vec![2.0, 3.0]),
                Value::vector(vec![4.0, 5.0]),
            ],
        ),
        (
            spec(7, CatalogDataType::scalar(DataType::Uuid).unwrap()),
            vec![
                Value::uuid(raw(1)),
                Value::null(DataType::Uuid),
                Value::uuid(raw(2)),
            ],
            vec![
                Value::uuid(raw(3)),
                Value::uuid(raw(4)),
                Value::uuid(raw(5)),
            ],
        ),
        (
            spec(8, CatalogDataType::decimal(10, 2).unwrap()),
            vec![
                Value::try_decimal(-1234, 10, 2).unwrap(),
                Value::null(DataType::Decimal),
                Value::try_decimal(5678, 10, 2).unwrap(),
            ],
            vec![
                Value::try_decimal(0, 10, 2).unwrap(),
                Value::try_decimal(1, 10, 2).unwrap(),
                Value::try_decimal(-1, 10, 2).unwrap(),
            ],
        ),
        (
            spec(9, CatalogDataType::scalar(DataType::Date).unwrap()),
            vec![Value::date(-1), Value::null(DataType::Date), Value::date(1)],
            vec![Value::date(i32::MIN), Value::date(0), Value::date(i32::MAX)],
        ),
        (
            spec(10, CatalogDataType::scalar(DataType::Bytes).unwrap()),
            vec![
                Value::bytes(vec![2]),
                Value::null(DataType::Bytes),
                Value::bytes(vec![1]),
            ],
            vec![
                Value::bytes(vec![0, 1]),
                Value::bytes(vec![]),
                Value::bytes(vec![0, 1]),
            ],
        ),
        (
            spec(11, CatalogDataType::unconstrained_decimal().unwrap()),
            vec![
                Value::try_decimal(-1234, 4, 2).unwrap(),
                Value::null(DataType::Decimal),
                Value::try_decimal(5, 1, 0).unwrap(),
            ],
            vec![
                Value::try_decimal(1, 1, 0).unwrap(),
                Value::try_decimal(100, 3, 2).unwrap(),
                Value::try_decimal(-123_456, 6, 3).unwrap(),
            ],
        ),
    ]
}

fn fixture() -> Fixture {
    let cases = cases();
    let specs = cases.iter().map(|case| case.0).collect::<Vec<_>>();
    let mut blocks = vec![DataBlockSpec::row_ids(0, &[1, 3, 7], DataPhysicalCodec::None).unwrap()];
    for (ordinal, (column, values, _)) in cases.iter().enumerate() {
        let encoding = if matches!(
            column.data_type().logical_type(),
            DataType::Json | DataType::Bytes
        ) {
            DataValueEncoding::Dictionary
        } else {
            DataValueEncoding::Plain
        };
        blocks.push(
            DataBlockSpec::column(
                0,
                ordinal as u32,
                *column,
                values,
                encoding,
                DataPhysicalCodec::None,
            )
            .unwrap(),
        );
    }
    blocks.push(DataBlockSpec::row_ids(1, &[8, 11, 20], DataPhysicalCodec::Lz4).unwrap());
    for (ordinal, (column, _, values)) in cases.iter().enumerate() {
        blocks.push(
            DataBlockSpec::column(
                1,
                ordinal as u32,
                *column,
                values,
                DataValueEncoding::Plain,
                DataPhysicalCodec::Lz4,
            )
            .unwrap(),
        );
    }
    let input = DataArtifactInput::new(header(specs.len() as u32), specs, vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    Fixture {
        bytes,
        reference,
        cases,
    }
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
fn all_public_column_types_roundtrip_across_plain_dictionary_none_and_lz4() {
    let Fixture {
        bytes,
        reference,
        cases,
    } = fixture();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();

    assert_eq!(layout.columns().len(), cases.len());
    for (ordinal, (spec, first, second)) in cases.iter().enumerate() {
        let column = layout.columns()[ordinal];
        assert_eq!(column.column_id(), spec.column_id());
        assert_eq!(column.ordinal(), ordinal as u32);
        assert_eq!(column.data_type(), spec.data_type());
        assert!(column.nullable());
        assert_eq!(column.first_block_index(), ordinal as u32 + 1);
        assert_eq!(column.block_count(), 2);
        assert_eq!(column.statistics_entry_index(), u32::MAX);
        assert_eq!(
            read_data_column(&bytes, &layout, 0, ordinal as u32).unwrap(),
            *first
        );
        assert_eq!(
            read_data_column(&bytes, &layout, 1, ordinal as u32).unwrap(),
            *second
        );
    }
}

#[test]
fn writer_rejects_value_descriptor_and_group_shape_mismatches() {
    let integer = spec(0, CatalogDataType::scalar(DataType::Integer).unwrap());
    assert!(DataBlockSpec::column(
        0,
        0,
        integer,
        &[Value::text("wrong")],
        DataValueEncoding::Plain,
        DataPhysicalCodec::None,
    )
    .is_err());
    let non_nullable = DataColumnSpec::new(integer.column_id(), integer.data_type(), false);
    assert!(DataBlockSpec::column(
        0,
        0,
        non_nullable,
        &[Value::null(DataType::Integer)],
        DataValueEncoding::Plain,
        DataPhysicalCodec::None,
    )
    .is_err());
    assert!(DataBlockSpec::column(
        0,
        0,
        integer,
        &[Value::integer(1)],
        DataValueEncoding::Dictionary,
        DataPhysicalCodec::None,
    )
    .is_err());
    let vector = spec(0, CatalogDataType::vector(1).unwrap());
    assert!(DataBlockSpec::column(
        0,
        0,
        vector,
        &[Value::vector(vec![f32::NAN])],
        DataValueEncoding::Plain,
        DataPhysicalCodec::None,
    )
    .is_err());

    let row_ids = DataBlockSpec::row_ids(0, &[1, 2, 3], DataPhysicalCodec::None).unwrap();
    let column = DataBlockSpec::column(
        0,
        0,
        integer,
        &[Value::integer(1), Value::integer(2)],
        DataValueEncoding::Plain,
        DataPhysicalCodec::None,
    )
    .unwrap();
    assert!(DataArtifactInput::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes(raw(0x21)).unwrap(),
            DatabaseId::from_bytes(raw(0x22)).unwrap(),
            ObjectId::from_user_bytes(raw(0x23)).unwrap(),
            SegmentId::from_bytes(raw(0x24)).unwrap(),
            DatabaseGeneration::new(11).unwrap(),
            CatalogGeneration::new(9).unwrap(),
            1,
            1,
            3,
            1,
            1,
            SegmentKind::Rows,
            0,
        )
        .unwrap(),
        vec![integer],
        vec![],
        vec![row_ids, column],
    )
    .is_err());

    let row_ids = DataBlockSpec::row_ids(0, &[1, 2, 3], DataPhysicalCodec::None).unwrap();
    let column = DataBlockSpec::column(
        0,
        0,
        integer,
        &[Value::integer(1), Value::integer(2), Value::integer(3)],
        DataValueEncoding::Plain,
        DataPhysicalCodec::None,
    )
    .unwrap();
    assert!(DataArtifactInput::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes(raw(0x21)).unwrap(),
            DatabaseId::from_bytes(raw(0x22)).unwrap(),
            ObjectId::from_user_bytes(raw(0x23)).unwrap(),
            SegmentId::from_bytes(raw(0x24)).unwrap(),
            DatabaseGeneration::new(11).unwrap(),
            CatalogGeneration::new(9).unwrap(),
            1,
            1,
            3,
            1,
            1,
            SegmentKind::Rows,
            0,
        )
        .unwrap(),
        vec![DataColumnSpec::new(
            integer.column_id(),
            integer.data_type(),
            false,
        )],
        vec![],
        vec![row_ids, column],
    )
    .is_err());
}

#[test]
fn adaptive_variable_encoding_selects_the_smaller_canonical_layout() {
    let text = spec(0, CatalogDataType::scalar(DataType::Text).unwrap());
    let repeated = (0..4096)
        .map(|index| Value::text(format!("payload-{}", index % 16)))
        .collect::<Vec<_>>();
    let adaptive = DataBlockSpec::column(
        0,
        0,
        text,
        &repeated,
        DataValueEncoding::Adaptive,
        DataPhysicalCodec::Lz4,
    )
    .unwrap();
    let plain = DataBlockSpec::column(
        0,
        0,
        text,
        &repeated,
        DataValueEncoding::Plain,
        DataPhysicalCodec::Lz4,
    )
    .unwrap();
    let dictionary = DataBlockSpec::column(
        0,
        0,
        text,
        &repeated,
        DataValueEncoding::Dictionary,
        DataPhysicalCodec::Lz4,
    )
    .unwrap();
    assert_eq!(
        adaptive.layout(),
        radixdb_storage::v6::DataLayout::DictionaryValues
    );
    assert!(adaptive.stored_bytes().len() < plain.stored_bytes().len());
    assert_eq!(
        adaptive.stored_bytes().len(),
        dictionary.stored_bytes().len()
    );

    let unique = (0..4096)
        .map(|index| Value::text(format!("unique-payload-{index:08x}")))
        .collect::<Vec<_>>();
    let adaptive = DataBlockSpec::column(
        0,
        0,
        text,
        &unique,
        DataValueEncoding::Adaptive,
        DataPhysicalCodec::Lz4,
    )
    .unwrap();
    let plain = DataBlockSpec::column(
        0,
        0,
        text,
        &unique,
        DataValueEncoding::Plain,
        DataPhysicalCodec::Lz4,
    )
    .unwrap();
    let dictionary = DataBlockSpec::column(
        0,
        0,
        text,
        &unique,
        DataValueEncoding::Dictionary,
        DataPhysicalCodec::Lz4,
    )
    .unwrap();
    assert_eq!(
        adaptive.layout(),
        radixdb_storage::v6::DataLayout::PlainValues
    );
    assert_eq!(adaptive.stored_bytes().len(), plain.stored_bytes().len());
    assert!(adaptive.stored_bytes().len() <= dictionary.stored_bytes().len());
}

#[test]
fn declared_decimal_capacity_accepts_exact_values_with_distinct_payload_parameters() {
    let decimal = spec(0, CatalogDataType::decimal(12, 3).unwrap());
    let values = vec![
        Value::try_decimal(12_340, 5, 3).unwrap(),
        Value::try_decimal(-1_234, 4, 2).unwrap(),
        Value::try_decimal(123_400, 6, 4).unwrap(),
    ];
    let block = DataBlockSpec::column(
        0,
        0,
        decimal,
        &values,
        DataValueEncoding::Plain,
        DataPhysicalCodec::None,
    )
    .expect("values fitting DECIMAL(12,3) must be accepted");
    let input = DataArtifactInput::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes(raw(0x21)).unwrap(),
            DatabaseId::from_bytes(raw(0x22)).unwrap(),
            ObjectId::from_user_bytes(raw(0x23)).unwrap(),
            SegmentId::from_bytes(raw(0x24)).unwrap(),
            DatabaseGeneration::new(11).unwrap(),
            CatalogGeneration::new(9).unwrap(),
            201,
            205,
            3,
            1,
            1,
            SegmentKind::Rows,
            987_654,
        )
        .unwrap(),
        vec![decimal],
        vec![],
        vec![
            DataBlockSpec::row_ids(0, &[1, 2, 3], DataPhysicalCodec::None).unwrap(),
            block,
        ],
    )
    .unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    assert_eq!(read_data_column(&bytes, &layout, 0, 0).unwrap(), values);

    for outside_capacity in [
        Value::try_decimal(12_341, 5, 4).unwrap(),
        Value::try_decimal(1_234_567_890, 10, 0).unwrap(),
    ] {
        assert!(DataBlockSpec::column(
            0,
            0,
            decimal,
            &[outside_capacity],
            DataValueEncoding::Plain,
            DataPhysicalCodec::None,
        )
        .is_err());
    }
}

#[test]
fn column_directory_and_payload_corruption_fail_closed_at_the_right_boundary() {
    let Fixture {
        bytes, reference, ..
    } = fixture();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();

    let mut bad_ordinal = bytes.clone();
    let columns = layout.sections()[0].offset() as usize;
    put_u32(&mut bad_ordinal, columns + 16, 1);
    refresh_section_crc(&mut bad_ordinal, 0);
    assert!(matches!(
        decode_data_artifact_layout(&bad_ordinal, reference),
        Err(FormatError::InvalidDataArtifact { .. })
    ));

    let mut bad_null_slot = bytes.clone();
    let integer_block = 1;
    let payload = layout.blocks()[integer_block].offset() as usize;
    bad_null_slot[payload + 32 + 1 + 8] = 1;
    refresh_block_crc(&mut bad_null_slot, integer_block);
    let reopened = decode_data_artifact_layout(&bad_null_slot, reference).unwrap();
    assert!(matches!(
        read_data_column(&bad_null_slot, &reopened, 0, 0),
        Err(FormatError::InvalidDataArtifact { .. })
    ));

    let mut bad_boolean = bytes.clone();
    let boolean_block = 4;
    let payload = layout.blocks()[boolean_block].offset() as usize;
    bad_boolean[payload + 32 + 1] = 2;
    refresh_block_crc(&mut bad_boolean, boolean_block);
    let reopened = decode_data_artifact_layout(&bad_boolean, reference).unwrap();
    assert!(matches!(
        read_data_column(&bad_boolean, &reopened, 0, 3),
        Err(FormatError::InvalidDataArtifact { .. })
    ));

    let mut bad_variable_null = bytes.clone();
    let text_block = 3;
    let payload = layout.blocks()[text_block].offset() as usize;
    put_u32(&mut bad_variable_null, payload + 32 + 1 + 8, 2);
    refresh_block_crc(&mut bad_variable_null, text_block);
    let reopened = decode_data_artifact_layout(&bad_variable_null, reference).unwrap();
    assert!(matches!(
        read_data_column(&bad_variable_null, &reopened, 0, 2),
        Err(FormatError::InvalidDataArtifact { .. })
    ));

    let mut bad_dictionary_code = bytes;
    let json_block = 6;
    let payload = layout.blocks()[json_block].offset() as usize;
    let distinct = u32::from_le_bytes(
        bad_dictionary_code[payload + 12..payload + 16]
            .try_into()
            .unwrap(),
    ) as usize;
    let dictionary_length = u64::from_le_bytes(
        bad_dictionary_code[payload + 24..payload + 32]
            .try_into()
            .unwrap(),
    ) as usize;
    let codes = payload + 40 + 1 + (distinct + 1) * 4 + dictionary_length;
    put_u32(&mut bad_dictionary_code, codes, u32::MAX);
    refresh_block_crc(&mut bad_dictionary_code, json_block);
    let reopened = decode_data_artifact_layout(&bad_dictionary_code, reference).unwrap();
    assert!(matches!(
        read_data_column(&bad_dictionary_code, &reopened, 0, 5),
        Err(FormatError::InvalidDataArtifact { .. })
    ));
}
