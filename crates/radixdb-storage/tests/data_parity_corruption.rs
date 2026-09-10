use std::cell::RefCell;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    decode_data_artifact_layout, encode_data_artifact, open_data_artifact_metadata,
    read_data_block, read_data_column, read_data_column_from_source, read_data_row_ids,
    read_data_row_ids_from_source, ArtifactId, ArtifactRef, ArtifactSource, CatalogGeneration,
    DataArtifactHeader, DataArtifactInput, DataBlockKind, DataBlockSpec, DataColumnSpec,
    DataPhysicalCodec, DataValueEncoding, DatabaseGeneration, DatabaseId, FormatError,
    FormatResult, SegmentId, SegmentKind, DATA_BLOCK_REF_BYTES, DATA_FOOTER_BYTES,
    DATA_HEADER_BYTES, DATA_SECTION_REF_BYTES,
};

const GROUP_ROWS: [usize; 2] = [3, 2];

struct Fixture {
    bytes: Vec<u8>,
    reference: ArtifactRef,
    row_ids: [Vec<u64>; 2],
    values: [Vec<Vec<Value>>; 2],
}

struct RangeTracingSource<'a> {
    bytes: &'a [u8],
    reads: RefCell<Vec<(u64, u64)>>,
}

impl<'a> RangeTracingSource<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            reads: RefCell::new(Vec::new()),
        }
    }
}

impl ArtifactSource for RangeTracingSource<'_> {
    fn byte_length(&self) -> FormatResult<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        ArtifactSource::read_exact_at(self.bytes, offset, destination)?;
        self.reads
            .borrow_mut()
            .push((offset, destination.len() as u64));
        Ok(())
    }
}

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn column(ordinal: u8, data_type: CatalogDataType) -> DataColumnSpec {
    DataColumnSpec::new(
        ObjectId::from_user_bytes(raw(0x30 + ordinal)).unwrap(),
        data_type,
        true,
    )
}

fn fixture() -> Fixture {
    let columns = vec![
        column(0, CatalogDataType::scalar(DataType::Integer).unwrap()),
        column(1, CatalogDataType::scalar(DataType::Text).unwrap()),
        column(2, CatalogDataType::scalar(DataType::Boolean).unwrap()),
        column(3, CatalogDataType::vector(2).unwrap()),
    ];
    let row_ids = [vec![7, 11, 19], vec![23, 29]];
    let values = [
        vec![
            vec![
                Value::integer(-7),
                Value::null(DataType::Integer),
                Value::integer(9),
            ],
            vec![Value::text("alpha"), Value::text(""), Value::text("alpha")],
            vec![
                Value::boolean(true),
                Value::null(DataType::Boolean),
                Value::boolean(false),
            ],
            vec![
                Value::vector(vec![1.0, 2.0]),
                Value::null(DataType::Vector),
                Value::vector(vec![3.0, 4.0]),
            ],
        ],
        vec![
            vec![Value::integer(i64::MIN), Value::integer(i64::MAX)],
            vec![Value::null(DataType::Text), Value::text("omega")],
            vec![Value::boolean(false), Value::boolean(true)],
            vec![
                Value::vector(vec![-1.0, -2.0]),
                Value::vector(vec![5.0, 6.0]),
            ],
        ],
    ];

    let mut blocks = Vec::new();
    for group in 0..2_u32 {
        blocks.push(
            DataBlockSpec::row_ids(
                group,
                &row_ids[group as usize],
                if group == 0 {
                    DataPhysicalCodec::None
                } else {
                    DataPhysicalCodec::Lz4
                },
            )
            .unwrap(),
        );
        for (ordinal, spec) in columns.iter().copied().enumerate() {
            blocks.push(
                DataBlockSpec::column(
                    group,
                    ordinal as u32,
                    spec,
                    &values[group as usize][ordinal],
                    if ordinal == 1 {
                        DataValueEncoding::Dictionary
                    } else {
                        DataValueEncoding::Plain
                    },
                    if (group as usize + ordinal) & 1 == 0 {
                        DataPhysicalCodec::None
                    } else {
                        DataPhysicalCodec::Lz4
                    },
                )
                .unwrap(),
            );
        }
    }
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x21)).unwrap(),
        DatabaseId::from_bytes(raw(0x22)).unwrap(),
        ObjectId::from_user_bytes(raw(0x23)).unwrap(),
        SegmentId::from_bytes(raw(0x24)).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        GROUP_ROWS.iter().sum::<usize>() as u64,
        columns.len() as u32,
        GROUP_ROWS.len() as u32,
        SegmentKind::Rows,
        2_345_678,
    )
    .unwrap();
    let input = DataArtifactInput::new(header, columns, vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    Fixture {
        bytes,
        reference,
        row_ids,
        values,
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn section_entry(section_index: usize) -> usize {
    DATA_HEADER_BYTES + section_index * DATA_SECTION_REF_BYTES
}

fn section_offset(bytes: &[u8], section_index: usize) -> usize {
    read_u64(bytes, section_entry(section_index) + 8) as usize
}

fn block_entry(bytes: &[u8], block_index: usize) -> usize {
    section_offset(bytes, 2) + block_index * DATA_BLOCK_REF_BYTES
}

fn refresh_header_crc(bytes: &mut [u8]) {
    let crc = radixdb_core::crc32_ieee(&bytes[..248]);
    put_u32(bytes, 248, crc);
}

fn refresh_section_crc(bytes: &mut [u8], section_index: usize) {
    let entry = section_entry(section_index);
    let offset = read_u64(bytes, entry + 8) as usize;
    let length = read_u64(bytes, entry + 16) as usize;
    let crc = radixdb_core::crc32_ieee(&bytes[offset..offset + length]);
    put_u32(bytes, entry + 40, crc);
}

fn refresh_block_crc(bytes: &mut [u8], block_index: usize) {
    let entry = block_entry(bytes, block_index);
    let offset = read_u64(bytes, entry + 16) as usize;
    let length = read_u64(bytes, entry + 24) as usize;
    let crc = radixdb_core::crc32_ieee(&bytes[offset..offset + length]);
    put_u32(bytes, entry + 48, crc);
    refresh_section_crc(bytes, 2);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn assert_invalid_layout(bytes: &[u8], reference: ArtifactRef) {
    assert!(decode_data_artifact_layout(bytes, reference).is_err());
}

#[test]
fn golden_artifact_slice_and_random_access_readers_have_exact_parity() {
    let fixture = fixture();
    assert_eq!(fixture.bytes.len(), 2_001);
    assert_eq!(
        hex(fixture.reference.body_sha256()),
        "536813bbcbba919060ca8bf0a8ba573485d6e7a51dc6c043efd8b45a7c1c3141"
    );

    let slice_layout = decode_data_artifact_layout(&fixture.bytes, fixture.reference).unwrap();
    let source_layout = open_data_artifact_metadata(fixture.bytes.as_slice(), fixture.reference)
        .unwrap()
        .into_layout();
    assert_eq!(slice_layout, source_layout);

    for group in 0..2_u32 {
        assert_eq!(
            read_data_row_ids(&fixture.bytes, &slice_layout, group).unwrap(),
            fixture.row_ids[group as usize]
        );
        assert_eq!(
            read_data_row_ids_from_source(fixture.bytes.as_slice(), &source_layout, group).unwrap(),
            fixture.row_ids[group as usize]
        );
        for column in 0..4_u32 {
            let from_slice =
                read_data_column(&fixture.bytes, &slice_layout, group, column).unwrap();
            let from_source = read_data_column_from_source(
                fixture.bytes.as_slice(),
                &source_layout,
                group,
                column,
            )
            .unwrap();
            assert_eq!(from_slice, fixture.values[group as usize][column as usize]);
            assert_eq!(from_source, from_slice);
        }
    }
}

#[test]
fn projected_scan_reads_only_row_ids_and_requested_column_blocks() {
    let fixture = fixture();
    let source = RangeTracingSource::new(&fixture.bytes);
    let opened = open_data_artifact_metadata(&source, fixture.reference).unwrap();
    source.reads.borrow_mut().clear();

    let mut projected = Vec::new();
    for group in 0..2_u32 {
        let row_ids = read_data_row_ids_from_source(&source, opened.layout(), group).unwrap();
        let text = read_data_column_from_source(&source, opened.layout(), group, 1).unwrap();
        let vector = read_data_column_from_source(&source, opened.layout(), group, 3).unwrap();
        for row in 0..GROUP_ROWS[group as usize] {
            projected.push((row_ids[row], text[row].clone(), vector[row].clone()));
        }
    }

    let mut expected_rows = Vec::new();
    for (group, row_count) in GROUP_ROWS.into_iter().enumerate() {
        for row in 0..row_count {
            expected_rows.push((
                fixture.row_ids[group][row],
                fixture.values[group][1][row].clone(),
                fixture.values[group][3][row].clone(),
            ));
        }
    }
    assert_eq!(projected, expected_rows);

    let mut expected_ranges = opened
        .layout()
        .blocks()
        .iter()
        .filter(|block| {
            block.kind() == DataBlockKind::RowIds
                || (block.kind() == DataBlockKind::Column
                    && matches!(block.column_ordinal(), 1 | 3))
        })
        .map(|block| (block.offset(), block.stored_length()))
        .collect::<Vec<_>>();
    let mut actual_ranges = source.reads.into_inner();
    expected_ranges.sort_unstable();
    actual_ranges.sort_unstable();
    assert_eq!(actual_ranges, expected_ranges);
}

#[test]
fn every_truncation_extra_byte_and_shell_checksum_mutation_fail_closed() {
    let fixture = fixture();
    for cut in 0..fixture.bytes.len() {
        assert_invalid_layout(&fixture.bytes[..cut], fixture.reference);
    }
    let mut extra = fixture.bytes.clone();
    extra.push(0);
    assert_invalid_layout(&extra, fixture.reference);

    let mut header_crc = fixture.bytes.clone();
    header_crc[104] ^= 1;
    assert!(matches!(
        decode_data_artifact_layout(&header_crc, fixture.reference),
        Err(FormatError::DataArtifactChecksumMismatch { scope: "header" })
    ));

    let mut section_crc = fixture.bytes.clone();
    let columns = section_offset(&section_crc, 0);
    section_crc[columns] ^= 1;
    assert!(matches!(
        decode_data_artifact_layout(&section_crc, fixture.reference),
        Err(FormatError::DataArtifactChecksumMismatch { scope: "section" })
    ));

    let mut footer_identity = fixture.bytes;
    let footer = footer_identity.len() - DATA_FOOTER_BYTES;
    footer_identity[footer + 16] ^= 1;
    assert!(matches!(
        decode_data_artifact_layout(&footer_identity, fixture.reference),
        Err(FormatError::DataArtifactChecksumMismatch {
            scope: "footer identity"
        })
    ));
}

#[test]
fn malformed_lengths_and_unknown_header_directory_or_block_tags_fail_closed() {
    let fixture = fixture();

    let mut header_length = fixture.bytes.clone();
    let malformed_file_length = header_length.len() as u64 - 1;
    put_u64(&mut header_length, 16, malformed_file_length);
    refresh_header_crc(&mut header_length);
    assert_invalid_layout(&header_length, fixture.reference);

    let mut section_length = fixture.bytes.clone();
    let columns = section_entry(0);
    put_u64(&mut section_length, columns + 16, 63);
    put_u64(&mut section_length, columns + 24, 63);
    assert_invalid_layout(&section_length, fixture.reference);

    let mut block_length = fixture.bytes.clone();
    let row_ids = block_entry(&block_length, 0);
    put_u64(&mut block_length, row_ids + 24, u64::MAX);
    refresh_section_crc(&mut block_length, 2);
    assert_invalid_layout(&block_length, fixture.reference);

    let mut future_header = fixture.bytes.clone();
    put_u16(&mut future_header, 8, 7);
    refresh_header_crc(&mut future_header);
    assert!(matches!(
        decode_data_artifact_layout(&future_header, fixture.reference),
        Err(FormatError::UnsupportedFormatVersion {
            owner: "data artifact",
            major: 7,
            minor: 0
        })
    ));

    let mut future_section = fixture.bytes.clone();
    put_u16(&mut future_section, section_entry(0) + 2, 2);
    assert_invalid_layout(&future_section, fixture.reference);

    for (field_offset, unknown) in [(0, 99_u16), (2, 99_u16)] {
        let mut unknown_block = fixture.bytes.clone();
        let entry = block_entry(&unknown_block, 0);
        put_u16(&mut unknown_block, entry + field_offset, unknown);
        refresh_section_crc(&mut unknown_block, 2);
        assert_invalid_layout(&unknown_block, fixture.reference);
    }
    let mut unknown_layout = fixture.bytes;
    let entry = block_entry(&unknown_layout, 0);
    put_u32(&mut unknown_layout, entry + 12, 99);
    refresh_section_crc(&mut unknown_layout, 2);
    assert_invalid_layout(&unknown_layout, fixture.reference);
}

#[test]
fn every_corrupt_payload_block_fails_at_first_access_instead_of_returning_data() {
    let fixture = fixture();
    let layout = decode_data_artifact_layout(&fixture.bytes, fixture.reference).unwrap();
    for block_index in 0..layout.blocks().len() {
        let mut corrupt = fixture.bytes.clone();
        corrupt[layout.blocks()[block_index].offset() as usize] ^= 1;
        let reopened = decode_data_artifact_layout(&corrupt, fixture.reference).unwrap();
        assert!(matches!(
            read_data_block(&corrupt, &reopened, block_index),
            Err(FormatError::DataArtifactChecksumMismatch { scope: "block" })
        ));
    }
}

#[test]
fn checksummed_unknown_logical_payload_is_rejected_by_the_lazy_decoder() {
    let fixture = fixture();
    let layout = decode_data_artifact_layout(&fixture.bytes, fixture.reference).unwrap();

    let mut unknown_row_ids = fixture.bytes.clone();
    unknown_row_ids[layout.blocks()[0].offset() as usize] = b'X';
    refresh_block_crc(&mut unknown_row_ids, 0);
    let reopened = decode_data_artifact_layout(&unknown_row_ids, fixture.reference).unwrap();
    assert!(matches!(
        read_data_row_ids(&unknown_row_ids, &reopened, 0),
        Err(FormatError::InvalidDataArtifact { .. })
    ));

    let first_column = layout
        .blocks()
        .iter()
        .position(|block| block.kind() == DataBlockKind::Column)
        .unwrap();
    let mut unknown_column = fixture.bytes;
    unknown_column[layout.blocks()[first_column].offset() as usize] = b'X';
    refresh_block_crc(&mut unknown_column, first_column);
    let reopened = decode_data_artifact_layout(&unknown_column, fixture.reference).unwrap();
    assert!(matches!(
        read_data_column(&unknown_column, &reopened, 0, 0),
        Err(FormatError::InvalidDataArtifact { .. })
    ));
}
