use std::cell::Cell;
use std::io::Write;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    encode_data_artifact, open_data_artifact_metadata, open_data_artifact_metadata_with_limits,
    read_data_bloom_from_source, read_data_column_from_source, read_data_row_ids_from_source,
    ArtifactFile, ArtifactId, ArtifactRef, ArtifactSource, CatalogGeneration, DataArtifactHeader,
    DataArtifactInput, DataBlockSpec, DataBloomConfig, DataColumnSpec, DataOpenLimits,
    DataPhysicalCodec, DataStatisticsSpec, DataValueEncoding, DatabaseGeneration, DatabaseId,
    FormatError, FormatResult, SegmentId, SegmentKind, DATA_FOOTER_BYTES, DATA_HEADER_BYTES,
    DATA_SECTION_COUNT, DATA_SECTION_REF_BYTES, MAX_DATA_OPEN_METADATA_BYTES, MAX_ROWS_PER_GROUP,
};

struct Fixture {
    bytes: Vec<u8>,
    reference: ArtifactRef,
    row_ids: Vec<u64>,
    values: Vec<Value>,
}

struct CountingSource<'a> {
    bytes: &'a [u8],
    read_calls: Cell<u64>,
    read_bytes: Cell<u64>,
}

impl<'a> CountingSource<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            read_calls: Cell::new(0),
            read_bytes: Cell::new(0),
        }
    }
}

impl ArtifactSource for CountingSource<'_> {
    fn byte_length(&self) -> FormatResult<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        ArtifactSource::read_exact_at(self.bytes, offset, destination)?;
        self.read_calls.set(self.read_calls.get() + 1);
        self.read_bytes
            .set(self.read_bytes.get() + destination.len() as u64);
        Ok(())
    }
}

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn fixture(row_count: usize) -> Fixture {
    let row_ids = (1..=row_count as u64).collect::<Vec<_>>();
    let values = (0..row_count)
        .map(|value| Value::integer(value as i64))
        .collect::<Vec<_>>();
    let column = DataColumnSpec::new(
        ObjectId::from_user_bytes(raw(0x43)).unwrap(),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x41)).unwrap(),
        DatabaseId::from_bytes(raw(0x42)).unwrap(),
        ObjectId::from_user_bytes(raw(0x44)).unwrap(),
        SegmentId::from_bytes(raw(0x45)).unwrap(),
        DatabaseGeneration::new(13).unwrap(),
        CatalogGeneration::new(11).unwrap(),
        301,
        305,
        row_count as u64,
        1,
        1,
        SegmentKind::Rows,
        1_234_567,
    )
    .unwrap();
    let blocks = vec![
        DataBlockSpec::row_ids(0, &row_ids, DataPhysicalCodec::None).unwrap(),
        DataBlockSpec::column(
            0,
            0,
            column,
            &values,
            DataValueEncoding::Plain,
            DataPhysicalCodec::None,
        )
        .unwrap(),
        DataBlockSpec::bloom(
            0,
            0,
            column,
            &values,
            DataBloomConfig::new(257, 5, 0x1122_3344_5566_7788).unwrap(),
            DataPhysicalCodec::None,
        )
        .unwrap(),
    ];
    let statistics = vec![DataStatisticsSpec::from_values(0, 0, column, &values, None).unwrap()];
    let input = DataArtifactInput::new(header, vec![column], statistics, blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    Fixture {
        bytes,
        reference,
        row_ids,
        values,
    }
}

fn section_offset(bytes: &[u8], section_index: usize) -> usize {
    let entry = DATA_HEADER_BYTES + section_index * DATA_SECTION_REF_BYTES;
    u64::from_le_bytes(bytes[entry + 8..entry + 16].try_into().unwrap()) as usize
}

#[test]
fn metadata_open_cost_depends_on_directory_shape_not_total_rows() {
    let small = fixture(1);
    let large = fixture(MAX_ROWS_PER_GROUP as usize);
    assert!(large.bytes.len() > small.bytes.len() * 100);

    let small_open = open_data_artifact_metadata(small.bytes.as_slice(), small.reference).unwrap();
    let large_open = open_data_artifact_metadata(large.bytes.as_slice(), large.reference).unwrap();
    assert_eq!(small_open.metrics(), large_open.metrics());
    assert!(large_open.metrics().read_bytes() * 100 < large.bytes.len() as u64);
    assert_eq!(
        large_open.layout().header().row_count(),
        u64::from(MAX_ROWS_PER_GROUP)
    );

    let decoded_row_ids =
        read_data_row_ids_from_source(large.bytes.as_slice(), large_open.layout(), 0).unwrap();
    assert_eq!(decoded_row_ids, large.row_ids);
    let decoded_values =
        read_data_column_from_source(large.bytes.as_slice(), large_open.layout(), 0, 0).unwrap();
    assert_eq!(decoded_values, large.values);
    let bloom = read_data_bloom_from_source(large.bytes.as_slice(), large_open.layout(), 0, 0)
        .unwrap()
        .unwrap();
    assert!(bloom.might_contain(&Value::integer(0)).unwrap());
    assert!(bloom
        .might_contain(&Value::integer(i64::from(MAX_ROWS_PER_GROUP) - 1))
        .unwrap());
}

#[test]
fn runtime_metadata_budget_is_enforced_before_variable_source_reads() {
    let fixture = fixture(32);
    let opened = open_data_artifact_metadata(fixture.bytes.as_slice(), fixture.reference).unwrap();
    let required = opened.metrics().accounted_allocation_bytes();
    assert!(required > 1);

    let source = CountingSource::new(&fixture.bytes);
    let limits = DataOpenLimits::new(required - 1).unwrap();
    assert!(matches!(
        open_data_artifact_metadata_with_limits(&source, fixture.reference, limits),
        Err(FormatError::DataArtifactLimitExceeded {
            field: "metadata-open accounted bytes",
            ..
        })
    ));
    assert_eq!(source.read_calls.get(), 2);
    assert_eq!(
        source.read_bytes.get(),
        (DATA_HEADER_BYTES + DATA_SECTION_COUNT * DATA_SECTION_REF_BYTES + DATA_FOOTER_BYTES)
            as u64
    );

    assert!(DataOpenLimits::new(0).is_err());
    assert!(DataOpenLimits::new(MAX_DATA_OPEN_METADATA_BYTES + 1).is_err());
}

#[test]
fn metadata_and_payload_corruption_keep_separate_validation_boundaries() {
    let fixture = fixture(16);
    let mut bad_metadata = fixture.bytes.clone();
    let column_directory = section_offset(&bad_metadata, 0);
    bad_metadata[column_directory] ^= 1;
    assert!(matches!(
        open_data_artifact_metadata(bad_metadata.as_slice(), fixture.reference),
        Err(FormatError::DataArtifactChecksumMismatch { scope: "section" })
    ));

    let opened = open_data_artifact_metadata(fixture.bytes.as_slice(), fixture.reference).unwrap();
    let mut bad_payload = fixture.bytes;
    bad_payload[opened.layout().blocks()[0].offset() as usize] ^= 1;
    let reopened = open_data_artifact_metadata(bad_payload.as_slice(), fixture.reference).unwrap();
    assert!(matches!(
        read_data_row_ids_from_source(bad_payload.as_slice(), reopened.layout(), 0),
        Err(FormatError::DataArtifactChecksumMismatch { scope: "block" })
    ));
}

#[test]
fn file_source_supports_metadata_open_and_selected_block_reads() {
    let fixture = fixture(128);
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&fixture.bytes).unwrap();
    file.flush().unwrap();

    let source = ArtifactFile::open(file.path()).unwrap();
    let opened = open_data_artifact_metadata(&source, fixture.reference).unwrap();
    assert_eq!(
        read_data_column_from_source(&source, opened.layout(), 0, 0).unwrap(),
        fixture.values
    );

    let missing = file.path().with_extension("missing");
    assert!(matches!(
        ArtifactFile::open(missing),
        Err(FormatError::ArtifactIo {
            operation: "open",
            kind: std::io::ErrorKind::NotFound
        })
    ));
}

#[test]
fn source_length_mismatch_fails_before_any_read() {
    let fixture = fixture(8);
    let source = CountingSource::new(&fixture.bytes[..fixture.bytes.len() - 1]);
    assert!(matches!(
        open_data_artifact_metadata(&source, fixture.reference),
        Err(FormatError::InvalidDataArtifact { .. })
    ));
    assert_eq!(source.read_calls.get(), 0);
    assert_eq!(source.read_bytes.get(), 0);
}
