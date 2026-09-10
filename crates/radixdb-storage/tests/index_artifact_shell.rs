use std::cell::RefCell;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    decode_data_artifact_layout, decode_index_artifact_layout, encode_data_artifact,
    encode_index_artifact, open_index_artifact_metadata, open_index_artifact_metadata_with_limits,
    read_index_page, read_index_page_from_source, read_index_section, ArtifactId, ArtifactRef,
    ArtifactSource, CatalogGeneration, DataArtifactHeader, DataArtifactInput, DataBlockSpec,
    DataColumnSpec, DataPhysicalCodec, DataValueEncoding, DatabaseGeneration, DatabaseId,
    FormatError, FormatResult, IndexAcceleratorKind, IndexAcceleratorSpec, IndexArtifactHeader,
    IndexArtifactInput, IndexKeyColumn, IndexNullsOrder, IndexOpenLimits, IndexPageCodec,
    IndexPageSpec, IndexSectionKind, IndexSectionSpec, IndexSortDirection, SegmentId, SegmentKind,
    INDEX_ACCELERATOR_ENTRY_BYTES, INDEX_FOOTER_BYTES, INDEX_HEADER_BYTES, INDEX_PAGE_ENTRY_BYTES,
    INDEX_SECTION_ENTRY_BYTES, MAX_INDEX_PAGES,
};

struct Fixture {
    bytes: Vec<u8>,
    reference: ArtifactRef,
    data_bytes: Vec<u8>,
    data_reference: ArtifactRef,
    exact_payload: Vec<u8>,
    ordered_payload: Vec<u8>,
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

fn data_fixture(
    artifact_marker: u8,
) -> (
    Vec<u8>,
    ArtifactRef,
    radixdb_storage::v6::DataArtifactLayout,
) {
    data_fixture_with_rows(artifact_marker, 3)
}

fn data_fixture_with_rows(
    artifact_marker: u8,
    row_count: usize,
) -> (
    Vec<u8>,
    ArtifactRef,
    radixdb_storage::v6::DataArtifactLayout,
) {
    let column = DataColumnSpec::new(
        ObjectId::from_user_bytes(raw(0x31)).unwrap(),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(artifact_marker)).unwrap(),
        DatabaseId::from_bytes(raw(0x22)).unwrap(),
        ObjectId::from_user_bytes(raw(0x23)).unwrap(),
        SegmentId::from_bytes(raw(0x24)).unwrap(),
        DatabaseGeneration::new(13).unwrap(),
        CatalogGeneration::new(11).unwrap(),
        101,
        103,
        row_count as u64,
        1,
        1,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let row_ids = (0..row_count as u64).collect::<Vec<_>>();
    let values = (0..row_count)
        .map(|value| Value::integer(value as i64))
        .collect::<Vec<_>>();
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
    ];
    let input = DataArtifactInput::new(header, vec![column], vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    (bytes, reference, layout)
}

fn key_column() -> IndexKeyColumn {
    IndexKeyColumn::new(
        ObjectId::from_user_bytes(raw(0x31)).unwrap(),
        DataType::Integer,
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )
}

fn fixture() -> Fixture {
    let (data_bytes, data_reference, data) = data_fixture(0x21);
    let exact_payload = b"exact page payload with repeated repeated repeated bytes".to_vec();
    let ordered_payload = b"ordered page payload".to_vec();
    let exact = IndexAcceleratorSpec::new(
        ObjectId::from_user_bytes(raw(0x52)).unwrap(),
        IndexAcceleratorKind::Exact,
        true,
        true,
        [0xa2; 32],
        vec![key_column()],
        3,
        vec![IndexSectionSpec::pages(
            IndexSectionKind::ExactPages,
            vec![
                IndexPageSpec::new(exact_payload.clone(), IndexPageCodec::Lz4, 3, 10, 20).unwrap(),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    let ordered = IndexAcceleratorSpec::new(
        ObjectId::from_user_bytes(raw(0x51)).unwrap(),
        IndexAcceleratorKind::Ordered,
        false,
        false,
        [0xa1; 32],
        vec![key_column()],
        3,
        vec![IndexSectionSpec::pages(
            IndexSectionKind::OrderedPages,
            vec![
                IndexPageSpec::new(ordered_payload.clone(), IndexPageCodec::None, 3, 30, 40)
                    .unwrap(),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    let header = IndexArtifactHeader::for_data(
        ArtifactId::from_bytes(raw(0x61)).unwrap(),
        DatabaseGeneration::new(19).unwrap(),
        CatalogGeneration::new(17).unwrap(),
        &data,
    )
    .unwrap();
    let input = IndexArtifactInput::new(header, &data, vec![exact, ordered]).unwrap();
    let (bytes, reference) = encode_index_artifact(&input).unwrap();
    Fixture {
        bytes,
        reference,
        data_bytes,
        data_reference,
        exact_payload,
        ordered_payload,
    }
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

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn refresh_header_crc(bytes: &mut [u8]) {
    let crc = radixdb_core::crc32_ieee(&bytes[..248]);
    put_u32(bytes, 248, crc);
}

fn refresh_directory_crc(bytes: &mut [u8]) {
    let directory_end = (read_u64(bytes, 200) + read_u64(bytes, 208)) as usize;
    let crc = radixdb_core::crc32_ieee(&bytes[INDEX_HEADER_BYTES..directory_end]);
    put_u32(bytes, 224, crc);
    refresh_header_crc(bytes);
}

#[test]
fn common_container_roundtrips_canonical_directories_and_data_binding() {
    let fixture = fixture();
    let data = decode_data_artifact_layout(&fixture.data_bytes, fixture.data_reference).unwrap();
    let layout = decode_index_artifact_layout(&fixture.bytes, fixture.reference, &data).unwrap();

    assert_eq!(&fixture.bytes[..8], b"RDX6IDX\0");
    assert_eq!(read_u16(&fixture.bytes, 8), 6);
    assert_eq!(read_u16(&fixture.bytes, 10), 0);
    assert_eq!(read_u32(&fixture.bytes, 12), INDEX_HEADER_BYTES as u32);
    assert_eq!(read_u64(&fixture.bytes, 16), fixture.bytes.len() as u64);
    assert_eq!(read_u32(&fixture.bytes, 152), 2);
    assert_eq!(read_u32(&fixture.bytes, 156), 4);
    assert_eq!(read_u64(&fixture.bytes, 160), 2);
    assert_eq!(read_u64(&fixture.bytes, 168), INDEX_HEADER_BYTES as u64);
    assert_eq!(
        read_u64(&fixture.bytes, 176),
        (2 * INDEX_ACCELERATOR_ENTRY_BYTES) as u64
    );
    assert_eq!(
        read_u64(&fixture.bytes, 184),
        (INDEX_HEADER_BYTES + 2 * INDEX_ACCELERATOR_ENTRY_BYTES) as u64
    );
    assert_eq!(
        read_u64(&fixture.bytes, 200),
        (INDEX_HEADER_BYTES + 2 * INDEX_ACCELERATOR_ENTRY_BYTES + 4 * INDEX_SECTION_ENTRY_BYTES)
            as u64
    );
    assert_eq!(
        read_u64(&fixture.bytes, 208),
        (2 * INDEX_PAGE_ENTRY_BYTES) as u64
    );
    let directory_end = (read_u64(&fixture.bytes, 200) + read_u64(&fixture.bytes, 208)) as usize;
    assert_eq!(
        read_u32(&fixture.bytes, 224),
        radixdb_core::crc32_ieee(&fixture.bytes[INDEX_HEADER_BYTES..directory_end])
    );

    let footer = fixture.bytes.len() - INDEX_FOOTER_BYTES;
    assert_eq!(&fixture.bytes[footer..footer + 8], b"RDX6END\0");
    assert_eq!(
        &fixture.bytes[footer + 16..],
        fixture.reference.body_sha256()
    );
    assert_eq!(
        layout.header().data_artifact_id(),
        fixture.data_reference.id()
    );
    assert_eq!(
        layout.header().data_body_sha256(),
        fixture.data_reference.body_sha256()
    );
    assert_eq!(layout.accelerators().len(), 2);
    assert!(
        layout.accelerators()[0].logical_index_id() < layout.accelerators()[1].logical_index_id()
    );
    assert_eq!(
        layout.accelerators()[0].kind(),
        IndexAcceleratorKind::Ordered
    );
    assert_eq!(layout.accelerators()[1].kind(), IndexAcceleratorKind::Exact);
    assert_eq!(layout.accelerators()[0].key_columns(), &[key_column()]);
    assert_eq!(layout.sections().len(), 4);
    assert_eq!(layout.pages().len(), 2);
    assert_eq!(
        read_index_page(&fixture.bytes, &layout, 0).unwrap(),
        fixture.ordered_payload
    );
    assert_eq!(
        read_index_page(&fixture.bytes, &layout, 1).unwrap(),
        fixture.exact_payload
    );
}

#[test]
fn metadata_open_reads_directories_and_key_descriptors_but_no_pages() {
    let fixture = fixture();
    let data = decode_data_artifact_layout(&fixture.data_bytes, fixture.data_reference).unwrap();
    let source = RangeTracingSource::new(&fixture.bytes);
    let opened = open_index_artifact_metadata(&source, fixture.reference, &data).unwrap();
    let reads = source.reads.borrow();

    assert_eq!(opened.layout().pages().len(), 2);
    assert_eq!(opened.metrics().read_calls(), 7);
    assert_eq!(reads.len(), 7);
    for page in opened.layout().pages() {
        assert!(!reads.iter().any(|(offset, length)| {
            let read_end = offset + length;
            let page_end = page.offset() + page.stored_length();
            *offset < page_end && read_end > page.offset()
        }));
    }
    drop(reads);

    let before = source.reads.borrow().len();
    assert_eq!(
        read_index_page_from_source(&source, opened.layout(), 1).unwrap(),
        fixture.exact_payload
    );
    assert_eq!(source.reads.borrow().len(), before + 1);
}

#[test]
fn large_page_directory_is_read_in_bounded_chunks_without_payload_reads() {
    let page_count = 4_097_usize;
    let (_data_bytes, _data_reference, data) = data_fixture_with_rows(0x25, page_count);
    let pages = (0..page_count)
        .map(|ordinal| {
            IndexPageSpec::new(
                vec![ordinal as u8],
                IndexPageCodec::None,
                1,
                ordinal as u64,
                ordinal as u64,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let accelerator = IndexAcceleratorSpec::new(
        ObjectId::from_user_bytes(raw(0x55)).unwrap(),
        IndexAcceleratorKind::Exact,
        false,
        false,
        [0xb5; 32],
        vec![key_column()],
        page_count as u64,
        vec![IndexSectionSpec::pages(IndexSectionKind::ExactPages, pages).unwrap()],
    )
    .unwrap();
    let header = IndexArtifactHeader::for_data(
        ArtifactId::from_bytes(raw(0x65)).unwrap(),
        DatabaseGeneration::new(19).unwrap(),
        CatalogGeneration::new(17).unwrap(),
        &data,
    )
    .unwrap();
    let input = IndexArtifactInput::new(header, &data, vec![accelerator]).unwrap();
    let (bytes, reference) = encode_index_artifact(&input).unwrap();
    let source = RangeTracingSource::new(&bytes);
    let opened = open_index_artifact_metadata(&source, reference, &data).unwrap();
    let reads = source.reads.borrow();

    assert_eq!(opened.layout().pages().len(), page_count);
    let page_directory_offset = read_u64(&bytes, 200);
    let page_directory_length = read_u64(&bytes, 208);
    let directory_reads = reads
        .iter()
        .filter(|(offset, _)| {
            *offset >= page_directory_offset
                && *offset < page_directory_offset + page_directory_length
        })
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(directory_reads.len(), 2);
    assert_eq!(
        directory_reads[0].1,
        (4_096 * INDEX_PAGE_ENTRY_BYTES) as u64
    );
    assert_eq!(directory_reads[1].1, INDEX_PAGE_ENTRY_BYTES as u64);
    for page in opened.layout().pages() {
        assert!(!reads.iter().any(|(offset, length)| {
            let read_end = offset + length;
            let page_end = page.offset() + page.stored_length();
            *offset < page_end && read_end > page.offset()
        }));
    }
}

#[test]
fn page_and_whole_section_checksums_are_lazy_and_fail_closed() {
    let fixture = fixture();
    let data = decode_data_artifact_layout(&fixture.data_bytes, fixture.data_reference).unwrap();
    let layout = decode_index_artifact_layout(&fixture.bytes, fixture.reference, &data).unwrap();
    let page = layout.pages()[0];
    let mut corrupted = fixture.bytes.clone();
    corrupted[page.offset() as usize] ^= 1;

    let reopened = decode_index_artifact_layout(&corrupted, fixture.reference, &data).unwrap();
    assert!(matches!(
        read_index_page(&corrupted, &reopened, 0),
        Err(FormatError::IndexArtifactChecksumMismatch { scope: "page" })
    ));
    let section_index = page.section_index() as usize;
    assert!(matches!(
        read_index_section(&corrupted, &reopened, section_index),
        Err(FormatError::IndexArtifactChecksumMismatch { scope: "section" })
    ));
}

#[test]
fn key_descriptor_corruption_is_rejected_during_metadata_open() {
    let fixture = fixture();
    let data = decode_data_artifact_layout(&fixture.data_bytes, fixture.data_reference).unwrap();
    let layout = decode_index_artifact_layout(&fixture.bytes, fixture.reference, &data).unwrap();
    let key_section = layout.sections()[0].reference();
    let mut corrupted = fixture.bytes.clone();
    corrupted[key_section.offset() as usize] ^= 1;
    assert!(matches!(
        open_index_artifact_metadata(&corrupted[..], fixture.reference, &data),
        Err(FormatError::IndexArtifactChecksumMismatch { scope: "section" })
    ));
}

#[test]
fn empty_pack_and_cross_data_binding_are_rejected() {
    let (_, _, data) = data_fixture(0x21);
    let header = IndexArtifactHeader::for_data(
        ArtifactId::from_bytes(raw(0x61)).unwrap(),
        DatabaseGeneration::new(19).unwrap(),
        CatalogGeneration::new(17).unwrap(),
        &data,
    )
    .unwrap();
    assert!(matches!(
        IndexArtifactInput::new(header, &data, vec![]),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let fixture = fixture();
    let (_, _, other_data) = data_fixture(0x29);
    assert!(matches!(
        decode_index_artifact_layout(&fixture.bytes, fixture.reference, &other_data),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));
}

#[test]
fn malformed_versions_flags_and_directory_shape_are_rejected() {
    let fixture = fixture();
    let data = decode_data_artifact_layout(&fixture.data_bytes, fixture.data_reference).unwrap();

    let mut future = fixture.bytes.clone();
    put_u16(&mut future, 8, 7);
    refresh_header_crc(&mut future);
    assert!(matches!(
        decode_index_artifact_layout(&future, fixture.reference, &data),
        Err(FormatError::UnsupportedFormatVersion {
            owner: "index artifact",
            major: 7,
            minor: 0
        })
    ));

    let mut unknown_kind = fixture.bytes.clone();
    put_u16(&mut unknown_kind, INDEX_HEADER_BYTES + 16, 99);
    refresh_directory_crc(&mut unknown_kind);
    assert!(matches!(
        decode_index_artifact_layout(&unknown_kind, fixture.reference, &data),
        Err(FormatError::UnknownIndexAcceleratorKind { tag: 99 })
    ));

    let mut future_accelerator = fixture.bytes.clone();
    put_u16(&mut future_accelerator, INDEX_HEADER_BYTES + 18, 2);
    refresh_directory_crc(&mut future_accelerator);
    assert!(matches!(
        decode_index_artifact_layout(&future_accelerator, fixture.reference, &data),
        Err(FormatError::UnsupportedIndexAcceleratorVersion { version: 2 })
    ));

    let mut bad_directory = fixture.bytes.clone();
    let section_offset_field = 184;
    let value = read_u64(&bad_directory, section_offset_field) + 8;
    bad_directory[section_offset_field..section_offset_field + 8]
        .copy_from_slice(&value.to_le_bytes());
    refresh_header_crc(&mut bad_directory);
    assert!(matches!(
        decode_index_artifact_layout(&bad_directory, fixture.reference, &data),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let mut corrupted_directory = fixture.bytes.clone();
    corrupted_directory[INDEX_HEADER_BYTES + 24] ^= 1;
    assert!(matches!(
        decode_index_artifact_layout(&corrupted_directory, fixture.reference, &data),
        Err(FormatError::IndexArtifactChecksumMismatch { scope: "directory" })
    ));

    let mut excessive_page_count = fixture.bytes.clone();
    excessive_page_count[160..168].copy_from_slice(&(MAX_INDEX_PAGES + 1).to_le_bytes());
    refresh_header_crc(&mut excessive_page_count);
    assert!(matches!(
        decode_index_artifact_layout(&excessive_page_count, fixture.reference, &data),
        Err(FormatError::IndexArtifactLimitExceeded {
            field: "page count",
            actual,
            limit
        }) if actual == MAX_INDEX_PAGES + 1 && limit == MAX_INDEX_PAGES
    ));
}

#[test]
fn runtime_metadata_budget_fails_before_descriptor_admission() {
    let fixture = fixture();
    let data = decode_data_artifact_layout(&fixture.data_bytes, fixture.data_reference).unwrap();
    let limit = IndexOpenLimits::new(1).unwrap();
    assert!(matches!(
        open_index_artifact_metadata_with_limits(
            &fixture.bytes[..],
            fixture.reference,
            &data,
            limit
        ),
        Err(FormatError::IndexArtifactLimitExceeded {
            field: "metadata-open accounted bytes",
            ..
        })
    ));
}

#[test]
fn every_prefix_truncation_and_trailing_bytes_are_rejected_without_panic() {
    let fixture = fixture();
    let data = decode_data_artifact_layout(&fixture.data_bytes, fixture.data_reference).unwrap();
    for length in 0..fixture.bytes.len() {
        assert!(
            decode_index_artifact_layout(&fixture.bytes[..length], fixture.reference, &data)
                .is_err()
        );
    }
    let mut trailing = fixture.bytes.clone();
    trailing.push(0);
    assert!(decode_index_artifact_layout(&trailing, fixture.reference, &data).is_err());
}

#[test]
fn canonical_construction_is_byte_deterministic() {
    let first = fixture();
    let second = fixture();
    assert_eq!(first.bytes, second.bytes);
    assert_eq!(first.reference, second.reference);
}

#[test]
fn constructors_reject_wrong_key_and_accelerator_section_shapes() {
    let exact_page = IndexPageSpec::new(vec![1], IndexPageCodec::None, 1, 0, 0).unwrap();
    let wrong_section =
        IndexSectionSpec::pages(IndexSectionKind::OrderedPages, vec![exact_page]).unwrap();
    assert!(IndexAcceleratorSpec::new(
        ObjectId::from_user_bytes(raw(0x51)).unwrap(),
        IndexAcceleratorKind::Exact,
        false,
        false,
        [0; 32],
        vec![key_column()],
        1,
        vec![wrong_section],
    )
    .is_err());

    let (_, _, data) = data_fixture(0x21);
    let header = IndexArtifactHeader::for_data(
        ArtifactId::from_bytes(raw(0x61)).unwrap(),
        DatabaseGeneration::new(19).unwrap(),
        CatalogGeneration::new(17).unwrap(),
        &data,
    )
    .unwrap();
    let missing_column = IndexKeyColumn::new(
        ObjectId::from_user_bytes(raw(0x77)).unwrap(),
        DataType::Integer,
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    );
    let exact = IndexAcceleratorSpec::new(
        ObjectId::from_user_bytes(raw(0x51)).unwrap(),
        IndexAcceleratorKind::Exact,
        false,
        false,
        [0; 32],
        vec![missing_column],
        1,
        vec![IndexSectionSpec::pages(
            IndexSectionKind::ExactPages,
            vec![IndexPageSpec::new(vec![1], IndexPageCodec::None, 1, 0, 0).unwrap()],
        )
        .unwrap()],
    )
    .unwrap();
    assert!(IndexArtifactInput::new(header, &data, vec![exact]).is_err());

    let duplicate_key = key_column();
    let duplicate_keys = IndexAcceleratorSpec::new(
        ObjectId::from_user_bytes(raw(0x53)).unwrap(),
        IndexAcceleratorKind::Exact,
        false,
        false,
        [0; 32],
        vec![duplicate_key, duplicate_key],
        1,
        vec![IndexSectionSpec::pages(
            IndexSectionKind::ExactPages,
            vec![IndexPageSpec::new(vec![1], IndexPageCodec::None, 1, 0, 0).unwrap()],
        )
        .unwrap()],
    );
    assert!(matches!(
        duplicate_keys,
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let too_many_primary_items = IndexAcceleratorSpec::new(
        ObjectId::from_user_bytes(raw(0x54)).unwrap(),
        IndexAcceleratorKind::Exact,
        false,
        false,
        [0; 32],
        vec![key_column()],
        1,
        vec![IndexSectionSpec::pages(
            IndexSectionKind::ExactPages,
            vec![IndexPageSpec::new(vec![1], IndexPageCodec::None, 2, 0, 0).unwrap()],
        )
        .unwrap()],
    );
    assert!(matches!(
        too_many_primary_items,
        Err(FormatError::InvalidIndexArtifact { .. })
    ));
}
