use std::cell::RefCell;

#[path = "support/exact_selection.rs"]
mod selection;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    decode_data_artifact_layout, decode_exact_index_page, decode_index_artifact_layout,
    encode_data_artifact, encode_exact_index_pages, encode_index_artifact, lookup_exact_index,
    lookup_exact_index_from_source, read_index_page, ArtifactId, ArtifactRef, ArtifactSource,
    CatalogGeneration, DataArtifactHeader, DataArtifactInput, DataArtifactLayout, DataBlockSpec,
    DataColumnSpec, DataPhysicalCodec, DataValueEncoding, DatabaseGeneration, DatabaseId,
    ExactIndexEntry, ExactIndexKey, ExactPageBuildLimits, FormatError, FormatResult,
    IndexAcceleratorKind, IndexAcceleratorSpec, IndexArtifactHeader, IndexArtifactInput,
    IndexKeyColumn, IndexNullsOrder, IndexPageCodec, IndexPageSpec, IndexSectionKind,
    IndexSectionSpec, IndexSortDirection, SegmentId, SegmentKind,
};

struct RangeTracingSource<'a> {
    bytes: &'a [u8],
    reads: RefCell<Vec<(u64, u64)>>,
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

fn column_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn data_fixture(
    row_count: usize,
) -> (
    Vec<u8>,
    ArtifactRef,
    DataArtifactLayout,
    Vec<DataColumnSpec>,
) {
    let columns = vec![
        DataColumnSpec::new(
            column_id(0x31),
            CatalogDataType::scalar(DataType::Integer).unwrap(),
            false,
        ),
        DataColumnSpec::new(
            column_id(0x32),
            CatalogDataType::scalar(DataType::Text).unwrap(),
            false,
        ),
        DataColumnSpec::new(
            column_id(0x33),
            CatalogDataType::scalar(DataType::Float).unwrap(),
            true,
        ),
    ];
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x41)).unwrap(),
        DatabaseId::from_bytes(raw(0x42)).unwrap(),
        column_id(0x43),
        SegmentId::from_bytes(raw(0x44)).unwrap(),
        DatabaseGeneration::new(13).unwrap(),
        CatalogGeneration::new(11).unwrap(),
        101,
        103,
        row_count as u64,
        columns.len() as u32,
        1,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let row_ids = (0..row_count as u64).collect::<Vec<_>>();
    let integers = (0..row_count)
        .map(|value| Value::integer(value as i64))
        .collect::<Vec<_>>();
    let texts = (0..row_count)
        .map(|value| Value::text(format!("value-{value}")))
        .collect::<Vec<_>>();
    let floats = (0..row_count)
        .map(|value| {
            if value.is_multiple_of(5) {
                Value::null(DataType::Float)
            } else {
                Value::float(value as f64 / 10.0)
            }
        })
        .collect::<Vec<_>>();
    let blocks = vec![
        DataBlockSpec::row_ids(0, &row_ids, DataPhysicalCodec::None).unwrap(),
        DataBlockSpec::column(
            0,
            0,
            columns[0],
            &integers,
            DataValueEncoding::Plain,
            DataPhysicalCodec::None,
        )
        .unwrap(),
        DataBlockSpec::column(
            0,
            1,
            columns[1],
            &texts,
            DataValueEncoding::Dictionary,
            DataPhysicalCodec::Lz4,
        )
        .unwrap(),
        DataBlockSpec::column(
            0,
            2,
            columns[2],
            &floats,
            DataValueEncoding::Plain,
            DataPhysicalCodec::None,
        )
        .unwrap(),
    ];
    let input = DataArtifactInput::new(header, columns.clone(), vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    (bytes, reference, layout, columns)
}

fn key_column(column: DataColumnSpec) -> IndexKeyColumn {
    IndexKeyColumn::new(
        column.column_id(),
        column.data_type().logical_type(),
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )
}

fn exact_container(
    data: &DataArtifactLayout,
    key_columns: Vec<IndexKeyColumn>,
    entries: Vec<ExactIndexEntry>,
    unique: bool,
    limits: ExactPageBuildLimits,
) -> (Vec<u8>, ArtifactRef) {
    let indexed_item_count = entries
        .iter()
        .map(|entry| entry.row_ordinals().len() as u64)
        .sum();
    let pages = encode_exact_index_pages(
        entries,
        unique,
        data.header().row_count(),
        IndexPageCodec::Lz4,
        limits,
    )
    .unwrap();
    exact_container_from_pages(data, key_columns, pages, unique, indexed_item_count)
}

fn exact_container_from_pages(
    data: &DataArtifactLayout,
    key_columns: Vec<IndexKeyColumn>,
    pages: Vec<IndexPageSpec>,
    unique: bool,
    indexed_item_count: u64,
) -> (Vec<u8>, ArtifactRef) {
    let accelerator = IndexAcceleratorSpec::new(
        column_id(0x51),
        IndexAcceleratorKind::Exact,
        unique,
        unique,
        [0x52; 32],
        key_columns,
        indexed_item_count,
        vec![IndexSectionSpec::pages(IndexSectionKind::ExactPages, pages).unwrap()],
    )
    .unwrap();
    let header = IndexArtifactHeader::for_data(
        ArtifactId::from_bytes(raw(0x53)).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(15).unwrap(),
        data,
    )
    .unwrap();
    encode_index_artifact(&IndexArtifactInput::new(header, data, vec![accelerator]).unwrap())
        .unwrap()
}

fn all_types_fixture() -> (
    Vec<u8>,
    ArtifactRef,
    DataArtifactLayout,
    Vec<DataColumnSpec>,
    Vec<Value>,
) {
    let types = vec![
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        CatalogDataType::scalar(DataType::Float).unwrap(),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        CatalogDataType::scalar(DataType::Boolean).unwrap(),
        CatalogDataType::scalar(DataType::Timestamp).unwrap(),
        CatalogDataType::scalar(DataType::Json).unwrap(),
        CatalogDataType::vector(3).unwrap(),
        CatalogDataType::scalar(DataType::Uuid).unwrap(),
        CatalogDataType::decimal(12, 3).unwrap(),
        CatalogDataType::scalar(DataType::Date).unwrap(),
        CatalogDataType::scalar(DataType::Bytes).unwrap(),
    ];
    let columns = types
        .into_iter()
        .enumerate()
        .map(|(index, data_type)| {
            DataColumnSpec::new(column_id(0x60 + index as u8), data_type, false)
        })
        .collect::<Vec<_>>();
    let values = vec![
        Value::integer(-42),
        Value::float(f64::NAN),
        Value::text("all-types"),
        Value::boolean(true),
        Value::timestamp(chrono::DateTime::from_timestamp_nanos(-1_234_567_890)),
        Value::try_json(r#"{"key":[1,true]}"#).unwrap(),
        Value::vector(vec![1.25, -2.5, 3.75]),
        Value::uuid(raw(0x71)),
        Value::try_decimal(-123_456, 12, 3).unwrap(),
        Value::date(-19_000),
        Value::bytes(vec![0, 1, 2, 0xff]),
    ];
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x72)).unwrap(),
        DatabaseId::from_bytes(raw(0x73)).unwrap(),
        column_id(0x74),
        SegmentId::from_bytes(raw(0x75)).unwrap(),
        DatabaseGeneration::new(23).unwrap(),
        CatalogGeneration::new(21).unwrap(),
        201,
        201,
        1,
        columns.len() as u32,
        1,
        SegmentKind::Rows,
        456_789,
    )
    .unwrap();
    let mut blocks = vec![DataBlockSpec::row_ids(0, &[0], DataPhysicalCodec::None).unwrap()];
    for (ordinal, (column, value)) in columns.iter().copied().zip(&values).enumerate() {
        blocks.push(
            DataBlockSpec::column(
                0,
                ordinal as u32,
                column,
                std::slice::from_ref(value),
                DataValueEncoding::Plain,
                DataPhysicalCodec::None,
            )
            .unwrap(),
        );
    }
    let input = DataArtifactInput::new(header, columns.clone(), vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    (bytes, reference, layout, columns, values)
}

#[test]
fn zero_entry_exact_page_is_canonical_only_for_nullable_unique_keys() {
    let (_, _, data, columns) = data_fixture(1);
    let (bytes, reference) = exact_container(
        &data,
        vec![key_column(columns[2])],
        Vec::new(),
        true,
        ExactPageBuildLimits::default(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    let accelerator = &layout.accelerators()[0];
    assert_eq!(accelerator.indexed_item_count(), 0);
    let section = layout.sections()[accelerator.first_section_index() as usize + 1];
    assert_eq!(section.page_count(), 1);
    assert_eq!(section.reference().item_count(), 0);
    let page = layout.pages()[section.first_page_index() as usize];
    assert_eq!(page.item_count(), 0);
    assert!(decode_exact_index_page(
        &read_index_page(&bytes, &layout, section.first_page_index() as usize).unwrap(),
        page,
        &data,
        accelerator,
    )
    .unwrap()
    .entries()
    .is_empty());
    assert_eq!(
        lookup_exact_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            &[Value::float(1.0)],
        )
        .unwrap(),
        None
    );

    let pages = encode_exact_index_pages(
        Vec::new(),
        true,
        data.header().row_count(),
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    let invalid = IndexAcceleratorSpec::new(
        column_id(0x54),
        IndexAcceleratorKind::Exact,
        true,
        true,
        [0x55; 32],
        vec![key_column(columns[1])],
        0,
        vec![IndexSectionSpec::pages(IndexSectionKind::ExactPages, pages).unwrap()],
    )
    .unwrap();
    let header = IndexArtifactHeader::for_data(
        ArtifactId::from_bytes(raw(0x56)).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(15).unwrap(),
        &data,
    )
    .unwrap();
    let error = IndexArtifactInput::new(header, &data, vec![invalid]).unwrap_err();
    assert!(error
        .to_string()
        .contains("has no nullable source key column"));
}

#[test]
fn exact_postings_roundtrip_across_bounded_lz4_pages_and_lookup() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(1_024);
    let key_columns = vec![key_column(columns[0]), key_column(columns[1])];
    let first = ExactIndexKey::from_values(
        &data,
        &key_columns,
        &[Value::integer(1), Value::text("alpha")],
    )
    .unwrap();
    let second = ExactIndexKey::from_values(
        &data,
        &key_columns,
        &[Value::integer(2), Value::text("beta")],
    )
    .unwrap();
    let third = ExactIndexKey::from_values(
        &data,
        &key_columns,
        &[Value::integer(3), Value::text("gamma")],
    )
    .unwrap();
    let entries = vec![
        ExactIndexEntry::new(third.clone(), vec![2]).unwrap(),
        ExactIndexEntry::new(first.clone(), vec![0, 3, 130, 1_000]).unwrap(),
        ExactIndexEntry::new(second.clone(), vec![1]).unwrap(),
    ];
    let limits = ExactPageBuildLimits::new(2, 1024 * 1024).unwrap();
    let (bytes, reference) =
        exact_container(&data, key_columns.clone(), entries.clone(), false, limits);
    let (second_bytes, second_reference) =
        exact_container(&data, key_columns, entries, false, limits);
    assert_eq!(bytes, second_bytes);
    assert_eq!(reference, second_reference);
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();

    assert_eq!(layout.accelerators().len(), 1);
    assert_eq!(layout.pages().len(), 2);
    let accelerator = &layout.accelerators()[0];
    let decoded = layout
        .pages()
        .iter()
        .copied()
        .map(|page| {
            let logical = read_index_page(&bytes, &layout, page.page_ordinal() as usize).unwrap();
            decode_exact_index_page(&logical, page, &data, accelerator).unwrap()
        })
        .collect::<Vec<_>>();

    assert_eq!(decoded[0].lookup(&first), Some(&[0, 3, 130, 1_000][..]));
    assert_eq!(decoded[0].lookup(&second), Some(&[1][..]));
    assert_eq!(decoded[1].lookup(&third), Some(&[2][..]));
    assert_eq!(decoded[1].lookup(&first), None);
    assert_eq!(
        lookup_exact_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            &[Value::integer(3), Value::text("gamma")],
        )
        .unwrap(),
        Some(vec![2])
    );
    assert_eq!(
        lookup_exact_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            &[Value::integer(9), Value::text("missing")],
        )
        .unwrap(),
        None
    );
}

#[test]
fn exact_hot_posting_continues_across_pages_without_losing_rows() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(512);
    let key_columns = vec![key_column(columns[0])];
    let key = ExactIndexKey::from_values(&data, &key_columns, &[Value::integer(7)]).unwrap();
    let rows = (0_u64..512).collect::<Vec<_>>();
    let limits = ExactPageBuildLimits::new(1, 128).unwrap();
    let (bytes, reference) = exact_container(
        &data,
        key_columns,
        vec![ExactIndexEntry::new(key.clone(), rows.clone()).unwrap()],
        false,
        limits,
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    assert!(layout.pages().len() > 2);
    let accelerator = &layout.accelerators()[0];
    let decoded = layout
        .pages()
        .iter()
        .copied()
        .map(|page| {
            let logical = read_index_page(&bytes, &layout, page.page_ordinal() as usize).unwrap();
            decode_exact_index_page(&logical, page, &data, accelerator).unwrap()
        })
        .collect::<Vec<_>>();
    assert!(!decoded[0].entries()[0].has_previous_fragment());
    assert!(decoded[0].entries()[0].has_next_fragment());
    assert!(decoded[1].entries()[0].has_previous_fragment());
    assert!(decoded[1].entries()[0].has_next_fragment());
    let last = decoded.last().unwrap().entries().last().unwrap();
    assert!(last.has_previous_fragment());
    assert!(!last.has_next_fragment());

    for _ in 0..2 {
        assert_eq!(
            lookup_exact_index(
                &bytes,
                &layout,
                &data,
                column_id(0x51),
                &[Value::integer(7)],
            )
            .unwrap(),
            Some(rows.clone())
        );
    }
}

#[test]
fn exact_lookup_rejects_rechecksummed_broken_continuation_flags() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(512);
    let key_columns = vec![key_column(columns[0])];
    let key = ExactIndexKey::from_values(&data, &key_columns, &[Value::integer(7)]).unwrap();
    let rows = (0_u64..512).collect::<Vec<_>>();
    let mut pages = encode_exact_index_pages(
        vec![ExactIndexEntry::new(key, rows.clone()).unwrap()],
        false,
        data.header().row_count(),
        IndexPageCodec::None,
        ExactPageBuildLimits::new(1, 128).unwrap(),
    )
    .unwrap();
    let original = &pages[1];
    let mut logical = original.logical_bytes().to_vec();
    let flags = read_u32(&logical, 40 + 20);
    let previous_fragment = 1_u32 << 1;
    assert_ne!(flags & previous_fragment, 0);
    put_u32(&mut logical, 40 + 20, flags & !previous_fragment);
    refresh_body_crc(&mut logical);
    pages[1] = IndexPageSpec::new(
        logical,
        IndexPageCodec::None,
        original.item_count(),
        original.minimum_key_hash(),
        original.maximum_key_hash(),
    )
    .unwrap();
    let (bytes, reference) =
        exact_container_from_pages(&data, key_columns, pages, false, rows.len() as u64);
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    assert!(matches!(
        lookup_exact_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            &[Value::integer(7)],
        ),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));
}

#[test]
fn exact_key_normalizes_float_aliases_and_tracks_null_components() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(8);
    let float_key = vec![key_column(columns[2])];
    let positive_zero =
        ExactIndexKey::from_values(&data, &float_key, &[Value::float(0.0)]).unwrap();
    let negative_zero =
        ExactIndexKey::from_values(&data, &float_key, &[Value::float(-0.0)]).unwrap();
    let canonical_nan =
        ExactIndexKey::from_values(&data, &float_key, &[Value::float(f64::NAN)]).unwrap();
    let alternate_nan = ExactIndexKey::from_values(
        &data,
        &float_key,
        &[Value::float(f64::from_bits(0x7ff8_0000_0000_0042))],
    )
    .unwrap();
    let null =
        ExactIndexKey::from_values(&data, &float_key, &[Value::null(DataType::Float)]).unwrap();

    assert_eq!(positive_zero, negative_zero);
    assert_eq!(canonical_nan, alternate_nan);
    assert!(!positive_zero.has_null_component());
    assert!(null.has_null_component());

    let duplicate_aliases = vec![
        ExactIndexEntry::new(positive_zero.clone(), vec![0]).unwrap(),
        ExactIndexEntry::new(negative_zero, vec![1]).unwrap(),
    ];
    assert!(matches!(
        encode_exact_index_pages(
            duplicate_aliases,
            false,
            data.header().row_count(),
            IndexPageCodec::None,
            ExactPageBuildLimits::default()
        ),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let (bytes, reference) = exact_container(
        &data,
        float_key,
        vec![ExactIndexEntry::new(positive_zero, vec![0]).unwrap()],
        false,
        ExactPageBuildLimits::default(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    let page = layout.pages()[0];
    let mut noncanonical_zero = read_index_page(&bytes, &layout, 0).unwrap();
    let key_start = 40 + read_u32(&noncanonical_zero, 12) as usize;
    noncanonical_zero[key_start + 15] = 0x80;
    refresh_key_and_body_crc(&mut noncanonical_zero);
    assert!(matches!(
        decode_exact_index_page(&noncanonical_zero, page, &data, &layout.accelerators()[0],),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));
}

#[test]
fn exact_key_and_page_cover_every_public_stored_value_type() {
    let (_data_bytes, _data_reference, data, columns, values) = all_types_fixture();
    let key_columns = columns.iter().copied().map(key_column).collect::<Vec<_>>();
    let key = ExactIndexKey::from_values(&data, &key_columns, &values).unwrap();
    let (bytes, reference) = exact_container(
        &data,
        key_columns,
        vec![ExactIndexEntry::new(key.clone(), vec![0]).unwrap()],
        true,
        ExactPageBuildLimits::default(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    let page = layout.pages()[0];
    let logical = read_index_page(&bytes, &layout, 0).unwrap();
    let decoded =
        decode_exact_index_page(&logical, page, &data, &layout.accelerators()[0]).unwrap();

    assert_eq!(decoded.lookup(&key), Some(&[0][..]));
    assert_eq!(
        lookup_exact_index(&bytes, &layout, &data, column_id(0x51), &values).unwrap(),
        Some(vec![0])
    );
}

#[test]
fn exact_lookup_reads_logarithmically_selected_pages_instead_of_scanning_pack() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(32);
    let key_columns = vec![key_column(columns[0])];
    let entries = (0..17_i64)
        .map(|value| {
            let key =
                ExactIndexKey::from_values(&data, &key_columns, &[Value::integer(value)]).unwrap();
            ExactIndexEntry::new(key, vec![value as u64]).unwrap()
        })
        .collect::<Vec<_>>();
    let (bytes, reference) = exact_container(
        &data,
        key_columns,
        entries,
        false,
        ExactPageBuildLimits::new(1, 1024 * 1024).unwrap(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    let source = RangeTracingSource {
        bytes: &bytes,
        reads: RefCell::new(Vec::new()),
    };

    assert_eq!(
        lookup_exact_index_from_source(
            &source,
            &layout,
            &data,
            column_id(0x51),
            &[Value::integer(15)],
        )
        .unwrap(),
        Some(vec![15])
    );
    let reads = source.reads.borrow();
    assert!(reads.len() <= 5);
    assert!(reads.len() < layout.pages().len());
    let mut distinct = reads.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        reads.len(),
        distinct.len(),
        "routing must hand the selected page to lookup without reading it twice"
    );
}

#[test]
fn exact_writer_rejects_noncanonical_postings_unique_violation_and_page_overflow() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(256);
    let keys = vec![key_column(columns[0])];
    let key = ExactIndexKey::from_values(&data, &keys, &[Value::integer(7)]).unwrap();

    assert!(ExactIndexEntry::new(key.clone(), vec![]).is_err());
    assert!(ExactIndexEntry::new(key.clone(), vec![2, 2]).is_err());
    assert!(ExactIndexEntry::new(key.clone(), vec![2, 1]).is_err());
    assert!(encode_exact_index_pages(
        vec![ExactIndexEntry::new(key.clone(), vec![1, 2]).unwrap()],
        true,
        data.header().row_count(),
        IndexPageCodec::None,
        ExactPageBuildLimits::default(),
    )
    .is_err());
    assert!(encode_exact_index_pages(
        vec![ExactIndexEntry::new(key.clone(), vec![256]).unwrap()],
        false,
        data.header().row_count(),
        IndexPageCodec::None,
        ExactPageBuildLimits::default(),
    )
    .is_err());
    let too_small = ExactPageBuildLimits::new(1, 40).unwrap();
    assert!(matches!(
        encode_exact_index_pages(
            vec![ExactIndexEntry::new(key, vec![1]).unwrap()],
            false,
            data.header().row_count(),
            IndexPageCodec::None,
            too_small,
        ),
        Err(FormatError::IndexArtifactLimitExceeded {
            field: "exact page logical bytes",
            ..
        })
    ));
}

#[test]
fn exact_decoder_rejects_nonminimal_and_out_of_range_postings() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(1_024);
    let keys = vec![key_column(columns[0])];
    let key = ExactIndexKey::from_values(&data, &keys, &[Value::integer(7)]).unwrap();
    let entries = vec![ExactIndexEntry::new(key, vec![128]).unwrap()];
    let (bytes, reference) =
        exact_container(&data, keys, entries, false, ExactPageBuildLimits::default());
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    let page = layout.pages()[0];
    let logical = read_index_page(&bytes, &layout, 0).unwrap();

    let mut bad_body = logical.clone();
    bad_body[40] ^= 1;
    assert!(matches!(
        decode_exact_index_page(&bad_body, page, &data, &layout.accelerators()[0]),
        Err(FormatError::IndexArtifactChecksumMismatch {
            scope: "exact page body"
        })
    ));

    let posting_start = posting_start(&logical);
    assert_eq!(&logical[posting_start..posting_start + 2], &[0x80, 0x01]);
    let mut impossible_count = logical.clone();
    put_u32(&mut impossible_count, 40 + 16, 3);
    let body_crc = radixdb_core::crc32_ieee(&impossible_count[40..]);
    put_u32(&mut impossible_count, 32, body_crc);
    assert!(matches!(
        decode_exact_index_page(&impossible_count, page, &data, &layout.accelerators()[0]),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let mut nonminimal = logical.clone();
    nonminimal[posting_start..posting_start + 2].copy_from_slice(&[0x80, 0x00]);
    refresh_entry_and_body_crc(&mut nonminimal);
    assert!(matches!(
        decode_exact_index_page(&nonminimal, page, &data, &layout.accelerators()[0]),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let mut outside = logical;
    outside[posting_start..posting_start + 2].copy_from_slice(&[0x80, 0x08]);
    refresh_entry_and_body_crc(&mut outside);
    assert!(matches!(
        decode_exact_index_page(&outside, page, &data, &layout.accelerators()[0]),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));
}

fn posting_start(page: &[u8]) -> usize {
    40 + read_u32(page, 12) as usize + read_u64(page, 16) as usize
}

fn refresh_entry_and_body_crc(page: &mut [u8]) {
    let posting_start = posting_start(page);
    let posting_length = read_u32(page, 40 + 12) as usize;
    let posting_crc =
        radixdb_core::crc32_ieee(&page[posting_start..posting_start + posting_length]);
    put_u32(page, 40 + 28, posting_crc);
    let body_crc = radixdb_core::crc32_ieee(&page[40..]);
    put_u32(page, 32, body_crc);
}

fn refresh_key_and_body_crc(page: &mut [u8]) {
    let key_start = 40 + read_u32(page, 12) as usize;
    let key_length = read_u32(page, 40 + 4) as usize;
    let key_crc = radixdb_core::crc32_ieee(&page[key_start..key_start + key_length]);
    put_u32(page, 40 + 24, key_crc);
    let body_crc = radixdb_core::crc32_ieee(&page[40..]);
    put_u32(page, 32, body_crc);
}

fn refresh_body_crc(page: &mut [u8]) {
    let body_crc = radixdb_core::crc32_ieee(&page[40..]);
    put_u32(page, 32, body_crc);
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
