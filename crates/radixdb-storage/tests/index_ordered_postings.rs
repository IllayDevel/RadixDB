use std::cell::RefCell;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    decode_data_artifact_layout, decode_index_artifact_layout, decode_ordered_index_page,
    encode_data_artifact, encode_index_artifact, encode_ordered_index_pages, read_index_page,
    scan_ordered_index, scan_ordered_index_from_source, ArtifactId, ArtifactRef, ArtifactSource,
    CatalogGeneration, DataArtifactHeader, DataArtifactInput, DataArtifactLayout, DataBlockSpec,
    DataColumnSpec, DataPhysicalCodec, DataValueEncoding, DatabaseGeneration, DatabaseId,
    FormatError, FormatResult, IndexAcceleratorKind, IndexAcceleratorSpec, IndexArtifactHeader,
    IndexArtifactInput, IndexKeyColumn, IndexNullsOrder, IndexPageCodec, IndexPageSpec,
    IndexScanDirection, IndexSectionKind, IndexSectionSpec, IndexSortDirection, OrderedIndexBound,
    OrderedIndexEntry, OrderedIndexKey, OrderedPageBuildLimits, SegmentId, SegmentKind,
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
            true,
        ),
        DataColumnSpec::new(
            column_id(0x32),
            CatalogDataType::scalar(DataType::Text).unwrap(),
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
    ];
    let input = DataArtifactInput::new(header, columns.clone(), vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    (bytes, reference, layout, columns)
}

fn ordered_types_fixture() -> (DataArtifactLayout, Vec<DataColumnSpec>, Vec<(Value, Value)>) {
    let types = vec![
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        CatalogDataType::scalar(DataType::Float).unwrap(),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        CatalogDataType::scalar(DataType::Boolean).unwrap(),
        CatalogDataType::scalar(DataType::Timestamp).unwrap(),
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
    let pairs = vec![
        (Value::integer(-1), Value::integer(1)),
        (Value::float(-0.0), Value::float(f64::NAN)),
        (Value::text("alpha"), Value::text("omega")),
        (Value::boolean(false), Value::boolean(true)),
        (
            Value::timestamp(chrono::DateTime::from_timestamp_nanos(-1)),
            Value::timestamp(chrono::DateTime::from_timestamp_nanos(1)),
        ),
        (Value::uuid(raw(0x10)), Value::uuid(raw(0x20))),
        (
            Value::try_decimal(-1_000, 12, 3).unwrap(),
            Value::try_decimal(1_000, 12, 3).unwrap(),
        ),
        (Value::date(-1), Value::date(1)),
        (Value::bytes(vec![0, 1]), Value::bytes(vec![0, 2])),
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
        2,
        columns.len() as u32,
        1,
        SegmentKind::Rows,
        456_789,
    )
    .unwrap();
    let mut blocks = vec![DataBlockSpec::row_ids(0, &[0, 1], DataPhysicalCodec::None).unwrap()];
    for (ordinal, (column, (low, high))) in columns.iter().copied().zip(pairs.iter()).enumerate() {
        blocks.push(
            DataBlockSpec::column(
                0,
                ordinal as u32,
                column,
                &[low.clone(), high.clone()],
                DataValueEncoding::Plain,
                DataPhysicalCodec::None,
            )
            .unwrap(),
        );
    }
    let input = DataArtifactInput::new(header, columns.clone(), vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let layout = decode_data_artifact_layout(&bytes, reference).unwrap();
    (layout, columns, pairs)
}

fn key_column(
    column: DataColumnSpec,
    direction: IndexSortDirection,
    nulls: IndexNullsOrder,
) -> IndexKeyColumn {
    IndexKeyColumn::new(
        column.column_id(),
        column.data_type().logical_type(),
        direction,
        nulls,
    )
}

fn ordered_container(
    data: &DataArtifactLayout,
    key_columns: Vec<IndexKeyColumn>,
    entries: Vec<OrderedIndexEntry>,
    unique: bool,
    limits: OrderedPageBuildLimits,
) -> (Vec<u8>, ArtifactRef) {
    let indexed_item_count = entries
        .iter()
        .map(|entry| entry.row_ordinals().len() as u64)
        .sum();
    let pages = encode_ordered_index_pages(
        entries,
        &key_columns,
        unique,
        data.header().row_count(),
        IndexPageCodec::Lz4,
        limits,
    )
    .unwrap();
    ordered_container_from_pages(data, key_columns, pages, unique, indexed_item_count)
}

fn ordered_container_from_pages(
    data: &DataArtifactLayout,
    key_columns: Vec<IndexKeyColumn>,
    pages: Vec<IndexPageSpec>,
    unique: bool,
    indexed_item_count: u64,
) -> (Vec<u8>, ArtifactRef) {
    let accelerator = IndexAcceleratorSpec::new(
        column_id(0x51),
        IndexAcceleratorKind::Ordered,
        unique,
        unique,
        [0x52; 32],
        key_columns,
        indexed_item_count,
        vec![IndexSectionSpec::pages(IndexSectionKind::OrderedPages, pages).unwrap()],
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

fn ordered_key(
    data: &DataArtifactLayout,
    columns: &[IndexKeyColumn],
    integer: Option<i64>,
    text: &str,
) -> OrderedIndexKey {
    OrderedIndexKey::from_values(
        data,
        columns,
        &[
            integer.map_or_else(|| Value::null(DataType::Integer), Value::integer),
            Value::text(text),
        ],
    )
    .unwrap()
}

#[test]
fn dense_integer_pages_remove_row_scaled_pk_descriptors() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(4096);
    let key_columns = vec![key_column(
        columns[0],
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )];
    let entries = (0_u64..4096)
        .map(|row| {
            let key = OrderedIndexKey::from_values(
                &data,
                &key_columns,
                &[Value::integer(row as i64 + 1)],
            )
            .unwrap();
            OrderedIndexEntry::new(key, vec![row]).unwrap()
        })
        .collect::<Vec<_>>();
    let (bytes, reference) = ordered_container(
        &data,
        key_columns.clone(),
        entries,
        true,
        OrderedPageBuildLimits::default(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    assert_eq!(layout.pages().len(), 1);
    assert!(layout.pages()[0].stored_length() < 128);

    let lower = OrderedIndexBound::new(
        OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(1024)]).unwrap(),
        true,
    );
    let upper = OrderedIndexBound::new(
        OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(1031)]).unwrap(),
        true,
    );
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            Some(&lower),
            Some(&upper),
            IndexScanDirection::Forward,
            0,
            32,
        )
        .unwrap(),
        (1023_u64..1031).collect::<Vec<_>>()
    );

    let page = layout.pages()[0];
    let mut logical = read_index_page(&bytes, &layout, 0).unwrap();
    assert_eq!(&logical[..4], b"IXD1");
    logical[40] ^= 1;
    assert!(matches!(
        decode_ordered_index_page(&logical, page, &data, &layout.accelerators()[0]),
        Err(FormatError::IndexArtifactChecksumMismatch {
            scope: "dense integer page body"
        })
    ));
}

#[test]
fn ordered_pages_preserve_composite_direction_nulls_ranges_and_row_order() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(16);
    let key_columns = vec![
        key_column(
            columns[0],
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        ),
        key_column(
            columns[1],
            IndexSortDirection::Descending,
            IndexNullsOrder::First,
        ),
    ];
    let first = ordered_key(&data, &key_columns, Some(1), "z");
    let second = ordered_key(&data, &key_columns, Some(1), "a");
    let third = ordered_key(&data, &key_columns, Some(2), "m");
    let last = ordered_key(&data, &key_columns, None, "q");
    let null_text = OrderedIndexKey::from_values(
        &data,
        &key_columns,
        &[Value::integer(1), Value::null(DataType::Text)],
    )
    .unwrap();
    let entries = vec![
        OrderedIndexEntry::new(last.clone(), vec![4]).unwrap(),
        OrderedIndexEntry::new(third.clone(), vec![3]).unwrap(),
        OrderedIndexEntry::new(first.clone(), vec![1, 5]).unwrap(),
        OrderedIndexEntry::new(second.clone(), vec![2]).unwrap(),
        OrderedIndexEntry::new(null_text, vec![6]).unwrap(),
    ];
    let limits = OrderedPageBuildLimits::new(2, 1024 * 1024).unwrap();
    let (bytes, reference) =
        ordered_container(&data, key_columns.clone(), entries.clone(), false, limits);
    let (second_bytes, second_reference) =
        ordered_container(&data, key_columns, entries, false, limits);
    assert_eq!(bytes, second_bytes);
    assert_eq!(reference, second_reference);
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    assert_eq!(layout.pages().len(), 3);

    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            None,
            None,
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap(),
        vec![6, 1, 5, 2, 3, 4]
    );
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            None,
            None,
            IndexScanDirection::Reverse,
            0,
            usize::MAX,
        )
        .unwrap(),
        vec![4, 3, 2, 5, 1, 6]
    );

    let lower = OrderedIndexBound::new(second.clone(), true);
    let upper = OrderedIndexBound::new(last.clone(), false);
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            Some(&lower),
            Some(&upper),
            IndexScanDirection::Forward,
            0,
            10,
        )
        .unwrap(),
        vec![2, 3]
    );
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            Some(&lower),
            Some(&upper),
            IndexScanDirection::Reverse,
            0,
            10,
        )
        .unwrap(),
        vec![3, 2]
    );
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            None,
            None,
            IndexScanDirection::Forward,
            3,
            2,
        )
        .unwrap(),
        vec![2, 3]
    );

    let equal_exclusive = OrderedIndexBound::new(third, false);
    assert!(scan_ordered_index(
        &bytes,
        &layout,
        &data,
        column_id(0x51),
        Some(&equal_exclusive),
        Some(&equal_exclusive),
        IndexScanDirection::Forward,
        0,
        10,
    )
    .unwrap()
    .is_empty());
    let reversed_lower = OrderedIndexBound::new(last, true);
    let reversed_upper = OrderedIndexBound::new(first, true);
    assert!(scan_ordered_index(
        &bytes,
        &layout,
        &data,
        column_id(0x51),
        Some(&reversed_lower),
        Some(&reversed_upper),
        IndexScanDirection::Forward,
        0,
        10,
    )
    .unwrap()
    .is_empty());
}

#[test]
fn ordered_hot_posting_continues_across_pages_in_both_directions() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(512);
    let key_columns = vec![key_column(
        columns[0],
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )];
    let key = OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(7)]).unwrap();
    let rows = (0_u64..512).collect::<Vec<_>>();
    let limits = OrderedPageBuildLimits::new(1, 128).unwrap();
    let (bytes, reference) = ordered_container(
        &data,
        key_columns,
        vec![OrderedIndexEntry::new(key.clone(), rows.clone()).unwrap()],
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
            decode_ordered_index_page(&logical, page, &data, accelerator).unwrap()
        })
        .collect::<Vec<_>>();
    assert!(!decoded[0].entries()[0].has_previous_fragment());
    assert!(decoded[0].entries()[0].has_next_fragment());
    assert!(decoded[1].entries()[0].has_previous_fragment());
    assert!(decoded[1].entries()[0].has_next_fragment());
    let last = decoded.last().unwrap().entries().last().unwrap();
    assert!(last.has_previous_fragment());
    assert!(!last.has_next_fragment());

    let bound = OrderedIndexBound::new(key, true);
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            Some(&bound),
            Some(&bound),
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap(),
        rows
    );
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            Some(&bound),
            Some(&bound),
            IndexScanDirection::Reverse,
            3,
            5,
        )
        .unwrap(),
        vec![508, 507, 506, 505, 504]
    );
}

#[test]
fn ordered_scan_rejects_rechecksummed_broken_continuation_flags() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(512);
    let key_columns = vec![key_column(
        columns[0],
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )];
    let key = OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(7)]).unwrap();
    let rows = (0_u64..512).collect::<Vec<_>>();
    let make_pages = || {
        encode_ordered_index_pages(
            vec![OrderedIndexEntry::new(key.clone(), rows.clone()).unwrap()],
            &key_columns,
            false,
            data.header().row_count(),
            IndexPageCodec::None,
            OrderedPageBuildLimits::new(1, 128).unwrap(),
        )
        .unwrap()
    };
    for (page_index, cleared_flag) in [(0_usize, 1_u32 << 2), (1, 1_u32 << 1)] {
        let mut pages = make_pages();
        let original = &pages[page_index];
        let mut logical = original.logical_bytes().to_vec();
        let flags = read_u32(&logical, 40 + 20);
        assert_ne!(flags & cleared_flag, 0);
        put_u32(&mut logical, 40 + 20, flags & !cleared_flag);
        refresh_body_crc(&mut logical);
        pages[page_index] = IndexPageSpec::new(
            logical,
            IndexPageCodec::None,
            original.item_count(),
            original.minimum_key_hash(),
            original.maximum_key_hash(),
        )
        .unwrap();
        let (bytes, reference) = ordered_container_from_pages(
            &data,
            key_columns.clone(),
            pages,
            false,
            rows.len() as u64,
        );
        let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
        let bound = OrderedIndexBound::new(key.clone(), true);
        assert!(matches!(
            scan_ordered_index(
                &bytes,
                &layout,
                &data,
                column_id(0x51),
                Some(&bound),
                Some(&bound),
                IndexScanDirection::Forward,
                0,
                usize::MAX,
            ),
            Err(FormatError::InvalidIndexArtifact { .. })
        ));
    }
}

#[test]
fn ordered_pages_route_leading_prefix_bounds_across_page_boundaries() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(16);
    let key_columns = vec![
        key_column(
            columns[0],
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        ),
        key_column(
            columns[1],
            IndexSortDirection::Descending,
            IndexNullsOrder::First,
        ),
    ];
    let entries = vec![
        OrderedIndexEntry::new(
            OrderedIndexKey::from_values(
                &data,
                &key_columns,
                &[Value::integer(1), Value::null(DataType::Text)],
            )
            .unwrap(),
            vec![6],
        )
        .unwrap(),
        OrderedIndexEntry::new(ordered_key(&data, &key_columns, Some(1), "z"), vec![1, 5]).unwrap(),
        OrderedIndexEntry::new(ordered_key(&data, &key_columns, Some(1), "a"), vec![2]).unwrap(),
        OrderedIndexEntry::new(ordered_key(&data, &key_columns, Some(2), "m"), vec![3]).unwrap(),
        OrderedIndexEntry::new(ordered_key(&data, &key_columns, None, "q"), vec![4]).unwrap(),
    ];
    let (bytes, reference) = ordered_container(
        &data,
        key_columns.clone(),
        entries,
        false,
        OrderedPageBuildLimits::new(2, 1024 * 1024).unwrap(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    assert_eq!(layout.pages().len(), 3);

    let prefix =
        OrderedIndexKey::from_values(&data, &key_columns[..1], &[Value::integer(1)]).unwrap();
    let inclusive = OrderedIndexBound::new(prefix.clone(), true);
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            Some(&inclusive),
            Some(&inclusive),
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap(),
        vec![6, 1, 5, 2]
    );
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            Some(&inclusive),
            Some(&inclusive),
            IndexScanDirection::Reverse,
            0,
            usize::MAX,
        )
        .unwrap(),
        vec![2, 5, 1, 6]
    );

    let exclusive = OrderedIndexBound::new(prefix, false);
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            Some(&exclusive),
            None,
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap(),
        vec![3, 4]
    );
}

#[test]
fn ordered_lookup_reads_logarithmic_fences_then_only_the_selected_range() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(64);
    let key_columns = vec![key_column(
        columns[0],
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )];
    let entries = (0..33_i64)
        .map(|value| {
            let key = OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(value)])
                .unwrap();
            OrderedIndexEntry::new(key, vec![value as u64]).unwrap()
        })
        .collect::<Vec<_>>();
    let (bytes, reference) = ordered_container(
        &data,
        key_columns.clone(),
        entries,
        false,
        OrderedPageBuildLimits::new(1, 1024 * 1024).unwrap(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    let key = OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(30)]).unwrap();
    let bound = OrderedIndexBound::new(key, true);
    let source = RangeTracingSource {
        bytes: &bytes,
        reads: RefCell::new(Vec::new()),
    };

    assert_eq!(
        scan_ordered_index_from_source(
            &source,
            &layout,
            &data,
            column_id(0x51),
            Some(&bound),
            Some(&bound),
            IndexScanDirection::Forward,
            0,
            1,
        )
        .unwrap(),
        vec![30]
    );
    assert!(source.reads.borrow().len() <= 13);
    assert!(source.reads.borrow().len() < layout.pages().len());
}

#[test]
fn ordered_pages_cover_every_logical_type_with_a_total_btree_order() {
    let (data, columns, pairs) = ordered_types_fixture();
    for (column, (low, high)) in columns.iter().copied().zip(pairs) {
        let key_columns = vec![key_column(
            column,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )];
        let low = OrderedIndexKey::from_values(&data, &key_columns, &[low]).unwrap();
        let high = OrderedIndexKey::from_values(&data, &key_columns, &[high]).unwrap();
        let (bytes, reference) = ordered_container(
            &data,
            key_columns,
            vec![
                OrderedIndexEntry::new(high, vec![1]).unwrap(),
                OrderedIndexEntry::new(low, vec![0]).unwrap(),
            ],
            true,
            OrderedPageBuildLimits::default(),
        );
        let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
        assert_eq!(
            scan_ordered_index(
                &bytes,
                &layout,
                &data,
                column_id(0x51),
                None,
                None,
                IndexScanDirection::Forward,
                0,
                2,
            )
            .unwrap(),
            vec![0, 1]
        );
        assert_eq!(
            scan_ordered_index(
                &bytes,
                &layout,
                &data,
                column_id(0x51),
                None,
                None,
                IndexScanDirection::Reverse,
                0,
                2,
            )
            .unwrap(),
            vec![1, 0]
        );
    }
}

#[test]
fn ordered_writer_rejects_invalid_postings_duplicates_unique_and_unordered_types() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(16);
    let key_columns = vec![key_column(
        columns[0],
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )];
    let key = OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(7)]).unwrap();
    assert!(OrderedIndexEntry::new(key.clone(), vec![]).is_err());
    assert!(OrderedIndexEntry::new(key.clone(), vec![2, 2]).is_err());
    assert!(encode_ordered_index_pages(
        vec![
            OrderedIndexEntry::new(key.clone(), vec![1]).unwrap(),
            OrderedIndexEntry::new(key.clone(), vec![2]).unwrap(),
        ],
        &key_columns,
        false,
        data.header().row_count(),
        IndexPageCodec::None,
        OrderedPageBuildLimits::default(),
    )
    .is_err());
    assert!(encode_ordered_index_pages(
        vec![OrderedIndexEntry::new(key.clone(), vec![1, 2]).unwrap()],
        &key_columns,
        true,
        data.header().row_count(),
        IndexPageCodec::None,
        OrderedPageBuildLimits::default(),
    )
    .is_err());

    let null_key =
        OrderedIndexKey::from_values(&data, &key_columns, &[Value::null(DataType::Integer)])
            .unwrap();
    let (nullable_unique, reference) = ordered_container(
        &data,
        key_columns.clone(),
        vec![OrderedIndexEntry::new(null_key, vec![1, 2]).unwrap()],
        true,
        OrderedPageBuildLimits::default(),
    );
    let nullable_unique_layout =
        decode_index_artifact_layout(&nullable_unique, reference, &data).unwrap();
    assert_eq!(
        scan_ordered_index(
            &nullable_unique,
            &nullable_unique_layout,
            &data,
            column_id(0x51),
            None,
            None,
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap(),
        vec![1, 2]
    );
    assert!(encode_ordered_index_pages(
        vec![OrderedIndexEntry::new(key.clone(), vec![16]).unwrap()],
        &key_columns,
        false,
        data.header().row_count(),
        IndexPageCodec::None,
        OrderedPageBuildLimits::default(),
    )
    .is_err());

    for unordered_type in [DataType::Json, DataType::Vector] {
        let unordered_column = IndexKeyColumn::new(
            columns[0].column_id(),
            unordered_type,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        );
        assert!(matches!(
            encode_ordered_index_pages(
                vec![OrderedIndexEntry::new(key.clone(), vec![1]).unwrap()],
                &[unordered_column],
                false,
                data.header().row_count(),
                IndexPageCodec::None,
                OrderedPageBuildLimits::default(),
            ),
            Err(FormatError::InvalidIndexArtifact { .. })
        ));
    }
}

#[test]
fn ordered_decoder_rejects_corrupt_noncanonical_and_unsorted_pages() {
    let (_data_bytes, _data_reference, data, columns) = data_fixture(256);
    let key_columns = vec![
        key_column(
            columns[0],
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        ),
        key_column(
            columns[1],
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        ),
    ];
    let first =
        OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(1), Value::text("a")])
            .unwrap();
    let second =
        OrderedIndexKey::from_values(&data, &key_columns, &[Value::integer(2), Value::text("b")])
            .unwrap();
    let (bytes, reference) = ordered_container(
        &data,
        key_columns,
        vec![
            OrderedIndexEntry::new(first, vec![128]).unwrap(),
            OrderedIndexEntry::new(second, vec![129]).unwrap(),
        ],
        false,
        OrderedPageBuildLimits::default(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    let page = layout.pages()[0];
    let logical = read_index_page(&bytes, &layout, 0).unwrap();

    let mut bad_body = logical.clone();
    bad_body[40] ^= 1;
    assert!(matches!(
        decode_ordered_index_page(&bad_body, page, &data, &layout.accelerators()[0]),
        Err(FormatError::IndexArtifactChecksumMismatch {
            scope: "ordered page body"
        })
    ));

    let key_area = 40 + read_u32(&logical, 12) as usize;
    let first_length = read_u32(&logical, 40 + 4) as usize;
    let second_length = read_u32(&logical, 72 + 4) as usize;
    assert_eq!(first_length, second_length);
    let mut unsorted = logical.clone();
    let (left, right) =
        unsorted[key_area..key_area + first_length + second_length].split_at_mut(first_length);
    left.swap_with_slice(right);
    refresh_key_crcs_and_body(&mut unsorted);
    assert!(matches!(
        decode_ordered_index_page(&unsorted, page, &data, &layout.accelerators()[0]),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let posting_start = posting_start(&logical);
    let mut impossible_count = logical.clone();
    put_u32(&mut impossible_count, 40 + 16, u32::MAX);
    refresh_body_crc(&mut impossible_count);
    assert!(matches!(
        decode_ordered_index_page(&impossible_count, page, &data, &layout.accelerators()[0],),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let mut outside = logical.clone();
    outside[posting_start..posting_start + 2].copy_from_slice(&[0x80, 0x02]);
    refresh_posting_crcs_and_body(&mut outside);
    assert!(matches!(
        decode_ordered_index_page(&outside, page, &data, &layout.accelerators()[0]),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));

    let mut nonminimal = logical;
    assert_eq!(&nonminimal[posting_start..posting_start + 2], &[0x80, 0x01]);
    nonminimal[posting_start..posting_start + 2].copy_from_slice(&[0x80, 0x00]);
    refresh_posting_crcs_and_body(&mut nonminimal);
    assert!(matches!(
        decode_ordered_index_page(&nonminimal, page, &data, &layout.accelerators()[0]),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));
}

fn posting_start(page: &[u8]) -> usize {
    40 + read_u32(page, 12) as usize + read_u64(page, 16) as usize
}

fn refresh_key_crcs_and_body(page: &mut [u8]) {
    let key_area = 40 + read_u32(page, 12) as usize;
    for descriptor in [40_usize, 72] {
        let key_offset = read_u32(page, descriptor) as usize;
        let key_length = read_u32(page, descriptor + 4) as usize;
        let crc = radixdb_core::crc32_ieee(
            &page[key_area + key_offset..key_area + key_offset + key_length],
        );
        put_u32(page, descriptor + 24, crc);
    }
    refresh_body_crc(page);
}

fn refresh_posting_crcs_and_body(page: &mut [u8]) {
    let posting_area = posting_start(page);
    for descriptor in [40_usize, 72] {
        let posting_offset = read_u32(page, descriptor + 8) as usize;
        let posting_length = read_u32(page, descriptor + 12) as usize;
        let crc = radixdb_core::crc32_ieee(
            &page[posting_area + posting_offset..posting_area + posting_offset + posting_length],
        );
        put_u32(page, descriptor + 28, crc);
    }
    refresh_body_crc(page);
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
