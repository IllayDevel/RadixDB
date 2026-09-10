use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::*;

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

#[derive(Default)]
struct RebuildCapture(Option<IndexRebuildRequest>);

impl RebuildRequestSink for RebuildCapture {
    fn request_rebuild(&mut self, request: IndexRebuildRequest) -> RebuildRequestStatus {
        self.0 = Some(request);
        RebuildRequestStatus::Enqueued
    }
}

fn request(
    staging: &std::path::Path,
    row_count: u64,
    limits: FanoutBuildLimits,
    ordered: bool,
    unique: bool,
    nullable: bool,
) -> ArtifactPairBuildRequest {
    let column = DataColumnSpec::new(
        object_id(0x21),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        nullable,
    );
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes([0x31; 16]).unwrap(),
        DatabaseId::from_bytes([0x32; 16]).unwrap(),
        object_id(0x33),
        SegmentId::from_bytes([0x34; 16]).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        row_count,
        1,
        row_count.div_ceil(u64::from(limits.row_group_rows())) as u32,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let key = vec![IndexKeyColumn::new(
        column.column_id(),
        DataType::Integer,
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )];
    let accelerator = if ordered {
        AcceleratorBuildSpec::ordered(
            object_id(0x41),
            unique,
            false,
            [0x51; 32],
            key,
            IndexPageCodec::Lz4,
            OrderedPageBuildLimits::default(),
        )
    } else {
        AcceleratorBuildSpec::exact(
            object_id(0x41),
            unique,
            false,
            [0x51; 32],
            key,
            IndexPageCodec::Lz4,
            ExactPageBuildLimits::default(),
        )
    }
    .unwrap();
    ArtifactPairBuildRequest::new(
        header,
        vec![column],
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0x35; 16]).unwrap(),
        vec![accelerator],
        limits,
        staging,
    )
    .unwrap()
}

fn composite_ordered_request(
    staging: &std::path::Path,
    row_count: u64,
) -> ArtifactPairBuildRequest {
    let first = DataColumnSpec::new(
        object_id(0x21),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        true,
    );
    let second = DataColumnSpec::new(
        object_id(0x22),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        true,
    );
    let limits = FanoutBuildLimits::new(2, 2, 1024 * 1024, 4_096).unwrap();
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes([0x31; 16]).unwrap(),
        DatabaseId::from_bytes([0x32; 16]).unwrap(),
        object_id(0x33),
        SegmentId::from_bytes([0x34; 16]).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        row_count,
        2,
        row_count.div_ceil(u64::from(limits.row_group_rows())) as u32,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let accelerator = AcceleratorBuildSpec::ordered(
        object_id(0x41),
        true,
        true,
        [0x51; 32],
        vec![
            IndexKeyColumn::new(
                first.column_id(),
                DataType::Integer,
                IndexSortDirection::Ascending,
                IndexNullsOrder::First,
            ),
            IndexKeyColumn::new(
                second.column_id(),
                DataType::Integer,
                IndexSortDirection::Descending,
                IndexNullsOrder::Last,
            ),
        ],
        IndexPageCodec::Lz4,
        OrderedPageBuildLimits::new(2, 1024 * 1024).unwrap(),
    )
    .unwrap();
    ArtifactPairBuildRequest::new(
        header,
        vec![first, second],
        vec![ColumnBuildPolicy::default(), ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0x35; 16]).unwrap(),
        vec![accelerator],
        limits,
        staging,
    )
    .unwrap()
}

#[test]
fn repeated_non_unique_posting_spills_without_rejecting_rows() {
    let staging = tempfile::tempdir().unwrap();
    let limits = FanoutBuildLimits::new(65_536, 65_536, 1024 * 1024, 4_096).unwrap();
    let row_count = 131_073;
    let request = request(staging.path(), row_count, limits, false, false, false);
    let built = build_artifact_pair(
        &request,
        (0..row_count).map(|row| Ok(SourceRow::new(row + 1, vec![Value::integer(7)]))),
    )
    .unwrap();
    let index = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    assert_eq!(
        lookup_exact_index(
            built.index_bytes(),
            &index,
            built.data_layout(),
            object_id(0x41),
            &[Value::integer(7)],
        )
        .unwrap(),
        Some((0..row_count).collect())
    );
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn nullable_unique_ordered_preserves_null_rows() {
    let staging = tempfile::tempdir().unwrap();
    let request = request(
        staging.path(),
        3,
        FanoutBuildLimits::default(),
        true,
        true,
        true,
    );
    let pair = build_artifact_pair(
        &request,
        [
            Ok(SourceRow::new(1, vec![Value::integer(1)])),
            Ok(SourceRow::new(2, vec![Value::null(DataType::Integer)])),
            Ok(SourceRow::new(3, vec![Value::null(DataType::Integer)])),
        ],
    )
    .unwrap();
    let layout = decode_index_artifact_layout(
        pair.index_bytes(),
        pair.index_reference(),
        pair.data_layout(),
    )
    .unwrap();
    let rows = scan_ordered_index(
        pair.index_bytes(),
        &layout,
        pair.data_layout(),
        object_id(0x41),
        None,
        None,
        IndexScanDirection::Forward,
        0,
        10,
    )
    .unwrap();
    assert_eq!(rows, vec![0, 1, 2], "ordered full scan lost NULL rows");
}

#[test]
fn nullable_unique_ordered_all_null_segment_is_publishable() {
    let staging = tempfile::tempdir().unwrap();
    let request = request(
        staging.path(),
        2,
        FanoutBuildLimits::default(),
        true,
        true,
        true,
    );
    let pair = build_artifact_pair(
        &request,
        (0..2).map(|row| {
            Ok(SourceRow::new(
                row + 1,
                vec![Value::null(DataType::Integer)],
            ))
        }),
    )
    .expect("nullable UNIQUE ORDERED must publish an all-NULL segment");
    let layout = decode_index_artifact_layout(
        pair.index_bytes(),
        pair.index_reference(),
        pair.data_layout(),
    )
    .unwrap();
    let rows = scan_ordered_index(
        pair.index_bytes(),
        &layout,
        pair.data_layout(),
        object_id(0x41),
        None,
        None,
        IndexScanDirection::Forward,
        0,
        10,
    )
    .unwrap();
    assert_eq!(rows, vec![0, 1], "ordered full scan lost all-NULL rows");
}

#[test]
fn nullable_unique_ordered_composite_spill_preserves_ranges_and_directions() {
    let staging = tempfile::tempdir().unwrap();
    let request = composite_ordered_request(staging.path(), 7);
    let null = || Value::null(DataType::Integer);
    let rows = [
        (1, vec![Value::integer(1), Value::integer(10)]),
        (2, vec![null(), Value::integer(7)]),
        (3, vec![null(), Value::integer(7)]),
        (4, vec![Value::integer(2), null()]),
        (5, vec![Value::integer(2), null()]),
        (6, vec![Value::integer(1), Value::integer(20)]),
        (7, vec![null(), Value::integer(9)]),
    ];
    let pair = build_artifact_pair(
        &request,
        rows.into_iter()
            .map(|(row_id, values)| Ok(SourceRow::new(row_id, values))),
    )
    .expect("nullable composite UNIQUE ORDERED must survive spilled runs");
    assert!(
        std::fs::read_dir(staging.path()).unwrap().next().is_none(),
        "completed spill build leaked temporary runs"
    );
    let layout = decode_index_artifact_layout(
        pair.index_bytes(),
        pair.index_reference(),
        pair.data_layout(),
    )
    .unwrap();
    let accelerator = &layout.accelerators()[0];
    let forward = scan_ordered_index(
        pair.index_bytes(),
        &layout,
        pair.data_layout(),
        object_id(0x41),
        None,
        None,
        IndexScanDirection::Forward,
        0,
        usize::MAX,
    )
    .unwrap();
    assert_eq!(forward, vec![6, 1, 2, 5, 0, 3, 4]);
    let reverse = scan_ordered_index(
        pair.index_bytes(),
        &layout,
        pair.data_layout(),
        object_id(0x41),
        None,
        None,
        IndexScanDirection::Reverse,
        0,
        usize::MAX,
    )
    .unwrap();
    assert_eq!(reverse, vec![4, 3, 0, 5, 2, 1, 6]);

    let lower = OrderedIndexBound::new(
        OrderedIndexKey::from_values(
            pair.data_layout(),
            accelerator.key_columns(),
            &[Value::integer(1), Value::integer(20)],
        )
        .unwrap(),
        true,
    );
    let upper = OrderedIndexBound::new(
        OrderedIndexKey::from_values(
            pair.data_layout(),
            accelerator.key_columns(),
            &[Value::integer(2), null()],
        )
        .unwrap(),
        true,
    );
    let window = scan_ordered_index(
        pair.index_bytes(),
        &layout,
        pair.data_layout(),
        object_id(0x41),
        Some(&lower),
        Some(&upper),
        IndexScanDirection::Forward,
        1,
        2,
    )
    .unwrap();
    assert_eq!(window, vec![0, 3]);
}

#[test]
fn nullable_unique_ordered_still_rejects_duplicate_complete_key() {
    let staging = tempfile::tempdir().unwrap();
    let request = composite_ordered_request(staging.path(), 2);
    let result = build_artifact_pair(
        &request,
        (1..=2).map(|row_id| {
            Ok(SourceRow::new(
                row_id,
                vec![Value::integer(1), Value::integer(10)],
            ))
        }),
    );
    assert!(matches!(
        result,
        Err(FormatError::InvalidIndexArtifact {
            detail: "unique ordered key owns more than one row"
        })
    ));
}

#[test]
fn nullable_unique_ordered_rebuild_preserves_spilled_null_postings() {
    let source_staging = tempfile::tempdir().unwrap();
    let request = composite_ordered_request(source_staging.path(), 7);
    let null = || Value::null(DataType::Integer);
    let pair = build_artifact_pair(
        &request,
        [
            (1, vec![Value::integer(1), Value::integer(10)]),
            (2, vec![null(), Value::integer(7)]),
            (3, vec![null(), Value::integer(7)]),
            (4, vec![Value::integer(2), null()]),
            (5, vec![Value::integer(2), null()]),
            (6, vec![Value::integer(1), Value::integer(20)]),
            (7, vec![null(), Value::integer(9)]),
        ]
        .into_iter()
        .map(|(row_id, values)| Ok(SourceRow::new(row_id, values))),
    )
    .unwrap();
    let source_layout = decode_index_artifact_layout(
        pair.index_bytes(),
        pair.index_reference(),
        pair.data_layout(),
    )
    .unwrap();
    let key_columns = source_layout.accelerators()[0].key_columns().to_vec();

    // Rebuild requests carry logical identity and the catalog-definition hash;
    // the current catalog supplies the physical accelerator kind to the
    // replacement builder.  Triggering a missing request through the exact
    // fallback isolates that public hand-off without adding a test-only API.
    let lookup =
        ExactLookupDefinition::new(object_id(0x41), true, true, [0x51; 32], key_columns.clone())
            .unwrap();
    let mut capture = RebuildCapture::default();
    visit_exact_index_or_scan(
        pair.data_bytes(),
        pair.data_layout(),
        &lookup,
        &[Value::integer(1), Value::integer(10)],
        IndexAccessState::Unavailable(UnavailableIndexReason::Missing),
        &mut capture,
        &mut |_| Ok(()),
    )
    .unwrap();
    let rebuild_staging = tempfile::tempdir().unwrap();
    let accelerator = AcceleratorBuildSpec::ordered(
        object_id(0x41),
        true,
        true,
        [0x51; 32],
        key_columns,
        IndexPageCodec::Lz4,
        OrderedPageBuildLimits::new(2, 1024 * 1024).unwrap(),
    )
    .unwrap();
    let rebuild = IndexReplacementBuildRequest::new(
        capture.0.expect("missing accelerator enqueues one rebuild"),
        ArtifactId::from_bytes([0x71; 16]).unwrap(),
        DatabaseGeneration::new(18).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        vec![accelerator],
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 4_096).unwrap(),
        rebuild_staging.path(),
    )
    .unwrap();
    let mut output = std::io::Cursor::new(Vec::new());
    let written =
        write_index_replacement(&rebuild, pair.data_bytes(), pair.data_layout(), &mut output)
            .unwrap();
    let bytes = output.into_inner();
    let layout =
        decode_index_artifact_layout(&bytes, written.reference(), pair.data_layout()).unwrap();
    assert_eq!(
        scan_ordered_index(
            &bytes,
            &layout,
            pair.data_layout(),
            object_id(0x41),
            None,
            None,
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap(),
        vec![6, 1, 2, 5, 0, 3, 4]
    );
    assert!(
        std::fs::read_dir(rebuild_staging.path())
            .unwrap()
            .next()
            .is_none(),
        "completed replacement build leaked temporary runs"
    );
}

#[test]
fn sort_run_configuration_rejects_unbounded_memory() {
    let limits = FanoutBuildLimits::new(65_536, u32::MAX, u64::MAX, 4_096);
    assert!(
        limits.is_err(),
        "unbounded sort-run configuration was admitted: {limits:?}"
    );
    assert!(FanoutBuildLimits::new(
        65_536,
        MAX_SORT_RUN_RECORDS,
        MAX_SORT_RUN_BYTES,
        MAX_SORT_RUN_FILES,
    )
    .is_ok());
}

#[test]
fn large_nonindexed_text_does_not_inherit_index_run_limits() {
    let staging = tempfile::tempdir().unwrap();
    let base = request(
        staging.path(),
        1,
        FanoutBuildLimits::default(),
        false,
        false,
        false,
    );
    let old = base.data_header();
    let header = DataArtifactHeader::new(
        old.artifact_id(),
        old.database_id(),
        old.table_id(),
        old.segment_id(),
        old.creation_generation(),
        old.catalog_generation(),
        501,
        509,
        1,
        2,
        1,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let request = ArtifactPairBuildRequest::new(
        header,
        vec![
            base.columns()[0],
            DataColumnSpec::new(
                object_id(0x22),
                CatalogDataType::scalar(DataType::Text).unwrap(),
                false,
            ),
        ],
        vec![ColumnBuildPolicy::default(); 2],
        DataPhysicalCodec::Lz4,
        base.index_artifact_id(),
        base.accelerators().to_vec(),
        FanoutBuildLimits::default(),
        staging.path(),
    )
    .unwrap();
    let text = "x".repeat(1024 * 1024 + 1);
    let result = build_artifact_pair(
        &request,
        [Ok(SourceRow::new(
            1,
            vec![Value::integer(1), Value::text(&text)],
        ))],
    );
    assert!(
        result.is_ok(),
        "legal unindexed TEXT cannot be published: {result:?}"
    );
}

#[test]
fn fanout_resource_budgets_are_bounded_and_lower_only() {
    let defaults = FanoutBuildLimits::default();
    assert_eq!(
        defaults.resident_byte_budget(),
        DEFAULT_FANOUT_RESIDENT_BYTES
    );
    assert_eq!(defaults.spill_byte_budget(), DEFAULT_FANOUT_SPILL_BYTES);
    assert_eq!(defaults.merge_fan_in(), DEFAULT_MERGE_FAN_IN);
    assert!(defaults.with_merge_fan_in(1).is_err());
    assert!(defaults.with_merge_fan_in(MAX_MERGE_FAN_IN + 1).is_err());
    assert!(defaults.with_merge_fan_in(MAX_MERGE_FAN_IN).is_ok());
    assert!(defaults
        .with_resource_budgets(4 * 1024 * 1024, 2 * 1024 * 1024)
        .is_ok());
    assert!(defaults
        .with_resource_budgets(MAX_FANOUT_RESIDENT_BYTES + 1, DEFAULT_FANOUT_SPILL_BYTES)
        .is_err());
    assert!(defaults
        .with_resource_budgets(DEFAULT_FANOUT_RESIDENT_BYTES, MAX_FANOUT_SPILL_BYTES + 1)
        .is_err());
    assert_eq!(defaults.planned_row_group_rows(0, 1).unwrap(), 1);
}

#[test]
fn wide_schema_plans_row_groups_before_buffer_allocation() {
    let limits = FanoutBuildLimits::default();
    let planned = limits.planned_row_group_rows(2_000, 4_096).unwrap();
    assert!(planned > 0 && planned < 2_000);

    let staging = tempfile::tempdir().unwrap();
    let columns = (0_u32..4_096)
        .map(|ordinal| {
            DataColumnSpec::new(
                ObjectId::from_user_bytes((u128::from(ordinal) + 1).to_le_bytes()).unwrap(),
                CatalogDataType::scalar(DataType::Integer).unwrap(),
                false,
            )
        })
        .collect::<Vec<_>>();
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes([0x71; 16]).unwrap(),
        DatabaseId::from_bytes([0x72; 16]).unwrap(),
        object_id(0x73),
        SegmentId::from_bytes([0x74; 16]).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        2_000,
        4_096,
        2_000_u64.div_ceil(u64::from(planned)) as u32,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(0x75),
        false,
        false,
        [0x76; 32],
        vec![IndexKeyColumn::new(
            columns[0].column_id(),
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    let request = ArtifactPairBuildRequest::new(
        header,
        columns,
        vec![ColumnBuildPolicy::default(); 4_096],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0x77; 16]).unwrap(),
        vec![accelerator],
        limits,
        staging.path(),
    );
    assert!(request.is_ok(), "wide request was rejected: {request:?}");
}

#[test]
fn wide_single_row_metadata_is_rejected_before_source_poll() {
    let staging = tempfile::tempdir().unwrap();
    let limits = FanoutBuildLimits::default()
        .with_resource_budgets(4 * 1024 * 1024, 2 * 1024 * 1024)
        .unwrap();
    let columns = (0_u32..4_096)
        .map(|ordinal| {
            DataColumnSpec::new(
                ObjectId::from_user_bytes((u128::from(ordinal) + 20_000).to_le_bytes()).unwrap(),
                CatalogDataType::scalar(DataType::Integer).unwrap(),
                false,
            )
        })
        .collect::<Vec<_>>();
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(0xb1),
        false,
        false,
        [0xb2; 32],
        vec![IndexKeyColumn::new(
            columns[0].column_id(),
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    let request = ArtifactPairBuildRequest::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes([0xb3; 16]).unwrap(),
            DatabaseId::from_bytes([0xb4; 16]).unwrap(),
            object_id(0xb5),
            SegmentId::from_bytes([0xb6; 16]).unwrap(),
            DatabaseGeneration::new(17).unwrap(),
            CatalogGeneration::new(13).unwrap(),
            501,
            509,
            1,
            4_096,
            1,
            SegmentKind::Rows,
            123_456,
        )
        .unwrap(),
        columns,
        vec![ColumnBuildPolicy::default(); 4_096],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0xb7; 16]).unwrap(),
        vec![accelerator],
        limits,
        staging.path(),
    )
    .unwrap();
    let rows = std::iter::from_fn(|| -> Option<FormatResult<SourceRow>> {
        panic!("metadata admission must precede source polling")
    });
    assert!(matches!(
        build_artifact_pair(&request, rows),
        Err(FormatError::DataArtifactLimitExceeded {
            field: "streaming data metadata resident bytes",
            ..
        })
    ));
}

#[test]
fn variable_row_group_payload_is_checked_before_retention() {
    let staging = tempfile::tempdir().unwrap();
    let integer = DataColumnSpec::new(
        object_id(0x81),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let mut columns = vec![integer];
    columns.extend((0_u8..4).map(|ordinal| {
        DataColumnSpec::new(
            object_id(0x82 + ordinal),
            CatalogDataType::scalar(DataType::Text).unwrap(),
            false,
        )
    }));
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes([0x87; 16]).unwrap(),
        DatabaseId::from_bytes([0x88; 16]).unwrap(),
        object_id(0x89),
        SegmentId::from_bytes([0x8a; 16]).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        1,
        columns.len() as u32,
        1,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(0x8b),
        false,
        false,
        [0x8c; 32],
        vec![IndexKeyColumn::new(
            integer.column_id(),
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    let make_request = |limits| {
        ArtifactPairBuildRequest::new(
            header,
            columns.clone(),
            vec![ColumnBuildPolicy::default(); columns.len()],
            DataPhysicalCodec::Lz4,
            ArtifactId::from_bytes([0x8d; 16]).unwrap(),
            vec![accelerator.clone()],
            limits,
            staging.path(),
        )
        .unwrap()
    };
    let text = "x".repeat(512 * 1024);
    let row = || {
        Ok(SourceRow::new(
            1,
            vec![
                Value::integer(1),
                Value::text(&text),
                Value::text(&text),
                Value::text(&text),
                Value::text(&text),
            ],
        ))
    };
    assert!(build_artifact_pair(&make_request(FanoutBuildLimits::default()), [row()]).is_ok());

    let low = FanoutBuildLimits::default()
        .with_resource_budgets(4 * 1024 * 1024, 2 * 1024 * 1024)
        .unwrap();
    assert!(matches!(
        build_artifact_pair(&make_request(low), [row()]),
        Err(FormatError::DataArtifactLimitExceeded {
            field: "row-group variable resident bytes",
            ..
        })
    ));
}

#[test]
fn rebuild_projection_is_rejected_before_decoding_an_oversized_group() {
    let source_staging = tempfile::tempdir().unwrap();
    let column = DataColumnSpec::new(
        object_id(0xc1),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        false,
    );
    let key_columns = vec![IndexKeyColumn::new(
        column.column_id(),
        DataType::Text,
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )];
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(0xc2),
        false,
        false,
        [0xc3; 32],
        key_columns.clone(),
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    let request = ArtifactPairBuildRequest::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes([0xc4; 16]).unwrap(),
            DatabaseId::from_bytes([0xc5; 16]).unwrap(),
            object_id(0xc6),
            SegmentId::from_bytes([0xc7; 16]).unwrap(),
            DatabaseGeneration::new(17).unwrap(),
            CatalogGeneration::new(13).unwrap(),
            501,
            509,
            1,
            1,
            1,
            SegmentKind::Rows,
            123_456,
        )
        .unwrap(),
        vec![column],
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0xc8; 16]).unwrap(),
        vec![accelerator.clone()],
        FanoutBuildLimits::default(),
        source_staging.path(),
    )
    .unwrap();
    let text = format!("key-{}", "x".repeat(768 * 1024));
    let pair =
        build_artifact_pair(&request, [Ok(SourceRow::new(1, vec![Value::text(&text)]))]).unwrap();
    let lookup =
        ExactLookupDefinition::new(object_id(0xc2), false, false, [0xc3; 32], key_columns).unwrap();
    let mut capture = RebuildCapture::default();
    visit_exact_index_or_scan(
        pair.data_bytes(),
        pair.data_layout(),
        &lookup,
        &[Value::text(&text)],
        IndexAccessState::Unavailable(UnavailableIndexReason::Missing),
        &mut capture,
        &mut |_| Ok(()),
    )
    .unwrap();
    let rebuild_staging = tempfile::tempdir().unwrap();
    let low = FanoutBuildLimits::default()
        .with_resource_budgets(4 * 1024 * 1024, 2 * 1024 * 1024)
        .unwrap();
    let rebuild = IndexReplacementBuildRequest::new(
        capture.0.unwrap(),
        ArtifactId::from_bytes([0xc9; 16]).unwrap(),
        DatabaseGeneration::new(18).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        vec![accelerator],
        low,
        rebuild_staging.path(),
    )
    .unwrap();
    let mut output = std::io::Cursor::new(Vec::new());
    assert!(matches!(
        write_index_replacement(&rebuild, pair.data_bytes(), pair.data_layout(), &mut output),
        Err(FormatError::IndexArtifactLimitExceeded {
            field: "rebuild projection resident bytes",
            ..
        })
    ));
}

#[test]
fn one_sixty_four_accelerators_share_one_resident_corridor() {
    for accelerator_count in [1_usize, 16, 64] {
        let staging = tempfile::tempdir().unwrap();
        let column = DataColumnSpec::new(
            object_id(0x91),
            CatalogDataType::scalar(DataType::Integer).unwrap(),
            false,
        );
        let header = DataArtifactHeader::new(
            ArtifactId::from_bytes([0x92; 16]).unwrap(),
            DatabaseId::from_bytes([0x93; 16]).unwrap(),
            object_id(0x94),
            SegmentId::from_bytes([0x95; 16]).unwrap(),
            DatabaseGeneration::new(17).unwrap(),
            CatalogGeneration::new(13).unwrap(),
            501,
            509,
            4,
            1,
            1,
            SegmentKind::Rows,
            123_456,
        )
        .unwrap();
        let accelerators = (0..accelerator_count)
            .map(|ordinal| {
                AcceleratorBuildSpec::exact(
                    ObjectId::from_user_bytes((10_000_u128 + ordinal as u128).to_le_bytes())
                        .unwrap(),
                    false,
                    false,
                    [ordinal as u8; 32],
                    vec![IndexKeyColumn::new(
                        column.column_id(),
                        DataType::Integer,
                        IndexSortDirection::Ascending,
                        IndexNullsOrder::Last,
                    )],
                    IndexPageCodec::Lz4,
                    ExactPageBuildLimits::default(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let request = ArtifactPairBuildRequest::new(
            header,
            vec![column],
            vec![ColumnBuildPolicy::default()],
            DataPhysicalCodec::Lz4,
            ArtifactId::from_bytes([0x96; 16]).unwrap(),
            accelerators,
            FanoutBuildLimits::default(),
            staging.path(),
        )
        .unwrap();
        let pair = build_artifact_pair(
            &request,
            (0..4).map(|row| Ok(SourceRow::new(row + 1, vec![Value::integer(row as i64)]))),
        )
        .unwrap();
        let layout = decode_index_artifact_layout(
            pair.index_bytes(),
            pair.index_reference(),
            pair.data_layout(),
        )
        .unwrap();
        assert_eq!(layout.accelerators().len(), accelerator_count);
    }
}

#[test]
fn all_accelerators_share_one_spill_budget() {
    let staging = tempfile::tempdir().unwrap();
    let column = DataColumnSpec::new(
        object_id(0xa1),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        false,
    );
    let limits = FanoutBuildLimits::new(64, 8, 1024 * 1024, 128)
        .unwrap()
        .with_resource_budgets(DEFAULT_FANOUT_RESIDENT_BYTES, 2 * 1024 * 1024)
        .unwrap();
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes([0xa2; 16]).unwrap(),
        DatabaseId::from_bytes([0xa3; 16]).unwrap(),
        object_id(0xa4),
        SegmentId::from_bytes([0xa5; 16]).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        400,
        1,
        7,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(0xa6),
        false,
        false,
        [0xa7; 32],
        vec![IndexKeyColumn::new(
            column.column_id(),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    let request = ArtifactPairBuildRequest::new(
        header,
        vec![column],
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0xa8; 16]).unwrap(),
        vec![accelerator],
        limits,
        staging.path(),
    )
    .unwrap();
    let result = build_artifact_pair(
        &request,
        (0..400).map(|row| {
            Ok(SourceRow::new(
                row + 1,
                vec![Value::text(format!("{row:08}-{}", "x".repeat(8 * 1024)))],
            ))
        }),
    );
    assert!(matches!(
        result,
        Err(FormatError::IndexArtifactLimitExceeded {
            field: "fanout spill bytes",
            ..
        })
    ));
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn multi_pass_merge_is_byte_deterministic_across_fan_in() {
    let narrow_staging = tempfile::tempdir().unwrap();
    let wide_staging = tempfile::tempdir().unwrap();
    let narrow_limits = FanoutBuildLimits::new(1_024, 8, 1024 * 1024, 4_096)
        .unwrap()
        .with_merge_fan_in(4)
        .unwrap();
    let wide_limits = FanoutBuildLimits::new(1_024, 8, 1024 * 1024, 4_096)
        .unwrap()
        .with_merge_fan_in(MAX_MERGE_FAN_IN)
        .unwrap();
    let rows = || {
        (0_u64..1_024).map(|row| {
            Ok(SourceRow::new(
                row + 1,
                vec![Value::integer((1_024 - row) as i64)],
            ))
        })
    };

    #[cfg(feature = "test-hooks")]
    reset_publication_diagnostics();
    let narrow = build_artifact_pair(
        &request(
            narrow_staging.path(),
            1_024,
            narrow_limits,
            true,
            false,
            false,
        ),
        rows(),
    )
    .unwrap();
    #[cfg(feature = "test-hooks")]
    {
        let counters = publication_diagnostics();
        assert_eq!(counters.sort_merge_passes, 3);
        assert_eq!(counters.sort_merge_input_runs, 128 + 32 + 8);
        assert!(counters.sort_run_read_bytes > counters.sort_run_write_bytes);
        println!(
            "MERGE_DIAGNOSTICS passes={} input_runs={} read_calls={} read_bytes={} write_calls={} write_bytes={}",
            counters.sort_merge_passes,
            counters.sort_merge_input_runs,
            counters.sort_run_read_calls,
            counters.sort_run_read_bytes,
            counters.sort_run_write_calls,
            counters.sort_run_write_bytes,
        );
    }

    let wide = build_artifact_pair(
        &request(wide_staging.path(), 1_024, wide_limits, true, false, false),
        rows(),
    )
    .unwrap();
    assert_eq!(narrow.data_bytes(), wide.data_bytes());
    assert_eq!(narrow.index_bytes(), wide.index_bytes());
    assert_eq!(narrow.data_reference(), wide.data_reference());
    assert_eq!(narrow.index_reference(), wide.index_reference());
    for staging in [&narrow_staging, &wide_staging] {
        assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
    }
}

#[test]
fn multi_pass_merge_preserves_unique_failure_and_cleans_runs() {
    let staging = tempfile::tempdir().unwrap();
    let limits = FanoutBuildLimits::new(64, 4, 1024 * 1024, 4_096)
        .unwrap()
        .with_merge_fan_in(2)
        .unwrap();
    let error = build_artifact_pair(
        &request(staging.path(), 64, limits, false, true, false),
        (0_u64..64).map(|row| {
            let value = if row == 63 { 0 } else { row };
            Ok(SourceRow::new(row + 1, vec![Value::integer(value as i64)]))
        }),
    )
    .unwrap_err();
    assert!(matches!(error, FormatError::InvalidIndexArtifact { .. }));
    assert!(error.to_string().contains("more than one row"));
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn intermediate_merge_spill_failure_cleans_every_run() {
    let staging = tempfile::tempdir().unwrap();
    let column = DataColumnSpec::new(
        object_id(0xd1),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        false,
    );
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(0xd2),
        false,
        false,
        [0xd3; 32],
        vec![IndexKeyColumn::new(
            column.column_id(),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    let limits = FanoutBuildLimits::new(512, 8, 1024 * 1024, 4_096)
        .unwrap()
        .with_merge_fan_in(4)
        .unwrap()
        .with_resource_budgets(DEFAULT_FANOUT_RESIDENT_BYTES, 6 * 1024 * 1024)
        .unwrap();
    let request = ArtifactPairBuildRequest::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes([0xd4; 16]).unwrap(),
            DatabaseId::from_bytes([0xd5; 16]).unwrap(),
            object_id(0xd6),
            SegmentId::from_bytes([0xd7; 16]).unwrap(),
            DatabaseGeneration::new(17).unwrap(),
            CatalogGeneration::new(13).unwrap(),
            501,
            509,
            512,
            1,
            1,
            SegmentKind::Rows,
            123_456,
        )
        .unwrap(),
        vec![column],
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0xd8; 16]).unwrap(),
        vec![accelerator],
        limits,
        staging.path(),
    )
    .unwrap();
    let error = build_artifact_pair(
        &request,
        (0_u64..512).map(|row| {
            Ok(SourceRow::new(
                row + 1,
                vec![Value::text(format!("{row:08}-{}", "x".repeat(8 * 1024)))],
            ))
        }),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        FormatError::IndexArtifactLimitExceeded {
            field: "fanout spill bytes",
            ..
        }
    ));
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn intermediate_merge_collision_preserves_the_unowned_file() {
    let staging = tempfile::tempdir().unwrap();
    let collision = staging.path().join("index-sort-0-merge-0000-00000000.run");
    std::fs::write(&collision, b"not-owned-by-the-builder").unwrap();
    let limits = FanoutBuildLimits::new(64, 4, 1024 * 1024, 4_096)
        .unwrap()
        .with_merge_fan_in(2)
        .unwrap();
    let error = build_artifact_pair(
        &request(staging.path(), 64, limits, false, false, false),
        (0_u64..64).map(|row| {
            Ok(SourceRow::new(
                row + 1,
                vec![Value::integer((64 - row) as i64)],
            ))
        }),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        FormatError::ArtifactIo {
            operation: "create merged sort run",
            ..
        }
    ));
    assert_eq!(
        std::fs::read(&collision).unwrap(),
        b"not-owned-by-the-builder"
    );
    let entries = std::fs::read_dir(staging.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, vec![collision.file_name().unwrap()]);
}
