use std::cell::Cell;
use std::rc::Rc;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    admit_constraint_mutation, build_artifact_pair, decode_data_artifact_layout,
    decode_index_artifact_layout, lookup_exact_index, read_data_column, read_data_row_ids,
    scan_ordered_index, visit_exact_index_or_scan, write_artifact_pair, write_index_replacement,
    AcceleratorBuildSpec, ArtifactId, ArtifactPairBuildRequest, ArtifactSource, CatalogGeneration,
    ColumnBuildPolicy, ConstraintMutationAdmission, ConstraintMutationError,
    ConstraintMutationKind, DataArtifactHeader, DataBlockKind, DataColumnSpec, DataPhysicalCodec,
    DataValueEncoding, DatabaseGeneration, DatabaseId, ExactFallbackReason, ExactLookupDefinition,
    ExactLookupPath, ExactPageBuildLimits, FanoutBuildLimits, FormatError, FormatResult,
    IndexAccessState, IndexKeyColumn, IndexNullsOrder, IndexPageCodec, IndexRebuildRequest,
    IndexReplacementBuildRequest, IndexScanDirection, IndexSectionKind, IndexSortDirection,
    OrderedIndexBound, OrderedIndexKey, OrderedPageBuildLimits, RebuildRequestSink,
    RebuildRequestStatus, SegmentId, SegmentKind, SourceRow, UnavailableIndexReason,
};
#[cfg(feature = "test-hooks")]
use radixdb_storage::v6::{publication_diagnostics, reset_publication_diagnostics};
#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn rows() -> Vec<SourceRow> {
    [
        (101, 5, "alpha"),
        (103, 1, "beta"),
        (107, 3, "alpha"),
        (109, 2, "gamma"),
        (113, 4, "beta"),
        (127, 6, "alpha"),
    ]
    .into_iter()
    .map(|(row_id, integer, text)| {
        SourceRow::new(row_id, vec![Value::integer(integer), Value::text(text)])
    })
    .collect()
}

fn request(staging: &std::path::Path, unique_text: bool) -> ArtifactPairBuildRequest {
    request_with_shape(
        staging,
        unique_text,
        6,
        3,
        ExactPageBuildLimits::new(2, 1024 * 1024).unwrap(),
    )
}

fn request_with_shape(
    staging: &std::path::Path,
    unique_text: bool,
    row_count: u64,
    row_group_count: u32,
    exact_limits: ExactPageBuildLimits,
) -> ArtifactPairBuildRequest {
    request_with_shape_and_exact_key(
        staging,
        unique_text,
        row_count,
        row_group_count,
        exact_limits,
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        vec![IndexKeyColumn::new(
            object_id(0x22),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
    )
}

fn request_with_shape_and_exact_key(
    staging: &std::path::Path,
    unique_text: bool,
    row_count: u64,
    row_group_count: u32,
    exact_limits: ExactPageBuildLimits,
    limits: FanoutBuildLimits,
    exact_key_columns: Vec<IndexKeyColumn>,
) -> ArtifactPairBuildRequest {
    request_with_shape_and_index_keys(
        staging,
        unique_text,
        row_count,
        row_group_count,
        exact_limits,
        OrderedPageBuildLimits::new(2, 1024 * 1024).unwrap(),
        limits,
        exact_key_columns,
        vec![IndexKeyColumn::new(
            object_id(0x21),
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
    )
}

#[allow(clippy::too_many_arguments)]
fn request_with_shape_and_index_keys(
    staging: &std::path::Path,
    unique_text: bool,
    row_count: u64,
    row_group_count: u32,
    exact_limits: ExactPageBuildLimits,
    ordered_limits: OrderedPageBuildLimits,
    limits: FanoutBuildLimits,
    exact_key_columns: Vec<IndexKeyColumn>,
    ordered_key_columns: Vec<IndexKeyColumn>,
) -> ArtifactPairBuildRequest {
    let integer_column = DataColumnSpec::new(
        object_id(0x21),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let text_column = DataColumnSpec::new(
        object_id(0x22),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        false,
    );
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x31)).unwrap(),
        DatabaseId::from_bytes(raw(0x32)).unwrap(),
        object_id(0x33),
        SegmentId::from_bytes(raw(0x34)).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        row_count,
        2,
        row_group_count,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let exact = AcceleratorBuildSpec::exact(
        object_id(0x41),
        unique_text,
        unique_text,
        [0x51; 32],
        exact_key_columns,
        IndexPageCodec::Lz4,
        exact_limits,
    )
    .unwrap();
    let ordered = AcceleratorBuildSpec::ordered(
        object_id(0x42),
        false,
        false,
        [0x52; 32],
        ordered_key_columns,
        IndexPageCodec::Lz4,
        ordered_limits,
    )
    .unwrap();
    ArtifactPairBuildRequest::new(
        header,
        vec![integer_column, text_column],
        vec![
            ColumnBuildPolicy::new(DataValueEncoding::Plain, DataPhysicalCodec::Lz4, None),
            ColumnBuildPolicy::new(DataValueEncoding::Dictionary, DataPhysicalCodec::Lz4, None),
        ],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes(raw(0x35)).unwrap(),
        vec![exact, ordered],
        limits,
        staging,
    )
    .unwrap()
}

struct CountingRows {
    rows: std::vec::IntoIter<SourceRow>,
    visits: Rc<Cell<usize>>,
}

impl Iterator for CountingRows {
    type Item = FormatResult<SourceRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|row| {
            self.visits.set(self.visits.get() + 1);
            Ok(row)
        })
    }
}

struct TracedSource<'a> {
    bytes: &'a [u8],
    read_calls: Cell<u64>,
    read_bytes: Cell<u64>,
}

impl<'a> TracedSource<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            read_calls: Cell::new(0),
            read_bytes: Cell::new(0),
        }
    }
}

impl ArtifactSource for TracedSource<'_> {
    fn byte_length(&self) -> FormatResult<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        self.bytes.read_exact_at(offset, destination)?;
        self.read_calls.set(self.read_calls.get() + 1);
        self.read_bytes
            .set(self.read_bytes.get() + destination.len() as u64);
        Ok(())
    }
}

struct RecordingRebuilds {
    requests: Vec<IndexRebuildRequest>,
    status: RebuildRequestStatus,
}

impl RecordingRebuilds {
    fn new(status: RebuildRequestStatus) -> Self {
        Self {
            requests: Vec::new(),
            status,
        }
    }
}

impl RebuildRequestSink for RecordingRebuilds {
    fn request_rebuild(&mut self, request: IndexRebuildRequest) -> RebuildRequestStatus {
        self.requests.push(request);
        self.status
    }
}

fn exact_lookup_definition() -> ExactLookupDefinition {
    ExactLookupDefinition::new(
        object_id(0x41),
        false,
        false,
        [0x51; 32],
        vec![IndexKeyColumn::new(
            object_id(0x22),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
    )
    .unwrap()
}

fn text_index_key() -> Vec<IndexKeyColumn> {
    vec![IndexKeyColumn::new(
        object_id(0x22),
        DataType::Text,
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    )]
}

fn unique_request(staging: &std::path::Path, nullable: bool) -> ArtifactPairBuildRequest {
    let column = DataColumnSpec::new(
        object_id(0x61),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        nullable,
    );
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x62)).unwrap(),
        DatabaseId::from_bytes(raw(0x63)).unwrap(),
        object_id(0x64),
        SegmentId::from_bytes(raw(0x65)).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        601,
        607,
        3,
        1,
        2,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let exact = AcceleratorBuildSpec::exact(
        object_id(0x66),
        true,
        true,
        [0x67; 32],
        vec![IndexKeyColumn::new(
            column.column_id(),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::new(2, 1024 * 1024).unwrap(),
    )
    .unwrap();
    ArtifactPairBuildRequest::new(
        header,
        vec![column],
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes(raw(0x68)).unwrap(),
        vec![exact],
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        staging,
    )
    .unwrap()
}

fn unique_definition() -> ExactLookupDefinition {
    ExactLookupDefinition::new(
        object_id(0x66),
        true,
        true,
        [0x67; 32],
        vec![IndexKeyColumn::new(
            object_id(0x61),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
    )
    .unwrap()
}

#[test]
fn one_source_pass_builds_data_and_spilled_exact_ordered_pages() {
    let staging = tempfile::tempdir().unwrap();
    let visits = Rc::new(Cell::new(0));
    let source = CountingRows {
        rows: rows().into_iter(),
        visits: Rc::clone(&visits),
    };
    let built = build_artifact_pair(&request(staging.path(), false), source).unwrap();
    assert_eq!(visits.get(), 6, "source rows must be consumed exactly once");
    assert_eq!(built.data_layout().header().row_count(), 6);
    assert_eq!(built.data_layout().row_groups().len(), 3);
    assert_eq!(
        read_data_row_ids(built.data_bytes(), built.data_layout(), 0).unwrap(),
        vec![101, 103]
    );
    assert_eq!(
        read_data_column(built.data_bytes(), built.data_layout(), 1, 0).unwrap(),
        vec![Value::integer(3), Value::integer(2)]
    );

    let index_layout = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    assert_eq!(index_layout.accelerators().len(), 2);
    assert_eq!(
        lookup_exact_index(
            built.index_bytes(),
            &index_layout,
            built.data_layout(),
            object_id(0x41),
            &[Value::text("alpha")],
        )
        .unwrap(),
        Some(vec![0, 2, 5])
    );

    let ordered = index_layout
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == object_id(0x42))
        .unwrap();
    let lower = OrderedIndexBound::new(
        OrderedIndexKey::from_values(
            built.data_layout(),
            ordered.key_columns(),
            &[Value::integer(2)],
        )
        .unwrap(),
        true,
    );
    let upper = OrderedIndexBound::new(
        OrderedIndexKey::from_values(
            built.data_layout(),
            ordered.key_columns(),
            &[Value::integer(5)],
        )
        .unwrap(),
        true,
    );
    assert_eq!(
        scan_ordered_index(
            built.index_bytes(),
            &index_layout,
            built.data_layout(),
            object_id(0x42),
            Some(&lower),
            Some(&upper),
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap(),
        vec![3, 2, 4, 0]
    );
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[cfg(feature = "test-hooks")]
#[test]
fn diagnostics_prove_one_source_pass_and_no_artifact_construction_readback() {
    let staging = tempfile::tempdir().unwrap();
    reset_publication_diagnostics();

    let built =
        build_artifact_pair(&request(staging.path(), false), rows().into_iter().map(Ok)).unwrap();
    let counters = publication_diagnostics();

    assert_eq!(counters.publication_invocations, 1);
    assert_eq!(counters.rebuild_invocations, 0);
    assert_eq!(counters.source_stream_passes, 1);
    assert_eq!(counters.source_rows, 6);
    assert_eq!(counters.data_encode_passes, 1);
    assert_eq!(counters.data_row_groups_encoded, 3);
    assert_eq!(counters.index_planning_passes, 2);
    assert_eq!(counters.index_encoding_passes, 2);
    assert_eq!(counters.postings_planned, 9);
    assert_eq!(counters.postings_encoded, 9);

    assert_eq!(counters.data_construction_read_calls, 0);
    assert_eq!(counters.data_construction_read_bytes, 0);
    assert_eq!(counters.index_construction_read_calls, 0);
    assert_eq!(counters.index_construction_read_bytes, 0);
    assert_eq!(
        counters.data_identity_read_bytes,
        (built.data_bytes().len() - radixdb_storage::v6::DATA_FOOTER_BYTES) as u64
    );
    assert_eq!(
        counters.index_identity_read_bytes,
        (built.index_bytes().len() - radixdb_storage::v6::INDEX_FOOTER_BYTES) as u64
    );
    assert!(counters.data_identity_read_calls > 0);
    assert!(counters.index_identity_read_calls > 0);
    assert!(counters.data_write_calls > 0);
    assert!(counters.data_write_bytes >= built.data_bytes().len() as u64);
    assert!(counters.index_write_calls > 0);
    assert!(counters.index_write_bytes >= built.index_bytes().len() as u64);
    assert!(counters.sort_run_write_calls > 0);
    assert!(counters.sort_run_write_bytes > 0);
    assert!(counters.sort_run_read_calls > 0);
    assert_eq!(
        counters.sort_run_read_bytes,
        counters.sort_run_write_bytes * 2,
        "the bounded run set is consumed once for planning and once for encoding"
    );

    reset_publication_diagnostics();
}

#[test]
fn exact_access_tri_state_uses_index_or_bounded_same_data_scan_without_false_negative() {
    let staging = tempfile::tempdir().unwrap();
    let built =
        build_artifact_pair(&request(staging.path(), false), rows().into_iter().map(Ok)).unwrap();
    let index_layout = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    let definition = exact_lookup_definition();

    let data_source = TracedSource::new(built.data_bytes());
    let index_source = TracedSource::new(built.index_bytes());
    let mut rebuilds = RecordingRebuilds::new(RebuildRequestStatus::Enqueued);
    let mut indexed_rows = Vec::new();
    let indexed = visit_exact_index_or_scan(
        &data_source,
        built.data_layout(),
        &definition,
        &[Value::text("alpha")],
        IndexAccessState::Available {
            source: &index_source,
            layout: &index_layout,
        },
        &mut rebuilds,
        &mut |row| {
            indexed_rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(indexed_rows, vec![0, 2, 5]);
    assert_eq!(indexed.path(), ExactLookupPath::Accelerator);
    assert_eq!(indexed.matched_rows(), 3);
    assert_eq!(indexed.scanned_row_groups(), 0);
    assert_eq!(indexed.scanned_column_blocks(), 0);
    assert!(indexed.fallback_reason().is_none());
    assert!(indexed.rebuild_status().is_none());
    assert!(rebuilds.requests.is_empty());
    assert_eq!(data_source.read_calls.get(), 0);

    let missing_data_source = TracedSource::new(built.data_bytes());
    let mut at_capacity = RecordingRebuilds::new(RebuildRequestStatus::AtCapacity);
    let mut fallback_rows = Vec::new();
    let missing = visit_exact_index_or_scan(
        &missing_data_source,
        built.data_layout(),
        &definition,
        &[Value::text("alpha")],
        IndexAccessState::Unavailable(UnavailableIndexReason::Missing),
        &mut at_capacity,
        &mut |row| {
            fallback_rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(fallback_rows, indexed_rows);
    assert_eq!(missing.path(), ExactLookupPath::DataScan);
    assert_eq!(missing.matched_rows(), 3);
    assert_eq!(missing.scanned_row_groups(), 3);
    assert_eq!(missing.scanned_column_blocks(), 3);
    assert_eq!(missing_data_source.read_calls.get(), 3);
    assert!(missing_data_source.read_bytes.get() < built.data_bytes().len() as u64);
    assert_eq!(
        missing.rebuild_status(),
        Some(RebuildRequestStatus::AtCapacity)
    );
    assert!(matches!(
        missing.fallback_reason(),
        Some(ExactFallbackReason::Unavailable(
            UnavailableIndexReason::Missing
        ))
    ));
    assert_eq!(at_capacity.requests.len(), 1);
    let request = &at_capacity.requests[0];
    assert_eq!(
        request.database_id(),
        built.data_layout().header().database_id()
    );
    assert_eq!(request.table_id(), built.data_layout().header().table_id());
    assert_eq!(
        request.segment_id(),
        built.data_layout().header().segment_id()
    );
    assert_eq!(request.data_artifact(), built.data_reference());
    assert_eq!(request.logical_index_id(), definition.logical_index_id());
    assert_eq!(request.definition_sha256(), definition.definition_sha256());

    let rebuilding_source = TracedSource::new(built.data_bytes());
    let mut already_pending = RecordingRebuilds::new(RebuildRequestStatus::AlreadyPending);
    let mut rebuilding_rows = Vec::new();
    let rebuilding = visit_exact_index_or_scan(
        &rebuilding_source,
        built.data_layout(),
        &definition,
        &[Value::text("alpha")],
        IndexAccessState::Rebuilding,
        &mut already_pending,
        &mut |row| {
            rebuilding_rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(rebuilding_rows, indexed_rows);
    assert_eq!(rebuilding.path(), ExactLookupPath::DataScan);
    assert!(matches!(
        rebuilding.fallback_reason(),
        Some(ExactFallbackReason::Rebuilding)
    ));
    assert!(rebuilding.rebuild_status().is_none());
    assert!(already_pending.requests.is_empty());
}

#[test]
fn corrupt_exact_page_falls_back_to_data_and_requests_rebuild() {
    let staging = tempfile::tempdir().unwrap();
    let built =
        build_artifact_pair(&request(staging.path(), false), rows().into_iter().map(Ok)).unwrap();
    let index_layout = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    let accelerator = index_layout
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == object_id(0x41))
        .unwrap();
    let exact_section_index = accelerator.first_section_index() as usize + 1;
    let page = index_layout
        .pages()
        .iter()
        .find(|page| page.section_index() as usize == exact_section_index)
        .copied()
        .unwrap();
    let mut corrupt_index = built.index_bytes().to_vec();
    corrupt_index[page.offset() as usize] ^= 0x80;

    let data_source = TracedSource::new(built.data_bytes());
    let index_source = TracedSource::new(&corrupt_index);
    let mut rebuilds = RecordingRebuilds::new(RebuildRequestStatus::Enqueued);
    let mut selected = Vec::new();
    let report = visit_exact_index_or_scan(
        &data_source,
        built.data_layout(),
        &exact_lookup_definition(),
        &[Value::text("alpha")],
        IndexAccessState::Available {
            source: &index_source,
            layout: &index_layout,
        },
        &mut rebuilds,
        &mut |row| {
            selected.push(row);
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(selected, vec![0, 2, 5]);
    assert_eq!(report.path(), ExactLookupPath::DataScan);
    assert_eq!(
        report.rebuild_status(),
        Some(RebuildRequestStatus::Enqueued)
    );
    assert!(matches!(
        report.fallback_reason(),
        Some(ExactFallbackReason::Unavailable(
            UnavailableIndexReason::Invalid(detail)
        )) if detail.contains("checksum mismatch")
    ));
    assert_eq!(rebuilds.requests.len(), 1);
}

#[test]
fn nullable_unique_all_null_rows_publish_a_valid_zero_entry_accelerator() {
    let first_staging = tempfile::tempdir().unwrap();
    let second_staging = tempfile::tempdir().unwrap();
    let source = || {
        [701_u64, 709, 719]
            .into_iter()
            .map(|row_id| Ok(SourceRow::new(row_id, vec![Value::null(DataType::Text)])))
    };
    let first = build_artifact_pair(&unique_request(first_staging.path(), true), source()).unwrap();
    let second =
        build_artifact_pair(&unique_request(second_staging.path(), true), source()).unwrap();
    assert_eq!(first.data_bytes(), second.data_bytes());
    assert_eq!(first.index_bytes(), second.index_bytes());

    let layout = decode_index_artifact_layout(
        first.index_bytes(),
        first.index_reference(),
        first.data_layout(),
    )
    .unwrap();
    let accelerator = &layout.accelerators()[0];
    assert!(accelerator.unique());
    assert!(accelerator.constraint_owned());
    assert_eq!(accelerator.indexed_item_count(), 0);
    let section = layout.sections()[accelerator.first_section_index() as usize + 1];
    assert_eq!(section.kind(), IndexSectionKind::ExactPages);
    assert_eq!(section.page_count(), 1);
    assert_eq!(section.reference().item_count(), 0);
    let page = layout.pages()[section.first_page_index() as usize];
    assert_eq!(page.item_count(), 0);
    assert_eq!((page.minimum_key_hash(), page.maximum_key_hash()), (0, 0));
    for value in [Value::null(DataType::Text), Value::text("not-present")] {
        assert_eq!(
            lookup_exact_index(
                first.index_bytes(),
                &layout,
                first.data_layout(),
                object_id(0x66),
                &[value],
            )
            .unwrap(),
            None
        );
    }

    let data_source = TracedSource::new(first.data_bytes());
    let index_source = TracedSource::new(first.index_bytes());
    let mut rebuilds = RecordingRebuilds::new(RebuildRequestStatus::Enqueued);
    let mut visited = false;
    let report = visit_exact_index_or_scan(
        &data_source,
        first.data_layout(),
        &unique_definition(),
        &[Value::text("not-present")],
        IndexAccessState::Available {
            source: &index_source,
            layout: &layout,
        },
        &mut rebuilds,
        &mut |_| {
            visited = true;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(report.path(), ExactLookupPath::Accelerator);
    assert_eq!(report.matched_rows(), 0);
    assert!(!visited);
    assert_eq!(data_source.read_calls.get(), 0);
    assert!(rebuilds.requests.is_empty());
}

#[test]
fn unique_constraint_mutations_require_a_verified_accelerator_without_scan_fallback() {
    let staging = tempfile::tempdir().unwrap();
    let built = build_artifact_pair(
        &unique_request(staging.path(), false),
        ["alpha", "beta", "gamma"]
            .into_iter()
            .enumerate()
            .map(|(ordinal, value)| {
                Ok(SourceRow::new(
                    801 + ordinal as u64,
                    vec![Value::text(value)],
                ))
            }),
    )
    .unwrap();
    let layout = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    let definition = unique_definition();

    for (mutation, state) in [
        (
            ConstraintMutationKind::Insert,
            IndexAccessState::Unavailable(UnavailableIndexReason::Missing),
        ),
        (ConstraintMutationKind::Delete, IndexAccessState::Rebuilding),
        (
            ConstraintMutationKind::Update,
            IndexAccessState::Unavailable(UnavailableIndexReason::Invalid(
                "page CRC mismatch".to_owned(),
            )),
        ),
    ] {
        let changed = if mutation == ConstraintMutationKind::Update {
            vec![object_id(0x61)]
        } else {
            Vec::new()
        };
        let error =
            admit_constraint_mutation(built.data_layout(), &definition, mutation, &changed, state)
                .err()
                .expect("protected mutation must be rejected");
        assert_eq!(error.to_string(), "constraint accelerator unavailable");
        assert_eq!(error.logical_index_id(), Some(object_id(0x66)));
        assert_eq!(error.mutation(), Some(mutation));
        assert!(matches!(
            error,
            ConstraintMutationError::AcceleratorUnavailable { .. }
        ));
    }

    assert!(matches!(
        admit_constraint_mutation(
            built.data_layout(),
            &definition,
            ConstraintMutationKind::Update,
            &[object_id(0x7f)],
            IndexAccessState::Unavailable(UnavailableIndexReason::Missing),
        )
        .unwrap_or_else(|error| panic!("non-key update was rejected: {error}")),
        ConstraintMutationAdmission::NotRequired
    ));

    let index_source = TracedSource::new(built.index_bytes());
    let admission = admit_constraint_mutation(
        built.data_layout(),
        &definition,
        ConstraintMutationKind::Insert,
        &[],
        IndexAccessState::Available {
            source: &index_source,
            layout: &layout,
        },
    )
    .unwrap_or_else(|error| panic!("valid accelerator was rejected: {error}"));
    let ConstraintMutationAdmission::Verified(guard) = admission else {
        panic!("UNIQUE insert did not require its accelerator");
    };
    assert_eq!(guard.mutation(), ConstraintMutationKind::Insert);
    assert_eq!(
        guard.conflicting_rows(&[Value::text("beta")]).unwrap(),
        Some(vec![1])
    );

    let page = layout.pages()[0];
    let mut corrupt = built.index_bytes().to_vec();
    corrupt[page.offset() as usize] ^= 0x80;
    let corrupt_source = TracedSource::new(&corrupt);
    let admission = admit_constraint_mutation(
        built.data_layout(),
        &definition,
        ConstraintMutationKind::Insert,
        &[],
        IndexAccessState::Available {
            source: &corrupt_source,
            layout: &layout,
        },
    )
    .unwrap_or_else(|error| panic!("metadata admission failed early: {error}"));
    let ConstraintMutationAdmission::Verified(guard) = admission else {
        panic!("UNIQUE insert did not require its accelerator");
    };
    let error = guard
        .conflicting_rows(&[Value::text("delta")])
        .expect_err("corrupt constraint page must reject DML");
    assert_eq!(error.to_string(), "constraint accelerator unavailable");
    assert!(matches!(
        error.reason(),
        Some(ExactFallbackReason::Unavailable(
            UnavailableIndexReason::Invalid(detail)
        )) if detail.contains("checksum mismatch")
    ));
}

#[test]
fn composite_fallback_uses_catalog_definition_and_prunes_columns_per_group() {
    let staging = tempfile::tempdir().unwrap();
    let exact_columns = vec![
        IndexKeyColumn::new(
            object_id(0x21),
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        ),
        IndexKeyColumn::new(
            object_id(0x22),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        ),
    ];
    let request = request_with_shape_and_exact_key(
        staging.path(),
        false,
        6,
        3,
        ExactPageBuildLimits::new(2, 1024 * 1024).unwrap(),
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        exact_columns.clone(),
    );
    let built = build_artifact_pair(&request, rows().into_iter().map(Ok)).unwrap();
    let index_layout = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    let definition = ExactLookupDefinition::new(
        object_id(0x41),
        false,
        false,
        [0x51; 32],
        exact_columns.clone(),
    )
    .unwrap();

    let data_source = TracedSource::new(built.data_bytes());
    let mut rebuilds = RecordingRebuilds::new(RebuildRequestStatus::Enqueued);
    let mut selected = Vec::new();
    let report = visit_exact_index_or_scan(
        &data_source,
        built.data_layout(),
        &definition,
        &[Value::integer(3), Value::text("alpha")],
        IndexAccessState::Unavailable(UnavailableIndexReason::Missing),
        &mut rebuilds,
        &mut |row| {
            selected.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(selected, vec![2]);
    assert_eq!(report.scanned_row_groups(), 3);
    assert_eq!(report.scanned_column_blocks(), 4);

    let mismatched_definition =
        ExactLookupDefinition::new(object_id(0x41), false, false, [0x99; 32], exact_columns)
            .unwrap();
    let index_source = TracedSource::new(built.index_bytes());
    let mut mismatch_rebuilds = RecordingRebuilds::new(RebuildRequestStatus::Enqueued);
    let mut mismatch_rows = Vec::new();
    let mismatch = visit_exact_index_or_scan(
        &data_source,
        built.data_layout(),
        &mismatched_definition,
        &[Value::integer(3), Value::text("alpha")],
        IndexAccessState::Available {
            source: &index_source,
            layout: &index_layout,
        },
        &mut mismatch_rebuilds,
        &mut |row| {
            mismatch_rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(mismatch_rows, selected);
    assert!(matches!(
        mismatch.fallback_reason(),
        Some(ExactFallbackReason::Unavailable(
            UnavailableIndexReason::CrossReferenceMismatch(detail)
        )) if detail.contains("differs from catalog definition")
    ));
    assert_eq!(mismatch_rebuilds.requests.len(), 1);
}

#[test]
fn corrupt_authoritative_data_fails_closed_instead_of_returning_empty_fallback() {
    let staging = tempfile::tempdir().unwrap();
    let built =
        build_artifact_pair(&request(staging.path(), false), rows().into_iter().map(Ok)).unwrap();
    let text_block = built
        .data_layout()
        .blocks()
        .iter()
        .find(|block| {
            block.kind() == DataBlockKind::Column
                && block.column_ordinal() == 1
                && block.row_group_ordinal() == 0
        })
        .unwrap();
    let mut corrupt_data = built.data_bytes().to_vec();
    corrupt_data[text_block.offset() as usize] ^= 0x40;
    let data_source = TracedSource::new(&corrupt_data);
    let mut rebuilds = RecordingRebuilds::new(RebuildRequestStatus::Enqueued);

    let error = visit_exact_index_or_scan(
        &data_source,
        built.data_layout(),
        &exact_lookup_definition(),
        &[Value::text("alpha")],
        IndexAccessState::Unavailable(UnavailableIndexReason::Missing),
        &mut rebuilds,
        &mut |_| panic!("corrupt authoritative data cannot yield a row"),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        FormatError::DataArtifactChecksumMismatch { .. }
    ));
    assert_eq!(rebuilds.requests.len(), 1);
}

#[test]
fn automatic_and_single_worker_fanout_are_byte_deterministic() {
    let first_staging = tempfile::tempdir().unwrap();
    let second_staging = tempfile::tempdir().unwrap();
    let first = build_artifact_pair(
        &request(first_staging.path(), false),
        rows().into_iter().map(Ok),
    )
    .unwrap();
    let single_worker_limits = FanoutBuildLimits::new(2, 2, 1024 * 1024, 128)
        .unwrap()
        .with_accelerator_preparation_workers(1)
        .unwrap();
    let second_request = request_with_shape_and_exact_key(
        second_staging.path(),
        false,
        6,
        3,
        ExactPageBuildLimits::new(2, 1024 * 1024).unwrap(),
        single_worker_limits,
        vec![IndexKeyColumn::new(
            object_id(0x22),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
    );
    let second = build_artifact_pair(&second_request, rows().into_iter().map(Ok)).unwrap();
    assert_eq!(first.data_bytes(), second.data_bytes());
    assert_eq!(first.data_reference(), second.data_reference());
    assert_eq!(first.index_bytes(), second.index_bytes());
    assert_eq!(first.index_reference(), second.index_reference());
}

#[test]
fn file_sinks_match_memory_fixture_and_place_variable_statistics_after_payloads() {
    let expected_staging = tempfile::tempdir().unwrap();
    let expected = build_artifact_pair(
        &request(expected_staging.path(), false),
        rows().into_iter().map(Ok),
    )
    .unwrap();

    let staging = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let data_path = output.path().join("segment.data");
    let index_path = output.path().join("segment.idx");
    let mut data_file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&data_path)
        .unwrap();
    let mut index_file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&index_path)
        .unwrap();
    let written = write_artifact_pair(
        &request(staging.path(), false),
        rows().into_iter().map(Ok),
        &mut data_file,
        &mut index_file,
    )
    .unwrap();
    drop((data_file, index_file));

    let data_bytes = std::fs::read(data_path).unwrap();
    let index_bytes = std::fs::read(index_path).unwrap();
    assert_eq!(data_bytes, expected.data_bytes());
    assert_eq!(index_bytes, expected.index_bytes());
    assert_eq!(written.data_reference(), expected.data_reference());
    assert_eq!(written.index_reference(), expected.index_reference());
    let decoded = decode_data_artifact_layout(&data_bytes, written.data_reference()).unwrap();
    let last_block_end = decoded
        .blocks()
        .iter()
        .map(|block| block.offset() + block.stored_length())
        .max()
        .unwrap();
    let statistic_values = decoded.sections()[4];
    assert!(statistic_values.stored_length() > 0);
    assert!(statistic_values.offset() >= last_block_end);
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn unique_violation_across_sort_runs_fails_and_cleans_staging() {
    let staging = tempfile::tempdir().unwrap();
    let error = build_artifact_pair(&request(staging.path(), true), rows().into_iter().map(Ok))
        .unwrap_err();
    assert!(matches!(error, FormatError::InvalidIndexArtifact { .. }));
    assert!(error.to_string().contains("more than one row"));
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn malformed_or_short_source_fails_without_leaking_runs() {
    let staging = tempfile::tempdir().unwrap();
    let mut source = rows();
    source.pop();
    let error = build_artifact_pair(&request(staging.path(), false), source.into_iter().map(Ok))
        .unwrap_err();
    assert!(matches!(error, FormatError::InvalidDataArtifact { .. }));
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn one_hot_key_streams_through_bounded_posting_fragments_and_cleans_runs() {
    let staging = tempfile::tempdir().unwrap();
    let source = (0_u64..256).map(|ordinal| {
        Ok(SourceRow::new(
            1_000 + ordinal,
            vec![Value::integer(ordinal as i64), Value::text("one-key")],
        ))
    });
    let request = request_with_shape_and_index_keys(
        staging.path(),
        false,
        256,
        128,
        ExactPageBuildLimits::new(1_024, 128).unwrap(),
        OrderedPageBuildLimits::new(1_024, 128).unwrap(),
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        text_index_key(),
        text_index_key(),
    );
    let built = build_artifact_pair(&request, source).unwrap();
    let index = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    );
    let index = index.unwrap();
    let exact = index
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == object_id(0x41))
        .unwrap();
    let pages = index.sections()[exact.first_section_index() as usize + 1].page_count();
    assert!(pages > 1, "hot posting must span bounded index pages");
    assert_eq!(
        lookup_exact_index(
            built.index_bytes(),
            &index,
            built.data_layout(),
            object_id(0x41),
            &[Value::text("one-key")],
        )
        .unwrap(),
        Some((0_u64..256).collect())
    );
    let ordered = index
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == object_id(0x42))
        .unwrap();
    let bound = OrderedIndexBound::new(
        OrderedIndexKey::from_values(
            built.data_layout(),
            ordered.key_columns(),
            &[Value::text("one-key")],
        )
        .unwrap(),
        true,
    );
    assert_eq!(
        scan_ordered_index(
            built.index_bytes(),
            &index,
            built.data_layout(),
            object_id(0x42),
            Some(&bound),
            Some(&bound),
            IndexScanDirection::Reverse,
            0,
            usize::MAX,
        )
        .unwrap(),
        (0_u64..256).rev().collect::<Vec<_>>()
    );
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn in_memory_hot_exact_and_ordered_keys_stream_as_bounded_fragments() {
    let staging = tempfile::tempdir().unwrap();
    let source = (0_u64..512).map(|ordinal| {
        Ok(SourceRow::new(
            10_000 + ordinal,
            vec![Value::integer(ordinal as i64), Value::text("one-key")],
        ))
    });
    let request = request_with_shape_and_index_keys(
        staging.path(),
        false,
        512,
        1,
        ExactPageBuildLimits::new(1_024, 128).unwrap(),
        OrderedPageBuildLimits::new(1_024, 128).unwrap(),
        FanoutBuildLimits::new(1_024, 1_024, 1024 * 1024, 128).unwrap(),
        text_index_key(),
        text_index_key(),
    );
    let built = build_artifact_pair(&request, source).unwrap();
    let index = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    for accelerator in index.accelerators() {
        let section = index.sections()[accelerator.first_section_index() as usize + 1];
        assert!(section.page_count() > 1);
    }
    let expected = (0_u64..512).collect::<Vec<_>>();
    assert_eq!(
        lookup_exact_index(
            built.index_bytes(),
            &index,
            built.data_layout(),
            object_id(0x41),
            &[Value::text("one-key")],
        )
        .unwrap(),
        Some(expected.clone())
    );
    let ordered = index
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == object_id(0x42))
        .unwrap();
    let bound = OrderedIndexBound::new(
        OrderedIndexKey::from_values(
            built.data_layout(),
            ordered.key_columns(),
            &[Value::text("one-key")],
        )
        .unwrap(),
        true,
    );
    assert_eq!(
        scan_ordered_index(
            built.index_bytes(),
            &index,
            built.data_layout(),
            object_id(0x42),
            Some(&bound),
            Some(&bound),
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap(),
        expected
    );
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn hot_exact_posting_corruption_falls_back_and_rebuilds_the_complete_chain() {
    let source_staging = tempfile::tempdir().unwrap();
    let row_count = 256_u64;
    let request = request_with_shape_and_index_keys(
        source_staging.path(),
        false,
        row_count,
        128,
        ExactPageBuildLimits::new(1_024, 128).unwrap(),
        OrderedPageBuildLimits::new(1_024, 128).unwrap(),
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        text_index_key(),
        text_index_key(),
    );
    let built = build_artifact_pair(
        &request,
        (0..row_count).map(|ordinal| {
            Ok(SourceRow::new(
                20_000 + ordinal,
                vec![Value::integer(ordinal as i64), Value::text("one-key")],
            ))
        }),
    )
    .unwrap();
    let layout = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    let accelerator = layout
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == object_id(0x41))
        .unwrap();
    let section_index = accelerator.first_section_index() as usize + 1;
    let fragmented_pages = layout
        .pages()
        .iter()
        .filter(|page| page.section_index() as usize == section_index)
        .copied()
        .collect::<Vec<_>>();
    assert!(fragmented_pages.len() > 2);
    let mut corrupt = built.index_bytes().to_vec();
    corrupt[fragmented_pages[1].offset() as usize] ^= 0x80;

    let data_source = TracedSource::new(built.data_bytes());
    let index_source = TracedSource::new(&corrupt);
    let mut rebuilds = RecordingRebuilds::new(RebuildRequestStatus::Enqueued);
    let mut selected = Vec::new();
    let report = visit_exact_index_or_scan(
        &data_source,
        built.data_layout(),
        &exact_lookup_definition(),
        &[Value::text("one-key")],
        IndexAccessState::Available {
            source: &index_source,
            layout: &layout,
        },
        &mut rebuilds,
        &mut |row| {
            selected.push(row);
            Ok(())
        },
    )
    .unwrap();
    let expected = (0..row_count).collect::<Vec<_>>();
    assert_eq!(selected, expected);
    assert_eq!(report.path(), ExactLookupPath::DataScan);
    assert_eq!(
        report.rebuild_status(),
        Some(RebuildRequestStatus::Enqueued)
    );
    assert_eq!(rebuilds.requests.len(), 1);

    let rebuild_staging = tempfile::tempdir().unwrap();
    let replacement = IndexReplacementBuildRequest::new(
        rebuilds.requests.remove(0),
        ArtifactId::from_bytes(raw(0x91)).unwrap(),
        DatabaseGeneration::new(18).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        vec![AcceleratorBuildSpec::exact(
            object_id(0x41),
            false,
            false,
            [0x51; 32],
            text_index_key(),
            IndexPageCodec::Lz4,
            ExactPageBuildLimits::new(1_024, 128).unwrap(),
        )
        .unwrap()],
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        rebuild_staging.path(),
    )
    .unwrap();
    let mut output = std::io::Cursor::new(Vec::new());
    let written = write_index_replacement(
        &replacement,
        built.data_bytes(),
        built.data_layout(),
        &mut output,
    )
    .unwrap();
    let replacement_bytes = output.into_inner();
    let replacement_layout =
        decode_index_artifact_layout(&replacement_bytes, written.reference(), built.data_layout())
            .unwrap();
    assert_eq!(
        lookup_exact_index(
            &replacement_bytes,
            &replacement_layout,
            built.data_layout(),
            object_id(0x41),
            &[Value::text("one-key")],
        )
        .unwrap(),
        Some(expected)
    );
    assert!(std::fs::read_dir(source_staging.path())
        .unwrap()
        .next()
        .is_none());
    assert!(std::fs::read_dir(rebuild_staging.path())
        .unwrap()
        .next()
        .is_none());
}

#[test]
fn low_memory_bool_status_and_tenant_skew_remains_queryable() {
    let staging = tempfile::tempdir().unwrap();
    let row_count = 4_096_u64;
    let hot_rows = row_count * 95 / 100;
    let boolean = DataColumnSpec::new(
        object_id(0xa1),
        CatalogDataType::scalar(DataType::Boolean).unwrap(),
        false,
    );
    let status = DataColumnSpec::new(
        object_id(0xa2),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        false,
    );
    let tenant = DataColumnSpec::new(
        object_id(0xa3),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let key = |column: DataColumnSpec| {
        vec![IndexKeyColumn::new(
            column.column_id(),
            column.data_type().logical_type(),
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )]
    };
    let exact_limits = ExactPageBuildLimits::new(1_024, 256).unwrap();
    let accelerators = vec![
        AcceleratorBuildSpec::exact(
            object_id(0xb1),
            false,
            false,
            [0xb1; 32],
            key(boolean),
            IndexPageCodec::Lz4,
            exact_limits,
        )
        .unwrap(),
        AcceleratorBuildSpec::ordered(
            object_id(0xb2),
            false,
            false,
            [0xb2; 32],
            key(status),
            IndexPageCodec::Lz4,
            OrderedPageBuildLimits::new(1_024, 256).unwrap(),
        )
        .unwrap(),
        AcceleratorBuildSpec::exact(
            object_id(0xb3),
            false,
            false,
            [0xb3; 32],
            key(tenant),
            IndexPageCodec::Lz4,
            exact_limits,
        )
        .unwrap(),
    ];
    let limits = FanoutBuildLimits::new(64, 64, 1024 * 1024, 512)
        .unwrap()
        .with_resource_budgets(4 * 1024 * 1024, 8 * 1024 * 1024)
        .unwrap();
    let request = ArtifactPairBuildRequest::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes(raw(0xc1)).unwrap(),
            DatabaseId::from_bytes(raw(0xc2)).unwrap(),
            object_id(0xc3),
            SegmentId::from_bytes(raw(0xc4)).unwrap(),
            DatabaseGeneration::new(17).unwrap(),
            CatalogGeneration::new(13).unwrap(),
            501,
            509,
            row_count,
            3,
            64,
            SegmentKind::Rows,
            123_456,
        )
        .unwrap(),
        vec![boolean, status, tenant],
        vec![ColumnBuildPolicy::default(); 3],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes(raw(0xc5)).unwrap(),
        accelerators,
        limits,
        staging.path(),
    )
    .unwrap();
    let built = build_artifact_pair(
        &request,
        (0..row_count).map(|row| {
            Ok(SourceRow::new(
                30_000 + row,
                vec![
                    Value::boolean(row < hot_rows),
                    Value::text(if row < hot_rows { "active" } else { "inactive" }),
                    Value::integer(42),
                ],
            ))
        }),
    )
    .unwrap();
    let index = decode_index_artifact_layout(
        built.index_bytes(),
        built.index_reference(),
        built.data_layout(),
    )
    .unwrap();
    assert_eq!(index.accelerators().len(), 3);
    for accelerator in index.accelerators() {
        let pages = index.sections()[accelerator.first_section_index() as usize + 1].page_count();
        assert!(pages > 1, "skewed accelerator must use bounded pages");
    }
    assert_eq!(
        lookup_exact_index(
            built.index_bytes(),
            &index,
            built.data_layout(),
            object_id(0xb1),
            &[Value::boolean(true)],
        )
        .unwrap()
        .unwrap()
        .len() as u64,
        hot_rows
    );
    assert_eq!(
        lookup_exact_index(
            built.index_bytes(),
            &index,
            built.data_layout(),
            object_id(0xb3),
            &[Value::integer(42)],
        )
        .unwrap(),
        Some((0..row_count).collect())
    );
    let ordered = index
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == object_id(0xb2))
        .unwrap();
    let active = OrderedIndexBound::new(
        OrderedIndexKey::from_values(
            built.data_layout(),
            ordered.key_columns(),
            &[Value::text("active")],
        )
        .unwrap(),
        true,
    );
    assert_eq!(
        scan_ordered_index(
            built.index_bytes(),
            &index,
            built.data_layout(),
            object_id(0xb2),
            Some(&active),
            Some(&active),
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .unwrap()
        .len() as u64,
        hot_rows
    );
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn streaming_writer_rejects_nonempty_destination_before_source_consumption() {
    let staging = tempfile::tempdir().unwrap();
    let visits = Rc::new(Cell::new(0));
    let source = CountingRows {
        rows: rows().into_iter(),
        visits: Rc::clone(&visits),
    };
    let mut data_output = std::io::Cursor::new(vec![0xAA]);
    let mut index_output = std::io::Cursor::new(Vec::new());
    let error = write_artifact_pair(
        &request(staging.path(), false),
        source,
        &mut data_output,
        &mut index_output,
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            FormatError::InvalidDataArtifact {
                detail: "streaming data destination is not empty"
            }
        ),
        "unexpected error: {error:?}"
    );
    assert_eq!(visits.get(), 0);
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[cfg(feature = "test-failpoints")]
#[test]
fn streaming_writers_expose_exact_body_and_footer_boundaries() {
    for point in [
        GenerationCrashPoint::DataAfterBodyBeforeFooter,
        GenerationCrashPoint::DataAfterFooterBeforeSync,
        GenerationCrashPoint::IndexAfterBodyBeforeFooter,
        GenerationCrashPoint::IndexAfterFooterBeforeSync,
    ] {
        let staging = tempfile::tempdir().unwrap();
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        let result =
            build_artifact_pair(&request(staging.path(), false), rows().into_iter().map(Ok));
        assert!(
            result.is_err(),
            "{} must interrupt the writer",
            point.name()
        );
        assert_eq!(guard.hit_count(), 1, "{} was not traversed", point.name());
    }
}
