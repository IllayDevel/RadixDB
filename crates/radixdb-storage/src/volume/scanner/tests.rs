use super::super::writer::VolumeBuilder;
use super::*;
use crate::expression::{
    AndExpr, BetweenExpr, ComparisonExpr, Expression, InListExpr, LikeExpr, NullCheckExpr, OrExpr,
    RangeExpr,
};
use crate::traits::VecScanner;
use crate::volume::column::ColumnData;
use crate::volume::writer::{ColSource, ColumnMapping};
use radixdb_core::{DataType, SchemaBuilder};
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn persist_test_artifact(
    root: &std::path::Path,
    segment_number: u64,
    eager: &FrozenVolume,
) -> Arc<FrozenVolume> {
    crate::volume::test_artifact::persist_eager_volume(root, segment_number, eager).volume
}

fn persist_test_artifact_with_group_rows(
    root: &std::path::Path,
    segment_number: u64,
    eager: &FrozenVolume,
    row_group_rows: u32,
) -> Arc<FrozenVolume> {
    crate::volume::test_artifact::persist_eager_volume_with_group_rows(
        root,
        segment_number,
        eager,
        row_group_rows,
    )
    .volume
}

fn make_test_volume() -> Arc<FrozenVolume> {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();

    let mut builder = VolumeBuilder::with_capacity(&schema, 5);
    builder.add_row(
        1,
        &Row::from_values(vec![
            Value::Integer(1),
            Value::text("apple"),
            Value::Float(1.50),
        ]),
    );
    builder.add_row(
        2,
        &Row::from_values(vec![
            Value::Integer(2),
            Value::text("banana"),
            Value::Float(0.75),
        ]),
    );
    builder.add_row(
        3,
        &Row::from_values(vec![
            Value::Integer(3),
            Value::text("cherry"),
            Value::Float(3.00),
        ]),
    );
    builder.add_row(
        4,
        &Row::from_values(vec![
            Value::Integer(4),
            Value::text("date"),
            Value::Float(5.00),
        ]),
    );
    builder.add_row(
        5,
        &Row::from_values(vec![
            Value::Integer(5),
            Value::text("elderberry"),
            Value::Float(8.00),
        ]),
    );
    Arc::new(builder.finish())
}

#[test]
fn empty_skip_sets_leave_single_cold_scan_on_no_overlay_fast_path() {
    let mut scanner = VolumeScanner::new(make_test_volume(), vec![0], None);
    scanner.set_skip_sets(
        Arc::new(rustc_hash::FxHashMap::default()),
        Arc::new(rustc_hash::FxHashSet::default()),
    );

    assert!(scanner.committed_tombstones.is_none());
    assert!(scanner.pending_cold_deletes.is_none());
    assert!(!scanner.has_row_skip_overlay);
    assert!(!scanner.should_skip_row(0));

    let mut tombstones = rustc_hash::FxHashMap::default();
    tombstones.insert(1, 1);
    scanner.set_skip_sets(
        Arc::new(tombstones),
        Arc::new(rustc_hash::FxHashSet::default()),
    );

    assert!(scanner.committed_tombstones.is_some());
    assert!(scanner.has_row_skip_overlay);
    assert!(scanner.should_skip_row(0));
    assert!(!scanner.should_skip_row(1));
}

#[derive(Debug, Clone)]
struct CountingGtIdExpr {
    calls: Arc<AtomicUsize>,
    target: Value,
    col_index: Option<usize>,
}

impl CountingGtIdExpr {
    fn new(calls: Arc<AtomicUsize>, target: i64) -> Self {
        Self {
            calls,
            target: Value::Integer(target),
            col_index: None,
        }
    }
}

impl Expression for CountingGtIdExpr {
    fn evaluate(&self, row: &Row) -> Result<bool> {
        Ok(self.evaluate_fast(row))
    }

    fn evaluate_fast(&self, row: &Row) -> bool {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let Some(col_index) = self.col_index else {
            return false;
        };
        let Some(Value::Integer(value)) = row.get(col_index) else {
            return false;
        };
        let Value::Integer(target) = self.target else {
            return false;
        };
        *value > target
    }

    fn with_aliases(
        &self,
        _aliases: &rustc_hash::FxHashMap<String, String>,
    ) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn prepare_for_schema(&mut self, schema: &radixdb_core::Schema) {
        self.col_index = schema.column_index_map().get("id").copied();
    }

    fn is_prepared(&self) -> bool {
        self.col_index.is_some()
    }

    fn get_column_name(&self) -> Option<&str> {
        Some("id")
    }

    fn collect_column_indices(&self, out: &mut Vec<usize>) -> bool {
        if let Some(col_index) = self.col_index {
            out.push(col_index);
            true
        } else {
            false
        }
    }

    fn get_comparison_info(&self) -> Option<(&str, radixdb_core::Operator, &Value)> {
        Some(("id", radixdb_core::Operator::Gt, &self.target))
    }

    fn is_conjunctive_simple(&self) -> bool {
        true
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
struct CountingTrueExpr {
    calls: Arc<AtomicUsize>,
}

impl CountingTrueExpr {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self { calls }
    }
}

impl Expression for CountingTrueExpr {
    fn evaluate(&self, row: &Row) -> Result<bool> {
        Ok(self.evaluate_fast(row))
    }

    fn evaluate_fast(&self, _row: &Row) -> bool {
        self.calls.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn with_aliases(
        &self,
        _aliases: &rustc_hash::FxHashMap<String, String>,
    ) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn prepare_for_schema(&mut self, _schema: &radixdb_core::Schema) {}

    fn is_prepared(&self) -> bool {
        true
    }

    fn collect_column_indices(&self, _out: &mut Vec<usize>) -> bool {
        true
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[test]
fn test_full_scan() {
    let vol = make_test_volume();
    let mut scanner = VolumeScanner::new(vol, vec![], None);

    let mut count = 0;
    while scanner.next() {
        let row = scanner.row();
        assert_eq!(row.len(), 3);
        count += 1;
    }
    assert_eq!(count, 5);
    assert!(scanner.err().is_none());
}

#[test]
fn test_projected_scan() {
    let vol = make_test_volume();
    // Only scan name and price (columns 1, 2)
    let mut scanner = VolumeScanner::new(vol, vec![1, 2], None);

    assert!(scanner.next());
    let row = scanner.row();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get(0), Some(&Value::text("apple")));
    assert_eq!(row.get(1), Some(&Value::Float(1.50)));
}

#[test]
fn v2_r3_public_scanner_admission_rejects_invalid_range_and_projection() {
    let volume = make_test_volume();
    assert!(VolumeScanner::try_new(Arc::clone(&volume), vec![3], None).is_err());
    assert!(VolumeScanner::try_with_range(Arc::clone(&volume), vec![0], 4, 3, None,).is_err());
    assert!(
        VolumeScanner::try_with_range_exact_projection(Arc::clone(&volume), vec![], 0, 6,).is_err()
    );

    let scanner = VolumeScanner::try_with_range(volume, vec![2, 0], 1, 4, None).unwrap();
    assert_eq!(scanner.estimated_count(), Some(3));
}

#[test]
fn test_projected_scan_preserves_duplicate_indices() {
    let vol = make_test_volume();
    let mut scanner = VolumeScanner::new(vol, vec![1, 1, 0], None);

    assert!(scanner.next());
    let row = scanner.row();
    assert_eq!(row.len(), 3);
    assert_eq!(row.get(0), Some(&Value::text("apple")));
    assert_eq!(row.get(1), Some(&Value::text("apple")));
    assert_eq!(row.get(2), Some(&Value::Integer(1)));
}

#[test]
fn test_exact_empty_projection_returns_zero_width_rows() {
    let vol = make_test_volume();
    let mut scanner = VolumeScanner::new_exact_projection(vol, vec![]);

    assert!(scanner.next());
    assert_eq!(scanner.row().len(), 0);
    assert!(scanner.err().is_none());
}

#[test]
fn artifact_empty_projection_default_is_honest_full_row_select_star() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 8, &eager);
    assert!(
        metadata_only.artifact_source().is_some(),
        "test must exercise artifact-backed group cache"
    );

    let mut scanner = VolumeScanner::new(metadata_only, vec![], None);

    assert!(scanner.next());
    assert_eq!(scanner.row().len(), 3);
    assert_eq!(scanner.row().get(0), Some(&Value::Integer(1)));
    assert_eq!(scanner.row().get(1), Some(&Value::text("apple")));
    assert_eq!(scanner.row().get(2), Some(&Value::Float(1.50)));

    let cache = scanner
        .group_cache
        .as_ref()
        .expect("artifact SELECT * scanner must load row-group cache");
    assert!(
        cache.columns.iter().all(|column| column.is_some()),
        "SELECT * must remain the explicit full-materialization path"
    );
}

#[test]
fn artifact_exact_empty_projection_loads_no_value_columns() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 9, &eager);
    assert!(
        metadata_only.artifact_source().is_some(),
        "test must exercise artifact-backed group cache"
    );

    let mut scanner = VolumeScanner::new_exact_projection(Arc::clone(&metadata_only), vec![]);

    assert!(scanner.next());
    assert_eq!(scanner.row().len(), 0);

    let cache = scanner
        .group_cache
        .as_ref()
        .expect("artifact exact empty projection scanner must keep group boundary cache");
    assert!(
        cache.columns.iter().all(|column| column.is_none()),
        "exact empty projection must not read value column blocks"
    );
}

#[test]
fn artifact_exact_empty_projection_bulk_collects_integer_equality_row_ids() {
    let schema = SchemaBuilder::new("bulk_row_ids")
        .column("id", DataType::Integer, false, true)
        .column("bucket", DataType::Integer, true, false)
        .build();
    let mut builder = VolumeBuilder::with_capacity(&schema, 6);
    for (id, bucket) in [
        (1, Value::Integer(2)),
        (2, Value::Integer(1)),
        (3, Value::Null(DataType::Integer)),
        (4, Value::Integer(2)),
        (5, Value::Integer(3)),
        (6, Value::Integer(2)),
    ] {
        builder.add_row(id, &Row::from_values(vec![Value::Integer(id), bucket]));
    }
    let eager = Arc::new(builder.finish());
    let dir = tempfile::tempdir().unwrap();
    let metadata_only = persist_test_artifact(dir.path(), 912, &eager);
    let mut filter = ComparisonExpr::eq("bucket", Value::Integer(2));
    filter.prepare_for_schema(&schema);
    let mut scanner = VolumeScanner::new_exact_projection(Arc::clone(&metadata_only), vec![]);
    scanner.set_filter(Box::new(filter));
    scanner.set_skip_sets(
        Arc::new(rustc_hash::FxHashMap::default()),
        Arc::new(rustc_hash::FxHashSet::from_iter([4])),
    );

    let mut row_ids = Vec::new();
    assert!(scanner.collect_remaining_row_ids(&mut row_ids).unwrap());
    assert_eq!(row_ids, vec![1, 6]);
    assert!(!scanner.next(), "bulk collection must consume the scanner");

    let mut range_filter = ComparisonExpr::gt("bucket", Value::Integer(1));
    range_filter.prepare_for_schema(&schema);
    let mut fallback = VolumeScanner::new_exact_projection(metadata_only, vec![]);
    fallback.set_filter(Box::new(range_filter));
    let mut untouched = Vec::new();
    assert!(
        !fallback.collect_remaining_row_ids(&mut untouched).unwrap(),
        "unsupported predicate must preserve the row-scanner fallback"
    );
    assert!(untouched.is_empty());
    let mut fallback_ids = Vec::new();
    while fallback.next() {
        fallback_ids.push(fallback.current_row_id().unwrap());
    }
    assert_eq!(fallback_ids, vec![1, 4, 5, 6]);
}

#[test]
fn artifact_scanner_emits_typed_batches_without_row_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 91, &eager);
    let mut scanner = VolumeScanner::new(metadata_only, vec![0, 2], None);

    assert!(scanner.supports_typed_batches());
    crate::instrumentation::begin_row_materialization_probe();
    let batch = scanner
        .next_typed_batch()
        .expect("read typed artifact batch")
        .expect("one DATA row group");
    let materialization = crate::instrumentation::end_row_materialization_probe();
    assert_eq!(batch.row_count(), 5);
    assert_eq!(batch.columns().len(), 2);
    assert!(matches!(
        &batch.columns()[0],
        crate::volume::column::ColumnData::Int64 { values, nulls }
            if values == &vec![1, 2, 3, 4, 5] && nulls.iter().all(|is_null| !is_null)
    ));
    assert!(matches!(
        &batch.columns()[1],
        crate::volume::column::ColumnData::Float64 { values, nulls }
            if values == &vec![1.5, 0.75, 3.0, 5.0, 8.0]
                && nulls.iter().all(|is_null| !is_null)
    ));
    assert_eq!(
        materialization.rows, 0,
        "typed artifact batch must not construct rows"
    );
    assert!(scanner.next_typed_batch().unwrap().is_none());
}

#[test]
fn artifact_typed_batch_matches_row_scanner_for_supported_types_and_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let schema = SchemaBuilder::new("typed_matrix")
        .column("id", DataType::Integer, true, false)
        .column("amount", DataType::Float, true, false)
        .column("name", DataType::Text, true, false)
        .column("active", DataType::Boolean, true, false)
        .column("created_at", DataType::Timestamp, true, false)
        .build();
    let ts1 = chrono::TimeZone::timestamp_opt(&chrono::Utc, 1_700_000_000, 123)
        .single()
        .unwrap();
    let ts2 = chrono::TimeZone::timestamp_opt(&chrono::Utc, 1_700_000_001, 456)
        .single()
        .unwrap();
    let expected = vec![
        vec![
            Value::Integer(1),
            Value::Float(10.5),
            Value::text("alpha"),
            Value::Boolean(true),
            Value::Timestamp(ts1),
        ],
        vec![
            Value::Integer(2),
            Value::Null(DataType::Float),
            Value::text("beta"),
            Value::Boolean(false),
            Value::Null(DataType::Timestamp),
        ],
        vec![
            Value::Null(DataType::Integer),
            Value::Float(-7.25),
            Value::Null(DataType::Text),
            Value::Null(DataType::Boolean),
            Value::Timestamp(ts2),
        ],
        vec![
            Value::Integer(4),
            Value::Float(0.0),
            Value::text("alpha"),
            Value::Boolean(true),
            Value::Timestamp(ts1),
        ],
    ];

    let mut builder = VolumeBuilder::with_capacity(&schema, expected.len());
    for (row_id, values) in expected.iter().enumerate() {
        builder.add_row(row_id as i64 + 1, &Row::from_values(values.clone()));
    }
    let eager = builder.finish();
    let metadata_only = persist_test_artifact(dir.path(), 93, &eager);

    let projection = vec![0, 1, 2, 3, 4];
    let mut typed_scanner =
        VolumeScanner::new(Arc::clone(&metadata_only), projection.clone(), None);
    assert!(
        typed_scanner.supports_typed_batches(),
        "clean artifact identity projection should advertise typed batches"
    );

    crate::instrumentation::begin_row_materialization_probe();
    let batch = typed_scanner
        .next_typed_batch()
        .expect("read typed batch")
        .expect("one row group");
    let materialization = crate::instrumentation::end_row_materialization_probe();

    assert_eq!(batch.row_count(), expected.len());
    assert_eq!(batch.columns().len(), projection.len());
    assert_eq!(
        materialization.rows, 0,
        "typed artifact batch must not construct row objects"
    );
    assert!(
        typed_scanner.next_typed_batch().unwrap().is_none(),
        "single small volume should emit one typed batch"
    );
    assert!(typed_scanner.err().is_none());

    for (column_idx, column) in batch.columns().iter().enumerate() {
        assert_eq!(column.data_type(), schema.columns[column_idx].data_type);
        assert_eq!(column.len(), expected.len());
        for (row_idx, expected_row) in expected.iter().enumerate() {
            assert_eq!(
                column.get_value(row_idx),
                expected_row[column_idx],
                "typed value mismatch at row {row_idx}, column {column_idx}"
            );
        }
    }

    let mut row_scanner = VolumeScanner::new(metadata_only, projection, None);
    let mut rows: Vec<Vec<Value>> = Vec::new();
    while row_scanner.next() {
        let row = row_scanner.row();
        rows.push(
            (0..row.len())
                .map(|idx| row.get(idx).unwrap().clone())
                .collect(),
        );
    }
    assert!(row_scanner.err().is_none());
    assert_eq!(
        rows, expected,
        "row scanner and typed batch must expose identical logical values"
    );
}

#[test]
fn artifact_typed_batch_supports_reordered_identity_projection() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 94, &eager);

    let mut typed_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![2, 0], None);
    assert!(
        typed_scanner.supports_typed_batches(),
        "reordered physical identity projection is a real typed-batch contract, not a row fallback"
    );

    crate::instrumentation::begin_row_materialization_probe();
    let batch = typed_scanner
        .next_typed_batch()
        .expect("read reordered typed batch")
        .expect("one row group");
    let materialization = crate::instrumentation::end_row_materialization_probe();

    assert_eq!(materialization.rows, 0);
    assert_eq!(batch.row_count(), 5);
    assert_eq!(batch.columns().len(), 2);
    assert_eq!(batch.columns()[0].data_type(), DataType::Float);
    assert_eq!(batch.columns()[1].data_type(), DataType::Integer);
    assert_eq!(batch.columns()[0].get_value(0), Value::Float(1.50));
    assert_eq!(batch.columns()[1].get_value(0), Value::Integer(1));
    assert_eq!(batch.columns()[0].get_value(4), Value::Float(8.00));
    assert_eq!(batch.columns()[1].get_value(4), Value::Integer(5));
    assert!(typed_scanner.next_typed_batch().unwrap().is_none());

    let mut row_scanner = VolumeScanner::new(metadata_only, vec![2, 0], None);
    assert!(row_scanner.next());
    assert_eq!(row_scanner.row().get(0), Some(&Value::Float(1.50)));
    assert_eq!(row_scanner.row().get(1), Some(&Value::Integer(1)));
}

#[test]
fn artifact_typed_batch_supports_partial_range_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let schema = SchemaBuilder::new("typed_aligned_range")
        .column("id", DataType::Integer, false, true)
        .column("kind", DataType::Text, false, false)
        .build();
    let group_size = crate::volume::column::ROW_GROUP_SIZE;
    let row_count = group_size * 2 + 3;
    let mut builder = VolumeBuilder::with_capacity(&schema, row_count);
    for idx in 0..row_count {
        let id = idx as i64 + 1;
        let kind = if id % 2 == 0 { "even" } else { "odd" };
        builder.add_row(
            id,
            &Row::from_values(vec![Value::Integer(id), Value::text(kind)]),
        );
    }
    let eager = builder.finish();
    let metadata_only = persist_test_artifact_with_group_rows(
        dir.path(),
        96,
        &eager,
        u32::try_from(group_size).unwrap(),
    );

    let range_start = group_size - 2;
    let range_end = group_size + 3;
    let mut typed_scanner = VolumeScanner::with_range(
        Arc::clone(&metadata_only),
        vec![0, 1],
        range_start,
        range_end,
        None,
    );
    assert!(
        typed_scanner.supports_typed_batches(),
        "immutable artifact constructor ranges have an exact typed-batch contract"
    );
    crate::instrumentation::begin_row_materialization_probe();
    let first = typed_scanner
        .next_typed_batch()
        .expect("read first partial range typed batch")
        .expect("first partial row group");
    let second = typed_scanner
        .next_typed_batch()
        .expect("read second partial range typed batch")
        .expect("second partial row group");
    let materialization = crate::instrumentation::end_row_materialization_probe();
    assert_eq!(first.row_count(), 2);
    assert_eq!(second.row_count(), 3);
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
    match &first.columns()[0] {
        ColumnData::Int64 { values, nulls } => {
            assert_eq!(values, &[group_size as i64 - 1, group_size as i64]);
            assert!(nulls.iter().all(|is_null| !*is_null));
        }
        _ => panic!("id must stay an Int64 typed column"),
    }
    match &first.columns()[1] {
        ColumnData::Dictionary {
            ids,
            dictionary,
            nulls,
        } => {
            assert_eq!(ids.len(), 2);
            assert!(nulls.iter().all(|is_null| !*is_null));
            assert_eq!(dictionary[ids[0] as usize].as_str(), "odd");
            assert_eq!(dictionary[*ids.last().unwrap() as usize].as_str(), "even");
        }
        _ => panic!("kind must stay a dictionary typed column"),
    }
    match &second.columns()[0] {
        ColumnData::Int64 { values, nulls } => {
            assert_eq!(
                values,
                &[
                    group_size as i64 + 1,
                    group_size as i64 + 2,
                    group_size as i64 + 3
                ]
            );
            assert!(nulls.iter().all(|is_null| !*is_null));
        }
        _ => panic!("second id batch must stay an Int64 typed column"),
    }
    assert!(typed_scanner.next_typed_batch().unwrap().is_none());

    let mut row_scanner =
        VolumeScanner::with_range(metadata_only, vec![0, 1], range_start, range_end, None);
    assert_eq!(row_scanner.estimated_count(), Some(5));
    assert!(row_scanner.next());
    assert_eq!(
        row_scanner.row().get(0),
        Some(&Value::Integer(group_size as i64 - 1))
    );
    assert_eq!(row_scanner.row().get(1), Some(&Value::text("odd")));
}

#[test]
fn artifact_typed_batches_support_schema_mapping_and_defaults_after_row_iteration_guard() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 92, &eager);

    let mut row_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![0], None);
    assert!(row_scanner.supports_typed_batches());
    assert!(row_scanner.next());
    assert!(
        !row_scanner.supports_typed_batches(),
        "mixed row and typed iteration would lose scanner ordering"
    );
    assert_eq!(
        row_scanner.typed_batch_fallback_reason(),
        Some(TypedBatchFallbackReason::RowAlreadyFetched)
    );
    let _ = row_scanner.take_row();
    assert!(
        !row_scanner.supports_typed_batches(),
        "typed iteration must not resume after a row was consumed"
    );
    assert_eq!(
        row_scanner.typed_batch_fallback_reason(),
        Some(TypedBatchFallbackReason::RowIterationStarted)
    );

    let mut mapped_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![0, 1], None);
    let mapped_schema = SchemaBuilder::new("mapped")
        .column("price", DataType::Float, false, false)
        .column("id", DataType::Integer, false, false)
        .column("name", DataType::Text, false, false)
        .build();
    mapped_scanner
        .set_column_mapping(
            ColumnMapping::try_new(
                &mapped_schema,
                &metadata_only,
                vec![
                    ColSource::Volume(2),
                    ColSource::Volume(0),
                    ColSource::Volume(1),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    assert!(
        mapped_scanner.supports_typed_batches(),
        "schema mappings over supported physical columns have an explicit typed contract"
    );
    let batch = mapped_scanner
        .next_typed_batch()
        .expect("mapped typed batch succeeds")
        .expect("mapped batch");
    assert_eq!(batch.row_count(), 5);
    assert_eq!(batch.columns().len(), 2);
    assert_eq!(batch.columns()[0].data_type(), DataType::Float);
    assert_eq!(batch.columns()[1].data_type(), DataType::Integer);
    assert_eq!(batch.columns()[0].get_value(0), Value::Float(1.50));
    assert_eq!(batch.columns()[1].get_value(0), Value::Integer(1));

    let mut default_mapped_scanner =
        VolumeScanner::new(Arc::clone(&metadata_only), vec![3, 0], None);
    let default_schema = SchemaBuilder::new("default_mapped")
        .column("id", DataType::Integer, false, false)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .column("status", DataType::Text, false, false)
        .build();
    default_mapped_scanner
        .set_column_mapping(
            ColumnMapping::try_new(
                &default_schema,
                &metadata_only,
                vec![
                    ColSource::Volume(0),
                    ColSource::Volume(1),
                    ColSource::Volume(2),
                    ColSource::Default(Value::text("default-status")),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    assert!(
        default_mapped_scanner.supports_typed_batches(),
        "supported schema defaults should be synthesized as typed columns"
    );
    let batch = default_mapped_scanner
        .next_typed_batch()
        .expect("default typed batch succeeds")
        .expect("default batch");
    assert_eq!(batch.row_count(), 5);
    assert_eq!(batch.columns().len(), 2);
    match &batch.columns()[0] {
        ColumnData::Dictionary {
            ids,
            dictionary,
            nulls,
        } => {
            assert_eq!(ids, &[0, 0, 0, 0, 0]);
            assert_eq!(dictionary.len(), 1);
            assert_eq!(dictionary[0].as_str(), "default-status");
            assert_eq!(nulls, &[false, false, false, false, false]);
        }
        _ => panic!("default status must be a dictionary typed column"),
    }
    assert_eq!(batch.columns()[1].get_value(0), Value::Integer(1));

    let mut unsupported_default_scanner = VolumeScanner::new(metadata_only, vec![3], None);
    let unsupported_schema = SchemaBuilder::new("unsupported_default")
        .column("id", DataType::Integer, false, false)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .column("external_id", DataType::Uuid, false, false)
        .build();
    unsupported_default_scanner
        .set_column_mapping(
            ColumnMapping::try_new(
                &unsupported_schema,
                &unsupported_default_scanner.volume,
                vec![
                    ColSource::Volume(0),
                    ColSource::Volume(1),
                    ColSource::Volume(2),
                    ColSource::Default(Value::uuid([7; 16])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    assert!(
        !unsupported_default_scanner.supports_typed_batches(),
        "unsupported default types must remain an explicit row fallback"
    );
    assert_eq!(
        unsupported_default_scanner.typed_batch_fallback_reason(),
        Some(TypedBatchFallbackReason::UnsupportedSchemaDefault)
    );
    assert!(unsupported_default_scanner.next_typed_batch().is_err());
}

#[test]
fn test_merging_scanner_reports_mixed_typed_and_row_sources_as_explicit_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 93, &eager);

    let cold = VolumeScanner::new(Arc::clone(&metadata_only), vec![0, 1], None);
    assert!(
        cold.supports_typed_batches(),
        "the cold source should be typed-capable by itself"
    );
    let hot = VecScanner::new(vec![Row::from_values(vec![
        Value::Integer(99),
        Value::text("hot"),
    ])]);

    let mut merged = MergingScanner::new(vec![Box::new(cold), Box::new(hot)]);
    assert!(
        !merged.supports_typed_batches(),
        "mixed typed/row sources must not silently use ColumnBatch"
    );
    assert_eq!(
        merged.typed_batch_fallback_reason(),
        Some(TypedBatchFallbackReason::MixedTypedAndRowSources)
    );
    assert!(
        merged.next_typed_batch().is_err(),
        "typed batch request over mixed sources must be rejected explicitly"
    );

    let mut seen = Vec::new();
    while merged.next() {
        seen.push((merged.row().get(0).cloned(), merged.row().get(1).cloned()));
    }
    assert!(merged.err().is_none());
    assert_eq!(seen.len(), 6);
    assert_eq!(
        seen[0],
        (Some(Value::Integer(1)), Some(Value::text("apple")))
    );
    assert_eq!(
        seen[5],
        (Some(Value::Integer(99)), Some(Value::text("hot")))
    );
}

#[test]
fn artifact_typed_batches_apply_visibility_tombstones_and_pending_deletes_columnar() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 95, &eager);

    fn read_projected_ids(mut scanner: VolumeScanner) -> Vec<i64> {
        assert!(
            scanner.supports_typed_batches(),
            "row-id visibility overlays should stay columnar"
        );
        crate::instrumentation::begin_row_materialization_probe();
        let batch = scanner
            .next_typed_batch()
            .expect("read overlay typed batch")
            .expect("visible overlay rows");
        let materialization = crate::instrumentation::end_row_materialization_probe();
        assert_eq!(materialization.rows, 0);
        assert_eq!(materialization.values, 0);
        assert!(scanner.next_typed_batch().unwrap().is_none());
        match &batch.columns()[0] {
            ColumnData::Int64 { values, nulls } => {
                assert!(nulls.iter().all(|is_null| !*is_null));
                values.clone()
            }
            _ => panic!("projected id must stay an Int64 typed column"),
        }
    }

    let mut visibility_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![0], None);
    visibility_scanner.set_visibility_bitmap(Some(Arc::new(vec![u64::MAX << 1])));
    assert_eq!(read_projected_ids(visibility_scanner), vec![2, 3, 4, 5]);

    let mut committed = rustc_hash::FxHashMap::default();
    committed.insert(1, 1);
    let mut tombstone_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![0], None);
    tombstone_scanner.set_skip_sets(
        Arc::new(committed),
        Arc::new(rustc_hash::FxHashSet::default()),
    );
    assert_eq!(read_projected_ids(tombstone_scanner), vec![2, 3, 4, 5]);

    let mut pending = rustc_hash::FxHashSet::default();
    pending.insert(2);
    let mut pending_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![0], None);
    pending_scanner.set_pending_cold_deletes(Arc::new(pending));
    assert_eq!(read_projected_ids(pending_scanner), vec![1, 3, 4, 5]);

    let mut all_committed = rustc_hash::FxHashMap::default();
    for rid in 1..=5 {
        all_committed.insert(rid, 1);
    }
    let mut all_skipped_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![0], None);
    all_skipped_scanner.set_skip_sets(
        Arc::new(all_committed),
        Arc::new(rustc_hash::FxHashSet::default()),
    );
    assert!(all_skipped_scanner.supports_typed_batches());
    crate::instrumentation::begin_row_materialization_probe();
    assert!(all_skipped_scanner.next_typed_batch().unwrap().is_none());
    let materialization = crate::instrumentation::end_row_materialization_probe();
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);

    let range_scanner = VolumeScanner::with_range(metadata_only, vec![0], 1, 4, None);
    assert!(
        range_scanner.supports_typed_batches(),
        "plain immutable constructor ranges are now handled by typed slicing"
    );
}

#[test]
fn artifact_typed_batches_fall_back_for_extension_scalar_types() {
    let dir = tempfile::tempdir().unwrap();
    let schema = SchemaBuilder::new("typed_extension_scalars")
        .column("id", DataType::Uuid, true, false)
        .column("amount", DataType::Decimal, true, false)
        .column("accounting_date", DataType::Date, true, false)
        .build();
    let expected = vec![
        vec![
            Value::uuid([1; 16]),
            Value::decimal(123_456, 12, 2),
            Value::date(20_001),
        ],
        vec![
            Value::Null(DataType::Uuid),
            Value::Null(DataType::Decimal),
            Value::Null(DataType::Date),
        ],
    ];

    let mut builder = VolumeBuilder::with_capacity(&schema, expected.len());
    for (row_id, values) in expected.iter().enumerate() {
        builder.add_row(row_id as i64 + 1, &Row::from_values(values.clone()));
    }
    let eager = builder.finish();
    let metadata_only = persist_test_artifact(dir.path(), 96, &eager);

    let mut typed_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![0, 1, 2], None);
    assert!(
        !typed_scanner.supports_typed_batches(),
        "UUID/Decimal/Date need an explicit typed-batch contract before bypassing the row adapter"
    );
    assert!(typed_scanner.next_typed_batch().is_err());

    let mut row_scanner = VolumeScanner::new(metadata_only, vec![0, 1, 2], None);
    let mut rows = Vec::new();
    while row_scanner.next() {
        let row = row_scanner.row();
        rows.push(
            (0..row.len())
                .map(|idx| row.get(idx).unwrap().clone())
                .collect::<Vec<_>>(),
        );
    }
    assert!(row_scanner.err().is_none());
    assert_eq!(rows, expected);
}

#[test]
fn uuid_filter_does_not_prune_non_null_extension_row_groups() {
    let schema = SchemaBuilder::new("uuid_row_group_pruning")
        .column("id", DataType::Uuid, false, true)
        .build();
    let row_count = crate::volume::column::ROW_GROUP_SIZE + 1;
    let mut builder = VolumeBuilder::with_capacity(&schema, row_count);
    for index in 0..row_count {
        let mut uuid = [0_u8; 16];
        uuid[8..].copy_from_slice(&(index as u64).to_be_bytes());
        builder.add_row(index as i64 + 1, &Row::from_values(vec![Value::uuid(uuid)]));
    }
    let volume = Arc::new(builder.finish());
    assert_eq!(volume.meta.row_groups.len(), 2);

    let target = Value::uuid([0; 16]);
    let mut filter = ComparisonExpr::eq("id", target.clone());
    filter.prepare_for_schema(&schema);
    let mut scanner = VolumeScanner::new(volume, vec![0], None);
    scanner.set_filter(Box::new(filter));

    let mut matches = Vec::new();
    while scanner.next() {
        matches.push(scanner.row().get(0).cloned().unwrap());
    }
    assert!(scanner.err().is_none());
    assert_eq!(matches, vec![target]);
}

#[test]
fn strict_comparisons_prune_row_groups_at_equal_bounds() {
    let schema = SchemaBuilder::new("strict_row_group_pruning")
        .column("id", DataType::Integer, false, true)
        .build();
    let mut builder = VolumeBuilder::with_capacity(&schema, 4);
    for value in [10_i64, 20, 30, 40] {
        builder.add_row(value, &Row::from_values(vec![Value::Integer(value)]));
    }
    let mut volume = builder.finish();
    Arc::make_mut(&mut volume.meta).row_groups = vec![
        crate::volume::column::RowGroupMeta {
            start_idx: 0,
            end_idx: 2,
            zone_maps: vec![crate::volume::column::ZoneMap {
                min: Value::Integer(10),
                max: Value::Integer(20),
                null_count: 0,
                row_count: 2,
            }],
        },
        crate::volume::column::RowGroupMeta {
            start_idx: 2,
            end_idx: 4,
            zone_maps: vec![crate::volume::column::ZoneMap {
                min: Value::Integer(30),
                max: Value::Integer(40),
                null_count: 0,
                row_count: 2,
            }],
        },
    ];
    let volume = Arc::new(volume);

    let mut greater_filter = ComparisonExpr::gt("id", Value::Integer(20));
    greater_filter.prepare_for_schema(&schema);
    let mut greater_scanner = VolumeScanner::new(Arc::clone(&volume), vec![0], None);
    greater_scanner.set_filter(Box::new(greater_filter));
    assert_eq!(
        greater_scanner.row_group_skips.as_deref(),
        Some(&[true, false][..])
    );

    let mut less_filter = ComparisonExpr::lt("id", Value::Integer(30));
    less_filter.prepare_for_schema(&schema);
    let mut less_scanner = VolumeScanner::new(volume, vec![0], None);
    less_scanner.set_filter(Box::new(less_filter));
    assert_eq!(
        less_scanner.row_group_skips.as_deref(),
        Some(&[false, true][..])
    );
}

#[test]
fn artifact_typed_batch_supports_bytes_and_json_columns() {
    let dir = tempfile::tempdir().unwrap();
    let schema = SchemaBuilder::new("typed_bytes_json")
        .column("payload", DataType::Bytes, true, false)
        .column("document", DataType::Json, true, false)
        .build();
    let expected = vec![
        vec![
            Value::bytes(vec![0, 1, 2, 3, 254, 255]),
            Value::json(r#"{"kind":"typed","n":1}"#),
        ],
        vec![Value::bytes(Vec::new()), Value::json(r#"{"kind":"empty"}"#)],
        vec![Value::Null(DataType::Bytes), Value::Null(DataType::Json)],
    ];

    let mut builder = VolumeBuilder::with_capacity(&schema, expected.len());
    for (row_id, values) in expected.iter().enumerate() {
        builder.add_row(row_id as i64 + 1, &Row::from_values(values.clone()));
    }
    let eager = builder.finish();
    let metadata_only = persist_test_artifact(dir.path(), 97, &eager);

    let mut typed_scanner = VolumeScanner::new(Arc::clone(&metadata_only), vec![0, 1], None);
    assert!(
        typed_scanner.supports_typed_batches(),
        "Bytes/Json have an explicit artifact typed-batch contract"
    );
    crate::instrumentation::begin_row_materialization_probe();
    let batch = typed_scanner
        .next_typed_batch()
        .expect("read typed bytes/json batch")
        .expect("one bytes/json DATA group");
    let materialization = crate::instrumentation::end_row_materialization_probe();
    assert_eq!(batch.row_count(), expected.len());
    assert_eq!(batch.columns().len(), 2);

    fn bytes_at(data: &[u8], offsets: &[(u64, u64)], row_idx: usize) -> Vec<u8> {
        let (offset, len) = offsets[row_idx];
        let offset = usize::try_from(offset).unwrap();
        let len = usize::try_from(len).unwrap();
        data[offset..offset + len].to_vec()
    }

    match &batch.columns()[0] {
        ColumnData::Bytes {
            data,
            offsets,
            ext_type,
            nulls,
        } => {
            assert_eq!(*ext_type, DataType::Bytes);
            assert_eq!(nulls, &[false, false, true]);
            assert_eq!(bytes_at(data, offsets, 0), vec![0, 1, 2, 3, 254, 255]);
            assert_eq!(bytes_at(data, offsets, 1), Vec::<u8>::new());
        }
        _ => panic!("payload must be a Bytes typed column"),
    }
    match &batch.columns()[1] {
        ColumnData::Bytes {
            data,
            offsets,
            ext_type,
            nulls,
        } => {
            assert_eq!(*ext_type, DataType::Json);
            assert_eq!(nulls, &[false, false, true]);
            assert_eq!(
                String::from_utf8(bytes_at(data, offsets, 0)).unwrap(),
                r#"{"kind":"typed","n":1}"#
            );
            assert_eq!(
                String::from_utf8(bytes_at(data, offsets, 1)).unwrap(),
                r#"{"kind":"empty"}"#
            );
        }
        _ => panic!("document must be a Json typed column"),
    }
    assert_eq!(
        materialization.rows, 0,
        "Bytes/Json typed batch must not use the row adapter"
    );
    assert_eq!(materialization.values, 0);
    assert!(typed_scanner.next_typed_batch().unwrap().is_none());

    let mut row_scanner = VolumeScanner::new(metadata_only, vec![0, 1], None);
    let mut rows = Vec::new();
    while row_scanner.next() {
        let row = row_scanner.row();
        rows.push(
            (0..row.len())
                .map(|idx| row.get(idx).unwrap().clone())
                .collect::<Vec<_>>(),
        );
    }
    assert!(row_scanner.err().is_none());
    assert_eq!(rows, expected);
}

#[test]
fn artifact_mapped_projection_loads_only_needed_physical_columns() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 7, &eager);
    assert!(
        metadata_only.artifact_source().is_some(),
        "test must exercise artifact-backed group cache"
    );

    let mapped_schema = SchemaBuilder::new("mapped_projection")
        .column("zero", DataType::Integer, false, false)
        .column("name", DataType::Text, false, false)
        .column("id", DataType::Integer, false, false)
        .build();
    let mapping = ColumnMapping::try_new(
        &mapped_schema,
        &metadata_only,
        vec![
            ColSource::Default(Value::Integer(0)),
            ColSource::Volume(1),
            ColSource::Volume(0),
        ],
    )
    .unwrap();
    let mut scanner = VolumeScanner::new(metadata_only, vec![1], None);
    scanner.set_column_mapping(mapping).unwrap();

    assert!(scanner.next());
    assert_eq!(scanner.row().len(), 1);
    assert_eq!(scanner.row().get(0), Some(&Value::text("apple")));

    let cache = scanner
        .group_cache
        .as_ref()
        .expect("artifact scanner must load one row-group cache");
    assert!(
        cache.columns[0].is_none(),
        "physical id column is not part of logical projection and must not be loaded"
    );
    assert!(
        cache.columns[1].is_some(),
        "logical projection maps to physical name column"
    );
    assert!(
        cache.columns[2].is_none(),
        "physical price column is not part of logical projection and must not be loaded"
    );
}

#[test]
fn r3_l03_batch_b_scanner_snapshot_pins_payload_before_first_row() {
    let schema = SchemaBuilder::new("artifact_lease_test")
        .column("id", DataType::Integer, false, true)
        .build();
    let mut builder = VolumeBuilder::with_capacity(&schema, 2);
    builder.add_row(1, &Row::from_values(vec![Value::Integer(1)]));
    builder.add_row(2, &Row::from_values(vec![Value::Integer(2)]));
    let eager = Arc::new(builder.finish());
    let dir = tempfile::tempdir().unwrap();
    let fixture =
        crate::volume::test_artifact::persist_eager_volume(dir.path(), 19, eager.as_ref());
    let mut scanner = VolumeScanner::new(fixture.volume, vec![0], None);

    // Compaction may retire the pathname after this topology snapshot was
    // created but before the consumer asks for its first row. The source
    // must already own the verified fd; opening lazily here would fail.
    std::fs::remove_file(&fixture.absolute_path).expect("retire visible payload pathname");
    let mut ids = Vec::new();
    while scanner.next() {
        ids.push(scanner.row().get(0).cloned().unwrap());
    }
    assert!(scanner.err().is_none());
    assert_eq!(ids, vec![Value::Integer(1), Value::Integer(2)]);
}

#[test]
fn test_complete_typed_predicate_skips_filter_row_evaluation() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = CountingGtIdExpr::new(Arc::clone(&calls), 3);
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![4, 5]);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "a pure typed comparison filter should be fully handled on column data"
    );
}

#[test]
fn test_mixed_numeric_predicates_decline_lossy_typed_specialization() {
    const EXACT: i64 = 1_i64 << 53;
    let schema = SchemaBuilder::new("numeric_boundary")
        .column("id", DataType::Integer, false, true)
        .column("score", DataType::Float, false, false)
        .build();
    let rows = [
        (Value::Integer(0), Value::Float(0.0)),
        (Value::Integer(EXACT), Value::Float(EXACT as f64)),
        (Value::Integer(EXACT + 1), Value::Float((EXACT + 2) as f64)),
        (Value::Integer(i64::MAX), Value::Float(1.0)),
    ];
    let mut builder = VolumeBuilder::with_capacity(&schema, rows.len());
    for (row_id, (id, score)) in rows.into_iter().enumerate() {
        builder.add_row(row_id as i64 + 1, &Row::from_values(vec![id, score]));
    }
    let volume = Arc::new(builder.finish());

    let collect_ids = |mut filter: Box<dyn Expression>| {
        filter.prepare_for_schema(&schema);
        let mut scanner = VolumeScanner::new(Arc::clone(&volume), vec![0], None);
        scanner.set_filter(filter);
        assert!(
            !scanner.filter_covered_by_typed_predicates,
            "mixed numeric identity must be evaluated by the canonical row predicate"
        );
        let mut ids = Vec::new();
        while scanner.next() {
            ids.push(scanner.row().get(0).cloned().unwrap());
        }
        assert!(scanner.err().is_none());
        ids
    };

    assert_eq!(
        collect_ids(Box::new(
            ComparisonExpr::eq("score", Value::Integer(EXACT),)
        )),
        vec![Value::Integer(EXACT)]
    );
    assert!(collect_ids(Box::new(ComparisonExpr::eq(
        "score",
        Value::Integer(EXACT + 1),
    )))
    .is_empty());
    assert!(collect_ids(Box::new(InListExpr::new(
        "id",
        vec![Value::Float(i64::MAX as f64)],
    )))
    .is_empty());
    assert!(collect_ids(Box::new(InListExpr::new(
        "score",
        vec![Value::Integer(EXACT + 1)],
    )))
    .is_empty());
}

#[test]
fn test_float_nan_typed_predicates_use_canonical_identity_and_order() {
    let schema = SchemaBuilder::new("float_nan")
        .column("id", DataType::Integer, false, true)
        .column("score", DataType::Float, false, false)
        .build();
    let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7ff8_0000_0000_0042);
    let mut builder = VolumeBuilder::with_capacity(&schema, 3);
    for (row_id, score) in [(1, nan_a), (2, nan_b), (3, 1.0)] {
        builder.add_row(
            row_id,
            &Row::from_values(vec![Value::Integer(row_id), Value::Float(score)]),
        );
    }
    let volume = Arc::new(builder.finish());

    let collect_ids = |mut filter: Box<dyn Expression>| {
        filter.prepare_for_schema(&schema);
        let mut scanner = VolumeScanner::new(Arc::clone(&volume), vec![0], None);
        scanner.set_filter(filter);
        assert!(
            scanner.filter_covered_by_typed_predicates,
            "oracle must execute the exact typed-column path"
        );
        let mut ids = Vec::new();
        while scanner.next() {
            ids.push(scanner.row().get(0).cloned().unwrap());
        }
        assert!(scanner.err().is_none());
        ids
    };

    assert_eq!(
        collect_ids(Box::new(ComparisonExpr::eq("score", Value::Float(nan_b),))),
        vec![Value::Integer(1), Value::Integer(2)]
    );
    assert_eq!(
        collect_ids(Box::new(ComparisonExpr::lt("score", Value::Float(nan_b),))),
        vec![Value::Integer(3)]
    );
    assert_eq!(
        collect_ids(Box::new(BetweenExpr::new(
            "score",
            Value::Float(nan_b),
            Value::Float(nan_b),
        ))),
        vec![Value::Integer(1), Value::Integer(2)]
    );
    assert_eq!(
        collect_ids(Box::new(InListExpr::new(
            "score",
            vec![Value::Float(nan_b)],
        ))),
        vec![Value::Integer(1), Value::Integer(2)]
    );
    assert_eq!(
        collect_ids(Box::new(InListExpr::not_in(
            "score",
            vec![Value::Float(nan_b)],
        ))),
        vec![Value::Integer(3)]
    );
}

#[test]
fn test_typed_predicate_rejects_before_partial_filter_row_evaluation() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = AndExpr::and(
        Box::new(ComparisonExpr::gt("id", Value::Integer(3))),
        Box::new(CountingTrueExpr::new(Arc::clone(&calls))),
    );
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![4, 5]);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "partial filters still evaluate fully, but only after typed prefilter accepts the row"
    );
}

#[test]
fn test_complete_typed_predicate_rejects_null_without_full_filter() {
    let schema = SchemaBuilder::new("nullable_test")
        .column("id", DataType::Integer, true, false)
        .column("name", DataType::Text, false, false)
        .build();
    let mut builder = VolumeBuilder::with_capacity(&schema, 2);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Null(DataType::Integer), Value::text("missing")]),
    );
    builder.add_row(
        2,
        &Row::from_values(vec![Value::Integer(4), Value::text("present")]),
    );
    let vol = Arc::new(builder.finish());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = CountingGtIdExpr::new(Arc::clone(&calls), 3);
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    let mut values = Vec::new();
    while scanner.next() {
        values.push(scanner.row().get(0).cloned().unwrap());
    }

    assert_eq!(values, vec![Value::Integer(4)]);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "pure typed predicate must handle NULL rejection without falling back to full filter"
    );
}

#[test]
fn test_between_predicate_is_fully_handled_on_column_data() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let mut filter = BetweenExpr::new("id", Value::Integer(2), Value::Integer(4));
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "inclusive BETWEEN should be represented exactly by typed column predicates"
    );
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![2, 3, 4]);
}

#[test]
fn test_between_column_filter_rejects_null_without_full_filter() {
    let schema = SchemaBuilder::new("nullable_between_test")
        .column("id", DataType::Integer, true, false)
        .column("name", DataType::Text, false, false)
        .build();
    let mut builder = VolumeBuilder::with_capacity(&schema, 3);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Null(DataType::Integer), Value::text("missing")]),
    );
    builder.add_row(
        2,
        &Row::from_values(vec![Value::Integer(3), Value::text("inside")]),
    );
    builder.add_row(
        3,
        &Row::from_values(vec![Value::Integer(5), Value::text("outside")]),
    );
    let vol = Arc::new(builder.finish());
    let mut filter = BetweenExpr::new("id", Value::Integer(2), Value::Integer(4));
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "BETWEEN fast path should be exact enough to own NULL rejection"
    );
    let mut values = Vec::new();
    while scanner.next() {
        values.push(scanner.row().get(0).cloned().unwrap());
    }

    assert_eq!(values, vec![Value::Integer(3)]);
}

#[test]
fn test_not_between_predicate_is_fully_handled_on_column_data() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let mut filter = BetweenExpr::not_between("id", Value::Integer(2), Value::Integer(4));
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "NOT BETWEEN should be represented exactly as a typed OR predicate"
    );
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![1, 5]);
}

#[test]
fn test_or_predicate_is_fully_handled_on_column_data() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let mut filter = OrExpr::or(
        Box::new(ComparisonExpr::eq("id", Value::Integer(1))),
        Box::new(ComparisonExpr::eq("id", Value::Integer(5))),
    );
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "simple OR should be represented exactly by the typed predicate tree"
    );
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![1, 5]);
}

#[test]
fn test_nested_and_or_predicate_is_fully_handled_on_column_data() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let mut filter = AndExpr::and(
        Box::new(ComparisonExpr::gt("id", Value::Integer(1))),
        Box::new(OrExpr::or(
            Box::new(ComparisonExpr::eq("name", Value::text("banana"))),
            Box::new(ComparisonExpr::eq("name", Value::text("date"))),
        )),
    );
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0, 1], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "nested AND/OR should preserve boolean shape in the typed predicate tree"
    );
    let mut rows = Vec::new();
    while scanner.next() {
        rows.push(scanner.row().clone());
    }

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0), Some(&Value::Integer(2)));
    assert_eq!(rows[0].get(1), Some(&Value::text("banana")));
    assert_eq!(rows[1].get(0), Some(&Value::Integer(4)));
    assert_eq!(rows[1].get(1), Some(&Value::text("date")));
}

#[test]
fn test_in_list_predicate_rejects_gaps_before_partial_filter_row_evaluation() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = AndExpr::and(
        Box::new(InListExpr::new(
            "id",
            vec![Value::Integer(2), Value::Integer(4)],
        )),
        Box::new(CountingTrueExpr::new(Arc::clone(&calls))),
    );
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![2, 4]);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        2,
        "exact IN prefilter should reject the id=3 gap before row-level filter evaluation"
    );
}

#[test]
fn test_pure_in_list_predicate_skips_full_filter_evaluation() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let mut filter = InListExpr::new("id", vec![Value::Integer(2), Value::Integer(4)]);
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "pure IN should be exactly covered by column predicates"
    );
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn test_not_in_list_with_null_rejects_before_partial_filter_row_evaluation() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = AndExpr::and(
        Box::new(InListExpr::not_in(
            "id",
            vec![Value::Integer(2), Value::Null(DataType::Integer)],
        )),
        Box::new(CountingTrueExpr::new(Arc::clone(&calls))),
    );
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(!scanner.next());
    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "NOT IN with NULL is UNKNOWN/false for all non-matching rows and should reject before Row"
    );
}

#[test]
fn test_pure_not_in_list_with_null_skips_full_filter_evaluation() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let mut filter = InListExpr::not_in(
        "id",
        vec![Value::Integer(2), Value::Null(DataType::Integer)],
    );
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "pure NOT IN with NULL should be exactly covered by column predicates"
    );
    assert!(!scanner.next());
}

#[test]
fn artifact_group_cache_records_materialized_rows_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let eager = make_test_volume();
    let metadata_only = persist_test_artifact(dir.path(), 12, &eager);
    let row_count = metadata_only.meta.row_count as u64;
    let column_count = metadata_only.columns.len() as u64;

    crate::instrumentation::begin_row_materialization_probe();
    let mut full = VolumeScanner::new(Arc::clone(&metadata_only), vec![], None);
    let mut full_rows = 0_u64;
    while full.next() {
        full_rows += 1;
    }
    let full_probe = crate::instrumentation::end_row_materialization_probe();
    assert_eq!(full_rows, row_count);
    assert_eq!(full_probe.calls, row_count);
    assert_eq!(full_probe.rows, row_count);
    assert_eq!(full_probe.values, row_count * column_count);

    crate::instrumentation::begin_row_materialization_probe();
    let mut projected = VolumeScanner::new(Arc::clone(&metadata_only), vec![0], None);
    let mut projected_rows = 0_u64;
    while projected.next() {
        projected_rows += 1;
    }
    let projected_probe = crate::instrumentation::end_row_materialization_probe();
    assert_eq!(projected_rows, row_count);
    assert_eq!(projected_probe.calls, row_count);
    assert_eq!(projected_probe.rows, row_count);
    assert_eq!(projected_probe.values, row_count);
}

fn make_nullable_name_volume() -> (radixdb_core::Schema, Arc<FrozenVolume>) {
    let schema = SchemaBuilder::new("nullable_name_test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, true, false)
        .build();
    let mut builder = VolumeBuilder::with_capacity(&schema, 3);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Integer(1), Value::Null(DataType::Text)]),
    );
    builder.add_row(
        2,
        &Row::from_values(vec![Value::Integer(2), Value::text("present")]),
    );
    builder.add_row(
        3,
        &Row::from_values(vec![Value::Integer(3), Value::Null(DataType::Text)]),
    );
    (schema, Arc::new(builder.finish()))
}

#[test]
fn test_is_null_predicate_skips_full_filter_evaluation() {
    let (schema, vol) = make_nullable_name_volume();
    let mut filter = NullCheckExpr::is_null("name");
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "pure IS NULL should be exactly covered by null bitmap predicate"
    );
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn test_is_not_null_predicate_skips_full_filter_evaluation() {
    let (schema, vol) = make_nullable_name_volume();
    let mut filter = NullCheckExpr::is_not_null("name");
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "pure IS NOT NULL should be exactly covered by null bitmap predicate"
    );
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![2]);
}

#[test]
fn test_null_check_prefilter_rejects_before_partial_filter_row_evaluation() {
    let (schema, vol) = make_nullable_name_volume();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = AndExpr::and(
        Box::new(NullCheckExpr::is_not_null("name")),
        Box::new(CountingTrueExpr::new(Arc::clone(&calls))),
    );
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![2]);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "null-check prefilter should reject NULL rows before row-level filter evaluation"
    );
}

#[test]
fn test_like_prefix_prefilter_rejects_before_partial_filter_row_evaluation() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = AndExpr::and(
        Box::new(LikeExpr::new("name", "a%")),
        Box::new(CountingTrueExpr::new(Arc::clone(&calls))),
    );
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![1], None);
    scanner.set_filter(Box::new(filter));

    let mut names = Vec::new();
    while scanner.next() {
        if let Some(Value::Text(name)) = scanner.row().get(0) {
            names.push(name.to_string());
        }
    }

    assert_eq!(names, vec!["apple"]);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "LIKE prefix prefilter should reject non-prefix rows before row-level filter evaluation"
    );
}

#[test]
fn test_negated_like_prefix_is_not_used_as_rejection_prefilter() {
    let schema = SchemaBuilder::new("not_like_test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    let mut builder = VolumeBuilder::with_capacity(&schema, 3);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Integer(1), Value::text("a")]),
    );
    builder.add_row(
        2,
        &Row::from_values(vec![Value::Integer(2), Value::text("apple")]),
    );
    builder.add_row(
        3,
        &Row::from_values(vec![Value::Integer(3), Value::text("banana")]),
    );
    let vol = Arc::new(builder.finish());
    let mut filter = LikeExpr::not_like("name", "a_%");
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        !scanner
            .typed_predicates
            .iter()
            .any(|predicate| matches!(predicate.kind, ColumnPredicateKind::LikePrefix { .. })),
        "NOT LIKE prefix is not a safe standalone rejection predicate"
    );
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn test_complex_and_column_prefilters_reject_before_row_filter_evaluation() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut filter = AndExpr::new(vec![
        Box::new(InListExpr::new(
            "id",
            vec![Value::Integer(2), Value::Integer(4)],
        )),
        Box::new(LikeExpr::new("name", "d%")),
        Box::new(CountingTrueExpr::new(Arc::clone(&calls))),
    ]);
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0, 1], None);
    scanner.set_filter(Box::new(filter));

    let mut rows = Vec::new();
    while scanner.next() {
        rows.push(scanner.row().clone());
    }

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&Value::Integer(4)));
    assert_eq!(rows[0].get(1), Some(&Value::text("date")));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "complex AND column prefilters should reject non-matching rows before row-level tail"
    );
}

#[test]
fn test_range_predicate_is_fully_handled_on_column_data() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .column("price", DataType::Float, false, false)
        .build();
    let vol = make_test_volume();
    let mut filter = RangeExpr::half_open("id", Value::Integer(2), Value::Integer(5));
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "half-open RangeExpr should be represented exactly by typed column predicates"
    );
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
    }

    assert_eq!(ids, vec![2, 3, 4]);
}

#[test]
fn test_range_predicate_rejects_null_without_full_filter() {
    let schema = SchemaBuilder::new("nullable_range_test")
        .column("id", DataType::Integer, true, false)
        .column("name", DataType::Text, false, false)
        .build();
    let mut builder = VolumeBuilder::with_capacity(&schema, 3);
    builder.add_row(
        1,
        &Row::from_values(vec![Value::Null(DataType::Integer), Value::text("missing")]),
    );
    builder.add_row(
        2,
        &Row::from_values(vec![Value::Integer(3), Value::text("inside")]),
    );
    builder.add_row(
        3,
        &Row::from_values(vec![Value::Integer(5), Value::text("outside")]),
    );
    let vol = Arc::new(builder.finish());
    let mut filter = RangeExpr::inclusive("id", Value::Integer(2), Value::Integer(4));
    filter.prepare_for_schema(&schema);

    let mut scanner = VolumeScanner::new(vol, vec![0], None);
    scanner.set_filter(Box::new(filter));

    assert!(
        scanner.filter_covered_by_typed_predicates,
        "RangeExpr fast path should own NULL rejection when exact"
    );
    let mut values = Vec::new();
    while scanner.next() {
        values.push(scanner.row().get(0).cloned().unwrap());
    }

    assert_eq!(values, vec![Value::Integer(3)]);
}

#[test]
fn test_range_scan() {
    let vol = make_test_volume();
    // Scan rows 2..4 (indices 2, 3)
    let mut scanner = VolumeScanner::with_range(Arc::clone(&vol), vec![], 2, 4, None);

    let mut count = 0;
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
        count += 1;
    }
    assert_eq!(count, 2);
    assert_eq!(ids, vec![3, 4]); // rows at index 2 and 3
}

#[test]
fn test_empty_scanner() {
    let mut scanner = VolumeScanner::empty();
    assert!(!scanner.next());
    assert!(scanner.err().is_none());
}

#[test]
fn r3_l02_batch_a_explicit_close_is_terminal_and_releases_owners() {
    let mut scanner = VolumeScanner::new(make_test_volume(), vec![0], None);
    assert!(scanner.next());
    scanner.group_cache = Some(GroupColumnCache {
        group_idx: 0,
        columns: Vec::new(),
        group_start: 0,
    });

    scanner.close().unwrap();

    assert!(!scanner.next(), "closed scanner must never resume");
    assert!(
        scanner.group_cache.is_none(),
        "close must release decoded cache"
    );
}

#[derive(Debug)]
struct CloseProbeScanner {
    closed: Arc<AtomicBool>,
    fail_close: bool,
    row: Row,
}

impl Scanner for CloseProbeScanner {
    fn next(&mut self) -> bool {
        false
    }

    fn row(&self) -> &Row {
        &self.row
    }

    fn err(&self) -> Option<&Error> {
        None
    }

    fn close(&mut self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        if self.fail_close {
            Err(Error::internal("close probe failure"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn r3_l02_batch_a_merged_close_drains_every_source_after_first_error() {
    let first_closed = Arc::new(AtomicBool::new(false));
    let second_closed = Arc::new(AtomicBool::new(false));
    let mut scanner = MergingScanner::new(vec![
        Box::new(CloseProbeScanner {
            closed: Arc::clone(&first_closed),
            fail_close: true,
            row: Row::new(),
        }),
        Box::new(CloseProbeScanner {
            closed: Arc::clone(&second_closed),
            fail_close: false,
            row: Row::new(),
        }),
    ]);

    assert!(scanner.close().is_err());
    assert!(first_closed.load(Ordering::Acquire));
    assert!(second_closed.load(Ordering::Acquire));
    assert!(!scanner.next());
    assert_eq!(scanner.estimated_count(), Some(0));
}

#[test]
fn test_take_row() {
    let vol = make_test_volume();
    let mut scanner = VolumeScanner::new(vol, vec![0], None);

    assert!(scanner.next());
    let row = scanner.take_row();
    assert_eq!(row.get(0), Some(&Value::Integer(1)));
}

#[test]
fn test_merging_scanner() {
    let vol = make_test_volume();

    // Create two scanners: first 2 rows, then last 2 rows
    let scanner1 = Box::new(VolumeScanner::with_range(
        Arc::clone(&vol),
        vec![0],
        0,
        2,
        None,
    ));
    let scanner2 = Box::new(VolumeScanner::with_range(
        Arc::clone(&vol),
        vec![0],
        3,
        5,
        None,
    ));

    let mut merger = MergingScanner::new(vec![scanner1, scanner2]);

    let mut ids = Vec::new();
    while merger.next() {
        if let Some(Value::Integer(id)) = merger.row().get(0) {
            ids.push(*id);
        }
    }
    assert_eq!(ids, vec![1, 2, 4, 5]); // rows 0,1 from first, rows 3,4 from second
    assert!(merger.err().is_none());
}

#[test]
fn merged_scanner_releases_consumed_source_owner() {
    struct OwnedScanner {
        _owner: Arc<()>,
        row: Row,
        advanced: bool,
    }
    impl Scanner for OwnedScanner {
        fn next(&mut self) -> bool {
            if self.advanced {
                false
            } else {
                self.advanced = true;
                true
            }
        }
        fn row(&self) -> &Row {
            &self.row
        }
        fn err(&self) -> Option<&Error> {
            None
        }
        fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn estimated_count(&self) -> Option<usize> {
            Some(usize::from(!self.advanced))
        }
    }

    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let first = OwnedScanner {
        _owner: owner,
        row: Row::from_values(vec![Value::Integer(1)]),
        advanced: false,
    };
    let second = VecScanner::new(vec![Row::from_values(vec![Value::Integer(2)])]);
    let mut merged = MergingScanner::new(vec![Box::new(first), Box::new(second)]);
    assert!(merged.next());
    assert!(merged.next());
    assert!(
        weak.upgrade().is_none(),
        "a completed source must release its owner before the merged cursor closes"
    );
    merged.close().unwrap();
}

#[test]
fn test_estimated_count() {
    let vol = make_test_volume();
    let scanner = VolumeScanner::new(Arc::clone(&vol), vec![], None);
    assert_eq!(scanner.estimated_count(), Some(5));

    let scanner = VolumeScanner::with_range(vol, vec![], 2, 4, None);
    assert_eq!(scanner.estimated_count(), Some(2));
}
