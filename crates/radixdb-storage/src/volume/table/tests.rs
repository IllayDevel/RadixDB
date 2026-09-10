use super::super::writer::VolumeBuilder;
use super::*;
use radixdb_core::SchemaBuilder;
use std::path::PathBuf;

use super::super::manifest::SegmentMeta;
use crate::expression::ComparisonExpr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Helper: create a SegmentManager with one segment from given rows.
fn make_segment_mgr(schema: &Schema, rows: &[(i64, Row)]) -> Arc<SegmentManager> {
    let mut builder = VolumeBuilder::with_capacity(schema, rows.len());
    for (id, row) in rows {
        builder.add_row(*id, row);
    }
    let vol = Arc::new(builder.finish());
    let min_id = rows.first().map(|(id, _)| *id).unwrap_or(0);
    let max_id = rows.last().map(|(id, _)| *id).unwrap_or(0);

    let mgr = Arc::new(SegmentManager::new("test", None));
    mgr.register_segment(
        1,
        vol,
        SegmentMeta {
            segment_id: 1,
            file_path: PathBuf::from("test.data"),
            row_count: rows.len(),
            min_row_id: min_id,
            max_row_id: max_id,
            schema_version: 0,
            seal_seq: 0,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    mgr
}

#[test]
fn filtered_aggregate_mixed_numeric_target_requires_exact_conversion() {
    const EXACT: i64 = 1_i64 << 53;
    let schema = SchemaBuilder::new("numeric_boundary")
        .column("id", DataType::Integer, false, true)
        .column("metric", DataType::Float, false, false)
        .build();
    let rows = vec![
        (
            EXACT,
            Row::from_values(vec![Value::Integer(EXACT), Value::Float(EXACT as f64)]),
        ),
        (
            i64::MAX,
            Row::from_values(vec![
                Value::Integer(i64::MAX),
                Value::Float((EXACT + 2) as f64),
            ]),
        ),
    ];
    let table = SegmentedTable::new(
        Box::new(MockHotTable::new(schema.clone(), Vec::new())),
        make_segment_mgr(&schema, &rows),
    );

    let exact_integer = ComparisonExpr::eq("id", Value::Float(EXACT as f64));
    assert_eq!(
        table.compute_filtered_aggregates(&[(AggregateOp::CountStar, 0)], &exact_integer,),
        Some(vec![Value::Integer(1)])
    );

    let fractional_integer = ComparisonExpr::eq("id", Value::Float(0.5));
    assert_eq!(
        table.compute_filtered_aggregates(&[(AggregateOp::CountStar, 0)], &fractional_integer,),
        Some(vec![Value::Integer(0)])
    );
    let out_of_range_integer = ComparisonExpr::eq("id", Value::Float(i64::MAX as f64));
    assert_eq!(
        table.compute_filtered_aggregates(&[(AggregateOp::CountStar, 0)], &out_of_range_integer,),
        Some(vec![Value::Integer(0)])
    );

    let exact_float = ComparisonExpr::eq("metric", Value::Integer(EXACT));
    assert_eq!(
        table.compute_filtered_aggregates(&[(AggregateOp::CountStar, 0)], &exact_float),
        Some(vec![Value::Integer(1)])
    );
    let rounded_float = ComparisonExpr::eq("metric", Value::Integer(EXACT + 1));
    assert_eq!(
        table.compute_filtered_aggregates(&[(AggregateOp::CountStar, 0)], &rounded_float),
        Some(vec![Value::Integer(0)])
    );
}

#[test]
fn filtered_aggregate_float_nan_uses_canonical_comparison() {
    let schema = SchemaBuilder::new("float_nan")
        .column("id", DataType::Integer, false, true)
        .column("metric", DataType::Float, false, false)
        .build();
    let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7ff8_0000_0000_0042);
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(nan_a)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(nan_b)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(3), Value::Float(1.0)]),
        ),
    ];
    let table = SegmentedTable::new(
        Box::new(MockHotTable::new(schema.clone(), Vec::new())),
        make_segment_mgr(&schema, &rows),
    );

    let equal_nan = ComparisonExpr::eq("metric", Value::Float(nan_b));
    assert_eq!(
        table.compute_filtered_aggregates(&[(AggregateOp::CountStar, 0)], &equal_nan),
        Some(vec![Value::Integer(2)])
    );

    let less_than_nan = ComparisonExpr::lt("metric", Value::Float(nan_b));
    assert_eq!(
        table.compute_filtered_aggregates(&[(AggregateOp::CountStar, 0)], &less_than_nan),
        Some(vec![Value::Integer(1)])
    );
}

#[test]
fn legacy_float_bloom_negative_never_prunes_volume() {
    let schema = SchemaBuilder::new("float_bloom_compat")
        .column("value", DataType::Float, false, false)
        .build();
    let mut builder = VolumeBuilder::new(&schema);
    builder.add_row(1, &Row::from_values(vec![Value::Float(-0.0)]));
    let mut volume = builder.finish();

    // Recreate the non-disabled raw-bit Float bloom stored by older artifact-backed
    // writers. Its signed-zero false negative is the compatibility case.
    let mut legacy_bloom = super::super::column::ColumnBloomFilter::new(4096);
    legacy_bloom.add_f64(-0.0);
    let negative_zero_hash =
        super::super::column::ColumnBloomFilter::hash_value_static(&Value::Float(-0.0));
    let positive_zero_hash =
        super::super::column::ColumnBloomFilter::hash_value_static(&Value::Float(0.0));
    assert!(legacy_bloom.might_contain_hash(negative_zero_hash));
    assert!(!legacy_bloom.might_contain_hash(positive_zero_hash));
    assert!(legacy_bloom.might_contain(&Value::Float(0.0)));
    Arc::make_mut(&mut volume.meta).bloom_filters[0] = legacy_bloom;

    for probe in [Value::Float(0.0), Value::Integer(0)] {
        let comparisons = [("value", radixdb_core::Operator::Eq, &probe)];
        let hashes = SegmentedTable::precompute_bloom_hashes(&comparisons);
        let (skip, start, end) = SegmentedTable::prune_volume(&volume, &comparisons, &hashes);
        assert!(!skip, "legacy Float bloom must not prune {probe:?}");
        assert_eq!((start, end), (0, 1));
    }
}

#[test]
fn cross_domain_probe_never_uses_integer_bloom_as_definitive() {
    let schema = SchemaBuilder::new("integer_bloom_compat")
        .column("value", DataType::Integer, false, false)
        .build();
    let mut builder = VolumeBuilder::new(&schema);
    builder.add_row(1, &Row::from_values(vec![Value::Integer(0)]));
    let volume = builder.finish();
    let probe = Value::Float(0.0);
    let raw_probe_hash = super::super::column::ColumnBloomFilter::hash_value_static(&probe);
    assert!(!volume.meta.bloom_filters[0].might_contain_hash(raw_probe_hash));
    assert!(volume.meta.bloom_filters[0].might_contain(&probe));

    let comparisons = [("value", radixdb_core::Operator::Eq, &probe)];
    let hashes = SegmentedTable::precompute_bloom_hashes(&comparisons);
    let (skip, start, end) = SegmentedTable::prune_volume(&volume, &comparisons, &hashes);
    assert!(!skip);
    assert_eq!((start, end), (0, 1));
}

/// Helper: create an artifact-backed segment manager from given rows.
/// The returned TempDir must stay alive while the segment is scanned,
/// because cold rows decode DATA row groups from the artifact on demand.
fn make_artifact_segment_mgr(
    schema: &Schema,
    rows: &[(i64, Row)],
) -> (Arc<SegmentManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mgr = Arc::new(SegmentManager::new("test", None));
    register_artifact_segment(&mgr, dir.path(), schema, 1, rows);
    (mgr, dir)
}

fn register_artifact_segment(
    mgr: &Arc<SegmentManager>,
    dir: &std::path::Path,
    schema: &Schema,
    segment_id: u64,
    rows: &[(i64, Row)],
) {
    let fixture =
        crate::volume::test_artifact::build_artifact_volume(dir, schema, segment_id, rows);
    assert!(fixture.volume.is_cold());
    assert!(fixture.volume.artifact_source().is_some());

    let min_id = rows.first().map(|(id, _)| *id).unwrap_or(0);
    let max_id = rows.last().map(|(id, _)| *id).unwrap_or(0);
    mgr.register_segment(
        segment_id,
        fixture.volume,
        SegmentMeta {
            segment_id,
            file_path: fixture.relative_path,
            row_count: rows.len(),
            min_row_id: min_id,
            max_row_id: max_id,
            schema_version: 0,
            seal_seq: 0,
            ..Default::default()
        },
        Some(schema),
    )
    .unwrap();
}

#[cfg(feature = "parallel")]
fn make_two_artifact_segment_mgr(
    schema: &Schema,
    first_rows: &[(i64, Row)],
    second_rows: &[(i64, Row)],
) -> (Arc<SegmentManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mgr = Arc::new(SegmentManager::new("test", None));
    register_artifact_segment(&mgr, dir.path(), schema, 1, first_rows);
    register_artifact_segment(&mgr, dir.path(), schema, 2, second_rows);
    (mgr, dir)
}

#[test]
fn artifact_columnar_group_state_uses_direct_array_only_for_bounded_domain() {
    assert!(ArtifactColumnarGroupState::new(Some((10, 12))).is_direct_array());
    assert!(!ArtifactColumnarGroupState::new(None).is_direct_array());
    assert!(
        !ArtifactColumnarGroupState::new(Some((
            0,
            ARTIFACT_COLUMNAR_GROUP_DIRECT_ARRAY_MAX_WIDTH as i64
        )))
        .is_direct_array(),
        "inclusive width max+1 must stay on hash map"
    );
    assert!(
        !ArtifactColumnarGroupState::new(Some((i64::MIN, i64::MAX))).is_direct_array(),
        "overflow-sized domains must stay on hash map"
    );
}

#[test]
fn artifact_grouped_aggregates_use_typed_batches_without_row_materialization() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Float, true, false)
        .build();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(5.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(1), Value::Null(DataType::Float)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(2), Value::Float(10.0)]),
        ),
        (
            4,
            Row::from_values(vec![Value::Null(DataType::Integer), Value::Float(20.0)]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_row_materialization_probe();
    let mut grouped = table
        .compute_grouped_aggregates(
            &[0],
            &[
                (AggregateOp::CountStar, 0),
                (AggregateOp::Count, 1),
                (AggregateOp::Sum, 1),
                (AggregateOp::Min, 1),
                (AggregateOp::Max, 1),
                (AggregateOp::Avg, 1),
            ],
        )
        .unwrap();
    let materialization = crate::instrumentation::end_row_materialization_probe();

    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Null(_)) => -1,
        Some(Value::Integer(value)) => *value,
        other => panic!("unexpected group key: {other:?}"),
    });
    assert_eq!(
        grouped
            .into_iter()
            .map(|result| (result.group_values, result.aggregate_values))
            .collect::<Vec<_>>(),
        vec![
            (
                vec![Value::Null(DataType::Integer)],
                vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Float(20.0),
                    Value::Float(20.0),
                    Value::Float(20.0),
                    Value::Float(20.0),
                ],
            ),
            (
                vec![Value::Integer(1)],
                vec![
                    Value::Integer(2),
                    Value::Integer(1),
                    Value::Float(5.0),
                    Value::Float(5.0),
                    Value::Float(5.0),
                    Value::Float(5.0),
                ],
            ),
            (
                vec![Value::Integer(2)],
                vec![
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Float(10.0),
                    Value::Float(10.0),
                    Value::Float(10.0),
                    Value::Float(10.0),
                ],
            ),
        ]
    );
    assert_eq!(
        materialization.rows, 0,
        "artifact-backed group path must not create Rows"
    );
    assert_eq!(
        materialization.values, 0,
        "artifact-backed group path must not create Values per row"
    );

    fn normalize_integer_groups(
        values: Vec<GroupedAggregateResult>,
    ) -> Vec<(Option<i64>, Vec<Value>)> {
        let mut normalized: Vec<(Option<i64>, Vec<Value>)> = values
            .into_iter()
            .map(|result| {
                let key = match result.group_values.first() {
                    Some(Value::Integer(value)) => Some(*value),
                    Some(Value::Null(DataType::Integer)) => None,
                    other => panic!("unexpected group key: {other:?}"),
                };
                (key, result.aggregate_values)
            })
            .collect();
        normalized.sort_by_key(|(key, _)| key.unwrap_or(-1));
        normalized
    }

    let typed = table
        .compute_grouped_aggregates(
            &[0],
            &[
                (AggregateOp::CountStar, 0),
                (AggregateOp::Count, 1),
                (AggregateOp::Sum, 1),
                (AggregateOp::Min, 1),
                (AggregateOp::Max, 1),
                (AggregateOp::Avg, 1),
            ],
        )
        .unwrap();
    let scanner = table
        .compute_grouped_aggregates_scanner(
            &[0],
            &[
                (AggregateOp::CountStar, 0),
                (AggregateOp::Count, 1),
                (AggregateOp::Sum, 1),
                (AggregateOp::Min, 1),
                (AggregateOp::Max, 1),
                (AggregateOp::Avg, 1),
            ],
            None,
        )
        .unwrap();
    assert_eq!(
        normalize_integer_groups(typed),
        normalize_integer_groups(scanner)
    );
}

#[test]
fn artifact_sum_uses_persisted_numeric_statistics_without_data_reads() {
    let schema = SchemaBuilder::new("test")
        .column("integer_value", DataType::Integer, true, false)
        .column("float_value", DataType::Float, true, false)
        .build();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(3), Value::Float(1.5)]),
        ),
        (
            2,
            Row::from_values(vec![
                Value::Null(DataType::Integer),
                Value::Null(DataType::Float),
            ]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(-7), Value::Float(-0.25)]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_volume_read_probe();
    crate::instrumentation::begin_decompression_probe();
    crate::instrumentation::begin_row_materialization_probe();
    let integer_sum = table.sum_column(0).unwrap();
    let float_sum = table.sum_column(1).unwrap();
    let materialization = crate::instrumentation::end_row_materialization_probe();
    let decompression = crate::instrumentation::end_decompression_probe();
    let volume_read = crate::instrumentation::end_volume_read_probe();

    assert_eq!(integer_sum.into_value().unwrap(), Value::Integer(-4));
    assert_eq!(integer_sum.count(), 2);
    assert_eq!(float_sum.into_value().unwrap(), Value::Float(1.25));
    assert_eq!(float_sum.count(), 2);
    assert_eq!(volume_read.calls, 0, "SUM must not read DATA blocks");
    assert_eq!(
        decompression.calls, 0,
        "SUM must not decompress DATA blocks"
    );
    assert_eq!(materialization.rows, 0, "SUM must not materialize rows");
    assert_eq!(materialization.values, 0, "SUM must not materialize values");
}

#[test]
fn artifact_columnar_grouped_aggregate_matches_100m_benchmark_shape() {
    const BENCHMARK_TABLE_ROWS: usize = 833_333;
    const BENCHMARK_BUCKETS: usize = 997;
    const EXPECTED_ROW_GROUPS: u64 = 13;
    const EXPECTED_SELECTED_BLOCKS: u64 = EXPECTED_ROW_GROUPS * 2;

    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let rows: Vec<(i64, Row)> = (1..=BENCHMARK_TABLE_ROWS as i64)
        .map(|id| {
            (
                id,
                Row::from_values(vec![
                    Value::Integer(id % BENCHMARK_BUCKETS as i64),
                    Value::Integer(((id * 31) % 1_000_000) + 1),
                ]),
            )
        })
        .collect();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_row_materialization_probe();
    crate::instrumentation::begin_artifact_columnar_group_probe();
    let grouped = table
        .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();
    let materialization = crate::instrumentation::end_row_materialization_probe();

    let mut group_seen = vec![false; BENCHMARK_BUCKETS];
    let mut total_count = 0i64;
    for result in &grouped {
        let bucket = match result.group_values.first() {
            Some(Value::Integer(value)) => *value as usize,
            other => panic!("unexpected benchmark group key: {other:?}"),
        };
        assert!(bucket < BENCHMARK_BUCKETS);
        group_seen[bucket] = true;
        match result.aggregate_values.first() {
            Some(Value::Integer(count)) => total_count += *count,
            other => panic!("unexpected COUNT(*) aggregate: {other:?}"),
        }
    }

    assert!(
        group_seen.into_iter().all(|seen| seen),
        "benchmark bucket distribution must produce all groups"
    );
    assert_eq!(grouped.len(), BENCHMARK_BUCKETS);
    assert_eq!(total_count, BENCHMARK_TABLE_ROWS as i64);
    assert_eq!(shape.applies, 1);
    assert_eq!(shape.row_groups, EXPECTED_ROW_GROUPS);
    assert_eq!(shape.selected_blocks, EXPECTED_SELECTED_BLOCKS);
    assert_eq!(shape.input_rows, BENCHMARK_TABLE_ROWS as u64);
    assert_eq!(shape.output_groups, BENCHMARK_BUCKETS as u64);
    assert_eq!(shape.direct_accumulators, 1);
    assert_eq!(shape.hash_accumulators, 0);
    assert_eq!(shape.local_merges, 1);
    assert_eq!(shape.merged_groups, BENCHMARK_BUCKETS as u64);
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
}

#[test]
fn artifact_columnar_grouped_aggregate_supports_timestamp_key_and_integer_values() {
    let schema = SchemaBuilder::new("test")
        .column("bucket_at", DataType::Timestamp, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let first = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let second = chrono::DateTime::from_timestamp(1_700_000_060, 0).unwrap();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Timestamp(first), Value::Integer(3)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Timestamp(first), Value::Integer(7)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Timestamp(second), Value::Integer(11)]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_row_materialization_probe();
    let mut grouped = table
        .compute_grouped_aggregates(
            &[0],
            &[
                (AggregateOp::CountStar, 0),
                (AggregateOp::Sum, 1),
                (AggregateOp::Min, 1),
                (AggregateOp::Max, 1),
                (AggregateOp::Avg, 1),
            ],
        )
        .unwrap();
    let materialization = crate::instrumentation::end_row_materialization_probe();

    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Timestamp(value)) => value.timestamp(),
        other => panic!("unexpected group key: {other:?}"),
    });
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].group_values, vec![Value::Timestamp(first)]);
    assert_eq!(
        grouped[0].aggregate_values,
        vec![
            Value::Integer(2),
            Value::Integer(10),
            Value::Integer(3),
            Value::Integer(7),
            Value::Float(5.0),
        ]
    );
    assert_eq!(grouped[1].group_values, vec![Value::Timestamp(second)]);
    assert_eq!(
        grouped[1].aggregate_values,
        vec![
            Value::Integer(1),
            Value::Integer(11),
            Value::Integer(11),
            Value::Integer(11),
            Value::Float(11.0),
        ]
    );
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
}

#[test]
fn artifact_columnar_grouped_aggregate_supports_null_key_and_float_values() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Float, true, false)
        .build();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(1.5)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Null(DataType::Integer), Value::Float(2.5)]),
        ),
        (
            3,
            Row::from_values(vec![
                Value::Null(DataType::Integer),
                Value::Null(DataType::Float),
            ]),
        ),
        (
            4,
            Row::from_values(vec![Value::Integer(1), Value::Float(4.0)]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_row_materialization_probe();
    crate::instrumentation::begin_artifact_columnar_group_probe();
    let mut grouped = table
        .compute_grouped_aggregates(
            &[0],
            &[
                (AggregateOp::CountStar, 0),
                (AggregateOp::Count, 1),
                (AggregateOp::Sum, 1),
                (AggregateOp::Min, 1),
                (AggregateOp::Max, 1),
                (AggregateOp::Avg, 1),
            ],
        )
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();
    let materialization = crate::instrumentation::end_row_materialization_probe();

    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Null(DataType::Integer)) => i64::MIN,
        Some(Value::Integer(value)) => *value,
        other => panic!("unexpected group key: {other:?}"),
    });

    assert_eq!(grouped.len(), 2);
    assert_eq!(
        grouped[0].group_values,
        vec![Value::Null(DataType::Integer)]
    );
    assert_eq!(
        grouped[0].aggregate_values,
        vec![
            Value::Integer(2),
            Value::Integer(1),
            Value::Float(2.5),
            Value::Float(2.5),
            Value::Float(2.5),
            Value::Float(2.5),
        ]
    );
    assert_eq!(grouped[1].group_values, vec![Value::Integer(1)]);
    assert_eq!(
        grouped[1].aggregate_values,
        vec![
            Value::Integer(2),
            Value::Integer(2),
            Value::Float(5.5),
            Value::Float(1.5),
            Value::Float(4.0),
            Value::Float(2.75),
        ]
    );
    assert_eq!(shape.applies, 1);
    assert_eq!(shape.row_groups, 1);
    assert_eq!(shape.selected_blocks, 2);
    assert_eq!(shape.input_rows, 4);
    assert_eq!(shape.output_groups, 2);
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
}

#[test]
fn artifact_columnar_grouped_aggregate_declines_empty_input_without_row_materialization() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let segment_mgr = Arc::new(SegmentManager::new("test", None));
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_row_materialization_probe();
    crate::instrumentation::begin_artifact_columnar_group_probe();
    let grouped = table.compute_grouped_aggregates(
        &[0],
        &[
            (AggregateOp::CountStar, 0),
            (AggregateOp::Count, 1),
            (AggregateOp::Sum, 1),
            (AggregateOp::Min, 1),
            (AggregateOp::Max, 1),
            (AggregateOp::Avg, 1),
        ],
    );
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();
    let materialization = crate::instrumentation::end_row_materialization_probe();

    assert!(
        grouped.is_none(),
        "the artifact operator must decline when no cold artifact exists"
    );
    assert_eq!(shape.applies, 0);
    assert_eq!(shape.row_groups, 0);
    assert_eq!(shape.selected_blocks, 0);
    assert_eq!(shape.input_rows, 0);
    assert_eq!(shape.output_groups, 0);
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
}

#[test]
fn artifact_columnar_grouped_aggregate_matches_scanner_integer_overflow_semantics() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Integer(i64::MAX)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(1), Value::Integer(1)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(2), Value::Integer(i64::MIN)]),
        ),
        (
            4,
            Row::from_values(vec![Value::Integer(2), Value::Integer(-1)]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    fn normalize(values: Vec<GroupedAggregateResult>) -> Vec<(i64, Vec<Value>)> {
        let mut normalized: Vec<(i64, Vec<Value>)> = values
            .into_iter()
            .map(|result| {
                let key = match result.group_values.first() {
                    Some(Value::Integer(value)) => *value,
                    other => panic!("unexpected group key: {other:?}"),
                };
                (key, result.aggregate_values)
            })
            .collect();
        normalized.sort_by_key(|(key, _)| *key);
        normalized
    }

    crate::instrumentation::begin_row_materialization_probe();
    crate::instrumentation::begin_artifact_columnar_group_probe();
    let typed = table
        .compute_grouped_aggregates(
            &[0],
            &[
                (AggregateOp::CountStar, 0),
                (AggregateOp::Sum, 1),
                (AggregateOp::Avg, 1),
                (AggregateOp::Min, 1),
                (AggregateOp::Max, 1),
            ],
        )
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();
    let materialization = crate::instrumentation::end_row_materialization_probe();

    let scanner = table
        .compute_grouped_aggregates_scanner(
            &[0],
            &[
                (AggregateOp::CountStar, 0),
                (AggregateOp::Sum, 1),
                (AggregateOp::Avg, 1),
                (AggregateOp::Min, 1),
                (AggregateOp::Max, 1),
            ],
            None,
        )
        .unwrap();

    let typed = normalize(typed);
    let scanner = normalize(scanner);
    assert_eq!(
        typed, scanner,
        "artifact-backed columnar aggregate must preserve scanner integer overflow semantics"
    );
    for (_, aggregates) in &typed {
        assert!(
            aggregates
                .get(1)
                .and_then(Value::as_decimal_parts)
                .is_some(),
            "overflowing INTEGER SUM must stay exact: {aggregates:?}"
        );
        assert!(matches!(aggregates.get(2), Some(Value::Float(_))));
    }
    assert_eq!(shape.applies, 1);
    assert_eq!(shape.output_groups, 2);
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
}

#[test]
fn artifact_columnar_grouped_aggregate_repeated_runs_match_results_and_shape() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let rows: Vec<(i64, Row)> = (1..=70_000)
        .map(|id| {
            (
                id,
                Row::from_values(vec![
                    Value::Integer(id % 97),
                    Value::Integer((id * 13) % 10_000),
                ]),
            )
        })
        .collect();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    fn normalize(values: Vec<GroupedAggregateResult>) -> Vec<(i64, Vec<Value>)> {
        let mut normalized: Vec<(i64, Vec<Value>)> = values
            .into_iter()
            .map(|result| {
                let key = match result.group_values.first() {
                    Some(Value::Integer(value)) => *value,
                    other => panic!("unexpected group key: {other:?}"),
                };
                (key, result.aggregate_values)
            })
            .collect();
        normalized.sort_by_key(|(key, _)| *key);
        normalized
    }

    let mut observed = Vec::new();
    for run in 0..2 {
        crate::instrumentation::begin_artifact_columnar_group_probe();
        crate::instrumentation::begin_row_materialization_probe();
        let grouped = table
            .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
            .unwrap();
        let materialization = crate::instrumentation::end_row_materialization_probe();
        let shape = crate::instrumentation::end_artifact_columnar_group_probe();
        assert_eq!(
            materialization.rows, 0,
            "run {run} must stay on columnar aggregate path"
        );
        assert_eq!(
            materialization.values, 0,
            "run {run} must not create per-row Values"
        );
        observed.push((run, normalize(grouped), shape));
    }

    assert_eq!(observed[0].1, observed[1].1);
    assert_eq!(observed[0].2.applies, 1);
    assert_eq!(observed[1].2.applies, 1);
    assert_eq!(observed[0].2.row_groups, observed[1].2.row_groups);
    assert_eq!(observed[0].2.selected_blocks, observed[1].2.selected_blocks);
    assert_eq!(observed[0].2.input_rows, observed[1].2.input_rows);
    assert_eq!(observed[0].2.output_groups, observed[1].2.output_groups);
    assert!(
        observed[0].2.row_groups > 1,
        "fixture must cover more than one DATA row group"
    );
}

#[test]
#[cfg(feature = "parallel")]
fn artifact_columnar_group_scheduler_merges_multiple_artifact_segments() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let first_rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Integer(20)]),
        ),
    ];
    let second_rows = vec![
        (
            3,
            Row::from_values(vec![Value::Integer(1), Value::Integer(5)]),
        ),
        (
            4,
            Row::from_values(vec![Value::Integer(3), Value::Integer(30)]),
        ),
    ];
    let (segment_mgr, _dir) = make_two_artifact_segment_mgr(&schema, &first_rows, &second_rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_artifact_columnar_group_probe();
    crate::instrumentation::begin_row_materialization_probe();
    let mut grouped = table
        .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
        .unwrap();
    let materialization = crate::instrumentation::end_row_materialization_probe();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();

    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Integer(value)) => *value,
        other => panic!("unexpected group key: {other:?}"),
    });
    assert_eq!(
        grouped
            .into_iter()
            .map(|result| (result.group_values, result.aggregate_values))
            .collect::<Vec<_>>(),
        vec![
            (
                vec![Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(15)]
            ),
            (
                vec![Value::Integer(2)],
                vec![Value::Integer(1), Value::Integer(20)]
            ),
            (
                vec![Value::Integer(3)],
                vec![Value::Integer(1), Value::Integer(30)]
            ),
        ]
    );
    assert_eq!(shape.applies, 1);
    assert_eq!(shape.scheduler_runs, 1);
    assert_eq!(shape.scheduled_segments, 2);
    assert_eq!(shape.local_merges, 2);
    assert_eq!(shape.merged_groups, 3);
    assert_eq!(shape.output_groups, 3);
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
}

#[test]
fn artifact_columnar_grouped_aggregate_falls_back_for_text_key() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Text, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::text("north"), Value::Integer(10)]),
        ),
        (
            2,
            Row::from_values(vec![Value::text("south"), Value::Integer(20)]),
        ),
        (
            3,
            Row::from_values(vec![Value::text("north"), Value::Integer(5)]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_artifact_columnar_group_probe();
    let mut grouped = table
        .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();

    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Text(value)) => value.to_string(),
        other => panic!("unexpected group key: {other:?}"),
    });
    assert_eq!(
        shape.applies, 0,
        "text keys are not part of the narrow ArtifactColumnarGroupPlan"
    );
    assert_eq!(shape.fallbacks, 1);
    assert_eq!(shape.fallback_group_key, 1);
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].group_values, vec![Value::text("north")]);
    assert_eq!(
        grouped[0].aggregate_values,
        vec![Value::Integer(2), Value::Integer(15)]
    );
    assert_eq!(grouped[1].group_values, vec![Value::text("south")]);
    assert_eq!(
        grouped[1].aggregate_values,
        vec![Value::Integer(1), Value::Integer(20)]
    );
}

#[test]
fn artifact_columnar_grouped_aggregate_falls_back_for_hot_overlay() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let cold_rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Integer(20)]),
        ),
    ];
    let hot_rows = vec![(
        3,
        Row::from_values(vec![Value::Integer(1), Value::Integer(5)]),
    )];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &cold_rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, hot_rows)), segment_mgr);

    crate::instrumentation::begin_artifact_columnar_group_probe();
    let mut grouped = table
        .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();

    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Integer(value)) => *value,
        other => panic!("unexpected group key: {other:?}"),
    });
    assert_eq!(
        shape.applies, 0,
        "hot rows require the semantic scanner path today"
    );
    assert_eq!(shape.fallbacks, 1);
    assert_eq!(shape.fallback_row_state, 1);
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].group_values, vec![Value::Integer(1)]);
    assert_eq!(
        grouped[0].aggregate_values,
        vec![Value::Integer(2), Value::Integer(15)]
    );
    assert_eq!(grouped[1].group_values, vec![Value::Integer(2)]);
    assert_eq!(
        grouped[1].aggregate_values,
        vec![Value::Integer(1), Value::Integer(20)]
    );
}

#[test]
fn artifact_columnar_grouped_aggregate_falls_back_for_hot_shadow_update() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let cold_rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Integer(20)]),
        ),
    ];
    let hot_rows = vec![(
        1,
        Row::from_values(vec![Value::Integer(2), Value::Integer(100)]),
    )];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &cold_rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, hot_rows)), segment_mgr);

    crate::instrumentation::begin_artifact_columnar_group_probe();
    let grouped = table
        .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();

    assert_eq!(
        shape.applies, 0,
        "hot shadow updates require the semantic scanner path today"
    );
    assert_eq!(shape.fallbacks, 1);
    assert_eq!(shape.fallback_row_state, 1);
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].group_values, vec![Value::Integer(2)]);
    assert_eq!(
        grouped[0].aggregate_values,
        vec![Value::Integer(2), Value::Integer(120)]
    );
}

#[test]
fn artifact_columnar_grouped_aggregate_falls_back_for_committed_tombstone() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(1), Value::Integer(5)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(2), Value::Integer(20)]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    segment_mgr.add_tombstones(&[2], 20);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_artifact_columnar_group_probe();
    let mut grouped = table
        .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();

    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Integer(value)) => *value,
        other => panic!("unexpected group key: {other:?}"),
    });
    assert_eq!(
        shape.applies, 0,
        "committed tombstones require the semantic scanner path today"
    );
    assert_eq!(shape.fallbacks, 1);
    assert_eq!(shape.fallback_row_state, 1);
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].group_values, vec![Value::Integer(1)]);
    assert_eq!(
        grouped[0].aggregate_values,
        vec![Value::Integer(1), Value::Integer(10)]
    );
    assert_eq!(grouped[1].group_values, vec![Value::Integer(2)]);
    assert_eq!(
        grouped[1].aggregate_values,
        vec![Value::Integer(1), Value::Integer(20)]
    );
}

#[test]
fn artifact_columnar_grouped_aggregate_falls_back_for_multi_key_group_by() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("shard", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let rows = vec![
        (
            1,
            Row::from_values(vec![
                Value::Integer(1),
                Value::Integer(10),
                Value::Integer(5),
            ]),
        ),
        (
            2,
            Row::from_values(vec![
                Value::Integer(1),
                Value::Integer(10),
                Value::Integer(7),
            ]),
        ),
        (
            3,
            Row::from_values(vec![
                Value::Integer(1),
                Value::Integer(20),
                Value::Integer(11),
            ]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);

    crate::instrumentation::begin_artifact_columnar_group_probe();
    let mut grouped = table
        .compute_grouped_aggregates(
            &[0, 1],
            &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 2)],
        )
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();

    grouped.sort_by_key(|result| {
        let bucket = match result.group_values.first() {
            Some(Value::Integer(value)) => *value,
            other => panic!("unexpected bucket key: {other:?}"),
        };
        let shard = match result.group_values.get(1) {
            Some(Value::Integer(value)) => *value,
            other => panic!("unexpected shard key: {other:?}"),
        };
        (bucket, shard)
    });
    assert_eq!(
        shape.applies, 0,
        "multi-key GROUP BY is not part of the narrow ArtifactColumnarGroupPlan"
    );
    assert_eq!(shape.fallbacks, 1);
    assert_eq!(shape.fallback_group_key, 1);
    assert_eq!(grouped.len(), 2);
    assert_eq!(
        grouped[0].group_values,
        vec![Value::Integer(1), Value::Integer(10)]
    );
    assert_eq!(
        grouped[0].aggregate_values,
        vec![Value::Integer(2), Value::Integer(12)]
    );
    assert_eq!(
        grouped[1].group_values,
        vec![Value::Integer(1), Value::Integer(20)]
    );
    assert_eq!(
        grouped[1].aggregate_values,
        vec![Value::Integer(1), Value::Integer(11)]
    );
}

#[test]
fn artifact_columnar_grouped_aggregate_falls_back_for_pending_tombstone() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(1), Value::Integer(5)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(2), Value::Integer(20)]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, vec![])), segment_mgr);
    table.segment_mgr.add_pending_tombstone(table.txn_id(), 2);

    crate::instrumentation::begin_artifact_columnar_group_probe();
    let mut grouped = table
        .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
        .unwrap();
    let shape = crate::instrumentation::end_artifact_columnar_group_probe();

    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Integer(value)) => *value,
        other => panic!("unexpected group key: {other:?}"),
    });
    assert_eq!(
        shape.applies, 0,
        "pending transaction tombstones require the semantic scanner path today"
    );
    assert_eq!(shape.fallbacks, 1);
    assert_eq!(shape.fallback_row_state, 1);
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].group_values, vec![Value::Integer(1)]);
    assert_eq!(
        grouped[0].aggregate_values,
        vec![Value::Integer(1), Value::Integer(10)]
    );
    assert_eq!(grouped[1].group_values, vec![Value::Integer(2)]);
    assert_eq!(
        grouped[1].aggregate_values,
        vec![Value::Integer(1), Value::Integer(20)]
    );
}

#[test]
fn artifact_columnar_grouped_aggregate_declines_snapshot_reads() {
    let schema = SchemaBuilder::new("test")
        .column("bucket", DataType::Integer, true, false)
        .column("amount", DataType::Integer, true, false)
        .build();
    let rows = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::Integer(10)]),
    )];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::with_snapshot_seq(
        Box::new(MockHotTable::new(schema, vec![])),
        segment_mgr,
        1,
    );

    assert!(
        table
            .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
            .is_none(),
        "snapshot reads decline storage grouped aggregation until snapshot-aware proof exists"
    );
}

#[test]
fn test_pk_metadata_refinement_uses_row_ids_without_block_reads() {
    let schema = SchemaBuilder::new("test")
        .add_primary_key("id", DataType::Integer)
        .column("value", DataType::Float, true, false)
        .build();
    let rows: Vec<(i64, Row)> = (1..=10)
        .map(|id| {
            (
                id,
                Row::from_values(vec![Value::Integer(id), Value::Float(id as f64)]),
            )
        })
        .collect();
    let mut builder = VolumeBuilder::with_capacity(&schema, rows.len());
    for (id, row) in &rows {
        builder.add_row(*id, row);
    }
    let volume = builder.finish();
    let mapping =
        super::super::writer::compute_column_mapping_with_drops(&schema, &volume, &[], 0, &[]);

    let eq = [("id", radixdb_core::Operator::Eq, &Value::Integer(6))];
    assert_eq!(
        SegmentedTable::refine_pk_metadata_range(&volume, &schema, &mapping, &eq, 0, 10),
        (false, 5, 6)
    );

    let range = [
        ("id", radixdb_core::Operator::Gte, &Value::Integer(3)),
        ("id", radixdb_core::Operator::Lte, &Value::Integer(7)),
    ];
    assert_eq!(
        SegmentedTable::refine_pk_metadata_range(&volume, &schema, &mapping, &range, 0, 10),
        (false, 2, 7)
    );

    let missing = [("id", radixdb_core::Operator::Eq, &Value::Integer(99))];
    assert_eq!(
        SegmentedTable::refine_pk_metadata_range(&volume, &schema, &mapping, &missing, 0, 10),
        (true, 0, 0)
    );
}

#[test]
fn metadata_primary_key_count_uses_artifact_row_ids_without_payload_reads() {
    let schema = SchemaBuilder::new("test")
        .add_primary_key("id", DataType::Integer)
        .column("payload", DataType::Text, false, false)
        .build();
    let cold_rows: Vec<(i64, Row)> = (1..=6_i64)
        .map(|id| {
            (
                id,
                Row::from_values(vec![
                    Value::Integer(id),
                    Value::text(format!("cold-payload-{id}")),
                ]),
            )
        })
        .collect();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &cold_rows);
    // `4` shadows a cold row and `8` exists only in the hot tail.  A
    // pending delete of cold `5` must be reflected without reading its
    // artifact-backed payload block.
    segment_mgr.add_pending_tombstone(1, 5);
    let hot_rows = vec![
        (
            4,
            Row::from_values(vec![Value::Integer(4), Value::text("hot-replacement")]),
        ),
        (
            8,
            Row::from_values(vec![Value::Integer(8), Value::text("hot-only")]),
        ),
    ];
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, hot_rows)), segment_mgr);

    let lower = Value::Integer(3);
    let upper = Value::Integer(8);
    let comparisons = [
        ("id", radixdb_core::Operator::Gte, &lower),
        ("id", radixdb_core::Operator::Lte, &upper),
    ];
    let range =
        IntegerPrimaryKeyRange::from_conjunctive_comparisons(&comparisons, "id", true).unwrap();

    crate::instrumentation::begin_volume_read_probe();
    crate::instrumentation::begin_decompression_probe();
    crate::instrumentation::begin_row_materialization_probe();
    crate::instrumentation::begin_metadata_pk_count_probe();
    let count = table
        .count_visible_integer_primary_key_range(&range)
        .expect("metadata path is applicable")
        .unwrap();
    let metadata = crate::instrumentation::end_metadata_pk_count_probe();
    let materialization = crate::instrumentation::end_row_materialization_probe();
    let decompression = crate::instrumentation::end_decompression_probe();
    let volume_read = crate::instrumentation::end_volume_read_probe();

    assert_eq!(count, 4, "visible ids are 3, 4, 6 and 8");
    assert_eq!(volume_read.calls, 0);
    assert_eq!(decompression.calls, 0);
    assert_eq!(materialization.rows, 0);
    assert_eq!(materialization.values, 0);
    assert_eq!(metadata.attempts, 1);
    assert_eq!(metadata.applied, 1);
    assert_eq!(metadata.intervals, 1);
    assert_eq!(metadata.candidate_rows, 5);
    assert_eq!(metadata.visible_rows, 4);
    assert_eq!(metadata.visibility_exclusions, 1);
    assert_eq!(metadata.hot_candidates, 2);
    assert_eq!(metadata.fallbacks, 0);

    let missing = Value::Integer(99);
    let missing_comparison = [("id", radixdb_core::Operator::Eq, &missing)];
    let missing_range =
        IntegerPrimaryKeyRange::from_conjunctive_comparisons(&missing_comparison, "id", true)
            .unwrap();
    crate::instrumentation::begin_volume_read_probe();
    crate::instrumentation::begin_decompression_probe();
    crate::instrumentation::begin_metadata_pk_count_probe();
    let missing_count = table
        .count_visible_integer_primary_key_range(&missing_range)
        .expect("metadata path remains applicable for a miss")
        .unwrap();
    let missing_metadata = crate::instrumentation::end_metadata_pk_count_probe();
    let missing_decompression = crate::instrumentation::end_decompression_probe();
    let missing_volume_read = crate::instrumentation::end_volume_read_probe();
    assert_eq!(missing_count, 0);
    assert_eq!(missing_volume_read.calls, 0);
    assert_eq!(missing_decompression.calls, 0);
    assert_eq!(missing_metadata.attempts, 1);
    assert_eq!(missing_metadata.applied, 1);
    assert_eq!(missing_metadata.candidate_rows, 0);
    assert_eq!(missing_metadata.visible_rows, 0);
}

#[test]
fn metadata_primary_key_count_falls_back_for_snapshot_reads() {
    let schema = SchemaBuilder::new("test")
        .add_primary_key("id", DataType::Integer)
        .column("payload", DataType::Text, false, false)
        .build();
    let cold_rows = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::text("cold")]),
    )];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &cold_rows);
    let table = SegmentedTable::with_snapshot_seq(
        Box::new(MockHotTable::new(schema, Vec::new())),
        segment_mgr,
        1,
    );
    let value = Value::Integer(1);
    let comparisons = [("id", radixdb_core::Operator::Eq, &value)];
    let range =
        IntegerPrimaryKeyRange::from_conjunctive_comparisons(&comparisons, "id", true).unwrap();

    assert!(
            table
                .count_visible_integer_primary_key_range(&range)
                .is_none(),
            "snapshot semantics use the established scanner fallback until the metadata proof covers them"
        );
}

#[test]
fn metadata_primary_key_count_falls_back_during_seal_overlap() {
    let schema = SchemaBuilder::new("test")
        .add_primary_key("id", DataType::Integer)
        .column("payload", DataType::Text, false, false)
        .build();
    let cold_rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::text("cold-1")]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::text("cold-2")]),
        ),
    ];
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &cold_rows);
    // The production seal protocol sets this before hot rows and their new
    // cold segment can coexist. Set the exact state directly rather than
    // using a timing-sensitive concurrent checkpoint test.
    segment_mgr.set_seal_overlap(cold_rows.len());
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, Vec::new())), segment_mgr);
    let value = Value::Integer(1);
    let comparisons = [("id", radixdb_core::Operator::Gte, &value)];
    let range =
        IntegerPrimaryKeyRange::from_conjunctive_comparisons(&comparisons, "id", true).unwrap();

    crate::instrumentation::begin_metadata_pk_count_probe();
    assert!(
        table
            .count_visible_integer_primary_key_range(&range)
            .is_none(),
        "overlap must route to the established generic scanner path"
    );
    let probe = crate::instrumentation::end_metadata_pk_count_probe();
    assert_eq!(probe.attempts, 1);
    assert_eq!(probe.applied, 0);
    assert_eq!(probe.fallbacks, 1);
    assert_eq!(probe.fallback_seal_overlap, 1);
}

#[test]
fn metadata_primary_key_count_falls_back_for_hot_only_table() {
    let schema = SchemaBuilder::new("test")
        .add_primary_key("id", DataType::Integer)
        .column("payload", DataType::Text, false, false)
        .build();
    let hot_rows = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::text("hot")]),
    )];
    let segment_mgr = Arc::new(SegmentManager::new("test", None));
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, hot_rows)), segment_mgr);
    let value = Value::Integer(1);
    let comparisons = [("id", radixdb_core::Operator::Eq, &value)];
    let range =
        IntegerPrimaryKeyRange::from_conjunctive_comparisons(&comparisons, "id", true).unwrap();

    crate::instrumentation::begin_metadata_pk_count_probe();
    assert!(table
        .count_visible_integer_primary_key_range(&range)
        .is_none());
    let probe = crate::instrumentation::end_metadata_pk_count_probe();
    assert_eq!(probe.attempts, 1);
    assert_eq!(probe.applied, 0);
    assert_eq!(probe.fallbacks, 1);
    assert_eq!(probe.fallback_unsupported, 1);
}

#[test]
fn test_filtered_aggregate_cold_projection_prunes_unused_columns() {
    let projection = SegmentedTable::build_cold_aggregate_projection(
        6,
        &[
            (AggregateOp::Sum, 4),
            (AggregateOp::CountStar, 0),
            (AggregateOp::Min, 2),
        ],
        &[],
    )
    .unwrap();

    assert_eq!(projection.columns, vec![4, 2]);
    assert_eq!(
        projection.aggregates,
        vec![
            (AggregateOp::Sum, 0),
            (AggregateOp::CountStar, 0),
            (AggregateOp::Min, 1),
        ]
    );
}

#[test]
fn test_grouped_aggregate_cold_projection_keeps_group_key_and_remaps_aggs() {
    let projection = SegmentedTable::build_cold_aggregate_projection(
        6,
        &[
            (AggregateOp::CountStar, 0),
            (AggregateOp::Sum, 4),
            (AggregateOp::Count, 1),
        ],
        &[1],
    )
    .unwrap();

    assert_eq!(projection.columns, vec![1, 4]);
    assert_eq!(projection.positions[1], Some(0));
    assert_eq!(projection.positions[4], Some(1));
    assert_eq!(
        projection.aggregates,
        vec![
            (AggregateOp::CountStar, 0),
            (AggregateOp::Sum, 1),
            (AggregateOp::Count, 0),
        ]
    );
}

#[test]
fn test_count_star_cold_projection_uses_empty_exact_projection() {
    let projection =
        SegmentedTable::build_cold_aggregate_projection(5, &[(AggregateOp::CountStar, 0)], &[])
            .unwrap();

    assert!(projection.columns.is_empty());
    assert_eq!(projection.aggregates, vec![(AggregateOp::CountStar, 0)]);
}

#[test]
fn test_segmented_table_scan_exact_empty_projection_returns_zero_width_cold_rows() {
    let schema = aggregate_projection_test_schema();
    let rows = aggregate_projection_test_rows();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let hot = MockHotTable::new(schema, Vec::new());
    let table = SegmentedTable::new(Box::new(hot), segment_mgr);

    let mut scanner = table.scan_exact_projection(&[], None).unwrap();

    assert!(scanner.next());
    assert_eq!(
        scanner.row().len(),
        0,
        "table-level exact-empty scan must preserve zero-width rows for cold artifact-backed"
    );
    scanner.close().unwrap();
}

#[test]
fn test_segmented_table_scan_exact_projection_prunes_unused_payload_cold_rows() {
    let schema = aggregate_projection_test_schema();
    let rows = aggregate_projection_test_rows();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let hot = MockHotTable::new(schema, Vec::new());
    let table = SegmentedTable::new(Box::new(hot), segment_mgr);

    let mut scanner = table.scan_exact_projection(&[1], None).unwrap();

    assert!(scanner.next());
    let row = scanner.row();
    assert_eq!(
        row.len(),
        1,
        "cold artifact-backed exact projection must not materialize unused payload columns"
    );
    assert_eq!(row.get(0), Some(&Value::Float(10.0)));
    assert!(
        !row.iter().any(
            |value| matches!(value, Value::Text(text) if text.as_str().starts_with("unused-"))
        ),
        "payload column leaked into a projected cold artifact-backed row"
    );
    scanner.close().unwrap();
}

#[test]
fn test_segmented_table_empty_hot_snapshot_keeps_artifact_typed_batch_contract() {
    let schema = aggregate_projection_test_schema();
    let rows = aggregate_projection_test_rows();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let hot = MockHotTable::new(schema, Vec::new());
    let table = SegmentedTable::new(Box::new(hot), segment_mgr);

    let mut scanner = table.scan_exact_projection(&[1], None).unwrap();
    assert!(
        scanner.supports_typed_batches(),
        "an empty hot snapshot must not inject a row-only MVCC scanner"
    );
    crate::instrumentation::begin_row_materialization_probe();
    let batch = scanner
        .next_typed_batch()
        .expect("typed scan succeeds")
        .expect("cold artifact-backed group exists");
    let materialization = crate::instrumentation::end_row_materialization_probe();
    assert_eq!(batch.row_count(), rows.len());
    assert_eq!(batch.columns().len(), 1);
    assert_eq!(
        materialization.rows, 0,
        "table-level typed scan must bypass the row adapter"
    );
}

#[test]
fn r8_l01_batch_f_hot_overlay_keeps_typed_transport_for_cold_prefix_and_hot_tail() {
    let schema = aggregate_projection_test_schema();
    let cold_rows = aggregate_projection_test_rows();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &cold_rows);
    let hot_rows = vec![(
        4,
        Row::from_values(vec![
            Value::Integer(4),
            Value::Float(40.0),
            Value::text("hot-unused"),
        ]),
    )];
    let hot = MockHotTable::new(schema, hot_rows);
    let table = SegmentedTable::new(Box::new(hot), segment_mgr);

    let mut scanner = table.scan_exact_projection(&[1], None).unwrap();
    assert!(
        scanner.supports_typed_batches(),
        "one hot row must not disable typed transport for the cold prefix"
    );
    let mut values = Vec::new();
    while let Some(batch) = scanner.next_typed_batch().unwrap() {
        let ColumnData::Float64 {
            values: batch_values,
            nulls,
        } = &batch.columns()[0]
        else {
            panic!("projected Float must remain a typed Float64 column")
        };
        assert!(nulls.iter().all(|is_null| !*is_null));
        values.extend(batch_values);
    }
    values.sort_by(f64::total_cmp);
    assert_eq!(values, vec![10.0, 20.0, 30.0, 40.0]);
    scanner.close().unwrap();
}

#[test]
fn jr14_grouped_fetch_decodes_each_projected_artifact_block_once_and_preserves_order() {
    let schema = aggregate_projection_test_schema();
    let rows = aggregate_projection_test_rows();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, Vec::new())), segment_mgr);
    let requested = [3, 1, 3, 2];
    crate::instrumentation::begin_decompression_probe();
    let fetched = table
        .collect_rows_by_ids_projected(&requested, &[0, 1, 2])
        .unwrap();
    let decompression = crate::instrumentation::end_decompression_probe();
    assert_eq!(
            decompression.calls, 3,
            "one row group with three projected columns must decode three blocks, not one block per row"
        );
    assert_eq!(
        fetched
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect::<Vec<_>>(),
        requested
    );
    assert_eq!(
        fetched
            .iter()
            .map(|(_, row)| row.get(1).cloned())
            .collect::<Vec<_>>(),
        vec![
            Some(Value::Float(30.0)),
            Some(Value::Float(10.0)),
            Some(Value::Float(30.0)),
            Some(Value::Float(20.0)),
        ]
    );
}

#[test]
fn test_segmented_table_collect_rows_by_ids_projected_prunes_cold_artifact_payload() {
    let schema = aggregate_projection_test_schema();
    let rows = aggregate_projection_test_rows();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let hot = MockHotTable::new(schema, Vec::new());
    let table = SegmentedTable::new(Box::new(hot), segment_mgr);

    crate::instrumentation::begin_row_materialization_probe();
    let rows = table.collect_rows_by_ids_projected(&[2], &[1]).unwrap();
    let materialization = crate::instrumentation::end_row_materialization_probe();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 2);
    assert_eq!(
        rows[0].1.len(),
        1,
        "row-id projected lookup must not materialize unused cold artifact-backed columns"
    );
    assert_eq!(rows[0].1.get(0), Some(&Value::Float(20.0)));
    assert_eq!(
        materialization.values, 1,
        "projected cold artifact-backed row-id lookup should materialize only requested values"
    );
}

#[test]
fn test_segmented_batch_membership_is_metadata_only_and_snapshot_aware() {
    let schema = SchemaBuilder::new("test")
        .add_primary_key("id", DataType::Integer)
        .column("payload", DataType::Text, false, false)
        .build();
    let cold_rows: Vec<(i64, Row)> = (1..=3_i64)
        .map(|id| {
            (
                id,
                Row::from_values(vec![Value::Integer(id), Value::text(format!("cold-{id}"))]),
            )
        })
        .collect();
    let (segment_mgr, _dir) = make_artifact_segment_mgr(&schema, &cold_rows);
    segment_mgr.add_tombstones(&[1], 20);
    // A hot replacement must remain visible even while its stale cold copy
    // is pending tombstone in the current transaction.
    segment_mgr.add_pending_tombstone(1, 2);
    segment_mgr.add_pending_tombstone(1, 3);
    let hot_rows = vec![(
        2,
        Row::from_values(vec![Value::Integer(2), Value::text("hot-2")]),
    )];
    let ids = [1, 2, 2, 3, 9];

    let current = SegmentedTable::new(
        Box::new(MockHotTable::new(schema.clone(), hot_rows.clone())),
        Arc::clone(&segment_mgr),
    );
    crate::instrumentation::begin_volume_read_probe();
    crate::instrumentation::begin_decompression_probe();
    crate::instrumentation::begin_row_materialization_probe();
    let mut current_matches = [true; 5];
    let current_hits = current
        .probe_visible_row_ids(&ids, &mut current_matches)
        .unwrap();
    let current_materialization = crate::instrumentation::end_row_materialization_probe();
    let current_decompression = crate::instrumentation::end_decompression_probe();
    let current_volume_read = crate::instrumentation::end_volume_read_probe();
    assert_eq!(current_matches, [false, true, true, false, false]);
    assert_eq!(current_hits, 2);
    assert_eq!(current_volume_read.calls, 0);
    assert_eq!(current_decompression.calls, 0);
    assert_eq!(current_materialization.rows, 0);

    let old_snapshot = SegmentedTable::with_snapshot_seq(
        Box::new(MockHotTable::new(schema, hot_rows)),
        segment_mgr,
        10,
    );
    let mut old_matches = [false; 5];
    let old_hits = old_snapshot
        .probe_visible_row_ids(&ids, &mut old_matches)
        .unwrap();
    assert_eq!(old_matches, [true, true, true, false, false]);
    assert_eq!(old_hits, 3);
}

#[test]
fn integer_primary_key_insert_batch_checks_all_cold_segments_without_payload_reads() {
    let schema = test_schema();
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = Arc::new(SegmentManager::new("test", None));
    for segment in 0..4_i64 {
        let first = segment * 5 + 1;
        let rows: Vec<(i64, Row)> = (first..first + 5)
            .map(|id| {
                (
                    id,
                    Row::from_values(vec![Value::Integer(id), Value::Float(id as f64)]),
                )
            })
            .collect();
        register_artifact_segment(&segment_mgr, dir.path(), &schema, segment as u64 + 1, &rows);
    }
    let mut table = SegmentedTable::new(
        Box::new(MockHotTable::new(schema, Vec::new())),
        Arc::clone(&segment_mgr),
    );

    crate::instrumentation::begin_volume_read_probe();
    table
        .insert_batch(vec![
            Row::from_values(vec![Value::Integer(100), Value::Float(100.0)]),
            Row::from_values(vec![Value::Integer(101), Value::Float(101.0)]),
        ])
        .unwrap();
    let non_conflict_reads = crate::instrumentation::end_volume_read_probe();
    assert_eq!(
        non_conflict_reads.calls, 0,
        "monotonic INTEGER PK batches must intersect resident row-id metadata"
    );

    crate::instrumentation::begin_volume_read_probe();
    let conflict = table
        .insert_batch(vec![Row::from_values(vec![
            Value::Integer(12),
            Value::Float(12.0),
        ])])
        .unwrap_err();
    let conflict_reads = crate::instrumentation::end_volume_read_probe();
    assert!(matches!(
        conflict,
        radixdb_core::Error::PrimaryKeyConstraint { row_id: 12 }
    ));
    assert_eq!(conflict_reads.calls, 0);

    segment_mgr.add_pending_tombstone(table.txn_id(), 12);
    crate::instrumentation::begin_volume_read_probe();
    table
        .insert_batch(vec![Row::from_values(vec![
            Value::Integer(12),
            Value::Float(120.0),
        ])])
        .unwrap();
    let replacement_reads = crate::instrumentation::end_volume_read_probe();
    assert_eq!(
        replacement_reads.calls, 0,
        "same-transaction cold replacement keeps the metadata-only path"
    );
}

type RowIdRangeLog = Arc<Mutex<Vec<Vec<(i64, i64)>>>>;

/// Minimal test table for the hot buffer
struct MockHotTable {
    schema: Schema,
    rows: Vec<(i64, Row)>,
    scan_columns_log: Option<Arc<Mutex<Vec<Vec<usize>>>>>,
    delete_batches_log: Option<Arc<Mutex<Vec<Vec<i64>>>>>,
    unfiltered_collects: Option<Arc<AtomicUsize>>,
    membership_fence: Option<Arc<parking_lot::RwLock<()>>>,
    range_scan_log: Option<RowIdRangeLog>,
    zone_maps: Option<Arc<crate::volume::zonemap::TableZoneMap>>,
}

impl MockHotTable {
    fn new(schema: Schema, rows: Vec<(i64, Row)>) -> Self {
        Self {
            schema,
            rows,
            scan_columns_log: None,
            delete_batches_log: None,
            unfiltered_collects: None,
            membership_fence: None,
            range_scan_log: None,
            zone_maps: None,
        }
    }

    fn with_scan_columns_log(mut self, log: Arc<Mutex<Vec<Vec<usize>>>>) -> Self {
        self.scan_columns_log = Some(log);
        self
    }

    fn with_delete_batches_log(mut self, log: Arc<Mutex<Vec<Vec<i64>>>>) -> Self {
        self.delete_batches_log = Some(log);
        self
    }

    fn with_batch_a_oracle(
        mut self,
        unfiltered_collects: Arc<AtomicUsize>,
        membership_fence: Arc<parking_lot::RwLock<()>>,
    ) -> Self {
        self.unfiltered_collects = Some(unfiltered_collects);
        self.membership_fence = Some(membership_fence);
        self
    }

    fn with_zone_map_oracle(
        mut self,
        zone_maps: Arc<crate::volume::zonemap::TableZoneMap>,
        log: RowIdRangeLog,
    ) -> Self {
        self.zone_maps = Some(zone_maps);
        self.range_scan_log = Some(log);
        self
    }
}

impl Table for MockHotTable {
    fn name(&self) -> &str {
        "test"
    }
    fn schema(&self) -> &Schema {
        &self.schema
    }
    fn txn_id(&self) -> i64 {
        1
    }
    fn create_column(&mut self, _: &str, _: DataType, _: bool) -> Result<()> {
        Ok(())
    }
    fn drop_column(&mut self, _: &str) -> Result<()> {
        Ok(())
    }
    fn insert(&mut self, row: Row) -> Result<Row> {
        let id = self.rows.len() as i64 + 1000;
        self.rows.push((id, row.clone()));
        Ok(row)
    }
    fn insert_batch(&mut self, rows: Vec<Row>) -> Result<()> {
        for row in rows {
            self.insert(row)?;
        }
        Ok(())
    }
    fn update(
        &mut self,
        _: Option<&dyn Expression>,
        _: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
    ) -> Result<i32> {
        Ok(0)
    }
    fn update_by_row_ids(
        &mut self,
        _: &[i64],
        _: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
    ) -> Result<i32> {
        Ok(0)
    }
    fn delete_by_row_ids(&mut self, row_ids: &[i64]) -> Result<i32> {
        if let Some(log) = &self.delete_batches_log {
            log.lock().unwrap().push(row_ids.to_vec());
        }
        Ok(0)
    }
    fn get_active_row_ids(&self) -> Vec<i64> {
        self.rows.iter().map(|(id, _)| *id).collect()
    }
    fn collect_shadow_row_ids_into(&self, dest: &mut FxHashSet<i64>) {
        for (id, _) in &self.rows {
            dest.insert(*id);
        }
    }
    fn membership_fence(&self) -> Option<Arc<parking_lot::RwLock<()>>> {
        self.membership_fence.clone()
    }
    fn delete(&mut self, _: Option<&dyn Expression>) -> Result<i32> {
        Ok(0)
    }
    fn scan(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn Scanner>> {
        if let Some(log) = &self.scan_columns_log {
            log.lock().unwrap().push(column_indices.to_vec());
        }
        let rows = self.collect_all_rows(where_expr)?;
        Ok(Box::new(crate::traits::MVCCScanner::from_rows(
            rows,
            CompactArc::new(self.schema.clone()),
            column_indices.to_vec(),
        )))
    }
    fn scan_exact_projection(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn Scanner>> {
        self.scan(column_indices, where_expr)
    }
    fn scan_with_row_id_ranges(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        row_id_ranges: &[(i64, i64)],
    ) -> Result<Box<dyn Scanner>> {
        if let Some(log) = &self.range_scan_log {
            log.lock().unwrap().push(row_id_ranges.to_vec());
        }
        let mut rows = RowVec::with_capacity(self.rows.len());
        for (id, row) in &self.rows {
            if !row_id_ranges
                .iter()
                .any(|(min, max)| id >= min && id <= max)
            {
                continue;
            }
            if where_expr.is_some_and(|expr| !expr.evaluate_fast(row)) {
                continue;
            }
            rows.push((*id, row.clone()));
        }
        Ok(Box::new(crate::traits::MVCCScanner::from_rows(
            rows,
            CompactArc::new(self.schema.clone()),
            column_indices.to_vec(),
        )))
    }
    fn collect_all_rows(&self, where_expr: Option<&dyn Expression>) -> Result<RowVec> {
        if where_expr.is_none() {
            if let Some(counter) = &self.unfiltered_collects {
                counter.fetch_add(1, Ordering::AcqRel);
            }
        }
        let mut rv = RowVec::with_capacity(self.rows.len());
        for (id, row) in &self.rows {
            if let Some(expr) = where_expr {
                if !expr.evaluate(row)? {
                    continue;
                }
            }
            rv.push((*id, row.clone()));
        }
        Ok(rv)
    }
    fn collect_rows_by_ids(&self, row_ids: &[i64]) -> Result<RowVec> {
        let mut rows = RowVec::with_capacity(row_ids.len());
        for row_id in row_ids {
            if let Some((_, row)) = self.rows.iter().find(|(id, _)| id == row_id) {
                rows.push((*row_id, row.clone()));
            }
        }
        Ok(rows)
    }
    fn collect_rows_by_ids_projected(
        &self,
        row_ids: &[i64],
        column_indices: &[usize],
    ) -> Result<RowVec> {
        self.collect_rows_by_ids(row_ids)?
            .into_iter()
            .map(|(row_id, row)| row.take_columns(column_indices).map(|row| (row_id, row)))
            .collect()
    }
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
    fn commit(&mut self) -> Result<()> {
        Ok(())
    }
    fn rollback(&mut self) {}
    fn rollback_to_timestamp(&self, _: i64) {}
    fn has_local_changes(&self) -> bool {
        false
    }
    fn create_index(&self, _: &str, _: &[&str], _: bool) -> Result<()> {
        Ok(())
    }
    fn build_index_with_type_detached(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        _: Option<IndexType>,
    ) -> Result<Arc<dyn Index>> {
        if columns.len() != 1 {
            return Err(radixdb_core::Error::NotSupported(
                "MockHotTable only builds single-column detached indexes".to_string(),
            ));
        }
        let (column_id, column) = self
            .schema
            .find_column(columns[0])
            .ok_or_else(|| radixdb_core::Error::ColumnNotFound(columns[0].to_string()))?;
        Ok(Arc::new(crate::index::BTreeIndex::new(
            name.to_string(),
            self.schema.table_name.clone(),
            column_id as i32,
            column.name.clone(),
            column.data_type,
            is_unique,
            0,
        )))
    }
    fn publish_detached_index(&self, _: Arc<dyn Index>) -> Result<()> {
        Ok(())
    }
    fn drop_index(&self, _: &str) -> Result<()> {
        Ok(())
    }
    fn rename_index(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn create_btree_index(&self, _: &str, _: bool, _: Option<&str>) -> Result<()> {
        Ok(())
    }
    fn drop_btree_index(&self, _: &str) -> Result<()> {
        Ok(())
    }
    fn rename_column(&mut self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn modify_column(&mut self, _: &str, _: DataType, _: bool) -> Result<()> {
        Ok(())
    }
    fn select(&self, _: &[&str], _: Option<&dyn Expression>) -> Result<Box<dyn QueryResult>> {
        Err(radixdb_core::Error::internal("not implemented"))
    }
    fn select_with_aliases(
        &self,
        _: &[&str],
        _: Option<&dyn Expression>,
        _: &FxHashMap<String, String>,
    ) -> Result<Box<dyn QueryResult>> {
        Err(radixdb_core::Error::internal("not implemented"))
    }
    fn select_as_of(
        &self,
        columns: &[&str],
        expr: Option<&dyn Expression>,
        _: &str,
        _: i64,
    ) -> Result<Box<dyn QueryResult>> {
        let mut rows = self.collect_all_rows(expr)?;
        let col_names = columns.iter().map(|c| c.to_string()).collect();
        Ok(Box::new(crate::traits::MemoryResult::with_rows(
            col_names,
            rows.drain_rows().collect(),
        )))
    }
    fn row_count(&self) -> usize {
        self.rows.len()
    }
    fn fast_row_count(&self) -> Option<usize> {
        Some(self.rows.len())
    }
    fn get_zone_maps(&self) -> Option<Arc<crate::volume::zonemap::TableZoneMap>> {
        self.zone_maps.clone()
    }
    fn max_column(&self, col_idx: usize) -> Option<Option<Value>> {
        let mut max: Option<Value> = None;
        for (_, row) in &self.rows {
            if let Some(val) = row.get(col_idx) {
                if !val.is_null() {
                    match &max {
                        None => max = Some(val.clone()),
                        Some(current) => {
                            if let Ok(std::cmp::Ordering::Greater) = val.compare(current) {
                                max = Some(val.clone());
                            }
                        }
                    }
                }
            }
        }
        Some(max)
    }
    fn min_column(&self, col_idx: usize) -> Option<Option<Value>> {
        let mut min: Option<Value> = None;
        for (_, row) in &self.rows {
            if let Some(val) = row.get(col_idx) {
                if !val.is_null() {
                    match &min {
                        None => min = Some(val.clone()),
                        Some(current) => {
                            if let Ok(std::cmp::Ordering::Less) = val.compare(current) {
                                min = Some(val.clone());
                            }
                        }
                    }
                }
            }
        }
        Some(min)
    }
    fn sum_column(&self, col_idx: usize) -> Option<DeferredSum> {
        let mut sum = DeferredSum::new();
        for (_, row) in &self.rows {
            if let Some(value) = row.get(col_idx) {
                sum.add_value(value);
            }
        }
        Some(sum)
    }
    fn get_partition_values(&self, column_name: &str) -> Option<Vec<Value>> {
        let col_idx = *self
            .schema
            .column_index_map()
            .get(&column_name.to_lowercase())?;
        let mut values = ValueSet::default();
        for (_, row) in &self.rows {
            if let Some(value) = row.get(col_idx) {
                if !value.is_null() {
                    values.insert(value.clone());
                }
            }
        }
        Some(values.into_iter().collect())
    }
    fn get_partition_count(&self, column_name: &str) -> Option<usize> {
        self.get_partition_values(column_name)
            .map(|values| values.len())
    }
}

fn test_schema() -> Schema {
    SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("value", DataType::Float, false, false)
        .build()
}

fn aggregate_projection_test_schema() -> Schema {
    SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("value", DataType::Float, false, false)
        .column("payload", DataType::Text, true, false)
        .build()
}

fn aggregate_projection_test_rows() -> Vec<(i64, Row)> {
    vec![
        (
            1,
            Row::from_values(vec![
                Value::Integer(1),
                Value::Float(10.0),
                Value::text("unused-1"),
            ]),
        ),
        (
            2,
            Row::from_values(vec![
                Value::Integer(2),
                Value::Float(20.0),
                Value::text("unused-2"),
            ]),
        ),
        (
            3,
            Row::from_values(vec![
                Value::Integer(3),
                Value::Float(30.0),
                Value::text("unused-3"),
            ]),
        ),
    ]
}

#[test]
fn test_filtered_aggregate_hot_side_uses_projected_scan() {
    let schema = aggregate_projection_test_schema();
    let scan_log = Arc::new(Mutex::new(Vec::new()));
    let hot = MockHotTable::new(schema.clone(), aggregate_projection_test_rows())
        .with_scan_columns_log(Arc::clone(&scan_log));
    let table = SegmentedTable::new(Box::new(hot), Arc::new(SegmentManager::new("test", None)));

    let mut filter = ComparisonExpr::gt("id", Value::Integer(1));
    filter.prepare_for_schema(&schema);

    let values = table
        .compute_filtered_aggregates_scanner(&[(AggregateOp::Sum, 1)], &filter)
        .unwrap();

    assert_eq!(values, vec![Value::Float(50.0)]);
    assert_eq!(*scan_log.lock().unwrap(), vec![vec![1]]);
}

#[test]
fn test_grouped_aggregate_hot_side_uses_projected_scan() {
    let schema = aggregate_projection_test_schema();
    let scan_log = Arc::new(Mutex::new(Vec::new()));
    let hot = MockHotTable::new(schema, aggregate_projection_test_rows())
        .with_scan_columns_log(Arc::clone(&scan_log));
    let table = SegmentedTable::new(Box::new(hot), Arc::new(SegmentManager::new("test", None)));

    let mut grouped = table
        .compute_grouped_aggregates_scanner(&[0], &[(AggregateOp::Sum, 1)], None)
        .unwrap();
    grouped.sort_by_key(|result| match result.group_values.first() {
        Some(Value::Integer(id)) => *id,
        other => panic!("unexpected group key: {other:?}"),
    });

    assert_eq!(
        grouped
            .iter()
            .map(|result| result.aggregate_values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Float(10.0)],
            vec![Value::Float(20.0)],
            vec![Value::Float(30.0)],
        ]
    );
    assert_eq!(*scan_log.lock().unwrap(), vec![vec![0, 1]]);
}

#[test]
fn test_segmented_select_streams_projected_rows() {
    let schema = aggregate_projection_test_schema();
    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            3,
            Row::from_values(vec![
                Value::Integer(3),
                Value::Float(30.0),
                Value::text("hot-unused"),
            ]),
        )],
    );
    let (segment_mgr, _dir) = make_artifact_segment_mgr(
        &schema,
        &[
            (
                1,
                Row::from_values(vec![
                    Value::Integer(1),
                    Value::Float(10.0),
                    Value::text("cold-unused-1"),
                ]),
            ),
            (
                2,
                Row::from_values(vec![
                    Value::Integer(2),
                    Value::Float(20.0),
                    Value::text("cold-unused-2"),
                ]),
            ),
        ],
    );
    let table = SegmentedTable::new(Box::new(hot), segment_mgr);

    let mut result = table.select(&["value"], None).unwrap();
    assert_eq!(result.columns(), &["value".to_string()]);

    let mut values = Vec::new();
    while result.next() {
        let row = result.take_row();
        assert_eq!(
            row.len(),
            1,
            "segmented select must expose projected row width"
        );
        values.push(row.get(0).cloned().unwrap());
    }
    result.close().unwrap();

    values.sort_by(|a, b| a.compare(b).unwrap());
    assert_eq!(
        values,
        vec![Value::Float(10.0), Value::Float(20.0), Value::Float(30.0)]
    );

    let mut current = table.select_as_of(&["value"], None, "CURRENT", 0).unwrap();
    assert_eq!(current.columns(), &["value".to_string()]);

    let mut current_values = Vec::new();
    while current.next() {
        let row = current.take_row();
        assert_eq!(
            row.len(),
            1,
            "segmented CURRENT select must reuse projected row width"
        );
        current_values.push(row.get(0).cloned().unwrap());
    }
    current.close().unwrap();

    current_values.sort_by(|a, b| a.compare(b).unwrap());
    assert_eq!(current_values, values);
}

#[test]
fn test_hot_only() {
    let schema = test_schema();
    let hot = MockHotTable::new(
        schema.clone(),
        vec![
            (
                1,
                Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
            ),
            (
                2,
                Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
            ),
        ],
    );
    let table = SegmentedTable::hot_only(Box::new(hot));

    assert_eq!(table.row_count(), 2);
    assert_eq!(table.segment_count(), 0);
}

#[test]
fn test_row_count_merges() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![
            (
                100,
                Row::from_values(vec![Value::Integer(100), Value::Float(500.0)]),
            ),
            (
                101,
                Row::from_values(vec![Value::Integer(101), Value::Float(600.0)]),
            ),
        ],
    );

    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(3), Value::Float(30.0)]),
        ),
    ];
    let mgr = make_segment_mgr(&schema, &seg_rows);

    let table = SegmentedTable::new(Box::new(hot), mgr);

    assert_eq!(table.row_count(), 5); // 3 segment + 2 hot
    assert_eq!(table.fast_row_count(), Some(5));
}

#[test]
fn seal_overlap_row_count_is_exact_and_disables_fast_count() {
    let schema = test_schema();
    let rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
    ];
    let manager = make_segment_mgr(&schema, &rows);
    manager.set_seal_overlap(rows.len());
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, rows.clone())), manager);

    assert_eq!(table.row_count(), 2);
    assert_eq!(table.fast_row_count(), None);
}

#[test]
fn test_collect_all_rows_merges() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            100,
            Row::from_values(vec![Value::Integer(100), Value::Float(500.0)]),
        )],
    );

    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
    ];
    let mgr = make_segment_mgr(&schema, &seg_rows);

    let table = SegmentedTable::new(Box::new(hot), mgr);

    let rows = table.collect_all_rows(None).unwrap();
    assert_eq!(rows.len(), 3); // 2 segment + 1 hot
}

#[test]
fn test_collect_all_rows_reads_artifact_backed_segment() {
    let schema = test_schema();
    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(3), Value::Float(30.0)]),
        ),
    ];

    let dir = tempfile::tempdir().unwrap();
    let fixture =
        crate::volume::test_artifact::build_artifact_volume(dir.path(), &schema, 42, &seg_rows);
    assert!(fixture.volume.is_cold());
    assert!(fixture.volume.artifact_source().is_some());

    let mgr = Arc::new(SegmentManager::new("test", None));
    mgr.register_segment(
        42,
        fixture.volume,
        SegmentMeta {
            segment_id: 42,
            file_path: fixture.relative_path,
            row_count: seg_rows.len(),
            min_row_id: 1,
            max_row_id: 3,
            schema_version: 0,
            seal_seq: 0,
            ..Default::default()
        },
        Some(&schema),
    )
    .unwrap();

    let hot = MockHotTable::new(schema.clone(), vec![]);
    let table = SegmentedTable::new(Box::new(hot), mgr);

    let rows = table.collect_all_rows(None).unwrap();
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(rows[1].1.get(1), Some(&Value::Float(20.0)));

    let mut scanner = table.scan(&[1], None).unwrap();
    let mut projected = Vec::new();
    while scanner.next() {
        projected.push(scanner.row().get(0).cloned().unwrap());
    }
    assert!(scanner.err().is_none());
    assert_eq!(
        projected,
        vec![Value::Float(10.0), Value::Float(20.0), Value::Float(30.0)]
    );

    let limited = table.collect_rows_with_limit(None, 2, 1).unwrap();
    let limited_ids: Vec<i64> = limited.iter().map(|(id, _)| *id).collect();
    assert_eq!(limited_ids, vec![2, 3]);

    let limited_unordered = table.collect_rows_with_limit_unordered(None, 2, 1).unwrap();
    let limited_unordered_ids: Vec<i64> = limited_unordered.iter().map(|(id, _)| *id).collect();
    assert_eq!(limited_unordered_ids, vec![2, 3]);

    let ordered = table
        .collect_rows_ordered_by_index("id", true, 2, 1)
        .unwrap();
    let ordered_ids: Vec<i64> = ordered.iter().map(|(id, _)| *id).collect();
    assert_eq!(ordered_ids, vec![2, 3]);

    let ordered_desc = table
        .collect_rows_ordered_by_index("id", false, 2, 1)
        .unwrap();
    let ordered_desc_ids: Vec<i64> = ordered_desc.iter().map(|(id, _)| *id).collect();
    assert_eq!(ordered_desc_ids, vec![2, 1]);

    let grouped = table.collect_rows_grouped_by_partition("id").unwrap();
    assert_eq!(grouped.len(), 3);

    let partition_rows = table
        .get_rows_for_partition_value("id", &Value::Integer(2))
        .unwrap();
    let partition_ids: Vec<i64> = partition_rows.iter().map(|(id, _)| *id).collect();
    assert_eq!(partition_ids, vec![2]);

    assert_eq!(table.get_partition_count("id"), Some(3));
    let partition_values = table.get_partition_values("id").unwrap();
    assert_eq!(partition_values.len(), 3);
    assert!(partition_values.contains(&Value::Integer(1)));
    assert!(partition_values.contains(&Value::Integer(2)));
    assert!(partition_values.contains(&Value::Integer(3)));

    let mut current = table
        .select_as_of(&["id", "value"], None, "CURRENT", 0)
        .unwrap();
    let mut current_ids = Vec::new();
    while current.next() {
        if let Some(Value::Integer(id)) = current.row().get(0) {
            current_ids.push(*id);
        }
    }
    assert_eq!(current_ids, vec![1, 2, 3]);

    let filter = crate::expression::ComparisonExpr::gt("id", Value::Integer(1));
    let filtered_aggs = table
        .compute_filtered_aggregates(
            &[
                (AggregateOp::CountStar, 0),
                (AggregateOp::Sum, 1),
                (AggregateOp::Min, 1),
                (AggregateOp::Max, 1),
            ],
            &filter,
        )
        .unwrap();
    assert_eq!(
        filtered_aggs,
        vec![
            Value::Integer(2),
            Value::Float(50.0),
            Value::Float(20.0),
            Value::Float(30.0)
        ]
    );

    let filtered_count = table
        .compute_filtered_aggregates(&[(AggregateOp::CountStar, 0)], &filter)
        .unwrap();
    assert_eq!(filtered_count, vec![Value::Integer(2)]);

    let grouped_aggs = table
        .compute_grouped_aggregates(&[0], &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)])
        .unwrap();
    let mut grouped_summary: Vec<(i64, Vec<Value>)> = grouped_aggs
        .into_iter()
        .map(|result| {
            let id = match result.group_values.first() {
                Some(Value::Integer(id)) => *id,
                other => panic!("unexpected grouped aggregate key: {other:?}"),
            };
            (id, result.aggregate_values)
        })
        .collect();
    grouped_summary.sort_by_key(|(id, _)| *id);
    assert_eq!(
        grouped_summary,
        vec![
            (1, vec![Value::Integer(1), Value::Float(10.0)]),
            (2, vec![Value::Integer(1), Value::Float(20.0)]),
            (3, vec![Value::Integer(1), Value::Float(30.0)]),
        ]
    );

    let multi_grouped_aggs = table
        .compute_grouped_aggregates(&[0, 1], &[(AggregateOp::CountStar, 0)])
        .unwrap();
    let mut multi_grouped_summary: Vec<(i64, u64, Vec<Value>)> = multi_grouped_aggs
        .into_iter()
        .map(|result| {
            let id = match result.group_values.first() {
                Some(Value::Integer(id)) => *id,
                other => panic!("unexpected multi grouped id key: {other:?}"),
            };
            let value_bits = match result.group_values.get(1) {
                Some(Value::Float(value)) => value.to_bits(),
                other => panic!("unexpected multi grouped value key: {other:?}"),
            };
            (id, value_bits, result.aggregate_values)
        })
        .collect();
    multi_grouped_summary.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        multi_grouped_summary,
        vec![
            (1, 10.0f64.to_bits(), vec![Value::Integer(1)]),
            (2, 20.0f64.to_bits(), vec![Value::Integer(1)]),
            (3, 30.0f64.to_bits(), vec![Value::Integer(1)]),
        ]
    );

    let filtered_grouped_aggs = table
        .compute_filtered_grouped_aggregates(
            &[0],
            &[(AggregateOp::CountStar, 0), (AggregateOp::Sum, 1)],
            &filter,
        )
        .unwrap();
    let mut filtered_grouped_summary: Vec<(i64, Vec<Value>)> = filtered_grouped_aggs
        .into_iter()
        .map(|result| {
            let id = match result.group_values.first() {
                Some(Value::Integer(id)) => *id,
                other => panic!("unexpected filtered grouped aggregate key: {other:?}"),
            };
            (id, result.aggregate_values)
        })
        .collect();
    filtered_grouped_summary.sort_by_key(|(id, _)| *id);
    assert_eq!(
        filtered_grouped_summary,
        vec![
            (2, vec![Value::Integer(1), Value::Float(20.0)]),
            (3, vec![Value::Integer(1), Value::Float(30.0)]),
        ]
    );

    let filtered_multi_grouped_aggs = table
        .compute_filtered_grouped_aggregates(&[0, 1], &[(AggregateOp::CountStar, 0)], &filter)
        .unwrap();
    let mut filtered_multi_grouped_summary: Vec<(i64, u64, Vec<Value>)> =
        filtered_multi_grouped_aggs
            .into_iter()
            .map(|result| {
                let id = match result.group_values.first() {
                    Some(Value::Integer(id)) => *id,
                    other => panic!("unexpected filtered multi grouped id key: {other:?}"),
                };
                let value_bits = match result.group_values.get(1) {
                    Some(Value::Float(value)) => value.to_bits(),
                    other => panic!("unexpected filtered multi grouped value key: {other:?}"),
                };
                (id, value_bits, result.aggregate_values)
            })
            .collect();
    filtered_multi_grouped_summary.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        filtered_multi_grouped_summary,
        vec![
            (2, 20.0f64.to_bits(), vec![Value::Integer(1)]),
            (3, 30.0f64.to_bits(), vec![Value::Integer(1)]),
        ]
    );

    let direct_row = table.segment_manager().get_cold_row(2).unwrap().unwrap();
    assert_eq!(direct_row.get(0), Some(&Value::Integer(2)));
    assert_eq!(direct_row.get(1), Some(&Value::Float(20.0)));

    let normalized_row = table
        .segment_manager()
        .get_cold_row_normalized(2, &schema)
        .unwrap()
        .unwrap();
    assert_eq!(normalized_row.get(0), Some(&Value::Integer(2)));
    assert_eq!(normalized_row.get(1), Some(&Value::Float(20.0)));

    let raw_segments = table.segment_manager().segments_raw();
    let cold_segment = raw_segments.get(&42).unwrap();
    assert!(
        cold_segment.volume.is_cold(),
        "direct cold row fetch must not materialize the DATA artifact"
    );
    assert!(cold_segment.volume.artifact_source().is_some());

    table
        .segment_manager()
        .add_pending_tombstone(table.txn_id(), 2);
    assert_eq!(table.get_active_row_ids(), vec![1, 3]);
    let sum = table.sum_column(1).unwrap();
    assert_eq!(sum.as_f64(), 40.0);
    assert_eq!(sum.count(), 2);
    assert_eq!(table.min_column(1), Some(Some(Value::Float(10.0))));
    assert_eq!(table.max_column(1), Some(Some(Value::Float(30.0))));
}

#[test]
fn test_unique_index_validation_reads_artifact_metadata_only_segment() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::text("dup")]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::text("dup")]),
        ),
    ];
    let (mgr, _dir) = make_artifact_segment_mgr(&schema, &seg_rows);
    let hot = MockHotTable::new(schema.clone(), vec![]);
    let table = SegmentedTable::new(Box::new(hot), mgr);

    let err = table
        .create_index("idx_name_unique", &["name"], true)
        .expect_err("duplicate artifact-backed cold values must reject unique index creation");
    assert!(err.to_string().contains("unique constraint"));
}

#[test]
fn test_unique_index_validation_accepts_distinct_artifact_metadata_only_segment() {
    let schema = SchemaBuilder::new("test")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, false, false)
        .build();
    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::text("alice")]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::text("bob")]),
        ),
    ];
    let (mgr, _dir) = make_artifact_segment_mgr(&schema, &seg_rows);
    let hot = MockHotTable::new(schema.clone(), vec![]);
    let table = SegmentedTable::new(Box::new(hot), mgr);

    table
        .create_index("idx_name_unique", &["name"], true)
        .expect("distinct artifact-backed cold values should allow unique index creation");
}

#[test]
fn test_max_column_merges() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            100,
            Row::from_values(vec![Value::Integer(100), Value::Float(500.0)]),
        )],
    );

    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(30.0)]),
        ),
    ];
    let mgr = make_segment_mgr(&schema, &seg_rows);

    let table = SegmentedTable::new(Box::new(hot), mgr);

    let max = table.max_column(1);
    assert_eq!(max, Some(Some(Value::Float(500.0))));
}

#[test]
fn test_sum_column_merges() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            100,
            Row::from_values(vec![Value::Integer(100), Value::Float(500.0)]),
        )],
    );

    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
    ];
    let mgr = make_segment_mgr(&schema, &seg_rows);

    let table = SegmentedTable::new(Box::new(hot), mgr);

    let sum = table.sum_column(1).unwrap();
    assert_eq!(sum.as_f64(), 530.0);
    assert_eq!(sum.count(), 3);
}

#[test]
fn test_scan_merges_via_merging_scanner() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            100,
            Row::from_values(vec![Value::Integer(100), Value::Float(500.0)]),
        )],
    );

    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
    ];
    let mgr = make_segment_mgr(&schema, &seg_rows);

    let table = SegmentedTable::new(Box::new(hot), mgr);

    let all_col_indices: Vec<usize> = (0..schema.columns.len()).collect();
    let mut scanner = table.scan(&all_col_indices, None).unwrap();

    let mut count = 0;
    let mut ids = Vec::new();
    while scanner.next() {
        if let Some(Value::Integer(id)) = scanner.row().get(0) {
            ids.push(*id);
        }
        count += 1;
    }
    assert_eq!(count, 3);
    assert_eq!(ids, vec![1, 2, 100]);
}

#[test]
fn test_scan_projects_hot_tail_like_cold_scanners() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            100,
            Row::from_values(vec![Value::Integer(100), Value::Float(500.0)]),
        )],
    );

    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
    ];
    let mgr = make_segment_mgr(&schema, &seg_rows);
    let table = SegmentedTable::new(Box::new(hot), mgr);

    let mut scanner = table.scan(&[0], None).unwrap();
    let mut rows = Vec::new();
    while scanner.next() {
        rows.push(scanner.take_row_with_id().unwrap());
    }

    assert_eq!(rows.len(), 3);
    assert!(
        rows.iter().all(|(_, row)| row.len() == 1),
        "cold and hot rows must obey the same projected scan width"
    );
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1.get(0), Some(&Value::Integer(1)));
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1.get(0), Some(&Value::Integer(2)));
    assert_eq!(rows[2].0, 100);
    assert_eq!(rows[2].1.get(0), Some(&Value::Integer(100)));
}

#[test]
fn test_projected_unordered_limit_keeps_hot_first_contract() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            100,
            Row::from_values(vec![Value::Integer(100), Value::Float(500.0)]),
        )],
    );

    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
    ];
    let mgr = make_segment_mgr(&schema, &seg_rows);
    let table = SegmentedTable::new(Box::new(hot), mgr);

    let rows = table
        .collect_rows_with_limit_unordered_projected(&[1], None, 2, 0)
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, row)| row.len() == 1));
    assert_eq!(rows[0].0, 100);
    assert_eq!(rows[0].1.get(0), Some(&Value::Float(500.0)));
    assert_eq!(rows[1].0, 1);
    assert_eq!(rows[1].1.get(0), Some(&Value::Float(10.0)));
}

#[test]
fn test_scan_where_does_not_resurrect_shadowed_cold_row() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(20.0)]),
        )],
    );

    let seg_rows: Vec<(i64, Row)> = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
    )];
    let mgr = make_segment_mgr(&schema, &seg_rows);
    let table = SegmentedTable::new(Box::new(hot), mgr);

    let mut filter = ComparisonExpr::eq("value", Value::Float(10.0));
    filter.prepare_for_schema(&schema);
    let all_col_indices: Vec<usize> = (0..schema.columns.len()).collect();

    let mut scanner = table.scan(&all_col_indices, Some(&filter)).unwrap();

    assert!(
        !scanner.next(),
        "newer hot row shadows the old cold row even when the hot value does not match WHERE"
    );
    assert!(scanner.err().is_none());
}

#[test]
fn r3_l02_batch_a_mixed_scan_uses_one_fenced_lightweight_shadow_snapshot() {
    let schema = test_schema();
    let unfiltered_collects = Arc::new(AtomicUsize::new(0));
    let fence = Arc::new(parking_lot::RwLock::new(()));
    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            100,
            Row::from_values(vec![Value::Integer(100), Value::Float(100.0)]),
        )],
    )
    .with_batch_a_oracle(Arc::clone(&unfiltered_collects), Arc::clone(&fence));
    let cold_rows = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::Float(1.0)]),
    )];
    let table = SegmentedTable::new(Box::new(hot), make_segment_mgr(&schema, &cold_rows));
    let mut filter = ComparisonExpr::gt("value", Value::Float(50.0));
    filter.prepare_for_schema(&schema);

    let writer_guard = fence.write();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = table.scan(&[0], Some(&filter)).map(|mut scanner| {
            let mut ids = Vec::new();
            while scanner.next() {
                ids.push(scanner.current_row_id().unwrap());
            }
            ids
        });
        finished_tx.send(result).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        finished_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "mixed scan must wait for the publication epoch fence"
    );
    drop(writer_guard);
    let ids = finished_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("scan resumes after publication")
        .unwrap();
    handle.join().unwrap();

    assert_eq!(ids, vec![100]);
    assert_eq!(
        unfiltered_collects.load(Ordering::Acquire),
        0,
        "shadow preparation must collect row IDs without unfiltered payload materialization"
    );
}

#[test]
fn r3_l02_batch_a_hot_zone_segments_reach_the_physical_scan() {
    use crate::volume::zonemap::ZoneMapBuilder;

    let schema = test_schema();
    let rows: Vec<(i64, Row)> = (1..=2000_i64)
        .map(|id| {
            (
                id,
                Row::from_values(vec![Value::Integer(id), Value::Float(id as f64)]),
            )
        })
        .collect();
    let mut builder = ZoneMapBuilder::new(1000);
    for (id, row) in &rows {
        builder.add_row_with_id(
            *id,
            &[
                ("id".to_string(), Value::Integer(*id)),
                ("value".to_string(), row.get(1).unwrap().clone()),
            ],
        );
    }
    let range_log = Arc::new(Mutex::new(Vec::new()));
    let hot = MockHotTable::new(schema.clone(), rows)
        .with_zone_map_oracle(Arc::new(builder.build()), Arc::clone(&range_log));
    let table = SegmentedTable::hot_only(Box::new(hot));
    let mut filter = ComparisonExpr::gt("value", Value::Float(1500.0));
    filter.prepare_for_schema(&schema);

    let mut scanner = table.scan(&[0], Some(&filter)).unwrap();
    let mut count = 0;
    while scanner.next() {
        count += 1;
    }

    assert_eq!(count, 500);
    assert_eq!(
        range_log.lock().unwrap().as_slice(),
        &[vec![(1001, 2000)]],
        "logical zone-map candidates must be executable row-ID ranges"
    );
}

#[test]
fn test_collect_all_rows_where_does_not_resurrect_shadowed_cold_row() {
    let schema = test_schema();

    let hot = MockHotTable::new(
        schema.clone(),
        vec![(
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(20.0)]),
        )],
    );

    let seg_rows: Vec<(i64, Row)> = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
    )];
    let mgr = make_segment_mgr(&schema, &seg_rows);
    let table = SegmentedTable::new(Box::new(hot), mgr);

    let mut filter = ComparisonExpr::eq("value", Value::Float(10.0));
    filter.prepare_for_schema(&schema);

    let rows = table.collect_all_rows(Some(&filter)).unwrap();

    assert!(
        rows.is_empty(),
        "newer hot row shadows the old cold row before WHERE filtering"
    );
}

#[test]
fn test_insert_goes_to_hot_buffer() {
    let schema = test_schema();
    let hot = MockHotTable::new(schema.clone(), vec![]);

    let mut table = SegmentedTable::hot_only(Box::new(hot));
    assert_eq!(table.row_count(), 0);

    table
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::Float(10.0),
        ]))
        .unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn filtered_cold_delete_reads_only_predicate_column_and_stages_row_id() {
    let schema = aggregate_projection_test_schema();
    let rows = aggregate_projection_test_rows();
    let (mgr, _dir) = make_artifact_segment_mgr(&schema, &rows);
    let delete_batches = Arc::new(Mutex::new(Vec::new()));
    let hot = MockHotTable::new(schema.clone(), Vec::new())
        .with_delete_batches_log(Arc::clone(&delete_batches));
    let mut table = SegmentedTable::new(Box::new(hot), Arc::clone(&mgr));
    let mut filter = ComparisonExpr::gt("value", Value::Float(0.0));
    filter.prepare_for_schema(&schema);

    crate::instrumentation::begin_volume_read_probe();
    crate::instrumentation::begin_decompression_probe();
    crate::instrumentation::begin_row_materialization_probe();
    let deleted = table.delete(Some(&filter)).unwrap();
    let materialization = crate::instrumentation::end_row_materialization_probe();
    let decompression = crate::instrumentation::end_decompression_probe();
    let volume_read = crate::instrumentation::end_volume_read_probe();

    assert_eq!(deleted, 3);
    assert_eq!(
        *delete_batches.lock().unwrap(),
        Vec::<Vec<i64>>::new(),
        "cold DELETE must not fabricate a second hot delete/WAL marker"
    );
    assert!(mgr.is_pending_tombstone(1, 1));
    assert!(mgr.is_pending_tombstone(1, 2));
    assert!(mgr.is_pending_tombstone(1, 3));
    assert_eq!(
        volume_read.calls, 2,
        "the DATA path reads only row IDs and the predicate column"
    );
    assert_eq!(
        decompression.calls, 2,
        "only the row-ID and predicate DATA blocks are decoded"
    );
    assert_eq!(
        materialization.values, 0,
        "matching row IDs must not materialize projected payload values"
    );
}

#[test]
fn test_delete_tombstones_cold_row() {
    let schema = test_schema();
    let hot = MockHotTable::new(schema.clone(), vec![]);

    let seg_rows: Vec<(i64, Row)> = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(3), Value::Float(30.0)]),
        ),
    ];
    let mgr = make_segment_mgr(&schema, &seg_rows);

    let mut table = SegmentedTable::new(Box::new(hot), Arc::clone(&mgr));

    // Delete row 2 via delete_by_row_ids
    let deleted = table.delete_by_row_ids(&[2]).unwrap();
    assert_eq!(deleted, 1);

    // Tombstones are deferred until commit. Before commit, the shared
    // segment manager still sees the row as live.
    assert_eq!(mgr.total_row_count(), 3);
    assert!(!mgr.is_tombstoned(2));

    // After commit, tombstones are applied to the shared segment manager.
    table.commit().unwrap();
    assert_eq!(mgr.total_row_count(), 2);
    assert!(mgr.is_tombstoned(2));
    assert!(mgr.row_exists(1));
    assert!(mgr.row_exists(3));
    assert!(!mgr.row_exists(2));
}

#[derive(Clone, Debug)]
struct BatchAFailingExpression;

impl Expression for BatchAFailingExpression {
    fn evaluate(&self, _row: &Row) -> Result<bool> {
        Err(radixdb_core::Error::internal(
            "r3-l04 injected expression failure",
        ))
    }

    fn evaluate_fast(&self, _row: &Row) -> bool {
        false
    }

    fn with_aliases(&self, _aliases: &FxHashMap<String, String>) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn prepare_for_schema(&mut self, _schema: &Schema) {}

    fn is_prepared(&self) -> bool {
        true
    }

    fn clone_box(&self) -> Box<dyn Expression> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[test]
fn r3_l04_batch_a_row_id_lookup_preserves_mixed_source_input_order() {
    let schema = test_schema();
    let cold_rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(3), Value::Float(30.0)]),
        ),
    ];
    let hot_rows = vec![(
        2,
        Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
    )];
    let table = SegmentedTable::new(
        Box::new(MockHotTable::new(schema.clone(), hot_rows)),
        make_segment_mgr(&schema, &cold_rows),
    );
    let requested = [2, 1, 3, 2];

    let rows = table.collect_rows_by_ids(&requested).unwrap();
    assert_eq!(
        rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
        requested
    );
    let projected = table
        .collect_rows_by_ids_projected(&requested, &[1])
        .unwrap();
    assert_eq!(
        projected
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect::<Vec<_>>(),
        requested
    );
    assert!(projected.iter().all(|(_, row)| row.len() == 1));
}

#[test]
fn r3_l04_batch_a_fetch_propagates_storage_and_expression_errors() {
    let schema = test_schema();
    let seg_rows = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
    )];
    let (mgr, dir) = make_artifact_segment_mgr(&schema, &seg_rows);
    let volume_path = {
        let manifest = mgr.manifest();
        dir.path().join(
            &manifest
                .segments
                .iter()
                .find(|segment| segment.segment_id == 1)
                .unwrap()
                .file_path,
        )
    };
    std::fs::OpenOptions::new()
        .write(true)
        .open(volume_path)
        .unwrap()
        .set_len(64)
        .unwrap();
    let cold_table =
        SegmentedTable::new(Box::new(MockHotTable::new(schema.clone(), Vec::new())), mgr);
    cold_table
        .fetch_rows_by_ids(&[1], &crate::expression::ConstBoolExpr::true_expr())
        .expect_err("cold materialization errors must cross the fetch boundary");

    let hot_table = SegmentedTable::hot_only(Box::new(MockHotTable::new(schema, seg_rows)));
    hot_table
        .fetch_rows_by_ids(&[1], &BatchAFailingExpression)
        .expect_err("expression errors must not be reclassified as non-match");
}

#[test]
fn r3_l04_batch_a_exact_count_waits_for_seal_publication() {
    let schema = test_schema();
    let hot_rows = vec![(
        2,
        Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
    )];
    let mgr = Arc::new(SegmentManager::new("test", None));
    let table = Arc::new(SegmentedTable::new(
        Box::new(MockHotTable::new(schema, hot_rows)),
        Arc::clone(&mgr),
    ));
    let seal_guard = mgr.acquire_seal_write();
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = Arc::clone(&table);
    let handle = std::thread::spawn(move || tx.send(reader.row_count()).unwrap());

    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "exact COUNT must not observe a partially published seal generation"
    );
    drop(seal_guard);
    assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(1)), Ok(1));
    handle.join().unwrap();
}

#[test]
fn limited_reads_wait_for_seal_publication_before_hot_only_shortcut() {
    let schema = test_schema();
    let hot_rows = vec![(
        2,
        Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
    )];
    let mgr = Arc::new(SegmentManager::new("test", None));
    let table = Arc::new(SegmentedTable::new(
        Box::new(MockHotTable::new(schema, hot_rows)),
        Arc::clone(&mgr),
    ));

    for operation in 0..4 {
        let seal_guard = mgr.acquire_seal_write();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = Arc::clone(&table);
        let handle = std::thread::spawn(move || {
            let rows = match operation {
                0 => reader.collect_rows_with_limit(None, 1, 0),
                1 => reader.collect_rows_with_limit_unordered(None, 1, 0),
                2 => reader.collect_rows_with_limit_unordered_projected(&[0], None, 1, 0),
                _ => reader.collect_rows_with_limit_unordered_exact_projected(&[0], None, 1, 0),
            };
            tx.send(rows.map(|rows| rows.len())).unwrap();
        });

        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "limited read operation {operation} crossed an unpublished seal generation"
        );
        drop(seal_guard);
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(1))
                .expect("limited reader must continue after publication"),
            Ok(1)
        );
        handle.join().unwrap();
    }
}

#[test]
fn r3_l04_batch_a_extrema_apply_cold_visibility() {
    let schema = test_schema();
    let cold_rows = vec![
        (
            1,
            Row::from_values(vec![Value::Integer(1), Value::Float(-10.0)]),
        ),
        (
            2,
            Row::from_values(vec![Value::Integer(2), Value::Float(5.0)]),
        ),
        (
            3,
            Row::from_values(vec![Value::Integer(3), Value::Float(99.0)]),
        ),
    ];
    let (mgr, _dir) = make_artifact_segment_mgr(&schema, &cold_rows);
    mgr.add_tombstones(&[1, 3], 1);
    let table = SegmentedTable::new(Box::new(MockHotTable::new(schema, Vec::new())), mgr);

    assert_eq!(table.min_column(1), Some(Some(Value::Float(5.0))));
    assert_eq!(table.max_column(1), Some(Some(Value::Float(5.0))));
}

#[test]
fn r3_l04_batch_a_partition_pushdown_declines_when_hot_owner_declines() {
    let schema = test_schema();
    let cold_rows = vec![(
        1,
        Row::from_values(vec![Value::Integer(1), Value::Float(10.0)]),
    )];
    let table = SegmentedTable::new(
        Box::new(MockHotTable::new(
            schema.clone(),
            vec![(
                2,
                Row::from_values(vec![Value::Integer(2), Value::Float(20.0)]),
            )],
        )),
        make_segment_mgr(&schema, &cold_rows),
    );

    assert!(table.collect_rows_grouped_by_partition("id").is_none());
    assert!(table
        .get_rows_for_partition_value("id", &Value::Integer(1))
        .is_none());
}

#[test]
fn v2_r3_cold_index_plan_is_private_until_commit_or_rollback() {
    let schema = test_schema();
    let mgr = Arc::new(SegmentManager::new("test", None));
    let index: Arc<dyn Index> = Arc::new(crate::index::HashIndex::new(
        "partial_value".to_string(),
        "test".to_string(),
        vec!["value".to_string()],
        vec![1],
        vec![DataType::Float],
        false,
        1,
    ));
    let values = vec![Value::Float(10.0)];
    index.add(&values, 1, 1).unwrap();
    mgr.record_cold_index_removal(1, Arc::clone(&index), values.clone(), 1);
    let table = SegmentedTable::new(
        Box::new(MockHotTable::new(schema, Vec::new())),
        Arc::clone(&mgr),
    );

    assert!(
        table.has_local_changes(),
        "a transaction-private index plan must make the transaction dirty"
    );
    let mut visible = Vec::new();
    index.get_row_ids_equal_into(&values, &mut visible).unwrap();
    assert_eq!(visible, vec![1], "uncommitted removal must stay visible");

    mgr.rollback_cold_index_removals(1);
    assert!(!table.has_local_changes());
    visible.clear();
    index.get_row_ids_equal_into(&values, &mut visible).unwrap();
    assert_eq!(visible, vec![1]);

    let statement_undo = ColdIndexRemovalStatementGuard::new(Arc::clone(&mgr), 1);
    mgr.record_cold_index_removal(1, Arc::clone(&index), values.clone(), 1);
    drop(statement_undo);
    visible.clear();
    index.get_row_ids_equal_into(&values, &mut visible).unwrap();
    assert_eq!(visible, vec![1]);
    assert!(!table.has_local_changes());
}
