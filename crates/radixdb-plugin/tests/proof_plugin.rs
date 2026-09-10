use std::cmp::Ordering;

use radixdb_plugin::prelude::*;

#[radixdb_plugin(
    id = "3c172878-0b44-4ed8-b8b7-33e4af599c50",
    name = "radix_spatial_proof",
    version = "1.0.0"
)]
mod spatial {
    use super::*;

    #[derive(Debug, Default, Clone, Copy, RadixType)]
    #[radix_type(
        id = "point",
        name = "point",
        codec = 1,
        semantic_revision = 1,
        storage = "fixed",
        max_bytes = 16,
        equality = point_equal,
        hash = point_hash,
        ordering = point_compare
    )]
    pub struct Point {
        #[radix_field(codec = "f64-le")]
        pub x: f64,
        #[radix_field(codec = "f64-le")]
        pub y: f64,
    }

    fn normalized(value: f64) -> f64 {
        if value.is_nan() {
            f64::from_bits(0x7ff8_0000_0000_0000)
        } else if value == 0.0 {
            0.0
        } else {
            value
        }
    }

    fn point_equal(left: &Point, right: &Point) -> bool {
        normalized(left.x).to_bits() == normalized(right.x).to_bits()
            && normalized(left.y).to_bits() == normalized(right.y).to_bits()
    }

    fn point_hash(value: &Point, sink: &mut HashSink<'_>) -> PluginResult<()> {
        sink.f64_bits(normalized(value.x))?;
        sink.f64_bits(normalized(value.y))
    }

    fn point_compare(left: &Point, right: &Point) -> Ordering {
        normalized(left.x)
            .total_cmp(&normalized(right.x))
            .then_with(|| normalized(left.y).total_cmp(&normalized(right.y)))
    }

    #[radixdb_scalar(
        id = "distance",
        name = "st_distance",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 4,
        cancellation = "bounded",
        max_output_bytes = 8
    )]
    pub fn distance(left: Point, right: Point) -> PluginResult<f64> {
        Ok((left.x - right.x).hypot(left.y - right.y))
    }

    #[radixdb_scalar(
        id = "point_lt",
        name = "point_lt",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_lt(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_compare(&left, &right) == Ordering::Less)
    }

    #[radixdb_scalar(
        id = "point_le",
        name = "point_le",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_le(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_compare(&left, &right) != Ordering::Greater)
    }

    #[radixdb_scalar(
        id = "point_eq",
        name = "point_eq",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_eq(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_equal(&left, &right))
    }

    #[radixdb_scalar(
        id = "point_ge",
        name = "point_ge",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_ge(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_compare(&left, &right) != Ordering::Less)
    }

    #[radixdb_scalar(
        id = "point_gt",
        name = "point_gt",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_gt(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_compare(&left, &right) == Ordering::Greater)
    }

    #[radixdb_scalar(
        id = "panic_probe",
        name = "panic_probe",
        semantic_revision = 1,
        volatile,
        strict,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 8
    )]
    fn panic_probe(_value: i64) -> PluginResult<i64> {
        panic!("proof panic must stop at the generated ABI barrier")
    }

    #[radixdb_batch(for_scalar = "distance", rows_per_cancel_check = 64)]
    pub fn distance_batch(
        left: ColumnView<'_, Point>,
        right: ColumnView<'_, Point>,
        output: &mut ColumnBuilder<'_, '_, f64>,
        context: &CallContext<'_>,
    ) -> PluginResult<()> {
        for (index, (left, right)) in left.zip(right).enumerate() {
            if index % 64 == 0 {
                context.check_cancelled()?;
            }
            output.push(distance(left?, right?)?)?;
        }
        Ok(())
    }

    #[radixdb_operator(
        id = "point_lt_operator",
        symbol = "<",
        semantic_revision = 1,
        function = "point_lt",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_lt_operator() {}

    #[radixdb_operator(
        id = "point_le_operator",
        symbol = "<=",
        semantic_revision = 1,
        function = "point_le",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_le_operator() {}

    #[radixdb_operator(
        id = "point_eq_operator",
        symbol = "=",
        semantic_revision = 1,
        function = "point_eq",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_eq_operator() {}

    #[radixdb_operator(
        id = "point_ge_operator",
        symbol = ">=",
        semantic_revision = 1,
        function = "point_ge",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_ge_operator() {}

    #[radixdb_operator(
        id = "point_gt_operator",
        symbol = ">",
        semantic_revision = 1,
        function = "point_gt",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_gt_operator() {}

    #[radixdb_operator_class(
        id = "point_btree",
        semantic_revision = 1,
        access_method = "btree",
        input = Point,
        key = BoundedBytes::<16>,
        key_codec_revision = 1
    )]
    fn point_key(point: Point) -> PluginResult<BoundedBytes<16>> {
        fn sortable(value: f64) -> [u8; 8] {
            let bits = normalized(value).to_bits();
            let ordered = if bits & (1_u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1_u64 << 63)
            };
            ordered.to_be_bytes()
        }
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&sortable(point.x));
        bytes.extend_from_slice(&sortable(point.y));
        BoundedBytes::new(bytes)
    }

    #[radixdb_planner_support(
        id = "distance_support",
        name = "st_distance_support",
        semantic_revision = 1,
        for_function = "distance",
        operator_class = "point_btree",
        always_recheck,
        max_spans = 8,
        max_output_bytes = 256
    )]
    fn distance_support(
        predicate: PredicateView<'_>,
        output: &mut CandidatePlanBuilder<'_, '_>,
    ) -> PluginResult<()> {
        let marker = predicate.as_bytes().first().copied().unwrap_or_default();
        output.push_span(CandidateSpan {
            start: vec![marker],
            end: vec![marker.saturating_add(1)],
        })
    }
}

#[test]
fn generated_descriptor_graph_is_abi_valid() {
    let descriptor = spatial::__radixdb_descriptor();
    radixdb_plugin::testing::validate_descriptor_graph(descriptor).unwrap();
    assert_eq!(descriptor.type_count, 1);
    assert_eq!(descriptor.function_count, 7);
    assert_eq!(descriptor.operator_count, 5);
    assert_eq!(descriptor.operator_class_count, 1);
    assert_eq!(descriptor.planner_support_count, 1);
}

#[test]
fn derived_type_passes_codec_and_semantic_laws() {
    let report = radixdb_plugin::testing::check_type::<spatial::Point>().unwrap();
    assert!(report.corpus_values >= 9);
    assert_eq!(report.codec_vectors[0].len(), 16);
    assert_eq!(report.codec_vectors.len(), report.hash_vectors.len());
}

#[test]
fn scalar_and_batch_results_match() {
    let descriptor = spatial::__radixdb_descriptor();
    let rows = [
        (
            spatial::Point { x: 0.0, y: 0.0 },
            spatial::Point { x: 3.0, y: 4.0 },
        ),
        (
            spatial::Point { x: -2.0, y: 7.0 },
            spatial::Point { x: 1.0, y: 3.0 },
        ),
    ];
    let batch_rows = rows
        .iter()
        .map(|(left, right)| {
            Ok(vec![
                radixdb_plugin::testing::TestValue::external(descriptor, left)?,
                radixdb_plugin::testing::TestValue::external(descriptor, right)?,
            ])
        })
        .collect::<PluginResult<Vec<_>>>()
        .unwrap();
    let batch = radixdb_plugin::testing::invoke_batch(
        descriptor,
        "distance",
        &batch_rows,
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        batch.status,
        radixdb_plugin::__private::abi::RADIX_STATUS_OK
    );
    assert!(batch.finished);

    for (index, arguments) in batch_rows.iter().enumerate() {
        let scalar = radixdb_plugin::testing::invoke_scalar(
            descriptor,
            "distance",
            arguments,
            Default::default(),
        )
        .unwrap();
        assert_eq!(scalar.outputs.as_slice(), &batch.outputs[index..=index]);
    }
}

#[test]
fn panic_and_cancellation_are_contained() {
    use radixdb_plugin::__private::abi;

    let descriptor = spatial::__radixdb_descriptor();
    let panic_report = radixdb_plugin::testing::invoke_scalar(
        descriptor,
        "panic_probe",
        &[radixdb_plugin::testing::TestValue::integer(1)],
        Default::default(),
    )
    .unwrap();
    assert_eq!(panic_report.status, abi::RADIX_STATUS_PANIC);
    assert!(!panic_report.finished);
    assert!(panic_report.outputs.is_empty());
    assert_eq!(
        panic_report.diagnostics[0].category,
        abi::RADIX_DIAGNOSTIC_PLUGIN_PANIC
    );

    let point = spatial::Point { x: 1.0, y: 2.0 };
    let rows = vec![vec![
        radixdb_plugin::testing::TestValue::external(descriptor, &point).unwrap(),
        radixdb_plugin::testing::TestValue::external(descriptor, &point).unwrap(),
    ]];
    let cancelled = radixdb_plugin::testing::invoke_batch(
        descriptor,
        "distance",
        &rows,
        radixdb_plugin::testing::TestCallOptions {
            cancelled: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(cancelled.status, abi::RADIX_STATUS_CANCELLED);
    assert!(!cancelled.finished);
    assert!(cancelled.outputs.is_empty());
    assert_eq!(
        cancelled.diagnostics[0].category,
        abi::RADIX_DIAGNOSTIC_CANCELLED
    );

    let deadline_expired = radixdb_plugin::testing::invoke_batch(
        descriptor,
        "distance",
        &rows,
        radixdb_plugin::testing::TestCallOptions {
            deadline_expired: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(deadline_expired.status, abi::RADIX_STATUS_CANCELLED);
    assert!(!deadline_expired.finished);
    assert!(deadline_expired.outputs.is_empty());
    assert_eq!(
        deadline_expired.diagnostics[0].category,
        abi::RADIX_DIAGNOSTIC_CANCELLED
    );
}

#[test]
fn host_owned_output_limit_fails_without_publication() {
    use radixdb_plugin::__private::abi;

    let descriptor = spatial::__radixdb_descriptor();
    let left = spatial::Point { x: 0.0, y: 0.0 };
    let right = spatial::Point { x: 3.0, y: 4.0 };
    let arguments = [
        radixdb_plugin::testing::TestValue::external(descriptor, &left).unwrap(),
        radixdb_plugin::testing::TestValue::external(descriptor, &right).unwrap(),
    ];
    let report = radixdb_plugin::testing::invoke_scalar(
        descriptor,
        "distance",
        &arguments,
        radixdb_plugin::testing::TestCallOptions {
            max_output_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(report.status, abi::RADIX_STATUS_LIMIT_EXCEEDED);
    assert!(!report.finished);
    assert!(report.outputs.is_empty());
}

#[test]
fn planner_support_is_bounded_and_keeps_residual_recheck() {
    use radixdb_plugin::__private::abi;

    let descriptor = spatial::__radixdb_descriptor();
    let report = radixdb_plugin::testing::invoke_planner(
        descriptor,
        "distance_support",
        &[41],
        Default::default(),
    )
    .unwrap();
    assert_eq!(report.status, abi::RADIX_STATUS_OK);
    assert_eq!(
        radixdb_plugin::testing::decode_candidate_spans(&report).unwrap(),
        vec![CandidateSpan {
            start: vec![41],
            end: vec![42],
        }]
    );
    assert_eq!(
        radixdb_plugin::testing::planner_recheck_policy(descriptor, "distance_support").unwrap(),
        abi::RADIX_RECHECK_ALWAYS
    );
}

#[test]
fn proof_plugin_author_source_contains_no_raw_boundary_code() {
    let source = include_str!("proof_plugin.rs");
    assert!(!source.contains("extern \"C\""));
    assert!(!source.contains(concat!("un", "safe")));
}
