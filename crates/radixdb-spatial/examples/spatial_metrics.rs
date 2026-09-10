use std::{hint::black_box, time::Instant};

use radixdb_core::{ExternalTypeRef, Value};
use radixdb_plugin::testing::{invoke_batch, invoke_scalar, TestValue};
use radixdb_plugin_host::{derive_object_id, registry_from_test_descriptor, InvocationLimits};
use radixdb_spatial::{descriptor, spatial, Box2d, Point, PACKAGE_ID};

const GRID_SIDE: usize = 1_000;
const BATCH_ROWS: usize = 32_768;

fn resident_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}

fn main() {
    let query = Box2d {
        min_x: 1_420.25,
        min_y: 2_330.25,
        max_x: 1_579.75,
        max_y: 2_489.75,
    };
    let baseline_rss_kib = resident_kib();
    let mut keys = Vec::with_capacity(GRID_SIDE * GRID_SIDE);
    let mut exact_matches = 0_usize;
    for x in 0..GRID_SIDE {
        for y in 0..GRID_SIDE {
            let point = Point {
                x: 1_000.0 + x as f64,
                y: 2_000.0 + y as f64,
            };
            exact_matches += usize::from(spatial::within_box(point, query).unwrap());
            keys.push(spatial::morton_bytes(point));
        }
    }
    keys.sort_unstable();
    let dataset_rss_kib = resident_kib();
    let spans = spatial::candidate_spans(query).unwrap();
    let candidates = spans
        .iter()
        .map(|span| {
            let start = keys.partition_point(|key| key.as_slice() < span.start.as_slice());
            let end = keys.partition_point(|key| key.as_slice() <= span.end.as_slice());
            end - start
        })
        .sum::<usize>();
    assert!(candidates >= exact_matches);
    let reduction = 1.0 - candidates as f64 / keys.len() as f64;
    let false_positive_ratio = if candidates == 0 {
        0.0
    } else {
        (candidates - exact_matches) as f64 / candidates as f64
    };

    drop(keys);
    let native_rows = (0..BATCH_ROWS)
        .map(|row| {
            let left = Point {
                x: row as f64 * 0.25,
                y: row as f64 * -0.125,
            };
            let right = Point {
                x: left.x + 3.0,
                y: left.y + 4.0,
            };
            (left, right)
        })
        .collect::<Vec<_>>();
    let point_rows = native_rows
        .iter()
        .map(|(left, right)| {
            Ok(vec![
                TestValue::external(descriptor(), left)?,
                TestValue::external(descriptor(), right)?,
            ])
        })
        .collect::<radixdb_plugin::PluginResult<Vec<_>>>()
        .unwrap();

    let direct_start = Instant::now();
    for (left, right) in &native_rows {
        black_box(spatial::distance(black_box(*left), black_box(*right))).unwrap();
    }
    let direct_elapsed = direct_start.elapsed();

    // This path resolves the generated descriptor and calls its generated ABI
    // wrapper through the SDK local test host.
    let wrapper_start = Instant::now();
    for row in &point_rows {
        black_box(invoke_scalar(
            descriptor(),
            "distance",
            row,
            Default::default(),
        ))
        .unwrap();
    }
    let wrapper_elapsed = wrapper_start.elapsed();

    let batch_start = Instant::now();
    let batch = black_box(invoke_batch(
        descriptor(),
        "distance",
        &point_rows,
        Default::default(),
    ))
    .unwrap();
    let batch_elapsed = batch_start.elapsed();
    assert_eq!(batch.outputs.len(), BATCH_ROWS);

    let point_type =
        ExternalTypeRef::new(derive_object_id(PACKAGE_ID, "point").unwrap(), 1).unwrap();
    let host_rows = native_rows
        .iter()
        .map(|(left, right)| {
            vec![
                Value::try_external(point_type, radixdb_spatial::encode(left).unwrap()).unwrap(),
                Value::try_external(point_type, radixdb_spatial::encode(right).unwrap()).unwrap(),
            ]
        })
        .collect::<Vec<_>>();
    // SAFETY: the statically linked descriptor and callbacks live for the
    // complete benchmark process.
    let registry = unsafe { registry_from_test_descriptor(descriptor()) }.unwrap();
    let distance_id = derive_object_id(PACKAGE_ID, "distance").unwrap();
    let host_scalar_start = Instant::now();
    for row in &host_rows {
        black_box(registry.invoke_scalar_function(distance_id, row, InvocationLimits::default()))
            .unwrap();
    }
    let host_scalar_elapsed = host_scalar_start.elapsed();
    let host_batch_start = Instant::now();
    let host_batch = black_box(registry.invoke_function_batch(
        distance_id,
        &host_rows,
        InvocationLimits::default(),
    ))
    .unwrap();
    let host_batch_elapsed = host_batch_start.elapsed();
    assert_eq!(host_batch.len(), BATCH_ROWS);

    let direct_rows_per_second = BATCH_ROWS as f64 / direct_elapsed.as_secs_f64();
    let wrapper_rows_per_second = BATCH_ROWS as f64 / wrapper_elapsed.as_secs_f64();
    let batch_rows_per_second = BATCH_ROWS as f64 / batch_elapsed.as_secs_f64();
    let host_scalar_rows_per_second = BATCH_ROWS as f64 / host_scalar_elapsed.as_secs_f64();
    let host_batch_rows_per_second = BATCH_ROWS as f64 / host_batch_elapsed.as_secs_f64();
    println!("dataset_rows={}", GRID_SIDE * GRID_SIDE);
    println!("candidate_spans={}", spans.len());
    println!("candidate_rows={candidates}");
    println!("exact_rows={exact_matches}");
    println!("candidate_reduction_percent={:.3}", reduction * 100.0);
    println!(
        "candidate_false_positive_percent={:.3}",
        false_positive_ratio * 100.0
    );
    println!(
        "dataset_rss_delta_kib={}",
        dataset_rss_kib.saturating_sub(baseline_rss_kib)
    );
    println!("direct_native_rows_per_second={direct_rows_per_second:.0}");
    println!("generated_wrapper_rows_per_second={wrapper_rows_per_second:.0}");
    println!(
        "generated_wrapper_overhead_ratio={:.3}",
        direct_rows_per_second / wrapper_rows_per_second
    );
    println!("sdk_test_batch_rows_per_second={batch_rows_per_second:.0}");
    println!(
        "sdk_test_batch_speedup={:.3}",
        batch_rows_per_second / wrapper_rows_per_second
    );
    println!("host_scalar_rows_per_second={host_scalar_rows_per_second:.0}");
    println!("host_batch_rows_per_second={host_batch_rows_per_second:.0}");
    println!(
        "host_batch_speedup={:.3}",
        host_batch_rows_per_second / host_scalar_rows_per_second
    );
}
