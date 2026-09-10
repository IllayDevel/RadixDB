use std::{
    fs,
    hint::black_box,
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use radixdb_core::{ExternalTypeRef, Value};
use radixdb_executor::{Executor, PluginRegistry};
use radixdb_plugin_host::{derive_object_id, registry_from_test_descriptor};
use radixdb_spatial::{Point, PACKAGE_ID};
use radixdb_storage::{instrumentation, mvcc::engine::MVCCEngine, Config};
use smallvec::smallvec;

const ROWS: usize = 8_192;
const RUNS: usize = 3;
const LOOKUPS: usize = 256;

#[derive(Debug, Clone, Copy)]
enum PayloadKind {
    BuiltinBytes,
    ExternalPoint,
}

#[derive(Debug, Clone, Copy)]
struct Metrics {
    hot_insert_ms: f64,
    wal_bytes: u64,
    hot_scan_ms: f64,
    hot_lookup_ms: f64,
    index_build_ms: f64,
    checkpoint_compaction_ms: f64,
    seal_calls: u64,
    compaction_calls: u64,
    cold_open_ms: f64,
    cold_scan_ms: f64,
    data_bytes: u64,
    index_bytes: u64,
}

fn registry() -> Arc<PluginRegistry> {
    // SAFETY: the statically linked proving-plugin callbacks remain loaded for
    // the lifetime of this benchmark process.
    unsafe { registry_from_test_descriptor(radixdb_spatial::descriptor()) }.unwrap()
}

fn config(path: &Path) -> Config {
    let mut config = Config {
        path: Some(path.to_string_lossy().into_owned()),
        ..Config::default()
    };
    config.persistence.checkpoint_interval = 0;
    config.persistence.checkpoint_on_close = false;
    config.persistence.target_volume_rows = 512;
    config.persistence.compact_threshold = 2;
    config.persistence.max_compaction_jobs = 1;
    config.persistence.max_compaction_input_segments = 8;
    config
}

fn open_engine(config: Config, registry: Arc<PluginRegistry>) -> Arc<MVCCEngine> {
    let engine = Arc::new(MVCCEngine::new(config));
    engine
        .install_catalog_runtime_binder(radixdb_executor::plugin_catalog_runtime_binder(registry));
    engine.open_engine().unwrap();
    engine.start_cleanup();
    engine
}

fn bind_point_catalog(executor: &Executor) {
    executor
        .execute("CREATE EXTENSION radixdb_spatial VERSION '1.0.0'")
        .unwrap();
    executor
        .execute("CREATE TYPE public.point FROM EXTENSION radixdb_spatial AS 'point'")
        .unwrap();
    for name in ["point_lt", "point_le", "point_eq", "point_ge", "point_gt"] {
        executor
            .execute(&format!(
                "CREATE FUNCTION public.{name}(left_value public.point NOT NULL, \
                 right_value public.point NOT NULL) RETURNS BOOLEAN NOT NULL LANGUAGE NATIVE \
                 FROM EXTENSION radixdb_spatial AS '{name}'"
            ))
            .unwrap();
    }
    for (function, operator, symbol) in [
        ("point_lt", "point_lt_operator", "<"),
        ("point_le", "point_le_operator", "<="),
        ("point_eq", "point_eq_operator", "="),
        ("point_ge", "point_ge_operator", ">="),
        ("point_gt", "point_gt_operator", ">"),
    ] {
        executor
            .execute(&format!(
                "CREATE OPERATOR public.{symbol} (LEFTARG = public.point, RIGHTARG = public.point, \
                 FUNCTION = public.{function}(public.point, public.point)) \
                 FROM EXTENSION radixdb_spatial AS '{operator}'"
            ))
            .unwrap();
    }
    executor
        .execute(
            "CREATE OPERATOR CLASS public.point_morton_btree FOR TYPE public.point USING BTREE \
             FROM EXTENSION radixdb_spatial AS 'point_morton_btree'",
        )
        .unwrap();
}

fn point_type() -> ExternalTypeRef {
    ExternalTypeRef::new(derive_object_id(PACKAGE_ID, "point").unwrap(), 1).unwrap()
}

fn payload(kind: PayloadKind, row: usize) -> Value {
    let point = Point {
        x: row as f64 + 0.25,
        y: -(row as f64) - 0.5,
    };
    let encoded = radixdb_spatial::encode(&point).unwrap();
    match kind {
        PayloadKind::BuiltinBytes => Value::bytes(encoded),
        PayloadKind::ExternalPoint => Value::try_external(point_type(), encoded).unwrap(),
    }
}

fn scan(executor: &Executor) -> usize {
    let mut result = executor.execute("SELECT payload FROM perf_values").unwrap();
    let mut rows = 0;
    while result.next() {
        black_box(result.row().get(0)).expect("projected payload");
        rows += 1;
    }
    rows
}

fn scan_median(executor: &Executor) -> f64 {
    assert_eq!(scan(executor), ROWS);
    let mut samples = Vec::with_capacity(5);
    for _ in 0..5 {
        let started = Instant::now();
        assert_eq!(scan(executor), ROWS);
        samples.push(started.elapsed().as_secs_f64() * 1_000.0);
    }
    median_f64(&mut samples)
}

fn lookup_median(executor: &Executor, kind: PayloadKind) -> f64 {
    let mut samples = Vec::with_capacity(5);
    for repeat in 0..6 {
        let started = Instant::now();
        for probe in 0..LOOKUPS {
            let row = (probe * 31 + repeat * 17) % ROWS;
            let mut result = executor
                .execute_with_params(
                    "SELECT COUNT(*) FROM perf_values WHERE payload = $1",
                    smallvec![payload(kind, row)],
                )
                .unwrap();
            assert!(result.next());
            assert_eq!(result.row().get(0).and_then(Value::as_int64), Some(1));
            assert!(!result.next());
        }
        if repeat != 0 {
            samples.push(started.elapsed().as_secs_f64() * 1_000.0);
        }
    }
    median_f64(&mut samples)
}

fn bytes_with_suffix(path: &Path, suffix: &str) -> u64 {
    let mut total = 0_u64;
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total = total.saturating_add(bytes_with_suffix(&path, suffix));
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(suffix))
        {
            total = total.saturating_add(entry.metadata().map_or(0, |metadata| metadata.len()));
        }
    }
    total
}

fn wait_for_compaction(engine: &MVCCEngine) -> Duration {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    let mut quiet_samples = 0;
    loop {
        let runtime = engine.runtime_stats_snapshot();
        let counters = instrumentation::snapshot();
        // One bounded compaction job is the measured unit. Remaining L0 debt
        // may intentionally keep the next request armed after that job.
        let quiet = !runtime.compaction_running && runtime.compaction_active_jobs == 0;
        quiet_samples = if quiet && counters.compaction_calls > 0 {
            quiet_samples + 1
        } else {
            0
        };
        if quiet_samples >= 3 {
            return started.elapsed();
        }
        assert!(
            Instant::now() < deadline,
            "compaction did not complete: requested={} running={} active_jobs={} calls={} l0_segments={} cold_segments={}",
            runtime.compaction_requested,
            runtime.compaction_running,
            runtime.compaction_active_jobs,
            counters.compaction_calls,
            runtime.cold_l0_segments,
            runtime.cold_segments,
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn one_run(kind: PayloadKind) -> Metrics {
    let temporary = tempfile::tempdir().unwrap();
    let database_path = temporary.path().join("database");
    let config = config(&database_path);
    let plugin_registry = registry();
    let engine = open_engine(config.clone(), Arc::clone(&plugin_registry));
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), plugin_registry);
    match kind {
        PayloadKind::BuiltinBytes => executor
            .execute("CREATE TABLE perf_values (id INTEGER PRIMARY KEY, payload BYTES NOT NULL)")
            .unwrap(),
        PayloadKind::ExternalPoint => {
            bind_point_catalog(&executor);
            executor
                .execute(
                    "CREATE TABLE perf_values (id INTEGER PRIMARY KEY, payload public.point NOT NULL)",
                )
                .unwrap()
        }
    };
    let values = (0..ROWS).map(|row| payload(kind, row)).collect::<Vec<_>>();
    let wal_before = bytes_with_suffix(&database_path, ".log");
    let insert_started = Instant::now();
    executor.execute("BEGIN").unwrap();
    for (row, value) in values.iter().enumerate() {
        executor
            .execute_with_params(
                "INSERT INTO perf_values (id, payload) VALUES ($1, $2)",
                smallvec![Value::Integer(row as i64), value.clone()],
            )
            .unwrap();
    }
    executor.execute("COMMIT").unwrap();
    let hot_insert_ms = insert_started.elapsed().as_secs_f64() * 1_000.0;
    let wal_bytes = bytes_with_suffix(&database_path, ".log").saturating_sub(wal_before);
    let hot_scan_ms = scan_median(&executor);

    let index_started = Instant::now();
    match kind {
        PayloadKind::BuiltinBytes => executor
            .execute("CREATE INDEX perf_payload_idx ON perf_values(payload) USING BTREE")
            .unwrap(),
        PayloadKind::ExternalPoint => executor
            .execute(
                "CREATE INDEX perf_payload_idx ON perf_values(payload public.point_morton_btree) USING BTREE",
            )
            .unwrap(),
    };
    let index_build_ms = index_started.elapsed().as_secs_f64() * 1_000.0;
    let hot_lookup_ms = lookup_median(&executor, kind);

    match kind {
        PayloadKind::BuiltinBytes => executor
            .execute(
                "CREATE TABLE maintenance_values (id INTEGER PRIMARY KEY, payload BYTES NOT NULL)",
            )
            .unwrap(),
        PayloadKind::ExternalPoint => executor
            .execute(
                "CREATE TABLE maintenance_values (id INTEGER PRIMARY KEY, payload public.point NOT NULL)",
            )
            .unwrap(),
    };
    instrumentation::reset();
    let mut checkpoint_duration = Duration::ZERO;
    for generation in 0..4 {
        executor.execute("BEGIN").unwrap();
        for offset in 0..2_048 {
            let row = generation * 2_048 + offset;
            executor
                .execute_with_params(
                    "INSERT INTO maintenance_values (id, payload) VALUES ($1, $2)",
                    smallvec![Value::Integer(row as i64), payload(kind, row)],
                )
                .unwrap();
        }
        executor.execute("COMMIT").unwrap();
        let checkpoint_started = Instant::now();
        executor.execute("PRAGMA CHECKPOINT").unwrap();
        checkpoint_duration += checkpoint_started.elapsed();
    }
    let compaction_wait = wait_for_compaction(&engine);
    let checkpoint_compaction_ms = (checkpoint_duration + compaction_wait).as_secs_f64() * 1_000.0;
    let counters = instrumentation::snapshot();
    assert!(counters.compaction_calls > 0);
    let data_bytes = bytes_with_suffix(&database_path, ".data");
    let index_bytes = bytes_with_suffix(&database_path, ".idx");
    drop(executor);
    engine.close_engine().unwrap();

    let cold_registry = registry();
    let open_started = Instant::now();
    let reopened = open_engine(config, Arc::clone(&cold_registry));
    let cold_open_ms = open_started.elapsed().as_secs_f64() * 1_000.0;
    let reopened_executor = Executor::with_plugin_registry(Arc::clone(&reopened), cold_registry);
    let cold_scan_ms = scan_median(&reopened_executor);
    drop(reopened_executor);
    reopened.close_engine().unwrap();

    Metrics {
        hot_insert_ms,
        wal_bytes,
        hot_scan_ms,
        hot_lookup_ms,
        index_build_ms,
        checkpoint_compaction_ms,
        seal_calls: counters.seal_calls,
        compaction_calls: counters.compaction_calls,
        cold_open_ms,
        cold_scan_ms,
        data_bytes,
        index_bytes,
    }
}

fn median_f64(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn median_u64(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

fn metric_f64(values: &[Metrics], read: impl Fn(&Metrics) -> f64) -> f64 {
    median_f64(&mut values.iter().map(read).collect::<Vec<_>>())
}

fn metric_u64(values: &[Metrics], read: impl Fn(&Metrics) -> u64) -> u64 {
    median_u64(&mut values.iter().map(read).collect::<Vec<_>>())
}

fn print_pair(name: &str, builtin: f64, external: f64, unit: &str) {
    println!("{name}_builtin_{unit}={builtin:.3}");
    println!("{name}_external_{unit}={external:.3}");
    println!("{name}_external_over_builtin={:.3}", external / builtin);
}

fn main() {
    let mut builtin = Vec::with_capacity(RUNS);
    let mut external = Vec::with_capacity(RUNS);
    for run in 0..RUNS {
        if run % 2 == 0 {
            builtin.push(one_run(PayloadKind::BuiltinBytes));
            external.push(one_run(PayloadKind::ExternalPoint));
        } else {
            external.push(one_run(PayloadKind::ExternalPoint));
            builtin.push(one_run(PayloadKind::BuiltinBytes));
        }
    }
    println!("rows={ROWS}");
    println!("runs={RUNS}");
    println!("payload_bytes=16");
    print_pair(
        "hot_insert",
        metric_f64(&builtin, |value| value.hot_insert_ms),
        metric_f64(&external, |value| value.hot_insert_ms),
        "ms",
    );
    print_pair(
        "wal",
        metric_u64(&builtin, |value| value.wal_bytes) as f64,
        metric_u64(&external, |value| value.wal_bytes) as f64,
        "bytes",
    );
    print_pair(
        "hot_scan",
        metric_f64(&builtin, |value| value.hot_scan_ms),
        metric_f64(&external, |value| value.hot_scan_ms),
        "ms",
    );
    print_pair(
        "hot_index_lookup_256",
        metric_f64(&builtin, |value| value.hot_lookup_ms),
        metric_f64(&external, |value| value.hot_lookup_ms),
        "ms",
    );
    print_pair(
        "index_build",
        metric_f64(&builtin, |value| value.index_build_ms),
        metric_f64(&external, |value| value.index_build_ms),
        "ms",
    );
    print_pair(
        "checkpoint_compaction",
        metric_f64(&builtin, |value| value.checkpoint_compaction_ms),
        metric_f64(&external, |value| value.checkpoint_compaction_ms),
        "ms",
    );
    print_pair(
        "cold_open",
        metric_f64(&builtin, |value| value.cold_open_ms),
        metric_f64(&external, |value| value.cold_open_ms),
        "ms",
    );
    print_pair(
        "cold_scan",
        metric_f64(&builtin, |value| value.cold_scan_ms),
        metric_f64(&external, |value| value.cold_scan_ms),
        "ms",
    );
    print_pair(
        "data_artifacts",
        metric_u64(&builtin, |value| value.data_bytes) as f64,
        metric_u64(&external, |value| value.data_bytes) as f64,
        "bytes",
    );
    print_pair(
        "index_artifacts",
        metric_u64(&builtin, |value| value.index_bytes) as f64,
        metric_u64(&external, |value| value.index_bytes) as f64,
        "bytes",
    );
    println!(
        "builtin_seal_calls={}",
        metric_u64(&builtin, |value| value.seal_calls)
    );
    println!(
        "external_seal_calls={}",
        metric_u64(&external, |value| value.seal_calls)
    );
    println!(
        "builtin_compaction_calls={}",
        metric_u64(&builtin, |value| value.compaction_calls)
    );
    println!(
        "external_compaction_calls={}",
        metric_u64(&external, |value| value.compaction_calls)
    );
}
