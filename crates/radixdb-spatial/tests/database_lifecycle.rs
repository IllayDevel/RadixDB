//! Database-level proof for the independently built spatial plugin.

use std::{process::Command, sync::Arc};

use radixdb_core::{ExternalTypeRef, Value};
use radixdb_executor::{Executor, PluginRegistry};
use radixdb_plugin_host::{
    derive_object_id, registry_from_test_descriptor, DatabasePluginAdmission, InvocationLimits,
    NormalizedPredicateArgument, ObjectKind, PlannerSupportOutcome, RequirementIssue,
};
use radixdb_spatial::{Box2d, Point, PACKAGE_ID};
use radixdb_storage::{mvcc::engine::MVCCEngine, Config};

const CHILD_PATH_ENV: &str = "RADIXDB_SPATIAL_CRASH_PATH";
const PUBLICATION_MODE_ENV: &str = "RADIXDB_SPATIAL_PUBLICATION_MODE";

fn registry() -> Arc<PluginRegistry> {
    // SAFETY: the statically linked proving-plugin callbacks remain loaded for
    // the lifetime of this test process.
    unsafe { registry_from_test_descriptor(radixdb_spatial::descriptor()) }.unwrap()
}

fn opened_engine(config: Config, registry: Arc<PluginRegistry>) -> Arc<MVCCEngine> {
    let engine = Arc::new(MVCCEngine::new(config));
    engine
        .install_catalog_runtime_binder(radixdb_executor::plugin_catalog_runtime_binder(registry));
    engine.open_engine().unwrap();
    engine
}

fn type_ref(local_id: &str) -> ExternalTypeRef {
    ExternalTypeRef::new(derive_object_id(PACKAGE_ID, local_id).unwrap(), 1).unwrap()
}

fn point_value(value: Point) -> Value {
    Value::try_external(type_ref("point"), radixdb_spatial::encode(&value).unwrap()).unwrap()
}

fn box_value(value: Box2d) -> Value {
    Value::try_external(type_ref("box2d"), radixdb_spatial::encode(&value).unwrap()).unwrap()
}

fn bind_spatial_catalog(executor: &Executor) {
    executor
        .execute("CREATE EXTENSION radixdb_spatial VERSION '1.0.0'")
        .unwrap();
    for (sql_name, local_id) in [
        ("point", "point"),
        ("box2d", "box2d"),
        ("polygon", "polygon"),
    ] {
        executor
            .execute(&format!(
                "CREATE TYPE public.{sql_name} FROM EXTENSION radixdb_spatial AS '{local_id}'"
            ))
            .unwrap();
    }
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

    for statement in [
        "CREATE FUNCTION public.st_distance(left_value public.point NOT NULL, right_value public.point NOT NULL) RETURNS FLOAT NOT NULL LANGUAGE NATIVE FROM EXTENSION radixdb_spatial AS 'distance'",
        "CREATE FUNCTION public.st_contains(shape public.polygon NOT NULL, probe public.point NOT NULL) RETURNS BOOLEAN NOT NULL LANGUAGE NATIVE FROM EXTENSION radixdb_spatial AS 'contains'",
        "CREATE FUNCTION public.st_intersects(left_value public.box2d NOT NULL, right_value public.box2d NOT NULL) RETURNS BOOLEAN NOT NULL LANGUAGE NATIVE FROM EXTENSION radixdb_spatial AS 'intersects'",
        "CREATE FUNCTION public.st_within_box(probe public.point NOT NULL, bounds public.box2d NOT NULL) RETURNS BOOLEAN NOT NULL LANGUAGE NATIVE FROM EXTENSION radixdb_spatial AS 'within_box'",
        "CREATE FUNCTION public.st_dwithin(probe public.point NOT NULL, center public.point NOT NULL, radius FLOAT NOT NULL) RETURNS BOOLEAN NOT NULL LANGUAGE NATIVE FROM EXTENSION radixdb_spatial AS 'within_radius'",
    ] {
        executor.execute(statement).unwrap();
    }
    executor
        .execute(
            "CREATE FUNCTION public.distance(left_value public.point NOT NULL, right_value public.point NOT NULL) \
             RETURNS FLOAT NOT NULL LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
             BEGIN RETURN st_distance(left_value, right_value); END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PLANNER SUPPORT public.within_box_support \
             FOR FUNCTION public.st_within_box(public.point, public.box2d) \
             FROM EXTENSION radixdb_spatial AS 'within_box_support'",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PLANNER SUPPORT public.within_radius_support \
             FOR FUNCTION public.st_dwithin(public.point, public.point, FLOAT) \
             FROM EXTENSION radixdb_spatial AS 'within_radius_support'",
        )
        .unwrap();
}

fn seed_database(path: &str) {
    let config = Config {
        path: Some(path.to_owned()),
        ..Config::default()
    };
    let registry = registry();
    let engine = opened_engine(config, Arc::clone(&registry));
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry);
    bind_spatial_catalog(&executor);
    executor
        .execute("CREATE TABLE spatial_cold (id INTEGER PRIMARY KEY, p public.point NOT NULL)")
        .unwrap();
    for x in 0..8_i64 {
        for y in 0..8_i64 {
            executor
                .execute_with_params(
                    "INSERT INTO spatial_cold (id, p) VALUES ($1, $2)",
                    smallvec::smallvec![
                        Value::Integer(x * 8 + y),
                        point_value(Point {
                            x: 1_000.0 + x as f64,
                            y: 2_000.0 + y as f64,
                        }),
                    ],
                )
                .unwrap();
        }
    }
    // External values become authoritative cold DATA before the abrupt exit.
    executor.execute("PRAGMA CHECKPOINT").unwrap();

    executor
        .execute(
            "CREATE TABLE spatial_indexed (id INTEGER PRIMARY KEY, p public.point NOT NULL); \
             CREATE TABLE spatial_scan (id INTEGER PRIMARY KEY, p public.point NOT NULL)",
        )
        .unwrap();
    // Leave these rows and the encoded index to WAL recovery. This proves the
    // supported v1.2 runtime-index boundary without teaching the immutable
    // physical INDEX writer how to call plugins.
    for x in 0..32_i64 {
        for y in 0..32_i64 {
            let id = x * 32 + y;
            let value = point_value(Point {
                x: 1_000.0 + x as f64,
                y: 2_000.0 + y as f64,
            });
            for table in ["spatial_indexed", "spatial_scan"] {
                executor
                    .execute_with_params(
                        &format!("INSERT INTO {table} (id, p) VALUES ($1, $2)"),
                        smallvec::smallvec![Value::Integer(id), value.clone()],
                    )
                    .unwrap();
            }
        }
    }
    executor
        .execute(
            "CREATE INDEX spatial_indexed_p ON spatial_indexed(p public.point_morton_btree) USING BTREE",
        )
        .unwrap();

    let replacement = point_value(Point {
        x: 1_010.25,
        y: 2_011.25,
    });
    for table in ["spatial_indexed", "spatial_scan"] {
        executor
            .execute_with_params(
                &format!("UPDATE {table} SET p = $1 WHERE id = 330"),
                smallvec::smallvec![replacement.clone()],
            )
            .unwrap();
        executor
            .execute(&format!("DELETE FROM {table} WHERE id = 1023"))
            .unwrap();
    }
    executor
        .execute_with_params(
            "UPDATE spatial_cold SET p = $1 WHERE id = 0",
            smallvec::smallvec![point_value(Point {
                x: 1_003.5,
                y: 2_003.5,
            })],
        )
        .unwrap();
    executor
        .execute("DELETE FROM spatial_cold WHERE id = 18")
        .unwrap();
    // Intentionally do not call close_engine: the parent must recover the
    // complete committed catalog, index, and post-checkpoint DML from WAL.
}

fn publish_catalog_then_exit(path: &str, commit: bool) {
    let config = Config {
        path: Some(path.to_owned()),
        ..Config::default()
    };
    let registry = registry();
    let engine = opened_engine(config, Arc::clone(&registry));
    let executor = Executor::with_plugin_registry(engine, registry);
    executor.execute("BEGIN").unwrap();
    bind_spatial_catalog(&executor);
    executor
        .execute(
            "CREATE TABLE publication_values (id INTEGER PRIMARY KEY, p public.point NOT NULL)",
        )
        .unwrap();
    executor
        .execute_with_params(
            "INSERT INTO publication_values (id, p) VALUES (1, $1)",
            smallvec::smallvec![point_value(Point { x: 7.0, y: 9.0 })],
        )
        .unwrap();
    executor
        .execute(
            "CREATE INDEX publication_values_p ON publication_values(p public.point_morton_btree) USING BTREE",
        )
        .unwrap();
    if commit {
        executor.execute("COMMIT").unwrap();
    }
}

fn integer_rows(executor: &Executor, sql: &str, parameters: Vec<Value>) -> Vec<i64> {
    let mut result = executor
        .execute_with_params(sql, parameters.into())
        .unwrap();
    let mut rows = Vec::new();
    while result.next() {
        rows.push(result.row().get(0).and_then(Value::as_int64).unwrap());
    }
    rows
}

#[test]
fn spatial_crash_seed() {
    let Ok(path) = std::env::var(CHILD_PATH_ENV) else {
        return;
    };
    seed_database(&path);
    std::process::exit(0);
}

#[test]
fn spatial_publication_seed() {
    let (Ok(path), Ok(mode)) = (
        std::env::var(CHILD_PATH_ENV),
        std::env::var(PUBLICATION_MODE_ENV),
    ) else {
        return;
    };
    publish_catalog_then_exit(&path, mode == "committed");
    std::process::exit(0);
}

#[test]
fn extension_catalog_and_index_publication_is_atomic_across_process_crash() {
    for (mode, committed) in [("uncommitted", false), ("committed", true)] {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().to_string_lossy().into_owned();
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("spatial_publication_seed")
            .arg("--nocapture")
            .env(CHILD_PATH_ENV, &path)
            .env(PUBLICATION_MODE_ENV, mode)
            .status()
            .unwrap();
        assert!(status.success(), "publication child failed for {mode}");

        let config = Config {
            path: Some(path),
            ..Config::default()
        };
        let registry = registry();
        let engine = opened_engine(config, Arc::clone(&registry));
        let executor = Executor::with_plugin_registry(Arc::clone(&engine), registry);
        assert_eq!(
            executor.plugin_admission().unwrap(),
            DatabasePluginAdmission::Normal
        );
        if committed {
            let expected = point_value(Point { x: 7.0, y: 9.0 });
            let mut result = executor
                .execute("SELECT p FROM publication_values WHERE id = 1")
                .unwrap();
            assert!(result.next());
            assert_eq!(result.row().get(0), Some(&expected));
            assert!(!result.next());
        } else {
            executor.execute("SELECT 1").unwrap();
            assert!(executor
                .execute("SELECT p FROM publication_values WHERE id = 1")
                .is_err());
        }
        drop(executor);
        engine.close_engine().unwrap();
    }
}

#[test]
fn spatial_index_pl_native_crash_reopen_and_missing_contracts() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().to_string_lossy().into_owned();
    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("spatial_crash_seed")
        .arg("--nocapture")
        .env(CHILD_PATH_ENV, &path)
        .status()
        .unwrap();
    assert!(status.success());

    let config = Config {
        path: Some(path.clone()),
        ..Config::default()
    };
    let registry = registry();
    let engine = opened_engine(config.clone(), Arc::clone(&registry));
    let executor = Executor::with_plugin_registry(Arc::clone(&engine), Arc::clone(&registry));
    assert_eq!(
        executor.plugin_admission().unwrap(),
        DatabasePluginAdmission::Normal
    );

    let cold_bounds = box_value(Box2d {
        min_x: 1_002.0,
        min_y: 2_002.0,
        max_x: 1_004.0,
        max_y: 2_004.0,
    });
    assert_eq!(
        integer_rows(
            &executor,
            "SELECT id FROM spatial_cold WHERE st_within_box(p, $1) ORDER BY id",
            vec![cold_bounds],
        ),
        vec![0, 19, 20, 26, 27, 28, 34, 35, 36]
    );

    let bounds = box_value(Box2d {
        min_x: 1_008.0,
        min_y: 2_008.0,
        max_x: 1_012.0,
        max_y: 2_012.0,
    });
    let direct_plan = registry
        .invoke_planner_support(
            derive_object_id(PACKAGE_ID, "within_box_support").unwrap(),
            &[
                NormalizedPredicateArgument::IndexedColumn,
                NormalizedPredicateArgument::Constant(&bounds),
            ],
            InvocationLimits::default(),
        )
        .unwrap();
    let PlannerSupportOutcome::Plan(direct_plan) = direct_plan else {
        panic!("spatial planner support unexpectedly fell back: {direct_plan:?}");
    };
    assert!(!direct_plan.spans.is_empty());
    assert!(direct_plan.requires_recheck);
    let indexed = integer_rows(
        &executor,
        "SELECT id FROM spatial_indexed WHERE st_within_box(p, $1) ORDER BY id",
        vec![bounds.clone()],
    );
    let scanned = integer_rows(
        &executor,
        "SELECT id FROM spatial_scan WHERE st_within_box(p, $1) ORDER BY id",
        vec![bounds.clone()],
    );
    assert_eq!(indexed, scanned);
    assert!(indexed.contains(&330));

    let center = point_value(Point {
        x: 1_010.0,
        y: 2_010.0,
    });
    assert_eq!(
        integer_rows(
            &executor,
            "SELECT id FROM spatial_indexed WHERE st_dwithin(p, $1, $2) ORDER BY id",
            vec![center.clone(), Value::Float(2.5)],
        ),
        integer_rows(
            &executor,
            "SELECT id FROM spatial_scan WHERE st_dwithin(p, $1, $2) ORDER BY id",
            vec![center.clone(), Value::Float(2.5)],
        )
    );

    let origin = point_value(Point { x: 0.0, y: 0.0 });
    let other = point_value(Point { x: 3.0, y: 4.0 });
    let mut parity = executor
        .execute_with_params(
            "SELECT st_distance($1, $2), distance($1, $2)",
            smallvec::smallvec![origin, other],
        )
        .unwrap();
    assert!(parity.next());
    assert_eq!(parity.row().get(0), parity.row().get(1));
    drop(parity);

    let mut explain = executor
        .execute_with_params(
            "EXPLAIN ANALYZE SELECT id FROM spatial_indexed WHERE st_within_box(p, $1)",
            smallvec::smallvec![bounds],
        )
        .unwrap();
    let mut saw_pruned_plan = false;
    let mut explain_lines = Vec::new();
    while explain.next() {
        let line = explain
            .row()
            .get(0)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        saw_pruned_plan |= line.contains("Plugin Candidate Scan")
            && line.contains("plans=1")
            && !line.contains("candidates=1023");
        explain_lines.push(line);
    }
    assert!(saw_pruned_plan, "unexpected EXPLAIN: {explain_lines:?}");
    drop(explain);
    drop(executor);
    engine.close_engine().unwrap();

    let missing_cases = [
        (Arc::new(PluginRegistry::empty()), None),
        (
            Arc::new(PluginRegistry::from_test_objects(
                [registry.package(&PACKAGE_ID).unwrap().as_ref().clone()],
                [],
                [],
                [],
                [],
                [],
            )),
            Some(ObjectKind::ExternalType),
        ),
        (
            Arc::new(PluginRegistry::from_test_objects(
                [registry.package(&PACKAGE_ID).unwrap().as_ref().clone()],
                ["point", "box2d", "polygon"].map(|local_id| {
                    registry
                        .external_type(&derive_object_id(PACKAGE_ID, local_id).unwrap())
                        .unwrap()
                        .as_ref()
                        .clone()
                }),
                [
                    "point_lt",
                    "point_le",
                    "point_eq",
                    "point_ge",
                    "point_gt",
                    "distance",
                    "contains",
                    "intersects",
                    "within_box",
                    "within_radius",
                ]
                .map(|local_id| {
                    registry
                        .function(&derive_object_id(PACKAGE_ID, local_id).unwrap())
                        .unwrap()
                        .as_ref()
                        .clone()
                }),
                [
                    "point_lt_operator",
                    "point_le_operator",
                    "point_eq_operator",
                    "point_ge_operator",
                    "point_gt_operator",
                ]
                .map(|local_id| {
                    registry
                        .operator(&derive_object_id(PACKAGE_ID, local_id).unwrap())
                        .unwrap()
                        .as_ref()
                        .clone()
                }),
                [],
                ["within_box_support", "within_radius_support"].map(|local_id| {
                    registry
                        .planner_support(&derive_object_id(PACKAGE_ID, local_id).unwrap())
                        .unwrap()
                        .as_ref()
                        .clone()
                }),
            )),
            Some(ObjectKind::OperatorClass),
        ),
    ];
    for (missing_registry, expected_kind) in missing_cases {
        let engine = opened_engine(config.clone(), Arc::clone(&missing_registry));
        let executor = Executor::with_plugin_registry(Arc::clone(&engine), missing_registry);
        let DatabasePluginAdmission::Restricted { issues } = executor.plugin_admission().unwrap()
        else {
            panic!("incomplete spatial package was admitted after reopen");
        };
        if let Some(expected_kind) = expected_kind {
            assert!(issues.iter().any(|issue| matches!(
                issue,
                RequirementIssue::MissingOrStaleObject { kind, .. } if *kind == expected_kind
            )));
        }
        assert!(executor.execute("SELECT 1").is_err());
        drop(executor);
        engine.close_engine().unwrap();
    }
}
