use radixdb::optimizer::bloom::BloomEffectivenessTracker;
use radixdb::optimizer::global_workload_learner;
use radixdb::Database;

fn plan(db: &Database, sql: &str) -> String {
    db.query(sql, ())
        .expect("EXPLAIN must execute")
        .map(|row| {
            row.expect("EXPLAIN row")
                .get::<String>(0)
                .expect("plan text")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn r5_l04_batch_g_planner_explain_runtime_contracts_have_live_producers() {
    let db = Database::open("memory://r5_l04_planner_explain_contract").expect("database");

    assert!(
        db.query("EXPLAIN SELECT * FROM missing_table", ()).is_err(),
        "plan-only EXPLAIN must propagate table metadata errors"
    );

    db.execute("CREATE TABLE lhs (id INTEGER PRIMARY KEY, k INTEGER)", ())
        .unwrap();
    db.execute("CREATE TABLE rhs (id INTEGER PRIMARY KEY, k INTEGER)", ())
        .unwrap();
    db.execute("INSERT INTO lhs VALUES (1, 1), (2, 2)", ())
        .unwrap();
    db.execute("INSERT INTO rhs VALUES (1, 1), (2, 3)", ())
        .unwrap();

    let join_plan = plan(
        &db,
        "EXPLAIN SELECT lhs.id FROM lhs JOIN rhs ON lhs.k = rhs.k",
    );
    assert!(
        join_plan.contains("runtime candidate"),
        "EXPLAIN must label a statically selected join as a runtime candidate:\n{join_plan}"
    );

    let analyze = plan(&db, "EXPLAIN ANALYZE SELECT k FROM lhs WHERE k > 0 LIMIT 1");
    assert_eq!(
        analyze.matches("actual rows=").count(),
        0,
        "generic child nodes must not inherit the statement output cardinality:\n{analyze}"
    );
    assert!(
        analyze.contains("request-local counters unavailable"),
        "EXPLAIN ANALYZE must not report process-global I/O deltas:\n{analyze}"
    );

    db.execute(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, emb VECTOR(3))",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_emb ON docs(emb) USING HNSW", ())
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, '[1.0, 0.0, 0.0]')", ())
        .unwrap();
    let vector_plan = plan(
        &db,
        "EXPLAIN SELECT id, VEC_DISTANCE_L2(emb, '[1.0, 0.0, 0.0]') AS dist FROM docs ORDER BY dist LIMIT 5",
    );
    assert!(
        vector_plan.contains("Vector Access: runtime candidate"),
        "static vector eligibility must not be presented as the executed operator:\n{vector_plan}"
    );

    db.execute(
        "CREATE TABLE build_side (id INTEGER PRIMARY KEY, k INTEGER)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE probe_side (id INTEGER PRIMARY KEY, k INTEGER)",
        (),
    )
    .unwrap();
    let build_values = (0..100)
        .map(|i| format!("({i}, {i})"))
        .collect::<Vec<_>>()
        .join(",");
    let probe_values = (0..200)
        .map(|i| format!("({i}, {})", 10_000 + i))
        .collect::<Vec<_>>()
        .join(",");
    db.execute(&format!("INSERT INTO build_side VALUES {build_values}"), ())
        .unwrap();
    db.execute(&format!("INSERT INTO probe_side VALUES {probe_values}"), ())
        .unwrap();

    let bloom = BloomEffectivenessTracker::global();
    bloom.reset();
    let rows = db
        .query(
            "SELECT p.id FROM probe_side p JOIN build_side b ON p.k = b.k LIMIT 5",
            (),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(rows.is_empty());
    assert!(
        bloom.total_checks() > 0,
        "the streaming LIMIT join must actually execute its runtime Bloom filter"
    );

    let learner = global_workload_learner();
    learner.clear();
    db.query("SELECT 1", ())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        learner.total_queries(),
        1,
        "production query completion must feed the workload learner"
    );
    learner.clear();
}
