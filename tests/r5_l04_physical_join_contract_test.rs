//! R5-L04 batch F: physical join algorithms must preserve logical JOIN semantics.

use radixdb::executor::OrderingProperty;
use radixdb::{Database, Result, Value};

fn db(name: &str) -> Database {
    Database::open(&format!("memory://r5_l04_join_{name}")).expect("open in-memory database")
}

#[test]
fn merge_join_applies_residual_on_predicates() -> Result<()> {
    let db = db("merge_residual");
    db.execute("CREATE TABLE l (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute(
        "INSERT INTO l SELECT value, value FROM GENERATE_SERIES(1,250)",
        (),
    )?;
    db.execute(
        "INSERT INTO r SELECT value, value FROM GENERATE_SERIES(1,250)",
        (),
    )?;
    let count: i64 = db.query_one("SELECT COUNT(*) FROM l JOIN r ON l.id=r.id AND l.v<r.v", ())?;
    assert_eq!(count, 0);
    Ok(())
}

#[test]
fn merge_ordering_requires_an_explicit_matching_physical_certificate() {
    let certified = OrderingProperty::ascending_nulls_last(vec![0, 1]);
    assert!(certified.proves_ascending_nulls_last(&[0]));
    assert!(certified.proves_ascending_nulls_last(&[0, 1]));
    assert!(!certified.proves_ascending_nulls_last(&[1]));
    assert!(!OrderingProperty::Unknown.proves_ascending_nulls_last(&[0]));
}

#[test]
fn cte_index_nested_loop_preserves_residual_on() -> Result<()> {
    let db = db("cte_index_nl");
    db.execute(
        "CREATE TABLE c_src (id INTEGER PRIMARY KEY, min_v INTEGER)",
        (),
    )?;
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute("INSERT INTO c_src VALUES (1,100),(2,5)", ())?;
    db.execute("INSERT INTO t VALUES (1,10),(2,20)", ())?;
    let ids: Vec<i64> = db
        .query(
            "WITH c AS (SELECT id, min_v FROM c_src) \
             SELECT t.id FROM c JOIN t ON c.id=t.id AND t.v>c.min_v ORDER BY t.id",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(ids, vec![2]);
    Ok(())
}

#[test]
fn outer_hash_residual_does_not_publish_false_unmatched_rows() -> Result<()> {
    let db = db("outer_residual");
    db.execute(
        "CREATE TABLE l (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
        (),
    )?;
    db.execute(
        "CREATE TABLE r (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
        (),
    )?;
    db.execute("INSERT INTO l VALUES (1,1,5),(2,2,5)", ())?;
    db.execute(
        "INSERT INTO r VALUES (10,1,4),(11,1,6),(20,2,4),(21,2,3)",
        (),
    )?;
    let rows = db
        .query(
            "SELECT l.id, r.id FROM l LEFT JOIN r ON l.k=r.k AND l.v<r.v ORDER BY l.id",
            (),
        )?
        .collect_vec()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64>(0)?, 1);
    assert_eq!(rows[0].get::<i64>(1)?, 11);
    assert_eq!(rows[1].get::<i64>(0)?, 2);
    assert!(rows[1].get_value(1).is_some_and(Value::is_null));

    let plan = db
        .query(
            "EXPLAIN ANALYZE SELECT l.id, r.id FROM l LEFT JOIN r ON l.k=r.k AND l.v<r.v",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<String>(0)))
        .collect::<Result<Vec<_>>>()?
        .join("\n");
    assert!(
        plan.contains("runtime candidate: Hash Join") || plan.contains("hash_streaming=1"),
        "LEFT residual must keep a hash-capable physical path:\n{plan}"
    );
    assert!(
        !plan.contains("runtime candidate: Nested Loop")
            && !plan.contains("Nested Loop Join")
            && !plan.contains("nested_loop=1"),
        "LEFT residual must not regress to nested-loop execution:\n{plan}"
    );
    Ok(())
}

#[test]
fn full_hash_residual_emits_each_unmatched_side_once() -> Result<()> {
    let db = db("full_outer_residual");
    db.execute(
        "CREATE TABLE l (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
        (),
    )?;
    db.execute(
        "CREATE TABLE r (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
        (),
    )?;
    db.execute("INSERT INTO l VALUES (1,1,5)", ())?;
    db.execute("INSERT INTO r VALUES (10,1,4),(11,1,3)", ())?;

    let rows = db
        .query(
            "SELECT l.id, r.id FROM l FULL JOIN r ON l.k=r.k AND l.v<r.v",
            (),
        )?
        .collect_vec()?;
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter()
            .filter(|row| {
                row.get_value(0).is_some_and(|value| !value.is_null())
                    && row.get_value(1).is_some_and(Value::is_null)
            })
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter(|row| {
                row.get_value(0).is_some_and(Value::is_null)
                    && row.get_value(1).is_some_and(|value| !value.is_null())
            })
            .count(),
        2
    );
    Ok(())
}

#[test]
fn merge_join_never_matches_null_keys() -> Result<()> {
    let db = db("merge_null");
    db.execute("CREATE TABLE l (id INTEGER PRIMARY KEY, k INTEGER)", ())?;
    db.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, k INTEGER)", ())?;
    db.execute(
        "INSERT INTO l SELECT value, value FROM GENERATE_SERIES(1,249)",
        (),
    )?;
    db.execute(
        "INSERT INTO r SELECT value, value FROM GENERATE_SERIES(1,249)",
        (),
    )?;
    db.execute("INSERT INTO l VALUES (250,NULL)", ())?;
    db.execute("INSERT INTO r VALUES (250,NULL)", ())?;
    let count: i64 = db.query_one("SELECT COUNT(*) FROM l JOIN r ON l.k=r.k", ())?;
    assert_eq!(count, 249);
    Ok(())
}

#[test]
fn index_nested_loop_limit_scans_until_enough_matches() -> Result<()> {
    let db = db("index_limit");
    db.execute("CREATE TABLE outer_t (id INTEGER PRIMARY KEY)", ())?;
    db.execute("CREATE TABLE inner_t (id INTEGER PRIMARY KEY)", ())?;
    db.execute(
        "INSERT INTO outer_t SELECT value FROM GENERATE_SERIES(1,200)",
        (),
    )?;
    db.execute("INSERT INTO inner_t VALUES (198),(199),(200)", ())?;
    let ids: Vec<i64> = db
        .query(
            "SELECT o.id FROM outer_t o JOIN inner_t i ON o.id=i.id LIMIT 3",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(ids, vec![198, 199, 200]);
    Ok(())
}

#[test]
fn index_nested_loop_filter_compile_errors_are_not_dropped() -> Result<()> {
    let db = db("index_filter_error");
    db.execute("CREATE TABLE l (id INTEGER PRIMARY KEY)", ())?;
    db.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute("INSERT INTO l VALUES (1)", ())?;
    db.execute("INSERT INTO r VALUES (1,10)", ())?;
    assert!(db
        .query(
            "SELECT l.id FROM l JOIN r ON l.id=r.id WHERE r.missing=1",
            (),
        )
        .is_err());
    Ok(())
}
