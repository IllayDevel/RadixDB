// Copyright 2026 RadixDB Contributors
// Licensed under the Apache License, Version 2.0.

use radixdb::{DataType, Database, Result, Value};

fn database(name: &str) -> Database {
    Database::open(&format!("memory://v2_r7_{name}")).expect("open in-memory database")
}

fn collect_i64(db: &Database, sql: &str) -> Result<Vec<i64>> {
    db.query(sql, ())?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect()
}

#[test]
fn cte_joins_rejoin_the_complete_select_pipeline() -> Result<()> {
    let db = database("cte_tail");
    db.execute(
        "CREATE TABLE target (id INTEGER PRIMARY KEY, v INTEGER); \
         INSERT INTO target VALUES (0, 5), (1, 10), (2, 10), (3, 20)",
        (),
    )?;

    // Fractional Float must not alias INTEGER PK 0 in the indexed nested-loop path.
    assert!(db
        .query(
            "WITH keys(k) AS (SELECT 0.5) \
             SELECT t.id FROM keys k JOIN target t ON k.k = t.id",
            (),
        )?
        .collect_vec()?
        .is_empty());

    let page = collect_i64(
        &db,
        "WITH keys(k) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) \
         SELECT t.id FROM keys k JOIN target t ON k.k=t.id \
         ORDER BY -t.id LIMIT 1 OFFSET 1",
    )?;
    assert_eq!(page, vec![2]);

    let distinct = collect_i64(
        &db,
        "WITH keys(k) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) \
         SELECT DISTINCT t.v FROM keys k JOIN target t ON k.k=t.id ORDER BY t.v",
    )?;
    assert_eq!(distinct, vec![10, 20]);

    let distinct_on: Vec<i64> = db
        .query(
            "WITH keys(k) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) \
             SELECT DISTINCT ON (t.v) t.v, t.id FROM keys k JOIN target t ON k.k=t.id \
             ORDER BY t.v, t.id DESC",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<i64>(1)))
        .collect::<Result<_>>()?;
    assert_eq!(distinct_on, vec![2, 3]);

    let unioned = collect_i64(
        &db,
        "WITH keys(k) AS (SELECT 1) \
         SELECT t.id FROM keys k JOIN target t ON k.k=t.id \
         UNION SELECT 99 ORDER BY 1",
    )?;
    assert_eq!(unioned, vec![1, 99]);
    Ok(())
}

#[test]
fn recursive_cte_binds_types_from_all_anchor_and_recursive_rows() -> Result<()> {
    let db = database("recursive_types");
    db.execute(
        "CREATE TABLE anchors (id INTEGER PRIMARY KEY, x INTEGER)",
        (),
    )?;
    db.execute("INSERT INTO anchors VALUES (1, NULL), (2, 1)", ())?;

    let values: Vec<Option<i64>> = db
        .query(
            "WITH RECURSIVE r(x) AS ( \
                 SELECT x FROM anchors \
                 UNION ALL SELECT x + 1 FROM r WHERE x = 1 \
             ) SELECT x FROM r ORDER BY x NULLS FIRST",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<Option<i64>>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(values, vec![None, Some(1), Some(2)]);
    Ok(())
}

#[test]
fn subquery_rewrite_preserves_types_and_visits_nested_containers() -> Result<()> {
    let db = database("subquery_rewrite");
    db.execute("CREATE TABLE typed (id INTEGER PRIMARY KEY, d DATE)", ())?;
    db.execute("INSERT INTO typed VALUES (1, DATE '2026-08-15')", ())?;

    let rows = db
        .query("SELECT (SELECT d FROM typed WHERE id=1)", ())?
        .collect_vec()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get_value(0).expect("typed scalar").data_type(),
        DataType::Date
    );

    let nested: i64 = db.query_one(
        "SELECT CASE (SELECT 1) WHEN 1 \
                THEN CAST((SELECT 2) AS INTEGER) ELSE 0 END",
        (),
    )?;
    assert_eq!(nested, 2);

    db.execute(
        "CREATE TABLE outer_rows (id INTEGER PRIMARY KEY, outer_only INTEGER)",
        (),
    )?;
    db.execute("CREATE TABLE inner_rows (id INTEGER PRIMARY KEY)", ())?;
    db.execute("INSERT INTO outer_rows VALUES (1, 41), (2, 42)", ())?;
    db.execute("INSERT INTO inner_rows VALUES (10)", ())?;
    let correlated = collect_i64(
        &db,
        "SELECT (SELECT outer_only FROM inner_rows LIMIT 1) \
         FROM outer_rows ORDER BY id",
    )?;
    assert_eq!(correlated, vec![41, 42]);

    let filtered = collect_i64(
        &db,
        "SELECT id FROM outer_rows \
         WHERE EXISTS (SELECT 1 FROM inner_rows WHERE outer_only = 41) \
         ORDER BY id",
    )?;
    assert_eq!(filtered, vec![1]);

    let having = db
        .query(
            "SELECT (SELECT COUNT(*) FROM inner_rows \
             HAVING outer_rows.outer_only > 41) \
             FROM outer_rows ORDER BY id",
            (),
        )?
        .collect_vec()?;
    assert!(having[0].get_value(0).is_some_and(Value::is_null));
    assert_eq!(having[1].get::<i64>(0)?, 1);

    let cast_predicate = collect_i64(
        &db,
        "SELECT id FROM outer_rows \
         WHERE CAST((SELECT 1) AS INTEGER) = 1 ORDER BY id",
    )
    .expect("CAST-contained scalar subquery must be rewritten");
    assert_eq!(cast_predicate, vec![1, 2]);

    let like_pattern = collect_i64(
        &db,
        "SELECT id FROM outer_rows \
         WHERE CAST(outer_only AS TEXT) LIKE (SELECT '4%') ORDER BY id",
    )
    .expect("LIKE-contained scalar subquery must be rewritten");
    assert_eq!(like_pattern, vec![1, 2]);
    Ok(())
}

#[test]
fn aggregate_and_window_projection_errors_are_not_null_cells() -> Result<()> {
    let db = database("projection_errors");
    db.execute(
        "CREATE TABLE values_t (id INTEGER PRIMARY KEY, v INTEGER)",
        (),
    )?;
    db.execute("INSERT INTO values_t VALUES (1, 10), (2, 20)", ())?;

    for sql in [
        "SELECT MOD(COUNT(*), 0) FROM values_t",
        "SELECT MOD(ROW_NUMBER() OVER (), 0) FROM values_t",
    ] {
        match db.query(sql, ()) {
            Err(_) => {}
            Ok(rows) => assert!(rows.collect_vec().is_err(), "query must fail: {sql}"),
        }
    }
    Ok(())
}

#[test]
fn aggregate_window_range_keeps_exact_integer_boundaries() -> Result<()> {
    let db = database("window_range");
    db.execute(
        "CREATE TABLE w (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
        (),
    )?;
    db.execute(
        "INSERT INTO w VALUES \
         (1, 9007199254740992, 10), (2, 9007199254740993, 20)",
        (),
    )?;
    let sums: Vec<i64> = db
        .query(
            "SELECT SUM(v) OVER (ORDER BY k \
             RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING) FROM w ORDER BY k",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(sums, vec![10, 20]);
    Ok(())
}

#[test]
fn as_of_timestamp_rejects_values_outside_nanosecond_domain() -> Result<()> {
    let db = database("as_of_range");
    db.execute("CREATE TABLE temporal (id INTEGER PRIMARY KEY)", ())?;
    db.execute("INSERT INTO temporal VALUES (1)", ())?;
    let error = match db.query(
        "SELECT * FROM temporal AS OF TIMESTAMP '9999-01-01 00:00:00'",
        (),
    ) {
        Err(error) => error,
        Ok(_) => panic!("year 9999 must not map to Unix epoch"),
    };
    assert!(error.to_string().contains("outside"));
    Ok(())
}
