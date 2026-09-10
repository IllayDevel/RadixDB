//! JR-03: a predicate with one schema-proven owner must cross a nested left-deep
//! JOIN boundary before any wide intermediate is materialized.

use radixdb::storage::instrumentation;
use radixdb::{Database, Error, Result};

#[test]
fn bound_root_filter_reaches_leaf_through_nested_join_relation_set() -> Result<()> {
    let db = Database::open("memory://jr03_nested_relation_set")?;
    db.execute(
        "CREATE TABLE jr_a (id INTEGER PRIMARY KEY, a_payload INTEGER)",
        (),
    )?;
    db.execute(
        "CREATE TABLE jr_b (id INTEGER PRIMARY KEY, a_id INTEGER, c_id INTEGER)",
        (),
    )?;
    db.execute(
        "CREATE TABLE jr_c (id INTEGER PRIMARY KEY, c_payload INTEGER)",
        (),
    )?;
    db.execute(
        "INSERT INTO jr_a SELECT value, value * 10 FROM GENERATE_SERIES(1,128)",
        (),
    )?;
    db.execute("INSERT INTO jr_a VALUES (129,1290)", ())?;
    db.execute(
        "INSERT INTO jr_b SELECT value, value, value FROM GENERATE_SERIES(1,128)",
        (),
    )?;
    db.execute(
        "INSERT INTO jr_c SELECT value, value * 100 FROM GENERATE_SERIES(1,128)",
        (),
    )?;

    // The parent LEFT JOIN may push this predicate into its preserved left
    // subtree, but the nested LEFT JOIN must stop it at b's nullable boundary.
    let unmatched = db
        .query(
            "SELECT a.id \
             FROM jr_a a \
             LEFT JOIN jr_b b ON b.a_id = a.id \
             LEFT JOIN jr_c c ON c.id = b.c_id \
             WHERE b.id IS NULL",
            (),
        )?
        .collect_vec()?;
    assert_eq!(unmatched.len(), 1);
    assert_eq!(unmatched[0].get::<i64>(0)?, 129);

    let ambiguous = db.query(
        "SELECT a.id \
         FROM jr_a a JOIN jr_b b ON b.a_id = a.id \
         WHERE id = 127",
        (),
    );
    assert!(matches!(ambiguous, Err(Error::AmbiguousColumn(column)) if column == "id"));

    db.execute(
        "CREATE VIEW jr_a_view AS SELECT id, a_payload FROM jr_a",
        (),
    )?;
    let view_rows = db
        .query(
            "SELECT v.id \
             FROM jr_a_view v JOIN jr_b b ON b.a_id = v.id \
             WHERE a_payload = 1270",
            (),
        )?
        .collect_vec()?;
    assert_eq!(view_rows.len(), 1);
    assert_eq!(view_rows[0].get::<i64>(0)?, 127);

    let values_rows = db
        .query(
            "SELECT a.id \
             FROM (VALUES (127), (999)) AS v(probe) \
             JOIN jr_a a ON a.id = v.probe \
             WHERE probe = 127",
            (),
        )?
        .collect_vec()?;
    assert_eq!(values_rows.len(), 1);
    assert_eq!(values_rows[0].get::<i64>(0)?, 127);

    instrumentation::reset();
    let rows = db
        .query(
            "SELECT a.id, c.c_payload \
             FROM jr_a a \
             JOIN jr_b b ON b.a_id = a.id \
             JOIN jr_c c ON c.id = b.c_id \
             WHERE a_payload = 1270",
            (),
        )?
        .collect_vec()?;
    let counters = instrumentation::snapshot();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0)?, 127);
    assert_eq!(rows[0].get::<i64>(1)?, 12_700);
    assert_eq!(counters.join_operator_calls, 2);
    assert_eq!(
        counters.join_max_output_rows, 1,
        "the nested a-b edge must not publish all 128 rows before filtering a.id"
    );
    assert_eq!(counters.join_output_rows, 2);
    Ok(())
}
