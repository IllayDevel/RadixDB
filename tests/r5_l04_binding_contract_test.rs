//! R5-L04 batch A: one bound row-shape/type contract for every SQL source.

use radixdb::functions::{global_registry, FunctionType};
use radixdb::{Database, Result, Value};

fn db(name: &str) -> Database {
    Database::open(&format!("memory://r5_l04_binding_{name}")).expect("open in-memory database")
}

#[test]
fn tvf_is_part_of_function_introspection() {
    let registry = global_registry();
    assert!(registry
        .get_infos("GENERATE_SERIES")
        .iter()
        .any(|info| info.function_type() == FunctionType::TableValued));
    assert!(registry
        .list_all()
        .iter()
        .any(|name| name == "GENERATE_SERIES"));
}

#[test]
fn durable_view_sql_preserves_literals_and_quoted_aliases() -> Result<()> {
    let db = db("view_sql");
    db.execute(
        "CREATE VIEW quoted_view AS SELECT 'O''Reilly' AS \"Exact Label\"",
        (),
    )?;

    let rows = db
        .query("SELECT \"Exact Label\" FROM quoted_view", ())?
        .collect_vec()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String>(0)?, "O'Reilly");
    Ok(())
}

#[test]
fn unresolved_quoted_identifier_is_not_a_string_literal() {
    let db = db("quoted_identifier");
    assert!(db.query("SELECT \"definitely_missing\"", ()).is_err());
}

#[test]
fn values_and_tvfs_require_exact_bound_row_shape() {
    let db = db("source_shape");
    for sql in [
        "SELECT * FROM (VALUES (1, 2), (3)) AS v(a, b)",
        "SELECT * FROM (VALUES (1, 2)) AS v(a)",
        "SELECT * FROM generate_series(1, 2) AS g(a, b)",
    ] {
        assert!(
            db.query(sql, ()).is_err(),
            "must reject shape mismatch: {sql}"
        );
    }
}

#[test]
fn except_is_left_associative_with_union() -> Result<()> {
    let db = db("set_associativity");
    let rows = db
        .query("SELECT 1 AS x UNION SELECT 2 EXCEPT SELECT 1", ())?
        .collect_vec()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0)?, 2);
    Ok(())
}

#[test]
fn show_create_table_is_executable_and_preserves_schema_contract() -> Result<()> {
    let db = db("show_create");
    db.execute(
        "CREATE TABLE source_table (\
            id INTEGER PRIMARY KEY, \
            a INTEGER, \
            b INTEGER, \
            embedding VECTOR(3), \
            CHECK (a > 0), \
            UNIQUE (a, b)\
        )",
        (),
    )?;

    let shown = db
        .query("SHOW CREATE TABLE source_table", ())?
        .collect_vec()?[0]
        .get::<String>(1)?;
    let clone_sql = shown.replacen("source_table", "clone_table", 1);
    db.execute(&clone_sql, ())?;

    assert!(db
        .execute("INSERT INTO clone_table (id, a, b) VALUES (1, -1, 1)", ())
        .is_err());
    db.execute("INSERT INTO clone_table (id, a, b) VALUES (2, 1, 1)", ())?;
    assert!(db
        .execute("INSERT INTO clone_table (id, a, b) VALUES (3, 1, 1)", ())
        .is_err());
    assert!(shown.contains("VECTOR(3)"), "{shown}");
    Ok(())
}

#[test]
fn scalar_and_in_subqueries_require_exact_arity() {
    let db = db("subquery_arity");
    for sql in [
        "SELECT (SELECT 1, 2)",
        "SELECT 1 IN (SELECT 1, 2)",
        "SELECT (1, 2) IN (SELECT 1)",
    ] {
        assert!(
            db.query(sql, ()).is_err(),
            "must reject arity mismatch: {sql}"
        );
    }
}

#[test]
fn set_operations_bind_one_common_output_type() -> Result<()> {
    let db = db("set_common_type");
    let rows = db
        .query(
            "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2.5 AS x) AS valueset",
            (),
        )?
        .collect_vec()?;
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|row| matches!(row.get_value(0), Some(Value::Float(_)))));
    Ok(())
}
