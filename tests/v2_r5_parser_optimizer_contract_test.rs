// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

use std::sync::Arc;

use radixdb::parser::parse_sql;
use radixdb::{Engine, MVCCEngine};

fn executor() -> (Arc<MVCCEngine>, radixdb::executor::Executor) {
    let engine = Arc::new(MVCCEngine::in_memory());
    engine.open_engine().expect("open engine");
    let executor = radixdb::executor::Executor::new(Arc::clone(&engine));
    (engine, executor)
}

#[test]
fn missing_delimiter_rejects_the_whole_program_before_mutation() {
    let (engine, executor) = executor();
    executor
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create table");

    assert!(executor
        .execute("INSERT INTO t VALUES (1) SELECT 1")
        .is_err());
    let tx = engine.begin_transaction().expect("begin");
    let rows = tx
        .get_table("t")
        .expect("table")
        .collect_rows_with_limit_unordered(None, 10, 0)
        .expect("scan");
    assert!(
        rows.is_empty(),
        "malformed program must not commit a prefix"
    );

    assert_eq!(
        parse_sql("SELECT 1; SELECT 2;;")
            .expect("delimited program")
            .len(),
        2
    );
}

#[test]
fn numeric_and_parameter_admission_is_fail_closed() {
    assert!(parse_sql("SELECT $1, ?").is_err());
    assert!(parse_sql("SELECT ?, $1").is_err());
    assert!(parse_sql("SELECT $1, $2").is_ok());
    assert!(parse_sql("SELECT ?, ?").is_ok());

    for sql in [
        "SELECT 1e999",
        "SELECT -1e999",
        "SELECT 1e-9999",
        "SELECT -1e-9999",
    ] {
        assert!(
            parse_sql(sql).is_err(),
            "invented f64 value admitted: {sql}"
        );
    }
    for sql in ["SELECT 0e-9999", "SELECT -0.0", "SELECT 5e-324"] {
        assert!(
            parse_sql(sql).is_ok(),
            "representable literal rejected: {sql}"
        );
    }
}

#[test]
fn persisted_sql_display_round_trips_semantics() {
    for sql in [
        "SELECT -1.5",
        "SELECT '{}'",
        "SELECT a, COUNT(*) FROM t GROUP BY GROUPING SETS ((a), ())",
        "SELECT a FROM t UNION SELECT a FROM u ORDER BY a LIMIT 1 OFFSET 1",
        "SELECT `select` FROM `table`",
        "COPY t FROM 'a''b.csv' WITH (FORMAT CSV, NULL 'x''y')",
        "CREATE INDEX idx ON t(v) USING HNSW WITH (metric = 'co''sine')",
    ] {
        let statements = parse_sql(sql).unwrap_or_else(|error| panic!("source {sql:?}: {error}"));
        assert_eq!(statements.len(), 1);
        let rendered = statements[0].to_string();
        let reparsed = parse_sql(&rendered)
            .unwrap_or_else(|error| panic!("rendered {rendered:?} from {sql:?}: {error}"));
        assert_eq!(reparsed.len(), 1);
        assert_eq!(reparsed[0].to_string(), rendered);
    }

    let negative = parse_sql("SELECT -1.5").unwrap()[0].to_string();
    assert!(negative.contains("-1.5"));
    let brace_text = parse_sql("SELECT '{}'").unwrap()[0].to_string();
    assert!(!brace_text.contains("JSON"));
    let grouping =
        parse_sql("SELECT a FROM t GROUP BY GROUPING SETS ((a), ())").unwrap()[0].to_string();
    assert!(grouping.contains("GROUP BY GROUPING SETS"));
}

#[test]
fn incomplete_or_ambiguous_grammar_is_rejected() {
    for sql in [
        "SELECT 'a' LIKE 'a' ESCAPE '!!'",
        "SELECT 'a' LIKE 'a' ESCAPE $1",
        "SELECT * FROM t JOIN u",
        "SELECT * FROM t LEFT JOIN u",
        "CREATE TABLE c (id INTEGER, p INTEGER REFERENCES p(id) ON DELETE CASCADE ON DELETE RESTRICT)",
        "BEGIN ISOLATION LEVEL REPEATABLE",
    ] {
        assert!(parse_sql(sql).is_err(), "invalid grammar admitted: {sql}");
    }
    assert!(parse_sql("SELECT 'a%' LIKE 'a!%' ESCAPE '!'").is_ok());
    assert!(parse_sql("SELECT * FROM t CROSS JOIN u").is_ok());
}

#[test]
fn multiline_diagnostic_pointer_tracks_the_full_prefix() {
    let sql = (1..=11)
        .map(|_| "SELECT 1;")
        .chain(std::iter::once("SELECT FROM"))
        .collect::<Vec<_>>()
        .join("\n");
    let formatted = parse_sql(&sql)
        .expect_err("line 12 must fail")
        .format_errors();
    let lines = formatted.lines().collect::<Vec<_>>();
    let source_index = lines
        .iter()
        .position(|line| line.starts_with("Line 12: "))
        .expect("line 12 context");
    let caret = lines[source_index + 1];
    assert!(
        caret.starts_with("         "),
        "caret lost diagnostic prefix: {caret:?}"
    );
    assert!(caret.ends_with('^'));
}
