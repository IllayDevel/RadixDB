// Copyright 2026 RadixDB Contributors
// Licensed under the Apache License, Version 2.0

#![cfg(feature = "prerelease-postgres")]

use postgres::{Client, NoTls, Row as PgRow};
use radixdb::{Database, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Cell {
    Null,
    Integer(i64),
    Boolean(bool),
    Text(String),
}

fn radix_rows(db: &Database, sql: &str) -> Vec<Vec<Cell>> {
    let mut result = db.query(sql, ()).unwrap_or_else(|error| {
        panic!("RadixDB query failed: {sql}\n{error}");
    });
    let width = result.columns().len();
    result
        .by_ref()
        .map(|row| {
            let row = row.expect("RadixDB row");
            (0..width)
                .map(|index| match row.get_value(index) {
                    Some(Value::Null(_)) | None => Cell::Null,
                    Some(Value::Integer(value)) => Cell::Integer(*value),
                    Some(Value::Boolean(value)) => Cell::Boolean(*value),
                    Some(Value::Text(value)) => Cell::Text(value.to_string()),
                    value => panic!("unsupported RadixDB oracle value at {index}: {value:?}"),
                })
                .collect()
        })
        .collect()
}

fn pg_distinct_rows(rows: Vec<PgRow>) -> Vec<Vec<Cell>> {
    rows.into_iter()
        .map(|row| {
            vec![
                Cell::Integer(row.get::<_, i64>(0)),
                Cell::Integer(row.get::<_, i64>(1)),
                Cell::Boolean(row.get::<_, bool>(2)),
                row.get::<_, Option<String>>(3)
                    .map(Cell::Text)
                    .unwrap_or(Cell::Null),
            ]
        })
        .collect()
}

fn pg_cube_rows(rows: Vec<PgRow>) -> Vec<Vec<Cell>> {
    rows.into_iter()
        .map(|row| {
            vec![
                row.get::<_, Option<i64>>(0)
                    .map(Cell::Integer)
                    .unwrap_or(Cell::Null),
                row.get::<_, Option<bool>>(1)
                    .map(Cell::Boolean)
                    .unwrap_or(Cell::Null),
                Cell::Integer(row.get::<_, i64>(2)),
                row.get::<_, Option<i64>>(3)
                    .map(Cell::Integer)
                    .unwrap_or(Cell::Null),
            ]
        })
        .collect()
}

fn pg_procedural_leaf_rows(rows: Vec<PgRow>) -> Vec<Vec<Cell>> {
    rows.into_iter()
        .map(|row| {
            vec![
                Cell::Integer(row.get::<_, i64>(0)),
                row.get::<_, Option<i64>>(1)
                    .map(Cell::Integer)
                    .unwrap_or(Cell::Null),
                Cell::Text(row.get::<_, String>(2)),
                row.get::<_, Option<bool>>(3)
                    .map(Cell::Boolean)
                    .unwrap_or(Cell::Null),
                row.get::<_, Option<bool>>(4)
                    .map(Cell::Boolean)
                    .unwrap_or(Cell::Null),
            ]
        })
        .collect()
}

#[test]
fn postgres_distinct_on_cube_and_explicit_null_order_match_exactly() {
    let dsn = std::env::var("RADIXDB_PRERELEASE_PG_DSN")
        .expect("RADIXDB_PRERELEASE_PG_DSN must identify the prerelease-owned PostgreSQL oracle");
    let mut postgres = Client::connect(&dsn, NoTls).expect("connect PostgreSQL oracle");
    let mut transaction = postgres
        .transaction()
        .expect("begin PostgreSQL oracle transaction");
    transaction
        .batch_execute(
            "CREATE TEMP TABLE b7_oracle_items (
                id BIGINT PRIMARY KEY,
                grp BIGINT NOT NULL,
                score INTEGER,
                flag BOOLEAN NOT NULL,
                label TEXT
             );
             INSERT INTO b7_oracle_items VALUES
                (1, 1, 10, TRUE, 'low'),
                (2, 1, 30, FALSE, 'high'),
                (3, 1, NULL, TRUE, NULL),
                (4, 2, 20, FALSE, 'middle'),
                (5, 2, 20, TRUE, 'tie'),
                (6, 3, NULL, FALSE, NULL);",
        )
        .expect("prepare PostgreSQL oracle fixture");

    let radix = Database::open_in_memory().unwrap();
    radix
        .execute(
            "CREATE TABLE b7_oracle_items (
                id INTEGER PRIMARY KEY,
                grp INTEGER NOT NULL,
                score INTEGER,
                flag BOOLEAN NOT NULL,
                label TEXT
             )",
            (),
        )
        .unwrap();
    radix
        .execute(
            "INSERT INTO b7_oracle_items VALUES
                (1, 1, 10, TRUE, 'low'),
                (2, 1, 30, FALSE, 'high'),
                (3, 1, NULL, TRUE, NULL),
                (4, 2, 20, FALSE, 'middle'),
                (5, 2, 20, TRUE, 'tie'),
                (6, 3, NULL, FALSE, NULL)",
            (),
        )
        .unwrap();

    let distinct_sql = "SELECT DISTINCT ON (grp) grp, id, flag, label
                        FROM b7_oracle_items
                        ORDER BY grp, score DESC NULLS LAST, id";
    let pg_distinct = pg_distinct_rows(
        transaction
            .query(distinct_sql, &[])
            .expect("PostgreSQL DISTINCT ON"),
    );
    assert_eq!(radix_rows(&radix, distinct_sql), pg_distinct);

    let cube_sql = "SELECT grp, flag, COUNT(*), SUM(score)
                    FROM b7_oracle_items
                    GROUP BY CUBE(grp, flag)
                    ORDER BY grp ASC NULLS LAST, flag ASC NULLS LAST";
    let pg_cube = pg_cube_rows(transaction.query(cube_sql, &[]).expect("PostgreSQL CUBE"));
    assert_eq!(radix_rows(&radix, cube_sql), pg_cube);

    transaction
        .rollback()
        .expect("rollback isolated PostgreSQL oracle fixture");
}

/// Differential oracle for SQL leaves admitted inside Radix procedures.
///
/// This deliberately compares only portable SQL expression semantics. It is
/// not, and must not become, a claim of PL/pgSQL syntax compatibility.
#[test]
fn procedural_portable_sql_leaves_match_postgresql() {
    let dsn = std::env::var("RADIXDB_PRERELEASE_PG_DSN")
        .expect("RADIXDB_PRERELEASE_PG_DSN must identify the prerelease-owned PostgreSQL oracle");
    let mut postgres = Client::connect(&dsn, NoTls).expect("connect PostgreSQL oracle");
    let mut transaction = postgres
        .transaction()
        .expect("begin PostgreSQL oracle transaction");
    transaction
        .batch_execute(
            "CREATE TEMP TABLE procedural_leaf_items (
                id BIGINT PRIMARY KEY,
                lhs BIGINT,
                rhs BIGINT NOT NULL,
                flag BOOLEAN,
                label TEXT
             );
             INSERT INTO procedural_leaf_items VALUES
                (1, 9, 4, TRUE, 'left'),
                (2, 2, 8, FALSE, NULL),
                (3, NULL, 5, NULL, 'unknown'),
                (4, 7, 7, TRUE, NULL);",
        )
        .expect("prepare PostgreSQL procedural leaf fixture");

    let radix = Database::open_in_memory().unwrap();
    radix
        .execute(
            "CREATE TABLE procedural_leaf_items (
                id INTEGER PRIMARY KEY,
                lhs INTEGER,
                rhs INTEGER NOT NULL,
                flag BOOLEAN,
                label TEXT
             )",
            (),
        )
        .unwrap();
    radix
        .execute(
            "INSERT INTO procedural_leaf_items VALUES
                (1, 9, 4, TRUE, 'left'),
                (2, 2, 8, FALSE, NULL),
                (3, NULL, 5, NULL, 'unknown'),
                (4, 7, 7, TRUE, NULL)",
            (),
        )
        .unwrap();

    let expression_sql = "SELECT id,
                CASE
                    WHEN lhs IS NULL THEN -1
                    WHEN lhs > rhs THEN lhs - rhs
                    ELSE rhs - lhs
                END,
                COALESCE(label, 'missing'),
                flag AND lhs > rhs,
                lhs = rhs
           FROM procedural_leaf_items
          ORDER BY id";
    let postgres_rows = pg_procedural_leaf_rows(
        transaction
            .query(expression_sql, &[])
            .expect("PostgreSQL portable procedural leaves"),
    );
    assert_eq!(radix_rows(&radix, expression_sql), postgres_rows);

    transaction
        .batch_execute(
            "UPDATE procedural_leaf_items
                SET lhs = COALESCE(lhs, 0) + 1
              WHERE flag = TRUE;
             DELETE FROM procedural_leaf_items WHERE lhs < rhs;",
        )
        .expect("mutate PostgreSQL procedural leaf fixture");
    radix
        .execute(
            "UPDATE procedural_leaf_items
                SET lhs = COALESCE(lhs, 0) + 1
              WHERE flag = TRUE",
            (),
        )
        .unwrap();
    radix
        .execute("DELETE FROM procedural_leaf_items WHERE lhs < rhs", ())
        .unwrap();

    let final_sql = "SELECT id, lhs, COALESCE(label, 'missing'), flag, lhs = rhs
                       FROM procedural_leaf_items ORDER BY id";
    let postgres_rows = pg_procedural_leaf_rows(
        transaction
            .query(final_sql, &[])
            .expect("PostgreSQL DML leaf outcome"),
    );
    assert_eq!(radix_rows(&radix, final_sql), postgres_rows);

    transaction
        .rollback()
        .expect("rollback isolated PostgreSQL procedural fixture");
}
