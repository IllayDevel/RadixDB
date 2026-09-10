// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

use chrono::{DateTime, TimeZone, Utc};
use radixdb::{named_params, Database};

fn explain(db: &Database, sql: &str) -> String {
    db.query(sql, ())
        .expect("EXPLAIN succeeds")
        .map(|row| row.expect("plan row").get::<String>(0).expect("plan text"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn query_ids(db: &Database, sql: &str) -> Vec<i64> {
    db.query(sql, ())
        .expect("query succeeds")
        .map(|row| row.expect("result row").get::<i64>(0).expect("id"))
        .collect()
}

fn timestamp(day: u32, hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, day, hour, 0, 0).unwrap()
}

#[test]
fn hot_timestamp_ranges_use_single_and_composite_btree_paths() {
    let db = Database::open("memory://timestamp_range_plan").expect("open db");
    db.execute(
        "CREATE TABLE single_jobs (id INTEGER PRIMARY KEY, due_at TIMESTAMP NOT NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX single_jobs_due_idx ON single_jobs (due_at)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO single_jobs VALUES
         (1, TIMESTAMP '2026-08-08 10:00:00'),
         (2, TIMESTAMP '2026-08-09 10:00:00'),
         (3, TIMESTAMP '2026-08-10 10:00:00')",
        (),
    )
    .unwrap();

    for predicate in [
        "due_at = TIMESTAMP '2026-08-09 10:00:00'",
        "due_at < TIMESTAMP '2026-08-09 10:00:00'",
        "due_at <= TIMESTAMP '2026-08-09 10:00:00'",
        "due_at > TIMESTAMP '2026-08-09 10:00:00'",
        "due_at >= TIMESTAMP '2026-08-09 10:00:00'",
        "due_at BETWEEN TIMESTAMP '2026-08-08 10:00:00' AND TIMESTAMP '2026-08-09 10:00:00'",
    ] {
        let plan = explain(
            &db,
            &format!("EXPLAIN SELECT id FROM single_jobs WHERE {predicate}"),
        );
        assert!(
            plan.contains("single_jobs_due_idx") && plan.contains("Access Path: scan.index"),
            "single-column TIMESTAMP predicate must use B-tree:\n{predicate}\n{plan}"
        );
    }

    db.execute(
        "CREATE TABLE jobs (
            id INTEGER PRIMARY KEY,
            state TEXT NOT NULL,
            due_at TIMESTAMP NOT NULL
         )",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX jobs_due_idx ON jobs (due_at, id)", ())
        .unwrap();
    db.execute(
        "INSERT INTO jobs VALUES
         (1, 'pending', TIMESTAMP '2026-08-08 10:00:00'),
         (2, 'pending', TIMESTAMP '2026-08-10 10:00:00'),
         (3, 'leased',  TIMESTAMP '2026-08-08 10:30:00'),
         (4, 'pending', TIMESTAMP '2026-08-08 11:00:00')",
        (),
    )
    .unwrap();

    let leading_range_plan = explain(
        &db,
        "EXPLAIN SELECT id FROM jobs
         WHERE due_at <= TIMESTAMP '2026-08-09 00:00:00'
         ORDER BY due_at, id LIMIT 2",
    );
    assert!(
        leading_range_plan.contains("jobs_due_idx")
            && leading_range_plan.contains("Access Path: scan.composite_index"),
        "leading TIMESTAMP range must use the composite B-tree:\n{leading_range_plan}"
    );

    let sql = "SELECT id FROM jobs
               WHERE state = 'pending'
                 AND due_at <= TIMESTAMP '2026-08-09 00:00:00'
               ORDER BY due_at, id LIMIT 2";
    let plan = explain(&db, &format!("EXPLAIN {sql}"));
    assert!(plan.contains("jobs_due_idx"), "missing index:\n{plan}");
    assert!(
        plan.contains("Access Path: scan.composite_index"),
        "leading TIMESTAMP range must be a composite index scan:\n{plan}"
    );
    assert!(
        plan.contains("Filter: state = pending"),
        "non-indexed state predicate must remain a residual filter:\n{plan}"
    );
    assert_eq!(query_ids(&db, sql), vec![1, 4]);

    assert_eq!(
        query_ids(
            &db,
            "SELECT id FROM jobs
             WHERE due_at BETWEEN TIMESTAMP '2026-08-08 10:00:00'
                              AND TIMESTAMP '2026-08-08 10:30:00'
             ORDER BY due_at, id",
        ),
        vec![1, 3]
    );

    let cutoff = timestamp(9, 0);
    let named_plan = db
        .query_named(
            "EXPLAIN SELECT id FROM jobs WHERE due_at <= :now ORDER BY due_at, id LIMIT 100",
            named_params! { now: cutoff },
        )
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        named_plan.contains("jobs_due_idx")
            && named_plan.contains("Access Path: scan.composite_index")
            && !named_plan.contains(":now"),
        "named TIMESTAMP parameter must be bound before planning:\n{named_plan}"
    );

    let positional_plan = db
        .query(
            "EXPLAIN SELECT id FROM jobs WHERE due_at <= $1 ORDER BY due_at, id LIMIT 100",
            (cutoff,),
        )
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        positional_plan.contains("jobs_due_idx")
            && positional_plan.contains("Access Path: scan.composite_index")
            && !positional_plan.contains("$1"),
        "positional TIMESTAMP parameter must be bound before planning:\n{positional_plan}"
    );

    db.execute(
        "CREATE TABLE scoped_jobs (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            due_at TIMESTAMP NOT NULL
         )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX scoped_jobs_due_idx ON scoped_jobs (tenant_id, due_at, id)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO scoped_jobs VALUES
         (1, 7, TIMESTAMP '2026-08-08 10:00:00'),
         (2, 7, TIMESTAMP '2026-08-10 10:00:00'),
         (3, 8, TIMESTAMP '2026-08-08 10:00:00')",
        (),
    )
    .unwrap();
    let scoped_sql = "SELECT id FROM scoped_jobs
                      WHERE tenant_id = 7
                        AND due_at >= TIMESTAMP '2026-08-08 00:00:00'
                      ORDER BY tenant_id, due_at, id";
    let scoped_plan = explain(&db, &format!("EXPLAIN {scoped_sql}"));
    assert!(
        scoped_plan.contains("scoped_jobs_due_idx")
            && scoped_plan.contains("Access Path: scan.composite_index"),
        "equality prefix plus trailing TIMESTAMP range must use the composite B-tree:\n{scoped_plan}"
    );
    assert_eq!(query_ids(&db, scoped_sql), vec![1, 2]);
}

#[test]
fn timestamp_range_uses_persisted_ordered_index_for_cold_and_mixed_storage() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        dir.path().display()
    ))
    .unwrap();
    db.execute(
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY, due_at TIMESTAMP NOT NULL)",
        (),
    )
    .unwrap();
    db.execute("CREATE INDEX jobs_due_idx ON jobs (due_at, id)", ())
        .unwrap();
    db.execute(
        "INSERT INTO jobs VALUES
         (1, TIMESTAMP '2026-08-08 10:00:00'),
         (2, TIMESTAMP '2026-08-10 10:00:00')",
        (),
    )
    .unwrap();
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();

    let query = "SELECT id FROM jobs
                 WHERE due_at <= TIMESTAMP '2026-08-09 00:00:00'
                 ORDER BY due_at, id";
    let cold_plan = explain(&db, &format!("EXPLAIN {query}"));
    assert!(
        cold_plan.contains("Access Path: scan.composite_index")
            && cold_plan.contains("jobs_due_idx")
            && cold_plan.contains("Cold Access Path: volume.composite_ordered_index")
            && !cold_plan.contains("Access Path: scan.cold_artifact"),
        "cold TIMESTAMP range must expose its persisted ordered path:\n{cold_plan}"
    );
    assert_eq!(query_ids(&db, query), vec![1]);

    db.execute(
        "INSERT INTO jobs VALUES (3, TIMESTAMP '2026-08-08 11:00:00')",
        (),
    )
    .unwrap();
    let mixed_plan = explain(&db, &format!("EXPLAIN {query}"));
    assert!(
        mixed_plan.contains("Access Path: scan.composite_index")
            && mixed_plan.contains("Cold Access Path: volume.composite_ordered_index")
            && !mixed_plan.contains("Access Path: scan.mixed_cold_artifact_hot"),
        "mixed TIMESTAMP range must merge persisted and hot index paths:\n{mixed_plan}"
    );
    assert_eq!(query_ids(&db, query), vec![1, 3]);
}

#[test]
fn single_column_timestamp_range_uses_ordered_postings_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        dir.path().display()
    ))
    .unwrap();
    db.execute(
        "CREATE TABLE sessions (id INTEGER PRIMARY KEY, expires_at TIMESTAMP NOT NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX sessions_expiry_idx ON sessions (expires_at)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO sessions VALUES
         (1, TIMESTAMP '2026-08-08 10:00:00'),
         (2, TIMESTAMP '2026-08-09 10:00:00'),
         (3, TIMESTAMP '2026-08-10 10:00:00')",
        (),
    )
    .unwrap();
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();

    let cutoff = timestamp(9, 10);
    let sql = "SELECT id FROM sessions
               WHERE expires_at >= :after
               ORDER BY expires_at LIMIT 2";
    let ids = db
        .query_named(sql, named_params! { after: cutoff })
        .unwrap()
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![2, 3]);

    let plan = db
        .query_named(&format!("EXPLAIN {sql}"), named_params! { after: cutoff })
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("Access Path: scan.index")
            && plan.contains("sessions_expiry_idx")
            && plan.contains("Cold Access Path: volume.ordered_index")
            && !plan.contains("Access Path: scan.cold_artifact"),
        "single-column TIMESTAMP range must use persisted ordered postings:\n{plan}"
    );
}
