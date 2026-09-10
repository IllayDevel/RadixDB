// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! RDB-0007 regressions for composite branch plans under OR.

use chrono::{TimeZone, Utc};
use radixdb::{named_params, Database};

fn collect_text(db: &Database, sql: &str, now: chrono::DateTime<Utc>, limit: i64) -> String {
    db.query_named(sql, named_params! { now: now, limit: limit })
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

fn collect_ids(db: &Database, sql: &str, now: chrono::DateTime<Utc>, limit: i64) -> Vec<i64> {
    db.query_named(sql, named_params! { now: now, limit: limit })
        .unwrap()
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect()
}

fn create_outbox(db: &Database, table: &str, with_indexes: bool) {
    db.execute(
        &format!(
            "CREATE TABLE {table} (
            id INTEGER PRIMARY KEY,
            state TEXT NOT NULL,
            available_at TIMESTAMP NOT NULL,
            lease_until TIMESTAMP,
            created_at TIMESTAMP NOT NULL,
            revision INTEGER NOT NULL
        )"
        ),
        (),
    )
    .unwrap();
    if with_indexes {
        db.execute(
            &format!(
                "CREATE INDEX {table}_available_idx
                 ON {table} (state, available_at)"
            ),
            (),
        )
        .unwrap();
        db.execute(
            &format!(
                "CREATE INDEX {table}_lease_idx
                 ON {table} (state, lease_until)"
            ),
            (),
        )
        .unwrap();
    }
    db.execute(
        &format!(
            "INSERT INTO {table} VALUES
         (1, 'pending', TIMESTAMP '2026-08-08 10:00:00', NULL,
             TIMESTAMP '2026-08-08 09:00:00', 1),
         (2, 'leased', TIMESTAMP '2026-08-08 08:00:00',
             TIMESTAMP '2026-08-08 11:00:00', TIMESTAMP '2026-08-08 08:00:00', 1),
         (3, 'leased', TIMESTAMP '2026-08-08 07:00:00', NULL,
             TIMESTAMP '2026-08-08 07:00:00', 1),
         (4, 'pending', TIMESTAMP '2026-08-11 10:00:00',
             TIMESTAMP '2026-08-08 06:00:00', TIMESTAMP '2026-08-08 06:00:00', 1),
         (5, 'pending', TIMESTAMP '2026-08-08 12:00:00', NULL,
             TIMESTAMP '2026-08-08 12:00:00', 1)"
        ),
        (),
    )
    .unwrap();
}

const QUERY: &str = "SELECT id, revision
FROM outbox_jobs
WHERE (state = 'pending' AND available_at <= :now)
   OR (state = 'leased' AND lease_until IS NOT NULL AND lease_until <= :now)
ORDER BY available_at, created_at, id
LIMIT :limit";

#[test]
fn hot_composite_or_branches_use_index_union_and_global_order_limit() {
    let db = Database::open("memory://rdb0007_hot").unwrap();
    create_outbox(&db, "outbox_jobs", true);
    create_outbox(&db, "outbox_jobs_seq", false);
    let now = Utc.with_ymd_and_hms(2026, 8, 9, 0, 0, 0).unwrap();

    let plan = collect_text(&db, &format!("EXPLAIN {QUERY}"), now, 2);
    assert!(
        plan.contains("Access Path: scan.multi_index")
            && plan.contains("outbox_jobs_available_idx")
            && plan.contains("outbox_jobs_lease_idx")
            && !plan.contains("Access Path: scan.seq")
            && !plan.contains(":now"),
        "both bound composite branches must be visible in one index-union plan:\n{plan}"
    );

    // ORDER BY/LIMIT is global across the union: leased row 2 sorts before
    // pending rows 1 and 5 even though it comes from the second branch.
    assert_eq!(collect_ids(&db, QUERY, now, 2), vec![2, 1]);
    let seq_query = QUERY.replace("outbox_jobs", "outbox_jobs_seq");
    let seq_plan = collect_text(&db, &format!("EXPLAIN {seq_query}"), now, 10);
    assert!(
        seq_plan.contains("Access Path: scan.seq"),
        "the oracle table deliberately has no secondary indexes:\n{seq_plan}"
    );
    assert_eq!(
        collect_ids(&db, QUERY, now, 10),
        collect_ids(&db, &seq_query, now, 10),
        "index union must be result-equivalent to the sequential oracle"
    );

    db.execute("BEGIN", ()).unwrap();
    assert_eq!(collect_ids(&db, QUERY, now, 10), vec![2, 1, 5]);
    db.execute("COMMIT", ()).unwrap();
}

#[test]
fn index_union_deduplicates_rows_matching_multiple_composite_branches() {
    let db = Database::open("memory://rdb0007_dedup").unwrap();
    create_outbox(&db, "outbox_jobs", true);
    db.execute(
        "CREATE INDEX outbox_jobs_created_idx
         ON outbox_jobs (state, created_at)",
        (),
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 8, 9, 0, 0, 0).unwrap();
    let sql = "SELECT id FROM outbox_jobs
               WHERE (state = 'pending' AND available_at <= :now)
                  OR (state = 'pending' AND created_at <= :now)
               ORDER BY id LIMIT :limit";

    let plan = collect_text(&db, &format!("EXPLAIN {sql}"), now, 10);
    assert!(
        plan.contains("Access Path: scan.multi_index")
            && plan.contains("outbox_jobs_available_idx")
            && plan.contains("outbox_jobs_created_idx"),
        "both overlapping composite branches must be planned:\n{plan}"
    );
    assert_eq!(
        collect_ids(&db, sql, now, 10),
        vec![1, 4, 5],
        "row 1/5 match both branches but must be returned once"
    );
}

#[test]
fn cold_and_mixed_or_plans_remain_honest_and_correct() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        dir.path().display()
    ))
    .unwrap();
    create_outbox(&db, "outbox_jobs", true);
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 8, 9, 0, 0, 0).unwrap();

    let cold_plan = collect_text(&db, &format!("EXPLAIN {QUERY}"), now, 10);
    assert!(
        cold_plan.contains("Access Path: scan.multi_index")
            && cold_plan.contains("Cold Access Path: volume.multi_index_union")
            && cold_plan.contains("outbox_jobs_available_idx")
            && cold_plan.contains("outbox_jobs_lease_idx")
            && !cold_plan.contains("Access Path: scan.cold_artifact"),
        "cold OR branches must use their persisted posting union:\n{cold_plan}"
    );
    assert_eq!(collect_ids(&db, QUERY, now, 10), vec![2, 1, 5]);

    db.execute(
        "INSERT INTO outbox_jobs VALUES
         (6, 'leased', TIMESTAMP '2026-08-08 06:00:00',
             TIMESTAMP '2026-08-08 07:00:00', TIMESTAMP '2026-08-08 05:00:00', 1)",
        (),
    )
    .unwrap();
    let mixed_plan = collect_text(&db, &format!("EXPLAIN {QUERY}"), now, 10);
    assert!(
        mixed_plan.contains("Access Path: scan.multi_index")
            && mixed_plan.contains("Cold Access Path: volume.multi_index_union")
            && !mixed_plan.contains("Access Path: scan.mixed_cold_artifact_hot"),
        "mixed storage must merge persisted and hot index unions:\n{mixed_plan}"
    );
    assert_eq!(collect_ids(&db, QUERY, now, 10), vec![6, 2, 1, 5]);
}
