// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! RDB-0011 regressions for persisted cold composite equality postings.

use std::{
    collections::BTreeMap,
    net::Ipv4Addr,
    path::{Path, PathBuf},
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb::{named_params, Database, Value};
use radixdb_client::{Connection, ExecuteResult, WireValue};

const USER_A: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x01, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa6,
];
const USER_B: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x02, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa7,
];
const AGGREGATE_A: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x81, 0x73, 0x01, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa6,
];
const AGGREGATE_B: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x81, 0x73, 0x02, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa7,
];
const OUTBOX_A: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x82, 0x73, 0x01, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa6,
];
const OUTBOX_B: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x82, 0x73, 0x02, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa7,
];

const LOOKUP: &str = "SELECT id FROM sync_events
    WHERE user_id = :user_id
      AND aggregate_id = :aggregate_id
      AND event_type = :event_type
    ORDER BY id";

const LITERAL_LOOKUP: &str = "SELECT id FROM sync_events
    WHERE user_id = '019fe3bf-6880-7301-b5d0-58ce355457a6'
      AND aggregate_id = '019fe3bf-6881-7301-b5d0-58ce355457a6'
      AND event_type = 'message.created'
    ORDER BY id";

fn parameters() -> radixdb::NamedParams {
    named_params! {
        user_id: Value::uuid(USER_A),
        aggregate_id: Value::uuid(AGGREGATE_A),
        event_type: "message.created",
    }
}

fn ids(db: &Database) -> Vec<i64> {
    db.query_named(LOOKUP, parameters())
        .expect("composite lookup")
        .map(|row| row.expect("result row").get::<i64>(0).expect("id"))
        .collect()
}

fn plan(db: &Database) -> String {
    db.query_named(&format!("EXPLAIN {LOOKUP}"), parameters())
        .expect("EXPLAIN composite lookup")
        .map(|row| row.expect("plan row").get::<String>(0).expect("plan text"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_index_plan(db: &Database) {
    let plan = plan(db);
    assert!(
        plan.contains("Access Path: scan.composite_index")
            && plan.contains("sync_events_user_aggregate_idx")
            && plan.contains("Cold Access Path: volume.composite_exact_index")
            && !plan.contains("Access Path: scan.cold_artifact"),
        "cold composite equality lookup must expose its real postings path:\n{plan}"
    );
}

fn assert_literal_lookup_matches_bound_lookup(db: &Database) {
    let literal_ids = db
        .query(LITERAL_LOOKUP, ())
        .expect("literal composite lookup")
        .map(|row| row.expect("literal result row").get::<i64>(0).expect("id"))
        .collect::<Vec<_>>();
    assert_eq!(literal_ids, ids(db));

    let literal_plan = db
        .query(&format!("EXPLAIN {LITERAL_LOOKUP}"), ())
        .expect("EXPLAIN literal composite lookup")
        .map(|row| {
            row.expect("literal plan row")
                .get::<String>(0)
                .expect("plan text")
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        literal_plan.contains("Access Path: scan.composite_index")
            && literal_plan.contains("sync_events_user_aggregate_idx")
            && literal_plan.contains("Cold Access Path: volume.composite_exact_index"),
        "literal UUID lookup must use the same persisted composite path:\n{literal_plan}"
    );
}

fn index_artifacts(root: &Path) -> Vec<PathBuf> {
    fn visit(dir: &Path, paths: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                visit(&path, paths);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".idx"))
            {
                paths.push(path);
            }
        }
    }

    let mut paths = Vec::new();
    visit(root, &mut paths);
    paths.sort();
    paths
}

fn insert(db: &Database, id: i64, user_id: [u8; 16], aggregate_id: [u8; 16], event_type: &str) {
    db.execute(
        "INSERT INTO sync_events
         (id, user_id, aggregate_id, event_type)
         VALUES (?, ?, ?, ?)",
        vec![
            Value::integer(id),
            Value::uuid(user_id),
            Value::uuid(aggregate_id),
            Value::text(event_type),
        ],
    )
    .expect("insert event");
}

const OUTBOX_LOOKUP: &str = "SELECT id, user_id, cursor, event_type, aggregate_type,
        aggregate_id, occurred_at
    FROM outbox_events
    WHERE outbox_job_id = :outbox_job_id
    ORDER BY user_id, cursor";

fn outbox_params(outbox_job_id: [u8; 16]) -> radixdb::NamedParams {
    named_params! { outbox_job_id: Value::uuid(outbox_job_id) }
}

fn user_uuid(tail: u8) -> [u8; 16] {
    let mut value = USER_A;
    value[15] = tail;
    value
}

fn insert_outbox_event(
    db: &Database,
    id: i64,
    outbox_job_id: [u8; 16],
    user_id: [u8; 16],
    cursor: i64,
) {
    db.execute(
        "INSERT INTO outbox_events
         (id, outbox_job_id, user_id, cursor, event_type, aggregate_type,
          aggregate_id, occurred_at)
         VALUES (?, ?, ?, ?, 'message.created', 'message', ?, 1000)",
        vec![
            Value::integer(id),
            Value::uuid(outbox_job_id),
            Value::uuid(user_id),
            Value::integer(cursor),
            Value::uuid(AGGREGATE_A),
        ],
    )
    .expect("insert outbox event");
}

fn outbox_ids(db: &Database, outbox_job_id: [u8; 16]) -> Vec<i64> {
    db.query_named(OUTBOX_LOOKUP, outbox_params(outbox_job_id))
        .expect("outbox lookup")
        .map(|row| row.expect("outbox row").get::<i64>(0).expect("id"))
        .collect()
}

fn outbox_plan(db: &Database, outbox_job_id: [u8; 16]) -> String {
    db.query_named(
        &format!("EXPLAIN {OUTBOX_LOOKUP}"),
        outbox_params(outbox_job_id),
    )
    .expect("EXPLAIN outbox lookup")
    .map(|row| row.expect("plan row").get::<String>(0).expect("plan text"))
    .collect::<Vec<_>>()
    .join("\n")
}

fn assert_outbox_index_plan(db: &Database, outbox_job_id: [u8; 16], cold: bool) {
    let plan = outbox_plan(db, outbox_job_id);
    assert!(
        plan.contains("Access Path: scan.index")
            && plan.contains("fk_outbox_events_outbox_job_id")
            && !plan.contains("Access Path: scan.cold_artifact"),
        "UUID equality must use the matching single-column index:\n{plan}"
    );
    if cold {
        assert!(
            plan.contains("Cold Access Path: volume.exact_index"),
            "cold UUID equality must expose persisted exact postings:\n{plan}"
        );
    }
}

#[test]
fn cold_composite_uuid_lookup_survives_mixed_reopen_wal_and_snapshot_restore() {
    let temp = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        temp.path().join("rdb0011").display()
    );

    {
        let db = Database::open(&dsn).expect("open database");
        db.execute(
            "CREATE TABLE sync_events (
                id INTEGER PRIMARY KEY,
                user_id UUID NOT NULL,
                aggregate_id UUID NOT NULL,
                event_type TEXT NOT NULL
             )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE INDEX sync_events_user_aggregate_idx
             ON sync_events (user_id, aggregate_id, event_type)",
            (),
        )
        .unwrap();

        insert(&db, 1, USER_A, AGGREGATE_A, "message.created");
        insert(&db, 2, USER_A, AGGREGATE_B, "message.created");
        insert(&db, 3, USER_B, AGGREGATE_A, "message.created");
        insert(&db, 4, USER_A, AGGREGATE_A, "message.deleted");
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        assert_eq!(ids(&db), vec![1]);
        assert_index_plan(&db);
        assert_literal_lookup_matches_bound_lookup(&db);

        // Same non-unique key in hot storage: mixed execution must merge both
        // physical sources without duplicating or losing either row.
        insert(&db, 5, USER_A, AGGREGATE_A, "message.created");
        assert_eq!(ids(&db), vec![1, 5]);
        assert_index_plan(&db);
        assert_literal_lookup_matches_bound_lookup(&db);
        db.close().expect("close without checkpoint");
    }

    {
        let db = Database::open(&dsn).expect("reopen and replay WAL");
        assert_eq!(ids(&db), vec![1, 5]);
        assert_index_plan(&db);

        db.execute("PRAGMA SNAPSHOT", ()).expect("create snapshot");
        insert(&db, 6, USER_A, AGGREGATE_A, "message.created");
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(ids(&db), vec![1, 5, 6]);
        assert_index_plan(&db);

        db.execute("PRAGMA RESTORE", ()).expect("restore snapshot");
        assert_eq!(ids(&db), vec![1, 5]);
        assert_index_plan(&db);
        db.close().expect("close restored database");
    }
}

#[test]
fn cold_uuid_equality_uses_single_and_composite_prefix_indexes_across_lifecycle() {
    let temp = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off&compact_threshold=2",
        temp.path().join("rdb0013").display()
    );

    {
        let db = Database::open(&dsn).expect("open database");
        db.execute(
            "CREATE TABLE outbox_events (
                id INTEGER PRIMARY KEY,
                outbox_job_id UUID NOT NULL,
                user_id UUID NOT NULL,
                cursor INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                aggregate_id UUID NOT NULL,
                occurred_at TIMESTAMP NOT NULL
             )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE INDEX fk_outbox_events_outbox_job_id
             ON outbox_events (outbox_job_id)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE UNIQUE INDEX outbox_events_outbox_user_uidx
             ON outbox_events (outbox_job_id, user_id)",
            (),
        )
        .unwrap();

        insert_outbox_event(&db, 1, OUTBOX_A, user_uuid(0x10), 30);
        insert_outbox_event(&db, 2, OUTBOX_A, user_uuid(0x20), 20);
        insert_outbox_event(&db, 3, OUTBOX_A, user_uuid(0x30), 10);
        insert_outbox_event(&db, 4, OUTBOX_B, user_uuid(0x40), 40);
        assert_eq!(outbox_ids(&db, OUTBOX_A), vec![1, 2, 3]);
        assert_outbox_index_plan(&db, OUTBOX_A, false);

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(outbox_ids(&db, OUTBOX_A), vec![1, 2, 3]);
        assert_outbox_index_plan(&db, OUTBOX_A, true);
        assert!(outbox_ids(&db, user_uuid(0xff)).is_empty());
        assert_outbox_index_plan(&db, user_uuid(0xff), true);

        // Mixed storage must use both persisted cold postings and the live hot
        // secondary index without scanning either complete source.
        insert_outbox_event(&db, 5, OUTBOX_A, user_uuid(0x50), 50);
        insert_outbox_event(&db, 6, OUTBOX_B, user_uuid(0x60), 60);
        assert_eq!(outbox_ids(&db, OUTBOX_A), vec![1, 2, 3, 5]);
        assert_outbox_index_plan(&db, OUTBOX_A, true);

        let literal = db
            .query(
                "SELECT id FROM outbox_events
                 WHERE outbox_job_id = '019fe3bf-6882-7301-b5d0-58ce355457a6'
                 ORDER BY user_id, cursor",
                (),
            )
            .unwrap()
            .map(|row| row.unwrap().get::<i64>(0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(literal, vec![1, 2, 3, 5]);
        let literal_plan = db
            .query(
                "EXPLAIN SELECT id FROM outbox_events
                 WHERE outbox_job_id = '019fe3bf-6882-7301-b5d0-58ce355457a6'
                 ORDER BY user_id, cursor",
                (),
            )
            .unwrap()
            .map(|row| row.unwrap().get::<String>(0).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            literal_plan.contains("Access Path: scan.index")
                && literal_plan.contains("Cold Access Path: volume.exact_index"),
            "UUID literal must use the same cold exact path as a bound UUID:\n{literal_plan}"
        );
        db.close().expect("leave a hot WAL tail");
    }

    {
        let db = Database::open(&dsn).expect("reopen and replay mixed state");
        assert_eq!(outbox_ids(&db, OUTBOX_A), vec![1, 2, 3, 5]);
        assert_outbox_index_plan(&db, OUTBOX_A, true);

        db.execute("PRAGMA SNAPSHOT", ()).unwrap();
        insert_outbox_event(&db, 7, OUTBOX_A, user_uuid(0x70), 70);
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(outbox_ids(&db, OUTBOX_A), vec![1, 2, 3, 5, 7]);
        assert_outbox_index_plan(&db, OUTBOX_A, true);

        db.execute("PRAGMA RESTORE", ()).unwrap();
        assert_eq!(outbox_ids(&db, OUTBOX_A), vec![1, 2, 3, 5]);
        assert_outbox_index_plan(&db, OUTBOX_A, true);

        // Dropping the single-column declaration leaves the leading prefix of
        // the composite index as the only eligible physical contract.
        db.execute(
            "DROP INDEX fk_outbox_events_outbox_job_id ON outbox_events",
            (),
        )
        .unwrap();
        let composite_plan = outbox_plan(&db, OUTBOX_A);
        assert!(
            composite_plan.contains("Access Path: scan.composite_index")
                && composite_plan.contains("outbox_events_outbox_user_uidx")
                && composite_plan.contains("Cold Access Path: volume.composite_exact_index"),
            "leading UUID prefix must remain bounded through the composite index:\n{composite_plan}"
        );
        assert_eq!(outbox_ids(&db, OUTBOX_A), vec![1, 2, 3, 5]);
        db.close().unwrap();
    }
}

#[test]
fn cold_uuid_primary_key_equality_keeps_its_generated_index() {
    let temp = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&checkpoint_on_close=off",
        temp.path().join("uuid_pk").display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE uuid_entities (
                id UUID PRIMARY KEY,
                payload TEXT NOT NULL
             )",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO uuid_entities (id, payload) VALUES (?, 'target')",
            vec![Value::uuid(OUTBOX_A)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO uuid_entities (id, payload) VALUES (?, 'other')",
            vec![Value::uuid(OUTBOX_B)],
        )
        .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        let rows = db
            .query_named(
                "SELECT payload FROM uuid_entities WHERE id = :id",
                named_params! { id: Value::uuid(OUTBOX_A) },
            )
            .unwrap()
            .map(|row| row.unwrap().get::<String>(0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec!["target"]);
        let plan = db
            .query_named(
                "EXPLAIN SELECT payload FROM uuid_entities WHERE id = :id",
                named_params! { id: Value::uuid(OUTBOX_A) },
            )
            .unwrap()
            .map(|row| row.unwrap().get::<String>(0).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("Access Path: scan.index")
                && plan.contains("Cold Access Path: volume.exact_index")
                && !plan.contains("Access Path: scan.cold_artifact"),
            "UUID primary key must retain a bounded cold lookup:\n{plan}"
        );
        db.close().unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    let count = db
        .query_named(
            "SELECT COUNT(*) FROM uuid_entities WHERE id = :id",
            named_params! { id: Value::uuid(OUTBOX_A) },
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get::<i64>(0)
        .unwrap();
    assert_eq!(count, 1);
    let plan = db
        .query_named(
            "EXPLAIN SELECT payload FROM uuid_entities WHERE id = :id",
            named_params! { id: Value::uuid(OUTBOX_A) },
        )
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("Cold Access Path: volume.exact_index"));
}

#[test]
fn cold_composite_index_claims_supported_integer_range_and_exact_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let db = Database::open(&format!(
        "file://{}?checkpoint_interval=3600&sync_mode=off",
        temp.path().display()
    ))
    .unwrap();
    db.execute(
        "CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            kind TEXT NOT NULL
         )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX items_lookup_idx ON items (tenant_id, sequence, kind)",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1, 7, 10, 'event')", ())
        .unwrap();
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();

    let prefix_plan = db
        .query("EXPLAIN SELECT id FROM items WHERE tenant_id = 7", ())
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        prefix_plan.contains("Access Path: scan.composite_index")
            && prefix_plan.contains("items_lookup_idx")
            && prefix_plan.contains("Cold Access Path: volume.composite_exact_index")
            && !prefix_plan.contains("Access Path: scan.cold_artifact"),
        "persisted leading equality prefix must use its exact postings:\n{prefix_plan}"
    );

    let range_plan = db
        .query(
            "EXPLAIN SELECT id FROM items
             WHERE tenant_id = 7 AND sequence >= 10
             ORDER BY sequence LIMIT 10",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        range_plan.contains("Access Path: scan.composite_index")
            && range_plan.contains("Cold Access Path: volume.composite_ordered_index"),
        "supported integer range must expose the persisted ordered path:\n{range_plan}"
    );
}

#[test]
fn composite_uuid_integer_range_is_ordered_and_bounded_across_hot_cold_and_reopen() {
    const PAGE: &str = "SELECT cursor FROM sync_cursor_events
        WHERE user_id = :user_id AND cursor > :after_cursor
        ORDER BY cursor LIMIT 10";

    let temp = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off&compact_threshold=2",
        temp.path().join("rdb0012").display()
    );
    let page_params = |after_cursor| {
        named_params! {
            user_id: Value::uuid(USER_A),
            after_cursor: Value::integer(after_cursor),
        }
    };
    let read_page = |db: &Database, after_cursor| {
        db.query_named(PAGE, page_params(after_cursor))
            .unwrap()
            .map(|row| row.unwrap().get::<i64>(0).unwrap())
            .collect::<Vec<_>>()
    };
    let assert_ordered_plan = |db: &Database, after_cursor| {
        let plan = db
            .query_named(&format!("EXPLAIN {PAGE}"), page_params(after_cursor))
            .unwrap()
            .map(|row| row.unwrap().get::<String>(0).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("Access Path: scan.composite_index")
                && plan.contains("sync_cursor_events_user_cursor_idx"),
            "composite range plan must be explicit:\n{plan}"
        );
        plan
    };

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE sync_cursor_events (
                id INTEGER PRIMARY KEY,
                user_id UUID NOT NULL,
                cursor INTEGER NOT NULL,
                payload TEXT NOT NULL
             )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE UNIQUE INDEX sync_cursor_events_user_cursor_idx
             ON sync_cursor_events (user_id, cursor)",
            (),
        )
        .unwrap();
        for cursor in 1..=120 {
            db.execute(
                "INSERT INTO sync_cursor_events VALUES (?, ?, ?, ?)",
                vec![
                    Value::integer(cursor),
                    Value::uuid(USER_A),
                    Value::integer(cursor),
                    Value::text(format!("event-{cursor}")),
                ],
            )
            .unwrap();
        }

        // Pure hot path uses the declared composite B-tree.
        assert_eq!(read_page(&db, 50), (51..=60).collect::<Vec<_>>());
        assert_ordered_plan(&db, 50);

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(read_page(&db, 50), (51..=60).collect::<Vec<_>>());
        let cold_plan = assert_ordered_plan(&db, 50);
        assert!(
            cold_plan.contains("Cold Access Path: volume.composite_ordered_index"),
            "checkpointed rows must use persisted ordered postings:\n{cold_plan}"
        );

        for cursor in 121..=125 {
            db.execute(
                "INSERT INTO sync_cursor_events VALUES (?, ?, ?, ?)",
                vec![
                    Value::integer(cursor),
                    Value::uuid(USER_A),
                    Value::integer(cursor),
                    Value::text(format!("hot-{cursor}")),
                ],
            )
            .unwrap();
        }
        assert_eq!(read_page(&db, 116), (117..=125).collect::<Vec<_>>());

        let bounded = db
            .query(
                "SELECT cursor FROM sync_cursor_events
                 WHERE user_id = '019fe3bf-6880-7301-b5d0-58ce355457a6'
                   AND cursor >= 51 AND cursor <= 55
                 ORDER BY cursor LIMIT 20",
                (),
            )
            .unwrap()
            .map(|row| row.unwrap().get::<i64>(0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(bounded, vec![51, 52, 53, 54, 55]);

        let descending = db
            .query(
                "SELECT cursor FROM sync_cursor_events
                 WHERE user_id = '019fe3bf-6880-7301-b5d0-58ce355457a6'
                   AND cursor < 6
                 ORDER BY cursor DESC LIMIT 3",
                (),
            )
            .unwrap()
            .map(|row| row.unwrap().get::<i64>(0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(descending, vec![5, 4, 3]);
        assert!(read_page(&db, 500).is_empty());
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).expect("reopen and replay hot WAL tail");
        assert_eq!(read_page(&db, 116), (117..=125).collect::<Vec<_>>());
        let plan = assert_ordered_plan(&db, 116);
        assert!(plan.contains("Cold Access Path: volume.composite_ordered_index"));
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(read_page(&db, 116), (117..=125).collect::<Vec<_>>());

        db.execute("PRAGMA SNAPSHOT", ()).unwrap();
        for cursor in 126..=130 {
            db.execute(
                "INSERT INTO sync_cursor_events VALUES (?, ?, ?, ?)",
                vec![
                    Value::integer(cursor),
                    Value::uuid(USER_A),
                    Value::integer(cursor),
                    Value::text(format!("compaction-{cursor}")),
                ],
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(read_page(&db, 124), (125..=130).collect::<Vec<_>>());
        assert_ordered_plan(&db, 124);

        db.execute("PRAGMA RESTORE", ()).unwrap();
        assert_eq!(read_page(&db, 116), (117..=125).collect::<Vec<_>>());
        assert_ordered_plan(&db, 116);
        db.close().unwrap();
    }
}

#[test]
fn persisted_index_cache_eviction_reopen_rename_and_truncate_preserve_results() {
    const RANGE_QUERY: &str = "SELECT sequence FROM posting_rebuild_items
        WHERE tenant_id = 7 AND sequence >= 4090 AND sequence <= 4100
        ORDER BY sequence";

    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("posting-rebuild");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        database_path.display()
    );
    let assert_range = |db: &Database| {
        let values = db
            .query(RANGE_QUERY, ())
            .unwrap()
            .map(|row| row.unwrap().get::<i64>(0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values, (4090..=4100).collect::<Vec<_>>());
        let plan = db
            .query(&format!("EXPLAIN {RANGE_QUERY}"), ())
            .unwrap()
            .map(|row| row.unwrap().get::<String>(0).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("Cold Access Path: volume.composite_ordered_index"),
            "recovered lookup must keep the persisted ordered path:\n{plan}"
        );
    };

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE posting_rebuild_items (
                id INTEGER PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                payload TEXT NOT NULL
             )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE INDEX posting_rebuild_items_lookup_idx
             ON posting_rebuild_items (tenant_id, sequence)",
            (),
        )
        .unwrap();
        db.execute("BEGIN", ()).unwrap();
        for sequence in 1..=4_200 {
            db.execute(
                "INSERT INTO posting_rebuild_items VALUES (?, 7, ?, 'payload')",
                vec![Value::integer(sequence), Value::integer(sequence)],
            )
            .unwrap();
        }
        db.execute("COMMIT", ()).unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_range(&db);

        // Alternate between distant pages with a deliberately tiny shared
        // cache. Correctness must not depend on a posting page remaining
        // resident after the next lookup.
        for (min, max) in [(1, 3), (4_090, 4_100), (2_000, 2_010), (4_190, 4_200)] {
            let query = format!(
                "SELECT sequence FROM posting_rebuild_items
                 WHERE tenant_id = 7 AND sequence >= {min} AND sequence <= {max}
                 ORDER BY sequence"
            );
            let values = db
                .query(&query, ())
                .unwrap()
                .map(|row| row.unwrap().get::<i64>(0).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(values, (min..=max).collect::<Vec<_>>());
        }
        db.close().unwrap();
    }

    let artifacts = index_artifacts(&database_path);
    assert!(
        !artifacts.is_empty(),
        "checkpoint must publish canonical V6 INDEX artifacts"
    );

    {
        let db = Database::open(&dsn).expect("reopen canonical V6 INDEX artifacts");
        assert_range(&db);

        db.execute(
            "ALTER TABLE posting_rebuild_items RENAME TO posting_rebuild_renamed",
            (),
        )
        .unwrap();
        let renamed = db
            .query(
                "SELECT sequence FROM posting_rebuild_renamed
                 WHERE tenant_id = 7 AND sequence >= 4090 AND sequence <= 4100
                 ORDER BY sequence",
                (),
            )
            .unwrap()
            .map(|row| row.unwrap().get::<i64>(0).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(renamed, (4090..=4100).collect::<Vec<_>>());
        assert_eq!(
            index_artifacts(&database_path),
            artifacts,
            "catalog-only rename must keep the immutable INDEX artifacts"
        );

        db.execute("TRUNCATE TABLE posting_rebuild_renamed", ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        let remaining = db
            .query("SELECT COUNT(*) FROM posting_rebuild_renamed", ())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap();
        assert_eq!(remaining, 0);
        db.close().unwrap();
    }

    // Missing/corrupt INDEX fallback and immutable replacement publication
    // belong to the index-rebuild lifecycle gates. Do not recreate retired
    // behavior by deleting a manifest-referenced sidecar during ordinary open.
    // TRUNCATE retires the INDEX reference immediately; physical bytes may
    // remain until reachability retention and bounded GC release them.
    let db = Database::open(&dsn).expect("reopen truncated artifact table");
    let remaining = db
        .query("SELECT COUNT(*) FROM posting_rebuild_renamed", ())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get::<i64>(0)
        .unwrap();
    assert_eq!(remaining, 0);
    db.close().unwrap();
}

fn tcp_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: Ipv4Addr::LOCALHOST.into(),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 4,
        max_inflight_frame_bytes: radixdb::server::default_max_inflight_frame_bytes(),
        max_databases: radixdb::server::default_max_databases(),
        max_database_name_bytes: radixdb::server::default_max_database_name_bytes(),
        connect_timeout_secs: 5,
        connection_idle_timeout_secs: 30,
        net_read_timeout_secs: 30,
        net_write_timeout_secs: 30,
        cursor_batch_max_rows: 128,
        cursor_batch_max_bytes: 1024 * 1024,
        max_frame_bytes: 4 * 1024 * 1024,
        copy_max_transaction_bytes: radixdb::server::default_copy_max_transaction_bytes(),
        max_compaction_jobs: radixdb::server::default_max_compaction_jobs(),
        storage_cpu_workers: radixdb::server::default_storage_cpu_workers(),
        page_cache_level: radixdb::server::default_page_cache_level(),
        page_cache_max_bytes: radixdb::server::default_page_cache_max_bytes(),
        page_cache_memory_reserve: radixdb::server::default_page_cache_memory_reserve(),
        target_volume_rows: default_target_volume_rows(),
        seal_hot_bytes_threshold: default_seal_hot_bytes_threshold(),
        seal_incremental_hot_bytes_threshold: default_seal_incremental_hot_bytes_threshold(),
        read_queue_depth: 1,
    }
}

fn tcp_command(client: &mut Connection, sql: &str) {
    let result = client
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    if let ExecuteResult::Cursor(cursor) = result {
        while !client.fetch(&cursor).unwrap().eof {}
    }
}

fn tcp_lookup_params() -> BTreeMap<String, WireValue> {
    BTreeMap::from([
        ("user_id".to_string(), WireValue::Uuid(USER_A)),
        ("aggregate_id".to_string(), WireValue::Uuid(AGGREGATE_A)),
        (
            "event_type".to_string(),
            WireValue::String("message.created".to_string()),
        ),
    ])
}

#[test]
fn tcp_bound_uuid_composite_lookup_uses_cold_postings() {
    let temp = tempfile::tempdir().unwrap();
    let server = Server::bind_ephemeral(&tcp_config(temp.path().join("tcp"))).unwrap();
    let address = server.local_addr().unwrap();

    std::thread::scope(|scope| {
        let worker = scope.spawn(|| server.serve_one());
        let mut client = Connection::connect(address).unwrap();
        client.authenticate("root", None).unwrap();
        client.select_database("rdb0011_tcp").unwrap();

        tcp_command(
            &mut client,
            "CREATE TABLE sync_events (
                id INTEGER PRIMARY KEY,
                user_id UUID NOT NULL,
                aggregate_id UUID NOT NULL,
                event_type TEXT NOT NULL
             )",
        );
        tcp_command(
            &mut client,
            "CREATE INDEX sync_events_user_aggregate_idx
             ON sync_events (user_id, aggregate_id, event_type)",
        );

        let mut insert_params = tcp_lookup_params();
        insert_params.insert("id".to_string(), WireValue::Int(1));
        let insert = client
            .execute_with_parameters(
                "INSERT INTO sync_events
                 (id, user_id, aggregate_id, event_type)
                 VALUES (:id, :user_id, :aggregate_id, :event_type)",
                insert_params,
            )
            .unwrap();
        assert!(matches!(insert, ExecuteResult::CommandComplete { .. }));
        tcp_command(&mut client, "PRAGMA CHECKPOINT");

        let query = client
            .execute_with_parameters(LOOKUP, tcp_lookup_params())
            .unwrap();
        let ExecuteResult::Cursor(cursor) = query else {
            panic!("lookup must return a cursor")
        };
        let batch = client.fetch(&cursor).unwrap();
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].values, [WireValue::Int(1)]);

        let explain = client
            .execute_with_parameters(format!("EXPLAIN {LOOKUP}"), tcp_lookup_params())
            .unwrap();
        let ExecuteResult::Cursor(cursor) = explain else {
            panic!("EXPLAIN must return a cursor")
        };
        let mut lines = Vec::new();
        loop {
            let batch = client.fetch(&cursor).unwrap();
            for row in batch.rows {
                let Some(WireValue::String(line)) = row.values.first() else {
                    panic!("EXPLAIN row must contain text")
                };
                lines.push(line.clone());
            }
            if batch.eof {
                break;
            }
        }
        let plan = lines.join("\n");
        assert!(
            plan.contains("Access Path: scan.composite_index")
                && plan.contains("Cold Access Path: volume.composite_exact_index")
                && plan.contains("sync_events_user_aggregate_idx")
                && !plan.contains(":user_id"),
            "TCP bound values must select the executable cold path:\n{plan}"
        );

        client.shutdown().unwrap();
        worker.join().unwrap().unwrap();
    });
}
