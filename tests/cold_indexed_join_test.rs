// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! RDB-0017 regressions for indexed joins over immutable artifact-backed rows.

use radixdb::{named_params, Database, Value};

const TENANT_A: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x01, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa6,
];
const TENANT_B: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x02, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa7,
];
const TENANT_C: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x03, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa8,
];
const TENANT_MISS: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0xff, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xff,
];
const TENANT_A_TEXT: &str = "019fe3bf-6880-7301-b5d0-58ce355457a6";

const QUERY: &str = "SELECT child.ordinal
FROM contract_rows child
JOIN contract_parents parent ON parent.id = child.tenant_id
WHERE parent.id = :tenant_id AND child.ordinal > :after
ORDER BY child.ordinal LIMIT :limit";

const REVERSED_QUERY: &str = "SELECT child.ordinal
FROM contract_parents parent
JOIN contract_rows child ON child.tenant_id = parent.id
WHERE parent.id = :tenant_id AND child.ordinal > :after
ORDER BY child.ordinal LIMIT :limit";

fn params(after: i64, limit: i64) -> radixdb::NamedParams {
    named_params! {
        tenant_id: Value::uuid(TENANT_A),
        after: Value::integer(after),
        limit: Value::integer(limit),
    }
}

fn ordinals(db: &Database, after: i64, limit: i64) -> Vec<i64> {
    ordinals_for(db, QUERY, params(after, limit))
}

fn ordinals_for(db: &Database, query: &str, params: radixdb::NamedParams) -> Vec<i64> {
    db.query_named(query, params)
        .unwrap()
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect()
}

fn plan(db: &Database, after: i64, limit: i64) -> String {
    db.query_named(&format!("EXPLAIN {QUERY}"), params(after, limit))
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_indexed_join(db: &Database, cold: bool) {
    let plan = plan(db, 5, 4);
    assert!(
        plan.contains("Join Access Path: join.index_nested_loop.secondary")
            && plan.contains("Access Path: scan.index")
            && plan.contains("contract_rows_tenant_ordinal_uidx")
            && !plan.contains("Join Access Path: join.hash"),
        "selective UUID FK join must retain its indexed nested-loop path:\n{plan}"
    );
    if cold {
        assert!(
            plan.contains("Cold Access Path: volume.exact_index"),
            "cold parent UUID probe must expose persisted exact postings:\n{plan}"
        );
    }
}

fn assert_reversed_indexed_join(db: &Database, cold: bool) {
    let plan = db
        .query_named(&format!("EXPLAIN {REVERSED_QUERY}"), params(5, 4))
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("Join Access Path: join.index_nested_loop.secondary")
            && plan.contains("Join Lookup Index: fk_contract_rows_tenant_id")
            && !plan.contains("Join Access Path: join.hash"),
        "reversed selective join must probe the child index:\n{plan}"
    );
    if cold {
        assert!(
            plan.contains("Cold Access Path: volume.exact_index"),
            "reversed cold join must expose persisted exact postings:\n{plan}"
        );
    }
}

fn insert_child(db: &Database, id: i64, tenant: [u8; 16], ordinal: i64) {
    db.execute(
        "INSERT INTO contract_rows (id, tenant_id, ordinal) VALUES (?, ?, ?)",
        vec![
            Value::integer(id),
            Value::uuid(tenant),
            Value::integer(ordinal),
        ],
    )
    .unwrap();
}

#[test]
fn selective_fk_join_uses_persisted_uuid_and_composite_paths_across_lifecycle() {
    let temp = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off&compact_threshold=2",
        temp.path().join("rdb0017").display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE contract_parents (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL
             )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE contract_rows (
                id INTEGER PRIMARY KEY,
                tenant_id UUID NOT NULL REFERENCES contract_parents(id),
                ordinal INTEGER NOT NULL
             )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE UNIQUE INDEX contract_rows_tenant_ordinal_uidx
             ON contract_rows (tenant_id, ordinal)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO contract_parents VALUES (?, 'target'), (?, 'other'), (?, 'empty')",
            vec![
                Value::uuid(TENANT_A),
                Value::uuid(TENANT_B),
                Value::uuid(TENANT_C),
            ],
        )
        .unwrap();
        for ordinal in 1..=12 {
            insert_child(&db, ordinal, TENANT_A, ordinal);
            insert_child(&db, 100 + ordinal, TENANT_B, ordinal);
        }

        assert_eq!(ordinals(&db, 5, 4), vec![6, 7, 8, 9]);
        assert_eq!(
            ordinals_for(&db, REVERSED_QUERY, params(5, 4)),
            vec![6, 7, 8, 9]
        );
        assert!(ordinals_for(
            &db,
            QUERY,
            named_params! {
                tenant_id: Value::uuid(TENANT_C),
                after: Value::integer(0),
                limit: Value::integer(4),
            }
        )
        .is_empty());
        assert!(ordinals_for(
            &db,
            QUERY,
            named_params! {
                tenant_id: Value::uuid(TENANT_MISS),
                after: Value::integer(0),
                limit: Value::integer(4),
            }
        )
        .is_empty());
        let literal = format!(
            "SELECT child.ordinal FROM contract_rows child \
             JOIN contract_parents parent ON parent.id = child.tenant_id \
             WHERE parent.id = '{TENANT_A_TEXT}' AND child.ordinal > 5 \
             ORDER BY child.ordinal LIMIT 4"
        );
        assert_eq!(
            db.query(&literal, ())
                .unwrap()
                .map(|row| row.unwrap().get::<i64>(0).unwrap())
                .collect::<Vec<_>>(),
            vec![6, 7, 8, 9]
        );
        assert_indexed_join(&db, false);
        assert_reversed_indexed_join(&db, false);

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(ordinals(&db, 5, 4), vec![6, 7, 8, 9]);
        assert_indexed_join(&db, true);
        assert_reversed_indexed_join(&db, true);

        // A hot update shadows the old cold posting. Generic indexed joins
        // must recheck the authoritative key and never join the stale value.
        db.execute(
            "UPDATE contract_rows SET tenant_id = ?, ordinal = 60 WHERE id = 6",
            vec![Value::uuid(TENANT_B)],
        )
        .unwrap();
        assert_eq!(
            ordinals_for(&db, REVERSED_QUERY, params(5, 4)),
            vec![7, 8, 9, 10]
        );

        insert_child(&db, 13, TENANT_A, 13);
        insert_child(&db, 113, TENANT_B, 13);
        assert_eq!(ordinals(&db, 10, 4), vec![11, 12, 13]);
        assert_indexed_join(&db, true);
        db.close().unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    assert_eq!(ordinals(&db, 10, 4), vec![11, 12, 13]);
    assert_indexed_join(&db, true);
    db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    assert_eq!(ordinals(&db, 5, 4), vec![7, 8, 9, 10]);
    assert_indexed_join(&db, true);
    assert_reversed_indexed_join(&db, true);
}
