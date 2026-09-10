// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! RDB-0034: INNER JOIN must see rows inserted by the current transaction.

use radixdb::{ApiTransaction, Database, Value};

const USER_ID: [u8; 16] = [
    0x01, 0xa0, 0x55, 0x34, 0x10, 0x00, 0x70, 0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
];
const CALL_ID: [u8; 16] = [
    0x01, 0xa0, 0x55, 0x34, 0x10, 0x00, 0x70, 0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
];
const PARTICIPANT_ID: [u8; 16] = [
    0x01, 0xa0, 0x55, 0x34, 0x10, 0x00, 0x70, 0x03, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
];
const OTHER_USER_ID: [u8; 16] = [
    0x01, 0xa0, 0x55, 0x34, 0x10, 0x00, 0x70, 0x04, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
];
const OTHER_CALL_ID: [u8; 16] = [
    0x01, 0xa0, 0x55, 0x34, 0x10, 0x00, 0x70, 0x05, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
];
const OTHER_PARTICIPANT_ID: [u8; 16] = [
    0x01, 0xa0, 0x55, 0x34, 0x10, 0x00, 0x70, 0x06, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
];
const HOT_CALL_ID: [u8; 16] = [
    0x01, 0xa0, 0x55, 0x34, 0x10, 0x00, 0x70, 0x07, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
];
const HOT_PARTICIPANT_ID: [u8; 16] = [
    0x01, 0xa0, 0x55, 0x34, 0x10, 0x00, 0x70, 0x08, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
];

const FORWARD_JOIN: &str = "SELECT cs.kind
FROM call_sessions cs
INNER JOIN call_participants owner ON owner.call_id = cs.id
WHERE cs.id = ? AND owner.user_id = ?
LIMIT 2";

const REVERSE_JOIN: &str = "SELECT cs.kind
FROM call_participants owner
INNER JOIN call_sessions cs ON cs.id = owner.call_id
WHERE owner.call_id = ? AND owner.user_id = ?
LIMIT 2";

fn create_uuid_schema(db: &Database) {
    db.execute("CREATE TABLE users (id UUID PRIMARY KEY)", ())
        .unwrap();
    db.execute(
        "CREATE TABLE call_sessions (
            id UUID PRIMARY KEY,
            creator_user_id UUID NOT NULL REFERENCES users(id),
            kind TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE call_participants (
            id UUID PRIMARY KEY,
            call_id UUID NOT NULL REFERENCES call_sessions(id),
            user_id UUID NOT NULL REFERENCES users(id),
            participant_state TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE UNIQUE INDEX call_participants_call_user_uidx
         ON call_participants(call_id, user_id)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX call_participants_user_state_idx
         ON call_participants(user_id, participant_state, call_id)",
        (),
    )
    .unwrap();
}

fn transaction_join_count(
    transaction: &mut ApiTransaction,
    sql: &str,
    call_id: [u8; 16],
    user_id: [u8; 16],
) -> usize {
    let rows = transaction
        .query(sql, vec![Value::uuid(call_id), Value::uuid(user_id)])
        .unwrap();
    let mut count = 0;
    for row in rows {
        row.unwrap();
        count += 1;
    }
    count
}

fn database_join_count(db: &Database, sql: &str, call_id: [u8; 16], user_id: [u8; 16]) -> usize {
    let rows = db
        .query(sql, vec![Value::uuid(call_id), Value::uuid(user_id)])
        .unwrap();
    let mut count = 0;
    for row in rows {
        row.unwrap();
        count += 1;
    }
    count
}

#[test]
fn hot_inner_join_sees_both_same_transaction_inserts() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(&format!(
        "file://{}?checkpoint_on_close=off",
        directory.path().join("integer").display()
    ))
    .unwrap();
    db.execute(
        "CREATE TABLE call_sessions (
            id INTEGER PRIMARY KEY,
            creator_user_id INTEGER NOT NULL,
            kind TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE call_participants (
            id INTEGER PRIMARY KEY,
            call_id INTEGER NOT NULL REFERENCES call_sessions(id),
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE UNIQUE INDEX call_participants_call_user_uidx
         ON call_participants(call_id, user_id)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX call_participants_user_state_idx
         ON call_participants(user_id, role, call_id)",
        (),
    )
    .unwrap();

    let mut transaction = db.begin().unwrap();
    transaction
        .execute("INSERT INTO call_sessions VALUES (100, 7, 'group')", ())
        .unwrap();
    transaction
        .execute(
            "INSERT INTO call_participants VALUES (200, 100, 7, 'owner')",
            (),
        )
        .unwrap();

    assert_eq!(
        transaction
            .query("SELECT id FROM call_sessions WHERE id = 100", ())
            .unwrap()
            .count(),
        1,
        "left point read must see the current transaction insert"
    );
    assert_eq!(
        transaction
            .query("SELECT id FROM call_participants WHERE id = 200", ())
            .unwrap()
            .count(),
        1,
        "right point read must see the current transaction insert"
    );
    assert_eq!(
        transaction
            .query(
                "SELECT cs.id
                 FROM call_sessions cs
                 INNER JOIN call_participants owner ON owner.call_id = cs.id
                 WHERE cs.id = 100 AND owner.user_id = 7",
                (),
            )
            .unwrap()
            .count(),
        1,
        "INNER JOIN must see both current-transaction inserts"
    );
}

#[test]
fn indexed_uuid_inner_join_preserves_hot_cold_and_reopen_visibility() {
    let directory = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_on_close=off",
        directory.path().join("uuid").display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        create_uuid_schema(&db);
        db.execute(
            "INSERT INTO users VALUES (?), (?)",
            vec![Value::uuid(USER_ID), Value::uuid(OTHER_USER_ID)],
        )
        .unwrap();

        let mut transaction = db.begin().unwrap();
        transaction
            .execute(
                "INSERT INTO call_sessions VALUES (?, ?, 'ad_hoc'), (?, ?, 'group')",
                vec![
                    Value::uuid(CALL_ID),
                    Value::uuid(USER_ID),
                    Value::uuid(OTHER_CALL_ID),
                    Value::uuid(OTHER_USER_ID),
                ],
            )
            .unwrap();

        assert_eq!(
            transaction_join_count(&mut transaction, FORWARD_JOIN, CALL_ID, USER_ID),
            0,
            "a missing right row must not produce a phantom match"
        );

        transaction
            .execute(
                "INSERT INTO call_participants VALUES (?, ?, ?, 'joined')",
                vec![
                    Value::uuid(OTHER_PARTICIPANT_ID),
                    Value::uuid(OTHER_CALL_ID),
                    Value::uuid(OTHER_USER_ID),
                ],
            )
            .unwrap();
        assert_eq!(
            transaction_join_count(&mut transaction, FORWARD_JOIN, CALL_ID, USER_ID),
            0,
            "an unrelated transaction-local index candidate must be rechecked"
        );

        transaction
            .execute(
                "INSERT INTO call_participants VALUES (?, ?, ?, 'invited')",
                vec![
                    Value::uuid(PARTICIPANT_ID),
                    Value::uuid(CALL_ID),
                    Value::uuid(USER_ID),
                ],
            )
            .unwrap();
        assert_eq!(
            transaction_join_count(&mut transaction, FORWARD_JOIN, CALL_ID, USER_ID),
            1,
            "the indexed UUID INNER JOIN must see the current call aggregate"
        );
        assert_eq!(
            transaction_join_count(&mut transaction, REVERSE_JOIN, CALL_ID, USER_ID),
            1,
            "read-your-writes must not depend on which JOIN side owns the UUID primary key"
        );

        transaction
            .execute(
                "UPDATE call_participants SET call_id = ? WHERE id = ?",
                vec![Value::uuid(OTHER_CALL_ID), Value::uuid(PARTICIPANT_ID)],
            )
            .unwrap();
        assert_eq!(
            transaction_join_count(&mut transaction, FORWARD_JOIN, CALL_ID, USER_ID),
            0,
            "the old unpublished index key must stop matching after UPDATE"
        );
        assert_eq!(
            transaction_join_count(&mut transaction, FORWARD_JOIN, OTHER_CALL_ID, USER_ID),
            1,
            "the new unpublished index key must match after UPDATE"
        );

        transaction
            .execute(
                "UPDATE call_participants SET call_id = ? WHERE id = ?",
                vec![Value::uuid(CALL_ID), Value::uuid(PARTICIPANT_ID)],
            )
            .unwrap();
        assert_eq!(
            transaction_join_count(&mut transaction, FORWARD_JOIN, CALL_ID, USER_ID),
            1
        );
        transaction.commit().unwrap();

        let second_session = Database::open(&dsn).unwrap();
        assert_eq!(
            database_join_count(&second_session, FORWARD_JOIN, CALL_ID, USER_ID),
            1,
            "a new session must see the committed aggregate"
        );
        drop(second_session);

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_eq!(
            database_join_count(&db, REVERSE_JOIN, CALL_ID, USER_ID),
            1,
            "checkpoint must preserve both JOIN orders"
        );
    }

    {
        let reopened = Database::open(&dsn).unwrap();
        assert_eq!(
            database_join_count(&reopened, FORWARD_JOIN, CALL_ID, USER_ID),
            1,
            "cold reopen must preserve the committed aggregate"
        );

        let mut mixed = reopened.begin().unwrap();
        mixed
            .execute(
                "INSERT INTO call_sessions VALUES (?, ?, 'broadcast')",
                vec![Value::uuid(HOT_CALL_ID), Value::uuid(USER_ID)],
            )
            .unwrap();
        mixed
            .execute(
                "INSERT INTO call_participants VALUES (?, ?, ?, 'invited')",
                vec![
                    Value::uuid(HOT_PARTICIPANT_ID),
                    Value::uuid(HOT_CALL_ID),
                    Value::uuid(USER_ID),
                ],
            )
            .unwrap();
        assert_eq!(
            transaction_join_count(&mut mixed, FORWARD_JOIN, CALL_ID, USER_ID),
            1,
            "the cold aggregate must remain visible in a mixed transaction"
        );
        assert_eq!(
            transaction_join_count(&mut mixed, FORWARD_JOIN, HOT_CALL_ID, USER_ID),
            1,
            "the hot aggregate must be visible beside cold rows"
        );
        mixed.commit().unwrap();
    }

    let reopened = Database::open(&dsn).unwrap();
    assert_eq!(
        database_join_count(&reopened, REVERSE_JOIN, HOT_CALL_ID, USER_ID),
        1,
        "the mixed-state hot aggregate must survive commit and reopen"
    );
}
