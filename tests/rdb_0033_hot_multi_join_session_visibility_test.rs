// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! RDB-0033: a deferred hot multi-JOIN must retain one valid read view until
//! the statement has consumed every table participating in the operator tree.

use chrono::{TimeZone, Utc};
use radixdb::{named_params, Database, NamedParams, Value};

const USER_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x01, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa6,
];
const DEVICE_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x02, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa7,
];
const SESSION_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x03, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa8,
];
const EMAIL_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x04, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa9,
];
const OLD_USER_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x72, 0x01, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xb6,
];
const OLD_DEVICE_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x72, 0x02, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xb7,
];
const OLD_SESSION_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x72, 0x03, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xb8,
];

const ACTIVE_SESSION_QUERY: &str = "SELECT s.id
FROM sessions AS s
INNER JOIN users AS u ON u.id = s.user_id
INNER JOIN devices AS d ON d.id = s.device_id
WHERE s.id = :session_id
  AND s.user_id = :user_id
  AND s.device_id = :device_id
  AND s.revoked_at IS NULL
  AND s.expires_at > :now
  AND (s.absolute_expires_at IS NULL OR s.absolute_expires_at > :now)
  AND d.user_id = :user_id
  AND d.revoked_at IS NULL
  AND u.disabled_at IS NULL
  AND u.deleted_at IS NULL";

fn active_params() -> NamedParams {
    named_params! {
        session_id: Value::uuid(SESSION_ID),
        user_id: Value::uuid(USER_ID),
        device_id: Value::uuid(DEVICE_ID),
        now: Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 12, 0, 0).unwrap()),
    }
}

fn selected_count(db: &Database, sql: &str, params: NamedParams) -> usize {
    let rows = db.query_named(sql, params).unwrap();
    let mut count = 0;
    for row in rows {
        row.unwrap();
        count += 1;
    }
    count
}

fn assert_active_views(db: &Database) {
    assert_eq!(
        selected_count(
            db,
            "SELECT id FROM sessions WHERE id = :session_id",
            named_params! { session_id: Value::uuid(SESSION_ID) },
        ),
        1,
        "the authoritative session row must remain visible"
    );
    assert_eq!(
        selected_count(
            db,
            "SELECT s.id FROM sessions s
             INNER JOIN users u ON u.id = s.user_id
             WHERE s.id = :session_id AND s.user_id = :user_id
               AND u.disabled_at IS NULL AND u.deleted_at IS NULL",
            named_params! {
                session_id: Value::uuid(SESSION_ID),
                user_id: Value::uuid(USER_ID),
            },
        ),
        1,
        "the first deferred JOIN edge must remain visible"
    );
    assert_eq!(
        selected_count(db, ACTIVE_SESSION_QUERY, active_params()),
        1,
        "the complete hot access-session JOIN must remain visible"
    );
}

fn create_fixture(db: &Database) {
    for statement in [
        "CREATE TABLE users (
            id UUID PRIMARY KEY,
            username TEXT NOT NULL,
            normalized_username TEXT NOT NULL,
            display_name TEXT NOT NULL,
            profile_revision INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            disabled_at TIMESTAMP,
            deleted_at TIMESTAMP,
            account_kind TEXT NOT NULL DEFAULT 'human',
            avatar_attachment_id UUID
        )",
        "CREATE TABLE devices (
            id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(id),
            platform TEXT NOT NULL,
            display_name TEXT NOT NULL,
            app_version TEXT,
            created_at TIMESTAMP NOT NULL,
            last_seen_at TIMESTAMP NOT NULL,
            revoked_at TIMESTAMP,
            revision INTEGER NOT NULL DEFAULT 1
        )",
        "CREATE TABLE sessions (
            id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(id),
            device_id UUID NOT NULL REFERENCES devices(id),
            token_family_id UUID NOT NULL,
            refresh_token_hash TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            last_used_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            revoked_at TIMESTAMP,
            revision INTEGER NOT NULL DEFAULT 1,
            absolute_expires_at TIMESTAMP
        )",
        "CREATE INDEX sessions_user_device_idx
         ON sessions (user_id, device_id, revoked_at)",
        "CREATE TABLE email_addresses (
            id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES users(id),
            state TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL
        )",
    ] {
        db.execute(statement, ()).unwrap();
    }

    db.execute(
        "INSERT INTO users (
            id, username, normalized_username, display_name, created_at, updated_at
         ) VALUES (?, 'deleted-candidate', 'deleted-candidate', 'Deleted candidate', ?, ?)",
        vec![
            Value::uuid(OLD_USER_ID),
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 0).unwrap()),
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 0).unwrap()),
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO devices (
            id, user_id, platform, display_name, created_at, last_seen_at
         ) VALUES (?, ?, 'linux', 'Old device', ?, ?)",
        vec![
            Value::uuid(OLD_DEVICE_ID),
            Value::uuid(OLD_USER_ID),
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 0).unwrap()),
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 0).unwrap()),
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO sessions (
            id, user_id, device_id, token_family_id, refresh_token_hash,
            created_at, last_used_at, expires_at, absolute_expires_at
         ) VALUES (?, ?, ?, ?, 'old-refresh', ?, ?, ?, ?)",
        vec![
            Value::uuid(OLD_SESSION_ID),
            Value::uuid(OLD_USER_ID),
            Value::uuid(OLD_DEVICE_ID),
            Value::uuid(OLD_SESSION_ID),
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 0).unwrap()),
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 0).unwrap()),
            Value::timestamp(Utc.with_ymd_and_hms(2026, 9, 28, 12, 0, 0).unwrap()),
            Value::timestamp(Utc.with_ymd_and_hms(2026, 11, 27, 12, 0, 0).unwrap()),
        ],
    )
    .unwrap();

    db.execute("PRAGMA CHECKPOINT", ()).unwrap();

    db.execute("BEGIN", ()).unwrap();
    db.execute(
        "UPDATE users SET username = 'deleted', normalized_username = 'deleted',
            display_name = 'Deleted user', deleted_at = ? WHERE id = ?",
        vec![
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 1).unwrap()),
            Value::uuid(OLD_USER_ID),
        ],
    )
    .unwrap();
    db.execute(
        "UPDATE sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
        vec![
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 1).unwrap()),
            Value::uuid(OLD_USER_ID),
        ],
    )
    .unwrap();
    db.execute(
        "UPDATE devices SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
        vec![
            Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 10, 0, 1).unwrap()),
            Value::uuid(OLD_USER_ID),
        ],
    )
    .unwrap();
    db.execute("COMMIT", ()).unwrap();

    assert_eq!(
        selected_count(
            db,
            "SELECT * FROM users WHERE disabled_at IS NULL AND deleted_at IS NULL",
            named_params! {},
        ),
        0,
        "the pre-commit active-user scan primes the shared semantic cache"
    );

    let mut active = db.begin().unwrap();
    active
        .execute(
            "INSERT INTO users (
            id, username, normalized_username, display_name, created_at, updated_at
         ) VALUES (?, 'alice', 'alice', 'Alice', ?, ?)",
            vec![
                Value::uuid(USER_ID),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 11, 0, 0).unwrap()),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 11, 0, 0).unwrap()),
            ],
        )
        .unwrap();
    active
        .execute(
            "INSERT INTO devices (
            id, user_id, platform, display_name, created_at, last_seen_at
         ) VALUES (?, ?, 'linux', 'Alice device', ?, ?)",
            vec![
                Value::uuid(DEVICE_ID),
                Value::uuid(USER_ID),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 11, 0, 0).unwrap()),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 11, 0, 0).unwrap()),
            ],
        )
        .unwrap();
    active
        .execute(
            "INSERT INTO sessions (
            id, user_id, device_id, token_family_id, refresh_token_hash,
            created_at, last_used_at, expires_at, absolute_expires_at
         ) VALUES (?, ?, ?, ?, 'active-refresh', ?, ?, ?, ?)",
            vec![
                Value::uuid(SESSION_ID),
                Value::uuid(USER_ID),
                Value::uuid(DEVICE_ID),
                Value::uuid(SESSION_ID),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 11, 0, 0).unwrap()),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 29, 11, 0, 0).unwrap()),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 9, 28, 12, 0, 0).unwrap()),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 11, 27, 12, 0, 0).unwrap()),
            ],
        )
        .unwrap();
    active.commit().unwrap();
}

#[test]
fn unrelated_hot_commit_cannot_hide_a_parameterized_multi_join() {
    let directory = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        directory.path().join("rdb0033").display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        create_fixture(&db);
        assert_active_views(&db);

        db.execute(
            "INSERT INTO email_addresses VALUES (?, ?, 'pending', ?)",
            vec![
                Value::uuid(EMAIL_ID),
                Value::uuid(USER_ID),
                Value::timestamp(Utc.with_ymd_and_hms(2026, 8, 30, 12, 0, 0).unwrap()),
            ],
        )
        .unwrap();

        for _ in 0..32 {
            assert_active_views(&db);
        }
    }

    let reopened = Database::open(&dsn).unwrap();
    assert_active_views(&reopened);
}

#[test]
#[ignore]
fn diagnose_live_consumer_cut() {
    use std::collections::BTreeMap;

    use radixdb_client::{Connection, ExecuteResult};

    let mut client = Connection::connect("127.0.0.1:25444").unwrap();
    client.authenticate("root", None).unwrap();
    client.select_database("rdb33_current").unwrap();
    for sql in [
        "SELECT id, disabled_at, deleted_at FROM users WHERE normalized_username = 'accessprobe'",
        "SELECT id FROM users u WHERE u.disabled_at IS NULL",
        "SELECT id FROM users u WHERE u.deleted_at IS NULL",
        "SELECT id FROM users u WHERE u.disabled_at IS NULL AND u.deleted_at IS NULL",
        "SELECT id FROM users u WHERE u.normalized_username = 'accessprobe' AND u.deleted_at IS NULL",
        "SELECT s.id, s.user_id, s.device_id, s.revoked_at FROM sessions s
         WHERE s.user_id IN (SELECT id FROM users WHERE normalized_username = 'accessprobe')",
        "SELECT d.id, d.user_id, d.revoked_at FROM devices d
         WHERE d.user_id IN (SELECT id FROM users WHERE normalized_username = 'accessprobe')",
        "SELECT s.id FROM sessions s
         INNER JOIN users u ON u.id = s.user_id
         WHERE u.normalized_username = 'accessprobe'",
        "SELECT s.id FROM sessions s
         INNER JOIN users u ON u.id = s.user_id
         INNER JOIN devices d ON d.id = s.device_id
         WHERE u.normalized_username = 'accessprobe'",
    ] {
        let ExecuteResult::Cursor(cursor) = client.execute(sql).unwrap() else {
            panic!("diagnostic query did not return a cursor")
        };
        let mut rows = Vec::new();
        loop {
            let batch = client.fetch(&cursor).unwrap();
            rows.extend(batch.rows);
            if batch.eof {
                break;
            }
        }
        eprintln!("{sql}\n{rows:#?}");
    }

    let parameters = BTreeMap::from([
        (
            "session_id".to_owned(),
            radixdb_client::WireValue::Uuid([
                1, 160, 76, 89, 126, 155, 117, 1, 143, 133, 205, 237, 206, 2, 190, 125,
            ]),
        ),
        (
            "user_id".to_owned(),
            radixdb_client::WireValue::Uuid([
                1, 160, 76, 89, 126, 155, 117, 1, 143, 133, 205, 205, 75, 34, 183, 77,
            ]),
        ),
        (
            "device_id".to_owned(),
            radixdb_client::WireValue::Uuid([
                1, 160, 76, 89, 126, 155, 117, 1, 143, 133, 205, 214, 53, 192, 195, 225,
            ]),
        ),
        (
            "now".to_owned(),
            radixdb_client::WireValue::DateTime {
                millis_since_unix_epoch_utc: 1_786_230_700_000,
            },
        ),
    ]);
    let ExecuteResult::Cursor(explain_cursor) = client
        .execute_with_parameters(
            format!("EXPLAIN {ACTIVE_SESSION_QUERY}"),
            parameters.clone(),
        )
        .unwrap()
    else {
        panic!("diagnostic EXPLAIN did not return a cursor")
    };
    let mut plan = Vec::new();
    loop {
        let batch = client.fetch(&explain_cursor).unwrap();
        plan.extend(batch.rows);
        if batch.eof {
            break;
        }
    }
    eprintln!("live plan: {plan:#?}");
    for sql in [
        "SELECT u.id FROM users u WHERE u.id = :user_id",
        "SELECT u.id FROM users u WHERE u.id = :user_id AND u.deleted_at IS NULL",
        "SELECT u.id FROM users u WHERE u.id = :user_id AND u.disabled_at IS NULL AND u.deleted_at IS NULL",
        "SELECT s.id FROM sessions s
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id",
        "SELECT s.id FROM sessions s
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id
           AND s.revoked_at IS NULL AND s.expires_at > :now
           AND (s.absolute_expires_at IS NULL OR s.absolute_expires_at > :now)",
        "SELECT s.id FROM sessions s INNER JOIN users u ON u.id = s.user_id
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id",
        "SELECT s.id, u.id, u.username, u.disabled_at, u.deleted_at
         FROM sessions s INNER JOIN users u ON u.id = s.user_id
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id",
        "SELECT * FROM sessions s INNER JOIN users u ON u.id = s.user_id
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id",
        "SELECT s.id FROM sessions s INNER JOIN users u ON u.id = s.user_id
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id
           AND u.disabled_at IS NULL AND u.deleted_at IS NULL",
        "SELECT s.id FROM sessions s INNER JOIN users u ON u.id = s.user_id
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id
           AND u.disabled_at IS NULL",
        "SELECT s.id FROM sessions s INNER JOIN users u ON u.id = s.user_id
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id
           AND u.deleted_at IS NULL",
        "SELECT s.id FROM sessions s INNER JOIN users u ON u.id = s.user_id
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id
           AND s.revoked_at IS NULL AND s.expires_at > :now
           AND (s.absolute_expires_at IS NULL OR s.absolute_expires_at > :now)",
        "SELECT s.id FROM sessions s INNER JOIN users u ON u.id = s.user_id
         WHERE s.id = :session_id AND s.user_id = :user_id AND s.device_id = :device_id
           AND s.revoked_at IS NULL AND s.expires_at > :now
           AND (s.absolute_expires_at IS NULL OR s.absolute_expires_at > :now)
           AND u.disabled_at IS NULL AND u.deleted_at IS NULL",
        ACTIVE_SESSION_QUERY,
    ] {
        let query_parameters = parameters
            .iter()
            .filter(|(name, _)| sql.contains(&format!(":{name}")))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        let ExecuteResult::Cursor(cursor) = client
            .execute_with_parameters(sql, query_parameters)
            .unwrap()
        else {
            panic!("diagnostic query did not return a cursor")
        };
        let mut rows = Vec::new();
        loop {
            let batch = client.fetch(&cursor).unwrap();
            rows.extend(batch.rows);
            if batch.eof {
                break;
            }
        }
        eprintln!("parameterized cut rows={}: {sql}\n{rows:#?}", rows.len());
    }
}
