// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![cfg(feature = "stress-tests")]

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::prerelease::{
    messenger_schema_plan, messenger_small_seed_plan, messenger_view_plan, SchemaPlan,
};
use radixdb::Database;
use radixdb_orm::DatabaseDescriptor;

fn execute_plan(db: &Database, plan: &SchemaPlan) {
    plan.validate().expect("validate SQL plan");
    for statement in &plan.statements {
        db.execute(&statement.sql, ())
            .unwrap_or_else(|error| panic!("{} failed: {error}", statement.id));
    }
}

fn canonical_rows(db: &Database, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = db
        .query(sql, ())
        .unwrap_or_else(|error| panic!("query failed `{sql}`: {error}"))
        .map(|row| format!("{:?}", row.expect("read result row").as_row()))
        .collect();
    rows.sort_unstable();
    rows
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    db.query_one(sql, ())
        .unwrap_or_else(|error| panic!("scalar failed `{sql}`: {error}"))
}

fn scalar_from_plan(db: &Database, plan: &radixdb::executor::CachedPlanRef) -> i64 {
    let mut rows = db.query_plan(plan, ()).expect("execute cached view plan");
    let row = rows
        .next()
        .expect("cached plan returned one row")
        .expect("read cached plan row");
    row.get(0).expect("cached plan scalar")
}

fn view_body(create_view: &str) -> &str {
    create_view
        .split_once(" AS ")
        .map(|(_, body)| body)
        .expect("CREATE VIEW has AS body")
}

fn assert_view_matches_inline(db: &Database, view: &str, body: &str) {
    let view_rows = canonical_rows(db, &format!("SELECT * FROM {view}"));
    let inline_rows = canonical_rows(db, body);
    assert_eq!(
        view_rows, inline_rows,
        "{view} changed multiplicity or values"
    );

    let view_minus_inline = canonical_rows(db, &format!("SELECT * FROM {view} EXCEPT {body}"));
    assert!(
        view_minus_inline.is_empty(),
        "{view} contains rows absent from inline SQL: {view_minus_inline:?}"
    );
    let inline_minus_view = canonical_rows(db, &format!("{body} EXCEPT SELECT * FROM {view}"));
    assert!(
        inline_minus_view.is_empty(),
        "inline SQL contains rows absent from {view}: {inline_minus_view:?}"
    );
}

fn descriptor(db: &Database) -> DatabaseDescriptor {
    let json: String = db
        .query_one("DESCRIBE DATABASE FORMAT JSON", ())
        .expect("describe database JSON");
    DatabaseDescriptor::from_json(&json).expect("decode database descriptor")
}

fn create_fixture(db: &Database) {
    execute_plan(db, &messenger_schema_plan());
    execute_plan(db, &messenger_small_seed_plan());
    execute_plan(db, &messenger_view_plan());
}

fn assert_all_views(db: &Database) {
    for statement in messenger_view_plan().statements {
        assert_view_matches_inline(db, &statement.id, view_body(&statement.sql));
    }

    assert_eq!(
        scalar_i64(
            db,
            "SELECT COUNT(*) FROM active_conversations_v WHERE conversation_id = 20 AND last_sequence IS NULL",
        ),
        1,
        "LEFT JOIN dropped the active conversation without a message"
    );
    assert_eq!(
        scalar_i64(
            db,
            "SELECT COUNT(*) FROM message_delivery_v WHERE message_id = 101 AND receipt_user_id IS NULL",
        ),
        1,
        "LEFT JOIN dropped the message without a receipt"
    );
    let direct_join = db
        .query(
            "SELECT * FROM active_conversations_v a LEFT JOIN conversation_unread_v u ON u.conversation_id = a.conversation_id AND u.user_id = a.user_id",
            (),
        )
        .expect("inspect nested view join columns");
    let direct_join_columns = direct_join.columns().to_vec();
    drop(direct_join);
    let inbox_rows = canonical_rows(db, "SELECT * FROM user_inbox_v");
    assert_eq!(
        inbox_rows.len(),
        3,
        "nested view multiplicity changed; columns={direct_join_columns:?}; rows={inbox_rows:?}"
    );
    assert_eq!(
        scalar_i64(
            db,
            "SELECT unread_count FROM user_inbox_v WHERE conversation_id = 10 AND user_id = 1",
        ),
        2
    );
    let no_message_unread = scalar_i64(
        db,
        "SELECT unread_count FROM user_inbox_v WHERE conversation_id = 20 AND user_id = 3",
    );
    assert_eq!(
        no_message_unread,
        0,
        "nullable aggregate changed: {:?}",
        canonical_rows(db, "SELECT * FROM conversation_unread_v")
    );
}

fn assert_base_model_checksum(db: &Database) {
    let rows = db
        .query("SELECT COUNT(*), SUM(id), SUM(sequence) FROM messages", ())
        .expect("scan message checksum")
        .collect_vec()
        .expect("collect checksum");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0).unwrap(), 2);
    assert_eq!(rows[0].get::<i64>(1).unwrap(), 201);
    assert_eq!(rows[0].get::<i64>(2).unwrap(), 3);

    assert_eq!(
        scalar_i64(
            db,
            "SELECT id FROM messages WHERE conversation_id = 10 AND sequence = 2",
        ),
        101,
        "composite index lookup disagrees with model"
    );
    assert_eq!(
        scalar_i64(
            db,
            "SELECT COUNT(*) FROM messages m JOIN conversation_members cm ON cm.conversation_id = m.conversation_id",
        ),
        4,
        "full join checksum disagrees with model"
    );
}

#[test]
fn b2_views_match_inline_and_cached_plans_follow_catalog_generation() {
    let db = Database::open("memory://prerelease-b2-views").expect("open B2 database");
    create_fixture(&db);
    assert_all_views(&db);
    assert_base_model_checksum(&db);

    let before = descriptor(&db);
    assert_eq!(before.tables.len(), 18);
    assert_eq!(before.views.len(), 8);
    let view_names: BTreeSet<_> = before.views.iter().map(|view| view.name.as_str()).collect();
    assert!(view_names.contains("user_inbox_v"));
    let dependencies: BTreeMap<_, _> = before
        .views
        .iter()
        .map(|view| (view.name.as_str(), view.dependencies.as_slice()))
        .collect();
    assert!(dependencies["user_inbox_v"]
        .iter()
        .any(|dependency| dependency == "active_conversations_v"));
    assert!(dependencies["forwarded_attachment_access_v"]
        .iter()
        .any(|dependency| dependency == "attachments"));

    let cached = db
        .cached_plan("SELECT COUNT(*) FROM pending_outbox_v")
        .expect("cache original view plan");
    assert_eq!(scalar_from_plan(&db, &cached), 1);
    db.execute("DROP VIEW pending_outbox_v", ())
        .expect("drop original view");
    db.execute(
        "CREATE VIEW pending_outbox_v AS SELECT id, message_id, retry_count, lease_owner FROM outbox_jobs WHERE state = 'done' AND id = -1",
        (),
    )
    .expect("create replacement view");
    assert_eq!(
        scalar_from_plan(&db, &cached),
        0,
        "cached plan retained the dropped view definition"
    );
    db.execute("DROP VIEW pending_outbox_v", ())
        .expect("drop replacement view");
    let original = messenger_view_plan()
        .statements
        .into_iter()
        .find(|statement| statement.id == "pending_outbox_v")
        .expect("original pending view");
    db.execute(&original.sql, ())
        .expect("recreate original pending view");
    assert_eq!(scalar_from_plan(&db, &cached), 1);
}

#[test]
fn b2_views_dependencies_and_results_survive_checkpoint_reopen_and_restore() {
    let directory = tempfile::tempdir().expect("B2 tempdir");
    let path = directory.path().join("messenger-b2");
    let dsn = format!("file://{}?checkpoint_on_close=off", path.display());

    let (initial_descriptor, initial_views) = {
        let db = Database::open(&dsn).expect("open B2 file database");
        create_fixture(&db);
        assert_all_views(&db);
        assert_base_model_checksum(&db);
        let descriptor_before = descriptor(&db);
        let views: BTreeMap<_, _> = messenger_view_plan()
            .statements
            .iter()
            .map(|statement| {
                (
                    statement.id.clone(),
                    canonical_rows(&db, &format!("SELECT * FROM {}", statement.id)),
                )
            })
            .collect();

        db.execute("PRAGMA SNAPSHOT", ())
            .expect("snapshot B2 fixture");
        db.execute(
            "INSERT INTO messages VALUES (102, 10, 3, 1, 'after snapshot', false)",
            (),
        )
        .expect("mutate after snapshot");
        db.execute(
            "INSERT INTO outbox_jobs VALUES (1002, 102, 'pending', 0, NULL, true)",
            (),
        )
        .expect("mutate outbox after snapshot");
        db.execute("DROP VIEW user_inbox_v", ())
            .expect("drop nested view after snapshot");
        db.execute("PRAGMA CHECKPOINT", ())
            .expect("checkpoint post-snapshot state");
        db.execute("PRAGMA RESTORE", ())
            .expect("restore B2 snapshot");

        assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM messages"), 2);
        assert_all_views(&db);
        let restored = descriptor(&db);
        assert_eq!(restored.tables, descriptor_before.tables);
        assert_eq!(restored.views, descriptor_before.views);
        db.execute("PRAGMA CHECKPOINT", ())
            .expect("checkpoint restored state");
        db.close().expect("close restored B2 database");
        (descriptor_before, views)
    };

    let reopened = Database::open(&dsn).expect("reopen B2 database");
    assert_all_views(&reopened);
    assert_base_model_checksum(&reopened);
    let reopened_descriptor = descriptor(&reopened);
    assert_eq!(reopened_descriptor.tables, initial_descriptor.tables);
    assert_eq!(reopened_descriptor.views, initial_descriptor.views);
    for (view, expected) in initial_views {
        assert_eq!(
            canonical_rows(&reopened, &format!("SELECT * FROM {view}")),
            expected,
            "{view} changed after restore/checkpoint/reopen"
        );
    }
}
