// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! JOIN correctness across hot, WAL replay and cold/reopen paths.

use radixdb::Database;

const UUID_A: &str = "018f11d4-3b29-7000-8000-000000000001";
const UUID_B: &str = "018f11d4-3b29-7000-8000-000000000002";

fn query_i64(db: &Database, sql: &str) -> i64 {
    db.query_one(sql, ())
        .unwrap_or_else(|error| panic!("query failed: {sql}: {error}"))
}

fn create_join_dataset(db: &Database) {
    db.execute(
        "CREATE TABLE uuid_parent (id UUID PRIMARY KEY, label TEXT NOT NULL)",
        (),
    )
    .expect("create uuid parent");
    db.execute(
        "CREATE TABLE uuid_child (id INTEGER PRIMARY KEY, parent_id UUID, label TEXT NOT NULL)",
        (),
    )
    .expect("create uuid child");
    db.execute(
        "CREATE INDEX idx_uuid_child_parent ON uuid_child(parent_id)",
        (),
    )
    .expect("index uuid child parent");

    db.execute(
        "CREATE TABLE text_parent (code TEXT UNIQUE, label TEXT NOT NULL)",
        (),
    )
    .expect("create text parent");
    db.execute(
        "CREATE TABLE text_child (id INTEGER PRIMARY KEY, parent_code TEXT, label TEXT NOT NULL)",
        (),
    )
    .expect("create text child");
    db.execute(
        "CREATE INDEX idx_text_child_parent ON text_child(parent_code)",
        (),
    )
    .expect("index text child parent");

    db.execute(
        "CREATE TABLE int_parent (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        (),
    )
    .expect("create int parent");
    db.execute(
        "CREATE TABLE int_child (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT NOT NULL)",
        (),
    )
    .expect("create int child");
    db.execute(
        "CREATE INDEX idx_int_child_parent ON int_child(parent_id)",
        (),
    )
    .expect("index int child parent");

    db.execute(
        &format!("INSERT INTO uuid_parent VALUES ('{UUID_A}', 'uuid-a'), ('{UUID_B}', 'uuid-b')"),
        (),
    )
    .expect("insert uuid parents");
    db.execute(
        &format!(
            "INSERT INTO uuid_child VALUES \
             (1, '{UUID_A}', 'uuid-child-a'), \
             (2, '{UUID_B}', 'uuid-child-b'), \
             (3, NULL, 'uuid-orphan')"
        ),
        (),
    )
    .expect("insert uuid children");

    db.execute(
        "INSERT INTO text_parent VALUES ('A-001', 'text-a'), ('B-002', 'text-b')",
        (),
    )
    .expect("insert text parents");
    db.execute(
        "INSERT INTO text_child VALUES \
         (1, 'A-001', 'text-child-a'), \
         (2, 'B-002', 'text-child-b'), \
         (3, 'Z-404', 'text-orphan')",
        (),
    )
    .expect("insert text children");

    db.execute(
        "INSERT INTO int_parent VALUES (1, 'int-a'), (2, 'int-b'), (3, 'int-no-child')",
        (),
    )
    .expect("insert int parents");
    db.execute(
        "INSERT INTO int_child VALUES \
         (1, 1, 'int-child-a'), \
         (2, 2, 'int-child-b'), \
         (3, 99, 'int-orphan')",
        (),
    )
    .expect("insert int children");
}

fn assert_join_contract(db: &Database, phase: &str) {
    assert_eq!(
        query_i64(
            db,
            "SELECT COUNT(*) FROM uuid_parent p JOIN uuid_child c ON p.id = c.parent_id"
        ),
        2,
        "{phase}: UUID parent->child join"
    );
    assert_eq!(
        query_i64(
            db,
            "SELECT COUNT(*) FROM uuid_child c JOIN uuid_parent p ON c.parent_id = p.id"
        ),
        2,
        "{phase}: UUID child->parent join"
    );

    assert_eq!(
        query_i64(
            db,
            "SELECT COUNT(*) FROM text_parent p JOIN text_child c ON p.code = c.parent_code"
        ),
        2,
        "{phase}: TEXT unique parent->child join"
    );
    assert_eq!(
        query_i64(
            db,
            "SELECT COUNT(*) FROM text_child c JOIN text_parent p ON c.parent_code = p.code"
        ),
        2,
        "{phase}: TEXT child->unique parent join"
    );

    assert_eq!(
        query_i64(
            db,
            "SELECT COUNT(*) FROM int_parent p JOIN int_child c ON p.id = c.parent_id"
        ),
        2,
        "{phase}: INTEGER parent->child join"
    );
    assert_eq!(
        query_i64(
            db,
            "SELECT COUNT(*) FROM int_child c JOIN int_parent p ON c.parent_id = p.id"
        ),
        2,
        "{phase}: INTEGER child->parent join"
    );

    assert_eq!(
        query_i64(
            db,
            "SELECT COUNT(*) FROM int_parent p LEFT JOIN int_child c ON p.id = c.parent_id"
        ),
        3,
        "{phase}: LEFT JOIN preserves unmatched left row"
    );
    assert_eq!(
        query_i64(
            db,
            "SELECT COUNT(*) FROM int_parent p LEFT JOIN int_child c ON p.id = c.parent_id \
             WHERE c.id IS NULL"
        ),
        1,
        "{phase}: LEFT JOIN exposes unmatched right side as NULL"
    );
}

#[test]
fn joins_are_symmetric_on_hot_cold_and_reopened_file_database() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dsn = format!("file://{}/join_reopen_cold", dir.path().display());

    let db = Database::open(&dsn).expect("open database");
    create_join_dataset(&db);
    assert_join_contract(&db, "hot");

    db.execute("PRAGMA CHECKPOINT", ()).expect("checkpoint");
    assert_join_contract(&db, "cold-after-checkpoint");
    db.close().expect("close database");

    let reopened = Database::open(&dsn).expect("reopen database");
    assert_join_contract(&reopened, "reopened-cold");
    reopened.close().expect("close reopened database");
}

#[test]
fn joins_are_symmetric_after_wal_replay_without_checkpoint() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dsn = format!("file://{}/join_wal_replay", dir.path().display());

    {
        let db = Database::open(&dsn).expect("open database");
        create_join_dataset(&db);
        assert_join_contract(&db, "hot-before-wal-replay");
        db.close()
            .expect("close database without explicit checkpoint");
    }

    let reopened = Database::open(&dsn).expect("reopen database from WAL");
    assert_join_contract(&reopened, "wal-replay");
    reopened.close().expect("close reopened database");
}
