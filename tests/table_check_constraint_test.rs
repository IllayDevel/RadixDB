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

//! RDB-0009 regression coverage for table-level CHECK constraints.

use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr},
    thread,
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb::Database;
use radixdb_client::{Connection, ExecuteResult};
use tempfile::{tempdir, NamedTempFile};

const OWNER: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e80";
const TARGET: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e81";
const OTHER_OWNER: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e82";
const OTHER_TARGET: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e83";
const ID_1: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e84";
const ID_2: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e85";
const ID_3: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7e86";

fn tcp_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
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

fn create_uuid_check_table(db: &Database, table: &str) {
    db.execute(
        &format!(
            "CREATE TABLE {table} (
                id UUID PRIMARY KEY,
                owner_id UUID NOT NULL,
                target_id UUID,
                CHECK (owner_id != target_id)
            )"
        ),
        (),
    )
    .unwrap();
}

#[test]
fn table_check_enforces_uuid_insert_update_and_explicit_transaction() {
    let db = Database::open("memory://rdb0009_uuid_dml").unwrap();
    create_uuid_check_table(&db, "blocks");

    db.execute(
        &format!("INSERT INTO blocks VALUES ('{ID_1}', '{OWNER}', '{TARGET}')"),
        (),
    )
    .unwrap();

    let error = db
        .execute(
            &format!("INSERT INTO blocks VALUES ('{ID_2}', '{OWNER}', '{OWNER}')"),
            (),
        )
        .expect_err("equal UUIDs must violate the table CHECK");
    assert!(error.to_string().contains("CHECK constraint failed"));
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM blocks", ())
            .unwrap(),
        1
    );

    let error = db
        .execute("UPDATE blocks SET target_id = owner_id", ())
        .expect_err("UPDATE must validate the complete post-change row");
    assert!(error.to_string().contains("CHECK constraint failed"));
    let target: String = db.query_one("SELECT target_id FROM blocks", ()).unwrap();
    assert_eq!(target, TARGET);

    db.execute("BEGIN", ()).unwrap();
    let error = db
        .execute("UPDATE blocks SET target_id = owner_id", ())
        .expect_err("explicit transaction must enforce table CHECK");
    assert!(error.to_string().contains("CHECK constraint failed"));
    db.execute("ROLLBACK", ())
        .expect("constraint error must leave the transaction rollback-capable");

    // SQL CHECK treats UNKNOWN as accepted. A NULL target produces UNKNOWN for !=.
    db.execute(
        &format!("INSERT INTO blocks VALUES ('{ID_3}', '{OTHER_OWNER}', NULL)"),
        (),
    )
    .unwrap();
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM blocks", ())
            .unwrap(),
        2
    );
}

#[test]
fn table_check_covers_upsert_and_insert_select() {
    let db = Database::open("memory://rdb0009_upsert_select").unwrap();
    db.execute(
        "CREATE TABLE pairs (
            id INTEGER PRIMARY KEY,
            owner_id UUID NOT NULL,
            target_id UUID NOT NULL,
            CHECK (owner_id != target_id)
        )",
        (),
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO pairs VALUES (1, '{OWNER}', '{TARGET}')"),
        (),
    )
    .unwrap();

    let error = db
        .execute(
            &format!(
                "INSERT INTO pairs VALUES (1, '{OTHER_OWNER}', '{OTHER_TARGET}')
                 ON CONFLICT (id) DO UPDATE SET target_id = owner_id"
            ),
            (),
        )
        .expect_err("upsert update must validate its complete post-change row");
    assert!(error.to_string().contains("CHECK constraint failed"));
    let target: String = db
        .query_one("SELECT target_id FROM pairs WHERE id = 1", ())
        .unwrap();
    assert_eq!(target, TARGET);

    db.execute(
        "CREATE TABLE pair_source (id INTEGER, owner_id UUID, target_id UUID)",
        (),
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO pair_source VALUES (2, '{OWNER}', '{OWNER}')"),
        (),
    )
    .unwrap();
    let error = db
        .execute(
            "INSERT INTO pairs SELECT id, owner_id, target_id FROM pair_source",
            (),
        )
        .expect_err("INSERT SELECT must enforce table CHECK");
    assert!(error.to_string().contains("CHECK constraint failed"));
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM pairs", ())
            .unwrap(),
        1
    );
}

#[test]
fn table_check_covers_csv_and_json_copy() {
    let db = Database::open("memory://rdb0009_copy").unwrap();
    create_uuid_check_table(&db, "csv_blocks");
    create_uuid_check_table(&db, "json_blocks");

    let mut csv = NamedTempFile::new().unwrap();
    writeln!(csv, "id,owner_id,target_id").unwrap();
    writeln!(csv, "{ID_1},{OWNER},{OWNER}").unwrap();
    let error = db
        .execute(
            &format!(
                "COPY csv_blocks FROM '{}' WITH (FORMAT CSV, HEADER true)",
                csv.path().display()
            ),
            (),
        )
        .expect_err("CSV COPY must enforce table CHECK");
    assert!(error.to_string().contains("CHECK constraint failed"));

    let mut json = NamedTempFile::new().unwrap();
    writeln!(
        json,
        "{{\"id\":\"{ID_2}\",\"owner_id\":\"{OWNER}\",\"target_id\":\"{OWNER}\"}}"
    )
    .unwrap();
    let error = db
        .execute(
            &format!(
                "COPY json_blocks FROM '{}' WITH (FORMAT JSON)",
                json.path().display()
            ),
            (),
        )
        .expect_err("JSON COPY must enforce table CHECK");
    assert!(error.to_string().contains("CHECK constraint failed"));

    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM csv_blocks", ())
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM json_blocks", ())
            .unwrap(),
        0
    );
}

#[test]
fn table_check_survives_wal_checkpoint_and_snapshot_reopen() {
    let dir = tempdir().unwrap();
    let dsn = format!("file://{}", dir.path().join("table_checks.db").display());

    {
        let db = Database::open(&dsn).unwrap();
        create_uuid_check_table(&db, "durable_blocks");
        db.execute(
            &format!("INSERT INTO durable_blocks VALUES ('{ID_1}', '{OWNER}', '{TARGET}')"),
            (),
        )
        .unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert!(db
            .execute(
                &format!(
                    "INSERT INTO durable_blocks VALUES ('{ID_2}', '{OTHER_OWNER}', '{OTHER_OWNER}')"
                ),
                (),
            )
            .is_err());
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.execute("PRAGMA SNAPSHOT", ()).unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&dsn).unwrap();
        assert!(db
            .execute(
                &format!(
                    "INSERT INTO durable_blocks VALUES ('{ID_3}', '{OTHER_TARGET}', '{OTHER_TARGET}')"
                ),
                (),
            )
            .is_err());
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM durable_blocks", ())
                .unwrap(),
            1
        );
        db.close().unwrap();
    }
}

#[test]
fn invalid_table_check_is_rejected_at_ddl_and_non_boolean_is_fail_closed() {
    let db = Database::open("memory://rdb0009_invalid_ddl").unwrap();

    let error = db
        .execute(
            "CREATE TABLE bad_reference (id INTEGER, CHECK (missing_column > 0))",
            (),
        )
        .expect_err("unknown CHECK column must reject CREATE TABLE");
    assert!(error.to_string().contains("invalid table CHECK expression"));

    db.execute(
        "CREATE TABLE non_boolean (id INTEGER PRIMARY KEY, CHECK (id + 1))",
        (),
    )
    .unwrap();
    let error = db
        .execute("INSERT INTO non_boolean VALUES (1)", ())
        .expect_err("non-boolean table CHECK must fail closed");
    assert!(error.to_string().contains("expected BOOLEAN or NULL"));
}

#[test]
fn table_check_matrix_and_alter_are_fail_closed() {
    let db = Database::open("memory://rdb0009_type_matrix").unwrap();
    db.execute(
        "CREATE TABLE typed_limits (
            id INTEGER PRIMARY KEY,
            minimum DECIMAL NOT NULL,
            maximum DECIMAL NOT NULL,
            label TEXT NOT NULL,
            active BOOLEAN NOT NULL,
            CHECK (minimum <= maximum),
            CHECK (label != '' OR active = FALSE)
        )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO typed_limits VALUES (1, 1.25, 2.50, 'valid', TRUE)",
        (),
    )
    .unwrap();
    assert!(db
        .execute(
            "INSERT INTO typed_limits VALUES (2, 3.00, 2.00, 'invalid', TRUE)",
            (),
        )
        .is_err());
    assert!(db
        .execute(
            "INSERT INTO typed_limits VALUES (3, 1.00, 2.00, '', TRUE)",
            (),
        )
        .is_err());

    let error = db
        .execute("ALTER TABLE typed_limits DROP COLUMN maximum", ())
        .expect_err("DROP must not orphan a persisted CHECK");
    assert!(error
        .to_string()
        .contains("table CHECK would become invalid"));
    let error = db
        .execute(
            "ALTER TABLE typed_limits RENAME COLUMN minimum TO lower_bound",
            (),
        )
        .expect_err("RENAME must not orphan a persisted CHECK");
    assert!(error
        .to_string()
        .contains("table CHECK would become invalid"));

    // An unrelated schema change remains legal.
    db.execute("ALTER TABLE typed_limits ADD COLUMN note TEXT", ())
        .unwrap();
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM typed_limits", ())
            .unwrap(),
        1
    );
}

#[test]
fn table_check_is_enforced_over_tcp_and_transaction_remains_rollback_capable() {
    let temp = tempdir().unwrap();
    let server = Server::bind_ephemeral(&tcp_config(temp.path().join("data"))).unwrap();
    let address = server.local_addr().unwrap();

    thread::scope(|scope| {
        let worker = scope.spawn(|| server.serve_one());
        let mut client = Connection::connect(address).unwrap();
        client.authenticate("root", None).unwrap();
        client.select_database("rdb0009_tcp").unwrap();
        assert!(matches!(
            client
                .execute(
                    "CREATE TABLE blocks (
                        id UUID PRIMARY KEY,
                        owner_id UUID NOT NULL,
                        target_id UUID NOT NULL,
                        CHECK (owner_id != target_id)
                    )"
                )
                .unwrap(),
            ExecuteResult::CommandComplete { .. }
        ));
        client.begin().unwrap();
        let error = client
            .execute(format!(
                "INSERT INTO blocks VALUES ('{ID_1}', '{OWNER}', '{OWNER}')"
            ))
            .expect_err("TCP INSERT must enforce table CHECK");
        assert!(error.to_string().contains("CHECK constraint failed"));
        assert!(client.in_transaction());
        client.rollback().unwrap();
        assert!(!client.in_transaction());
        drop(client);
        worker.join().unwrap().unwrap();
    });
}
