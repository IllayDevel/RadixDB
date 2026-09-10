// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb::Database;
use radixdb_client::{Connection, ExecuteResult, Row, WireValue};
use radixdb_orm::{DescriptorEnvelope, DescriptorKind, DynamicRecord, TableDescriptor, TypedValue};

struct StopServerOnDrop<'a>(&'a AtomicBool);

impl Drop for StopServerOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn test_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 8,
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

fn connect(address: SocketAddr, database: &str) -> Connection {
    let mut client = Connection::connect(address).expect("connect");
    client.authenticate("root", None).expect("authenticate");
    client.select_database(database).expect("select database");
    client
}

fn assert_command(result: ExecuteResult) {
    assert!(matches!(result, ExecuteResult::CommandComplete { .. }));
}

fn consume_success(client: &mut Connection, result: ExecuteResult) {
    let ExecuteResult::Cursor(cursor) = result else {
        return;
    };
    loop {
        if client.fetch(&cursor).expect("fetch command cursor").eof {
            return;
        }
    }
}

fn count(client: &mut Connection, sql: &str) -> i64 {
    let ExecuteResult::Cursor(cursor) = client.execute(sql).expect("open count cursor") else {
        panic!("count query did not return a cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch count");
    assert!(batch.eof);
    let Row { values } = batch.rows.into_iter().next().expect("count row");
    let WireValue::Int(value) = values.into_iter().next().expect("count value") else {
        panic!("COUNT did not return an integer");
    };
    value
}

fn single_text(client: &mut Connection, sql: &str) -> String {
    let ExecuteResult::Cursor(cursor) = client.execute(sql).expect("open text cursor") else {
        panic!("text query did not return a cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch text row");
    assert!(batch.eof);
    let Row { values } = batch.rows.into_iter().next().expect("text row");
    let WireValue::String(value) = values.into_iter().next().expect("text value") else {
        panic!("query did not return text");
    };
    value
}

fn has_index(client: &mut Connection, table: &str, expected: &str) -> bool {
    let ExecuteResult::Cursor(cursor) = client
        .execute(format!("SHOW INDEXES FROM {table}"))
        .expect("show indexes")
    else {
        panic!("SHOW INDEXES did not return a cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch indexes");
    batch
        .rows
        .into_iter()
        .any(|Row { values }| values.get(1) == Some(&WireValue::String(expected.to_string())))
}

fn query_succeeds(client: &mut Connection, sql: &str) -> bool {
    let Ok(ExecuteResult::Cursor(cursor)) = client.execute(sql) else {
        return false;
    };
    loop {
        let batch = client.fetch(&cursor).expect("fetch query result");
        if batch.eof {
            return true;
        }
    }
}

fn rdb_0030_endpoint_id(position: i64) -> String {
    format!("018f2b34-7a10-7cc2-8f3a-{position:012x}")
}

const RDB_0031_PARENT_ID: [u8; 16] = [
    0x01, 0xa0, 0x05, 0x08, 0x7a, 0x7f, 0x72, 0xc2, 0xa2, 0x25, 0x30, 0xb4, 0xd5, 0x5c, 0xc1, 0xfc,
];

fn rdb_0031_child_id(tag: u8) -> [u8; 16] {
    [
        0x01, 0xa0, 0x04, 0xd7, 0xd9, 0xe7, 0x78, tag, 0x9d, 0x8f, 0x84, 0x41, 0x8a, 0xad, 0x75,
        tag,
    ]
}

#[test]
fn explicit_tcp_ddl_is_atomic_rollback_capable_and_durable() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database = "tcp_transactional_ddl";

    let config = test_config(data_dir.clone());
    let server = Server::bind_ephemeral(&config).expect("bind server");
    let address = server.local_addr().expect("server address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let _stop_server_on_drop = StopServerOnDrop(&shutdown);
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut owner = connect(address, database);
        let mut observer = connect(address, database);

        assert_command(
            owner
                .execute(
                    "CREATE TABLE existing_messages (
                        id INTEGER PRIMARY KEY,
                        body TEXT NOT NULL
                    )",
                )
                .expect("create existing table"),
        );
        owner.begin().expect("begin existing-table index rollback");
        assert_command(
            owner
                .execute("CREATE INDEX discarded_body_idx ON existing_messages (body)")
                .expect("stage discarded index"),
        );
        owner.rollback().expect("rollback existing-table index");
        assert!(!has_index(
            &mut observer,
            "existing_messages",
            "discarded_body_idx"
        ));

        owner.begin().expect("begin existing-table index commit");
        assert_command(
            owner
                .execute("CREATE INDEX existing_body_idx ON existing_messages (body)")
                .expect("stage committed index"),
        );
        owner.commit().expect("commit existing-table index");
        assert!(has_index(
            &mut observer,
            "existing_messages",
            "existing_body_idx"
        ));

        owner.begin().expect("begin rollback transaction");
        assert_command(
            owner
                .execute("CREATE TABLE rolled_back (id INTEGER PRIMARY KEY, value TEXT)")
                .expect("create private table"),
        );
        assert_command(
            owner
                .execute("INSERT INTO rolled_back VALUES (1, 'private')")
                .expect("insert private row"),
        );
        assert_command(
            owner
                .execute("CREATE INDEX rolled_back_value_idx ON rolled_back (value)")
                .expect("stage private index"),
        );
        assert_eq!(count(&mut owner, "SELECT COUNT(*) FROM rolled_back"), 1);
        assert!(
            observer.execute("SELECT * FROM rolled_back").is_err(),
            "another TCP session must not see CREATE TABLE before commit"
        );
        owner.rollback().expect("rollback DDL+DML");
        assert!(observer.execute("SELECT * FROM rolled_back").is_err());

        owner.begin().expect("begin commit transaction");
        assert_command(
            owner
                .execute(
                    "CREATE TABLE schema_migrations (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMP NOT NULL
                    )",
                )
                .expect("create migrations table"),
        );
        assert_command(
            owner
                .execute(
                    "CREATE TABLE conversations (
                        id INTEGER PRIMARY KEY,
                        title TEXT NOT NULL
                    )",
                )
                .expect("create conversations table"),
        );
        assert_command(
            owner
                .execute(
                    "CREATE TABLE messages (
                        id INTEGER PRIMARY KEY,
                        conversation_id INTEGER NOT NULL REFERENCES conversations(id),
                        body TEXT NOT NULL
                    )",
                )
                .expect("create messages table"),
        );
        assert_command(
            owner
                .execute(
                    "CREATE INDEX messages_conversation_idx
                     ON messages (conversation_id, id)",
                )
                .expect("stage messages index"),
        );
        assert_command(
            owner
                .execute("INSERT INTO conversations VALUES (7, 'general')")
                .expect("insert parent row"),
        );
        assert_command(
            owner
                .execute("INSERT INTO messages VALUES (1, 7, 'committed')")
                .expect("insert migration data"),
        );
        assert_command(
            owner
                .execute(
                    "INSERT INTO schema_migrations
                     VALUES (1, TIMESTAMP '2026-08-09 00:00:00')",
                )
                .expect("record migration"),
        );
        owner
            .commit()
            .expect("commit schema and migration atomically");

        assert_eq!(count(&mut observer, "SELECT COUNT(*) FROM messages"), 1);
        assert_eq!(
            count(&mut observer, "SELECT COUNT(*) FROM schema_migrations"),
            1
        );
        assert!(has_index(
            &mut observer,
            "messages",
            "messages_conversation_idx"
        ));

        owner.begin().expect("begin failing DDL transaction");
        assert_command(
            owner
                .execute("CREATE TABLE failed_ddl (id INTEGER PRIMARY KEY)")
                .expect("create table before DDL error"),
        );
        let error = owner
            .execute("CREATE INDEX failed_idx ON failed_ddl (missing_column)")
            .expect_err("invalid DDL must fail");
        assert!(error.to_string().contains("missing_column"));
        assert!(
            owner.in_transaction(),
            "DDL error must keep rollback available"
        );
        owner.rollback().expect("rollback after DDL error");
        assert!(observer.execute("SELECT * FROM failed_ddl").is_err());

        drop(owner);
        drop(observer);
        shutdown.store(true, Ordering::Release);
        worker
            .join()
            .expect("server thread joins")
            .expect("server stops cleanly");
    });

    // The CREATE TABLE/CREATE INDEX records share the user transaction's WAL
    // commit marker, so both schema and migration row must survive a restart.
    let restart_config = test_config(data_dir);
    let restart_server = Server::bind_ephemeral(&restart_config).expect("bind restart server");
    let restart_address = restart_server.local_addr().expect("restart address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| restart_server.serve_one());
        let mut client = connect(restart_address, database);
        assert_eq!(count(&mut client, "SELECT COUNT(*) FROM messages"), 1);
        assert_eq!(
            count(&mut client, "SELECT COUNT(*) FROM schema_migrations"),
            1
        );
        assert!(has_index(
            &mut client,
            "messages",
            "messages_conversation_idx"
        ));
        assert!(has_index(
            &mut client,
            "existing_messages",
            "existing_body_idx"
        ));
        drop(client);
        worker
            .join()
            .expect("restart thread joins")
            .expect("restart server serves client");
    });
}

#[test]
fn explicit_tcp_alter_add_column_is_isolated_rollback_capable_and_durable() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database = "tcp_transactional_alter";

    let config = test_config(data_dir.clone());
    let server = Server::bind_ephemeral(&config).expect("bind server");
    let address = server.local_addr().expect("server address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let _stop_server_on_drop = StopServerOnDrop(&shutdown);
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut owner = connect(address, database);
        let mut observer = connect(address, database);

        assert_command(
            owner
                .execute(
                    "CREATE TABLE schema_migrations (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMP NOT NULL
                    )",
                )
                .expect("create migration history"),
        );
        assert_command(
            owner
                .execute(
                    "CREATE TABLE sessions (
                        id INTEGER PRIMARY KEY,
                        label TEXT NOT NULL,
                        deleted_at TIMESTAMP
                    )",
                )
                .expect("create sessions"),
        );
        assert_command(
            owner
                .execute(
                    "INSERT INTO sessions VALUES (
                        0,
                        'pre-alter',
                        TIMESTAMP '2026-08-08 00:00:00'
                    )",
                )
                .expect("insert row predating ALTER"),
        );
        let checkpoint = owner
            .execute("PRAGMA CHECKPOINT")
            .expect("seal pre-ALTER row into cold storage");
        consume_success(&mut owner, checkpoint);

        owner.begin().expect("begin rollback ALTER");
        assert_command(
            owner
                .execute("ALTER TABLE sessions ADD COLUMN rolled_back_at TIMESTAMP")
                .expect("stage rollback column"),
        );
        assert!(query_succeeds(
            &mut owner,
            "SELECT rolled_back_at FROM sessions"
        ));
        assert!(
            !query_succeeds(&mut observer, "SELECT rolled_back_at FROM sessions"),
            "another TCP session observed an uncommitted column"
        );
        owner.rollback().expect("rollback staged column");
        assert!(!query_succeeds(
            &mut owner,
            "SELECT rolled_back_at FROM sessions"
        ));

        owner.begin().expect("begin failing ALTER");
        assert_command(
            owner
                .execute("ALTER TABLE sessions ADD COLUMN discarded_at TIMESTAMP")
                .expect("stage column before error"),
        );
        owner
            .execute("ALTER TABLE sessions ADD COLUMN discarded_at TIMESTAMP")
            .expect_err("duplicate staged column must fail");
        assert!(
            owner.in_transaction(),
            "statement error must leave protocol-11 rollback available"
        );
        owner.rollback().expect("rollback after ALTER error");
        assert!(!query_succeeds(
            &mut observer,
            "SELECT discarded_at FROM sessions"
        ));

        owner.begin().expect("begin rollback after staged UPDATE");
        assert_command(
            owner
                .execute("ALTER TABLE sessions ADD COLUMN discarded_update_at TIMESTAMP")
                .expect("stage column for UPDATE rollback"),
        );
        assert_command(
            owner
                .execute(
                    "UPDATE sessions
                     SET discarded_update_at = deleted_at
                     WHERE label = 'pre-alter'",
                )
                .expect("update through staged column before rollback"),
        );
        owner
            .execute("SELECT missing_after_update FROM sessions")
            .expect_err("injected failure after staged UPDATE");
        owner.rollback().expect("rollback staged UPDATE migration");
        assert!(!query_succeeds(
            &mut observer,
            "SELECT discarded_update_at FROM sessions"
        ));

        owner.begin().expect("begin rollback after staged index");
        assert_command(
            owner
                .execute("ALTER TABLE sessions ADD COLUMN discarded_index_at TIMESTAMP")
                .expect("stage column for index rollback"),
        );
        assert_command(
            owner
                .execute(
                    "UPDATE sessions
                     SET discarded_index_at = deleted_at
                     WHERE label = 'pre-alter'",
                )
                .expect("update indexed staged column before rollback"),
        );
        assert_command(
            owner
                .execute(
                    "CREATE INDEX discarded_cleanup_idx
                     ON sessions (label, discarded_index_at, id)",
                )
                .expect("stage index before rollback"),
        );
        assert_command(
            owner
                .execute(
                    "INSERT INTO schema_migrations
                     VALUES (99, TIMESTAMP '2026-08-08 12:00:00')",
                )
                .expect("stage migration history before injected failure"),
        );
        owner
            .execute("INSERT INTO schema_migrations VALUES (99, CURRENT_TIMESTAMP)")
            .expect_err("injected failure after staged index and history");
        owner.rollback().expect("rollback staged index migration");
        assert!(!query_succeeds(
            &mut observer,
            "SELECT discarded_index_at FROM sessions"
        ));
        assert!(!has_index(
            &mut observer,
            "sessions",
            "discarded_cleanup_idx"
        ));
        assert_eq!(
            count(
                &mut observer,
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 99"
            ),
            0
        );

        owner.begin().expect("begin committed migration");
        assert_command(
            owner
                .execute("ALTER TABLE sessions ADD COLUMN absolute_expires_at TIMESTAMP")
                .expect("stage committed column"),
        );
        assert_command(
            owner
                .execute(
                    "ALTER TABLE sessions
                     ADD COLUMN session_state TEXT NOT NULL DEFAULT 'active'",
                )
                .expect("stage defaulted column"),
        );
        assert_command(
            owner
                .execute(
                    "ALTER TABLE sessions
                     ADD COLUMN cleanup_available_at TIMESTAMP",
                )
                .expect("stage cleanup queue column"),
        );
        assert_command(
            owner
                .execute(
                    "UPDATE sessions
                     SET cleanup_available_at = deleted_at
                     WHERE label = 'pre-alter' AND deleted_at IS NOT NULL",
                )
                .expect("backfill through staged cleanup column"),
        );
        assert_command(
            owner
                .execute(
                    "CREATE INDEX sessions_cleanup_queue_idx
                     ON sessions (label, cleanup_available_at, id)",
                )
                .expect("stage cleanup queue index"),
        );
        assert_command(
            owner
                .execute(
                    "INSERT INTO schema_migrations
                     VALUES (2, TIMESTAMP '2026-08-09 00:00:00')",
                )
                .expect("stage migration history"),
        );
        assert_command(
            owner
                .execute(
                    "INSERT INTO sessions (id, label, absolute_expires_at)
                     VALUES (1, 'owner-visible', TIMESTAMP '2026-08-10 00:00:00')",
                )
                .expect("write through staged schema"),
        );
        assert!(query_succeeds(
            &mut owner,
            "SELECT absolute_expires_at FROM sessions"
        ));
        assert_eq!(
            count(
                &mut owner,
                "SELECT COUNT(*) FROM sessions WHERE session_state = 'active'"
            ),
            2
        );
        assert!(
            !query_succeeds(&mut observer, "SELECT absolute_expires_at FROM sessions"),
            "observer saw committed-schema candidate before commit"
        );
        assert!(
            !query_succeeds(&mut observer, "SELECT cleanup_available_at FROM sessions"),
            "observer saw staged cleanup column before commit"
        );
        assert!(
            !has_index(&mut observer, "sessions", "sessions_cleanup_queue_idx"),
            "observer saw staged cleanup index before commit"
        );
        assert_eq!(
            count(
                &mut observer,
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 2"
            ),
            0
        );
        owner
            .commit()
            .expect("commit ALTER and migration atomically");

        assert!(query_succeeds(
            &mut observer,
            "SELECT absolute_expires_at FROM sessions"
        ));
        assert_eq!(
            count(
                &mut observer,
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 2"
            ),
            1
        );
        assert_eq!(count(&mut observer, "SELECT COUNT(*) FROM sessions"), 2);
        assert_eq!(
            count(
                &mut observer,
                "SELECT COUNT(*) FROM sessions WHERE session_state = 'active'"
            ),
            2
        );
        assert_eq!(
            count(
                &mut observer,
                "SELECT COUNT(*) FROM sessions WHERE cleanup_available_at IS NOT NULL"
            ),
            1
        );
        assert_eq!(
            count(
                &mut observer,
                "SELECT COUNT(*) FROM sessions
                 WHERE label = 'pre-alter'
                   AND cleanup_available_at = TIMESTAMP '2026-08-08 00:00:00'
                   AND id = 0"
            ),
            1,
            "the staged composite index must contain the transaction-local UPDATE"
        );
        assert!(has_index(
            &mut observer,
            "sessions",
            "sessions_cleanup_queue_idx"
        ));

        owner.begin().expect("begin first concurrent ALTER");
        observer.begin().expect("begin second concurrent ALTER");
        assert_command(
            owner
                .execute("ALTER TABLE sessions ADD COLUMN first_writer_at TIMESTAMP")
                .expect("stage first concurrent ALTER"),
        );
        assert_command(
            observer
                .execute("ALTER TABLE sessions ADD COLUMN stale_writer_at TIMESTAMP")
                .expect("stage stale concurrent ALTER"),
        );
        owner.commit().expect("commit first concurrent ALTER");
        observer
            .commit()
            .expect_err("stale schema overlay must not commit over a newer catalog");
        assert!(
            !observer.in_transaction(),
            "failed stale DDL commit must be fully aborted"
        );
        assert!(query_succeeds(
            &mut owner,
            "SELECT first_writer_at FROM sessions"
        ));
        assert!(!query_succeeds(
            &mut observer,
            "SELECT stale_writer_at FROM sessions"
        ));

        drop(owner);
        drop(observer);
        shutdown.store(true, Ordering::Release);
        worker
            .join()
            .expect("server thread joins")
            .expect("server stops cleanly");
    });

    let restart_config = test_config(data_dir);
    let restart_server = Server::bind_ephemeral(&restart_config).expect("bind restart server");
    let restart_address = restart_server.local_addr().expect("restart address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| restart_server.serve_one());
        let mut client = connect(restart_address, database);
        assert!(query_succeeds(
            &mut client,
            "SELECT absolute_expires_at FROM sessions"
        ));
        assert_eq!(
            count(
                &mut client,
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 2"
            ),
            1
        );
        assert_eq!(count(&mut client, "SELECT COUNT(*) FROM sessions"), 2);
        assert_eq!(
            count(
                &mut client,
                "SELECT COUNT(*) FROM sessions WHERE session_state = 'active'"
            ),
            2
        );
        assert_eq!(
            count(
                &mut client,
                "SELECT COUNT(*) FROM sessions WHERE cleanup_available_at IS NOT NULL"
            ),
            1
        );
        assert_eq!(
            count(
                &mut client,
                "SELECT COUNT(*) FROM sessions
                 WHERE label = 'pre-alter'
                   AND cleanup_available_at = TIMESTAMP '2026-08-08 00:00:00'
                   AND id = 0"
            ),
            1,
            "the staged composite index must remain correct after checkpoint/restart"
        );
        assert!(has_index(
            &mut client,
            "sessions",
            "sessions_cleanup_queue_idx"
        ));
        assert!(query_succeeds(
            &mut client,
            "SELECT first_writer_at FROM sessions"
        ));
        assert!(!query_succeeds(
            &mut client,
            "SELECT stale_writer_at FROM sessions"
        ));
        drop(client);
        worker
            .join()
            .expect("restart server joins")
            .expect("restart server stops cleanly");
    });
}

#[test]
fn r4_l05_messenger_contracts_tcp_constraints_are_private_revalidated_and_durable() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database = "tcp_transactional_constraints";

    let config = test_config(data_dir.clone());
    let server = Server::bind_ephemeral(&config).expect("bind server");
    let address = server.local_addr().expect("server address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut owner = connect(address, database);
        let mut observer = connect(address, database);

        assert_command(
            owner
                .execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
                .expect("create parents"),
        );
        assert_command(
            owner
                .execute("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER)")
                .expect("create children"),
        );
        assert_command(
            owner
                .execute("INSERT INTO parents VALUES (1)")
                .expect("parent"),
        );
        assert_command(
            owner
                .execute("INSERT INTO children VALUES (1, 1)")
                .expect("valid child"),
        );

        // A staged constraint is enforced by its owner but remains invisible to
        // other sessions and disappears completely on rollback.
        owner.begin().expect("begin private CHECK");
        assert_command(
            owner
                .execute("ALTER TABLE children ADD CONSTRAINT CHECK(parent_id > 0)")
                .expect("stage private CHECK"),
        );
        owner
            .execute("INSERT INTO children VALUES (2, -1)")
            .expect_err("owner must use private CHECK schema");
        assert_command(
            observer
                .execute("INSERT INTO children VALUES (2, -1)")
                .expect("observer still uses committed schema"),
        );
        owner.rollback().expect("rollback private CHECK");
        assert_eq!(
            count(
                &mut observer,
                "SELECT COUNT(*) FROM children WHERE parent_id = -1"
            ),
            1
        );
        assert_command(
            observer
                .execute("DELETE FROM children WHERE id = 2")
                .expect("remove rollback witness"),
        );

        // Statement-time validation is insufficient: a concurrent committed
        // orphan must make the DDL commit fail rather than publish a stale FK.
        owner.begin().expect("begin conflicting FK");
        assert_command(
            owner
                .execute(
                    "ALTER TABLE children ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parents(id)",
                )
                .expect("stage FK"),
        );
        assert_command(
            observer
                .execute("INSERT INTO children VALUES (3, 999)")
                .expect("concurrent orphan before FK publication"),
        );
        owner
            .commit()
            .expect_err("commit-time FK revalidation must reject the orphan");
        assert!(!owner.in_transaction());

        // The failed commit must not leak either schema metadata or its backing
        // index. The old schema still admits another orphan.
        assert_command(
            observer
                .execute("INSERT INTO children VALUES (4, 999)")
                .expect("failed FK commit published nothing"),
        );
        assert_command(
            observer
                .execute("DELETE FROM children WHERE parent_id = 999")
                .expect("remove conflicting rows"),
        );

        owner.begin().expect("begin successful FK");
        assert_command(
            owner
                .execute(
                    "ALTER TABLE children ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE RESTRICT",
                )
                .expect("stage durable FK"),
        );
        owner.commit().expect("commit durable FK");
        observer
            .execute("INSERT INTO children VALUES (5, 999)")
            .expect_err("committed FK must reject orphan");
        assert_command(
            observer
                .execute("INSERT INTO children VALUES (5, 1)")
                .expect("committed FK accepts parent"),
        );

        drop(owner);
        drop(observer);
        shutdown.store(true, Ordering::Release);
        worker
            .join()
            .expect("server thread joins")
            .expect("server stops cleanly");
    });

    let restart_config = test_config(data_dir);
    let restart_server = Server::bind_ephemeral(&restart_config).expect("bind restart server");
    let restart_address = restart_server.local_addr().expect("restart address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| restart_server.serve_one());
        let mut client = connect(restart_address, database);
        client
            .execute("INSERT INTO children VALUES (6, 999)")
            .expect_err("replayed FK must reject orphan");
        assert_command(
            client
                .execute("INSERT INTO children VALUES (6, 1)")
                .expect("replayed FK accepts parent"),
        );
        drop(client);
        worker
            .join()
            .expect("restart thread joins")
            .expect("restart server stops cleanly");
    });
}

#[test]
fn rdb_0030_tcp_migration_publishes_final_index_generation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database = "rdb_0030_tcp";
    let config = test_config(data_dir);

    {
        let server = Server::bind_ephemeral(&config).expect("bind seed server");
        let address = server.local_addr().expect("seed server address");
        let shutdown = AtomicBool::new(false);
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.run_until(&shutdown));
            let mut client = connect(address, database);
            assert_command(
                client
                    .execute(
                        "CREATE TABLE egress_proxy_endpoints (
                            id UUID PRIMARY KEY,
                            profile_id UUID NOT NULL,
                            position INTEGER NOT NULL CHECK (position >= 1)
                        )",
                    )
                    .expect("create endpoint table"),
            );
            for position in 1..=24 {
                assert_command(
                    client
                        .execute(format!(
                            "INSERT INTO egress_proxy_endpoints VALUES (
                                '{}',
                                '018f2b34-7a10-7cc2-8f3a-9d4b5c6d7000',
                                {position}
                            )",
                            rdb_0030_endpoint_id(position)
                        ))
                        .expect("seed endpoint"),
                );
            }
            let checkpoint = client
                .execute("PRAGMA CHECKPOINT")
                .expect("checkpoint seed");
            consume_success(&mut client, checkpoint);
            drop(client);
            shutdown.store(true, Ordering::Release);
            worker
                .join()
                .expect("seed server joins")
                .expect("seed server stops");
        });
    }

    {
        let server = Server::bind_ephemeral(&config).expect("bind migration server");
        let address = server.local_addr().expect("migration server address");
        let shutdown = AtomicBool::new(false);
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.run_until(&shutdown));
            let mut client = connect(address, database);
            client.begin().expect("begin RDB-0030 migration");
            assert_command(
                client
                    .execute(
                        "ALTER TABLE egress_proxy_endpoints
                         ADD COLUMN sort_key INTEGER NOT NULL DEFAULT 1
                         CHECK (sort_key >= 1)",
                    )
                    .expect("add sort_key"),
            );
            assert_command(
                client
                    .execute("UPDATE egress_proxy_endpoints SET sort_key = position")
                    .expect("backfill sort_key"),
            );
            assert_command(
                client
                    .execute(
                        "CREATE INDEX egress_proxy_endpoint_sort_idx
                         ON egress_proxy_endpoints (profile_id, sort_key, id)",
                    )
                    .expect("stage final-view index"),
            );
            client
                .commit()
                .expect("commit RDB-0030 migration without uncertain durability");
            assert_eq!(
                count(
                    &mut client,
                    "SELECT COUNT(*) FROM egress_proxy_endpoints
                     WHERE sort_key = position",
                ),
                24
            );
            assert!(has_index(
                &mut client,
                "egress_proxy_endpoints",
                "egress_proxy_endpoint_sort_idx"
            ));
            let checkpoint = client
                .execute("PRAGMA CHECKPOINT")
                .expect("checkpoint migration");
            consume_success(&mut client, checkpoint);
            drop(client);
            shutdown.store(true, Ordering::Release);
            worker
                .join()
                .expect("migration server joins")
                .expect("migration server stops");
        });
    }

    {
        let server = Server::bind_ephemeral(&config).expect("bind verification server");
        let address = server.local_addr().expect("verification server address");
        let shutdown = AtomicBool::new(false);
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.run_until(&shutdown));
            let mut client = connect(address, database);
            assert_eq!(
                count(
                    &mut client,
                    "SELECT COUNT(*) FROM egress_proxy_endpoints
                     WHERE sort_key = position",
                ),
                24
            );
            assert!(has_index(
                &mut client,
                "egress_proxy_endpoints",
                "egress_proxy_endpoint_sort_idx"
            ));
            drop(client);
            shutdown.store(true, Ordering::Release);
            worker
                .join()
                .expect("verification server joins")
                .expect("verification server stops");
        });
    }
}

#[test]
fn orm_01_tcp_drop_constraint_is_private_cached_and_durable() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database = "tcp_constraint_drop";
    let config = test_config(data_dir.clone());

    let server = Server::bind_ephemeral(&config).expect("bind server");
    let address = server.local_addr().expect("server address");
    let shutdown = AtomicBool::new(false);
    thread::scope(|scope| {
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut owner = connect(address, database);
        let mut observer = connect(address, database);

        assert_command(
            owner
                .execute(
                    "CREATE TABLE guarded (
                        id INTEGER PRIMARY KEY,
                        code TEXT UNIQUE,
                        score INTEGER CHECK (score >= 0)
                    )",
                )
                .expect("create guarded"),
        );
        assert_command(
            owner
                .execute("INSERT INTO guarded VALUES (1, 'same', 1)")
                .expect("seed guarded"),
        );
        let prepared = observer
            .prepare("INSERT INTO guarded VALUES ($1, $2, $3)")
            .expect("prepare before DDL");

        owner.begin().expect("begin rollback drop");
        assert_command(
            owner
                .execute("ALTER TABLE guarded DROP CONSTRAINT uq_guarded_code")
                .expect("stage UNIQUE drop"),
        );
        assert_command(
            owner
                .execute("ALTER TABLE guarded DROP CONSTRAINT chk_guarded_1")
                .expect("stage CHECK drop"),
        );
        assert_command(
            owner
                .execute("INSERT INTO guarded VALUES (2, 'same', -1)")
                .expect("owner uses private dropped constraints"),
        );
        observer
            .execute("INSERT INTO guarded VALUES (3, 'same', 1)")
            .expect_err("observer still uses committed UNIQUE");
        owner.rollback().expect("rollback drops");
        assert_eq!(count(&mut observer, "SELECT COUNT(*) FROM guarded"), 1);

        owner.begin().expect("begin committed drop");
        assert_command(
            owner
                .execute("ALTER TABLE guarded DROP CONSTRAINT uq_guarded_code")
                .expect("stage durable UNIQUE drop"),
        );
        assert_command(
            owner
                .execute("ALTER TABLE guarded DROP CONSTRAINT chk_guarded_1")
                .expect("stage durable CHECK drop"),
        );
        owner.commit().expect("commit drops");

        let result = observer
            .execute_prepared(
                &prepared,
                vec![
                    WireValue::Int(2),
                    WireValue::String("same".into()),
                    WireValue::Int(-2),
                ],
            )
            .expect("prepared statement rebinds after schema generation change");
        consume_success(&mut observer, result);
        observer.close_prepared(prepared).expect("close prepared");

        drop(owner);
        drop(observer);
        shutdown.store(true, Ordering::Release);
        worker.join().unwrap().unwrap();
    });

    let server = Server::bind_ephemeral(&test_config(data_dir)).expect("bind restart server");
    let address = server.local_addr().expect("restart address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| server.serve_one());
        let mut client = connect(address, database);
        assert_command(
            client
                .execute("INSERT INTO guarded VALUES (3, 'same', -3)")
                .expect("dropped constraints remain absent after replay"),
        );
        assert_eq!(count(&mut client, "SELECT COUNT(*) FROM guarded"), 3);
        drop(client);
        worker.join().unwrap().unwrap();
    });
}

#[test]
fn rdb_0031_tcp_cold_parent_non_key_update_preserves_foreign_key() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database = "rdb_0031_tcp";
    let config = test_config(data_dir);

    {
        let server = Server::bind_ephemeral(&config).expect("bind seed server");
        let address = server.local_addr().expect("seed server address");
        let shutdown = AtomicBool::new(false);
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.run_until(&shutdown));
            let mut client = connect(address, database);
            assert_command(
                client
                    .execute(
                        "CREATE TABLE egress_proxy_profiles (
                            id UUID PRIMARY KEY,
                            profile TEXT NOT NULL UNIQUE,
                            revision INTEGER NOT NULL
                        )",
                    )
                    .expect("create profiles"),
            );
            assert_command(
                client
                    .execute(
                        "CREATE TABLE egress_proxy_endpoints (
                            id UUID PRIMARY KEY,
                            profile_id UUID NOT NULL REFERENCES egress_proxy_profiles(id),
                            position INTEGER NOT NULL,
                            sort_key INTEGER NOT NULL,
                            UNIQUE (profile_id, position)
                        )",
                    )
                    .expect("create endpoints"),
            );
            assert_command(
                client
                    .execute(
                        "CREATE INDEX egress_proxy_endpoint_sort_idx
                         ON egress_proxy_endpoints(profile_id, sort_key, id)",
                    )
                    .expect("create sort index"),
            );
            assert_command(
                client
                    .execute(
                        "INSERT INTO egress_proxy_profiles VALUES
                         ('01a00508-7a7f-72c2-a225-30b4d55cc1fc', 'link_previews', 1)",
                    )
                    .expect("seed profile"),
            );
            for position in 1..=8 {
                assert_command(
                    client
                        .execute(format!(
                            "INSERT INTO egress_proxy_endpoints VALUES
                             ('{}', '01a00508-7a7f-72c2-a225-30b4d55cc1fc',
                              {position}, {position})",
                            rdb_0030_endpoint_id(position)
                        ))
                        .expect("seed endpoint"),
                );
            }
            let checkpoint = client
                .execute("PRAGMA CHECKPOINT")
                .expect("checkpoint seed");
            consume_success(&mut client, checkpoint);
            drop(client);
            shutdown.store(true, Ordering::Release);
            worker.join().unwrap().unwrap();
        });
    }

    {
        let server = Server::bind_ephemeral(&config).expect("bind mutation server");
        let address = server.local_addr().expect("mutation server address");
        let shutdown = AtomicBool::new(false);
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.run_until(&shutdown));
            let mut client = connect(address, database);
            client.begin().expect("begin proxy mutation");

            let mut insert = BTreeMap::new();
            insert.insert("id".to_string(), WireValue::Uuid(rdb_0031_child_id(9)));
            insert.insert(
                "profile_id".to_string(),
                WireValue::Uuid(RDB_0031_PARENT_ID),
            );
            assert_command(
                client
                    .execute_with_parameters(
                        "INSERT INTO egress_proxy_endpoints
                         VALUES (:id, :profile_id, 1000001, 1000001)",
                        insert,
                    )
                    .expect("insert endpoint"),
            );

            let mut update = BTreeMap::new();
            update.insert(
                "profile_id".to_string(),
                WireValue::Uuid(RDB_0031_PARENT_ID),
            );
            update.insert("revision".to_string(), WireValue::Int(1));
            assert_command(
                client
                    .execute_with_parameters(
                        "UPDATE egress_proxy_profiles SET revision = revision + 1
                         WHERE id = :profile_id AND revision = :revision",
                        update,
                    )
                    .expect("update non-key parent revision"),
            );
            assert_eq!(
                count(
                    &mut client,
                    "SELECT COUNT(*) FROM egress_proxy_endpoints
                     WHERE profile_id = '01a00508-7a7f-72c2-a225-30b4d55cc1fc'",
                ),
                9
            );
            client
                .commit()
                .expect("unchanged parent identity must commit");
            assert_eq!(
                count(
                    &mut client,
                    "SELECT revision FROM egress_proxy_profiles
                     WHERE profile = 'link_previews'",
                ),
                2
            );
            let checkpoint = client
                .execute("PRAGMA CHECKPOINT")
                .expect("checkpoint mutation");
            consume_success(&mut client, checkpoint);
            drop(client);
            shutdown.store(true, Ordering::Release);
            worker.join().unwrap().unwrap();
        });
    }

    {
        let server = Server::bind_ephemeral(&config).expect("bind verification server");
        let address = server.local_addr().expect("verification server address");
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.serve_one());
            let mut client = connect(address, database);
            assert_eq!(
                count(&mut client, "SELECT COUNT(*) FROM egress_proxy_endpoints",),
                9
            );
            assert_eq!(
                count(
                    &mut client,
                    "SELECT revision FROM egress_proxy_profiles
                     WHERE profile = 'link_previews'",
                ),
                2
            );
            drop(client);
            worker.join().unwrap().unwrap();
        });
    }
}

#[test]
fn orm_02_tcp_describe_json_preserves_canonical_descriptor_bytes() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database = "orm_02_descriptor";
    let server = Server::bind_ephemeral(&test_config(data_dir)).expect("bind server");
    let address = server.local_addr().expect("server address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut client = connect(address, database);
        assert_command(
            client
                .execute(
                    "CREATE TABLE people (
                        id UUID PRIMARY KEY,
                        external_id TEXT UNIQUE NOT NULL,
                        name TEXT CHECK (name <> '')
                    )",
                )
                .expect("create table"),
        );

        let first = single_text(&mut client, "DESCRIBE TABLE people FORMAT JSON");
        let second = single_text(&mut client, "DESCRIBE TABLE people FORMAT JSON");
        assert_eq!(
            first, second,
            "unchanged catalog must serialize byte-identically"
        );

        let descriptor =
            DescriptorEnvelope::<TableDescriptor>::from_json(&first, DescriptorKind::Table)
                .expect("decode TCP descriptor");
        assert_eq!(descriptor.to_json().unwrap(), first);
        assert_eq!(
            descriptor.payload.computed_fingerprint().unwrap(),
            descriptor.payload.fingerprint
        );
        assert_eq!(descriptor.payload.name, "people");
        assert_eq!(descriptor.payload.constraints.len(), 3);

        drop(client);
        shutdown.store(true, Ordering::Release);
        worker.join().unwrap().unwrap();
    });
}

#[test]
fn orm_02_embedded_and_tcp_publish_identical_descriptor_bytes_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database = "orm_02_transport_parity";
    let database_path = data_dir.join("databases").join(database);
    let dsn = format!("file://{}", database_path.display());

    let embedded_json = {
        let db = Database::open(&dsn).expect("open embedded fixture");
        db.execute(
            "CREATE TABLE people (
                id UUID PRIMARY KEY,
                external_id TEXT UNIQUE NOT NULL,
                active BOOLEAN DEFAULT TRUE CHECK (active = TRUE),
                score DECIMAL(18, 4)
            )",
            (),
        )
        .expect("create embedded fixture");
        db.execute(
            "CREATE INDEX idx_people_active ON people(active) WHERE active = TRUE",
            (),
        )
        .expect("create partial index");
        let json = db
            .query_one::<String, _>("DESCRIBE TABLE people FORMAT JSON", ())
            .expect("embedded descriptor");
        db.execute("PRAGMA CHECKPOINT", ())
            .expect("checkpoint descriptor fixture");
        db.close().expect("close embedded fixture");
        json
    };

    let server = Server::bind_ephemeral(&test_config(data_dir)).expect("bind server");
    let address = server.local_addr().expect("server address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| server.serve_one());
        let mut client = connect(address, database);
        let tcp_json = single_text(&mut client, "DESCRIBE TABLE people FORMAT JSON");
        assert_eq!(tcp_json, embedded_json);
        let before =
            DescriptorEnvelope::<TableDescriptor>::from_json(&tcp_json, DescriptorKind::Table)
                .expect("decode pre-alter descriptor")
                .payload;
        assert_command(
            client
                .execute("ALTER TABLE people ADD COLUMN note TEXT")
                .expect("alter schema"),
        );
        let after_json = single_text(&mut client, "DESCRIBE TABLE people FORMAT JSON");
        let after =
            DescriptorEnvelope::<TableDescriptor>::from_json(&after_json, DescriptorKind::Table)
                .expect("decode post-alter descriptor")
                .payload;
        assert_ne!(after.schema_generation, before.schema_generation);
        assert_ne!(after.fingerprint, before.fingerprint);
        assert!(
            radixdb_orm::ensure_schema_fingerprint(&before.fingerprint, &after.fingerprint)
                .is_err()
        );
        drop(client);
        worker.join().unwrap().unwrap();
    });
}

#[test]
fn orm_mixed_01_tcp_sync_raw_orm_raw_share_transaction_and_connection() {
    let temp = tempfile::tempdir().expect("temp dir");
    let server =
        Server::bind_ephemeral(&test_config(temp.path().join("data"))).expect("bind server");
    let address = server.local_addr().expect("server address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut client = connect(address, "orm_mixed_sync");
        client
            .schema()
            .create_table("orm_mixed")
            .column(radixdb_orm::Column::integer("id").primary_key(true))
            .column(radixdb_orm::Column::text("name"))
            .execute()
            .expect("create through bound ORM schema API");

        client.begin().expect("begin shared transaction");
        assert_command(
            client
                .execute("INSERT INTO orm_mixed VALUES (1, 'raw-before')")
                .expect("raw insert"),
        );

        let entity = client.entity("orm_mixed").expect("dynamic entity");
        let query = entity
            .query()
            .select([entity.column("name").unwrap().expr()])
            .filter(entity.column("id").unwrap().eq(1_i64));
        let result = query.fetch(&mut client).expect("ORM query in transaction");
        let ExecuteResult::Cursor(cursor) = result else {
            panic!("ORM query must return cursor")
        };
        let batch = client.fetch(&cursor).expect("fetch ORM query");
        assert_eq!(
            batch.rows[0].values,
            vec![WireValue::String("raw-before".to_string())]
        );

        let mut record = DynamicRecord::new(entity.descriptor().clone());
        record.set("id", TypedValue::Integer(2)).unwrap();
        record
            .set("name", TypedValue::Text("orm-middle".to_string()))
            .unwrap();
        record.insert(&mut client).expect("ORM record insert");
        assert!(!record.is_dirty());

        let repeated = |id: i64| {
            entity
                .query()
                .select([entity.column("name").unwrap().expr()])
                .filter(entity.column("id").unwrap().eq(id))
        };
        let first = radixdb_orm::OrmBuilder::to_sql(&repeated(1)).unwrap();
        let second = radixdb_orm::OrmBuilder::to_sql(&repeated(2)).unwrap();
        assert_eq!(first.sql, second.sql);
        assert_eq!(first.shape_fingerprint, second.shape_fingerprint);
        let prepared = client.prepare(&first.sql).expect("prepare generated SQL");
        for (id, expected) in [(1, "raw-before"), (2, "orm-middle")] {
            let ExecuteResult::Cursor(cursor) = client
                .execute_prepared(&prepared, vec![WireValue::Int(id)])
                .expect("execute repeated ORM shape")
            else {
                panic!("prepared ORM SELECT must return cursor")
            };
            let batch = client.fetch(&cursor).expect("fetch prepared ORM shape");
            assert_eq!(
                batch.rows[0].values,
                vec![WireValue::String(expected.to_string())]
            );
        }
        client
            .close_prepared(prepared)
            .expect("close generated prepared statement");
        assert_eq!(count(&mut client, "SELECT COUNT(*) FROM orm_mixed"), 2);
        client.rollback().expect("rollback shared transaction");
        assert_eq!(count(&mut client, "SELECT COUNT(*) FROM orm_mixed"), 0);

        let query = radixdb_orm::QueryBuilder::from_relation(radixdb_orm::table("orm_mixed"))
            .select([radixdb_orm::Expr::column("id")]);
        let ExecuteResult::Cursor(cursor) = client
            .execute_orm(&radixdb_orm::OrmBuilder::document(&query).unwrap())
            .expect("open ORM cursor")
        else {
            panic!("ORM query must return cursor")
        };
        assert!(
            client.execute("SELECT 1").is_err(),
            "ORM must preserve the transport commands-in-sync guard"
        );
        client.cancel(cursor).expect("cancel ORM cursor");
        assert_eq!(count(&mut client, "SELECT COUNT(*) FROM orm_mixed"), 0);

        let ExecuteResult::Cursor(_abandoned) = client
            .execute_orm(&radixdb_orm::OrmBuilder::document(&query).unwrap())
            .expect("open abandoned ORM cursor")
        else {
            panic!("ORM query must return cursor")
        };
        drop(client);
        let mut replacement = connect(address, "orm_mixed_sync");
        assert_eq!(count(&mut replacement, "SELECT COUNT(*) FROM orm_mixed"), 0);

        drop(replacement);
        shutdown.store(true, Ordering::Release);
        worker.join().unwrap().unwrap();
    });
}

#[cfg(feature = "orm-async-tests")]
#[test]
fn orm_mixed_01_tcp_async_raw_orm_raw_share_transaction_and_connection() {
    use radixdb_client::{AsyncConnection, AsyncTimeouts};

    let temp = tempfile::tempdir().expect("temp dir");
    let server =
        Server::bind_ephemeral(&test_config(temp.path().join("data"))).expect("bind server");
    let address = server.local_addr().expect("server address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Tokio runtime");
        runtime.block_on(async {
            let mut client = AsyncConnection::connect(address, AsyncTimeouts::default())
                .await
                .expect("async connect");
            client
                .authenticate("root", None)
                .await
                .expect("authenticate");
            client
                .select_database("orm_mixed_async")
                .await
                .expect("select database");
            client
                .schema()
                .create_table("orm_mixed")
                .column(radixdb_orm::Column::integer("id").primary_key(true))
                .column(radixdb_orm::Column::text("name"))
                .execute()
                .await
                .expect("create through async ORM schema API");

            client.begin().await.expect("begin shared transaction");
            assert!(matches!(
                client
                    .execute("INSERT INTO orm_mixed VALUES (1, 'raw-before')")
                    .await
                    .expect("raw insert"),
                ExecuteResult::CommandComplete { .. }
            ));

            let entity = client.entity("orm_mixed").await.expect("dynamic entity");
            let query = entity
                .query()
                .select([entity.column("name").unwrap().expr()])
                .filter(entity.column("id").unwrap().eq(1_i64));
            let result = query
                .fetch_async(&mut client)
                .await
                .expect("ORM query in transaction");
            let ExecuteResult::Cursor(cursor) = result else {
                panic!("ORM query must return cursor")
            };
            let batch = client.fetch(&cursor).await.expect("fetch ORM query");
            assert_eq!(
                batch.rows[0].values,
                vec![WireValue::String("raw-before".to_string())]
            );

            let mut record = DynamicRecord::new(entity.descriptor().clone());
            record.set("id", TypedValue::Integer(2)).unwrap();
            record
                .set("name", TypedValue::Text("orm-middle".to_string()))
                .unwrap();
            record
                .insert_async(&mut client)
                .await
                .expect("async ORM record insert");
            assert!(!record.is_dirty());

            let ExecuteResult::Cursor(cursor) = client
                .execute("SELECT COUNT(*) FROM orm_mixed")
                .await
                .expect("open count cursor")
            else {
                panic!("count query did not return a cursor")
            };
            let batch = client.fetch(&cursor).await.expect("fetch count");
            assert_eq!(batch.rows[0].values, vec![WireValue::Int(2)]);

            client
                .rollback()
                .await
                .expect("rollback shared transaction");
            let ExecuteResult::Cursor(cursor) = client
                .execute("SELECT COUNT(*) FROM orm_mixed")
                .await
                .expect("open post-rollback count")
            else {
                panic!("count query did not return a cursor")
            };
            let batch = client
                .fetch(&cursor)
                .await
                .expect("fetch post-rollback count");
            assert_eq!(batch.rows[0].values, vec![WireValue::Int(0)]);

            let query = radixdb_orm::QueryBuilder::from_relation(radixdb_orm::table("orm_mixed"))
                .select([radixdb_orm::Expr::column("id")]);
            let ExecuteResult::Cursor(cursor) = query
                .fetch_async(&mut client)
                .await
                .expect("open async ORM cursor")
            else {
                panic!("ORM query must return cursor")
            };
            assert!(
                client.execute("SELECT 1").await.is_err(),
                "async ORM must preserve commands-in-sync"
            );
            client
                .cancel(cursor)
                .await
                .expect("cancel async ORM cursor");
            let ExecuteResult::Cursor(cursor) = client
                .execute("SELECT COUNT(*) FROM orm_mixed")
                .await
                .expect("connection reusable after ORM cursor cancellation")
            else {
                panic!("count query did not return a cursor")
            };
            assert_eq!(
                client.fetch(&cursor).await.unwrap().rows[0].values,
                vec![WireValue::Int(0)]
            );
        });

        shutdown.store(true, Ordering::Release);
        worker.join().unwrap().unwrap();
    });
}
