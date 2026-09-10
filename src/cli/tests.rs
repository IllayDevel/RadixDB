use super::*;
use chrono::{TimeZone, Utc};
use clap::CommandFactory;
use std::str::FromStr;

#[test]
fn snapshot_help_publishes_an_executable_identity_format_and_database_scope() {
    const SNAPSHOT_ID: &str = "0123456789abcdef0123456789abcdef";
    let help = Args::command().render_long_help().to_string();
    assert!(help.contains(SNAPSHOT_ID));
    assert!(help.contains("Complete database snapshots to retain"));
    assert!(help.contains("ID printed by --snapshot"));
    crate::storage::v6::SnapshotId::from_str(SNAPSHOT_ID)
        .expect("the snapshot ID printed in help must be accepted by storage");

    let args = Args::try_parse_from([
        "radixdb-cli",
        "--db",
        "file:///tmp/radixdb-help-contract",
        "--restore",
        SNAPSHOT_ID,
    ])
    .expect("the published restore command must parse");
    assert_eq!(args.restore.as_deref(), Some(SNAPSHOT_ID));
    validate_cli_args(&args).expect("the published restore command must be admissible");
}

fn collect_query_rows(database: &Database, sql: &str) -> Vec<crate::api::ResultRow> {
    database
        .query(sql, ())
        .unwrap()
        .collect::<crate::Result<Vec<_>>>()
        .unwrap()
}

fn assert_window_partition_contract(database: &Database) {
    let ranked = collect_query_rows(
            database,
            "SELECT id, department_id, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC, id) AS position \
             FROM employees ORDER BY id",
        );
    assert_eq!(ranked.len(), 4);
    assert_eq!(
        ranked
            .iter()
            .map(|row| row.get::<i64>(0).unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        ranked
            .iter()
            .map(|row| row.get::<i64>(2).unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 1, 1]
    );

    let aggregate = collect_query_rows(
        database,
        "SELECT id, SUM(salary) OVER (PARTITION BY department_id) AS department_salary \
             FROM employees ORDER BY id",
    );
    assert_eq!(aggregate.len(), 4);
    assert_eq!(aggregate[3].get::<i64>(1).unwrap(), 70);

    let filtered = collect_query_rows(
            database,
            "WITH ranked AS (SELECT id, department_id, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC, id) AS position FROM employees) \
             SELECT id FROM ranked WHERE position = 1 ORDER BY id",
        );
    assert_eq!(
        filtered
            .iter()
            .map(|row| row.get::<i64>(0).unwrap())
            .collect::<Vec<_>>(),
        vec![1, 3, 4]
    );

    let page = collect_query_rows(
            database,
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC, id) AS position \
             FROM employees ORDER BY department_id NULLS LAST, position LIMIT 2 OFFSET 2",
        );
    assert_eq!(
        page.iter()
            .map(|row| row.get::<i64>(0).unwrap())
            .collect::<Vec<_>>(),
        vec![3, 4]
    );

    let nulls_first = collect_query_rows(
            database,
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC, id) AS position \
             FROM employees ORDER BY department_id NULLS FIRST, position",
        );
    assert_eq!(nulls_first[0].get::<i64>(0).unwrap(), 4);
}

fn create_window_fixture(database: &Database, index_sql: &str) {
    database
            .execute(
                &format!(
                    "CREATE TABLE employees (id INTEGER PRIMARY KEY, department_id INTEGER, name TEXT NOT NULL, salary INTEGER NOT NULL); \
                     {index_sql} \
                     INSERT INTO employees VALUES (1, 1, 'Alice', 120), (2, 1, 'Boris', 90), (3, 2, 'Clara', 80), (4, NULL, 'Dan', 70)"
                ),
                (),
            )
            .unwrap();
}

fn create_storage_generation_fixture(root: &Path) {
    for member in RESETTABLE_STORAGE_MEMBERS {
        let path = root.join(member);
        if member.starts_with("CONTROL.") {
            std::fs::write(path, member).unwrap();
        } else {
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(path.join("owned"), member).unwrap();
        }
    }
}

#[test]
fn r8_l01_batch_j_cli_bounds_statement_and_streams_display_window() {
    let mut statement = String::new();
    append_cli_statement_with_limit(&mut statement, "SELECT", 12).unwrap();
    assert_eq!(statement, "SELECT");
    assert!(append_cli_statement_with_limit(&mut statement, "123456", 12).is_err());
    assert_eq!(statement, "SELECT", "rejected line must not mutate buffer");

    let db = Database::open_in_memory().unwrap();
    let rows = db
        .query("SELECT value FROM generate_series(1, 20) AS value", ())
        .unwrap();
    let (retained, total) = collect_cli_rows(rows, 6).unwrap();
    assert_eq!(total, 20);
    assert_eq!(retained.len(), 6);
    let values: Vec<i64> = retained
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Integer(value)] => *value,
            other => panic!("unexpected generated row: {other:?}"),
        })
        .collect();
    assert_eq!(values, vec![1, 2, 3, 18, 19, 20]);
}

#[test]
fn r6_l02_cli_lexical_scalar_contract_has_single_typed_owner() {
    let input = "SELECT 'a;''b';\n\n/* outer ; /* inner ; */ end */ SELECT/*gap*/2;\n-- comment ;\nSELECT 3;";
    let statements = split_sql_statements(input).expect("core lexer splits batch");
    assert_eq!(statements.len(), 3);
    for statement in &statements {
        assert_eq!(parse_sql(statement).unwrap().len(), 1);
    }
    assert!(!sql_input_is_complete("SELECT 'unterminated;"));
    assert!(!sql_input_is_complete("SELECT 1 -- ;"));
    assert!(sql_input_is_complete("SELECT ';' /* ; */; -- trailing"));

    assert_eq!(
        classify_cli_statement("-- leading comment\nWITH q AS (SELECT 1) SELECT * FROM q").unwrap(),
        CliStatementKind::Rows
    );
    assert_eq!(
        classify_cli_statement("INSERT INTO t VALUES (' RETURNING ')").unwrap(),
        CliStatementKind::Command
    );
    assert_eq!(
        classify_cli_statement("INSERT INTO t VALUES (1) RETURNING id").unwrap(),
        CliStatementKind::Rows
    );
    assert_eq!(
        classify_cli_statement("/* transaction */ BEGIN TRANSACTION").unwrap(),
        CliStatementKind::Begin(None)
    );

    assert!(reject_legacy_cli_params("SELECT $1 -- PARAMS: 1e3").is_err());
    assert!(reject_legacy_cli_params("SELECT '-- PARAMS: text'").is_ok());

    let integer = value_to_json(&Value::Integer(i64::MAX));
    assert_eq!(integer["type"], "INTEGER");
    assert_eq!(integer["value"], i64::MAX.to_string());
    let nan = value_to_json(&Value::Float(f64::NAN));
    assert_eq!(nan["type"], "FLOAT");
    assert_eq!(nan["value"], "NaN");
    assert!(nan["bits"].is_string());
    let timestamp = value_to_json(&Value::timestamp(
        Utc.timestamp_opt(1, 234_567_890).single().unwrap(),
    ));
    assert_eq!(timestamp["nanos_since_unix_epoch_utc"], "1234567890");
    let json = value_to_json(&Value::try_json(r#"{"n":1}"#).unwrap());
    assert_eq!(json["value"]["n"], 1);
    assert_eq!(
        value_to_json(&Value::bytes(vec![0, 255]))["raw_hex"],
        "00ff"
    );
    let decimal = value_to_json(&Value::try_decimal(100, 3, 2).unwrap());
    assert_eq!(decimal["unscaled"], "100");
    assert_eq!(decimal["scale"], 2);
}

#[test]
fn r12_batch_a_nullable_indexed_window_partition_survives_reopen() {
    let memory = Database::open_in_memory().unwrap();
    create_window_fixture(
        &memory,
        "CREATE INDEX employees_department_idx ON employees (department_id) USING BTREE;",
    );
    assert_window_partition_contract(&memory);

    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("window-null");
    let dsn = format!(
        "file://{}?sync_mode=full&checkpoint_interval=0",
        database_path.display()
    );

    {
        let database = Database::open(&dsn).unwrap();
        create_window_fixture(
            &database,
            "CREATE INDEX employees_department_idx ON employees (department_id) USING BTREE;",
        );
        assert_window_partition_contract(&database);
        database.close().unwrap();
    }

    let reopened = Database::open(&dsn).unwrap();
    assert_window_partition_contract(&reopened);

    let mut snapshot = reopened
        .begin_with_isolation(crate::IsolationLevel::SnapshotIsolation)
        .unwrap();
    let writer = reopened.clone();
    writer
        .execute("DELETE FROM employees WHERE id = 4", ())
        .unwrap();
    let snapshot_rows = snapshot
            .query(
                "SELECT id, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC, id) FROM employees ORDER BY id",
                (),
            )
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
    assert_eq!(snapshot_rows.len(), 4, "snapshot lost the NULL partition");
    snapshot.rollback().unwrap();

    assert_eq!(
            collect_query_rows(
                &reopened,
                "SELECT id, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC, id) FROM employees ORDER BY id",
            )
            .len(),
            3,
            "committed tombstone was ignored"
        );
    reopened
        .execute("INSERT INTO employees VALUES (5, NULL, 'Eva', 60)", ())
        .unwrap();
    assert_eq!(
            collect_query_rows(
                &reopened,
                "SELECT id, ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC, id) FROM employees ORDER BY id",
            )
            .len(),
            4,
            "hot NULL row was lost beside cold partitions"
        );
    reopened.close().unwrap();

    for (name, index_sql) in [
            ("scan", ""),
            (
                "fk-index",
                "CREATE TABLE departments (id INTEGER PRIMARY KEY); INSERT INTO departments VALUES (1), (2);",
            ),
        ] {
            let path = directory.path().join(name);
            let dsn = format!(
                "file://{}?sync_mode=full&checkpoint_interval=0",
                path.display()
            );
            {
                let database = Database::open(&dsn).unwrap();
                if name == "fk-index" {
                    database.execute(index_sql, ()).unwrap();
                    database
                        .execute(
                            "CREATE TABLE employees (id INTEGER PRIMARY KEY, department_id INTEGER REFERENCES departments(id), name TEXT NOT NULL, salary INTEGER NOT NULL); \
                             INSERT INTO employees VALUES (1, 1, 'Alice', 120), (2, 1, 'Boris', 90), (3, 2, 'Clara', 80), (4, NULL, 'Dan', 70)",
                            (),
                        )
                        .unwrap();
                } else {
                    create_window_fixture(&database, index_sql);
                }
                database.close().unwrap();
            }
            let database = Database::open(&dsn).unwrap();
            assert_window_partition_contract(&database);
            database.close().unwrap();
        }
}

#[test]
fn r12_batch_a_cli_preserves_begin_isolation_and_routes_savepoints() {
    let database = Database::open_in_memory().unwrap();
    database
        .execute(
            "CREATE TABLE cli_batch_a (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .unwrap();

    for level in ["SERIALIZABLE", "REPEATABLE READ", "READ UNCOMMITTED"] {
        let unsupported = execute_batch_input(
            &database,
            &format!("BEGIN ISOLATION LEVEL {level}; ROLLBACK;"),
            false,
            true,
            0,
            1_000,
        )
        .expect_err("unsupported isolation must not be silently weakened");
        assert!(
            unsupported.contains("unsupported isolation level"),
            "unexpected error for {level}: {unsupported}"
        );
    }

    execute_batch_input(
        &database,
        "BEGIN ISOLATION LEVEL SNAPSHOT; \
             INSERT INTO cli_batch_a VALUES (1, 'kept'); \
             SAVEPOINT before_second; \
             INSERT INTO cli_batch_a VALUES (2, 'discarded'); \
             ROLLBACK TO SAVEPOINT before_second; \
             RELEASE SAVEPOINT before_second; \
             COMMIT;",
        false,
        true,
        0,
        1_000,
    )
    .unwrap();
    assert_eq!(
        database
            .query_one::<i64, _>("SELECT COUNT(*) FROM cli_batch_a", ())
            .unwrap(),
        1
    );

    let mut interactive = Cli::new(database.clone(), true, 0, 1_000).unwrap();
    interactive
        .execute_query("BEGIN ISOLATION LEVEL SNAPSHOT")
        .unwrap();
    let transaction_id = interactive.tx.as_ref().unwrap().id();
    assert_eq!(
        database
            .engine()
            .registry()
            .get_isolation_level(transaction_id),
        crate::IsolationLevel::SnapshotIsolation
    );
    interactive.execute_query("SAVEPOINT interactive").unwrap();
    interactive
        .execute_query("ROLLBACK TO SAVEPOINT interactive")
        .unwrap();
    interactive
        .execute_query("RELEASE SAVEPOINT interactive")
        .unwrap();
    interactive.execute_query("ROLLBACK").unwrap();
}

#[test]
fn r12_batch_a_set_isolation_is_connection_local_and_immutable_in_transaction() {
    let database = Database::open_in_memory().unwrap();
    let peer = database.clone();

    database
        .execute("SET ISOLATION_LEVEL = 'SNAPSHOT'", ())
        .unwrap();
    assert_eq!(
        database.default_isolation_level().unwrap(),
        crate::IsolationLevel::SnapshotIsolation
    );
    assert_eq!(
        peer.default_isolation_level().unwrap(),
        crate::IsolationLevel::ReadCommitted
    );
    assert_eq!(
        database
            .query_one::<String, _>("PRAGMA ISOLATION_LEVEL", ())
            .unwrap(),
        "SNAPSHOT"
    );
    let mut configured = database.begin().unwrap();
    assert_eq!(
        database
            .engine()
            .registry()
            .get_isolation_level(configured.id()),
        crate::IsolationLevel::SnapshotIsolation
    );
    assert!(configured
        .execute("SET ISOLATION_LEVEL = 'SNAPSHOT'", ())
        .is_err());

    let peer_default = peer.begin().unwrap();
    assert_eq!(
        database
            .engine()
            .registry()
            .get_isolation_level(peer_default.id()),
        crate::IsolationLevel::ReadCommitted
    );
}

#[test]
fn r6_l02_cli_batch_has_one_fail_fast_transaction_outcome() {
    let db = Database::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE cli_batch (id INTEGER PRIMARY KEY, value TEXT)",
        (),
    )
    .unwrap();

    execute_batch_input(
        &db,
        "BEGIN; INSERT INTO cli_batch VALUES (1, 'committed'); COMMIT;",
        false,
        true,
        0,
        1_000,
    )
    .unwrap();
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM cli_batch", ())
            .unwrap(),
        1
    );

    let error = execute_batch_input(
            &db,
            "BEGIN; INSERT INTO cli_batch VALUES (2, 'rollback'); INSERT INTO cli_batch VALUES (1, 'duplicate'); INSERT INTO cli_batch VALUES (3, 'must-not-run'); COMMIT;",
            false,
            true,
            0,
            1_000,
        )
        .unwrap_err();
    assert!(
        error.to_ascii_lowercase().contains("constraint failed"),
        "unexpected duplicate-key error: {error}"
    );
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM cli_batch", ())
            .unwrap(),
        1
    );

    let error = execute_batch_input(
        &db,
        "BEGIN; INSERT INTO cli_batch VALUES (4, 'open');",
        false,
        true,
        0,
        1_000,
    )
    .unwrap_err();
    assert!(error.contains("open transaction"));
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM cli_batch WHERE id = 4", ())
            .unwrap(),
        0
    );

    let error = execute_batch_input(
            &db,
            "INSERT INTO cli_batch VALUES (5, 'prefix'); SELECT * FROM missing_table; INSERT INTO cli_batch VALUES (6, 'must-not-run');",
            false,
            true,
            0,
            1_000,
        )
        .unwrap_err();
    assert!(error.contains("missing_table"));
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM cli_batch WHERE id = 6", ())
            .unwrap(),
        0
    );

    execute_batch_input(
        &db,
        "BEGIN; SELECT COUNT(*) FROM cli_batch; ROLLBACK;",
        false,
        true,
        0,
        1_000,
    )
    .unwrap();
}

#[test]
fn r2_l06_batch_b_cli_durability_options_are_fail_closed() {
    for arguments in [
        [
            "radixdb-cli",
            "-d",
            "file:///tmp/r2-l06",
            "--profile",
            "typo",
        ],
        ["radixdb-cli", "-d", "file:///tmp/r2-l06", "--sync", "typo"],
        [
            "radixdb-cli",
            "-d",
            "file:///tmp/r2-l06",
            "--compression",
            "maybe",
        ],
    ] {
        let args = Args::try_parse_from(arguments).unwrap();
        assert!(build_dsn(&args).is_err());
    }
}

#[test]
fn r12_batch_f_sync_precedence_and_banner_use_effective_engine_config() {
    let cases = [
        (
            vec!["--sync", "none"],
            "none",
            "none (fastest, less durable)",
        ),
        (
            vec!["--profile", "fast", "--sync", "normal"],
            "normal",
            "normal (balanced)",
        ),
        (
            vec!["--profile", "durable"],
            "full",
            "full (slowest, most durable)",
        ),
    ];
    for (arguments, expected_mode, expected_description) in cases {
        let directory = tempfile::tempdir().unwrap();
        let base = format!(
            "file://{}?sync_mode=full&checkpoint_on_close=off",
            directory.path().join("database").display()
        );
        let mut argv = vec!["radixdb-cli", "-d", base.as_str()];
        argv.extend(arguments);
        let args = Args::try_parse_from(argv).unwrap();
        let dsn = build_dsn(&args).unwrap();
        assert!(dsn.contains(&format!("sync_mode={expected_mode}")));
        assert_eq!(dsn.matches("sync_mode=").count(), 1);

        let database = Database::open(&dsn).unwrap();
        assert_eq!(
            effective_sync_description(&database).unwrap(),
            expected_description
        );
        database.close().unwrap();
    }

    let directory = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?sync_mode=none&checkpoint_on_close=off",
        directory.path().join("database").display()
    );
    let args = Args::try_parse_from(["radixdb-cli", "-d", dsn.as_str()]).unwrap();
    let effective = build_dsn(&args).unwrap();
    assert_eq!(effective, dsn, "DSN wins when no profile or flag is set");
    let database = Database::open(&effective).unwrap();
    assert_eq!(
        effective_sync_description(&database).unwrap(),
        "none (fastest, less durable)"
    );
    database.close().unwrap();
}

#[test]
fn r2_l06_batch_b_reset_rolls_back_complete_generation_on_staging_failure() {
    let directory = tempfile::tempdir().unwrap();
    create_storage_generation_fixture(directory.path());

    let error = reset_storage_generation_with_hook(directory.path(), |staged| {
        if staged == 4 {
            Err(io::Error::other("injected generation staging failure"))
        } else {
            Ok(())
        }
    })
    .expect_err("reset must surface the staging failure");
    assert!(error.to_string().contains("injected"));
    for member in RESETTABLE_STORAGE_MEMBERS {
        assert!(directory.path().join(member).exists(), "missing {member}");
    }
    assert!(!std::fs::read_dir(directory.path())
        .unwrap()
        .flatten()
        .any(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with(".radixdb-reset-")));
}

#[test]
fn reset_storage_removes_current_generation_but_preserves_lock_and_snapshots() {
    let directory = tempfile::tempdir().unwrap();
    create_storage_generation_fixture(directory.path());
    std::fs::write(directory.path().join("LOCK"), b"owner").unwrap();
    std::fs::create_dir_all(directory.path().join("snapshots/retained")).unwrap();
    std::fs::write(
        directory.path().join("snapshots/retained/manifest"),
        b"snapshot",
    )
    .unwrap();

    reset_storage_generation(directory.path()).unwrap();

    for member in RESETTABLE_STORAGE_MEMBERS {
        assert!(!directory.path().join(member).exists(), "retained {member}");
    }
    assert!(directory.path().join("LOCK").exists());
    assert!(directory
        .path()
        .join("snapshots/retained/manifest")
        .exists());
    assert!(!std::fs::read_dir(directory.path())
        .unwrap()
        .flatten()
        .any(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with(".radixdb-reset-")));
}

#[cfg(unix)]
#[test]
fn reset_storage_unlinks_members_without_following_symlinks() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    std::fs::write(external.path().join("must-survive"), b"external").unwrap();

    symlink(external.path(), directory.path().join("catalog")).unwrap();
    symlink(
        directory.path().join("missing-control-target"),
        directory.path().join("CONTROL.0"),
    )
    .unwrap();
    std::fs::create_dir(directory.path().join("wal")).unwrap();

    reset_storage_generation(directory.path()).unwrap();

    for member in ["catalog", "CONTROL.0", "wal"] {
        let error = std::fs::symlink_metadata(directory.path().join(member))
            .expect_err("reset member must be absent");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
    assert_eq!(
        std::fs::read(external.path().join("must-survive")).unwrap(),
        b"external"
    );
}

#[test]
fn v2_r10_cli_actions_are_exclusive_and_reset_requires_persistence() {
    for arguments in [
        vec!["radixdb-cli", "-e", "SELECT 1", "-f", "query.sql"],
        vec!["radixdb-cli", "--snapshot", "--restore"],
        vec!["radixdb-cli", "--reset-storage", "--snapshot"],
        vec![
            "radixdb-cli",
            "--export-sql",
            "dump.sql",
            "--import-sql",
            "dump.sql",
        ],
        vec!["radixdb-cli", "-e", "SELECT 1", "--export-sql", "-"],
    ] {
        assert!(
            Args::try_parse_from(arguments).is_err(),
            "conflicting action flags must fail during argument admission"
        );
    }

    let memory_reset = Args::try_parse_from(["radixdb-cli", "--reset-storage"]).unwrap();
    assert!(validate_cli_args(&memory_reset).is_err());
    let file_reset = Args::try_parse_from([
        "radixdb-cli",
        "--db",
        "file:///tmp/radixdb-reset-contract",
        "--reset-storage",
    ])
    .unwrap();
    validate_cli_args(&file_reset).unwrap();

    let memory_import = Args::try_parse_from(["radixdb-cli", "--import-sql", "dump.sql"]).unwrap();
    assert!(validate_cli_args(&memory_import).is_err());
}

#[test]
fn icp_6_9_cli_import_publishes_only_a_verified_complete_staging_database() {
    let directory = tempfile::tempdir().unwrap();
    let dump = directory.path().join("logical.sql");
    let source = Database::open_in_memory().unwrap();
    source
            .execute(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES (1, 'one'), (2, 'two')",
                (),
            )
            .unwrap();
    let exported = export_sql_to_destination(&source, dump.to_str().unwrap()).unwrap();
    assert_eq!(exported.tables, 1);
    assert_eq!(exported.rows, 2);

    let target = directory.path().join("imported");
    let target_dsn = format!(
        "file://{}?sync_mode=none&checkpoint_on_close=off&compression=off",
        target.display()
    );
    let imported = import_sql_from_source(
        &target_dsn,
        &target,
        dump.to_str().expect("temporary path is UTF-8"),
    )
    .unwrap();
    assert_eq!(imported, exported);
    let reopened = Database::open(&target_dsn).unwrap();
    assert_eq!(
        reopened
            .query_one::<i64, _>("SELECT COUNT(*) FROM items", ())
            .unwrap(),
        2
    );
    reopened.close().unwrap();

    let corrupt = directory.path().join("corrupt.sql");
    let mut bytes = std::fs::read(&dump).unwrap();
    let needle = b"'two'";
    let offset = bytes
        .windows(needle.len())
        .position(|window| window == needle)
        .expect("dump contains second value");
    bytes[offset + 1] = b'T';
    std::fs::write(&corrupt, bytes).unwrap();
    let rejected_target = directory.path().join("rejected");
    let rejected_dsn = format!("file://{}", rejected_target.display());
    let error = import_sql_from_source(
        &rejected_dsn,
        &rejected_target,
        corrupt.to_str().expect("temporary path is UTF-8"),
    )
    .expect_err("checksum mismatch must reject publication");
    assert!(error.contains("checksum mismatch"), "{error}");
    assert!(!rejected_target.exists());
    let rejected_prefix = ".rejected.import.";
    assert!(!std::fs::read_dir(directory.path())
        .unwrap()
        .flatten()
        .any(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with(rejected_prefix)));
}

#[test]
fn v2_r10_cli_table_values_and_json_window_are_exact() {
    assert_eq!(format_value(&Value::Float(1.123_456_789)), "1.123456789");
    assert_eq!(format_value(&Value::Float(1.0)), "1.0");
    assert_eq!(format_value(&Value::Float(-0.0)), "-0.0");
    assert_eq!(
        format_value(&Value::try_decimal(12_340, 5, 3).unwrap()),
        "12.340"
    );
    assert_eq!(format_value(&Value::date(0)), "1970-01-01");
    assert_eq!(format_value(&Value::bytes(vec![0, 255])), "0x00ff");

    let rows = vec![
        vec![Value::Integer(1)],
        vec![Value::Integer(2)],
        vec![Value::Integer(9)],
    ];
    let json = json_query_result(&["id".to_string()], &rows, 10, 3);
    assert_eq!(json["count"], 10);
    assert_eq!(json["retained_count"], 3);
    assert_eq!(json["omitted_count"], 7);
    assert_eq!(json["truncated"], true);
    assert_eq!(json["window"]["kind"], "head_tail");
    assert_eq!(json["window"]["head_count"], 1);
    assert_eq!(json["window"]["tail_count"], 2);
}

#[test]
fn v2_r10_reset_reports_rollback_namespace_failure() {
    let directory = tempfile::tempdir().unwrap();
    create_storage_generation_fixture(directory.path());

    let error = reset_storage_generation_with_hook(directory.path(), |staged| {
        if staged == 1 {
            // Simulate a competing/error-recovery writer recreating the
            // original namespace before our reverse rename.
            std::fs::write(directory.path().join("CONTROL.0"), b"competing")?;
            return Err(io::Error::other("injected staging failure"));
        }
        Ok(())
    })
    .expect_err("rollback conflict must remain visible in the reset outcome");
    let message = error.to_string();
    assert!(message.contains("rollback also failed"), "{message}");
    assert!(
        message.contains("namespace state may be mixed"),
        "{message}"
    );
    assert!(message.contains("both it and quarantine"), "{message}");
}
