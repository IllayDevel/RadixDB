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

//! Frozen logical migration oracle for the catalog-artifact V6 program.
//!
//! Import receives only a verified SQL stream and an absent target. The source
//! physical database is removed before import so this test cannot accidentally
//! turn into a V5 filesystem reader or copier during the V6 cutover.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
#[cfg(feature = "cli")]
use std::process::{Command, Output};

use radixdb::sql_dump::{export_sql_dump_to_file, import_sql_dump_to_new_database};
use radixdb::{Database, Result};
use sha2::{Digest, Sha256};

// The release exporter predates automatic supporting-index creation for
// foreign keys. Keep its transport identity immutable for the cross-binary
// gate, while the current roundtrip oracle includes that durable index.
#[cfg(feature = "cli")]
const FROZEN_OLD_EXPORTER_DUMP_SHA256: &str =
    "25ad00ee0c9f71a67cb387477387c74ed423e696a4af8a322979a8ad0be45de1";
const CURRENT_DUMP_SHA256: &str =
    "b108ab01415430e321e4b2ded95875fbb85ed7799252cda6be4d5d45d12c5ba3";
const EXPECTED_LOGICAL_SHA256: &str =
    "b3ccd40fb608b8ebb2bc3fbf571e39ee08d4326e7b65e44d49709966ba67da7e";

#[cfg(feature = "cli")]
const MIGRATION_FIXTURE_SQL: &str = r#"
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    balance INTEGER NOT NULL CHECK (balance >= 0)
);
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    amount INTEGER NOT NULL CHECK (amount > 0)
);
CREATE INDEX events_account_kind_idx ON events(account_id, kind);
CREATE VIEW account_events_v AS
SELECT a.name AS account_name, e.id AS event_id, e.kind AS kind
FROM accounts a INNER JOIN events e ON e.account_id = a.id;
BEGIN;
INSERT INTO accounts VALUES (1, 'alpha', 1000), (2, 'beta', 500);
INSERT INTO events VALUES
    (10, 1, 'credit', 100),
    (11, 1, 'debit', 250),
    (20, 2, 'debit', 80);
UPDATE accounts SET balance = 900 WHERE id = 1;
DELETE FROM events WHERE id = 20;
COMMIT;
BEGIN;
INSERT INTO accounts VALUES (3, 'transient', 10);
INSERT INTO events VALUES (30, 3, 'debit', 1);
ROLLBACK;
PRAGMA CHECKPOINT;
"#;

fn dsn(path: &Path) -> String {
    format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=on",
        path.display()
    )
}

fn logical_checksum(database: &Database) -> Result<String> {
    let mut canonical = String::from("accounts\n");
    for row in database.query("SELECT id, name, balance FROM accounts ORDER BY id", ())? {
        let row = row?;
        canonical.push_str(&format!(
            "{}|{}|{}\n",
            row.get::<i64>(0)?,
            row.get::<String>(1)?,
            row.get::<i64>(2)?
        ));
    }
    canonical.push_str("events\n");
    for row in database.query(
        "SELECT id, account_id, kind, amount FROM events ORDER BY id",
        (),
    )? {
        let row = row?;
        canonical.push_str(&format!(
            "{}|{}|{}|{}\n",
            row.get::<i64>(0)?,
            row.get::<i64>(1)?,
            row.get::<String>(2)?,
            row.get::<i64>(3)?
        ));
    }
    Ok(format!("{:x}", Sha256::digest(canonical.as_bytes())))
}

fn assert_logical_state(database: &Database) -> Result<()> {
    assert_eq!(logical_checksum(database)?, EXPECTED_LOGICAL_SHA256);
    assert_eq!(
        database.query_one::<i64, _>("SELECT COUNT(*) FROM accounts", ())?,
        2
    );
    assert_eq!(
        database.query_one::<i64, _>("SELECT COUNT(*) FROM events", ())?,
        2
    );
    assert_eq!(
        database.query_one::<i64, _>("SELECT COUNT(*) FROM accounts WHERE id = 3", ())?,
        0,
        "rolled-back account became visible"
    );
    let joined = database
        .query(
            "SELECT account_name, event_id, kind FROM account_events_v ORDER BY event_id",
            (),
        )?
        .map(|row| {
            let row = row?;
            Ok((
                row.get::<String>(0)?,
                row.get::<i64>(1)?,
                row.get::<String>(2)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        joined,
        vec![
            ("alpha".to_string(), 10, "credit".to_string()),
            ("alpha".to_string(), 11, "debit".to_string()),
        ]
    );
    let plan = database
        .query(
            "EXPLAIN SELECT id FROM events WHERE account_id = 1 AND kind = 'debit'",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<String>(0)))
        .collect::<Result<Vec<_>>>()?
        .join("\n");
    assert!(
        plan.contains("events_account_kind_idx"),
        "migration oracle lost declared index behavior:\n{plan}"
    );
    Ok(())
}

fn seed_source(database: &Database) -> Result<()> {
    database.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            balance INTEGER NOT NULL CHECK (balance >= 0)
        )",
        (),
    )?;
    database.execute(
        "CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            amount INTEGER NOT NULL CHECK (amount > 0)
        )",
        (),
    )?;
    database.execute(
        "CREATE INDEX events_account_kind_idx ON events(account_id, kind)",
        (),
    )?;
    database.execute(
        "CREATE VIEW account_events_v AS
         SELECT a.name AS account_name, e.id AS event_id, e.kind AS kind
         FROM accounts a INNER JOIN events e ON e.account_id = a.id",
        (),
    )?;

    let mut committed = database.begin()?;
    committed.execute(
        "INSERT INTO accounts VALUES (1, 'alpha', 1000), (2, 'beta', 500)",
        (),
    )?;
    committed.execute(
        "INSERT INTO events VALUES
            (10, 1, 'credit', 100),
            (11, 1, 'debit', 250),
            (20, 2, 'debit', 80)",
        (),
    )?;
    committed.execute("UPDATE accounts SET balance = 900 WHERE id = 1", ())?;
    committed.execute("DELETE FROM events WHERE id = 20", ())?;
    committed.commit()?;

    let mut rolled_back = database.begin()?;
    rolled_back.execute("INSERT INTO accounts VALUES (3, 'transient', 10)", ())?;
    rolled_back.execute("INSERT INTO events VALUES (30, 3, 'debit', 1)", ())?;
    rolled_back.rollback()?;
    database.execute("PRAGMA CHECKPOINT", ())?;
    Ok(())
}

#[cfg(feature = "cli")]
fn run_cli(binary: &Path, arguments: &[&str]) -> Output {
    let output = Command::new(binary)
        .args(arguments)
        .output()
        .unwrap_or_else(|error| panic!("cannot run '{}': {error}", binary.display()));
    assert!(
        output.status.success(),
        "'{} {}' failed with {}:\nstdout:\n{}\nstderr:\n{}",
        binary.display(),
        arguments.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[cfg(feature = "cli")]
fn migration_exporter(current_binary: &Path) -> std::path::PathBuf {
    let exporter = std::env::var_os("RADIXDB_SQL_MIGRATION_EXPORTER")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| current_binary.to_path_buf());
    assert!(
        exporter.is_file(),
        "migration exporter '{}' is not a file",
        exporter.display()
    );
    if std::env::var_os("RADIXDB_REQUIRE_EXTERNAL_SQL_EXPORTER").is_some() {
        let exporter = std::fs::canonicalize(&exporter).expect("canonical exporter path");
        let current = std::fs::canonicalize(current_binary).expect("canonical current CLI path");
        assert_ne!(
            exporter, current,
            "cross-binary gate requires an independently built old exporter"
        );

        let version = run_cli(&exporter, &["--version"]);
        let version = String::from_utf8(version.stdout).expect("exporter version is UTF-8");
        assert!(
            version.contains("profile=release"),
            "frozen migration exporter must be a release binary: {version}"
        );
        if let Some(expected_revision) = std::env::var_os("RADIXDB_SQL_MIGRATION_EXPORTER_REVISION")
        {
            let expected_revision = expected_revision
                .to_str()
                .expect("expected exporter revision is UTF-8");
            assert!(
                version.contains(&format!("git={expected_revision}")),
                "migration exporter provenance mismatch: {version}"
            );
        }
    }
    exporter
}

#[test]
fn ca_00_6_sql_export_import_checksum_oracle() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let source_root = fixture.path().join("v5-source");
    let target_root = fixture.path().join("v6-target");
    let dump_path = fixture.path().join("migration.sql");

    let source = Database::open(&dsn(&source_root))?;
    seed_source(&source)?;
    assert_logical_state(&source)?;
    let exported = export_sql_dump_to_file(&source, &dump_path)?;
    assert_eq!(exported.tables, 2);
    assert_eq!(exported.rows, 4);
    assert_eq!(
        exported.sha256,
        CURRENT_DUMP_SHA256,
        "fresh current database changed the current SQL transport:\n{}",
        std::fs::read_to_string(&dump_path)?
    );
    source.close()?;
    drop(source);

    // The import boundary has no source path and cannot use physical V5 bytes.
    std::fs::remove_dir_all(&source_root)?;
    assert!(!source_root.exists());
    assert!(!target_root.exists());

    let imported = import_sql_dump_to_new_database(
        &dsn(&target_root),
        &target_root,
        BufReader::new(File::open(&dump_path)?),
    )?;
    assert_eq!(imported, exported);

    let target = Database::open(&dsn(&target_root))?;
    assert_logical_state(&target)?;
    let reexport_path = fixture.path().join("reexport.sql");
    let reexported = export_sql_dump_to_file(&target, &reexport_path)?;
    assert_eq!(reexported, exported);
    assert_eq!(std::fs::read(&reexport_path)?, std::fs::read(&dump_path)?);
    target.close()?;
    drop(target);

    let mut damaged = std::fs::read(&dump_path)?;
    let marker = b"'alpha'";
    let offset = damaged
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("dump contains deterministic fixture value");
    damaged[offset + 1] = b'A';
    let damaged_target = fixture.path().join("damaged-target");
    let error = import_sql_dump_to_new_database(
        &dsn(&damaged_target),
        &damaged_target,
        BufReader::new(damaged.as_slice()),
    )
    .expect_err("one-byte dump corruption must fail checksum validation");
    assert!(error.to_string().contains("checksum mismatch"), "{error}");
    assert!(
        !damaged_target.exists(),
        "failed import published a partial target"
    );

    let occupied_target = fixture.path().join("occupied-target");
    std::fs::create_dir(&occupied_target)?;
    std::fs::write(occupied_target.join("sentinel"), b"unchanged")?;
    let error = import_sql_dump_to_new_database(
        &dsn(&occupied_target),
        &occupied_target,
        BufReader::new(File::open(&dump_path)?),
    )
    .expect_err("existing target must be rejected");
    assert!(error.to_string().contains("already exists"), "{error}");
    assert_eq!(
        std::fs::read(occupied_target.join("sentinel"))?,
        b"unchanged"
    );

    Ok(())
}

#[cfg(feature = "cli")]
#[test]
fn ca_70_3_old_exporter_to_current_importer_is_source_free() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let source_root = fixture.path().join("old-source");
    let target_root = fixture.path().join("new-target");
    let seed_path = fixture.path().join("seed.sql");
    let dump_path = fixture.path().join("migration.sql");
    let reexport_path = fixture.path().join("reexport.sql");
    std::fs::write(&seed_path, MIGRATION_FIXTURE_SQL)?;

    let current_binary = Path::new(env!("CARGO_BIN_EXE_radixdb-cli"));
    let exporter = migration_exporter(current_binary);
    let exporter_is_current =
        std::fs::canonicalize(&exporter)? == std::fs::canonicalize(current_binary)?;
    let source_dsn = dsn(&source_root);
    let target_dsn = dsn(&target_root);
    let source_dsn = source_dsn.as_str();
    let target_dsn = target_dsn.as_str();
    let seed_path = seed_path.to_str().expect("temporary path is UTF-8");
    let dump_path_text = dump_path.to_str().expect("temporary path is UTF-8");
    let reexport_path_text = reexport_path.to_str().expect("temporary path is UTF-8");

    run_cli(
        &exporter,
        &["--quiet", "--db", source_dsn, "--file", seed_path],
    );
    run_cli(
        &exporter,
        &[
            "--quiet",
            "--db",
            source_dsn,
            "--export-sql",
            dump_path_text,
        ],
    );

    let dump = std::fs::read(&dump_path)?;
    let expected_sha = if exporter_is_current {
        CURRENT_DUMP_SHA256
    } else {
        FROZEN_OLD_EXPORTER_DUMP_SHA256
    };
    let expected_footer = format!("-- radixdb-sql-dump-sha256: {expected_sha}\n");
    assert!(
        dump.ends_with(expected_footer.as_bytes()),
        "old exporter changed the frozen SQL transport identity"
    );

    // Import receives only the dump path. Removing the old physical source
    // makes any accidental compatibility reader or filesystem copier fail.
    std::fs::remove_dir_all(&source_root)?;
    assert!(!source_root.exists());
    assert!(!target_root.exists());
    run_cli(
        current_binary,
        &[
            "--quiet",
            "--db",
            target_dsn,
            "--import-sql",
            dump_path_text,
        ],
    );

    let target = Database::open(target_dsn)?;
    assert_logical_state(&target)?;
    target.close()?;
    drop(target);

    run_cli(
        current_binary,
        &[
            "--quiet",
            "--db",
            target_dsn,
            "--export-sql",
            reexport_path_text,
        ],
    );
    assert_eq!(
        std::fs::read(&reexport_path)?,
        dump,
        "reopened target must reproduce the old binary's exact SQL stream"
    );
    Ok(())
}
