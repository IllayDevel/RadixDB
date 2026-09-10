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

//! Real-shape logical migration rehearsal shared by the pre-cutover dry run
//! and the post-cutover V5-export/V6-import acceptance gate.

#![cfg(feature = "cli")]

use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use radixdb::{Database, Result};
use sha2::{Digest, Sha256};

#[allow(dead_code)]
#[path = "common/prerelease/schema.rs"]
mod messenger_fixture;

use messenger_fixture::{
    messenger_schema_plan, messenger_small_seed_plan, messenger_view_plan, SchemaPlan,
};

const EXPECTED_TABLES: usize = 18;
const EXPECTED_VIEWS: usize = 8;
const EXPECTED_ROWS: i64 = 37;
const EXPECTED_STATEMENTS: u64 = 109;
const EXPECTED_LOGICAL_CONTENT_SHA256: &str =
    "4de6650e4896a1da7e1dd769655ca0454e93e980dd7940489bd6cc871bb533f4";

fn dsn(path: &Path) -> String {
    format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=on",
        path.display()
    )
}

fn append_plan(sql: &mut String, plan: SchemaPlan) {
    plan.validate().expect("valid messenger fixture plan");
    for statement in plan.statements {
        writeln!(sql, "{};", statement.sql.trim_end_matches(';'))
            .expect("String writes cannot fail");
    }
}

fn fixture_sql() -> String {
    let mut sql = String::new();
    append_plan(&mut sql, messenger_schema_plan());
    writeln!(&mut sql, "BEGIN;").unwrap();
    append_plan(&mut sql, messenger_small_seed_plan());
    writeln!(&mut sql, "COMMIT;").unwrap();
    append_plan(&mut sql, messenger_view_plan());
    writeln!(&mut sql, "PRAGMA CHECKPOINT;").unwrap();
    sql
}

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

fn migration_exporter(current_binary: &Path) -> PathBuf {
    let exporter = std::env::var_os("RADIXDB_SQL_MIGRATION_EXPORTER")
        .map(PathBuf::from)
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
            "migration rehearsal requires an independently built exporter"
        );
        let version = run_cli(&exporter, &["--version"]);
        let version = String::from_utf8(version.stdout).expect("exporter version is UTF-8");
        assert!(
            version.contains("profile=release"),
            "migration exporter must be a release binary: {version}"
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

fn dump_sha256(dump: &[u8]) -> &str {
    let text = std::str::from_utf8(dump).expect("SQL dump is UTF-8");
    text.lines()
        .next_back()
        .and_then(|line| line.strip_prefix("-- radixdb-sql-dump-sha256: "))
        .expect("SQL dump has checksum footer")
}

fn logical_content_sha256(dump: &[u8]) -> String {
    let text = std::str::from_utf8(dump).expect("SQL dump is UTF-8");
    let mut canonical = String::with_capacity(text.len());
    for line in text.lines() {
        if line.starts_with("-- radixdb-sql-dump-sha256: ") {
            continue;
        }
        if line.starts_with("-- source-engine-version: ") {
            canonical.push_str("-- source-engine-version: <normalized>\n");
        } else {
            canonical.push_str(line);
            canonical.push('\n');
        }
    }
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

fn assert_dump_counts(dump: &[u8]) {
    let text = std::str::from_utf8(dump).expect("SQL dump is UTF-8");
    let expected = format!(
        "-- radixdb-sql-dump-counts: tables={EXPECTED_TABLES} rows={EXPECTED_ROWS} statements={EXPECTED_STATEMENTS}"
    );
    let actual = text
        .lines()
        .find(|line| line.starts_with("-- radixdb-sql-dump-counts: "))
        .expect("SQL dump has count trailer");
    assert_eq!(actual, expected);
}

fn assert_current_artifact_tree(root: &Path) {
    fn visit(root: &Path, path: &Path, files: &mut Vec<PathBuf>) {
        assert!(files.len() < 16_384, "artifact tree exceeded test bound");
        for entry in fs::read_dir(path).expect("read migration target") {
            let entry = entry.expect("read migration target entry");
            let file_type = entry.file_type().expect("read migration target type");
            assert!(!file_type.is_symlink(), "artifact tree contains a symlink");
            if file_type.is_dir() {
                visit(root, &entry.path(), files);
            } else {
                assert!(file_type.is_file(), "artifact tree contains a special file");
                files.push(
                    entry
                        .path()
                        .strip_prefix(root)
                        .expect("artifact belongs to target")
                        .to_path_buf(),
                );
            }
        }
    }

    let mut files = Vec::new();
    visit(root, root, &mut files);
    assert!(
        root.join("CONTROL.0").is_file() || root.join("CONTROL.1").is_file(),
        "migration target has no published CONTROL"
    );
    for required in [".cat", ".mft", ".data", ".idx", ".log"] {
        assert!(
            files
                .iter()
                .any(|path| path.to_string_lossy().ends_with(required)),
            "migration target has no '{required}' member"
        );
    }
    for path in &files {
        let text = path.to_string_lossy();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        assert!(
            !path.components().any(|part| part.as_os_str() == "volumes"),
            "migration target retained legacy volume directory: {text}"
        );
        assert!(
            !text.ends_with(".vol")
                && !text.ends_with(".rpi")
                && name != "manifest.bin"
                && name != "generation.bin"
                && name != "checkpoint.meta"
                && !name.starts_with("ddl-")
                && !name.ends_with(".log.bak")
                && !name.starts_with("wal-temp-")
                && !name.starts_with("wal_"),
            "migration target retained a legacy artifact: {text}"
        );
    }
}

fn assert_catalog_shape(database: &Database) -> Result<()> {
    let tables = database
        .query("SHOW TABLES", ())?
        .collect::<Result<Vec<_>>>()?;
    let views = database
        .query("SHOW VIEWS", ())?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(tables.len(), EXPECTED_TABLES);
    assert_eq!(views.len(), EXPECTED_VIEWS);

    let table_names = [
        "attachments",
        "audit_log",
        "command_results",
        "conversation_members",
        "conversations",
        "devices",
        "forwarded_attachment_access",
        "forwarded_messages",
        "message_versions",
        "messages",
        "outbox_jobs",
        "reactions",
        "receipts",
        "refresh_tokens",
        "sessions",
        "sync_events",
        "tenants",
        "users",
    ];
    let mut rows = 0_i64;
    for table in table_names {
        rows += database.query_one::<i64, _>(&format!("SELECT COUNT(*) FROM {table}"), ())?;
    }
    assert_eq!(rows, EXPECTED_ROWS);

    for view in [
        "active_conversations_v",
        "conversation_health_v",
        "conversation_unread_v",
        "forwarded_attachment_access_v",
        "message_delivery_v",
        "pending_outbox_v",
        "user_inbox_v",
        "user_sync_feed_v",
    ] {
        let _: i64 = database.query_one(&format!("SELECT COUNT(*) FROM {view}"), ())?;
    }

    for (query, index) in [
        (
            "SELECT id FROM messages WHERE sender_id = 1 AND conversation_id = 10",
            "messages_sender_idx",
        ),
        (
            "SELECT id FROM sync_events WHERE message_id = 100 AND user_id = 2",
            "sync_message_idx",
        ),
        (
            "SELECT id FROM outbox_jobs WHERE state = 'pending' AND visible = true AND id = 1000",
            "outbox_state_idx",
        ),
        (
            "SELECT id FROM audit_log WHERE tenant_id = 1 AND entity_kind = 'message' AND entity_id = 100",
            "audit_entity_idx",
        ),
    ] {
        let plan = database
            .query(&format!("EXPLAIN {query}"), ())?
            .map(|row| row.and_then(|row| row.get::<String>(0)))
            .collect::<Result<Vec<_>>>()?
            .join("\n");
        assert!(plan.contains(index), "expected {index} in plan:\n{plan}");
    }
    Ok(())
}

#[test]
fn real_messenger_shape_survives_source_free_process_migration() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let source_root = fixture.path().join("old-source");
    let target_root = fixture.path().join("candidate-target");
    let seed_path = fixture.path().join("messenger.sql");
    let dump_path = fixture.path().join("migration.sql");
    let independent_dump_path = fixture.path().join("migration-independent.sql");
    let third_dump_path = fixture.path().join("migration-third.sql");
    let reexport_path = fixture.path().join("reexport.sql");
    std::fs::write(&seed_path, fixture_sql())?;

    let current_binary = Path::new(env!("CARGO_BIN_EXE_radixdb-cli"));
    let exporter = migration_exporter(current_binary);
    let source_dsn = dsn(&source_root);
    let target_dsn = dsn(&target_root);
    let seed_path = seed_path.to_str().expect("temporary path is UTF-8");
    let dump_path_text = dump_path.to_str().expect("temporary path is UTF-8");
    let independent_dump_path_text = independent_dump_path
        .to_str()
        .expect("temporary path is UTF-8");
    let third_dump_path_text = third_dump_path.to_str().expect("temporary path is UTF-8");
    let reexport_path_text = reexport_path.to_str().expect("temporary path is UTF-8");

    run_cli(
        &exporter,
        &["--quiet", "--db", &source_dsn, "--file", seed_path],
    );
    run_cli(
        &exporter,
        &[
            "--quiet",
            "--db",
            &source_dsn,
            "--export-sql",
            dump_path_text,
        ],
    );
    for destination in [independent_dump_path_text, third_dump_path_text] {
        run_cli(
            &exporter,
            &["--quiet", "--db", &source_dsn, "--export-sql", destination],
        );
    }
    let dump = std::fs::read(&dump_path)?;
    assert_dump_counts(&dump);
    assert_eq!(std::fs::read(&independent_dump_path)?, dump);
    assert_eq!(std::fs::read(&third_dump_path)?, dump);
    assert_eq!(
        logical_content_sha256(&dump),
        EXPECTED_LOGICAL_CONTENT_SHA256,
        "logical statements changed after normalizing non-semantic source version provenance"
    );
    assert_eq!(dump_sha256(&dump).len(), 64);

    std::fs::remove_dir_all(&source_root)?;
    assert!(!source_root.exists());
    assert!(!target_root.exists());
    run_cli(
        current_binary,
        &[
            "--quiet",
            "--db",
            &target_dsn,
            "--import-sql",
            dump_path_text,
        ],
    );

    let target = Database::open(&target_dsn)?;
    assert_catalog_shape(&target)?;
    target.close()?;
    drop(target);
    let reopened = Database::open(&target_dsn)?;
    assert_catalog_shape(&reopened)?;
    reopened.close()?;
    drop(reopened);

    run_cli(
        current_binary,
        &[
            "--quiet",
            "--db",
            &target_dsn,
            "--export-sql",
            reexport_path_text,
        ],
    );
    assert_eq!(std::fs::read(&reexport_path)?, dump);
    assert_current_artifact_tree(&target_root);
    Ok(())
}
