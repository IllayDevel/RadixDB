// Copyright 2026 RadixDB Contributors
// SPDX-License-Identifier: Apache-2.0

#![cfg(all(feature = "cli", unix))]

use radixdb::storage::v6::{
    encode_snapshot_manifest, open_snapshot_manifest, SnapshotManifest, SNAPSHOT_MANIFEST_FILE,
};
use radixdb::Database;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

#[test]
fn r12_batch_b_external_backup_is_exact_self_describing_and_fail_closed() {
    let fixture = tempfile::tempdir().expect("fixture");
    let source = fixture.path().join("source");
    let backup = fixture.path().join("backup");
    let dsn = format!(
        "file://{}?sync_mode=full&checkpoint_interval=0&keep_snapshots=3",
        source.display()
    );
    let database = Database::open(&dsn).expect("open source");
    database
        .execute(
            "CREATE TABLE exact_backup (id INTEGER PRIMARY KEY, value TEXT NOT NULL); \
             INSERT INTO exact_backup VALUES (1, 'old')",
            (),
        )
        .expect("create old recovery point");
    let old = database.create_snapshot().expect("old snapshot");
    database
        .execute("INSERT INTO exact_backup VALUES (2, 'new')", ())
        .expect("advance source");
    database.close().expect("close source");

    let backup_output = run_script(
        "release/backup-external.sh",
        &[source.as_path(), backup.as_path()],
    );
    assert_success(&backup_output, "external backup");
    let metadata = read_metadata(&backup.join("BACKUP.env"));
    for key in [
        "format",
        "created_unix_seconds",
        "cli_version",
        "git_commit",
        "build_profile",
        "build_target",
        "cargo_lock_sha256",
        "physical_format",
        "database_id",
        "snapshot_id",
    ] {
        assert!(metadata.contains_key(key), "missing backup identity {key}");
    }
    assert_eq!(metadata["format"], "radixdb-external-backup-v2");
    let exact_id = &metadata["snapshot_id"];
    let exact_path = backup.join("snapshots").join(exact_id);
    let exact_manifest = open_snapshot_manifest(&exact_path).expect("exact copied snapshot");
    assert_eq!(exact_manifest.snapshot_id().to_string(), *exact_id);
    assert_eq!(
        exact_manifest.database_id().to_string(),
        metadata["database_id"]
    );
    assert_eq!(
        fs::read_to_string(backup.join("SHA256SUMS")).unwrap(),
        checksum_inventory(&backup)
    );

    // Add another fully valid retained snapshot and give it a later wall-clock
    // time. An implicit "latest" restore would now choose the old generation.
    make_writable(&backup);
    let old_source = source.join("snapshots").join(&old.snapshot_id);
    let old_in_backup = backup.join("snapshots").join(&old.snapshot_id);
    copy_tree(&old_source, &old_in_backup);
    rewrite_snapshot_time(
        &old_in_backup,
        exact_manifest
            .created_unix_ns()
            .saturating_add(1_000_000_000),
    );
    write_inventory(&backup);

    let source_gone = fixture.path().join("source-not-available");
    fs::rename(&source, &source_gone).expect("remove source root from recovery path");
    let restored = fixture.path().join("restored");
    let restore_output = run_script(
        "release/restore-external.sh",
        &[backup.as_path(), restored.as_path()],
    );
    assert_success(&restore_output, "exact restore");
    assert!(
        String::from_utf8_lossy(&restore_output.stdout).contains(exact_id),
        "restore did not report the selected recovery point"
    );
    let restored_dsn = format!("file://{}", restored.display());
    let restored_db = Database::open(&restored_dsn).unwrap();
    assert_eq!(
        restored_db
            .query_one::<i64, _>("SELECT COUNT(*) FROM exact_backup", ())
            .unwrap(),
        2,
        "restore fell back to the timestamp-latest old snapshot"
    );
    restored_db.close().unwrap();

    let missing = fixture.path().join("missing-exact");
    copy_tree(&backup, &missing);
    make_writable(&missing);
    fs::remove_dir_all(missing.join("snapshots").join(exact_id)).unwrap();
    write_inventory(&missing);
    let missing_target = fixture.path().join("must-not-exist-missing");
    let missing_output = run_script(
        "release/restore-external.sh",
        &[missing.as_path(), missing_target.as_path()],
    );
    assert!(!missing_output.status.success());
    assert!(
        !missing_target.exists(),
        "restore created a target before validation"
    );

    let incompatible = fixture.path().join("incompatible");
    copy_tree(&backup, &incompatible);
    make_writable(&incompatible);
    replace_metadata(&incompatible, "physical_format", "999.0");
    write_inventory(&incompatible);
    let incompatible_target = fixture.path().join("must-not-exist-incompatible");
    let incompatible_output = run_script(
        "release/restore-external.sh",
        &[incompatible.as_path(), incompatible_target.as_path()],
    );
    assert!(!incompatible_output.status.success());
    assert!(!incompatible_target.exists());
    let diagnostic = String::from_utf8_lossy(&incompatible_output.stderr);
    assert!(
        diagnostic.contains("required reader identity"),
        "{diagnostic}"
    );

    let tampered = fixture.path().join("tampered");
    copy_tree(&backup, &tampered);
    make_writable(&tampered);
    replace_metadata(&tampered, "database_id", "11111111111111111111111111111111");
    let tampered_target = fixture.path().join("must-not-exist-tampered");
    let tampered_output = run_script(
        "release/restore-external.sh",
        &[tampered.as_path(), tampered_target.as_path()],
    );
    assert!(!tampered_output.status.success());
    assert!(!tampered_target.exists());
    assert!(String::from_utf8_lossy(&tampered_output.stderr).contains("checksum"));
}

fn run_script(script: &str, arguments: &[&Path]) -> Output {
    let mut command = Command::new(Path::new(env!("CARGO_MANIFEST_DIR")).join(script));
    command.env("RADIXDB_CLI_BIN", env!("CARGO_BIN_EXE_radixdb-cli"));
    command.args(arguments);
    command.output().expect("run release script")
}

fn assert_success(output: &Output, operation: &str) {
    assert!(
        output.status.success(),
        "{operation} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn read_metadata(path: &Path) -> BTreeMap<String, String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| {
            let (key, value) = line.split_once('=').expect("metadata key=value");
            (key.to_string(), value.to_string())
        })
        .collect()
}

fn replace_metadata(root: &Path, key: &str, value: &str) {
    let path = root.join("BACKUP.env");
    let current = fs::read_to_string(&path).unwrap();
    let updated = current
        .lines()
        .map(|line| {
            if line.starts_with(&format!("{key}=")) {
                format!("{key}={value}")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(path, updated).unwrap();
}

fn rewrite_snapshot_time(snapshot: &Path, created_unix_ns: u64) {
    let manifest = open_snapshot_manifest(snapshot).unwrap();
    let rewritten = SnapshotManifest::new(
        manifest.snapshot_id(),
        manifest.database_id(),
        manifest.database_generation(),
        manifest.database_manifest(),
        manifest.catalog(),
        manifest.members().to_vec(),
        created_unix_ns,
    )
    .unwrap();
    fs::write(
        snapshot.join(SNAPSHOT_MANIFEST_FILE),
        encode_snapshot_manifest(&rewritten).unwrap(),
    )
    .unwrap();
    open_snapshot_manifest(snapshot).expect("rewritten snapshot remains valid");
}

fn copy_tree(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        let file_type = entry.file_type().unwrap();
        assert!(!file_type.is_symlink());
        if file_type.is_dir() {
            copy_tree(&entry.path(), &destination);
        } else {
            fs::copy(entry.path(), destination).unwrap();
        }
    }
}

fn make_writable(root: &Path) {
    for path in recursive_paths(root) {
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(permissions.mode() | 0o200);
        fs::set_permissions(path, permissions).unwrap();
    }
}

fn recursive_paths(root: &Path) -> Vec<PathBuf> {
    let mut paths = vec![root.to_path_buf()];
    if root.is_dir() {
        for entry in fs::read_dir(root).unwrap() {
            paths.extend(recursive_paths(&entry.unwrap().path()));
        }
    }
    paths
}

fn write_inventory(root: &Path) {
    fs::write(root.join("SHA256SUMS"), checksum_inventory(root)).unwrap();
}

fn checksum_inventory(root: &Path) -> String {
    let mut files = recursive_paths(root)
        .into_iter()
        .filter(|path| path.is_file())
        .filter(|path| path.file_name().is_none_or(|name| name != "SHA256SUMS"))
        .collect::<Vec<_>>();
    files.sort_by_key(|path| path.strip_prefix(root).unwrap().to_path_buf());
    files
        .into_iter()
        .map(|path| {
            let relative = path.strip_prefix(root).unwrap();
            format!(
                "{:x}  {}\n",
                Sha256::digest(fs::read(&path).unwrap()),
                relative.display()
            )
        })
        .collect()
}
