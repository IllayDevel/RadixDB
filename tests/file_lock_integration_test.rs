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

// Test file lock prevents multiple database opens from different PROCESSES
// Note: Unix flock() allows the same process to acquire the lock multiple times
// The file lock is designed for inter-process locking, not intra-process

use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use radixdb::{Database, Error};
use tempfile::tempdir;

const CHILD_MODE: &str = "RADIXDB_FILE_LOCK_CHILD";
const CHILD_DSN: &str = "RADIXDB_FILE_LOCK_DSN";
const CHILD_READY: &str = "RADIXDB_FILE_LOCK_READY";
const CHILD_RELEASE: &str = "RADIXDB_FILE_LOCK_RELEASE";

struct ChildGuard {
    child: Child,
    release_path: PathBuf,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.release_path, b"release");
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

#[test]
fn file_lock_child_process_holds_database() {
    if std::env::var_os(CHILD_MODE).is_none() {
        return;
    }

    let dsn = std::env::var(CHILD_DSN).expect("child DSN");
    let ready_path = PathBuf::from(std::env::var(CHILD_READY).expect("child ready path"));
    let release_path = PathBuf::from(std::env::var(CHILD_RELEASE).expect("child release path"));
    let _database = Database::open(&dsn).expect("child database open");
    std::fs::write(&ready_path, b"ready").expect("publish child readiness");

    let deadline = Instant::now() + Duration::from_secs(15);
    while !release_path.exists() {
        assert!(Instant::now() < deadline, "parent did not release child");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn r2_l01_batch_a_file_lock_excludes_a_second_process_and_releases_cleanly() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_db");
    let dsn = format!("file://{}", db_path.to_str().unwrap());
    let ready_path = dir.path().join("child.ready");
    let release_path = dir.path().join("child.release");
    let artifact_sentinel = db_path
        .join("artifacts")
        .join("ff")
        .join("must-survive-reset.data");

    let child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("file_lock_child_process_holds_database")
        .arg("--nocapture")
        .env(CHILD_MODE, "1")
        .env(CHILD_DSN, &dsn)
        .env(CHILD_READY, &ready_path)
        .env(CHILD_RELEASE, &release_path)
        .spawn()
        .expect("spawn lock-holder process");
    let mut child = ChildGuard {
        child,
        release_path: release_path.clone(),
    };

    let deadline = Instant::now() + Duration::from_secs(15);
    while !ready_path.exists() {
        if let Some(status) = child.child.try_wait().unwrap() {
            panic!("lock-holder process exited before readiness: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "lock-holder process did not become ready"
        );
        thread::sleep(Duration::from_millis(10));
    }

    // Add the sentinel only after the child has opened a valid database. An
    // orphan DATA artifact before the first open is intentionally rejected
    // by the recovery contract and cannot serve as a lock-contention fixture.
    std::fs::create_dir_all(artifact_sentinel.parent().unwrap()).unwrap();
    std::fs::write(&artifact_sentinel, b"owned").unwrap();

    let error = match Database::open(&dsn) {
        Ok(_) => panic!("second process must not open an actively owned database"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::DatabaseLocked));

    // Cargo exports CARGO_BIN_EXE_* even when a required-feature binary is not
    // built. Keep the default workspace gate independent of that phantom path;
    // the CLI-enabled gate executes the additional offline-reset contract.
    #[cfg(feature = "cli")]
    {
        let reset = Command::new(env!("CARGO_BIN_EXE_radixdb-cli"))
            .arg("--quiet")
            .arg("--db")
            .arg(&dsn)
            .arg("--reset-storage")
            .arg("--execute")
            .arg("SELECT 1")
            .output()
            .expect("run offline reset contender");
        assert!(
            !reset.status.success(),
            "offline reset must refuse an actively owned database"
        );
        assert!(
            artifact_sentinel.exists(),
            "offline reset must not mutate artifacts before acquiring ownership"
        );
        assert!(
            db_path.join("wal").exists(),
            "offline reset must not remove WAL before acquiring ownership"
        );
    }
    std::fs::remove_file(&artifact_sentinel).unwrap();

    std::fs::write(&release_path, b"release").unwrap();
    let status = child.child.wait().unwrap();
    assert!(status.success(), "lock-holder process failed: {status}");
    std::mem::forget(child);

    let reopened = Database::open(&dsn).expect("lock must be released after child exit");
    drop(reopened);
}

#[test]
fn test_file_lock_released_on_drop() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_db2");
    let dsn = format!("file://{}", db_path.to_str().unwrap());

    // Open and close database multiple times
    for i in 1..=3 {
        println!("Opening database iteration {}", i);
        let db = Database::open(&dsn)
            .unwrap_or_else(|_| panic!("Database should open on iteration {}", i));

        // Do a simple operation
        db.execute(
            "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)",
            [],
        )
        .ok();

        println!("Closing database iteration {}", i);
        drop(db);
    }

    println!("File lock release test passed!");
}
