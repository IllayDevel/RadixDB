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

//! File-based database locking to prevent concurrent access from multiple processes.
//!
//! This module provides OS-level file locking to ensure only one process can
//! access a database directory at a time. It uses:
//! - `flock()` on Unix systems (Linux, macOS)
//! - `LockFileEx()` on Windows
//!

use std::fs::{self, File, OpenOptions};
#[cfg(not(target_os = "wasi"))]
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(unix)]
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use radixdb_core::{Error, Result};

/// Represents an exclusive lock on a database directory.
///
/// The lock is automatically released when this struct is dropped.
#[derive(Debug, Clone)]
pub struct FileLock {
    inner: Arc<FileLockInner>,
}

#[derive(Debug)]
struct FileLockInner {
    /// The lock file handle (kept open to maintain the lock)
    #[allow(dead_code)]
    file: File,
    /// Path to the lock file
    path: PathBuf,
    /// Canonical database root owned by this lock.
    root: PathBuf,
    /// Filesystem identity protects against replacing or moving the root while
    /// an in-memory publisher still exists.
    root_identity: FilesystemIdentity,
    /// `flock` protects an inode rather than a pathname. Remembering the inode
    /// prevents a replaced `LOCK` file from creating two apparent writers.
    lock_identity: FilesystemIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FilesystemIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl FileLock {
    /// Acquire an exclusive lock on the database directory.
    ///
    /// Creates a canonical `LOCK` file in the database directory and locks it using
    /// OS-level file locking. Returns an error if the lock cannot be acquired
    /// (typically because another process has it).
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    ///
    /// # Returns
    /// * `Ok(FileLock)` - Lock was acquired successfully
    /// * `Err` - Lock could not be acquired (database is in use by another process)
    ///
    /// # Example
    /// ```ignore
    /// let lock = FileLock::acquire("/path/to/db")?;
    /// // ... use database ...
    /// // Lock is released when `lock` is dropped
    /// ```
    pub fn acquire(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref();

        // Ensure the directory exists
        fs::create_dir_all(db_path)
            .map_err(|e| Error::internal(format!("failed to create database directory: {}", e)))?;

        // Lock file path
        let lock_file_path = db_path.join("LOCK");

        // Open the lock file WITHOUT truncating — truncating before acquiring
        // the lock would destroy another process's PID if it currently holds the lock.
        #[allow(unused_mut)]
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_file_path)
            .map_err(|e| Error::internal(format!("failed to open lock file: {}", e)))?;

        // Try to acquire an exclusive lock (platform-specific).
        // This must happen BEFORE any file content modification.
        acquire_lock_for_open(&file, &lock_file_path)?;

        // Now that we hold the lock, clear and rewrite with our PID.
        // std::process::id() is not supported on WASI, so skip on that target.
        #[cfg(not(target_os = "wasi"))]
        {
            file.set_len(0)
                .map_err(|e| Error::internal(format!("failed to truncate lock file: {}", e)))?;
            let pid = std::process::id();
            write!(file, "{}", pid).ok();
            file.sync_all().ok();
        }

        let root = fs::canonicalize(db_path)
            .map_err(|e| Error::internal(format!("failed to resolve database directory: {e}")))?;
        let root_identity =
            filesystem_identity(&fs::metadata(&root).map_err(|e| {
                Error::internal(format!("failed to inspect database directory: {e}"))
            })?);
        let lock_identity = filesystem_identity(
            &file
                .metadata()
                .map_err(|e| Error::internal(format!("failed to inspect lock file: {e}")))?,
        );

        Ok(Self {
            inner: Arc::new(FileLockInner {
                file,
                path: root.join("LOCK"),
                root,
                root_identity,
                lock_identity,
            }),
        })
    }

    /// Get the path to the lock file
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Canonical database root protected by this lock.
    pub(crate) fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Verify that `path` still resolves to the exact root and `LOCK` inode
    /// acquired by this owner. This accepts symlink aliases to the same root,
    /// but rejects copied, moved or replaced directories.
    pub(crate) fn validate_root(&self, path: &Path) -> std::io::Result<bool> {
        let canonical = fs::canonicalize(path)?;
        let root_identity = filesystem_identity(&fs::metadata(&canonical)?);
        let lock_identity = filesystem_identity(&fs::metadata(canonical.join("LOCK"))?);
        Ok(root_identity == self.inner.root_identity
            && lock_identity == self.inner.lock_identity
            && canonical == self.inner.root)
    }
}

// Do NOT delete LOCK on drop. On Unix, flock protects the inode, not the path.
// The file is harmless on disk and the OS releases the lock when the final
// Arc<FileLockInner> (and therefore its File) is dropped.

#[cfg(unix)]
fn filesystem_identity(metadata: &fs::Metadata) -> FilesystemIdentity {
    FilesystemIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(unix))]
fn filesystem_identity(_metadata: &fs::Metadata) -> FilesystemIdentity {
    FilesystemIdentity {}
}

// ============================================================================
// Linux implementation
// ============================================================================

fn acquire_lock(file: &File) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();

    // SAFETY: fd is a valid file descriptor from AsRawFd on an open File.
    // libc::flock is safe to call with valid fd and standard flock flags.
    // LOCK_EX = exclusive lock, LOCK_NB = non-blocking
    let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };

    if result != 0 {
        let errno = std::io::Error::last_os_error();
        if errno.raw_os_error() == Some(libc::EWOULDBLOCK) {
            return Err(Error::DatabaseLocked);
        }
        return Err(Error::internal(format!(
            "failed to acquire lock: {}",
            errno
        )));
    }

    Ok(())
}

#[cfg(unix)]
fn acquire_lock_for_open(file: &File, lock_file_path: &Path) -> Result<()> {
    const SAME_PROCESS_HANDOFF_GRACE: Duration = Duration::from_millis(100);

    match acquire_lock(file) {
        Ok(()) => return Ok(()),
        Err(error) if !matches!(error, Error::DatabaseLocked) => return Err(error),
        Err(_) => {}
    }
    let owned_by_current_process = fs::read_to_string(lock_file_path)
        .ok()
        .and_then(|contents| contents.trim().parse::<u32>().ok())
        .is_some_and(|pid| pid == std::process::id());
    if !owned_by_current_process {
        return Err(Error::DatabaseLocked);
    }

    // A forked test/tool process inherits flock until exec closes its
    // CLOEXEC descriptors. The original in-process owner may already be gone,
    // so permit one short same-PID handoff instead of reporting a false second
    // writer. A genuinely retained owner still fails at the fixed deadline.
    let deadline = Instant::now() + SAME_PROCESS_HANDOFF_GRACE;
    loop {
        std::thread::sleep(Duration::from_millis(1));
        match acquire_lock(file) {
            Ok(()) => return Ok(()),
            Err(error) if !matches!(error, Error::DatabaseLocked) => return Err(error),
            Err(_) if Instant::now() < deadline => {}
            Err(_) => return Err(Error::DatabaseLocked),
        }
    }
}

#[cfg(not(unix))]
fn acquire_lock_for_open(file: &File, _lock_file_path: &Path) -> Result<()> {
    acquire_lock(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_acquire_lock() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_db");

        // Should be able to acquire lock
        let lock = FileLock::acquire(&db_path).unwrap();

        // Lock file should exist
        assert!(db_path.join("LOCK").exists());

        // Lock file should contain our PID.
        let contents = fs::read_to_string(db_path.join("LOCK")).unwrap();
        assert_eq!(contents, std::process::id().to_string());

        drop(lock);
    }

    #[test]
    fn test_lock_prevents_second_acquisition() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_db");

        // Acquire first lock
        let _lock1 = FileLock::acquire(&db_path).unwrap();

        // Second lock should fail
        let result = FileLock::acquire(&db_path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("locked by another process"));
    }

    #[test]
    fn cloned_owner_keeps_the_same_writer_lock_alive() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("shared_owner");
        let lock = FileLock::acquire(&db_path).unwrap();
        let publisher_owner = lock.clone();
        drop(lock);

        assert!(matches!(
            FileLock::acquire(&db_path).unwrap_err(),
            Error::DatabaseLocked
        ));
        drop(publisher_owner);
        FileLock::acquire(&db_path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn same_process_lock_handoff_waits_for_a_retained_clone() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("same-process-handoff");
        let lock = FileLock::acquire(&db_path).unwrap();
        let retained = lock.clone();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            drop(retained);
        });
        drop(lock);

        FileLock::acquire(&db_path)
            .expect("same-process lock handoff must tolerate fork-sized lag");
        release.join().unwrap();
    }

    #[test]
    fn test_lock_released_on_drop() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_db");

        // Acquire and release lock
        {
            let _lock = FileLock::acquire(&db_path).unwrap();
        }

        // Should be able to acquire again after drop
        let _lock2 = FileLock::acquire(&db_path).unwrap();
    }
}
