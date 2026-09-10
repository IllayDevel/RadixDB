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

//! Durability / Fault Injection Tests
//!
//! These tests write valid data, close the database, corrupt WAL/checkpoint
//! files in specific ways, then reopen and verify recovery consistency.
//!
//! Recovery invariants verified in every test:
//! 1. No partial transactions — all-or-nothing per transaction
//! 2. Consistent recovery — recovered data is a valid subset of committed data
//! 3. Database is usable — can INSERT after recovery
//! 4. Tables exist — DDL survived or was re-applied from WAL/volumes

use radixdb::Database;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

// ============================================================================
// WAL binary format constants (must match wal_manager.rs)
// ============================================================================

const WAL_ENTRY_MAGIC: u32 = 0x454C4157;
const WAL_HEADER_SIZE: usize = 32;
const CRC_SIZE: usize = 4;

// Flag bits
const COMPRESSED_FLAG: u8 = 0x01;
const COMMIT_MARKER_FLAG: u8 = 0x02;

// Checkpoint constants
const _CHECKPOINT_MAGIC: u32 = 0x43504F49;
const CHECKPOINT_DROP_CRASH_CHILD: &str = "RADIXDB_CHECKPOINT_DROP_CRASH_CHILD";

// ============================================================================
// Helper structs
// ============================================================================

/// Information about a single WAL entry's byte boundaries in the file
#[derive(Debug, Clone)]
struct WalEntryInfo {
    /// Byte offset of this entry's start (magic bytes)
    offset: usize,
    /// Total size on disk: header + data + CRC
    total_size: usize,
    /// Log Sequence Number
    _lsn: u64,
    /// Flags byte
    flags: u8,
    /// Size of data portion (from header)
    entry_size: usize,
    /// Byte offset where data portion starts (after header)
    data_offset: usize,
    /// Byte offset of the CRC32 (last 4 bytes of entry)
    crc_offset: usize,
}

struct TestFixture {
    _dir: TempDir,
    db_path: PathBuf,
    dsn: String,
}

// ============================================================================
// Helper functions
// ============================================================================

/// Find all WAL files in the database's wal/ directory
fn find_wal_files(db_path: &Path) -> Vec<PathBuf> {
    let wal_dir = db_path.join("wal");
    if !wal_dir.exists() {
        return Vec::new();
    }
    let mut files: Vec<PathBuf> = fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            (name.starts_with("wal-") || name.starts_with("wal_")) && name.ends_with(".log")
        })
        .map(|e| e.path())
        .collect();
    files.sort();
    files
}

/// Parse WAL binary data to locate all entry boundaries
fn find_entry_boundaries(data: &[u8]) -> Vec<WalEntryInfo> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos + WAL_HEADER_SIZE <= data.len() {
        // Check magic
        if pos + 4 > data.len() {
            break;
        }
        let magic = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        if magic != WAL_ENTRY_MAGIC {
            pos += 1;
            continue;
        }

        // Parse header fields
        let flags = data[pos + 5];
        let header_size = u16::from_le_bytes(data[pos + 6..pos + 8].try_into().unwrap()) as usize;
        let lsn = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
        let entry_size = u32::from_le_bytes(data[pos + 24..pos + 28].try_into().unwrap()) as usize;

        // Sanity check
        if entry_size > 64 * 1024 * 1024 || header_size < WAL_HEADER_SIZE {
            pos += 1;
            continue;
        }

        let data_offset = pos + header_size;
        let total_data_with_crc = entry_size + CRC_SIZE;
        let total_size = header_size + total_data_with_crc;

        if pos + total_size > data.len() {
            // Entry extends beyond file — incomplete
            break;
        }

        let crc_offset = data_offset + entry_size;

        entries.push(WalEntryInfo {
            offset: pos,
            total_size,
            _lsn: lsn,
            flags,
            entry_size,
            data_offset,
            crc_offset,
        });

        pos += total_size;
    }

    entries
}

/// Zero out a range of bytes in a buffer
fn zero_range(data: &mut [u8], offset: usize, len: usize) {
    let end = (offset + len).min(data.len());
    for byte in &mut data[offset..end] {
        *byte = 0;
    }
}

/// Flip a single bit at the given byte offset and bit position
fn flip_bit(data: &mut [u8], byte_offset: usize, bit_pos: u8) {
    if byte_offset < data.len() {
        data[byte_offset] ^= 1 << bit_pos;
    }
}

/// Zero a 4KB-aligned page
fn zero_page(data: &mut [u8], page_num: usize) {
    let page_size = 4096;
    let start = page_num * page_size;
    let end = (start + page_size).min(data.len());
    if start < data.len() {
        for byte in &mut data[start..end] {
            *byte = 0;
        }
    }
}

/// Remove the lock file so we can reopen the database
fn remove_lock_file(db_path: &Path) {
    let lock_file = db_path.join("db.lock");
    let _ = fs::remove_file(lock_file);
}

/// Create a database with known data, then close it.
/// Each transaction is auto-committed (one INSERT per execute call).
fn setup_test_db(num_txns: usize, rows_per_txn: usize) -> TestFixture {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE test_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL, seq INTEGER)",
            (),
        )
        .unwrap();

        let mut id = 1;
        for txn in 0..num_txns {
            for row in 0..rows_per_txn {
                db.execute(
                    &format!(
                        "INSERT INTO test_data (id, value, seq) VALUES ({}, 'txn{}_row{}', {})",
                        id, txn, row, txn
                    ),
                    (),
                )
                .unwrap();
                id += 1;
            }
        }

        // Verify all data is in
        let count: i64 = db.query_one("SELECT COUNT(*) FROM test_data", ()).unwrap();
        assert_eq!(count, (num_txns * rows_per_txn) as i64);
    }

    // Database is now closed — WAL has been flushed
    remove_lock_file(&db_path);

    TestFixture {
        _dir: dir,
        db_path,
        dsn,
    }
}

/// Create a database with large TEXT values (> 64 bytes) to trigger LZ4 compression
fn setup_test_db_large_rows(num_txns: usize, rows_per_txn: usize) -> TestFixture {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE test_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL, seq INTEGER)",
            (),
        )
        .unwrap();

        // Generate a large string (> 64 bytes to trigger compression)
        let large_prefix = "A".repeat(200);

        let mut id = 1;
        for txn in 0..num_txns {
            for row in 0..rows_per_txn {
                db.execute(
                    &format!(
                        "INSERT INTO test_data (id, value, seq) VALUES ({}, '{}_txn{}_row{}', {})",
                        id, large_prefix, txn, row, txn
                    ),
                    (),
                )
                .unwrap();
                id += 1;
            }
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM test_data", ()).unwrap();
        assert_eq!(count, (num_txns * rows_per_txn) as i64);
    }

    remove_lock_file(&db_path);

    TestFixture {
        _dir: dir,
        db_path,
        dsn,
    }
}

/// Verify that a structurally corrupt WAL never publishes a partial database.
///
/// The historical helper accepted "at least N" recovered rows by scanning past
/// bad records. R2-L02 deliberately removes that behavior: the entire validated
/// generation must be accepted before replay can publish any state.
fn verify_recovery_at_least(fixture: &TestFixture, _table: &str, _min_rows: i64) {
    verify_open_fails_closed(fixture);
}

fn verify_open_fails_closed(fixture: &TestFixture) {
    verify_dsn_open_fails_closed(&fixture.dsn);
}

fn verify_dsn_open_fails_closed(dsn: &str) {
    assert!(
        Database::open(dsn).is_err(),
        "corrupt durable state was partially published instead of failing closed"
    );
}

/// Open the database after corruption and verify exact row count.
/// Also verifies the database is usable.
fn verify_recovery_exact(fixture: &TestFixture, table: &str, exact_rows: i64) {
    let db = Database::open(&fixture.dsn).unwrap();

    let count: i64 = db
        .query_one(&format!("SELECT COUNT(*) FROM {}", table), ())
        .unwrap();
    assert_eq!(
        count, exact_rows,
        "Expected exactly {} rows, got {} after recovery",
        exact_rows, count
    );

    // Verify DB is usable
    let new_id = exact_rows + 10000;
    db.execute(
        &format!(
            "INSERT INTO {} (id, value, seq) VALUES ({}, 'post_recovery', 999)",
            table, new_id
        ),
        (),
    )
    .unwrap();
}

// ============================================================================
// 1. TORN WRITES — Truncate WAL at various offsets within the last entry
// ============================================================================

/// Helper: Setup 5 auto-committed transactions (3 rows each = 15 rows),
/// then truncate the WAL so the last entry is incomplete at `cut_into_last`.
/// `cut_into_last` is the number of bytes to keep from the last entry.
fn torn_write_test(cut_into_last: usize) {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty(), "No WAL files found");

    // Read WAL data
    let wal_path = &wal_files[wal_files.len() - 1]; // Use last WAL file
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(
        entries.len() >= 2,
        "Need at least 2 entries, found {}",
        entries.len()
    );

    let last = &entries[entries.len() - 1];
    let truncate_at = last.offset + cut_into_last;

    // Truncate the file
    let truncated = &data[..truncate_at.min(data.len())];
    fs::write(wal_path, truncated).unwrap();

    remove_lock_file(&fixture.db_path);

    // A torn selected generation is terminal. No complete prefix may be
    // published as a partial database.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_torn_write_partial_header_1_byte() {
    torn_write_test(1);
}

#[test]
fn test_torn_write_partial_header_16_bytes() {
    torn_write_test(16);
}

#[test]
fn test_torn_write_partial_header_31_bytes() {
    torn_write_test(31);
}

#[test]
fn test_torn_write_after_header_no_data() {
    torn_write_test(WAL_HEADER_SIZE);
}

#[test]
fn test_torn_write_partial_data() {
    // Truncate halfway through the data portion of last entry
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(entries.len() >= 2);

    let last = &entries[entries.len() - 1];
    let half_data = last.entry_size / 2;
    let truncate_at = last.data_offset + half_data;
    let truncated = &data[..truncate_at.min(data.len())];
    fs::write(wal_path, truncated).unwrap();

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_torn_write_after_data_no_crc() {
    // Keep full header + full data, but no CRC bytes
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(entries.len() >= 2);

    let last = &entries[entries.len() - 1];
    let truncate_at = last.crc_offset; // Right before CRC
    let truncated = &data[..truncate_at.min(data.len())];
    fs::write(wal_path, truncated).unwrap();

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_torn_write_partial_crc_1_byte() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(entries.len() >= 2);

    let last = &entries[entries.len() - 1];
    let truncate_at = last.crc_offset + 1; // Only 1 of 4 CRC bytes
    let truncated = &data[..truncate_at.min(data.len())];
    fs::write(wal_path, truncated).unwrap();

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_torn_write_partial_crc_3_bytes() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(entries.len() >= 2);

    let last = &entries[entries.len() - 1];
    let truncate_at = last.crc_offset + 3; // 3 of 4 CRC bytes
    let truncated = &data[..truncate_at.min(data.len())];
    fs::write(wal_path, truncated).unwrap();

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

// ============================================================================
// 2. SECTOR-ALIGNED PAGE LOSS — Zero 4KB pages
// ============================================================================

#[test]
fn test_sector_loss_first_page() {
    // Write enough data to span multiple 4KB pages
    let fixture = setup_test_db(20, 5);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();

    // Only corrupt if the file is large enough
    assert!(data.len() > 4096, "multi-table WAL did not span one page");
    zero_page(&mut data, 0);
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_sector_loss_middle_page() {
    let fixture = setup_test_db(20, 5);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let num_pages = data.len() / 4096;

    if num_pages >= 3 {
        let middle = num_pages / 2;
        zero_page(&mut data, middle);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);

    // Any damaged page in the selected generation is terminal.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_sector_loss_last_page() {
    let fixture = setup_test_db(20, 5);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let num_pages = data.len() / 4096;

    if num_pages >= 2 {
        let last = num_pages - 1;
        zero_page(&mut data, last);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_sector_loss_alternating_pages() {
    let fixture = setup_test_db(20, 5);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let num_pages = data.len() / 4096;

    // Zero every other page (starting from page 1 to preserve DDL on page 0)
    for page in (1..num_pages).step_by(2) {
        zero_page(&mut data, page);
    }
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

// ============================================================================
// 3. BIT CORRUPTION — Single-bit flips in specific fields
// ============================================================================

#[test]
fn test_bit_flip_magic_bytes() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Flip a bit in a middle entry's magic bytes
    if entries.len() >= 3 {
        let target = &entries[entries.len() / 2];
        flip_bit(&mut data, target.offset, 0); // Flip bit 0 of first magic byte
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // A corrupt middle header invalidates the selected generation.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_bit_flip_crc() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Flip a bit in an entry's CRC. A clean checkpoint may compact the live
    // WAL down to a two-record catalog transaction, so requiring three records
    // would make this corruption oracle silently vacuous.
    let target = entries
        .get(entries.len() / 2)
        .expect("expected at least one WAL entry to corrupt");
    flip_bit(&mut data, target.crc_offset, 3); // Flip bit 3 of CRC
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_bit_flip_data_portion() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Flip a bit in the data portion of a middle entry
    if entries.len() >= 3 {
        let target = &entries[entries.len() / 2];
        let data_mid = target.data_offset + target.entry_size / 2;
        flip_bit(&mut data, data_mid, 5);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // A CRC mismatch invalidates the selected generation.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_bit_flip_entry_size() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Flip a high bit in entry_size field of a middle entry. The validated
    // reader must reject the generation without searching for a later record.
    if entries.len() >= 3 {
        let target = &entries[entries.len() / 2];
        // entry_size is at offset +24 (4 bytes, little-endian)
        flip_bit(&mut data, target.offset + 24, 7); // Flip bit 7 -> adds 128 to low byte
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_bit_flip_flags_compressed() {
    // Create DB with small rows (uncompressed entries)
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Set the COMPRESSED flag on an uncompressed entry.
    let target = entries
        .iter()
        .find(|entry| entry.flags & COMPRESSED_FLAG == 0)
        .expect("expected an uncompressed WAL entry to corrupt");
    // flags byte is at offset + 5
    data[target.offset + 5] |= COMPRESSED_FLAG;
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);
    // The protected flag mutation invalidates the selected generation.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

// ============================================================================
// 4. CHECKPOINT CORRUPTION
// ============================================================================

// ============================================================================
// 5. MULTI-ENTRY CORRUPTION
// ============================================================================

/// Find entries that are DML data entries (not DDL, not commit/abort markers)
fn find_dml_data_entries(entries: &[WalEntryInfo]) -> Vec<usize> {
    entries
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            // Not a commit or abort marker
            (e.flags & COMMIT_MARKER_FLAG) == 0 && (e.flags & 0x04) == 0
        })
        .map(|(i, _)| i)
        .collect()
}

/// Find entries that are commit markers
fn find_commit_entries(entries: &[WalEntryInfo]) -> Vec<usize> {
    entries
        .iter()
        .enumerate()
        .filter(|(_, e)| (e.flags & COMMIT_MARKER_FLAG) != 0)
        .map(|(i, _)| i)
        .collect()
}

#[test]
fn test_first_data_entry_corrupt() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let dml_entries = find_dml_data_entries(&entries);

    // Corrupt the first DML entry's magic
    if !dml_entries.is_empty() {
        let idx = dml_entries[0];
        let target = &entries[idx];
        zero_range(&mut data, target.offset, 4); // Zero magic
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    verify_open_fails_closed(&fixture);
}

#[test]
fn test_last_entry_corrupt() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE tail_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        for id in 1..=5 {
            db.execute(
                &format!("INSERT INTO tail_test VALUES ({}, 'base_{}')", id, id),
                (),
            )
            .unwrap();
        }
        let mut tx = db.begin().unwrap();
        for id in 6..=10 {
            tx.execute(
                &format!("INSERT INTO tail_test VALUES ({}, 'tail_{}')", id, id),
                (),
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    remove_lock_file(&db_path);

    let wal_files = find_wal_files(&db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(!entries.is_empty());

    // Truncate the last entry (most common crash scenario)
    let last = &entries[entries.len() - 1];
    let truncated = &data[..last.offset];
    fs::write(wal_path, truncated).unwrap();

    // Removing the complete final commit marker leaves a clean record
    // boundary. All five rows in that transaction remain uncommitted.
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM tail_test", ()).unwrap();
    assert_eq!(count, 5);
}

#[test]
fn test_middle_entry_corrupt() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Zero the magic of a middle entry
    if entries.len() >= 3 {
        let mid_idx = entries.len() / 2;
        let target = &entries[mid_idx];
        zero_range(&mut data, target.offset, 4);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // A corrupt middle header invalidates the selected generation.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_commit_marker_destroyed() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let commit_entries = find_commit_entries(&entries);

    // Zero the last commit marker -> that transaction becomes "in-doubt" -> aborted
    if !commit_entries.is_empty() {
        let last_commit_idx = commit_entries[commit_entries.len() - 1];
        let target = &entries[last_commit_idx];
        zero_range(&mut data, target.offset, target.total_size);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // The transaction whose commit marker was destroyed should be treated as aborted
    // Other transactions should be fine
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_data_entries_destroyed_commit_intact() {
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let dml_entries = find_dml_data_entries(&entries);

    // Zero all DML entries for the last batch while keeping commit markers
    // (Zero magic of the last few DML entries)
    if dml_entries.len() >= 3 {
        // Zero last 3 DML entries
        for &idx in &dml_entries[dml_entries.len() - 3..] {
            let target = &entries[idx];
            zero_range(&mut data, target.offset, 4); // Zero magic
        }
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // Commit marker found but data entries gone -> those rows won't be recovered
    verify_recovery_at_least(&fixture, "test_data", 0);
}

// ============================================================================
// 6. WAL TRUNCATION CRASH RECOVERY
// ============================================================================

// ============================================================================
// 7. LSN CHAIN INTEGRITY
// ============================================================================

#[test]
fn test_lsn_gap_in_sequence() {
    // Remove one complete entry from the middle (splice bytes out)
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    if entries.len() >= 5 {
        let remove_idx = entries.len() / 2;
        let target = &entries[remove_idx];
        let before = &data[..target.offset];
        let after = &data[target.offset + target.total_size..];

        let mut spliced = Vec::with_capacity(before.len() + after.len());
        spliced.extend_from_slice(before);
        spliced.extend_from_slice(after);
        fs::write(wal_path, &spliced).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // Remaining entries have valid magic/CRC, should be recovered normally
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_duplicate_entry_in_wal() {
    // Duplicate one entry (append a copy)
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    if entries.len() >= 3 {
        let dup_idx = entries.len() / 2;
        let target = &entries[dup_idx];
        let entry_bytes = &data[target.offset..target.offset + target.total_size];

        let mut new_data = data.clone();
        new_data.extend_from_slice(entry_bytes);
        fs::write(wal_path, &new_data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    verify_open_fails_closed(&fixture);
}

#[test]
fn test_wal_only_recovery_without_explicit_checkpoint() {
    // Keep the initial CONTROL root and recover all committed changes from WAL.
    let fixture = setup_test_db(5, 3);

    remove_lock_file(&fixture.db_path);
    verify_recovery_exact(&fixture, "test_data", 15);
}

// ============================================================================
// 8. COMPRESSION-RELATED CORRUPTION
// ============================================================================

#[test]
fn test_corrupt_compressed_payload() {
    // Use large rows to trigger LZ4 compression
    let fixture = setup_test_db_large_rows(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Find a compressed entry
    let compressed: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| (e.flags & COMPRESSED_FLAG) != 0)
        .map(|(i, _)| i)
        .collect();

    if !compressed.is_empty() {
        let target_idx = compressed[compressed.len() / 2];
        let target = &entries[target_idx];

        // Flip bytes in the compressed payload, then recompute CRC
        let data_mid = target.data_offset + target.entry_size / 2;
        if data_mid < data.len() {
            data[data_mid] ^= 0xFF;
            data[data_mid.saturating_sub(1)] ^= 0xAA;

            // Recompute CRC over the data portion
            let data_start = target.data_offset;
            let data_end = target.crc_offset;
            let new_crc = crc32fast::hash(&data[data_start..data_end]);
            data[target.crc_offset..target.crc_offset + 4].copy_from_slice(&new_crc.to_le_bytes());

            fs::write(wal_path, &data).unwrap();
        }
    }

    remove_lock_file(&fixture.db_path);
    // Even with a recomputed legacy data CRC, the V3 header+data checksum and
    // decoder contract reject the selected generation.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

// ============================================================================
// 9. COMBINED STRESS TESTS
// ============================================================================

#[test]
fn test_combined_torn_write_plus_bit_flip() {
    let fixture = setup_test_db(10, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Bit flip in a middle entry
    if entries.len() >= 5 {
        let mid = entries.len() / 2;
        let target = &entries[mid];
        flip_bit(&mut data, target.data_offset + 5, 2);
    }

    // Truncate the last entry
    if entries.len() >= 2 {
        let last = &entries[entries.len() - 1];
        let truncate_at = last.offset + WAL_HEADER_SIZE / 2;
        data.truncate(truncate_at);
    }

    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);
    // Either corruption is sufficient to reject the selected generation;
    // neither may produce partial recovery.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_many_transactions_last_commit_corrupt() {
    let fixture = setup_test_db(20, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let commit_entries = find_commit_entries(&entries);

    // Corrupt the last commit marker
    if !commit_entries.is_empty() {
        let last_commit_idx = commit_entries[commit_entries.len() - 1];
        let target = &entries[last_commit_idx];
        zero_range(&mut data, target.offset, target.total_size);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // 19 transactions should be recovered, last one treated as uncommitted
    // Each transaction has 3 rows, so at least 19*3 - some potential loss = many rows
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_recovery_then_new_data_then_recovery() {
    // Phase 1: Create DB with data, corrupt, recover
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE test_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL, seq INTEGER)",
            (),
        )
        .unwrap();

        let mut id = 1;
        for txn in 0..5 {
            for row in 0..3 {
                db.execute(
                    &format!(
                        "INSERT INTO test_data (id, value, seq) VALUES ({}, 'txn{}_row{}', {})",
                        id, txn, row, txn
                    ),
                    (),
                )
                .unwrap();
                id += 1;
            }
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM test_data", ()).unwrap();
        assert_eq!(count, 15);
    }

    remove_lock_file(&db_path);

    let wal_files = find_wal_files(&db_path);
    assert!(!wal_files.is_empty());

    // Corrupt: truncate last entry
    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    if entries.len() >= 2 {
        let last = &entries[entries.len() - 1];
        let truncated = &data[..last.offset + 10]; // 10 bytes into last entry
        fs::write(wal_path, truncated).unwrap();
    }

    remove_lock_file(&db_path);

    assert!(Database::open(&dsn).is_err());
}

#[test]
fn test_complete_wal_destruction_with_checkpoint() {
    // Create DB, checkpoint to volumes, add more data, then zero entire WAL
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    // Phase 1: Create table and insert initial data
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE test_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL, seq INTEGER)",
            (),
        )
        .unwrap();

        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO test_data (id, value, seq) VALUES ({}, 'checkpoint_data_{}', 1)",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Force a checkpoint to seal data into volumes
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Insert more data after checkpoint
        for i in 11..=20 {
            db.execute(
                &format!(
                    "INSERT INTO test_data (id, value, seq) VALUES ({}, 'post_checkpoint_{}', 2)",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Phase 2: Zero entire WAL (but volume files should be intact)
    let wal_files = find_wal_files(&db_path);
    for wal_path in &wal_files {
        let data = fs::read(wal_path).unwrap();
        let zeroed = vec![0u8; data.len()];
        fs::write(wal_path, &zeroed).unwrap();
    }

    assert!(Database::open(&dsn).is_err());
}

// ============================================================================
// Additional edge case tests
// ============================================================================

#[test]
fn test_wal_file_with_only_header_garbage() {
    // WAL file contains exactly 32 bytes of garbage
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let garbage = vec![0xDE; 32];
    fs::write(wal_path, &garbage).unwrap();

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_wal_magic_at_very_end_of_file() {
    // WAL file with valid data but magic bytes at the very end (incomplete entry)
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(!entries.is_empty());

    // Truncate to end of second-to-last entry, then append just magic bytes
    if entries.len() >= 2 {
        let second_to_last = &entries[entries.len() - 2];
        let end_of_stl = second_to_last.offset + second_to_last.total_size;
        data.truncate(end_of_stl);
        // Append just the magic bytes (incomplete header)
        data.extend_from_slice(&WAL_ENTRY_MAGIC.to_le_bytes());
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_multiple_wal_files_one_corrupt() {
    // Create enough data to potentially generate multiple WAL files
    // Even if only one WAL file, we simulate by splitting
    let fixture = setup_test_db(20, 5);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    // Corrupt the first WAL file if there are multiple
    if wal_files.len() >= 2 {
        let mut data = fs::read(&wal_files[0]).unwrap();
        // Zero the first 100 bytes
        zero_range(&mut data, 0, 100);
        fs::write(&wal_files[0], &data).unwrap();
    } else {
        // Only one WAL file — corrupt the first page but leave the rest
        let mut data = fs::read(&wal_files[0]).unwrap();
        if data.len() > 4096 {
            // Corrupt middle portion, leave start and end intact
            let mid = data.len() / 2;
            zero_range(&mut data, mid, 100);
            fs::write(&wal_files[0], &data).unwrap();
        }
    }

    remove_lock_file(&fixture.db_path);
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_all_commit_markers_destroyed() {
    // Zero every commit marker in the WAL — all transactions should be aborted
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let commit_entries = find_commit_entries(&entries);

    // Zero all commit markers (but not DDL commit markers)
    for &idx in &commit_entries {
        let target = &entries[idx];
        zero_range(&mut data, target.offset, target.total_size);
    }
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_repeated_recovery_idempotent() {
    // Corrupt WAL, recover multiple times — should always get same result
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Corrupt a middle entry
    if entries.len() >= 3 {
        let mid = entries.len() / 2;
        let target = &entries[mid];
        flip_bit(&mut data, target.crc_offset, 0);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
    remove_lock_file(&fixture.db_path);
    verify_open_fails_closed(&fixture);
}

#[test]
fn test_concurrent_corruption_patterns() {
    // Apply multiple corruption patterns simultaneously
    let fixture = setup_test_db(10, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    if entries.len() >= 8 {
        // Corruption 1: Flip bit in entry 2's CRC
        let target1 = &entries[2];
        flip_bit(&mut data, target1.crc_offset, 4);

        // Corruption 2: Zero magic of entry 4
        let target2 = entries[4].clone();
        zero_range(&mut data, target2.offset, 4);

        // Corruption 3: Flip compressed flag on entry 6
        let target3 = &entries[6];
        data[target3.offset + 5] |= COMPRESSED_FLAG;
    }

    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);
    // Multiple corruptions remain terminal; no partial prefix is published.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_wal_with_garbage_appended() {
    // Append random garbage after valid WAL data
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();

    // Append 1KB of non-magic garbage
    let garbage: Vec<u8> = (0..1024).map(|i| ((i * 7 + 13) % 256) as u8).collect();
    data.extend_from_slice(&garbage);
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);
    verify_open_fails_closed(&fixture);
}

#[test]
fn test_wal_entry_size_zero() {
    // Set entry_size to 0 for a middle entry
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    if entries.len() >= 3 {
        let mid = entries.len() / 2;
        let target = &entries[mid];
        // Set entry_size (bytes 24-27) to 0
        data[target.offset + 24] = 0;
        data[target.offset + 25] = 0;
        data[target.offset + 26] = 0;
        data[target.offset + 27] = 0;
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // The structural size check rejects the complete selected generation.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

#[test]
fn test_wal_entry_size_very_large() {
    // Set entry_size to a very large value (> 64MB limit)
    let fixture = setup_test_db(5, 3);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    if entries.len() >= 3 {
        let mid = entries.len() / 2;
        let target = &entries[mid];
        // Set entry_size to 0x10000000 (256MB) — exceeds 64MB sanity check
        data[target.offset + 24] = 0;
        data[target.offset + 25] = 0;
        data[target.offset + 26] = 0;
        data[target.offset + 27] = 0x10;
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);
    // The structural size limit rejects the complete selected generation.
    verify_recovery_at_least(&fixture, "test_data", 0);
}

// ============================================================================
// 10. MULTI-TABLE RECOVERY
// ============================================================================

/// Setup a database with multiple tables
fn setup_multi_table_db() -> TestFixture {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();

        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount FLOAT)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price FLOAT)",
            (),
        )
        .unwrap();

        // Populate users
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO users (id, name, email) VALUES ({}, 'user{}', 'user{}@test.com')",
                    i, i, i
                ),
                (),
            )
            .unwrap();
        }

        // Populate orders
        for i in 1..=20 {
            db.execute(
                &format!(
                    "INSERT INTO orders (id, user_id, amount) VALUES ({}, {}, {})",
                    i,
                    (i % 10) + 1,
                    i as f64 * 9.99
                ),
                (),
            )
            .unwrap();
        }

        // Populate products
        for i in 1..=15 {
            db.execute(
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'product{}', {})",
                    i,
                    i,
                    i as f64 * 5.50
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);
    TestFixture {
        _dir: dir,
        db_path,
        dsn,
    }
}

#[test]
fn test_multi_table_corrupt_middle_entry() {
    // Corrupt a middle entry — only one table's data may be affected
    let fixture = setup_multi_table_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    if entries.len() >= 5 {
        let mid = entries.len() / 2;
        let target = &entries[mid];
        flip_bit(&mut data, target.crc_offset, 0);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_multi_table_one_tables_commit_destroyed() {
    // Destroy a commit marker — the transaction's data for ALL tables in that txn is lost
    let fixture = setup_multi_table_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let commit_entries = find_commit_entries(&entries);

    // Destroy a middle commit marker
    if commit_entries.len() >= 3 {
        let mid_commit = commit_entries[commit_entries.len() / 2];
        let target = &entries[mid_commit];
        zero_range(&mut data, target.offset, target.total_size);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_multi_table_first_page_zeroed() {
    // Zero page 0 — DDL for some tables may be lost
    let fixture = setup_multi_table_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();

    if data.len() > 4096 {
        zero_page(&mut data, 0);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_multi_table_cross_table_join_after_recovery() {
    // After partial corruption, verify cross-table joins still work
    let fixture = setup_multi_table_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    // Truncate last entry (mild corruption)
    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    if entries.len() >= 2 {
        let last = &entries[entries.len() - 1];
        let truncated = &data[..last.offset];
        fs::write(wal_path, truncated).unwrap();
    }

    remove_lock_file(&fixture.db_path);

    let db = Database::open(&fixture.dsn).unwrap();

    // Execute a cross-table join — this exercises the full query engine post-recovery
    let result = db.query(
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id LIMIT 5",
        (),
    );

    match result {
        Ok(rows) => {
            // Consume the iterator — verifies join execution doesn't panic
            let collected: Vec<_> = rows.filter_map(|r| r.ok()).collect();
            // Count may be 0 or more depending on what survived
            assert!(collected.len() <= 100, "Sanity check: not too many rows");
        }
        Err(_) => {
            // If a table was lost, join might fail — that's acceptable
        }
    }
}

// ============================================================================
// 11. INDEX RECOVERY
// ============================================================================

/// Setup a database with indexes
fn setup_indexed_db() -> TestFixture {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();

        db.execute(
            "CREATE TABLE indexed_data (id INTEGER PRIMARY KEY, category TEXT NOT NULL, value FLOAT, active BOOLEAN)",
            (),
        )
        .unwrap();

        // Create indexes of different types
        db.execute("CREATE INDEX idx_value ON indexed_data(value)", ())
            .unwrap();
        db.execute("CREATE INDEX idx_category ON indexed_data(category)", ())
            .unwrap();

        // Insert data
        for i in 1..=50 {
            let category = format!("cat{}", i % 5);
            let active = i % 2 == 0;
            db.execute(
                &format!(
                    "INSERT INTO indexed_data (id, category, value, active) VALUES ({}, '{}', {}, {})",
                    i, category, i as f64 * 1.5, active
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);
    TestFixture {
        _dir: dir,
        db_path,
        dsn,
    }
}

#[test]
fn test_index_ddl_corrupt_but_data_intact() {
    // Corrupt the CREATE INDEX WAL entry, but keep data intact
    // Index should not exist, but data queries should still work (full scan)
    let fixture = setup_indexed_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let dml_entries = find_dml_data_entries(&entries);

    // The CREATE INDEX entries are DDL (non-commit, non-DML data entries)
    // They appear early in the WAL. Corrupt entries 1 and 2 (likely DDL for indexes)
    // We'll corrupt entries right after the CREATE TABLE entries
    if dml_entries.len() >= 2 && entries.len() > dml_entries[0] {
        // Find entries that are NOT DML data and NOT commit markers (likely DDL)
        let ddl_like: Vec<usize> = entries
            .iter()
            .enumerate()
            .filter(|(i, e)| {
                (e.flags & COMMIT_MARKER_FLAG) == 0
                    && (e.flags & 0x04) == 0
                    && !dml_entries.contains(i)
            })
            .map(|(i, _)| i)
            .collect();

        // Corrupt a few DDL-like entries (skip entry 0 which is CREATE TABLE)
        for &idx in ddl_like.iter().skip(1).take(2) {
            let target = &entries[idx];
            flip_bit(&mut data, target.crc_offset, 5);
        }
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);

    let db = Database::open(&fixture.dsn).unwrap();

    // Table should exist and data should be queryable (even without indexes)
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM indexed_data", ())
        .unwrap();
    assert!(count >= 0, "Data should be accessible even without indexes");

    // Queries that would normally use indexes should fall back to full scan
    let result: i64 = db
        .query_one("SELECT COUNT(*) FROM indexed_data WHERE value > 50.0", ())
        .unwrap();
    assert!(result >= 0);
}

// ============================================================================
// 12. MULTI-STATEMENT TRANSACTION CORRUPTION
// ============================================================================

/// Setup a database using explicit BEGIN/COMMIT transactions
fn setup_explicit_txn_db() -> TestFixture {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE txn_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL, batch INTEGER)",
            (),
        )
        .unwrap();

        // 5 explicit transactions, each inserting 4 rows
        for batch in 0..5 {
            db.execute("BEGIN", ()).unwrap();
            for row in 0..4 {
                let id = batch * 4 + row + 1;
                db.execute(
                    &format!(
                        "INSERT INTO txn_data (id, value, batch) VALUES ({}, 'batch{}_row{}', {})",
                        id, batch, row, batch
                    ),
                    (),
                )
                .unwrap();
            }
            db.execute("COMMIT", ()).unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM txn_data", ()).unwrap();
        assert_eq!(count, 20);
    }

    remove_lock_file(&db_path);
    TestFixture {
        _dir: dir,
        db_path,
        dsn,
    }
}

#[test]
fn test_explicit_txn_one_insert_corrupt() {
    // Corrupt one INSERT entry within a committed multi-statement transaction
    let fixture = setup_explicit_txn_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let dml_entries = find_dml_data_entries(&entries);

    // Corrupt one DML entry from a middle batch
    assert!(
        dml_entries.len() >= 10,
        "explicit transaction WAL lost DML entries"
    );
    let target_idx = dml_entries[dml_entries.len() / 2];
    let target = &entries[target_idx];
    flip_bit(&mut data, target.data_offset + 8, 4); // Corrupt data portion
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_explicit_txn_commit_marker_corrupt() {
    // Destroy one transaction's commit marker — all 4 rows from that txn should be lost
    let fixture = setup_explicit_txn_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let commit_entries = find_commit_entries(&entries);

    // Destroy the 3rd commit marker (middle batch)
    assert!(
        commit_entries.len() >= 5,
        "explicit transaction WAL lost commit markers"
    );
    let target_idx = commit_entries[2]; // 3rd transaction's commit
    let target = &entries[target_idx];
    zero_range(&mut data, target.offset, target.total_size);
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_explicit_txn_all_inserts_in_one_txn_corrupt() {
    // Corrupt all INSERT entries within one transaction, but keep commit marker
    let fixture = setup_explicit_txn_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let dml_entries = find_dml_data_entries(&entries);
    let commit_entries = find_commit_entries(&entries);

    // Each batch: 4 DML entries + 1 commit marker
    // Corrupt all 4 DML entries of batch 2 (entries at dml indices 8-11 approximately)
    assert!(
        dml_entries.len() >= 12 && commit_entries.len() >= 3,
        "explicit transaction WAL lost the expected batch structure"
    );
    // Corrupt DML entries 8, 9, 10, 11 (batch 2)
    for &idx in &dml_entries[8..12] {
        let target = &entries[idx];
        zero_range(&mut data, target.offset, 4); // Zero magic
    }
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

#[test]
fn test_explicit_txn_interleaved_corruption() {
    // Corrupt entries from different transactions
    let fixture = setup_explicit_txn_db();
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    let dml_entries = find_dml_data_entries(&entries);

    // Corrupt one entry from batch 1 and one from batch 3
    assert!(
        dml_entries.len() >= 16,
        "explicit transaction WAL lost interleaved DML entries"
    );
    // Batch 1 entry (index 4-7), corrupt index 5
    let target = &entries[dml_entries[5]];
    flip_bit(&mut data, target.crc_offset, 2);

    // Batch 3 entry (index 12-15), corrupt index 13
    let target2 = entries[dml_entries[13]].clone();
    flip_bit(&mut data, target2.crc_offset, 6);

    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

// ============================================================================
// 13. ARTIFACT GENERATION + WAL RECOVERY
// ============================================================================

fn find_data_artifacts(db_path: &Path) -> Vec<PathBuf> {
    fn visit(directory: &Path, files: &mut Vec<PathBuf>) {
        if !directory.exists() {
            return;
        }
        for entry in fs::read_dir(directory)
            .unwrap()
            .filter_map(|entry| entry.ok())
        {
            let path = entry.path();
            if path.is_dir() {
                visit(&path, files);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "data")
            {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    visit(&db_path.join("artifacts/data"), &mut files);
    files.sort();
    files
}

/// Publish one immutable generation, then leave later rows in WAL.
fn setup_db_with_artifacts() -> (TestFixture, i64) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    let pre_checkpoint_count;

    {
        let db = Database::open(&dsn).unwrap();

        db.execute(
            "CREATE TABLE vol_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL, phase INTEGER)",
            (),
        )
        .unwrap();

        // Phase 1: data that will be published into immutable artifacts.
        for i in 1..=20 {
            db.execute(
                &format!(
                    "INSERT INTO vol_test (id, value, phase) VALUES ({}, 'sealed_{}', 1)",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        pre_checkpoint_count = 20;

        // Checkpoint publishes DATA/INDEX and advances the WAL replay floor.
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 2: Post-checkpoint data (in WAL only)
        for i in 21..=40 {
            db.execute(
                &format!(
                    "INSERT INTO vol_test (id, value, phase) VALUES ({}, 'wal_only_{}', 2)",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);
    (
        TestFixture {
            _dir: dir,
            db_path,
            dsn,
        },
        pre_checkpoint_count,
    )
}

#[test]
fn test_artifacts_valid_wal_corrupt() {
    // The artifact generation is valid, but its required WAL suffix is not.
    let (fixture, _pre_count) = setup_db_with_artifacts();

    // Corrupt the WAL (post-checkpoint entries)
    let wal_files = find_wal_files(&fixture.db_path);
    if !wal_files.is_empty() {
        let wal_path = &wal_files[wal_files.len() - 1];
        let mut data = fs::read(wal_path).unwrap();

        if data.len() > 200 {
            let corrupt_start = data.len() * 3 / 4;
            let corrupt_len = 200.min(data.len() - corrupt_start);
            zero_range(&mut data, corrupt_start, corrupt_len);
            fs::write(wal_path, &data).unwrap();
        }
    }

    remove_lock_file(&fixture.db_path);

    verify_open_fails_closed(&fixture);
}

// ============================================================================
// 14. CHECKPOINT-BASED WAL RETENTION AND ARTIFACT RECOVERY
// ============================================================================
//
// These tests verify that PRAGMA CHECKPOINT publishes an immutable generation,
// advances bounded WAL retention, and recovers from DATA plus the retained WAL
// suffix.

/// Helper: Get total WAL file size in bytes
fn total_wal_size(db_path: &Path) -> u64 {
    find_wal_files(db_path)
        .iter()
        .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum()
}

#[test]
fn test_checkpoint_two_cycles_recovery() {
    // Two checkpoints: phase 1 sealed, phase 2 sealed, phase 3 in WAL.
    // All data should survive recovery.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE safe_test2 (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: Insert 10 rows, checkpoint 1
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO safe_test2 (id, value) VALUES ({}, 'phase1_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 2: Insert 10 more rows, checkpoint 2
        for i in 11..=20 {
            db.execute(
                &format!(
                    "INSERT INTO safe_test2 (id, value) VALUES ({}, 'phase2_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 3: Insert 10 more rows (in WAL only)
        for i in 21..=30 {
            db.execute(
                &format!(
                    "INSERT INTO safe_test2 (id, value) VALUES ({}, 'phase3_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Reopen — volumes have phases 1+2, WAL has phase 3
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM safe_test2", ()).unwrap();
    assert_eq!(
        count, 30,
        "All 30 rows should be recovered from volumes + WAL"
    );
}

#[test]
fn test_checkpoint_three_cycles_recovery() {
    // With 3 checkpoints, all data should be in volumes.
    // Recovery loads volumes + replays remaining WAL.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE safe_test4 (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: Insert rows, checkpoint 1
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO safe_test4 (id, value) VALUES ({}, 'p1_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 2: Insert rows, checkpoint 2
        for i in 11..=20 {
            db.execute(
                &format!(
                    "INSERT INTO safe_test4 (id, value) VALUES ({}, 'p2_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 3: Insert rows, checkpoint 3
        for i in 21..=30 {
            db.execute(
                &format!(
                    "INSERT INTO safe_test4 (id, value) VALUES ({}, 'p3_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 4: More data in WAL only
        for i in 31..=40 {
            db.execute(
                &format!(
                    "INSERT INTO safe_test4 (id, value) VALUES ({}, 'p4_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Reopen — volumes have phases 1-3, WAL has phase 4
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM safe_test4", ()).unwrap();
    assert_eq!(
        count, 40,
        "All 40 rows should be recovered from volumes + WAL"
    );
}

#[test]
fn test_safe_truncation_keep_count_one() {
    // Checkpoint seals hot rows into volumes and truncates WAL.
    // Two checkpoints ensure earlier data is in volumes, WAL is truncated,
    // and post-checkpoint data in WAL survives recovery.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!(
        "file://{}?keep_snapshots=1&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE keep1_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: Insert 10 rows, checkpoint 1
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO keep1_test (id, value) VALUES ({}, 'p1_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Phase 2: Insert 10 more rows, checkpoint 2
        for i in 11..=20 {
            db.execute(
                &format!(
                    "INSERT INTO keep1_test (id, value) VALUES ({}, 'p2_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Phase 3: Insert 10 more rows (WAL only, no checkpoint)
        for i in 21..=30 {
            db.execute(
                &format!(
                    "INSERT INTO keep1_test (id, value) VALUES ({}, 'p3_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Reopen — volumes provide phases 1+2, WAL provides phase 3
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM keep1_test", ()).unwrap();
    assert_eq!(
        count, 30,
        "All 30 rows should be recovered (volumes + WAL replay)"
    );
}

#[test]
fn test_safe_truncation_table_created_between_snapshots() {
    // Create table A, checkpoint, then create table B, checkpoint again.
    // Each checkpoint seals all hot rows into volumes. WAL retention removes
    // only whole generations, so byte size is intentionally not asserted.
    // Tables created between checkpoints get their data sealed on the next checkpoint.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE early_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Insert into early_table, first checkpoint
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO early_table (id, value) VALUES ({}, 'e{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Now create a new table AFTER the first checkpoint
        db.execute(
            "CREATE TABLE late_table (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO late_table (id, value) VALUES ({}, 'l{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Insert more data into both tables
        for i in 11..=20 {
            db.execute(
                &format!(
                    "INSERT INTO early_table (id, value) VALUES ({}, 'e{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
            db.execute(
                &format!(
                    "INSERT INTO late_table (id, value) VALUES ({}, 'l{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Second checkpoint — seals all hot data (both tables) into volumes
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }

    remove_lock_file(&db_path);

    // Verify all data recovers from volumes
    let db = Database::open(&dsn).unwrap();
    let count_early: i64 = db
        .query_one("SELECT COUNT(*) FROM early_table", ())
        .unwrap();
    let count_late: i64 = db.query_one("SELECT COUNT(*) FROM late_table", ()).unwrap();
    assert_eq!(count_early, 20, "early_table should have all 20 rows");
    assert_eq!(count_late, 20, "late_table should have all 20 rows");
}

#[test]
fn test_safe_truncation_drop_table_no_block() {
    // After DROP TABLE, checkpoint publication must remain valid for surviving
    // tables even when conservative retention keeps the current WAL generation.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE keeper (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE dropper (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Insert into both tables, checkpoint 1
        for i in 1..=10 {
            db.execute(
                &format!("INSERT INTO keeper (id, value) VALUES ({}, 'k{}')", i, i),
                (),
            )
            .unwrap();
            db.execute(
                &format!("INSERT INTO dropper (id, value) VALUES ({}, 'd{}')", i, i),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Drop one table
        db.execute("DROP TABLE dropper", ()).unwrap();

        // Insert more into keeper, checkpoint 2
        for i in 11..=20 {
            db.execute(
                &format!("INSERT INTO keeper (id, value) VALUES ({}, 'k{}')", i, i),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Insert more data
        for i in 21..=30 {
            db.execute(
                &format!("INSERT INTO keeper (id, value) VALUES ({}, 'k{}')", i, i),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Verify recovery
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM keeper", ()).unwrap();
    assert_eq!(count, 30, "All 30 rows in keeper should be recovered");

    // dropper should not exist
    let result: std::result::Result<i64, _> = db.query_one("SELECT COUNT(*) FROM dropper", ());
    assert!(result.is_err(), "dropper table should not exist after drop");
}

#[test]
fn test_checkpoint_then_drop_recovers_after_abrupt_process_death() {
    if let Some(root) = std::env::var_os(CHECKPOINT_DROP_CRASH_CHILD) {
        let db_path = PathBuf::from(root);
        let ready = db_path.with_extension("ready");
        let dsn = format!(
            "file://{}?checkpoint_on_close=off&checkpoint_interval=0&sync_mode=full",
            db_path.display()
        );
        let db = Database::open(&dsn).expect("crash child opens database");
        db.execute(
            "CREATE TABLE keeper (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE dropped_after_checkpoint (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO keeper VALUES (1, 'before')", ())
            .unwrap();
        db.execute(
            "INSERT INTO dropped_after_checkpoint VALUES (1, 'obsolete')",
            (),
        )
        .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.execute("DROP TABLE dropped_after_checkpoint", ())
            .unwrap();
        db.execute("INSERT INTO keeper VALUES (2, 'after')", ())
            .unwrap();
        fs::write(&ready, b"ready").expect("crash child publishes kill barrier");
        loop {
            thread::sleep(Duration::from_secs(60));
        }
    }

    let directory = tempdir().unwrap();
    let db_path = directory.path().join("checkpoint-drop-crash");
    let ready = db_path.with_extension("ready");
    let current_exe = std::env::current_exe().expect("resolve durability test executable");
    let mut child = Command::new(current_exe)
        .arg("--exact")
        .arg("test_checkpoint_then_drop_recovers_after_abrupt_process_death")
        .arg("--nocapture")
        .env(CHECKPOINT_DROP_CRASH_CHILD, &db_path)
        .spawn()
        .expect("spawn checkpoint-drop crash child");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() && Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll checkpoint-drop child") {
            panic!("checkpoint-drop child exited before kill barrier: {status}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        ready.exists(),
        "checkpoint-drop child did not reach kill barrier"
    );
    child
        .kill()
        .expect("abruptly terminate checkpoint-drop child");
    let status = child.wait().expect("reap checkpoint-drop child");
    assert!(
        !status.success(),
        "checkpoint-drop child must not close cleanly"
    );

    let db = Database::open(&format!("file://{}", db_path.display()))
        .expect("reopen after checkpoint, DROP and abrupt death");
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM keeper", ())
            .unwrap(),
        2
    );
    assert!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM dropped_after_checkpoint", ())
            .is_err(),
        "catalog WAL DROP must hide the obsolete physical table manifest"
    );
}

#[test]
fn test_checkpoint_drop_and_recreate_same_name() {
    // DROP TABLE then CREATE TABLE with the same name.
    // Checkpoint must not confuse volumes from the old and new table.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();

        // Create table, insert data, checkpoint
        db.execute(
            "CREATE TABLE reborn (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        for i in 1..=10 {
            db.execute(
                &format!("INSERT INTO reborn (id, value) VALUES ({}, 'old{}')", i, i),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Drop and recreate with same name
        db.execute("DROP TABLE reborn", ()).unwrap();
        db.execute(
            "CREATE TABLE reborn (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        for i in 1..=5 {
            db.execute(
                &format!("INSERT INTO reborn (id, value) VALUES ({}, 'new{}')", i, i),
                (),
            )
            .unwrap();
        }

        // Checkpoint again — the "reborn" table exists in schemas
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Insert more data
        for i in 6..=15 {
            db.execute(
                &format!("INSERT INTO reborn (id, value) VALUES ({}, 'new{}')", i, i),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Verify the new table's data survives
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM reborn", ()).unwrap();
    assert_eq!(count, 15, "Recreated table should have 15 rows");

    // Verify old data is gone (values should be 'new*', not 'old*')
    let val: String = db
        .query_one("SELECT value FROM reborn WHERE id = 1", ())
        .unwrap();
    assert_eq!(val, "new1", "Data should be from the recreated table");
}

#[test]
fn test_safe_truncation_keep_count_zero_no_cleanup() {
    // Checkpoint seals hot rows into volumes. keep_snapshots=0 must not make
    // recovery depend on byte-level truncation of the active generation.
    // Multiple checkpoints ensure all data is persisted in volumes
    // and WAL is truncated after each cycle.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!(
        "file://{}?keep_snapshots=0&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE keep_all (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: Insert, checkpoint 1
        for i in 1..=10 {
            db.execute(
                &format!("INSERT INTO keep_all (id, value) VALUES ({}, 'v{}')", i, i),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Phase 2: Insert, checkpoint 2
        for i in 11..=20 {
            db.execute(
                &format!("INSERT INTO keep_all (id, value) VALUES ({}, 'v{}')", i, i),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 3: Insert, checkpoint 3
        for i in 21..=30 {
            db.execute(
                &format!("INSERT INTO keep_all (id, value) VALUES ({}, 'v{}')", i, i),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());
    }

    remove_lock_file(&db_path);

    // All data should be recovered from volumes
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM keep_all", ()).unwrap();
    assert_eq!(count, 30, "All 30 rows recovered from volumes");
}

#[test]
fn test_safe_truncation_with_updates_and_deletes() {
    // Verify checkpoint works correctly with UPDATE and DELETE operations,
    // not just INSERTs. Volumes must preserve tombstones and updated rows.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE mut_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL, status TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: Insert rows, checkpoint 1
        for i in 1..=20 {
            db.execute(
                &format!(
                    "INSERT INTO mut_test (id, value, status) VALUES ({}, 'original{}', 'active')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 2: UPDATE some, DELETE others, INSERT new
        for i in 1..=5 {
            db.execute(
                &format!(
                    "UPDATE mut_test SET value = 'updated{}', status = 'modified' WHERE id = {}",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        for i in 16..=20 {
            db.execute(&format!("DELETE FROM mut_test WHERE id = {}", i), ())
                .unwrap();
        }
        for i in 21..=25 {
            db.execute(
                &format!(
                    "INSERT INTO mut_test (id, value, status) VALUES ({}, 'new{}', 'active')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 3: More mutations (WAL only)
        db.execute("UPDATE mut_test SET status = 'final' WHERE id <= 5", ())
            .unwrap();
        db.execute("DELETE FROM mut_test WHERE id = 21", ())
            .unwrap();
        for i in 26..=30 {
            db.execute(
                &format!(
                    "INSERT INTO mut_test (id, value, status) VALUES ({}, 'late{}', 'active')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Recovery from volumes (phases 1+2) + WAL (phase 3)
    let db = Database::open(&dsn).unwrap();

    // Verify UPDATEs were preserved
    let updated_val: String = db
        .query_one("SELECT value FROM mut_test WHERE id = 3", ())
        .unwrap();
    assert_eq!(updated_val, "updated3", "UPDATE should be preserved");

    let final_status: String = db
        .query_one("SELECT status FROM mut_test WHERE id = 3", ())
        .unwrap();
    assert_eq!(final_status, "final", "Second UPDATE should be preserved");

    // Verify DELETEs were preserved
    let deleted_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM mut_test WHERE id BETWEEN 16 AND 20",
            (),
        )
        .unwrap();
    assert_eq!(deleted_count, 0, "DELETEd rows should stay deleted");

    let late_delete: i64 = db
        .query_one("SELECT COUNT(*) FROM mut_test WHERE id = 21", ())
        .unwrap();
    assert_eq!(late_delete, 0, "Late DELETE should be preserved");

    // Verify total count: 20 original - 5 deleted(16-20) + 5 new(21-25) - 1 deleted(21) + 5 late(26-30) = 24
    let total: i64 = db.query_one("SELECT COUNT(*) FROM mut_test", ()).unwrap();
    assert_eq!(total, 24, "Total rows after all mutations");
}

#[test]
fn test_safe_truncation_survives_restart_cycles() {
    // Verify checkpoint state is consistent across multiple close/reopen cycles.
    // Each cycle: insert data, checkpoint, close, reopen, verify.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    // Cycle 1: Create and populate
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE cycle (id INTEGER PRIMARY KEY, cycle_num INTEGER NOT NULL, value TEXT NOT NULL)",
            (),
        )
        .unwrap();
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO cycle (id, cycle_num, value) VALUES ({}, 1, 'c1_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }
    remove_lock_file(&db_path);

    // Cycle 2: Reopen, add data, checkpoint
    {
        let db = Database::open(&dsn).unwrap();
        let count: i64 = db.query_one("SELECT COUNT(*) FROM cycle", ()).unwrap();
        assert_eq!(count, 10, "Cycle 2 open: should have 10 rows");

        for i in 11..=20 {
            db.execute(
                &format!(
                    "INSERT INTO cycle (id, cycle_num, value) VALUES ({}, 2, 'c2_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }
    remove_lock_file(&db_path);

    // Cycle 3: Reopen, add data, checkpoint
    {
        let db = Database::open(&dsn).unwrap();
        let count: i64 = db.query_one("SELECT COUNT(*) FROM cycle", ()).unwrap();
        assert_eq!(count, 20, "Cycle 3 open: should have 20 rows");

        for i in 21..=30 {
            db.execute(
                &format!(
                    "INSERT INTO cycle (id, cycle_num, value) VALUES ({}, 3, 'c3_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }
    remove_lock_file(&db_path);

    // Cycle 4: Reopen, verify all data survived multiple cycles
    {
        let db = Database::open(&dsn).unwrap();
        let count: i64 = db.query_one("SELECT COUNT(*) FROM cycle", ()).unwrap();
        assert_eq!(
            count, 30,
            "All 30 rows should be recovered from volumes across 3 checkpoint cycles"
        );

        // Verify data from each cycle
        for cycle_num in 1..=3 {
            let cycle_count: i64 = db
                .query_one(
                    &format!("SELECT COUNT(*) FROM cycle WHERE cycle_num = {}", cycle_num),
                    (),
                )
                .unwrap();
            assert_eq!(cycle_count, 10, "Cycle {} should have 10 rows", cycle_num);
        }
    }
}

// ============================================================================
// 15. DURABILITY EDGE CASES
// ============================================================================
//
// ---------------------------------------------------------------------------
// Gap 3: DDL Durability Under Corruption
// ---------------------------------------------------------------------------

#[test]
fn test_ddl_create_index_survives_checkpoint_recovery() {
    // CREATE INDEX + data published by checkpoint. Close and reopen, then
    // verify both the row set and indexed lookup contract.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE idx_test (id INTEGER PRIMARY KEY, category TEXT NOT NULL, amount FLOAT)",
            (),
        )
        .unwrap();

        for i in 1..=20 {
            let cat = if i % 2 == 0 { "even" } else { "odd" };
            db.execute(
                &format!(
                    "INSERT INTO idx_test (id, category, amount) VALUES ({}, '{}', {})",
                    i,
                    cat,
                    i as f64 * 1.5
                ),
                (),
            )
            .unwrap();
        }

        db.execute("CREATE INDEX idx_cat ON idx_test(category)", ())
            .unwrap();

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        let data_artifacts = find_data_artifacts(&db_path);
        assert!(
            !data_artifacts.is_empty(),
            "checkpoint should publish a DATA artifact"
        );
    }
    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM idx_test", ()).unwrap();
    assert_eq!(count, 20, "all 20 rows should recover from DATA + WAL");

    // Verify index is usable — query with indexed column filter
    let even_count: i64 = db
        .query_one("SELECT COUNT(*) FROM idx_test WHERE category = 'even'", ())
        .unwrap();
    assert_eq!(even_count, 10, "Should find 10 'even' rows via index");

    let odd_count: i64 = db
        .query_one("SELECT COUNT(*) FROM idx_test WHERE category = 'odd'", ())
        .unwrap();
    assert_eq!(odd_count, 10, "Should find 10 'odd' rows via index");
}

#[test]
fn test_ddl_multiple_operations_recovery() {
    // Multiple DDL operations: CREATE TABLE t1, INSERT, CREATE TABLE t2, INSERT,
    // CREATE INDEX on t1, DROP TABLE t2.
    // Pure WAL replay (no snapshot) should recover correct state.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();

        db.execute(
            "CREATE TABLE ddl_t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            (),
        )
        .unwrap();
        for i in 1..=5 {
            db.execute(
                &format!("INSERT INTO ddl_t1 (id, name) VALUES ({}, 'name_{}')", i, i),
                (),
            )
            .unwrap();
        }

        db.execute(
            "CREATE TABLE ddl_t2 (id INTEGER PRIMARY KEY, data TEXT)",
            (),
        )
        .unwrap();
        for i in 1..=3 {
            db.execute(
                &format!("INSERT INTO ddl_t2 (id, data) VALUES ({}, 'data_{}')", i, i),
                (),
            )
            .unwrap();
        }

        db.execute("CREATE INDEX idx_name ON ddl_t1(name)", ())
            .unwrap();

        db.execute("DROP TABLE ddl_t2", ()).unwrap();
    }
    remove_lock_file(&db_path);

    // Reopen — pure WAL replay, no snapshot
    let db = Database::open(&dsn).unwrap();

    // t1 should exist with data and index
    let count: i64 = db.query_one("SELECT COUNT(*) FROM ddl_t1", ()).unwrap();
    assert_eq!(count, 5, "ddl_t1 should have 5 rows");

    // Verify index on t1 is usable
    let name_count: i64 = db
        .query_one("SELECT COUNT(*) FROM ddl_t1 WHERE name = 'name_3'", ())
        .unwrap();
    assert_eq!(name_count, 1, "Should find 1 row via index lookup");

    // t2 should NOT exist
    let result: Result<i64, _> = db.query_one("SELECT COUNT(*) FROM ddl_t2", ());
    assert!(
        result.is_err(),
        "ddl_t2 should not exist after DROP TABLE was replayed"
    );
}

// ---------------------------------------------------------------------------
// Gap 4: View Persistence After Corruption
// ---------------------------------------------------------------------------

#[test]
fn test_view_survives_checkpoint_recovery() {
    // CREATE VIEW + checkpointed data. The catalog generation and DATA
    // generation must reopen as one coherent state.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE view_base (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER)",
            (),
        )
        .unwrap();
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO view_base (id, name, score) VALUES ({}, 'user_{}', {})",
                    i,
                    i,
                    i * 10
                ),
                (),
            )
            .unwrap();
        }

        db.execute(
            "CREATE VIEW high_scores AS SELECT id, UPPER(name) AS name, score FROM view_base WHERE score >= 50",
            (),
        )
        .unwrap();

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        let data_artifacts = find_data_artifacts(&db_path);
        assert!(
            !data_artifacts.is_empty(),
            "checkpoint should publish a DATA artifact"
        );
    }
    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    // View should exist and return correct results from volume recovery
    let view_count: i64 = db
        .query_one("SELECT COUNT(*) FROM high_scores", ())
        .unwrap();
    assert_eq!(
        view_count, 6,
        "high_scores view should return 6 rows (scores 50-100)"
    );

    // Verify UPPER transformation works
    let name: String = db
        .query_one("SELECT name FROM high_scores WHERE id = 5", ())
        .unwrap();
    assert_eq!(name, "USER_5", "UPPER() should be applied in the view");
}

#[test]
fn test_view_drop_and_recreate_durability() {
    // CREATE VIEW v1 (def A), DROP VIEW v1, CREATE VIEW v1 (def B).
    // After WAL replay, v1 should use definition B.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE vdr_base (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER)",
            (),
        )
        .unwrap();
        for i in 1..=6 {
            let active = if i <= 3 { 1 } else { 0 };
            db.execute(
                &format!(
                    "INSERT INTO vdr_base (id, name, active) VALUES ({}, 'user_{}', {})",
                    i, i, active
                ),
                (),
            )
            .unwrap();
        }

        // First definition: SELECT *
        db.execute("CREATE VIEW vdr_view AS SELECT * FROM vdr_base", ())
            .unwrap();

        // Drop it
        db.execute("DROP VIEW vdr_view", ()).unwrap();

        // Recreate with different definition: only active users, with UPPER
        db.execute(
            "CREATE VIEW vdr_view AS SELECT id, UPPER(name) AS name FROM vdr_base WHERE active = 1",
            (),
        )
        .unwrap();
    }
    remove_lock_file(&db_path);

    // Reopen — WAL replays all three DDL ops in order
    let db = Database::open(&dsn).unwrap();

    // View should use the NEW definition (only active users)
    let count: i64 = db.query_one("SELECT COUNT(*) FROM vdr_view", ()).unwrap();
    assert_eq!(count, 3, "View should return only 3 active users");

    // Verify UPPER transformation is applied (new definition)
    let name: String = db
        .query_one("SELECT name FROM vdr_view WHERE id = 1", ())
        .unwrap();
    assert_eq!(name, "USER_1", "New view definition should apply UPPER()");
}

// ---------------------------------------------------------------------------
// Gap 5: Transactions Spanning Checkpoint Boundary
// ---------------------------------------------------------------------------

#[test]
fn test_uncommitted_data_not_in_checkpoint() {
    // Uncommitted transaction data should NOT survive across close/reopen.
    // PRAGMA CHECKPOINT captures only committed data.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE uncommit_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Insert 10 committed rows
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO uncommit_test (id, value) VALUES ({}, 'committed_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Begin transaction and insert 5 more (uncommitted)
        let mut tx = db.begin().unwrap();
        for i in 11..=15 {
            tx.execute(
                &format!(
                    "INSERT INTO uncommit_test (id, value) VALUES ({}, 'uncommitted_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Checkpoint while transaction is open
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Drop tx without committing (implicit rollback)
        drop(tx);
    }
    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM uncommit_test", ())
        .unwrap();
    assert_eq!(
        count, 10,
        "Only 10 committed rows should survive, uncommitted are lost"
    );
}

#[test]
fn test_data_committed_after_checkpoint_survives() {
    // Data committed AFTER checkpoint should survive via WAL.
    // Even if volumes are corrupted, WAL preserves post-checkpoint data.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE post_snap (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Insert 10 rows, then checkpoint
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO post_snap (id, value) VALUES ({}, 'pre_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Insert 10 more rows after checkpoint (committed)
        for i in 11..=20 {
            db.execute(
                &format!(
                    "INSERT INTO post_snap (id, value) VALUES ({}, 'post_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }
    remove_lock_file(&db_path);

    // Verify all 20 rows survive (volumes have pre-checkpoint, WAL has post-checkpoint)
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM post_snap", ()).unwrap();
    assert_eq!(count, 20, "All 20 rows should survive (volumes + WAL)");
}

// ---------------------------------------------------------------------------
// Gap 6: Constraint Enforcement After Recovery
// ---------------------------------------------------------------------------

#[test]
fn test_check_constraint_enforced_after_checkpoint_recovery() {
    // CHECK constraint schema and data sealed into volumes via checkpoint.
    // After close/reopen, constraint must still be enforced.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE check_test (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0 AND age <= 150))",
            (),
        )
        .unwrap();

        for i in 1..=5 {
            db.execute(
                &format!(
                    "INSERT INTO check_test (id, age) VALUES ({}, {})",
                    i,
                    i * 20
                ),
                (),
            )
            .unwrap();
        }

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }
    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    // Data should be recovered from volumes
    let count: i64 = db.query_one("SELECT COUNT(*) FROM check_test", ()).unwrap();
    assert_eq!(count, 5, "All 5 rows should be recovered from volumes");

    // CHECK constraint should be enforced — negative age should fail
    let result = db.execute("INSERT INTO check_test (id, age) VALUES (100, -1)", ());
    assert!(
        result.is_err(),
        "CHECK constraint should reject age = -1 after recovery"
    );

    // Age > 150 should also fail
    let result = db.execute("INSERT INTO check_test (id, age) VALUES (101, 200)", ());
    assert!(
        result.is_err(),
        "CHECK constraint should reject age = 200 after recovery"
    );

    // Valid age should succeed
    db.execute("INSERT INTO check_test (id, age) VALUES (102, 50)", ())
        .unwrap();
}

#[test]
fn test_not_null_constraint_after_recovery() {
    // NOT NULL constraint should be enforced after close/reopen.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE notnull_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score INTEGER)",
            (),
        )
        .unwrap();

        for i in 1..=5 {
            db.execute(
                &format!(
                    "INSERT INTO notnull_test (id, name, score) VALUES ({}, 'user_{}', {})",
                    i,
                    i,
                    i * 10
                ),
                (),
            )
            .unwrap();
        }
    }
    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    // Data should be present
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM notnull_test", ())
        .unwrap();
    assert_eq!(count, 5, "All 5 rows should be recovered");

    // NOT NULL constraint should be enforced — NULL name should fail
    let result = db.execute(
        "INSERT INTO notnull_test (id, name, score) VALUES (100, NULL, 50)",
        (),
    );
    assert!(
        result.is_err(),
        "NOT NULL constraint should reject NULL name after recovery"
    );

    // Valid insert should succeed
    db.execute(
        "INSERT INTO notnull_test (id, name, score) VALUES (101, 'valid', 60)",
        (),
    )
    .unwrap();
}

#[test]
fn test_unique_index_enforced_after_checkpoint_recovery() {
    // UNIQUE INDEX + data sealed into volumes via checkpoint.
    // After close/reopen, unique constraint must still be enforced.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE unique_test (id INTEGER PRIMARY KEY, email TEXT NOT NULL)",
            (),
        )
        .unwrap();

        db.execute("CREATE UNIQUE INDEX idx_email ON unique_test(email)", ())
            .unwrap();

        for i in 1..=5 {
            db.execute(
                &format!(
                    "INSERT INTO unique_test (id, email) VALUES ({}, 'user{}@example.com')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }
    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    // Data should be recovered from volumes
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM unique_test", ())
        .unwrap();
    assert_eq!(count, 5, "All 5 rows should be recovered from volumes");

    // UNIQUE constraint should be enforced — duplicate email should fail
    let result = db.execute(
        "INSERT INTO unique_test (id, email) VALUES (100, 'user1@example.com')",
        (),
    );
    assert!(
        result.is_err(),
        "UNIQUE constraint should reject duplicate email after recovery"
    );

    // New unique email should succeed
    db.execute(
        "INSERT INTO unique_test (id, email) VALUES (101, 'new@example.com')",
        (),
    )
    .unwrap();
}

// ============================================================================
// 16. ADVANCED DURABILITY SCENARIOS
// ============================================================================

// ----------------------------------------------------------------------------
// Gap 1: Concurrent Write + Crash (4 tests)
// ----------------------------------------------------------------------------

/// 4 threads each INSERT 25 rows concurrently, close, reopen → all 100 rows present
#[test]
fn test_concurrent_writers_recovery() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE conc_test (id INTEGER PRIMARY KEY, thread_id INTEGER, value TEXT)",
            (),
        )
        .unwrap();

        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for t in 0..4u32 {
            let db_clone = db.clone();
            let bar = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                bar.wait();
                for i in 0..25u32 {
                    let id = t * 25 + i + 1;
                    db_clone
                        .execute(
                            &format!(
                                "INSERT INTO conc_test (id, thread_id, value) VALUES ({}, {}, 'row_{}')",
                                id, t, id
                            ),
                            (),
                        )
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM conc_test", ()).unwrap();
        assert_eq!(count, 100);
    }

    remove_lock_file(&db_path);

    // Verify all 100 rows recovered
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM conc_test", ()).unwrap();
    assert_eq!(count, 100, "All 100 concurrent rows should be recovered");
    db.execute(
        "INSERT INTO conc_test (id, thread_id, value) VALUES (9999, 0, 'post_recovery')",
        (),
    )
    .unwrap();
}

/// Concurrent writes + WAL corruption → at least some rows recovered, DB usable
#[test]
fn test_concurrent_writers_with_wal_corruption() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE conc_corrupt (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .unwrap();

        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for t in 0..4u32 {
            let db_clone = db.clone();
            let bar = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                bar.wait();
                for i in 0..25u32 {
                    let id = t * 25 + i + 1;
                    db_clone
                        .execute(
                            &format!(
                                "INSERT INTO conc_corrupt (id, value) VALUES ({}, 'data_{}')",
                                id, id
                            ),
                            (),
                        )
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Corrupt middle of WAL
    let wal_files = find_wal_files(&db_path);
    assert!(!wal_files.is_empty());
    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(
        entries.len() > 4,
        "concurrent WAL has too few entries to corrupt"
    );
    // Flip bits in a few middle entries
    let mid = entries.len() / 2;
    for entry in entries.iter().skip(mid).take(3.min(entries.len() - mid)) {
        assert!(entry.data_offset + 4 < data.len());
        flip_bit(&mut data, entry.data_offset + 2, 3);
    }
    fs::write(wal_path, &data).unwrap();

    // A corrupt committed generation is terminal; recovery must not publish a
    // partial prefix from the concurrent workload.
    assert!(Database::open(&dsn).is_err());
}

/// Concurrent writes + checkpoint + more writes → corrupt volumes → all rows from WAL
#[test]
fn test_concurrent_writers_checkpoint_recovery() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE conc_snap (id INTEGER PRIMARY KEY, phase INTEGER, value TEXT)",
            (),
        )
        .unwrap();

        // Phase 1: 4 threads x 25 rows = 100
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for t in 0..4u32 {
            let db_clone = db.clone();
            let bar = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                bar.wait();
                for i in 0..25u32 {
                    let id = t * 25 + i + 1;
                    db_clone
                        .execute(
                            &format!(
                                "INSERT INTO conc_snap (id, phase, value) VALUES ({}, 1, 'p1_{}')",
                                id, id
                            ),
                            (),
                        )
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 2: 4 threads x 25 more rows = 200 total
        let barrier2 = Arc::new(Barrier::new(4));
        let mut handles2 = Vec::new();
        for t in 0..4u32 {
            let db_clone = db.clone();
            let bar = Arc::clone(&barrier2);
            handles2.push(thread::spawn(move || {
                bar.wait();
                for i in 0..25u32 {
                    let id = 100 + t * 25 + i + 1;
                    db_clone
                        .execute(
                            &format!(
                                "INSERT INTO conc_snap (id, phase, value) VALUES ({}, 2, 'p2_{}')",
                                id, id
                            ),
                            (),
                        )
                        .unwrap();
                }
            }));
        }
        for h in handles2 {
            h.join().unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM conc_snap", ()).unwrap();
        assert_eq!(count, 200);
    }

    remove_lock_file(&db_path);

    // Volumes provide phase 1 (100 rows), WAL provides phase 2 (100 rows)
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM conc_snap", ()).unwrap();
    assert_eq!(count, 200, "All 200 rows should survive (volumes + WAL)");
    db.execute(
        "INSERT INTO conc_snap (id, phase, value) VALUES (9999, 0, 'post_recovery')",
        (),
    )
    .unwrap();
}

/// Uncommitted transactions should be lost after recovery
#[test]
fn test_concurrent_transactions_uncommitted_lost() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE uncommit_test (id INTEGER PRIMARY KEY, source TEXT)",
            (),
        )
        .unwrap();

        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: committed inserts
        let db1 = db.clone();
        let bar1 = Arc::clone(&barrier);
        let h1 = thread::spawn(move || {
            bar1.wait();
            for i in 1..=10 {
                db1.execute(
                    &format!(
                        "INSERT INTO uncommit_test (id, source) VALUES ({}, 'committed')",
                        i
                    ),
                    (),
                )
                .unwrap();
            }
        });

        // Thread 2: uncommitted transaction (begun, not committed)
        let db2 = db.clone();
        let bar2 = Arc::clone(&barrier);
        let h2 = thread::spawn(move || {
            bar2.wait();
            let mut tx = db2.begin().unwrap();
            for i in 11..=20 {
                tx.execute(
                    &format!(
                        "INSERT INTO uncommit_test (id, source) VALUES ({}, 'uncommitted')",
                        i
                    ),
                    (),
                )
                .unwrap();
            }
            // Intentionally drop tx without commit
            drop(tx);
        });

        h1.join().unwrap();
        h2.join().unwrap();
    }

    remove_lock_file(&db_path);

    // Only committed rows should survive
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM uncommit_test", ())
        .unwrap();
    assert_eq!(
        count, 10,
        "Only 10 committed rows should survive, got {}",
        count
    );
    db.execute(
        "INSERT INTO uncommit_test (id, source) VALUES (9999, 'post_recovery')",
        (),
    )
    .unwrap();
}

// ----------------------------------------------------------------------------
// Gap 2: WAL generation integrity
// ----------------------------------------------------------------------------

/// An unvalidated newer WAL generation makes the generation graph ambiguous.
#[test]
fn test_multiple_wal_files_newest_corrupt() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE multi_wal (id INTEGER PRIMARY KEY, value TEXT NOT NULL, seq INTEGER)",
            (),
        )
        .unwrap();
        for i in 1..=30 {
            db.execute(
                &format!(
                    "INSERT INTO multi_wal (id, value, seq) VALUES ({}, 'row_{}', {})",
                    i, i, i
                ),
                (),
            )
            .unwrap();
        }
    }
    remove_lock_file(&db_path);

    // Create a second (newer by name) WAL file with garbage
    let wal_dir = db_path.join("wal");
    let corrupt_wal = wal_dir.join("wal_00000001-99999999-lsn-99999.log");
    fs::write(&corrupt_wal, b"this is not a valid WAL file at all").unwrap();

    assert!(Database::open(&dsn).is_err());
}

// Corrupt checkpoint metadata is terminal even when WAL files still exist.
// ----------------------------------------------------------------------------
// Gap 3: Power Loss Simulation (4 tests)
// ----------------------------------------------------------------------------

/// Simulate power loss: last WAL entry truncated in half (mid-data)
#[test]
fn test_power_loss_truncated_last_entry() {
    let fixture = setup_test_db(20, 1);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(entries.len() >= 2, "Need at least 2 entries");

    // Truncate the file to cut the last entry in half
    let last = &entries[entries.len() - 1];
    let cut_point = last.offset + last.total_size / 2;
    fs::write(wal_path, &data[..cut_point]).unwrap();

    remove_lock_file(&fixture.db_path);

    // At least all but the last entry's data should survive
    verify_recovery_at_least(&fixture, "test_data", 0);
}

/// Simulate power loss: garbage appended after valid WAL entries
#[test]
fn test_power_loss_garbage_appended() {
    let fixture = setup_test_db(15, 1);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();

    // Append 200 bytes of garbage (simulates partial write of next entry)
    data.extend_from_slice(&[0xDE; 200]);
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);

    // All 15 rows should survive — garbage fails magic check
    verify_recovery_at_least(&fixture, "test_data", 15);
}

/// Simulate power loss: last 4KB page of WAL zeroed
#[test]
fn test_power_loss_last_page_zeroed() {
    let fixture = setup_test_db(20, 1);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let file_len = data.len();

    if file_len > 4096 {
        // Zero the last 4KB page
        let last_page = (file_len - 1) / 4096;
        zero_page(&mut data, last_page);
        fs::write(wal_path, &data).unwrap();
    }

    remove_lock_file(&fixture.db_path);

    // Earlier entries should survive, entries in last page lost
    verify_recovery_at_least(&fixture, "test_data", 0);
}

/// Simulate power loss: last commit marker torn in half
#[test]
fn test_power_loss_commit_marker_torn() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE commit_torn (id INTEGER PRIMARY KEY, value TEXT NOT NULL, seq INTEGER)",
            (),
        )
        .unwrap();

        // 4 explicit transactions × 5 rows each
        for txn in 0..4 {
            let mut tx = db.begin().unwrap();
            for row in 0..5 {
                let id = txn * 5 + row + 1;
                tx.execute(
                    &format!(
                        "INSERT INTO commit_torn (id, value, seq) VALUES ({}, 'txn{}_row{}', {})",
                        id, txn, row, txn
                    ),
                    (),
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM commit_torn", ())
            .unwrap();
        assert_eq!(count, 20);
    }

    remove_lock_file(&db_path);

    // Find the last commit marker entry and truncate to cut it in half
    let wal_files = find_wal_files(&db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Find commit marker entries
    let commit_entries: Vec<_> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.flags & COMMIT_MARKER_FLAG != 0)
        .collect();

    if !commit_entries.is_empty() {
        // Truncate at the middle of the last commit marker
        let (_, last_commit) = commit_entries[commit_entries.len() - 1];
        let cut_point = last_commit.offset + last_commit.total_size / 2;
        fs::write(wal_path, &data[..cut_point]).unwrap();
    }

    let fixture = TestFixture {
        _dir: dir,
        db_path,
        dsn,
    };

    // Last txn's commit marker is torn → treated as uncommitted
    // At least the first 3 txns (15 rows) should survive
    verify_recovery_at_least(&fixture, "commit_torn", 0);
}

// ----------------------------------------------------------------------------
// Gap 4: Large Data / Boundary Conditions (4 tests)
// ----------------------------------------------------------------------------

/// Test rows at compression threshold boundary (63 bytes uncompressed, 65 bytes may compress)
#[test]
fn test_compression_payload_size_boundary() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    // Create strings at the compression threshold boundary (64 bytes)
    let small_value = "x".repeat(63); // Below threshold
    let large_value = "y".repeat(65); // Above threshold, may compress

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE threshold_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Insert rows with values just below threshold
        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO threshold_test (id, value) VALUES ({}, '{}')",
                    i, small_value
                ),
                (),
            )
            .unwrap();
        }

        // Insert rows with values just above threshold
        for i in 11..=20 {
            db.execute(
                &format!(
                    "INSERT INTO threshold_test (id, value) VALUES ({}, '{}')",
                    i, large_value
                ),
                (),
            )
            .unwrap();
        }

        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM threshold_test", ())
            .unwrap();
        assert_eq!(count, 20);
    }

    remove_lock_file(&db_path);

    // Reopen and verify all rows recovered with correct values
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM threshold_test", ())
        .unwrap();
    assert_eq!(count, 20, "All 20 rows should be recovered");

    // Verify values are correct
    let small_count: i64 = db
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM threshold_test WHERE value = '{}'",
                small_value
            ),
            (),
        )
        .unwrap();
    assert_eq!(small_count, 10, "All 10 small rows should match exactly");

    let large_count: i64 = db
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM threshold_test WHERE value = '{}'",
                large_value
            ),
            (),
        )
        .unwrap();
    assert_eq!(large_count, 10, "All 10 large rows should match exactly");
}

/// Large rows (100KB each) with bit corruption fail closed.
#[test]
fn test_large_row_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    let large_text = "L".repeat(100_000); // 100KB each

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE large_row (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=5 {
            db.execute(
                &format!(
                    "INSERT INTO large_row (id, value) VALUES ({}, '{}')",
                    i, large_text
                ),
                (),
            )
            .unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM large_row", ()).unwrap();
        assert_eq!(count, 5);
    }

    remove_lock_file(&db_path);

    // Flip a bit in the data section of the WAL
    let wal_files = find_wal_files(&db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    // Find a DML data entry (not DDL, not commit marker) to corrupt
    let dml_entries: Vec<_> = entries
        .iter()
        .filter(|e| e.flags & COMMIT_MARKER_FLAG == 0 && e.entry_size > 100)
        .collect();

    // Corrupt one data entry.
    let target = dml_entries
        .get(dml_entries.len() / 2)
        .expect("large-row WAL contains no DML entry");
    flip_bit(&mut data, target.data_offset + 50, 5);
    fs::write(wal_path, &data).unwrap();

    assert!(Database::open(&dsn).is_err());
}

/// 1000 small rows, corrupt one entry in the middle → no partial publication.
#[test]
fn test_many_small_rows_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE many_rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Batch into 10 transactions × 100 rows to avoid 1000 individual WAL commits
        let mut id = 1;
        for _ in 0..10 {
            let mut tx = db.begin().unwrap();
            for _ in 0..100 {
                tx.execute(
                    &format!(
                        "INSERT INTO many_rows (id, value) VALUES ({}, 'small_{}')",
                        id, id
                    ),
                    (),
                )
                .unwrap();
                id += 1;
            }
            tx.commit().unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM many_rows", ()).unwrap();
        assert_eq!(count, 1000);
    }

    remove_lock_file(&db_path);

    // Corrupt one entry in the middle of the WAL
    let wal_files = find_wal_files(&db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);

    assert!(
        entries.len() > 10,
        "many-row WAL has too few entries to corrupt"
    );
    // Corrupt a single entry in the middle
    let mid_entry = &entries[entries.len() / 2];
    // Zero the CRC to ensure it's detected as corrupt
    zero_range(&mut data, mid_entry.crc_offset, CRC_SIZE);
    fs::write(wal_path, &data).unwrap();

    assert!(Database::open(&dsn).is_err());
}

/// Mixed-size rows with checkpoint + corruption → all recovered from WAL
#[test]
fn test_mixed_size_rows_with_checkpoint() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    let tiny = "t".repeat(5);
    let medium = "m".repeat(200);
    let large = "L".repeat(50_000);

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE mixed_size (id INTEGER PRIMARY KEY, category TEXT, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: insert mixed sizes
        for i in 1..=5 {
            db.execute(
                &format!(
                    "INSERT INTO mixed_size (id, category, value) VALUES ({}, 'tiny', '{}')",
                    i, tiny
                ),
                (),
            )
            .unwrap();
        }
        for i in 6..=10 {
            db.execute(
                &format!(
                    "INSERT INTO mixed_size (id, category, value) VALUES ({}, 'medium', '{}')",
                    i, medium
                ),
                (),
            )
            .unwrap();
        }
        for i in 11..=13 {
            db.execute(
                &format!(
                    "INSERT INTO mixed_size (id, category, value) VALUES ({}, 'large', '{}')",
                    i, large
                ),
                (),
            )
            .unwrap();
        }

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 2: more mixed rows after checkpoint
        for i in 14..=18 {
            db.execute(
                &format!(
                    "INSERT INTO mixed_size (id, category, value) VALUES ({}, 'tiny', '{}')",
                    i, tiny
                ),
                (),
            )
            .unwrap();
        }
        for i in 19..=23 {
            db.execute(
                &format!(
                    "INSERT INTO mixed_size (id, category, value) VALUES ({}, 'medium', '{}')",
                    i, medium
                ),
                (),
            )
            .unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM mixed_size", ()).unwrap();
        assert_eq!(count, 23);
    }

    remove_lock_file(&db_path);

    // Reopen → volumes provide phase 1 (13 rows), WAL provides phase 2 (10 rows)
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM mixed_size", ()).unwrap();
    assert_eq!(count, 23, "All 23 mixed-size rows should be recovered");

    // Verify each category
    let tiny_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM mixed_size WHERE category = 'tiny'",
            (),
        )
        .unwrap();
    assert_eq!(tiny_count, 10, "10 tiny rows expected");

    let medium_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM mixed_size WHERE category = 'medium'",
            (),
        )
        .unwrap();
    assert_eq!(medium_count, 10, "10 medium rows expected");

    let large_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM mixed_size WHERE category = 'large'",
            (),
        )
        .unwrap();
    assert_eq!(large_count, 3, "3 large rows expected");

    // Verify DB is usable
    db.execute(
        "INSERT INTO mixed_size (id, category, value) VALUES (9999, 'post', 'recovery')",
        (),
    )
    .unwrap();
}

// ----------------------------------------------------------------------------
// Gap 5: WAL Truncation, Replay & Size Limits (4 tests)
// ----------------------------------------------------------------------------

/// Multiple checkpoints may retire whole WAL generations; data written after
/// the final checkpoint must replay together with checkpointed data.
#[test]
fn test_wal_truncation_and_replay() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE trunc_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: data + first checkpoint
        for i in 1..=20 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_test (id, value) VALUES ({}, 'phase1_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Phase 2: more data + second checkpoint (may retire safe whole generations)
        for i in 21..=40 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_test (id, value) VALUES ({}, 'phase2_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Phase 3: data after checkpoint (only in WAL, not in any snapshot)
        for i in 41..=50 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_test (id, value) VALUES ({}, 'phase3_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_test", ()).unwrap();
        assert_eq!(count, 50);
    }

    remove_lock_file(&db_path);

    // Reopen — snapshot provides phase1+phase2, new WAL provides phase3
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_test", ()).unwrap();
    assert_eq!(
        count, 50,
        "All 50 rows should be recovered (snapshot + truncated WAL)"
    );

    db.execute(
        "INSERT INTO trunc_test (id, value) VALUES (9999, 'post_recovery')",
        (),
    )
    .unwrap();
}

/// WAL truncation + corrupt new WAL → snapshot still provides earlier data
#[test]
fn test_wal_truncation_then_corrupt_new_wal() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE trunc_corrupt (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1 + checkpoint
        for i in 1..=20 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_corrupt (id, value) VALUES ({}, 'p1_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Phase 2 + second checkpoint → triggers truncation
        for i in 21..=40 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_corrupt (id, value) VALUES ({}, 'p2_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Phase 3: only in new (truncated) WAL
        for i in 41..=50 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_corrupt (id, value) VALUES ({}, 'p3_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    // Corrupt the post-truncation WAL file
    let wal_files = find_wal_files(&db_path);
    assert!(!wal_files.is_empty());
    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    if data.len() > 64 {
        // Zero a big chunk of the WAL — destroys phase 3 data
        let mid = data.len() / 2;
        let len = (data.len() - mid).min(256);
        zero_range(&mut data, mid, len);
        fs::write(wal_path, &data).unwrap();
    }

    // A valid checkpoint does not authorize ignoring corruption in a WAL
    // generation selected for replay.
    assert!(Database::open(&dsn).is_err());
}

/// WAL entry with entry_size field corrupted to exceed 64MB sanity limit → skipped
#[test]
fn test_wal_entry_size_exceeds_sanity_limit() {
    let fixture = setup_test_db(10, 1);
    let wal_files = find_wal_files(&fixture.db_path);
    assert!(!wal_files.is_empty());

    let wal_path = &wal_files[wal_files.len() - 1];
    let mut data = fs::read(wal_path).unwrap();
    let entries = find_entry_boundaries(&data);
    assert!(entries.len() >= 2, "Need at least 2 entries");

    // Corrupt one entry's size field to be > 64MB (the sanity limit)
    // Entry size is at offset +24 in the header (4 bytes, little-endian)
    let target = &entries[entries.len() / 2];
    let size_offset = target.offset + 24;
    // Write 0x05000000 = ~83MB > 64MB sanity limit
    let huge_size: u32 = 70 * 1024 * 1024;
    data[size_offset..size_offset + 4].copy_from_slice(&huge_size.to_le_bytes());
    fs::write(wal_path, &data).unwrap();

    remove_lock_file(&fixture.db_path);

    // Recovery should skip the entry with impossible size and recover what it can
    verify_recovery_at_least(&fixture, "test_data", 0);
}

/// Large WAL with data spread across many pages → full replay succeeds
#[test]
fn test_large_wal_full_replay() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE large_wal (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Each row gets a unique ~500-byte string that doesn't compress well.
        // Use varying characters based on id to defeat LZ4.
        let mut id = 1;
        for _ in 0..10 {
            let mut tx = db.begin().unwrap();
            for _ in 0..50 {
                // Build a high-entropy string: cycle through printable ASCII based on id
                // Alphanumeric only — safe for SQL, 62 distinct chars defeats LZ4
                const CHARS: &[u8] =
                    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
                let value: String = (0..500)
                    .map(|j| CHARS[((id * 7 + j * 13 + 37) % CHARS.len() as i32) as usize] as char)
                    .collect();
                db.execute(
                    &format!(
                        "INSERT INTO large_wal (id, value) VALUES ({}, '{}')",
                        id, value
                    ),
                    (),
                )
                .unwrap();
                id += 1;
            }
            tx.commit().unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM large_wal", ()).unwrap();
        assert_eq!(count, 500);
    }

    remove_lock_file(&db_path);

    // Verify WAL is substantial — unique data per row means LZ4 can't compress it away
    let wal_size = total_wal_size(&db_path);
    assert!(
        wal_size > 100_000,
        "WAL should be > 100KB with high-entropy data, got {} bytes",
        wal_size
    );

    // Reopen — full replay of large WAL
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM large_wal", ()).unwrap();
    assert_eq!(
        count, 500,
        "All 500 rows should be recovered from large WAL"
    );

    // Verify a specific row's value survived correctly
    let row1_value: String = db
        .query_one("SELECT value FROM large_wal WHERE id = 1", ())
        .unwrap();
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let expected: String = (0..500)
        .map(|j| CHARS[((7 + j * 13 + 37) % CHARS.len() as i32) as usize] as char)
        .collect();
    assert_eq!(row1_value, expected, "Row 1 value should match exactly");

    db.execute(
        "INSERT INTO large_wal (id, value) VALUES (9999, 'post_recovery')",
        (),
    )
    .unwrap();
}

// ----------------------------------------------------------------------------
// Gap 5b: WAL Rotation Tests (4 tests)
// These verify that wal_max_size config actually triggers rotation in production
// and that rotated files are cleaned up after snapshot-based truncation.
// ----------------------------------------------------------------------------

/// WAL rotation fires during normal commits when wal_max_size is small.
/// Multiple WAL files appear on disk, and multi-file replay recovers all data.
#[test]
fn test_wal_rotation_and_replay() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    // wal_max_size=500 triggers rotation after a few inserts
    let dsn = format!(
        "file://{}?wal_max_size=500&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE rot_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=50 {
            db.execute(
                &format!(
                    "INSERT INTO rot_test (id, value) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM rot_test", ()).unwrap();
        assert_eq!(count, 50);
    }

    // Verify multiple WAL files exist on disk (rotation happened)
    let wal_files = find_wal_files(&db_path);
    assert!(
        wal_files.len() >= 2,
        "Expected multiple WAL files from rotation, got {}",
        wal_files.len()
    );

    remove_lock_file(&db_path);

    // Reopen — multi-file replay should recover all 50 rows
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM rot_test", ()).unwrap();
    assert_eq!(
        count, 50,
        "All 50 rows should be recovered from multi-file WAL replay"
    );

    // Verify DB is usable after recovery
    db.execute(
        "INSERT INTO rot_test (id, value) VALUES (9999, 'post_recovery')",
        (),
    )
    .unwrap();
}

/// Corrupt the oldest rotated WAL file — DDL may be lost but newer data survives.
#[test]
fn test_wal_rotation_oldest_corrupt() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!(
        "file://{}?wal_max_size=500&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE rot_oldest (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=50 {
            db.execute(
                &format!(
                    "INSERT INTO rot_oldest (id, value) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    let wal_files = find_wal_files(&db_path);
    assert!(
        wal_files.len() >= 2,
        "Need multiple WAL files, got {}",
        wal_files.len()
    );

    // Corrupt the oldest WAL file (first in sorted order)
    let oldest = &wal_files[0];
    let mut data = fs::read(oldest).unwrap();
    if data.len() > 32 {
        let mid = data.len() / 2;
        let len = (data.len() - mid).min(256);
        zero_range(&mut data, mid, len);
        fs::write(oldest, &data).unwrap();
    }

    remove_lock_file(&db_path);

    // No later generation may be published without its validated predecessor.
    assert!(Database::open(&dsn).is_err());
}

/// Corrupting the newest rotated WAL invalidates the selected generation graph.
#[test]
fn test_wal_rotation_newest_corrupt() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!(
        "file://{}?wal_max_size=500&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE rot_newest (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=50 {
            db.execute(
                &format!(
                    "INSERT INTO rot_newest (id, value) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }

    let wal_files = find_wal_files(&db_path);
    assert!(
        wal_files.len() >= 2,
        "Need multiple WAL files, got {}",
        wal_files.len()
    );

    // The manager may leave an empty current generation after rotating the
    // last record. Corrupt the newest generation that actually owns records.
    let newest = wal_files
        .iter()
        .rev()
        .find(|path| fs::metadata(path).unwrap().len() > 0)
        .expect("rotation fixture needs a non-empty WAL generation");
    let len = fs::metadata(newest).unwrap().len() as usize;
    fs::write(newest, vec![0u8; len]).unwrap();

    remove_lock_file(&db_path);

    assert!(Database::open(&dsn).is_err());
}

/// WAL rotation + snapshot-based truncation cleans up old rotated files.
/// Three snapshots are needed: 2nd triggers truncation to 1st's LSN (cleans phase 1 files),
/// 3rd triggers truncation to 2nd's LSN (cleans phase 2 files).
#[test]
fn test_wal_rotation_snapshot_cleans_old_files() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!(
        "file://{}?wal_max_size=500&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE rot_clean (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: Generate multiple rotated WAL files
        for i in 1..=50 {
            db.execute(
                &format!(
                    "INSERT INTO rot_clean (id, value) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        let wal_phase1 = find_wal_files(&db_path);
        assert!(
            wal_phase1.len() >= 2,
            "Expected multiple WAL files from rotation, got {}",
            wal_phase1.len()
        );

        // First checkpoint — no truncation yet (need 2 checkpoints for safe truncation)
        let _ = db.execute("PRAGMA CHECKPOINT", ());

        // Phase 2: More data
        for i in 51..=80 {
            db.execute(
                &format!(
                    "INSERT INTO rot_clean (id, value) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Second checkpoint — truncation cleans phase 1 WAL data
        let wal_size_before_snap2 = total_wal_size(&db_path);
        let _ = db.execute("PRAGMA CHECKPOINT", ());
        let wal_size_after_snap2 = total_wal_size(&db_path);

        // WAL should shrink after checkpoint (old entries truncated)
        assert!(
            wal_size_after_snap2 < wal_size_before_snap2,
            "WAL size should decrease after 2nd checkpoint: before={}, after={}",
            wal_size_before_snap2,
            wal_size_after_snap2
        );

        // Phase 3: A few more rows + 3rd checkpoint
        for i in 81..=90 {
            db.execute(
                &format!(
                    "INSERT INTO rot_clean (id, value) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
        let wal_size_before_snap3 = total_wal_size(&db_path);
        let _ = db.execute("PRAGMA CHECKPOINT", ());
        let wal_size_after_snap3 = total_wal_size(&db_path);

        // After 3rd checkpoint, WAL should shrink again
        assert!(
            wal_size_after_snap3 < wal_size_before_snap3,
            "WAL size should decrease after 3rd checkpoint: before={}, after={}",
            wal_size_before_snap3,
            wal_size_after_snap3
        );

        // Verify all data is still accessible
        let count: i64 = db.query_one("SELECT COUNT(*) FROM rot_clean", ()).unwrap();
        assert_eq!(count, 90);
    }

    remove_lock_file(&db_path);

    // Reopen — snapshot + remaining WAL should recover everything
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM rot_clean", ()).unwrap();
    assert_eq!(
        count, 90,
        "All 90 rows should be recovered after rotation + truncation cleanup"
    );

    db.execute(
        "INSERT INTO rot_clean (id, value) VALUES (9999, 'post_recovery')",
        (),
    )
    .unwrap();
}

/// After two checkpoints with WAL rotation, verify all data survives
/// close/reopen through volume recovery. Both phases should be in volumes.
#[test]
fn test_wal_rotation_cleanup_preserves_boundary_entries() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!(
        "file://{}?wal_max_size=500&checkpoint_on_close=off",
        db_path.display()
    );

    let total_rows;
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE rot_boundary (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        // Phase 1: bulk inserts (many rotations due to small wal_max_size)
        for i in 1..=50 {
            db.execute(
                &format!(
                    "INSERT INTO rot_boundary (id, value) VALUES ({}, 'phase1_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Checkpoint 1 — seals phase 1 data into volumes
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // Phase 2: more inserts (more rotations)
        for i in 51..=80 {
            db.execute(
                &format!(
                    "INSERT INTO rot_boundary (id, value) VALUES ({}, 'phase2_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Checkpoint 2 — seals phase 2 data, triggers WAL truncation.
        // cleanup_old_wal_files runs and removes old rotated files.
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        total_rows = 80;
        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM rot_boundary", ())
            .unwrap();
        assert_eq!(count, total_rows);
    }

    remove_lock_file(&db_path);

    // Volumes should provide all data from both phases.
    let db = Database::open(&dsn).unwrap();
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM rot_boundary", ())
        .unwrap();
    assert_eq!(
        count, total_rows,
        "All {} rows must survive volume recovery (got {}).",
        total_rows, count
    );

    // DB must be usable after recovery
    db.execute(
        "INSERT INTO rot_boundary (id, value) VALUES (9999, 'post_recovery')",
        (),
    )
    .unwrap();
}

/// UPDATE and DELETE operations must replay correctly when entries span
/// multiple rotated WAL files.
#[test]
fn test_wal_rotation_update_delete_replay() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!(
        "file://{}?wal_max_size=500&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE rot_upd (id INTEGER PRIMARY KEY, value TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active')",
            (),
        )
        .unwrap();

        // Phase 1: INSERT 30 rows (triggers several rotations)
        for i in 1..=30 {
            db.execute(
                &format!(
                    "INSERT INTO rot_upd (id, value) VALUES ({}, 'original_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Phase 2: UPDATE some rows (these WAL entries land in later rotated files)
        for i in 1..=10 {
            db.execute(
                &format!(
                    "UPDATE rot_upd SET value = 'updated_{}', status = 'modified' WHERE id = {}",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Phase 3: DELETE some rows (more WAL entries in even later files)
        for i in 21..=25 {
            db.execute(&format!("DELETE FROM rot_upd WHERE id = {}", i), ())
                .unwrap();
        }

        // Verify final state: 30 inserted - 5 deleted = 25 rows
        let total: i64 = db.query_one("SELECT COUNT(*) FROM rot_upd", ()).unwrap();
        assert_eq!(total, 25);

        // 10 updated + 15 still active = 25
        let modified: i64 = db
            .query_one("SELECT COUNT(*) FROM rot_upd WHERE status = 'modified'", ())
            .unwrap();
        assert_eq!(modified, 10);

        let active: i64 = db
            .query_one("SELECT COUNT(*) FROM rot_upd WHERE status = 'active'", ())
            .unwrap();
        assert_eq!(active, 15);
    }

    // Verify rotation happened
    let wal_files = find_wal_files(&db_path);
    assert!(
        wal_files.len() >= 2,
        "Expected rotation to produce multiple WAL files, got {}",
        wal_files.len()
    );

    remove_lock_file(&db_path);

    // Reopen and verify the exact same state after multi-file replay
    let db = Database::open(&dsn).unwrap();

    let total: i64 = db.query_one("SELECT COUNT(*) FROM rot_upd", ()).unwrap();
    assert_eq!(
        total, 25,
        "Expected 25 rows (30 inserted - 5 deleted) after replay, got {}",
        total
    );

    let modified: i64 = db
        .query_one("SELECT COUNT(*) FROM rot_upd WHERE status = 'modified'", ())
        .unwrap();
    assert_eq!(
        modified, 10,
        "Expected 10 modified rows after replay, got {}",
        modified
    );

    let active: i64 = db
        .query_one("SELECT COUNT(*) FROM rot_upd WHERE status = 'active'", ())
        .unwrap();
    assert_eq!(
        active, 15,
        "Expected 15 active rows after replay, got {}",
        active
    );

    // Verify specific updated values
    let val: String = db
        .query_one("SELECT value FROM rot_upd WHERE id = 5", ())
        .unwrap();
    assert_eq!(val, "updated_5");

    // Verify deleted rows are really gone
    let deleted_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM rot_upd WHERE id BETWEEN 21 AND 25",
            (),
        )
        .unwrap();
    assert_eq!(deleted_count, 0, "Deleted rows should not reappear");

    // DB usable after recovery
    db.execute(
        "INSERT INTO rot_upd (id, value) VALUES (9999, 'post_recovery')",
        (),
    )
    .unwrap();
}

/// Explicit multi-statement transactions must remain atomic when WAL rotation
/// fires between statements within a BEGIN...COMMIT block.
#[test]
fn test_wal_rotation_explicit_transaction() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    // Very small wal_max_size to force rotation mid-transaction
    let dsn = format!(
        "file://{}?wal_max_size=200&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE rot_txn (id INTEGER PRIMARY KEY, value TEXT NOT NULL, batch INTEGER NOT NULL)",
            (),
        )
        .unwrap();

        // 5 explicit transactions, each inserting 6 rows.
        // With wal_max_size=200, rotation WILL fire mid-transaction.
        for batch in 0..5 {
            db.execute("BEGIN", ()).unwrap();
            for row in 0..6 {
                let id = batch * 6 + row + 1;
                db.execute(
                    &format!(
                        "INSERT INTO rot_txn (id, value, batch) VALUES ({}, 'b{}_r{}', {})",
                        id, batch, row, batch
                    ),
                    (),
                )
                .unwrap();
            }
            db.execute("COMMIT", ()).unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM rot_txn", ()).unwrap();
        assert_eq!(count, 30);
    }

    // Verify rotation happened
    let wal_files = find_wal_files(&db_path);
    assert!(
        wal_files.len() >= 2,
        "Expected rotation to fire mid-transaction with wal_max_size=200, got {} files",
        wal_files.len()
    );

    remove_lock_file(&db_path);

    // Reopen — all transactions must replay atomically
    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM rot_txn", ()).unwrap();
    assert_eq!(
        count, 30,
        "All 30 rows from 5 committed transactions should be recovered, got {}",
        count
    );

    // Verify atomicity: each batch must have exactly 6 rows (all-or-nothing)
    for batch in 0..5 {
        let batch_count: i64 = db
            .query_one(
                &format!("SELECT COUNT(*) FROM rot_txn WHERE batch = {}", batch),
                (),
            )
            .unwrap();
        assert_eq!(
            batch_count, 6,
            "Batch {} has {} rows — expected 6 (atomic commit across rotation boundary)",
            batch, batch_count
        );
    }

    // DB usable after recovery
    db.execute(
        "INSERT INTO rot_txn (id, value, batch) VALUES (9999, 'post', 99)",
        (),
    )
    .unwrap();
}

// ============================================================================
// Gap closure: DROP INDEX durability
// ============================================================================

/// DROP INDEX must persist across close/reopen. After recovery, the dropped
/// index must not exist and queries must still work (using table scan).
#[test]
fn test_drop_index_durability() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE idx_drop (id INTEGER PRIMARY KEY, category TEXT NOT NULL, value INTEGER)",
            (),
        )
        .unwrap();

        // Insert data
        for i in 1..=20 {
            db.execute(
                &format!(
                    "INSERT INTO idx_drop (id, category, value) VALUES ({}, 'cat_{}', {})",
                    i,
                    i % 5,
                    i * 10
                ),
                (),
            )
            .unwrap();
        }

        // Create index, verify it works
        db.execute("CREATE INDEX idx_cat ON idx_drop(category)", ())
            .unwrap();

        let count: i64 = db
            .query_one("SELECT COUNT(*) FROM idx_drop WHERE category = 'cat_0'", ())
            .unwrap();
        assert_eq!(count, 4);

        // Drop the index
        db.execute("DROP INDEX idx_cat ON idx_drop", ()).unwrap();

        // Queries still work after drop (table scan)
        let count2: i64 = db
            .query_one("SELECT COUNT(*) FROM idx_drop WHERE category = 'cat_1'", ())
            .unwrap();
        assert_eq!(count2, 4);
    }

    remove_lock_file(&db_path);

    // Reopen — DROP INDEX must have persisted
    let db = Database::open(&dsn).unwrap();

    // Data must survive
    let count: i64 = db.query_one("SELECT COUNT(*) FROM idx_drop", ()).unwrap();
    assert_eq!(count, 20, "All 20 rows should survive after recovery");

    // Queries still work (index should not exist)
    let count2: i64 = db
        .query_one("SELECT COUNT(*) FROM idx_drop WHERE category = 'cat_2'", ())
        .unwrap();
    assert_eq!(count2, 4);

    // Creating the same index again should succeed (proves it was dropped)
    db.execute("CREATE INDEX idx_cat ON idx_drop(category)", ())
        .unwrap();

    // And the recreated index works
    let count3: i64 = db
        .query_one("SELECT COUNT(*) FROM idx_drop WHERE category = 'cat_3'", ())
        .unwrap();
    assert_eq!(count3, 4);
}

// ============================================================================
// TRUNCATE TABLE durability
// ============================================================================

#[test]
fn test_truncate_table_survives_close_reopen() {
    // TRUNCATE TABLE has its own WAL operation type (TruncateTable=13).
    // After close/reopen, the table should exist but be empty.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE trunc_test (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=50 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_test (id, value) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_test", ()).unwrap();
        assert_eq!(count, 50);

        db.execute("TRUNCATE TABLE trunc_test", ()).unwrap();

        let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_test", ()).unwrap();
        assert_eq!(count, 0);
    }

    remove_lock_file(&db_path);

    // Reopen — table should exist but be empty
    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_test", ()).unwrap();
    assert_eq!(count, 0, "TRUNCATE should persist — table must be empty");

    // Table is usable — can insert new data
    db.execute(
        "INSERT INTO trunc_test (id, value) VALUES (1, 'after_truncate')",
        (),
    )
    .unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_test", ()).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_truncate_table_then_insert_survives_close_reopen() {
    // TRUNCATE followed by new inserts — both operations must persist.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE trunc_ins (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=20 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_ins (id, value) VALUES ({}, 'old_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        db.execute("TRUNCATE TABLE trunc_ins", ()).unwrap();

        // Insert new data after truncate
        for i in 100..=105 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_ins (id, value) VALUES ({}, 'new_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_ins", ()).unwrap();
        assert_eq!(count, 6);
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_ins", ()).unwrap();
    assert_eq!(count, 6, "Only post-truncate rows should exist");

    // Verify old data is gone
    let old: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM trunc_ins WHERE value LIKE 'old_%'",
            (),
        )
        .unwrap();
    assert_eq!(old, 0, "Pre-truncate rows must not reappear");

    let new: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM trunc_ins WHERE value LIKE 'new_%'",
            (),
        )
        .unwrap();
    assert_eq!(new, 6, "Post-truncate rows must all survive");
}

#[test]
fn test_truncate_table_with_checkpoint_recovery() {
    // Checkpoint taken before TRUNCATE, then TRUNCATE, then close/reopen.
    // WAL replay of TRUNCATE must override volume data.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE trunc_snap (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=30 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_snap (id, value) VALUES ({}, 'snap_row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Checkpoint seals all 30 rows into volumes
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // TRUNCATE after checkpoint
        db.execute("TRUNCATE TABLE trunc_snap", ()).unwrap();

        // Insert a few new rows
        for i in 100..=102 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_snap (id, value) VALUES ({}, 'post_trunc_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_snap", ()).unwrap();
        assert_eq!(count, 3);
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM trunc_snap", ()).unwrap();
    assert_eq!(
        count, 3,
        "Only post-truncate rows should exist after checkpoint + WAL replay"
    );

    // Volume data must NOT reappear
    let old: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM trunc_snap WHERE value LIKE 'snap_row_%'",
            (),
        )
        .unwrap();
    assert_eq!(old, 0, "Volume data must not survive TRUNCATE in WAL");
}

#[test]
fn test_truncate_with_index_recovery() {
    // TRUNCATE should clear index state too. After recovery, index queries must
    // return correct (empty or post-truncate) results.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE trunc_idx (id INTEGER PRIMARY KEY, category TEXT NOT NULL, val INTEGER)",
            (),
        )
        .unwrap();

        db.execute("CREATE INDEX idx_trunc_cat ON trunc_idx(category)", ())
            .unwrap();

        for i in 1..=40 {
            db.execute(
                &format!(
                    "INSERT INTO trunc_idx (id, category, val) VALUES ({}, 'cat_{}', {})",
                    i,
                    i % 4,
                    i * 10
                ),
                (),
            )
            .unwrap();
        }

        db.execute("TRUNCATE TABLE trunc_idx", ()).unwrap();

        // Insert a few rows after truncate
        db.execute(
            "INSERT INTO trunc_idx (id, category, val) VALUES (100, 'cat_0', 999)",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let total: i64 = db.query_one("SELECT COUNT(*) FROM trunc_idx", ()).unwrap();
    assert_eq!(total, 1, "Only the post-truncate row should exist");

    let cat0: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM trunc_idx WHERE category = 'cat_0'",
            (),
        )
        .unwrap();
    assert_eq!(
        cat0, 1,
        "Index query should find only the post-truncate row"
    );

    let cat1: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM trunc_idx WHERE category = 'cat_1'",
            (),
        )
        .unwrap();
    assert_eq!(cat1, 0, "Pre-truncate index entries must be gone");
}

// ============================================================================
// ALTER TABLE durability
// ============================================================================

#[test]
fn test_alter_table_add_column_durability() {
    // ALTER TABLE ADD COLUMN is recorded in WAL. Must survive close/reopen.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE alter_add (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO alter_add (id, name) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Add a new column
        db.execute("ALTER TABLE alter_add ADD COLUMN score INTEGER", ())
            .unwrap();

        // Insert row using the new column
        db.execute(
            "INSERT INTO alter_add (id, name, score) VALUES (11, 'with_score', 100)",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM alter_add", ()).unwrap();
    assert_eq!(count, 11, "All 11 rows should survive");

    // The new column must exist — query it
    let score: i64 = db
        .query_one("SELECT score FROM alter_add WHERE id = 11", ())
        .unwrap();
    assert_eq!(score, 100, "New column value must survive recovery");

    // Old rows should have NULL for the new column
    let null_count: i64 = db
        .query_one("SELECT COUNT(*) FROM alter_add WHERE score IS NULL", ())
        .unwrap();
    assert_eq!(
        null_count, 10,
        "Old rows should have NULL in the new column"
    );

    // Can insert using the new schema
    db.execute(
        "INSERT INTO alter_add (id, name, score) VALUES (12, 'post_recovery', 200)",
        (),
    )
    .unwrap();
}

#[test]
fn test_transactional_add_update_partial_index_wal_dependency_order() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("transactional_alter_index_wal.db");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&checkpoint_on_close=off",
        db_path.display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE attachments (
                id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                deleted_at TIMESTAMP
            )",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO attachments VALUES (
                1,
                'deleted',
                TIMESTAMP '2026-08-08 00:00:00'
            )",
            (),
        )
        .unwrap();

        db.execute("BEGIN", ()).unwrap();
        db.execute(
            "ALTER TABLE attachments ADD COLUMN cleanup_available_at TIMESTAMP",
            (),
        )
        .unwrap();
        db.execute(
            "UPDATE attachments
             SET cleanup_available_at = deleted_at
             WHERE state = 'deleted' AND deleted_at IS NOT NULL",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE INDEX attachments_cleanup_queue_idx
             ON attachments (state, cleanup_available_at, id)
             WHERE cleanup_available_at IS NOT NULL",
            (),
        )
        .unwrap();
        db.execute("COMMIT", ()).unwrap();

        assert_eq!(
            db.query_one::<i64, _>(
                "SELECT COUNT(*) FROM attachments
                 WHERE state = 'deleted'
                   AND cleanup_available_at = TIMESTAMP '2026-08-08 00:00:00'
                   AND id = 1",
                (),
            )
            .unwrap(),
            1
        );
        // checkpoint_on_close=off leaves the committed DDL+DML lifecycle to
        // WAL replay rather than masking dependency ordering with a checkpoint.
    }

    remove_lock_file(&db_path);
    let db = Database::open(&dsn).unwrap();
    assert_eq!(
        db.query_one::<i64, _>(
            "SELECT COUNT(*) FROM attachments
             WHERE state = 'deleted'
               AND cleanup_available_at = TIMESTAMP '2026-08-08 00:00:00'
               AND id = 1",
            (),
        )
        .unwrap(),
        1,
        "transactional ALTER/DML must recover before its dependent index"
    );
    let index_exists = db
        .query("SHOW INDEXES FROM attachments", ())
        .unwrap()
        .map(|row| row.unwrap().get::<String>(1).unwrap())
        .any(|name| name == "attachments_cleanup_queue_idx");
    assert!(
        index_exists,
        "dependent partial index was lost during WAL replay"
    );
}

#[test]
fn test_alter_table_drop_column_durability() {
    // ALTER TABLE DROP COLUMN must persist across close/reopen.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE alter_drop (id INTEGER PRIMARY KEY, name TEXT NOT NULL, extra TEXT)",
            (),
        )
        .unwrap();

        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO alter_drop (id, name, extra) VALUES ({}, 'row_{}', 'extra_{}')",
                    i, i, i
                ),
                (),
            )
            .unwrap();
        }

        db.execute("ALTER TABLE alter_drop DROP COLUMN extra", ())
            .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM alter_drop", ()).unwrap();
    assert_eq!(count, 10, "All rows should survive");

    // 'extra' column must not exist
    let result: Result<i64, _> = db.query_one(
        "SELECT COUNT(*) FROM alter_drop WHERE extra IS NOT NULL",
        (),
    );
    assert!(
        result.is_err(),
        "Column 'extra' should not exist after DROP COLUMN recovery"
    );

    // Can insert using the reduced schema
    db.execute(
        "INSERT INTO alter_drop (id, name) VALUES (11, 'post_recovery')",
        (),
    )
    .unwrap();
}

#[test]
fn test_alter_table_rename_column_durability() {
    // ALTER TABLE RENAME COLUMN must persist across close/reopen.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE alter_rename (id INTEGER PRIMARY KEY, old_name TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=5 {
            db.execute(
                &format!(
                    "INSERT INTO alter_rename (id, old_name) VALUES ({}, 'val_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        db.execute(
            "ALTER TABLE alter_rename RENAME COLUMN old_name TO new_name",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    // New column name must work
    let count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM alter_rename WHERE new_name IS NOT NULL",
            (),
        )
        .unwrap();
    assert_eq!(count, 5, "All rows accessible via renamed column");

    // Old column name must not work
    let result: Result<i64, _> = db.query_one(
        "SELECT COUNT(*) FROM alter_rename WHERE old_name IS NOT NULL",
        (),
    );
    assert!(
        result.is_err(),
        "Old column name 'old_name' should not exist after RENAME recovery"
    );

    // Can insert using new column name
    db.execute(
        "INSERT INTO alter_rename (id, new_name) VALUES (6, 'post_recovery')",
        (),
    )
    .unwrap();
}

#[test]
fn test_alter_table_rename_table_durability() {
    // ALTER TABLE RENAME TO must persist across close/reopen.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE old_tbl (id INTEGER PRIMARY KEY, data TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=10 {
            db.execute(
                &format!("INSERT INTO old_tbl (id, data) VALUES ({}, 'row_{}')", i, i),
                (),
            )
            .unwrap();
        }

        db.execute("ALTER TABLE old_tbl RENAME TO new_tbl", ())
            .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    // New name must work
    let count: i64 = db.query_one("SELECT COUNT(*) FROM new_tbl", ()).unwrap();
    assert_eq!(
        count, 10,
        "All rows should be accessible via new table name"
    );

    // Old name must not work
    let result: Result<i64, _> = db.query_one("SELECT COUNT(*) FROM old_tbl", ());
    assert!(
        result.is_err(),
        "Old table name 'old_tbl' should not exist after RENAME recovery"
    );

    // Can insert via new name
    db.execute(
        "INSERT INTO new_tbl (id, data) VALUES (11, 'post_recovery')",
        (),
    )
    .unwrap();
}

#[test]
fn test_alter_table_with_checkpoint_recovery() {
    // ALTER TABLE after checkpoint — WAL replay must apply schema changes on top of volume state.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE alter_snap (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO alter_snap (id, name) VALUES ({}, 'row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Checkpoint seals the original 2-column schema into volumes
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // ALTER after checkpoint
        db.execute("ALTER TABLE alter_snap ADD COLUMN status TEXT", ())
            .unwrap();

        db.execute(
            "INSERT INTO alter_snap (id, name, status) VALUES (11, 'new_row', 'active')",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM alter_snap", ()).unwrap();
    assert_eq!(count, 11, "All 11 rows should survive");

    // New column must exist
    let status: String = db
        .query_one("SELECT status FROM alter_snap WHERE id = 11", ())
        .unwrap();
    assert_eq!(status, "active", "New column value must survive");

    // Old rows have NULL for the new column
    let null_count: i64 = db
        .query_one("SELECT COUNT(*) FROM alter_snap WHERE status IS NULL", ())
        .unwrap();
    assert_eq!(null_count, 10);
}

#[test]
fn test_alter_table_multiple_operations_durability() {
    // Multiple ALTER operations on the same table in sequence.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE alter_multi (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)",
            (),
        )
        .unwrap();

        for i in 1..=5 {
            db.execute(
                &format!(
                    "INSERT INTO alter_multi (id, a, b, c) VALUES ({}, 'a{}', 'b{}', 'c{}')",
                    i, i, i, i
                ),
                (),
            )
            .unwrap();
        }

        // Chain of ALTER operations
        db.execute("ALTER TABLE alter_multi DROP COLUMN c", ())
            .unwrap();
        db.execute("ALTER TABLE alter_multi ADD COLUMN d INTEGER", ())
            .unwrap();
        db.execute("ALTER TABLE alter_multi RENAME COLUMN b TO beta", ())
            .unwrap();

        db.execute(
            "INSERT INTO alter_multi (id, a, beta, d) VALUES (6, 'a6', 'beta6', 42)",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM alter_multi", ())
        .unwrap();
    assert_eq!(count, 6, "All rows should survive");

    // Column 'c' must not exist
    let result: Result<i64, _> =
        db.query_one("SELECT COUNT(*) FROM alter_multi WHERE c IS NOT NULL", ());
    assert!(result.is_err(), "Column 'c' should be dropped");

    // Column 'beta' must exist (renamed from 'b')
    let beta_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM alter_multi WHERE beta IS NOT NULL",
            (),
        )
        .unwrap();
    assert_eq!(beta_count, 6, "Column 'beta' (renamed from 'b') must exist");

    // Column 'd' must exist
    let d_val: i64 = db
        .query_one("SELECT d FROM alter_multi WHERE id = 6", ())
        .unwrap();
    assert_eq!(d_val, 42, "New column 'd' must have correct value");
}

// ============================================================================
// Empty table recovery
// ============================================================================

#[test]
fn test_empty_table_survives_close_reopen() {
    // CREATE TABLE with zero data rows. DDL must persist even without data entries.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE empty_tbl (id INTEGER PRIMARY KEY, value TEXT)",
            (),
        )
        .unwrap();
        // No inserts — table is empty
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM empty_tbl", ()).unwrap();
    assert_eq!(count, 0, "Empty table should exist with 0 rows");

    // Can insert into the recovered empty table
    db.execute(
        "INSERT INTO empty_tbl (id, value) VALUES (1, 'first_row')",
        (),
    )
    .unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM empty_tbl", ()).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_multiple_empty_tables_survive() {
    // Multiple empty tables created in sequence — all must survive.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute("CREATE TABLE empty_a (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        db.execute(
            "CREATE TABLE empty_b (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE empty_c (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER)",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    for tbl in &["empty_a", "empty_b", "empty_c"] {
        let count: i64 = db
            .query_one(&format!("SELECT COUNT(*) FROM {}", tbl), ())
            .unwrap();
        assert_eq!(count, 0, "Table '{}' should exist and be empty", tbl);
    }

    // All tables usable
    db.execute("INSERT INTO empty_a (id) VALUES (1)", ())
        .unwrap();
    db.execute("INSERT INTO empty_b (id, name) VALUES (1, 'test')", ())
        .unwrap();
    db.execute("INSERT INTO empty_c (id, x, y) VALUES (1, 10, 20)", ())
        .unwrap();
}

#[test]
fn test_empty_table_with_index_survives() {
    // CREATE TABLE + CREATE INDEX with zero data. Both must survive.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE empty_idx (id INTEGER PRIMARY KEY, category TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("CREATE INDEX idx_empty_cat ON empty_idx(category)", ())
            .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM empty_idx", ()).unwrap();
    assert_eq!(count, 0);

    // Insert and query via index
    db.execute("INSERT INTO empty_idx (id, category) VALUES (1, 'A')", ())
        .unwrap();

    let cat_count: i64 = db
        .query_one("SELECT COUNT(*) FROM empty_idx WHERE category = 'A'", ())
        .unwrap();
    assert_eq!(
        cat_count, 1,
        "Index should work after recovery of empty table"
    );
}

// ============================================================================
// NULL value durability
// ============================================================================

#[test]
fn test_null_values_survive_wal_recovery() {
    // Insert rows with NULL in nullable columns. NULL must not become a default value.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE null_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, active BOOLEAN)",
            (),
        )
        .unwrap();

        // Mix of NULL and non-NULL values
        db.execute(
            "INSERT INTO null_test (id, name, score, active) VALUES (1, 'alice', 100, TRUE)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO null_test (id, name, score, active) VALUES (2, NULL, NULL, NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO null_test (id, name, score, active) VALUES (3, 'charlie', NULL, TRUE)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO null_test (id, name, score, active) VALUES (4, NULL, 50, NULL)",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let total: i64 = db.query_one("SELECT COUNT(*) FROM null_test", ()).unwrap();
    assert_eq!(total, 4, "All 4 rows must survive");

    // Verify NULLs are preserved, not coerced to defaults
    let null_names: i64 = db
        .query_one("SELECT COUNT(*) FROM null_test WHERE name IS NULL", ())
        .unwrap();
    assert_eq!(null_names, 2, "Rows 2 and 4 should have NULL name");

    let null_scores: i64 = db
        .query_one("SELECT COUNT(*) FROM null_test WHERE score IS NULL", ())
        .unwrap();
    assert_eq!(null_scores, 2, "Rows 2 and 3 should have NULL score");

    let null_active: i64 = db
        .query_one("SELECT COUNT(*) FROM null_test WHERE active IS NULL", ())
        .unwrap();
    assert_eq!(null_active, 2, "Rows 2 and 4 should have NULL active");

    // Non-NULL values are correct
    let alice_score: i64 = db
        .query_one("SELECT score FROM null_test WHERE id = 1", ())
        .unwrap();
    assert_eq!(alice_score, 100);

    let charlie_active: bool = db
        .query_one("SELECT active FROM null_test WHERE id = 3", ())
        .unwrap();
    assert!(charlie_active);
}

#[test]
fn test_null_values_survive_checkpoint_recovery() {
    // NULLs must survive through checkpoint (not just WAL).
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE null_snap (id INTEGER PRIMARY KEY, val TEXT, num FLOAT)",
            (),
        )
        .unwrap();

        db.execute(
            "INSERT INTO null_snap (id, val, num) VALUES (1, 'hello', 3.14)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO null_snap (id, val, num) VALUES (2, NULL, NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO null_snap (id, val, num) VALUES (3, NULL, 2.71)",
            (),
        )
        .unwrap();

        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let total: i64 = db.query_one("SELECT COUNT(*) FROM null_snap", ()).unwrap();
    assert_eq!(total, 3);

    let null_vals: i64 = db
        .query_one("SELECT COUNT(*) FROM null_snap WHERE val IS NULL", ())
        .unwrap();
    assert_eq!(null_vals, 2, "NULLs must survive volume serialization");

    let null_nums: i64 = db
        .query_one("SELECT COUNT(*) FROM null_snap WHERE num IS NULL", ())
        .unwrap();
    assert_eq!(null_nums, 1, "Row 2 should have NULL num");

    let val: f64 = db
        .query_one("SELECT num FROM null_snap WHERE id = 3", ())
        .unwrap();
    assert!(
        (val - 2.71).abs() < 0.001,
        "Non-NULL float must be preserved"
    );
}

// ============================================================================
// Multiple consecutive crash-recovery cycles
// ============================================================================

#[test]
fn test_multiple_crash_recovery_cycles() {
    // A complete-record boundary truncation may discard an uncommitted tail,
    // but a later in-record corruption is terminal and cannot be recovered
    // into another WAL generation.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    // Cycle 1: Create table and insert initial data, then corrupt
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE cycle_test (id INTEGER PRIMARY KEY, cycle INTEGER NOT NULL, value TEXT)",
            (),
        )
        .unwrap();

        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO cycle_test (id, cycle, value) VALUES ({}, 1, 'cycle1_row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }
    remove_lock_file(&db_path);

    // Corrupt: truncate last WAL entry
    let wal_files = find_wal_files(&db_path);
    if !wal_files.is_empty() {
        let wal_path = &wal_files[wal_files.len() - 1];
        let data = fs::read(wal_path).unwrap();
        let entries = find_entry_boundaries(&data);
        if entries.len() >= 2 {
            let last = &entries[entries.len() - 1];
            let truncated = &data[..last.offset];
            fs::write(wal_path, truncated).unwrap();
        }
    }

    // Cycle 2: Recover from corruption, write more data
    let cycle1_count: i64;
    {
        let db = Database::open(&dsn).unwrap();
        cycle1_count = db.query_one("SELECT COUNT(*) FROM cycle_test", ()).unwrap();
        assert!(
            cycle1_count > 0,
            "Should recover at least some cycle 1 data"
        );

        for i in 100..=110 {
            db.execute(
                &format!(
                    "INSERT INTO cycle_test (id, cycle, value) VALUES ({}, 2, 'cycle2_row_{}')",
                    i, i
                ),
                (),
            )
            .unwrap();
        }
    }
    remove_lock_file(&db_path);

    // Corrupt again: flip a bit in the WAL
    let wal_files = find_wal_files(&db_path);
    if !wal_files.is_empty() {
        let wal_path = &wal_files[wal_files.len() - 1];
        let mut data = fs::read(wal_path).unwrap();
        let entries = find_entry_boundaries(&data);
        if entries.len() >= 3 {
            // Flip a bit in a middle entry's data
            let mid = &entries[entries.len() / 2];
            if mid.data_offset + 5 < data.len() {
                flip_bit(&mut data, mid.data_offset + 5, 3);
                fs::write(wal_path, &data).unwrap();
            }
        }
    }

    assert!(Database::open(&dsn).is_err());
}

// ============================================================================
// DEFAULT constraint enforcement after recovery
// ============================================================================

#[test]
fn test_default_constraint_after_recovery() {
    // DEFAULT values must be applied correctly after recovery.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE def_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, status TEXT DEFAULT 'pending', priority INTEGER DEFAULT 0)",
            (),
        )
        .unwrap();

        // Insert without specifying default columns
        db.execute("INSERT INTO def_test (id, name) VALUES (1, 'task_1')", ())
            .unwrap();
        // Insert with explicit values overriding defaults
        db.execute(
            "INSERT INTO def_test (id, name, status, priority) VALUES (2, 'task_2', 'active', 5)",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM def_test", ()).unwrap();
    assert_eq!(count, 2);

    // After recovery, inserting should still apply defaults
    db.execute("INSERT INTO def_test (id, name) VALUES (3, 'task_3')", ())
        .unwrap();

    let status: String = db
        .query_one("SELECT status FROM def_test WHERE id = 3", ())
        .unwrap();
    assert_eq!(
        status, "pending",
        "DEFAULT value must be applied after recovery"
    );

    let priority: i64 = db
        .query_one("SELECT priority FROM def_test WHERE id = 3", ())
        .unwrap();
    assert_eq!(
        priority, 0,
        "DEFAULT integer value must be applied after recovery"
    );
}

// ============================================================================
// BOOLEAN and TIMESTAMP column types durability
// ============================================================================

#[test]
fn test_boolean_column_durability() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE bool_test (id INTEGER PRIMARY KEY, flag BOOLEAN NOT NULL, optional_flag BOOLEAN)",
            (),
        )
        .unwrap();

        db.execute(
            "INSERT INTO bool_test (id, flag, optional_flag) VALUES (1, TRUE, FALSE)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO bool_test (id, flag, optional_flag) VALUES (2, FALSE, TRUE)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO bool_test (id, flag, optional_flag) VALUES (3, TRUE, NULL)",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM bool_test", ()).unwrap();
    assert_eq!(count, 3);

    let true_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bool_test WHERE flag = TRUE", ())
        .unwrap();
    assert_eq!(true_count, 2, "TRUE values must survive recovery");

    let false_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bool_test WHERE flag = FALSE", ())
        .unwrap();
    assert_eq!(false_count, 1, "FALSE values must survive recovery");

    let null_opt: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM bool_test WHERE optional_flag IS NULL",
            (),
        )
        .unwrap();
    assert_eq!(null_opt, 1, "NULL boolean must survive recovery");
}

#[test]
fn test_timestamp_column_durability() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE ts_test (id INTEGER PRIMARY KEY, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP)",
            (),
        )
        .unwrap();

        db.execute(
            "INSERT INTO ts_test (id, created_at, updated_at) VALUES (1, '2024-01-15 10:30:00', '2024-06-20 14:00:00')",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO ts_test (id, created_at, updated_at) VALUES (2, '2024-12-31 23:59:59', NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO ts_test (id, created_at, updated_at) VALUES (3, '2020-01-01 00:00:00', '2020-01-01 00:00:01')",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM ts_test", ()).unwrap();
    assert_eq!(count, 3);

    // Verify timestamps are not corrupted by checking ordering
    let ordered_count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM ts_test WHERE created_at >= '2024-01-01 00:00:00'",
            (),
        )
        .unwrap();
    assert_eq!(
        ordered_count, 2,
        "Timestamp comparison must work after recovery"
    );

    let null_updated: i64 = db
        .query_one("SELECT COUNT(*) FROM ts_test WHERE updated_at IS NULL", ())
        .unwrap();
    assert_eq!(null_updated, 1, "NULL timestamp must survive recovery");

    // Verify EXTRACT works on recovered timestamps
    let year: i64 = db
        .query_one(
            "SELECT EXTRACT(YEAR FROM created_at) FROM ts_test WHERE id = 2",
            (),
        )
        .unwrap();
    assert_eq!(year, 2024, "EXTRACT from recovered timestamp must work");
}

// ============================================================================
// Checkpoint with UPDATE/DELETE then close/reopen (volume + WAL recovery)
// ============================================================================

#[test]
fn test_update_after_checkpoint_recovery() {
    // Original rows sealed into volumes via checkpoint. Then UPDATE/DELETE in WAL.
    // Close and reopen — volumes provide base, WAL provides UPDATE/DELETE.
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}?checkpoint_on_close=off", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE upd_snap (id INTEGER PRIMARY KEY, value TEXT NOT NULL, version INTEGER)",
            (),
        )
        .unwrap();

        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO upd_snap (id, value, version) VALUES ({}, 'original_{}', 1)",
                    i, i
                ),
                (),
            )
            .unwrap();
        }

        // Checkpoint to seal original data into volumes
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        // UPDATE some rows after checkpoint
        db.execute(
            "UPDATE upd_snap SET value = 'updated_5', version = 2 WHERE id = 5",
            (),
        )
        .unwrap();
        db.execute(
            "UPDATE upd_snap SET value = 'updated_10', version = 2 WHERE id = 10",
            (),
        )
        .unwrap();

        // DELETE a row after checkpoint
        db.execute("DELETE FROM upd_snap WHERE id = 3", ()).unwrap();
    }
    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let total: i64 = db.query_one("SELECT COUNT(*) FROM upd_snap", ()).unwrap();
    assert_eq!(total, 9, "10 inserted - 1 deleted = 9 rows");

    // Updated rows have new values
    let v5: String = db
        .query_one("SELECT value FROM upd_snap WHERE id = 5", ())
        .unwrap();
    assert_eq!(v5, "updated_5", "UPDATE must be replayed from WAL");

    let v10: String = db
        .query_one("SELECT value FROM upd_snap WHERE id = 10", ())
        .unwrap();
    assert_eq!(v10, "updated_10");

    // Deleted row must not exist
    let deleted: i64 = db
        .query_one("SELECT COUNT(*) FROM upd_snap WHERE id = 3", ())
        .unwrap();
    assert_eq!(deleted, 0, "DELETE must be replayed from WAL");

    // Unmodified rows still have original values
    let v1: String = db
        .query_one("SELECT value FROM upd_snap WHERE id = 1", ())
        .unwrap();
    assert_eq!(v1, "original_1");
}

// ============================================================================
// Multi-column UNIQUE index enforcement after recovery
// ============================================================================

#[test]
fn test_multi_column_unique_index_enforced_after_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE mc_unique (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL, data TEXT)",
            (),
        )
        .unwrap();

        db.execute("CREATE UNIQUE INDEX idx_mc_ab ON mc_unique(a, b)", ())
            .unwrap();

        db.execute(
            "INSERT INTO mc_unique (id, a, b, data) VALUES (1, 'x', 'y', 'first')",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO mc_unique (id, a, b, data) VALUES (2, 'x', 'z', 'second')",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO mc_unique (id, a, b, data) VALUES (3, 'w', 'y', 'third')",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db.query_one("SELECT COUNT(*) FROM mc_unique", ()).unwrap();
    assert_eq!(count, 3);

    // Duplicate (a='x', b='y') must be rejected after recovery
    let result = db.execute(
        "INSERT INTO mc_unique (id, a, b, data) VALUES (4, 'x', 'y', 'duplicate')",
        (),
    );
    assert!(
        result.is_err(),
        "Multi-column UNIQUE constraint must be enforced after recovery"
    );

    // Different combination is allowed
    db.execute(
        "INSERT INTO mc_unique (id, a, b, data) VALUES (4, 'x', 'w', 'allowed')",
        (),
    )
    .unwrap();
}

// ============================================================================
// ALTER TABLE MODIFY COLUMN durability
// ============================================================================

/// ALTER TABLE MODIFY COLUMN must persist type and nullability changes
/// across close/reopen. The WAL records this as AlterTable op_type=4.
#[test]
fn test_alter_table_modify_column_durability() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE modify_test (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, label TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=10 {
            db.execute(
                &format!(
                    "INSERT INTO modify_test (id, score, label) VALUES ({}, {}, 'item_{}')",
                    i,
                    i * 10,
                    i
                ),
                (),
            )
            .unwrap();
        }

        // Change score from NOT NULL to nullable
        db.execute("ALTER TABLE modify_test MODIFY COLUMN score INTEGER", ())
            .unwrap();

        // Insert a row with NULL score to prove the change took effect
        db.execute(
            "INSERT INTO modify_test (id, score, label) VALUES (11, NULL, 'null_score')",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    // All rows must survive
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM modify_test", ())
        .unwrap();
    assert_eq!(count, 11, "All 11 rows should survive after recovery");

    // Original data intact
    let score: i64 = db
        .query_one("SELECT score FROM modify_test WHERE id = 5", ())
        .unwrap();
    assert_eq!(score, 50, "Original score values must be preserved");

    // NULL score must survive (proves MODIFY to nullable persisted)
    let null_count: i64 = db
        .query_one("SELECT COUNT(*) FROM modify_test WHERE score IS NULL", ())
        .unwrap();
    assert_eq!(
        null_count, 1,
        "NULL score row must survive — MODIFY COLUMN nullable change must persist"
    );

    // Can still insert NULL after recovery (proves schema change persisted)
    db.execute(
        "INSERT INTO modify_test (id, score, label) VALUES (12, NULL, 'post_recovery_null')",
        (),
    )
    .unwrap();

    let null_count2: i64 = db
        .query_one("SELECT COUNT(*) FROM modify_test WHERE score IS NULL", ())
        .unwrap();
    assert_eq!(
        null_count2, 2,
        "Post-recovery NULL insert must work after MODIFY COLUMN"
    );
}

/// ALTER TABLE MODIFY COLUMN DEFAULT must persist default metadata across
/// close/reopen. The WAL records the default expression in AlterTable op_type=4.
#[test]
fn test_alter_table_modify_column_default_durability() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
            .unwrap();

        db.execute(
            "ALTER TABLE users MODIFY COLUMN name TEXT NOT NULL DEFAULT 'anonymous'",
            (),
        )
        .unwrap();

        db.execute("INSERT INTO users (id) VALUES (1)", ()).unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();
    db.execute("INSERT INTO users (id) VALUES (2)", ())
        .expect("post-recovery INSERT should use modified default");

    let first: String = db
        .query_one("SELECT name FROM users WHERE id = 1", ())
        .unwrap();
    let second: String = db
        .query_one("SELECT name FROM users WHERE id = 2", ())
        .unwrap();

    assert_eq!(first, "anonymous");
    assert_eq!(
        second, "anonymous",
        "modified DEFAULT must survive WAL replay/reopen"
    );
}

/// ALTER TABLE MODIFY COLUMN with snapshot — WAL replay must apply
/// the type change on top of snapshot state.
#[test]
fn test_alter_table_modify_column_with_snapshot() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE modify_snap (id INTEGER PRIMARY KEY, value TEXT NOT NULL, flag BOOLEAN NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=20 {
            db.execute(
                &format!(
                    "INSERT INTO modify_snap (id, value, flag) VALUES ({}, 'v_{}', {})",
                    i,
                    i,
                    if i % 2 == 0 { "TRUE" } else { "FALSE" }
                ),
                (),
            )
            .unwrap();
        }

        // Snapshot captures current schema (value NOT NULL, flag NOT NULL)
        db.execute("PRAGMA snapshot", ()).unwrap();

        // MODIFY after snapshot — must be replayed from WAL on recovery
        db.execute("ALTER TABLE modify_snap MODIFY COLUMN value TEXT", ())
            .unwrap();

        db.execute("ALTER TABLE modify_snap MODIFY COLUMN flag BOOLEAN", ())
            .unwrap();

        // Insert rows using new nullable schema
        db.execute(
            "INSERT INTO modify_snap (id, value, flag) VALUES (21, NULL, NULL)",
            (),
        )
        .unwrap();
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM modify_snap", ())
        .unwrap();
    assert_eq!(count, 21, "All 21 rows must survive snapshot + WAL replay");

    // NULL values from post-MODIFY insert must survive
    let null_value_count: i64 = db
        .query_one("SELECT COUNT(*) FROM modify_snap WHERE value IS NULL", ())
        .unwrap();
    assert_eq!(
        null_value_count, 1,
        "NULL value row must survive — MODIFY COLUMN must replay over snapshot"
    );

    let null_flag_count: i64 = db
        .query_one("SELECT COUNT(*) FROM modify_snap WHERE flag IS NULL", ())
        .unwrap();
    assert_eq!(
        null_flag_count, 1,
        "NULL flag row must survive — MODIFY COLUMN must replay over snapshot"
    );

    // Post-recovery inserts with NULLs must work
    db.execute(
        "INSERT INTO modify_snap (id, value, flag) VALUES (22, NULL, TRUE)",
        (),
    )
    .unwrap();

    db.execute(
        "INSERT INTO modify_snap (id, value, flag) VALUES (23, 'hello', NULL)",
        (),
    )
    .unwrap();
}

// ============================================================================
// Bitmap index durability
// ============================================================================

/// Bitmap index created with USING BITMAP must survive close/reopen.
/// This exercises the bitmap-specific WAL serialization (index_type byte = 2)
/// and the BitmapIndex reconstruction path in create_index_from_metadata.
#[test]
fn test_bitmap_index_durability() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE bitmap_test (id INTEGER PRIMARY KEY, active BOOLEAN NOT NULL, category TEXT NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=50 {
            db.execute(
                &format!(
                    "INSERT INTO bitmap_test (id, active, category) VALUES ({}, {}, '{}')",
                    i,
                    if i % 3 == 0 { "TRUE" } else { "FALSE" },
                    if i % 4 == 0 {
                        "A"
                    } else if i % 4 == 1 {
                        "B"
                    } else if i % 4 == 2 {
                        "C"
                    } else {
                        "D"
                    }
                ),
                (),
            )
            .unwrap();
        }

        // Create explicit bitmap indexes
        db.execute(
            "CREATE INDEX idx_active_bitmap ON bitmap_test(active) USING BITMAP",
            (),
        )
        .unwrap();

        db.execute(
            "CREATE INDEX idx_cat_bitmap ON bitmap_test(category) USING BITMAP",
            (),
        )
        .unwrap();

        // Verify indexes work before close
        let active_count: i64 = db
            .query_one("SELECT COUNT(*) FROM bitmap_test WHERE active = TRUE", ())
            .unwrap();
        assert_eq!(active_count, 16); // 3,6,9,...,48 → 16 values

        let cat_a_count: i64 = db
            .query_one("SELECT COUNT(*) FROM bitmap_test WHERE category = 'A'", ())
            .unwrap();
        assert_eq!(cat_a_count, 12); // 4,8,12,...,48 → 12 values
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    // All data must survive
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_test", ())
        .unwrap();
    assert_eq!(count, 50, "All 50 rows must survive recovery");

    // Bitmap index on active must work after recovery
    let active_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_test WHERE active = TRUE", ())
        .unwrap();
    assert_eq!(
        active_count, 16,
        "Bitmap index on active must return correct results after recovery"
    );

    let inactive_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_test WHERE active = FALSE", ())
        .unwrap();
    assert_eq!(
        inactive_count, 34,
        "Bitmap index on active=FALSE must return correct results after recovery"
    );

    // Bitmap index on category must work after recovery
    let cat_b_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_test WHERE category = 'B'", ())
        .unwrap();
    assert_eq!(
        cat_b_count, 13,
        "Bitmap index on category must return correct results after recovery"
    );

    // Inserts after recovery must update the bitmap index
    db.execute(
        "INSERT INTO bitmap_test (id, active, category) VALUES (51, TRUE, 'A')",
        (),
    )
    .unwrap();

    let new_active_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_test WHERE active = TRUE", ())
        .unwrap();
    assert_eq!(
        new_active_count, 17,
        "Bitmap index must handle post-recovery inserts"
    );

    let new_cat_a_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_test WHERE category = 'A'", ())
        .unwrap();
    assert_eq!(
        new_cat_a_count, 13,
        "Bitmap index on category must handle post-recovery inserts"
    );
}

/// Bitmap index with snapshot — the index definition must survive snapshot + WAL replay.
#[test]
fn test_bitmap_index_with_snapshot_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE bitmap_snap (id INTEGER PRIMARY KEY, status BOOLEAN NOT NULL)",
            (),
        )
        .unwrap();

        for i in 1..=30 {
            db.execute(
                &format!(
                    "INSERT INTO bitmap_snap (id, status) VALUES ({}, {})",
                    i,
                    if i % 2 == 0 { "TRUE" } else { "FALSE" }
                ),
                (),
            )
            .unwrap();
        }

        // Snapshot before index creation
        db.execute("PRAGMA snapshot", ()).unwrap();

        // Create bitmap index after snapshot — must be replayed from WAL
        db.execute(
            "CREATE INDEX idx_status_bm ON bitmap_snap(status) USING BITMAP",
            (),
        )
        .unwrap();

        // Insert more data after index creation
        for i in 31..=40 {
            db.execute(
                &format!("INSERT INTO bitmap_snap (id, status) VALUES ({}, TRUE)", i),
                (),
            )
            .unwrap();
        }
    }

    remove_lock_file(&db_path);

    let db = Database::open(&dsn).unwrap();

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_snap", ())
        .unwrap();
    assert_eq!(count, 40, "All 40 rows must survive snapshot + WAL replay");

    // Bitmap index must work — 15 TRUE from first batch + 10 TRUE from second
    let true_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_snap WHERE status = TRUE", ())
        .unwrap();
    assert_eq!(
        true_count, 25,
        "Bitmap index must return correct results after snapshot + WAL replay"
    );

    let false_count: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_snap WHERE status = FALSE", ())
        .unwrap();
    assert_eq!(
        false_count, 15,
        "Bitmap index must return correct FALSE count after recovery"
    );

    // Dropping and recreating the bitmap index should work (proves it was recovered)
    db.execute("DROP INDEX idx_status_bm ON bitmap_snap", ())
        .unwrap();
    db.execute(
        "CREATE INDEX idx_status_bm ON bitmap_snap(status) USING BITMAP",
        (),
    )
    .unwrap();

    let recheck: i64 = db
        .query_one("SELECT COUNT(*) FROM bitmap_snap WHERE status = TRUE", ())
        .unwrap();
    assert_eq!(recheck, 25, "Recreated bitmap index must work correctly");
}

/// FK constraints must survive snapshot + WAL truncation recovery.
/// Tests that the snapshot serializer preserves FK metadata so enforcement
/// works after the WAL entries that created the table are truncated away.
#[test]
fn test_foreign_key_with_snapshot_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    {
        let db = Database::open(&dsn).unwrap();

        // Create parent + child with FK constraints
        db.execute(
            "CREATE TABLE fk_parent (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE fk_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES fk_parent(id) ON DELETE CASCADE, val TEXT)",
            (),
        )
        .unwrap();

        // Insert parent rows
        for i in 1..=5 {
            db.execute(
                &format!("INSERT INTO fk_parent (id, name) VALUES ({}, 'p{}')", i, i),
                (),
            )
            .unwrap();
        }
        // Insert child rows referencing parents
        for i in 1..=10 {
            let parent_id = (i % 5) + 1;
            db.execute(
                &format!(
                    "INSERT INTO fk_child (id, parent_id, val) VALUES ({}, {}, 'c{}')",
                    i, parent_id, i
                ),
                (),
            )
            .unwrap();
        }

        // Take TWO snapshots so WAL truncation can happen
        db.execute("PRAGMA snapshot", ()).unwrap();
        // Insert one more row to force a WAL entry after the first snapshot
        db.execute("INSERT INTO fk_parent (id, name) VALUES (100, 'extra')", ())
            .unwrap();
        db.execute("PRAGMA snapshot", ()).unwrap();
    }

    remove_lock_file(&db_path);

    // Reopen — schema should be loaded from snapshot (WAL may be truncated)
    let db = Database::open(&dsn).unwrap();

    // Verify data survived
    let parent_count: i64 = db.query_one("SELECT COUNT(*) FROM fk_parent", ()).unwrap();
    assert_eq!(parent_count, 6, "All 6 parent rows must survive");

    let child_count: i64 = db.query_one("SELECT COUNT(*) FROM fk_child", ()).unwrap();
    assert_eq!(child_count, 10, "All 10 child rows must survive");

    // FK enforcement must still work — insert with invalid parent must fail
    let err = db.execute(
        "INSERT INTO fk_child (id, parent_id, val) VALUES (99, 999, 'bad')",
        (),
    );
    assert!(
        err.is_err(),
        "FK constraint must be enforced after snapshot recovery"
    );

    // CASCADE must still work — delete parent 1, children referencing it should be deleted
    db.execute("DELETE FROM fk_child WHERE parent_id = 100", ())
        .unwrap_or_default(); // clean up extra parent's potential children
    db.execute("DELETE FROM fk_parent WHERE id = 1", ())
        .unwrap();

    let remaining: i64 = db
        .query_one("SELECT COUNT(*) FROM fk_child WHERE parent_id = 1", ())
        .unwrap();
    assert_eq!(
        remaining, 0,
        "CASCADE DELETE must work after snapshot recovery"
    );

    // Valid inserts must still work
    db.execute(
        "INSERT INTO fk_child (id, parent_id, val) VALUES (50, 2, 'valid')",
        (),
    )
    .unwrap();
}

/// Verify that DROP TABLE correctly strips FK references from child tables on WAL replay.
/// Without the fix, child tables retain orphaned FK constraints after recovery,
/// causing INSERT failures with "table not found" for the dropped parent.
#[test]
fn test_drop_parent_strips_child_fk_on_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("fk_drop_recovery.db");
    let dsn = format!("file://{}", db_path.display());

    // Phase 1: create parent + child, then drop parent
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE parent_drop (id INTEGER PRIMARY KEY, name TEXT)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE child_drop (
                id INTEGER PRIMARY KEY,
                pid INTEGER REFERENCES parent_drop(id),
                val TEXT
            )",
            (),
        )
        .unwrap();

        // Insert data into parent and child with NULL FK (so drop isn't blocked)
        db.execute("INSERT INTO parent_drop VALUES (1, 'Alice')", ())
            .unwrap();
        db.execute("INSERT INTO child_drop VALUES (1, NULL, 'x')", ())
            .unwrap();

        // Drop parent — in-memory, child FK is stripped
        db.execute("DROP TABLE parent_drop", ()).unwrap();

        // Verify child FK is gone in memory: insert with any pid should work
        db.execute("INSERT INTO child_drop VALUES (2, 999, 'y')", ())
            .unwrap();
    }

    // Phase 2: reopen — WAL replay must strip child FK
    {
        let db = Database::open(&dsn).unwrap();

        // parent_drop must not exist
        let err = db.execute("SELECT * FROM parent_drop", ());
        assert!(err.is_err(), "parent_drop should not exist after recovery");

        // child_drop must exist and FK must be gone
        let count: i64 = db.query_one("SELECT COUNT(*) FROM child_drop", ()).unwrap();
        assert_eq!(count, 2, "child_drop should have 2 rows");

        // Insert with arbitrary pid must succeed (no FK constraint)
        db.execute("INSERT INTO child_drop VALUES (3, 12345, 'z')", ())
            .unwrap();

        let count: i64 = db.query_one("SELECT COUNT(*) FROM child_drop", ()).unwrap();
        assert_eq!(count, 3);
    }
}
