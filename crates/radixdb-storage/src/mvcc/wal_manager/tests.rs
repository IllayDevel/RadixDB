use super::*;
use std::sync::Arc;
use tempfile::tempdir;

fn find_last_lsn(path: &Path) -> Result<u64> {
    let mut reader = ValidatedWalReader::open(path)?;
    let mut last_lsn = 0;
    while let Some(entry) = reader.next_entry()? {
        last_lsn = last_lsn.max(entry.lsn);
    }
    Ok(last_lsn)
}

#[test]
fn canonical_retired_wal_directory_is_not_an_active_generation() {
    let directory = tempdir().unwrap();
    let retired = directory.path().join("retired");
    fs::create_dir(&retired).unwrap();
    fs::write(retired.join(WALManager::canonical_filename(2)), b"retired").unwrap();
    WALManager::validate_retired_directory(&retired, 3).unwrap();
}

#[test]
fn physical_snapshot_boundary_returns_an_exact_immutable_wal_suffix() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path(), SyncMode::Full).unwrap();
    append_committed_for_snapshot(&wal, 1, 10);
    let checkpoint = wal.create_checkpoint().unwrap();
    wal.prepare_checkpoint_retention(checkpoint).unwrap();
    append_committed_for_snapshot(&wal, 2, 20);

    let floor = crate::v6::WalReplayFloor::new(crate::v6::WalGeneration::new(1).unwrap(), 0);
    let sources = wal.freeze_snapshot_generations(floor).unwrap();
    assert_eq!(
        sources
            .iter()
            .map(|source| source.generation().get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(sources.iter().all(|source| {
        source.path().is_file()
            && std::fs::metadata(source.path()).unwrap().len() == source.byte_length()
    }));
    let frozen = sources
        .iter()
        .map(|source| std::fs::read(source.path()).unwrap())
        .collect::<Vec<_>>();

    append_committed_for_snapshot(&wal, 3, 30);
    assert_eq!(
        frozen,
        sources
            .iter()
            .map(|source| std::fs::read(source.path()).unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(wal.current_sequence(), 3);
}

#[test]
fn physical_snapshot_boundary_admits_an_empty_initial_wal_member() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WALManager::new(directory.path(), SyncMode::Full).unwrap();
    let floor = crate::v6::WalReplayFloor::new(crate::v6::WalGeneration::new(1).unwrap(), 0);

    let sources = wal.freeze_snapshot_generations(floor).unwrap();

    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].generation().get(), 1);
    assert_eq!(sources[0].byte_length(), 0);
    assert_eq!(wal.current_sequence(), 2);
}

fn append_committed_for_snapshot(wal: &WALManager, txn_id: i64, row_id: i64) {
    wal.append_entry(WALEntry::new(
        txn_id,
        "snapshot_rows".to_string(),
        row_id,
        WALOperationType::Insert,
        vec![row_id as u8],
    ))
    .unwrap();
    wal.write_commit_marker(txn_id).unwrap();
}

#[test]
fn retired_wal_directory_rejects_noncanonical_or_reachable_members() {
    let directory = tempdir().unwrap();
    let retired = directory.path().join("retired");
    fs::create_dir(&retired).unwrap();
    fs::write(retired.join("wal.tmp"), b"invalid").unwrap();
    assert!(WALManager::validate_retired_directory(&retired, 3).is_err());
    fs::remove_file(retired.join("wal.tmp")).unwrap();
    fs::write(
        retired.join(WALManager::canonical_filename(3)),
        b"reachable",
    )
    .unwrap();
    assert!(WALManager::validate_retired_directory(&retired, 3).is_err());
}

#[test]
fn runtime_pending_durability_counts_buffered_and_unsynced_wal_bytes() {
    let directory = tempdir().unwrap();
    let wal = WALManager::new(directory.path(), SyncMode::None).unwrap();
    wal.append_entry(WALEntry::new(
        1,
        "runtime".to_string(),
        1,
        WALOperationType::Insert,
        vec![7; 256],
    ))
    .unwrap();
    assert!(wal.pending_durability_bytes().unwrap() > 0);
    wal.sync().unwrap();
    assert_eq!(wal.pending_durability_bytes(), Some(0));
    wal.close().unwrap();
}

#[test]
fn catalog_mutation_and_shared_commit_use_one_exact_wal_lsn() {
    use radixdb_catalog::{
        CatalogMutation, CatalogMutationSet, ObjectId, ObjectKind, ObjectPrecondition,
    };

    let directory = tempdir().unwrap();
    let wal = WALManager::new(directory.path(), SyncMode::Full).unwrap();
    let mutation = CatalogMutationSet::new(
        [1; 16],
        [2; 16],
        1,
        vec![CatalogMutation::drop(
            ObjectPrecondition::new(ObjectId::new(), ObjectKind::View, 1).unwrap(),
        )],
        Vec::new(),
        Vec::new(),
    )
    .unwrap();

    let commit_lsn = wal
        .write_catalog_commit(41, [3; 16], 73, &mutation)
        .unwrap();
    assert_eq!(commit_lsn, 2);

    let mut observed = Vec::new();
    let info = wal
        .replay_two_phase(0, |entry| {
            observed.push(entry);
            Ok(())
        })
        .unwrap();
    assert_eq!(info.committed_transactions, 1);
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].operation, WALOperationType::CatalogMutation);
    assert_eq!(observed[1].operation, WALOperationType::Commit);
    assert_eq!(observed[1].lsn, commit_lsn);

    let decoded =
        crate::v6::decode_catalog_wal(&observed[0].data, crate::v6::CatalogWalReplayLimits::hard())
            .unwrap();
    assert_eq!(decoded.transactions().len(), 1);
    let transaction = &decoded.transactions()[0];
    assert_eq!(transaction.commit_lsn(), commit_lsn);
    assert_eq!(transaction.successor_catalog_id(), [3; 16]);
    assert_eq!(transaction.created_unix_ns(), 73);
    assert_eq!(transaction.mutation_set(), &mutation);
    let mut expected_identity = [0_u8; 16];
    expected_identity[..8].copy_from_slice(&41_i64.to_le_bytes());
    expected_identity[8..].copy_from_slice(&commit_lsn.to_le_bytes());
    assert_eq!(transaction.transaction_id().as_bytes(), expected_identity);

    let extracted = wal.read_catalog_transactions(0, 1024 * 1024).unwrap();
    assert_eq!(extracted, observed[0].data);
}

#[test]
fn r8_l01_batch_a_wal_hooks_reset_during_unwind() {
    let directory = tempdir().unwrap();
    let wal = WALManager::new(directory.path(), SyncMode::None).unwrap();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _append_hook = WalAppendTestHookGuard::install(&wal, std::sync::Arc::new(|_| {}));
        let _close_hook = WalCloseTestHookGuard::install(&wal, std::sync::Arc::new(|_| {}));
        panic!("exercise panic-safe hook reset");
    }));
    assert!(unwind.is_err());
    assert!(wal
        .append_test_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_none());
    assert!(wal
        .close_test_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_none());
}

#[test]
fn wal_test_hooks_are_scoped_to_one_manager() {
    let left_directory = tempdir().unwrap();
    let right_directory = tempdir().unwrap();
    let left = WALManager::new(left_directory.path(), SyncMode::None).unwrap();
    let right = WALManager::new(right_directory.path(), SyncMode::None).unwrap();
    let append_hits = Arc::new(AtomicU64::new(0));
    let close_hits = Arc::new(AtomicU64::new(0));

    let append_hits_by_hook = Arc::clone(&append_hits);
    let _append_hook = WalAppendTestHookGuard::install(
        &left,
        Arc::new(move |_| {
            append_hits_by_hook.fetch_add(1, Ordering::Relaxed);
        }),
    );
    let close_hits_by_hook = Arc::clone(&close_hits);
    let _close_hook = WalCloseTestHookGuard::install(
        &left,
        Arc::new(move |_| {
            close_hits_by_hook.fetch_add(1, Ordering::Relaxed);
        }),
    );

    // Engine-local transaction IDs deliberately collide. Neither hook may
    // observe the unrelated manager.
    right
        .append_entry(WALEntry::new(
            1,
            "right".to_string(),
            1,
            WALOperationType::Insert,
            vec![1],
        ))
        .unwrap();
    right.close().unwrap();
    assert_eq!(append_hits.load(Ordering::Relaxed), 0);
    assert_eq!(close_hits.load(Ordering::Relaxed), 0);

    left.append_entry(WALEntry::new(
        1,
        "left".to_string(),
        1,
        WALOperationType::Insert,
        vec![1],
    ))
    .unwrap();
    left.close().unwrap();
    assert_eq!(append_hits.load(Ordering::Relaxed), 1);
    assert_eq!(close_hits.load(Ordering::Relaxed), 1);
}

#[test]
fn test_wal_operation_type() {
    assert_eq!(WALOperationType::from_u8(1), Some(WALOperationType::Insert));
    assert_eq!(WALOperationType::from_u8(4), Some(WALOperationType::Commit));
    assert_eq!(WALOperationType::from_u8(0), None);
    for retired_catalog_tag in 6..=12 {
        assert_eq!(WALOperationType::from_u8(retired_catalog_tag), None);
    }
    assert_eq!(
        WALOperationType::from_u8(13),
        Some(WALOperationType::TruncateTable)
    );
    assert_eq!(WALOperationType::from_u8(14), None);
    assert_eq!(
        WALOperationType::from_u8(15),
        Some(WALOperationType::CatalogMutation)
    );

    assert!(WALOperationType::TruncateTable.is_ddl());
    assert!(WALOperationType::CatalogMutation.is_ddl());
    assert!(!WALOperationType::Insert.is_ddl());
    assert!(WALOperationType::Commit.is_transaction_end());
    assert!(!WALOperationType::Insert.is_transaction_end());
}

#[test]
fn catalog_owned_wal_v3_is_rejected_and_v4_roundtrips() {
    let mut entry = WALEntry::new(
        123,
        "test_table".to_string(),
        456,
        WALOperationType::Insert,
        vec![1, 2, 3, 4],
    );
    entry.lsn = 42; // Set LSN for encoding
    entry.previous_lsn = 41; // Set previous LSN for chaining
    entry.flags = WalFlags::NONE;

    let encoded = entry.encode().unwrap();
    assert!(!encoded.is_empty());

    // 32-byte header format: magic(4) + version(1) + flags(1) + header_size(2) +
    // LSN(8) + prev_lsn(8) + entry_size(4) + reserved(4) = 32 bytes
    // Verify header
    let magic = u32::from_le_bytes(encoded[0..4].try_into().unwrap());
    assert_eq!(magic, WAL_ENTRY_MAGIC);
    let version = encoded[4];
    assert_eq!(version, WAL_FORMAT_VERSION);
    let flags = WalFlags::from_byte(encoded[5]);
    assert_eq!(flags, WalFlags::NONE);
    let header_size = u16::from_le_bytes(encoded[6..8].try_into().unwrap());
    assert_eq!(header_size, WAL_HEADER_SIZE);
    let lsn = u64::from_le_bytes(encoded[8..16].try_into().unwrap());
    assert_eq!(lsn, 42);
    let previous_lsn = u64::from_le_bytes(encoded[16..24].try_into().unwrap());
    assert_eq!(previous_lsn, 41);

    // Data starts at offset 32, includes CRC at end
    let decoded = WALEntry::decode(entry.lsn, entry.previous_lsn, flags, &encoded[32..]).unwrap();

    assert_eq!(decoded.lsn, entry.lsn);
    assert_eq!(decoded.previous_lsn, entry.previous_lsn);
    assert_eq!(decoded.flags, entry.flags);
    assert_eq!(decoded.txn_id, 123);
    assert_eq!(decoded.table_id, entry.table_id);
    assert_eq!(decoded.row_id, 456);
    assert_eq!(decoded.operation, WALOperationType::Insert);
    assert_eq!(decoded.data, vec![1, 2, 3, 4]);

    // The retired name-addressed V3 framing must fail at the version boundary.
    // A cutover database never guesses how a previous record body was encoded.
    let mut legacy_v3 = encoded.clone();
    legacy_v3[4] = 3;
    let crc_offset = legacy_v3.len() - 4;
    let legacy_crc = crc32fast::hash(&legacy_v3[..crc_offset]);
    legacy_v3[crc_offset..].copy_from_slice(&legacy_crc.to_le_bytes());

    let public_decode = WALEntry::decode(
        entry.lsn,
        entry.previous_lsn,
        flags,
        &legacy_v3[WAL_HEADER_SIZE as usize..],
    );
    assert!(
        public_decode.is_err(),
        "current-version public decoder accepted retired name-addressed WAL"
    );

    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy-v3.wal");
    fs::write(&path, legacy_v3).unwrap();
    let mut reader = ValidatedWalReader::open(&path).unwrap();
    let error = reader.next_entry().unwrap_err();
    assert!(error.to_string().contains("unsupported WAL version"));
}

#[test]
fn test_wal_entry_crc_validation() {
    let mut entry = WALEntry::new(
        1,
        "test".to_string(),
        100,
        WALOperationType::Insert,
        vec![1, 2, 3],
    );
    entry.lsn = 1;
    entry.previous_lsn = 0;
    entry.flags = WalFlags::NONE;

    let mut encoded = entry.encode().unwrap();

    // Corrupt the data portion (after 32-byte header)
    if encoded.len() > 40 {
        encoded[40] ^= 0xFF; // Flip some bits in data portion
    }

    // Decode should fail due to CRC mismatch
    let result = WALEntry::decode(entry.lsn, entry.previous_lsn, entry.flags, &encoded[32..]);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("checksum mismatch"));
}

#[cfg(feature = "test-mutations")]
#[test]
fn b10_checksum_verification_invariant() {
    let mut entry = WALEntry::new(
        10,
        "b10".to_string(),
        100,
        WALOperationType::Insert,
        vec![1, 2, 3, 4],
    );
    entry.lsn = 10;
    entry.previous_lsn = 9;
    entry.flags = WalFlags::NONE;

    let mut encoded = entry.encode().expect("encode B10 WAL entry");
    let logical_payload_offset = WAL_HEADER_SIZE as usize + MIN_WAL_RECORD_DATA_SIZE;
    encoded[logical_payload_offset] ^= 0x40;

    let decoded = WALEntry::decode(
        entry.lsn,
        entry.previous_lsn,
        entry.flags,
        &encoded[WAL_HEADER_SIZE as usize..],
    );
    assert!(
        decoded.is_err(),
        "PRV-B10 invariant checksum_verification: corrupted WAL payload was accepted"
    );
    assert!(
        decoded
            .expect_err("corrupted WAL must be rejected")
            .to_string()
            .contains("checksum mismatch"),
        "PRV-B10 invariant checksum_verification: corruption was not rejected by CRC"
    );
}

#[test]
fn test_r2_l02_batch_a_validated_wal_contract() {
    const MAX_RECORD_DATA: usize = 64 * 1024 * 1024;

    fn append_committed(wal: &WALManager, txn_id: i64, row_id: i64, data: Vec<u8>) {
        wal.append_entry(WALEntry::new(
            txn_id,
            "r2_l02".to_string(),
            row_id,
            WALOperationType::Insert,
            data,
        ))
        .unwrap();
        wal.write_commit_marker(txn_id).unwrap();
    }

    fn wal_file(wal: &WALManager, wal_dir: &Path) -> PathBuf {
        wal_dir.join(wal.current_wal_file())
    }

    fn record_offsets(bytes: &[u8]) -> Vec<(usize, u64)> {
        let mut result = Vec::new();
        let mut offset = 0usize;
        while offset.checked_add(32).is_some_and(|end| end <= bytes.len()) {
            let magic = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            if magic != WAL_ENTRY_MAGIC {
                break;
            }
            let lsn = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
            let header_size =
                u16::from_le_bytes(bytes[offset + 6..offset + 8].try_into().unwrap()) as usize;
            let entry_size =
                u32::from_le_bytes(bytes[offset + 24..offset + 28].try_into().unwrap()) as usize;
            result.push((offset, lsn));
            let Some(next) = offset
                .checked_add(header_size)
                .and_then(|value| value.checked_add(entry_size))
                .and_then(|value| value.checked_add(4))
            else {
                break;
            };
            if next <= offset || next > bytes.len() {
                break;
            }
            offset = next;
        }
        result
    }

    fn replay_is_terminal_without_publication(wal_dir: &Path) -> bool {
        let Ok(wal) = WALManager::new(wal_dir, SyncMode::Full) else {
            return true;
        };
        let mut callbacks = 0usize;
        let result = wal.replay_two_phase(0, |_entry| {
            callbacks += 1;
            Ok(())
        });
        let _ = wal.close();
        result.is_err() && callbacks == 0
    }

    let mut failures = Vec::new();

    // A commit/abort outcome is authoritative only after the complete record,
    // including its CRC, has passed the same decoder used by REDO.
    {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let wal = WALManager::new(&wal_dir, SyncMode::Full).unwrap();
        append_committed(&wal, 1, 10, vec![1, 2, 3]);
        let path = wal_file(&wal, &wal_dir);
        wal.close().unwrap();

        let mut bytes = fs::read(&path).unwrap();
        let commit_offset = record_offsets(&bytes)
            .into_iter()
            .find_map(|(offset, lsn)| (lsn == 2).then_some(offset))
            .unwrap();
        // Preserve txn_id and header flags, but invalidate the protected
        // timestamp byte so the old analysis fast path still admits txn 1.
        bytes[commit_offset + WAL_HEADER_SIZE as usize + 19] ^= 0x5a;
        fs::write(&path, bytes).unwrap();

        if !replay_is_terminal_without_publication(&wal_dir) {
            failures.push("corrupt commit marker influenced recovery outcome".to_string());
        }
    }

    // Every header evolution field is part of admission. Unknown version,
    // undersized header and non-zero reserved bytes must not select V4.
    for (name, mutation) in [
        ("unknown version", (4usize, vec![WAL_FORMAT_VERSION + 1])),
        ("undersized header", (6usize, 31u16.to_le_bytes().to_vec())),
        ("non-zero reserved", (28usize, vec![1, 0, 0, 0])),
    ] {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let wal = WALManager::new(&wal_dir, SyncMode::Full).unwrap();
        append_committed(&wal, 2, 20, vec![4, 5, 6]);
        let path = wal_file(&wal, &wal_dir);
        wal.close().unwrap();

        let mut bytes = fs::read(&path).unwrap();
        let (offset, replacement) = mutation;
        bytes[offset..offset + replacement.len()].copy_from_slice(&replacement);
        fs::write(&path, bytes).unwrap();

        if !replay_is_terminal_without_publication(&wal_dir) {
            failures.push(format!("{} selected the V4 decoder", name));
        }
    }

    // Writer and decoder share one logical (decoded) size budget. Highly
    // compressible input must not bypass the replay limit.
    {
        let mut encoded = WALEntry::new(
            3,
            "r2_l02".to_string(),
            30,
            WALOperationType::Insert,
            vec![0u8; 1024],
        )
        .encode()
        .unwrap();
        let flags = WalFlags::from_byte(encoded[5]);
        assert!(flags.contains(WalFlags::COMPRESSED));
        let payload_offset = WAL_HEADER_SIZE as usize + MIN_WAL_RECORD_DATA_SIZE;
        encoded[payload_offset..payload_offset + 4]
            .copy_from_slice(&((MAX_RECORD_DATA + 1) as u32).to_le_bytes());
        let crc_offset = encoded.len() - 4;
        let crc = crc32fast::hash(&encoded[WAL_HEADER_SIZE as usize..crc_offset]);
        encoded[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        if WALEntry::decode(1, 0, flags, &encoded[WAL_HEADER_SIZE as usize..]).is_ok() {
            failures.push("LZ4 decoded payload exceeded the logical record budget".to_string());
        }

        let dir = tempdir().unwrap();
        let wal = WALManager::new(dir.path(), SyncMode::Full).unwrap();
        let oversized_entry = WALEntry::new(
            3,
            "r2_l02".to_string(),
            30,
            WALOperationType::Insert,
            vec![0u8; MAX_RECORD_DATA + 1],
        );
        if wal.append_entry(oversized_entry).is_ok() {
            failures.push("writer accepted a record that replay must reject".to_string());
        }
        let _ = wal.close();
    }

    // A valid committed tail more than the historical 1 MiB magic-scan
    // window after a damaged header must not become successful partial replay.
    {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let wal = WALManager::new(&wal_dir, SyncMode::Full).unwrap();
        append_committed(&wal, 10, 100, vec![10]);

        let mut state = 0x9e37_79b9_u32;
        let mut incompressible = Vec::with_capacity(2 * 1024 * 1024);
        for _ in 0..incompressible.capacity() {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            incompressible.push((state >> 24) as u8);
        }
        append_committed(&wal, 20, 200, incompressible);
        append_committed(&wal, 30, 300, vec![30]);
        let path = wal_file(&wal, &wal_dir);
        wal.close().unwrap();

        let mut bytes = fs::read(&path).unwrap();
        let large_offset = record_offsets(&bytes)
            .into_iter()
            .find_map(|(offset, lsn)| (lsn == 3).then_some(offset))
            .unwrap();
        bytes[large_offset] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        if !replay_is_terminal_without_publication(&wal_dir) {
            failures.push("large-record resync returned successful partial recovery".to_string());
        }
    }

    assert!(
        failures.is_empty(),
        "R2-L02 batch A violations:\n{}",
        failures.join("\n")
    );
}

#[test]
fn test_r2_l02_batch_b_durable_transition_contract() {
    use std::sync::{mpsc, Arc, Condvar};
    use std::time::Duration;

    let _failpoint_guard = crate::test_failpoints::FailpointGuard::new();
    let mut failures = Vec::new();

    // LSN reservation, predecessor publication, buffer admission and the
    // checkpoint watermark must be one transition. The hook forces the old
    // implementation to reserve LSN 1 and pause before buffer admission.
    {
        eprintln!("R2-L02 batch B oracle: checkpoint admission");
        let dir = tempdir().unwrap();
        let wal = Arc::new(WALManager::new(dir.path(), SyncMode::None).unwrap());
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_release = Arc::clone(&release);
        let append_hook = WalAppendTestHookGuard::install(
            &wal,
            Arc::new(move |entry| {
                if entry.row_id == 111 {
                    started_tx.send(()).unwrap();
                    let (lock, condvar) = &*hook_release;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        let (next, timeout) = condvar
                            .wait_timeout(released, Duration::from_secs(5))
                            .unwrap();
                        released = next;
                        assert!(!timeout.timed_out(), "append hook release timed out");
                    }
                }
            }),
        );

        let first_wal = Arc::clone(&wal);
        let first = std::thread::spawn(move || {
            first_wal.append_entry(WALEntry::new(
                1,
                "r2_l02".to_string(),
                111,
                WALOperationType::Insert,
                vec![1],
            ))
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        let admitted_lsn = wal
            .append_entry(WALEntry::new(
                2,
                "r2_l02".to_string(),
                222,
                WALOperationType::Insert,
                vec![2],
            ))
            .unwrap();
        let checkpoint_lsn = wal.create_checkpoint().unwrap();

        {
            let (lock, condvar) = &*release;
            *lock.lock().unwrap() = true;
            condvar.notify_all();
        }
        let delayed_lsn = first.join().unwrap().unwrap();
        drop(append_hook);

        if checkpoint_lsn != admitted_lsn || checkpoint_lsn >= delayed_lsn {
            failures.push(format!(
                "checkpoint published non-admitted LSN: admitted={}, checkpoint={}, delayed={}",
                admitted_lsn, checkpoint_lsn, delayed_lsn
            ));
        }
        let _ = wal.close();
    }

    // A failed shared-buffer write must retain every previously accepted
    // record for ordered retry; another transaction's later commit cannot
    // make a lost data record look committed.
    {
        eprintln!("R2-L02 batch B oracle: write failure ownership");
        crate::test_failpoints::reset_all();
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let wal = WALManager::new(&wal_dir, SyncMode::None).unwrap();
        wal.append_entry(WALEntry::new(
            10,
            "r2_l02".to_string(),
            100,
            WALOperationType::Insert,
            vec![10],
        ))
        .unwrap();
        crate::test_failpoints::WAL_WRITE_FAIL.store(true, Ordering::Release);
        assert!(wal.flush().is_err());
        crate::test_failpoints::WAL_WRITE_FAIL.store(false, Ordering::Release);
        wal.write_commit_marker(10).unwrap();
        wal.close().unwrap();

        let wal = WALManager::new(&wal_dir, SyncMode::Full).unwrap();
        let mut recovered_rows = Vec::new();
        wal.replay_two_phase(0, |entry| {
            if !entry.is_commit_marker() {
                recovered_rows.push(entry.row_id);
            }
            Ok(())
        })
        .unwrap();
        if recovered_rows != vec![100] {
            failures.push(format!(
                "failed flush lost accepted shared-buffer rows: {:?}",
                recovered_rows
            ));
        }
        wal.close().unwrap();
        crate::test_failpoints::reset_all();
    }

    // Once the commit marker has been written, fsync failure is an explicit
    // uncertain durability outcome. It must poison further admission rather
    // than masquerade as an ordinary rollback followed by successful work.
    {
        eprintln!("R2-L02 batch B oracle: sync uncertainty");
        crate::test_failpoints::reset_all();
        let dir = tempdir().unwrap();
        let wal = WALManager::new(dir.path(), SyncMode::Full).unwrap();
        wal.append_entry(WALEntry::new(
            20,
            "r2_l02".to_string(),
            200,
            WALOperationType::Insert,
            vec![20],
        ))
        .unwrap();
        crate::test_failpoints::WAL_SYNC_FAIL.store(true, Ordering::Release);
        let outcome = wal.write_commit_marker(20);
        crate::test_failpoints::WAL_SYNC_FAIL.store(false, Ordering::Release);
        let uncertain = outcome.as_ref().err().is_some_and(|error| {
            error
                .to_string()
                .contains("durability outcome is uncertain")
        });
        let late_append = wal.append_entry(WALEntry::new(
            21,
            "r2_l02".to_string(),
            201,
            WALOperationType::Insert,
            vec![21],
        ));
        if !uncertain || wal.is_running() || late_append.is_ok() {
            failures.push("sync failure did not enter terminal uncertain state".to_string());
        }
        let _ = wal.close();
        crate::test_failpoints::reset_all();
    }

    // Normal mode advertises a durability deadline. A transaction outcome
    // therefore performs sync even when it arrives before the old interval
    // comparison would have fired and no later append follows.
    {
        eprintln!("R2-L02 batch B oracle: Normal durability");
        crate::test_failpoints::reset_all();
        let dir = tempdir().unwrap();
        let config = PersistenceConfig {
            sync_interval_ms: 60_000,
            ..Default::default()
        };
        let wal = WALManager::with_config(dir.path(), SyncMode::Normal, Some(&config)).unwrap();
        wal.append_entry(WALEntry::new(
            30,
            "r2_l02".to_string(),
            300,
            WALOperationType::Insert,
            vec![30],
        ))
        .unwrap();
        crate::test_failpoints::WAL_SYNC_FAIL.store(true, Ordering::Release);
        let outcome = wal.write_commit_marker(30);
        crate::test_failpoints::WAL_SYNC_FAIL.store(false, Ordering::Release);
        if !outcome.as_ref().err().is_some_and(|error| {
            error
                .to_string()
                .contains("durability outcome is uncertain")
        }) {
            failures.push("Normal commit had no idle durability deadline".to_string());
        }
        let _ = wal.close();
        crate::test_failpoints::reset_all();
    }

    // Close first closes admission, then drains. The hook pauses after that
    // transition; an append racing the drain must already be rejected.
    {
        eprintln!("R2-L02 batch B oracle: close admission");
        let dir = tempdir().unwrap();
        let wal = Arc::new(WALManager::new(dir.path(), SyncMode::None).unwrap());
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_release = Arc::clone(&release);
        let target_path = wal.path().to_path_buf();
        let close_hook = WalCloseTestHookGuard::install(
            &wal,
            Arc::new(move |path| {
                if path != target_path {
                    return;
                }
                started_tx.send(()).unwrap();
                let (lock, condvar) = &*hook_release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    let (next, timeout) = condvar
                        .wait_timeout(released, Duration::from_secs(5))
                        .unwrap();
                    released = next;
                    assert!(!timeout.timed_out(), "close hook release timed out");
                }
            }),
        );

        let closing_wal = Arc::clone(&wal);
        let closing = std::thread::spawn(move || closing_wal.close());
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let late_append = wal.append_entry(WALEntry::new(
            40,
            "r2_l02".to_string(),
            400,
            WALOperationType::Insert,
            vec![40],
        ));
        {
            let (lock, condvar) = &*release;
            *lock.lock().unwrap() = true;
            condvar.notify_all();
        }
        let close_result = closing.join().unwrap();
        drop(close_hook);
        if late_append.is_ok() {
            failures.push("close admitted a late append after final drain".to_string());
        }
        if let Err(error) = close_result {
            failures.push(format!(
                "close drain failed after rejecting admission: {}",
                error
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "R2-L02 batch B violations:\n{}",
        failures.join("\n")
    );
}

#[test]
fn checkpoint_retention_reports_the_actual_generation_after_size_rotations() {
    let directory = tempdir().unwrap();
    let wal_dir = directory.path().join("wal");
    let wal = WALManager::new(&wal_dir, SyncMode::Full).unwrap();

    for txn_id in 1..=2 {
        wal.append_entry(WALEntry::new(
            txn_id,
            "rotated_checkpoint".to_string(),
            txn_id,
            WALOperationType::Insert,
            vec![u8::try_from(txn_id).unwrap(); 64],
        ))
        .unwrap();
        wal.write_commit_marker(txn_id).unwrap();
        wal.current_file_position
            .store(wal.max_file_size() + 1, Ordering::Release);
        assert!(wal.maybe_rotate().unwrap());
    }

    wal.append_entry(WALEntry::new(
        3,
        "rotated_checkpoint".to_string(),
        3,
        WALOperationType::Insert,
        vec![3; 64],
    ))
    .unwrap();
    wal.write_commit_marker(3).unwrap();
    let checkpoint = wal.create_checkpoint().unwrap();
    let selected = wal.prepare_checkpoint_retention(checkpoint).unwrap();

    assert_eq!(selected.get(), wal.current_sequence());
    assert!(
        selected.get() >= 3,
        "checkpoint collapsed prior WAL rotations"
    );
    wal.close().unwrap();
}

#[test]
fn checkpoint_retirement_candidates_cover_every_existing_generation_below_floor() {
    let directory = tempdir().unwrap();
    let wal_dir = directory.path().join("wal");
    let wal = WALManager::new(&wal_dir, SyncMode::Full).unwrap();

    for txn_id in 1..=4 {
        wal.append_entry(WALEntry::new(
            txn_id,
            "retirement_set".to_string(),
            txn_id,
            WALOperationType::Insert,
            vec![u8::try_from(txn_id).unwrap(); 64],
        ))
        .unwrap();
        wal.write_commit_marker(txn_id).unwrap();
        wal.current_file_position
            .store(wal.max_file_size() + 1, Ordering::Release);
        assert!(wal.maybe_rotate().unwrap());
    }

    let retained_floor = crate::v6::WalGeneration::new(4).unwrap();
    let candidates = wal
        .checkpoint_retirement_candidates(retained_floor)
        .unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|generation| generation.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let retired_dir = wal_dir.join("retired");
    fs::create_dir(&retired_dir).unwrap();
    fs::rename(
        wal_dir.join(WALManager::canonical_filename(2)),
        retired_dir.join(WALManager::canonical_filename(2)),
    )
    .unwrap();
    assert_eq!(
        wal.checkpoint_retirement_candidates(retained_floor)
            .unwrap()
            .iter()
            .map(|generation| generation.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "a partially retired generation must remain retryable"
    );

    wal.confirm_checkpoint_publication(
        crate::v6::WalReplayFloor::new(retained_floor, wal.current_lsn.load(Ordering::Acquire)),
        &candidates,
    );
    assert!(wal
        .validated_closed_generations
        .lock()
        .unwrap()
        .iter()
        .all(|generation| generation.sequence >= retained_floor.get()));
    wal.close().unwrap();
}

#[test]
fn required_wal_suffix_rejects_only_gaps_at_or_after_the_replay_floor() {
    fn generation(name: &str, start_lsn: u64, end_lsn: u64) -> ValidatedWalGeneration {
        ValidatedWalGeneration {
            path: PathBuf::from(name),
            name: name.to_string(),
            start_lsn,
            end_lsn,
            sequence: start_lsn,
            identity: WalFileIdentity::default(),
        }
    }

    let missing_middle = vec![generation("wal-a", 0, 4), generation("wal-c", 8, 12)];
    assert!(WALManager::validate_required_generation_suffix(&missing_middle, 0).is_err());
    assert!(WALManager::validate_required_generation_suffix(&missing_middle, 8).is_ok());

    let contiguous = vec![generation("wal-a", 0, 4), generation("wal-b", 4, 8)];
    assert!(WALManager::validate_required_generation_suffix(&contiguous, 0).is_ok());
    assert!(WALManager::validate_required_generation_suffix(&contiguous, 4).is_ok());

    let retained_suffix = vec![generation("wal-c", 8, 12)];
    assert!(WALManager::validate_required_generation_suffix(&retained_suffix, 8).is_ok());
    assert!(WALManager::validate_required_generation_suffix(&retained_suffix, 0).is_err());

    let retained_after_floor = generation("wal-retained", 8, 12);
    assert!(WALManager::validate_required_generation_suffix(&[retained_after_floor], 0).is_err());
}

#[test]
fn test_wal_entry_magic_marker() {
    let mut entry = WALEntry::new(1, "test".to_string(), 1, WALOperationType::Insert, vec![]);
    entry.lsn = 1;

    let encoded = entry.encode().unwrap();

    // Check magic marker at the beginning
    let magic = u32::from_le_bytes(encoded[0..4].try_into().unwrap());
    assert_eq!(magic, WAL_ENTRY_MAGIC);
}

#[test]
fn test_wal_entry_commit_rollback() {
    let commit = WALEntry::commit(100);
    assert_eq!(commit.txn_id, 100);
    assert_eq!(commit.operation, WALOperationType::Commit);
    assert!(commit.table_id.is_none());

    let rollback = WALEntry::rollback(200);
    assert_eq!(rollback.txn_id, 200);
    assert_eq!(rollback.operation, WALOperationType::Rollback);
}

#[test]
fn test_wal_flags() {
    // Test flag operations
    let mut flags = WalFlags::NONE;
    assert_eq!(flags.as_byte(), 0);
    assert!(!flags.contains(WalFlags::COMMIT_MARKER));

    flags.set(WalFlags::COMMIT_MARKER);
    assert!(flags.contains(WalFlags::COMMIT_MARKER));
    assert!(!flags.contains(WalFlags::ABORT_MARKER));

    flags.set(WalFlags::COMPRESSED);
    assert!(flags.contains(WalFlags::COMMIT_MARKER));
    assert!(flags.contains(WalFlags::COMPRESSED));

    flags.clear(WalFlags::COMMIT_MARKER);
    assert!(!flags.contains(WalFlags::COMMIT_MARKER));
    assert!(flags.contains(WalFlags::COMPRESSED));

    // Test union
    let combined = WalFlags::COMMIT_MARKER.union(WalFlags::CHECKPOINT_MARKER);
    assert!(combined.contains(WalFlags::COMMIT_MARKER));
    assert!(combined.contains(WalFlags::CHECKPOINT_MARKER));
    assert!(!combined.contains(WalFlags::ABORT_MARKER));

    // Test from_byte
    let restored = WalFlags::from_byte(combined.as_byte());
    assert_eq!(restored, combined);
}

#[test]
fn test_commit_abort_markers() {
    // Test commit marker
    let commit_marker = WALEntry::commit_marker(42);
    assert_eq!(commit_marker.txn_id, 42);
    assert!(commit_marker.is_commit_marker());
    assert!(!commit_marker.is_abort_marker());
    assert!(commit_marker.flags.contains(WalFlags::COMMIT_MARKER));

    // Test abort marker
    let abort_marker = WALEntry::abort_marker(43);
    assert_eq!(abort_marker.txn_id, 43);
    assert!(!abort_marker.is_commit_marker());
    assert!(abort_marker.is_abort_marker());
    assert!(abort_marker.flags.contains(WalFlags::ABORT_MARKER));

    // Test regular commit (without marker flag)
    let regular_commit = WALEntry::commit(44);
    assert!(regular_commit.is_commit_marker()); // Still recognized via operation type

    // Test regular rollback (without marker flag)
    let regular_rollback = WALEntry::rollback(45);
    assert!(regular_rollback.is_abort_marker()); // Still recognized via operation type
}

#[test]
fn test_previous_lsn_chaining() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

    // Initial previous_lsn should be 0
    assert_eq!(wal.previous_lsn(), 0);

    // Add entries and verify chaining
    let entry1 = WALEntry::new(1, "test".to_string(), 1, WALOperationType::Insert, vec![1]);
    let lsn1 = wal.append_entry(entry1).unwrap();
    assert_eq!(lsn1, 1);
    assert_eq!(wal.previous_lsn(), 1);

    let entry2 = WALEntry::new(1, "test".to_string(), 2, WALOperationType::Insert, vec![2]);
    let lsn2 = wal.append_entry(entry2).unwrap();
    assert_eq!(lsn2, 2);
    assert_eq!(wal.previous_lsn(), 2);

    // Commit both transactions so they show up in two-phase replay
    wal.write_commit_marker(1).unwrap();

    // Verify entries have correct previous_lsn when replayed
    let mut entries = Vec::new();
    let mut commit_markers = Vec::new();
    wal.replay_two_phase(0, |entry| {
        if entry.is_commit_marker() {
            commit_markers.push((entry.lsn, entry.previous_lsn));
        } else {
            entries.push((entry.lsn, entry.previous_lsn));
        }
        Ok(())
    })
    .unwrap();

    // Two data entries
    assert_eq!(entries.len(), 2);
    // First entry's previous_lsn is 0 (initial)
    assert_eq!(entries[0], (1, 0));
    // Second entry's previous_lsn is 1 (links to first)
    assert_eq!(entries[1], (2, 1));
    // Commit marker is also passed to callback
    assert_eq!(commit_markers.len(), 1);

    wal.close().unwrap();
}

#[test]
fn test_wal_manager_creation() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    let wal = WALManager::new(&wal_path, SyncMode::Normal).unwrap();
    assert!(wal.is_running());
    assert_eq!(wal.current_lsn(), 0);

    wal.close().unwrap();
    assert!(!wal.is_running());
}

#[test]
fn test_wal_manager_append_entry() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

    let entry = WALEntry::new(
        1,
        "test".to_string(),
        100,
        WALOperationType::Insert,
        vec![1, 2, 3],
    );

    let lsn = wal.append_entry(entry).unwrap();
    assert_eq!(lsn, 1);

    let entry2 = WALEntry::commit(1);
    let lsn2 = wal.append_entry(entry2).unwrap();
    assert_eq!(lsn2, 2);

    wal.close().unwrap();
}

#[test]
fn ordered_batch_append_is_byte_identical_to_sequential_append() {
    fn entries() -> Vec<WALEntry> {
        (0..128)
            .map(|row_id| {
                let mut entry = WALEntry::new(
                    7,
                    "batch".to_string(),
                    row_id,
                    WALOperationType::Insert,
                    vec![row_id as u8; 256],
                );
                entry.timestamp = 123_456_789;
                entry
            })
            .collect()
    }

    let sequential_dir = tempdir().unwrap();
    let sequential = WALManager::new(sequential_dir.path(), SyncMode::None).unwrap();
    for entry in entries() {
        sequential.append_entry(entry).unwrap();
    }
    let sequential_path = sequential_dir.path().join(sequential.current_wal_file());
    sequential.close().unwrap();

    let batch_dir = tempdir().unwrap();
    let batch = WALManager::new(batch_dir.path(), SyncMode::None).unwrap();
    let runtime = crate::cpu_runtime::StorageCpuRuntime::new(4);
    let lease = runtime.acquire(128);
    let lsns = batch.append_entries(entries(), &lease).unwrap();
    assert_eq!(lsns, (1..=128).collect::<Vec<_>>());
    drop(lease);
    let batch_path = batch_dir.path().join(batch.current_wal_file());
    batch.close().unwrap();

    assert_eq!(
        fs::read(sequential_path).unwrap(),
        fs::read(batch_path).unwrap()
    );
    assert!(runtime.snapshot().peak_workers_in_use >= 1);
}

#[test]
fn batch_encode_failure_admits_no_partial_prefix() {
    let directory = tempdir().unwrap();
    let wal = WALManager::new(directory.path(), SyncMode::None).unwrap();
    let runtime = crate::cpu_runtime::StorageCpuRuntime::new(4);
    let lease = runtime.acquire(2);
    let valid = WALEntry::new(
        9,
        "valid".to_string(),
        1,
        WALOperationType::Insert,
        vec![1, 2, 3],
    );
    let invalid = WALEntry::new(9, None, 2, WALOperationType::Insert, vec![4, 5, 6]);
    assert!(wal.append_entries(vec![valid, invalid], &lease).is_err());
    assert_eq!(wal.current_lsn(), 0);
    assert_eq!(wal.current_file_size(), 0);
    wal.close().unwrap();
}

#[test]
fn test_wal_manager_replay() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Write some entries
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        for i in 1..=5 {
            let entry = WALEntry::new(
                i,
                format!("table_{}", i),
                i * 100,
                WALOperationType::Insert,
                vec![i as u8],
            );
            wal.append_entry(entry).unwrap();
            // Commit each transaction so it shows up in two-phase replay
            wal.write_commit_marker(i).unwrap();
        }

        wal.close().unwrap();
    }

    // Replay entries using two-phase recovery
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        let mut data_count = 0;
        let mut commit_count = 0;
        wal.replay_two_phase(0, |entry| {
            assert!(entry.lsn > 0);
            if entry.is_commit_marker() {
                commit_count += 1;
            } else {
                data_count += 1;
                assert!(entry.table_id.is_some());
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(data_count, 5);
        assert_eq!(commit_count, 5); // 5 commit markers for 5 transactions
    }
}

#[test]
fn test_wal_manager_checkpoint() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

    // Add some entries
    for i in 1..=3 {
        let entry = WALEntry::new(
            i,
            "test".to_string(),
            i * 10,
            WALOperationType::Insert,
            vec![],
        );
        wal.append_entry(entry).unwrap();
    }

    let checkpoint = wal.create_checkpoint().unwrap();
    assert_eq!(checkpoint, wal.current_lsn());
    assert_eq!(wal.last_checkpoint_lsn(), checkpoint);
    assert!(
        !wal_path.join("checkpoint.meta").exists(),
        "checkpoint authority belongs to the CONTROL-selected generation"
    );

    wal.close().unwrap();
}

#[test]
fn test_wal_manager_sync_modes() {
    let dir = tempdir().unwrap();

    // Test SyncMode::None
    {
        let wal_path = dir.path().join("wal_none");
        let wal = WALManager::new(&wal_path, SyncMode::None).unwrap();
        assert!(!wal.should_sync(WALOperationType::Commit));
        assert!(!wal.should_sync(WALOperationType::Rollback));
        assert!(!wal.should_sync(WALOperationType::Insert));
        wal.close().unwrap();
    }

    // Normal mode makes committed state and schema changes durable
    // immediately, while rollback remains a forced write without the
    // redundant fsync. Missing rollback bytes cannot publish a transaction.
    {
        let wal_path = dir.path().join("wal_normal");
        let wal = WALManager::new(&wal_path, SyncMode::Normal).unwrap();
        assert!(wal.should_sync(WALOperationType::Commit));
        assert!(wal.should_sync(WALOperationType::CatalogMutation));
        assert!(!wal.should_sync(WALOperationType::Rollback));
        wal.close().unwrap();
    }

    // Test SyncMode::Full
    {
        let wal_path = dir.path().join("wal_full");
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();
        assert!(wal.should_sync(WALOperationType::Commit));
        assert!(wal.should_sync(WALOperationType::Rollback));
        assert!(wal.should_sync(WALOperationType::Insert));
        wal.close().unwrap();
    }
}

#[test]
fn b6_normal_sync_deadline_is_independent_of_wall_clock() {
    use radixdb_core::time_compat::{TestWallClockGuard, UNIX_EPOCH};
    use std::time::Duration;

    let dir = tempdir().unwrap();
    let config = PersistenceConfig {
        sync_interval_ms: 20,
        ..PersistenceConfig::default()
    };
    let base = UNIX_EPOCH + Duration::from_secs(1_730_613_600);
    let clock = TestWallClockGuard::install(base);
    let wal = WALManager::with_config(
        dir.path().join("wal_monotonic"),
        SyncMode::Normal,
        Some(&config),
    )
    .unwrap();

    assert!(!wal.should_sync(WALOperationType::Insert));
    clock.set(base - Duration::from_secs(86_400));
    assert!(!wal.should_sync(WALOperationType::Insert));
    clock.set(base + Duration::from_secs(86_400 * 365));
    assert!(!wal.should_sync(WALOperationType::Insert));

    std::thread::sleep(Duration::from_millis(25));
    assert!(!wal.should_sync(WALOperationType::Rollback));
    assert!(wal.should_sync(WALOperationType::Insert));
    wal.close().unwrap();
}

#[test]
fn test_wal_manager_multiple_operations() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

    // Transaction 1
    let insert = WALEntry::new(
        1,
        "users".to_string(),
        1,
        WALOperationType::Insert,
        vec![1, 2, 3],
    );
    let lsn1 = wal.append_entry(insert).unwrap();

    let update = WALEntry::new(
        1,
        "users".to_string(),
        1,
        WALOperationType::Update,
        vec![4, 5, 6],
    );
    let lsn2 = wal.append_entry(update).unwrap();

    let commit = WALEntry::commit(1);
    let lsn3 = wal.append_entry(commit).unwrap();

    assert_eq!(lsn1, 1);
    assert_eq!(lsn2, 2);
    assert_eq!(lsn3, 3);

    // Transaction 2
    let insert2 = WALEntry::new(
        2,
        "orders".to_string(),
        100,
        WALOperationType::Insert,
        vec![],
    );
    let lsn4 = wal.append_entry(insert2).unwrap();

    let rollback = WALEntry::rollback(2);
    let lsn5 = wal.append_entry(rollback).unwrap();

    assert_eq!(lsn4, 4);
    assert_eq!(lsn5, 5);

    wal.close().unwrap();
}

#[test]
fn test_wal_catalog_operations() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    let wal = WALManager::new(&wal_path, SyncMode::Normal).unwrap();

    // Typed catalog mutations force sync in Normal mode.
    let catalog_mutation = WALEntry::new(1, None, 0, WALOperationType::CatalogMutation, vec![]);
    assert!(catalog_mutation.operation.is_ddl());

    let lsn = wal.append_entry(catalog_mutation).unwrap();
    assert_eq!(lsn, 1);

    wal.close().unwrap();
}

#[test]
fn test_find_last_lsn() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Create WAL with entries
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        for i in 1..=10 {
            let entry = WALEntry::new(
                i,
                "test".to_string(),
                i * 10,
                WALOperationType::Insert,
                vec![],
            );
            wal.append_entry(entry).unwrap();
        }

        wal.close().unwrap();
    }

    // Find WAL file and check last LSN
    let mut wal_files: Vec<_> = fs::read_dir(&wal_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("wal-") && name.ends_with(".log")
        })
        .collect();

    assert!(!wal_files.is_empty());
    wal_files.sort_by_key(|e| e.file_name());

    let last_lsn = find_last_lsn(&wal_files.last().unwrap().path()).unwrap();
    assert_eq!(last_lsn, 10);
}

#[test]
fn test_two_phase_recovery_committed() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Create WAL with committed transaction
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        // Transaction 1: Insert entries and commit
        let entry1 = WALEntry::new(
            1, // txn_id
            "test".to_string(),
            100,
            WALOperationType::Insert,
            vec![1, 2, 3],
        );
        wal.append_entry(entry1).unwrap();

        let entry2 = WALEntry::new(
            1, // same txn_id
            "test".to_string(),
            101,
            WALOperationType::Insert,
            vec![4, 5, 6],
        );
        wal.append_entry(entry2).unwrap();

        // Write commit marker for transaction 1
        wal.write_commit_marker(1).unwrap();

        wal.close().unwrap();
    }

    // Replay using two-phase recovery
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        let mut applied_entries = Vec::new();
        let mut commit_count = 0;
        let result = wal
            .replay_two_phase(0, |entry| {
                if entry.is_commit_marker() {
                    commit_count += 1;
                } else {
                    applied_entries.push(entry.row_id);
                }
                Ok(())
            })
            .unwrap();

        // Both data entries should be applied (transaction was committed)
        assert_eq!(applied_entries.len(), 2);
        assert_eq!(applied_entries, vec![100, 101]);
        // Commit marker should also be passed to callback
        assert_eq!(commit_count, 1);
        assert_eq!(result.committed_transactions, 1);
        assert_eq!(result.aborted_transactions, 0);
        assert_eq!(result.applied_entries, 2);
        assert_eq!(result.skipped_entries, 0);

        wal.close().unwrap();
    }
}

#[test]
fn test_two_phase_recovery_uncommitted() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Create WAL with uncommitted transaction (no commit marker)
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        // Transaction 1: Insert entries but DON'T commit (simulating crash)
        let entry1 = WALEntry::new(
            1, // txn_id
            "test".to_string(),
            100,
            WALOperationType::Insert,
            vec![1, 2, 3],
        );
        wal.append_entry(entry1).unwrap();

        let entry2 = WALEntry::new(
            1, // same txn_id
            "test".to_string(),
            101,
            WALOperationType::Insert,
            vec![4, 5, 6],
        );
        wal.append_entry(entry2).unwrap();

        // NO commit marker - simulating crash before commit

        wal.close().unwrap();
    }

    // Replay using two-phase recovery
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        let mut applied_entries = Vec::new();
        let result = wal
            .replay_two_phase(0, |entry| {
                applied_entries.push(entry.row_id);
                Ok(())
            })
            .unwrap();

        // No entries should be applied (transaction was in-doubt/uncommitted)
        assert_eq!(applied_entries.len(), 0);
        assert_eq!(result.committed_transactions, 0);
        assert_eq!(result.aborted_transactions, 0);
        assert_eq!(result.applied_entries, 0);
        assert_eq!(result.skipped_entries, 2); // Both entries skipped

        wal.close().unwrap();
    }
}

#[test]
fn r2_l05_a_recovery_reports_in_doubt_transaction_high_water() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");
    let durable_timestamp = crate::timestamp::get_fast_timestamp() + 1_000_000;
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();
        let mut entry = WALEntry::new(
            77,
            "test".to_string(),
            100,
            WALOperationType::Insert,
            vec![1, 2, 3],
        );
        entry.timestamp = durable_timestamp;
        wal.append_entry(entry).unwrap();
        wal.close().unwrap();
    }

    let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();
    let info = wal.replay_two_phase(0, |_| Ok(())).unwrap();
    assert_eq!(info.max_transaction_id, 77);
    assert_eq!(info.applied_entries, 0);
    assert_eq!(info.skipped_entries, 1);
    assert_eq!(wal.transaction_high_water(), 77);
    assert!(
        !wal_path.join("checkpoint.meta").exists(),
        "recovery high-water must not revive the retired checkpoint owner"
    );
    wal.close().unwrap();
}

#[test]
fn test_two_phase_recovery_aborted() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Create WAL with explicitly aborted transaction
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        // Transaction 1: Insert entries then abort
        let entry1 = WALEntry::new(
            1, // txn_id
            "test".to_string(),
            100,
            WALOperationType::Insert,
            vec![1, 2, 3],
        );
        wal.append_entry(entry1).unwrap();

        // Write abort marker for transaction 1
        wal.write_abort_marker(1).unwrap();

        wal.close().unwrap();
    }

    // Replay using two-phase recovery
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        let mut applied_entries = Vec::new();
        let result = wal
            .replay_two_phase(0, |entry| {
                applied_entries.push(entry.row_id);
                Ok(())
            })
            .unwrap();

        // No entries should be applied (transaction was aborted)
        assert_eq!(applied_entries.len(), 0);
        assert_eq!(result.committed_transactions, 0);
        assert_eq!(result.aborted_transactions, 1);
        assert_eq!(result.applied_entries, 0);
        assert_eq!(result.skipped_entries, 1); // Entry skipped

        wal.close().unwrap();
    }
}

#[test]
fn test_two_phase_recovery_mixed_transactions() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Create WAL with mixed transactions
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        // Transaction 1: Committed
        let entry1 = WALEntry::new(
            1,
            "test".to_string(),
            100,
            WALOperationType::Insert,
            vec![1],
        );
        wal.append_entry(entry1).unwrap();
        wal.write_commit_marker(1).unwrap();

        // Transaction 2: Aborted
        let entry2 = WALEntry::new(
            2,
            "test".to_string(),
            200,
            WALOperationType::Insert,
            vec![2],
        );
        wal.append_entry(entry2).unwrap();
        wal.write_abort_marker(2).unwrap();

        // Transaction 3: Uncommitted (in-doubt)
        let entry3 = WALEntry::new(
            3,
            "test".to_string(),
            300,
            WALOperationType::Insert,
            vec![3],
        );
        wal.append_entry(entry3).unwrap();

        // Transaction 4: Committed
        let entry4 = WALEntry::new(
            4,
            "test".to_string(),
            400,
            WALOperationType::Insert,
            vec![4],
        );
        wal.append_entry(entry4).unwrap();
        wal.write_commit_marker(4).unwrap();

        wal.close().unwrap();
    }

    // Replay using two-phase recovery
    {
        let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

        let mut applied_entries = Vec::new();
        let mut commit_markers = Vec::new();
        let result = wal
            .replay_two_phase(0, |entry| {
                if entry.is_commit_marker() {
                    commit_markers.push(entry.txn_id);
                } else {
                    applied_entries.push(entry.row_id);
                }
                Ok(())
            })
            .unwrap();

        // Only transactions 1 and 4 should be applied (data entries)
        assert_eq!(applied_entries.len(), 2);
        assert!(applied_entries.contains(&100)); // from txn 1
        assert!(applied_entries.contains(&400)); // from txn 4
                                                 // Commit markers for txn 1 and 4 should also be passed
        assert_eq!(commit_markers.len(), 2);
        assert!(commit_markers.contains(&1));
        assert!(commit_markers.contains(&4));
        assert_eq!(result.committed_transactions, 2);
        assert_eq!(result.aborted_transactions, 1);
        assert_eq!(result.applied_entries, 2);
        assert_eq!(result.skipped_entries, 2); // txn 2 and txn 3 entries

        wal.close().unwrap();
    }
}

#[test]
fn test_wal_rotation_basic() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Create WAL with small max size to trigger rotation
    let config = PersistenceConfig {
        wal_max_size: 500, // 500 bytes - very small to trigger rotation
        ..Default::default()
    };

    let wal = WALManager::with_config(&wal_path, SyncMode::Full, Some(&config)).unwrap();

    // Initial state
    assert_eq!(wal.current_sequence(), 1);

    // Write entries that should exceed 500 bytes
    for i in 1..=10 {
        let entry = WALEntry::new(
            i,
            "test_table".to_string(),
            i * 100,
            WALOperationType::Insert,
            vec![0u8; 100], // 100 bytes of data
        );
        wal.append_entry(entry).unwrap();
    }

    // Check if rotation would be needed
    let current_size = wal.current_file_size();
    let initial_file = wal.current_wal_file();

    // Manually trigger rotation check
    let rotated = wal.maybe_rotate().unwrap();

    if rotated {
        // Sequence should have incremented
        assert!(
            wal.current_sequence() > 0,
            "Sequence should increment after rotation"
        );

        // File position should have reset
        assert!(
            wal.current_file_size() < current_size,
            "File position should reset after rotation"
        );

        // New WAL file should have different name
        let new_file = wal.current_wal_file();
        assert_ne!(
            new_file, initial_file,
            "WAL filename should change after rotation"
        );
    }

    wal.close().unwrap();
}

#[test]
fn test_wal_rotation_preserves_data() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Create WAL with larger max size (avoid multiple rotations during writes)
    let config = PersistenceConfig {
        wal_max_size: 2000, // Large enough for initial writes
        ..Default::default()
    };

    let wal = WALManager::with_config(&wal_path, SyncMode::Full, Some(&config)).unwrap();

    // Write entries before rotation
    for i in 1..=3 {
        let entry = WALEntry::new(
            i,
            "test".to_string(),
            i * 10,
            WALOperationType::Insert,
            vec![0u8; 50],
        );
        wal.append_entry(entry).unwrap();
        wal.write_commit_marker(i).unwrap();
    }

    // Count files before rotation
    let files_before: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
        .collect();

    let initial_file = wal.current_wal_file();

    // Force rotation by temporarily modifying the position
    // (in production, this would happen naturally when file exceeds max_wal_size)
    wal.current_file_position
        .store(wal.max_file_size() + 1, Ordering::Release);
    wal.maybe_rotate().unwrap();

    // Verify rotation occurred
    let new_file = wal.current_wal_file();
    assert_ne!(
        initial_file, new_file,
        "WAL file should have changed after rotation"
    );

    // Write more entries after rotation
    for i in 4..=6 {
        let entry = WALEntry::new(
            i,
            "test".to_string(),
            i * 10,
            WALOperationType::Insert,
            vec![0u8; 50],
        );
        wal.append_entry(entry).unwrap();
        wal.write_commit_marker(i).unwrap();
    }

    // Count files after rotation (should be 2)
    let files_after: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
        .collect();

    assert!(
        files_after.len() > files_before.len(),
        "Should have more WAL files after rotation"
    );

    wal.close().unwrap();

    // Reopen and replay - should get all committed entries from BOTH files
    let wal = WALManager::with_config(&wal_path, SyncMode::Full, Some(&config)).unwrap();

    let mut row_ids = Vec::new();
    let mut commit_count = 0;
    let result = wal
        .replay_two_phase(0, |entry| {
            if entry.is_commit_marker() {
                commit_count += 1;
            } else {
                row_ids.push(entry.row_id);
            }
            Ok(())
        })
        .unwrap();

    // Should have all 6 data entries (3 from before rotation + 3 from after)
    assert_eq!(
        row_ids.len(),
        6,
        "Should have 6 entries total from both WAL files"
    );
    assert_eq!(commit_count, 6, "Should have 6 commit markers");
    assert_eq!(
        result.committed_transactions, 6,
        "Should have 6 committed transactions"
    );

    // Verify row IDs are in order (entries from all files)
    let expected: Vec<i64> = (1..=6).map(|i| i * 10).collect();
    assert_eq!(row_ids, expected);

    wal.close().unwrap();
}

#[test]
fn test_wal_no_rotation_below_threshold() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal");

    // Create WAL with large max size (default)
    let wal = WALManager::new(&wal_path, SyncMode::Full).unwrap();

    // Write a few small entries
    for i in 1..=3 {
        let entry = WALEntry::new(
            i,
            "test".to_string(),
            i * 10,
            WALOperationType::Insert,
            vec![1, 2, 3],
        );
        wal.append_entry(entry).unwrap();
    }

    // Get initial values
    let initial_sequence = wal.current_sequence();
    let initial_file = wal.current_wal_file();

    // Rotation should not occur (file size is below threshold)
    let rotated = wal.maybe_rotate().unwrap();
    assert!(!rotated, "Should not rotate below threshold");

    // Verify nothing changed
    assert_eq!(wal.current_sequence(), initial_sequence);
    assert_eq!(wal.current_wal_file(), initial_file);

    wal.close().unwrap();
}
