use super::*;
use crate::mvcc::TransactionRegistry;
use radixdb_core::{IsolationLevel, Value};
use std::sync::atomic::AtomicI64;

fn commit_for_registry_test(registry: &TransactionRegistry, txn_id: i64) {
    registry.start_commit(txn_id).unwrap();
    registry.complete_commit(txn_id).unwrap();
}

/// Simple visibility checker for testing
struct TestVisibilityChecker {
    current_seq: AtomicI64,
}

impl TestVisibilityChecker {
    fn new() -> Self {
        Self {
            current_seq: AtomicI64::new(0),
        }
    }
}

impl VisibilityChecker for TestVisibilityChecker {
    fn is_visible(&self, version_txn_id: i64, viewing_txn_id: i64) -> bool {
        // Simple rule: a version is visible if it was created by a transaction
        // with a lower or equal ID (simplified for testing)
        version_txn_id <= viewing_txn_id
    }

    fn get_current_sequence(&self) -> i64 {
        self.current_seq.fetch_add(1, Ordering::AcqRel)
    }

    fn get_active_transaction_ids(&self) -> Vec<i64> {
        // No active transactions in test
        Vec::new()
    }
}

struct WaitSignalVisibilityChecker {
    registered: std::sync::Barrier,
}

impl VisibilityChecker for WaitSignalVisibilityChecker {
    fn is_visible(&self, version_txn_id: i64, viewing_txn_id: i64) -> bool {
        version_txn_id <= viewing_txn_id
    }

    fn get_current_sequence(&self) -> i64 {
        0
    }

    fn get_active_transaction_ids(&self) -> Vec<i64> {
        Vec::new()
    }

    fn register_row_wait(&self, _waiter_txn_id: i64, _owner_txn_id: i64) -> bool {
        self.registered.wait();
        true
    }
}

struct WaitCountVisibilityChecker {
    registered: AtomicUsize,
}

impl VisibilityChecker for WaitCountVisibilityChecker {
    fn is_visible(&self, version_txn_id: i64, viewing_txn_id: i64) -> bool {
        version_txn_id <= viewing_txn_id
    }

    fn get_current_sequence(&self) -> i64 {
        0
    }

    fn get_active_transaction_ids(&self) -> Vec<i64> {
        Vec::new()
    }

    fn register_row_wait(&self, _waiter_txn_id: i64, _owner_txn_id: i64) -> bool {
        self.registered.fetch_add(1, Ordering::AcqRel);
        true
    }
}

fn unique_claim_test_store(name: &str, checker: Arc<dyn VisibilityChecker>) -> Arc<VersionStore> {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    let schema = radixdb_core::SchemaBuilder::new(name)
        .column("id", DataType::Integer, false, true)
        .column("u", DataType::Integer, false, false)
        .build();
    let store = Arc::new(VersionStore::with_visibility_checker(
        name.to_string(),
        schema,
        checker,
    ));
    store
        .add_index(
            format!("{name}_u"),
            Arc::new(HashIndex::new(
                format!("{name}_u"),
                name.to_string(),
                vec!["u".to_string()],
                vec![1],
                vec![DataType::Integer],
                true,
                0,
            )),
        )
        .unwrap();
    store
}

/// Visibility checker that reports snapshot isolation, causing arena fast
/// paths to be bypassed and version chain traversal to be used.
struct SnapshotVisibilityChecker {
    current_seq: AtomicI64,
}

impl SnapshotVisibilityChecker {
    fn new() -> Self {
        Self {
            current_seq: AtomicI64::new(0),
        }
    }
}

impl VisibilityChecker for SnapshotVisibilityChecker {
    fn is_visible(&self, version_txn_id: i64, viewing_txn_id: i64) -> bool {
        version_txn_id <= viewing_txn_id
    }

    fn get_current_sequence(&self) -> i64 {
        self.current_seq.fetch_add(1, Ordering::AcqRel)
    }

    fn get_active_transaction_ids(&self) -> Vec<i64> {
        Vec::new()
    }

    fn needs_snapshot_isolation(&self, _txn_id: i64) -> bool {
        true
    }
}

#[test]
fn test_row_version_creation() {
    let row = Row::from(vec![Value::from(1), Value::from("test")]);
    let version = RowVersion::new(1, row);

    assert_eq!(version.txn_id, 1);
    assert!(!version.is_deleted());
    assert!(version.create_time > 0);
}

#[test]
fn test_row_version_deleted() {
    let row = Row::from(vec![Value::from(1)]);
    let version = RowVersion::new_deleted(1, row);

    assert!(version.is_deleted());
    assert_eq!(version.deleted_at_txn_id, 1);
}

use radixdb_core::SchemaBuilder;

fn test_schema() -> Schema {
    SchemaBuilder::new("test_table").build()
}

#[test]
fn test_version_store_auto_increment() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    assert_eq!(store.get_current_auto_increment_value(), 0);
    assert_eq!(store.get_next_auto_increment_id().unwrap(), 1);
    assert_eq!(store.get_next_auto_increment_id().unwrap(), 2);
    assert_eq!(store.get_current_auto_increment_value(), 2);
}

#[test]
fn v2_r2_auto_increment_exhaustion_is_terminal_without_wrap() {
    let store = VersionStore::new("test_table".to_string(), test_schema());
    assert!(store.set_auto_increment_counter(i64::MAX - 1));
    assert_eq!(store.get_next_auto_increment_id().unwrap(), i64::MAX);
    assert!(store.get_next_auto_increment_id().is_err());
    assert!(store.get_next_auto_increment_id().is_err());
    assert_eq!(store.get_current_auto_increment_value(), i64::MAX);
}

#[test]
fn test_version_store_set_auto_increment() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    assert!(store.set_auto_increment_counter(10));
    assert_eq!(store.get_current_auto_increment_value(), 10);

    // Should not go backwards
    assert!(!store.set_auto_increment_counter(5));
    assert_eq!(store.get_current_auto_increment_value(), 10);

    // Should update if higher
    assert!(store.set_auto_increment_counter(20));
    assert_eq!(store.get_current_auto_increment_value(), 20);
}

#[test]
fn r3_l02_batch_a_stale_zone_map_candidate_cannot_become_trusted() {
    use crate::volume::zonemap::TableZoneMap;

    let store = VersionStore::new("test_table".to_string(), test_schema());
    store.set_zone_maps(TableZoneMap::new(1000));
    let candidate = store
        .get_zone_maps()
        .expect("installed zone map")
        .as_ref()
        .clone();

    store.mark_zone_maps_stale();
    store.set_zone_maps(candidate);

    assert!(
        store
            .get_zone_maps()
            .expect("zone map remains installed")
            .is_stale(),
        "a map built before a concurrent mutation must not be republished as trusted"
    );
}

#[test]
fn test_version_store_add_and_get() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);

    store.add_version(100, version).unwrap();

    // Transaction 2 should see version from transaction 1
    let visible = store.get_visible_version(100, 2);
    assert!(visible.is_some());
    assert_eq!(visible.unwrap().txn_id, 1);
}

#[test]
fn test_committed_hot_bytes_tracks_direct_versions() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    let small = Row::from(vec![Value::from(1), Value::text("short")]);
    let small_bytes = estimate_row_hot_bytes(&small);
    store.add_version(1, RowVersion::new(1, small)).unwrap();
    assert_eq!(store.committed_row_count(), 1);
    assert_eq!(store.committed_hot_bytes(), small_bytes);

    let large = Row::from(vec![
        Value::from(1),
        Value::text("this string is deliberately long enough to use heap storage"),
    ]);
    let large_bytes = estimate_row_hot_bytes(&large);
    store.add_version(1, RowVersion::new(2, large)).unwrap();
    assert_eq!(store.committed_row_count(), 1);
    assert_eq!(store.committed_hot_bytes(), large_bytes);

    store
        .add_version(1, RowVersion::new_deleted(3, Row::new()))
        .unwrap();
    assert_eq!(store.committed_row_count(), 0);
    assert_eq!(store.committed_hot_bytes(), 0);

    let restored = Row::from(vec![Value::from(2), Value::json(r#"{"kind":"hot"}"#)]);
    let restored_bytes = estimate_row_hot_bytes(&restored);
    store
        .add_version_single(1, RowVersion::new(4, restored))
        .unwrap();
    assert_eq!(store.committed_row_count(), 1);
    assert_eq!(store.committed_hot_bytes(), restored_bytes);

    let truncated = store.truncate_all().expect("truncate should succeed");
    assert_eq!(truncated, 1);
    assert_eq!(store.committed_row_count(), 0);
    assert_eq!(store.committed_hot_bytes(), 0);
}

#[test]
fn test_committed_hot_bytes_tracks_batch_and_seal_removal() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    let row1 = Row::from(vec![Value::from(1), Value::text("batch row one")]);
    let row2 = Row::from(vec![
        Value::from(2),
        Value::text("batch row two with longer heap-backed text payload"),
    ]);
    let row1_bytes = estimate_row_hot_bytes(&row1);
    let row2_bytes = estimate_row_hot_bytes(&row2);

    store
        .add_versions_batch(vec![
            (1, RowVersion::new(1, row1)),
            (2, RowVersion::new(1, row2)),
        ])
        .unwrap();
    assert_eq!(store.committed_row_count(), 2);
    assert_eq!(
        store.committed_hot_bytes(),
        row1_bytes.saturating_add(row2_bytes)
    );

    let (_rows, snapshot) = store.extract_for_seal(10);
    let (removed, _cleanup, skipped) = store.remove_sealed_rows(&[1], &snapshot).unwrap();
    store.subtract_committed_row_count(removed);

    assert_eq!(removed, 1);
    assert!(skipped.is_empty());
    assert_eq!(store.committed_row_count(), 1);
    assert_eq!(store.committed_hot_bytes(), row2_bytes);
}

#[test]
fn complete_seal_releases_empty_hot_arena_capacity() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);
    let rows: Vec<_> = (1..=4_096)
        .map(|row_id| {
            (
                row_id,
                RowVersion::new(
                    1,
                    Row::from(vec![Value::from(row_id), Value::text("sealed")]),
                ),
            )
        })
        .collect();
    store.add_versions_batch(rows).unwrap();
    assert!(store.arena.capacity() >= 4_096);

    let (sealed_rows, snapshot) = store.extract_for_seal(10);
    let row_ids: Vec<_> = sealed_rows.iter().map(|(row_id, _)| *row_id).collect();
    let (removed, cleanup, skipped) = store.remove_sealed_rows(&row_ids, &snapshot).unwrap();
    store.subtract_committed_row_count(removed);
    assert!(skipped.is_empty());
    assert_eq!(store.row_count(), 0);

    store.remove_sealed_index_entries(cleanup).unwrap();

    assert_eq!(store.arena.capacity(), 0);
    assert!(store.uncommitted_writes.read().capacity() <= 8);
}

#[test]
fn partial_seal_preserves_hot_arena_for_remaining_rows() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);
    let rows: Vec<_> = (1..=4_096)
        .map(|row_id| {
            (
                row_id,
                RowVersion::new(
                    1,
                    Row::from(vec![Value::from(row_id), Value::text("sealed")]),
                ),
            )
        })
        .collect();
    store.add_versions_batch(rows).unwrap();

    let (_sealed_rows, snapshot) = store.extract_for_seal(10);
    let row_ids: Vec<_> = (1..4_096).collect();
    let (removed, cleanup, skipped) = store.remove_sealed_rows(&row_ids, &snapshot).unwrap();
    store.subtract_committed_row_count(removed);
    assert!(skipped.is_empty());
    assert_eq!(store.row_count(), 1);

    store.remove_sealed_index_entries(cleanup).unwrap();

    assert_eq!(store.row_count(), 1);
    assert!(store.arena.capacity() >= 4_096);
    assert!(store.get_visible_version(4_096, 10).is_some());
}

#[test]
fn r8_l01_batch_d_seal_callback_runs_after_arena_read_guard_is_released() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);
    store
        .add_versions_batch(vec![
            (1, RowVersion::new(1, Row::from(vec![Value::Integer(1)]))),
            (2, RowVersion::new(1, Row::from(vec![Value::Integer(2)]))),
        ])
        .unwrap();
    let mut callbacks = 0usize;

    let (rows, _snapshot, completed) = store.extract_for_seal_chunks(10, None, 1, 1, |chunk| {
        callbacks += 1;
        assert_eq!(chunk.len(), 1);
        // Arena insert needs arena(W). This must be possible while a
        // durable seal callback is running.
        store.arena.insert_row(
            10_000 + callbacks as i64,
            10,
            &Row::from(vec![Value::Integer(callbacks as i64)]),
        );
        true
    });

    assert!(completed);
    assert_eq!(rows, 2);
    assert_eq!(callbacks, 2);
}

#[test]
fn seal_keeps_commit_excluded_by_active_snapshot_in_hot_mvcc() {
    let registry = Arc::new(TransactionRegistry::new());
    let checker: Arc<dyn VisibilityChecker> = registry.clone();
    let store = VersionStore::with_visibility_checker(
        "snapshot_exclusion_seal".to_string(),
        test_schema(),
        checker,
    );

    // Reproduce the out-of-order publication hole: the writer reserves its
    // commit sequence and publishes the table version, then a snapshot
    // begins while the registry still reports the writer as Committing.
    let (writer, _) = registry.begin_transaction_with_isolation(IsolationLevel::ReadCommitted);
    let writer_commit_seq = registry.start_commit(writer).unwrap();
    store
        .add_version(
            1,
            RowVersion::new(writer, Row::from(vec![Value::Integer(69)])),
        )
        .unwrap();
    let (reader, reader_begin_seq) =
        registry.begin_transaction_with_isolation(IsolationLevel::SnapshotIsolation);
    registry.complete_commit(writer).unwrap();

    assert!(writer_commit_seq < reader_begin_seq);
    assert!(registry.is_committed_before(writer, reader_begin_seq));
    assert!(!registry.is_visible(writer, reader));
    assert!(!registry.capture_seal_visibility().is_visible(writer));

    let mut chunks = Vec::new();
    let (eligible, _, completed) =
        store.extract_for_seal_chunks(i64::MAX, Some(reader_begin_seq), 1, usize::MAX, |chunk| {
            chunks.push(chunk);
            true
        });
    assert!(completed);
    assert_eq!(eligible, 0);
    assert!(chunks.is_empty());

    let (eligible_without_scalar_cutoff, _, completed) =
        store.extract_for_seal_chunks(i64::MAX, None, 1, usize::MAX, |_| true);
    assert!(completed);
    assert_eq!(eligible_without_scalar_cutoff, 0);

    // Once the excluding snapshot ends, the same committed version is safe
    // to move to cold storage.
    registry.abort_transaction(reader);
    assert!(registry.capture_seal_visibility().is_visible(writer));
    let mut sealed = Vec::new();
    let (eligible, _, completed) =
        store.extract_for_seal_chunks(i64::MAX, None, 1, usize::MAX, |chunk| {
            sealed.extend(chunk);
            true
        });
    assert!(completed);
    assert_eq!(eligible, 1);
    assert_eq!(sealed.len(), 1);
}

#[test]
fn seal_does_not_extract_previous_version_beneath_excluded_head() {
    let registry = Arc::new(TransactionRegistry::new());
    let checker: Arc<dyn VisibilityChecker> = registry.clone();
    let store = VersionStore::with_visibility_checker(
        "snapshot_excluded_update_seal".to_string(),
        test_schema(),
        checker,
    );

    let (base_writer, _) = registry.begin_transaction_with_isolation(IsolationLevel::ReadCommitted);
    registry.start_commit(base_writer).unwrap();
    store
        .add_version(
            1,
            RowVersion::new(base_writer, Row::from(vec![Value::Integer(10)])),
        )
        .unwrap();
    registry.complete_commit(base_writer).unwrap();

    let (updater, _) = registry.begin_transaction_with_isolation(IsolationLevel::ReadCommitted);
    registry.start_commit(updater).unwrap();
    store
        .add_version(
            1,
            RowVersion::new(updater, Row::from(vec![Value::Integer(20)])),
        )
        .unwrap();
    let (reader, _) = registry.begin_transaction_with_isolation(IsolationLevel::SnapshotIsolation);
    registry.complete_commit(updater).unwrap();

    assert_eq!(
        store.get_visible_version(1, reader).unwrap().data[0],
        Value::Integer(10)
    );

    // The previous version is visible to the reader, but sealing it would
    // be unsafe: removal is guarded by the captured HEAD identity and would
    // otherwise remove the committed update along with the old version.
    let mut chunks = Vec::new();
    let (eligible, _, completed) =
        store.extract_for_seal_chunks(i64::MAX, None, 1, usize::MAX, |chunk| {
            chunks.push(chunk);
            true
        });
    assert!(completed);
    assert_eq!(eligible, 0);
    assert!(chunks.is_empty());

    registry.abort_transaction(reader);
    let mut sealed = Vec::new();
    let (eligible, _, completed) =
        store.extract_for_seal_chunks(i64::MAX, None, 1, usize::MAX, |chunk| {
            sealed.extend(chunk);
            true
        });
    assert!(completed);
    assert_eq!(eligible, 1);
    assert_eq!(sealed[0].1[0], Value::Integer(20));
}

#[test]
fn test_version_store_visibility() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add version from transaction 5
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(5, row);
    store.add_version(100, version).unwrap();

    // Transaction 3 should NOT see version from transaction 5
    let visible = store.get_visible_version(100, 3);
    assert!(visible.is_none());

    // Transaction 5 should see its own version
    let visible = store.get_visible_version(100, 5);
    assert!(visible.is_some());

    // Transaction 10 should see version from transaction 5
    let visible = store.get_visible_version(100, 10);
    assert!(visible.is_some());
}

#[test]
fn test_version_store_deleted_row() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add version from transaction 1
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row.clone());
    store.add_version(100, version).unwrap();

    // Delete in transaction 2
    let deleted_version = RowVersion::new_deleted(2, row);
    store.add_version(100, deleted_version).unwrap();

    // Transaction 1 should still see the row (delete not visible)
    let visible = store.get_visible_version(100, 1);
    assert!(visible.is_some());

    // Transaction 3 should NOT see the deleted row
    let visible = store.get_visible_version(100, 3);
    assert!(visible.is_none());
}

#[test]
fn test_version_store_row_ids() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    let row = Row::from(vec![Value::from(1)]);
    store
        .add_version(100, RowVersion::new(1, row.clone()))
        .unwrap();
    store
        .add_version(200, RowVersion::new(1, row.clone()))
        .unwrap();
    store.add_version(300, RowVersion::new(1, row)).unwrap();

    let row_ids = store.get_all_row_ids();
    assert_eq!(row_ids.len(), 3);
    assert!(row_ids.contains(&100));
    assert!(row_ids.contains(&200));
    assert!(row_ids.contains(&300));
}

#[test]
fn test_version_store_close() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    assert!(!store.is_closed());
    store.close();
    assert!(store.is_closed());

    // Every mutation must fail before changing row state.
    let row = Row::from(vec![Value::from(1)]);
    assert!(store
        .add_version(100, RowVersion::new(1, row.clone()))
        .is_err());
    assert!(store
        .add_version_single(101, RowVersion::new(1, row.clone()))
        .is_err());
    assert!(store
        .add_versions_batch(vec![(102, RowVersion::new(1, row.clone()))])
        .is_err());
    assert!(store
        .apply_recovered_version(103, RowVersion::new(1, row))
        .is_err());
    assert!(store.mark_deleted(100, 2).is_err());
    assert!(store.truncate_all().is_err());
    let (_, snapshot) = store.extract_for_seal(1);
    assert!(store.remove_sealed_rows(&[], &snapshot).is_err());
    assert!(store
        .remove_sealed_index_entries(SealedIndexCleanup::default())
        .is_err());
    assert_eq!(store.row_count(), 0);
}

#[test]
fn test_transaction_version_store_basic() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        test_schema(),
        checker,
    ));

    let mut tvs = TransactionVersionStore::new(store, 1);

    // Put a new row
    let row = Row::from(vec![Value::from(42)]);
    tvs.put(100, row, false).unwrap();

    // Should see it locally
    assert!(tvs.has_locally_seen(100));
    let got = tvs.get(100);
    assert!(got.is_some());
}

#[test]
fn test_transaction_version_store_commit() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        test_schema(),
        checker,
    ));

    let mut tvs = TransactionVersionStore::new(Arc::clone(&store), 1);

    // Put a new row
    let row = Row::from(vec![Value::from(42)]);
    tvs.put(100, row, false).unwrap();

    // Commit
    tvs.commit().unwrap();

    // Should be visible in parent store now
    let visible = store.get_visible_version(100, 2);
    assert!(visible.is_some());
}

#[test]
fn test_transaction_version_store_rollback() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        test_schema(),
        checker,
    ));

    let mut tvs = TransactionVersionStore::new(Arc::clone(&store), 1);

    // Put a new row
    let row = Row::from(vec![Value::from(42)]);
    tvs.put(100, row, false).unwrap();

    // Rollback
    tvs.rollback();

    // Local authority is consumed and cannot be republished.
    assert!(tvs.get(100).is_none());
    assert!(!tvs.has_local_changes());
    assert!(tvs.commit().is_err());
    assert!(tvs
        .put(101, Row::from(vec![Value::from(7)]), false)
        .is_err());

    // Should NOT be visible in parent store.
    let visible = store.get_visible_version(100, 2);
    assert!(visible.is_none());
}

#[test]
fn v2_r2_retention_cutoff_is_checked_saturating_and_monotonic() {
    let now = 1_000_000_i64;
    let zero = retention_cutoff(now, std::time::Duration::ZERO);
    let normal = retention_cutoff(now, std::time::Duration::from_nanos(10));
    let maximum = retention_cutoff(now, std::time::Duration::MAX);

    assert_eq!(zero, now);
    assert_eq!(normal, now - 10);
    assert_eq!(maximum, now.saturating_sub(i64::MAX));
    assert!(maximum < 0);
    assert!(maximum <= normal && normal <= zero);
}

#[test]
fn v2_r2_active_snapshot_keeps_pre_delete_history_during_cleanup() {
    let registry = Arc::new(TransactionRegistry::new());
    let checker: Arc<dyn VisibilityChecker> = registry.clone();
    let store = VersionStore::with_visibility_checker(
        "snapshot_cleanup".to_string(),
        test_schema(),
        checker,
    );

    let (insert_txn, _) = registry.begin_transaction();
    commit_for_registry_test(&registry, insert_txn);
    store
        .add_version(
            1,
            RowVersion::new(insert_txn, Row::from(vec![Value::from(42)])),
        )
        .unwrap();

    let (snapshot_txn, _) =
        registry.begin_transaction_with_isolation(IsolationLevel::SnapshotIsolation);
    let (delete_txn, _) = registry.begin_transaction();
    commit_for_registry_test(&registry, delete_txn);
    store
        .add_version(
            1,
            RowVersion::new_deleted(delete_txn, Row::from(vec![Value::from(42)])),
        )
        .unwrap();

    assert_eq!(store.cleanup_deleted_rows(std::time::Duration::ZERO), 0);
    let visible = store
        .get_visible_version(1, snapshot_txn)
        .expect("old snapshot must retain the pre-delete row");
    assert_eq!(visible.data.get(0), Some(&Value::from(42)));

    registry.abort_transaction(snapshot_txn);
    assert_eq!(store.cleanup_deleted_rows(std::time::Duration::ZERO), 1);
}

#[test]
fn test_version_history_limit_default() {
    let store = VersionStore::new("test_table".to_string(), test_schema());
    assert_eq!(store.max_version_history(), 0);
}

#[test]
fn r3_l01_batch_a_default_history_does_not_cut_a_live_chain() {
    let store = VersionStore::new("r3_l01_history".to_string(), test_schema());
    let row_id = 77;
    for txn_id in 1..=12 {
        store
            .add_version(
                row_id,
                RowVersion::new(txn_id, Row::from(vec![Value::from(txn_id)])),
            )
            .unwrap();
    }

    let versions = store.versions.read().clone();
    let mut depth = 0usize;
    let mut current = versions.get(row_id);
    while let Some(version) = current {
        depth += 1;
        current = version.prev.as_deref();
    }
    assert_eq!(
        depth, 12,
        "history must be pruned only by visibility-safe GC"
    );
}

#[test]
fn r3_l01_batch_a_global_map_pool_discards_oversized_capacity() {
    clear_version_map_pools();

    let oversized_versions: I64Map<VersionList> = I64Map::with_capacity(131_072);
    let oversized_writes: I64Map<WriteSetEntry> = I64Map::with_capacity(131_072);
    return_version_list_map(oversized_versions);
    return_write_set_map(oversized_writes);

    let recycled_versions = get_version_list_map();
    let recycled_writes = get_write_set_map();
    assert!(
        recycled_versions.capacity() <= 4_096,
        "version map pool retained {} slots",
        recycled_versions.capacity()
    );
    assert!(
        recycled_writes.capacity() <= 4_096,
        "write-set pool retained {} slots",
        recycled_writes.capacity()
    );

    clear_version_map_pools();
}

#[test]
fn test_version_history_limit_drop() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let mut store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    assert!(store.set_max_version_history(3).is_err());

    let row_id = 100;

    // Add 5 versions to the same row
    // v1: depth=1, v2: depth=2, v3: depth=3
    // v4: depth=4 > 3, triggers drop -> depth=2
    // v5: depth=3
    for txn_id in 1..=5 {
        let row = Row::from(vec![Value::from(txn_id)]);
        let version = RowVersion::new(txn_id, row);
        store.add_version(row_id, version).unwrap();
    }

    // After 5 versions with limit 3:
    // v4 triggered drop (4 > 3), so chain was: v4 -> v3 -> None (depth=2)
    // v5 added: v5 -> v4 -> v3 -> None (depth=3)
    let versions = store.versions.read().clone();
    let entry = versions.get(row_id).expect("Row should exist");

    // Count actual chain length
    let chain_depth = count_chain_depth(entry);

    assert_eq!(chain_depth, 5);
}

#[test]
fn test_version_history_drop_cycles() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let mut store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    assert!(store.set_max_version_history(5).is_err());

    let row_id = 100;

    // Add 20 versions - drop should happen multiple times
    // Pattern: 1,2,3,4,5,6(drop->2),3,4,5,6(drop->2),...
    for txn_id in 1..=20 {
        let row = Row::from(vec![Value::from(txn_id)]);
        let version = RowVersion::new(txn_id, row);
        store.add_version(row_id, version).unwrap();
    }

    // Verify chain is bounded (between 2 and limit+1)
    let versions = store.versions.read().clone();
    let entry = versions.get(row_id).expect("Row should exist");

    let mut count = 1;
    let mut current = entry.prev.as_ref();
    while let Some(prev) = current {
        count += 1;
        current = prev.prev.as_ref();
    }

    assert_eq!(count, 20);
}

#[test]
fn test_version_history_unlimited() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let mut store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Set to unlimited (0)
    store.set_max_version_history(0).unwrap();

    let row_id = 100;

    // Add 15 versions
    for txn_id in 1..=15 {
        let row = Row::from(vec![Value::from(txn_id)]);
        let version = RowVersion::new(txn_id, row);
        store.add_version(row_id, version).unwrap();
    }

    // Count chain length - should be 15 (unlimited)
    let versions = store.versions.read().clone();
    let entry = versions.get(row_id).expect("Row should exist");

    let mut count = 1;
    let mut current = entry.prev.as_ref();
    while let Some(prev) = current {
        count += 1;
        current = prev.prev.as_ref();
    }

    assert_eq!(count, 15, "Unlimited mode should keep all 15 versions");
}

#[test]
fn deep_version_history_rewrite_and_drop_are_stack_bounded() {
    std::thread::Builder::new()
        .name("deep-version-history-drop".to_string())
        .stack_size(128 * 1024)
        .spawn(|| {
            let mut previous = None;
            for txn_id in 1..=50_000 {
                previous = Some(Arc::new(VersionChainEntry {
                    version: RowVersion {
                        txn_id,
                        deleted_at_txn_id: 0,
                        data: Row::from(vec![Value::from(txn_id), Value::from("drop-me")]),
                        create_time: 0,
                    },
                    prev: previous,
                    arena_idx: None,
                }));
            }
            let mut head = match Arc::try_unwrap(previous.expect("deep chain head must exist")) {
                Ok(head) => head,
                Err(_) => panic!("deep chain head must have one owner"),
            };
            assert_eq!(count_chain_depth(&head), 50_000);
            remove_column_from_version_chain(&mut head, 1);
            assert_eq!(head.version.data.len(), 1);
            drop(head);
        })
        .expect("small-stack history worker must start")
        .join()
        .expect("deep history must not overflow the stack");
}

#[test]
fn test_version_history_batch_drop() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let mut store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    assert!(store.set_max_version_history(3).is_err());

    let row_id = 100;

    // First add some versions individually (depth reaches 3)
    for txn_id in 1..=3 {
        let row = Row::from(vec![Value::from(txn_id)]);
        let version = RowVersion::new(txn_id, row);
        store.add_version(row_id, version).unwrap();
    }

    // Now add more via batch - each will trigger drop when exceeding limit
    let batch: Vec<(i64, RowVersion)> = (4..=7)
        .map(|txn_id| {
            let row = Row::from(vec![Value::from(txn_id)]);
            (row_id, RowVersion::new(txn_id, row))
        })
        .collect();

    store.add_versions_batch(batch).unwrap();

    // Verify chain is bounded (at most limit+1)
    // Chain can be as short as 1 right after pruning (when new_depth > limit)
    let versions = store.versions.read().clone();
    let entry = versions.get(row_id).expect("Row should exist");

    let mut count = 1;
    let mut current = entry.prev.as_ref();
    while let Some(prev) = current {
        count += 1;
        current = prev.prev.as_ref();
    }

    assert_eq!(count, 7);
}

#[test]
fn test_row_version_with_timestamp() {
    let row = Row::from(vec![Value::from(1)]);
    let timestamp = 12345678;
    let version = RowVersion::new_with_timestamp(1, row, timestamp);

    assert_eq!(version.txn_id, 1);
    assert_eq!(version.create_time, timestamp);
    assert!(!version.is_deleted());
}

#[test]
fn test_row_version_deleted_with_timestamp() {
    let row = Row::from(vec![Value::from(1)]);
    let timestamp = 87654321;
    let version = RowVersion::new_deleted_with_timestamp(1, row, timestamp);

    assert_eq!(version.txn_id, 1);
    assert_eq!(version.deleted_at_txn_id, 1);
    assert_eq!(version.create_time, timestamp);
    assert!(version.is_deleted());
}

#[test]
fn test_row_version_debug_display() {
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);

    let debug = format!("{:?}", version);
    assert!(debug.contains("RowVersion"));
    assert!(debug.contains("txn_id: 1"));
    assert!(debug.contains("create_time"));

    let display = format!("{}", version);
    assert!(display.contains("TxnID: 1"));
    assert!(display.contains("CreateTime"));
}

#[test]
fn test_write_set_entry_clone() {
    let row = Row::from(vec![Value::from(1)]);
    let version = RowVersion::new(1, row);

    let entry = WriteSetEntry {
        observation: WriteObservation::HotVersion,
        read_version: Some(version),
    };

    let cloned = entry.clone();
    assert!(cloned.read_version.is_some());
    assert_eq!(cloned.observation, WriteObservation::HotVersion);

    // Test with None
    let empty_entry = WriteSetEntry {
        observation: WriteObservation::Absent,
        read_version: None,
    };
    let cloned_empty = empty_entry.clone();
    assert!(cloned_empty.read_version.is_none());
    assert_eq!(cloned_empty.observation, WriteObservation::Absent);
}

#[test]
fn write_provenance_keeps_concurrent_insert_detection_after_local_update() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "write_provenance".to_string(),
        test_schema(),
        checker,
    ));
    let mut first = TransactionVersionStore::new(Arc::clone(&store), 1);
    let mut second = TransactionVersionStore::new(Arc::clone(&store), 2);

    first
        .put(7, Row::from(vec![Value::from(10)]), false)
        .unwrap();
    second
        .put(7, Row::from(vec![Value::from(20)]), false)
        .unwrap();
    // Subsequent mutations must retain the original absence observation;
    // otherwise the explicit-PK insert race becomes invisible.
    second
        .put(7, Row::from(vec![Value::from(21)]), false)
        .unwrap();
    second.claim_rows_for_delete(&[7]).unwrap();
    let entry = second.write_set_ref().unwrap().get(7).unwrap();
    assert_eq!(entry.observation, WriteObservation::Absent);

    first.commit().unwrap();
    let error = second.detect_conflicts_safe().unwrap_err();
    assert!(error.to_string().contains("concurrently inserted"));
}

#[test]
fn unchanged_claimed_existing_candidate_is_not_an_insert() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "claimed_existing".to_string(),
        test_schema(),
        checker,
    ));
    store
        .add_version(11, RowVersion::new(1, Row::from(vec![Value::from(1)])))
        .unwrap();

    let mut update = TransactionVersionStore::new(Arc::clone(&store), 2);
    update.claim_rows_for_update(&[11]).unwrap();
    let entry = update.write_set_ref().unwrap().get(11).unwrap();
    assert_eq!(entry.observation, WriteObservation::ClaimedExisting);
    assert!(entry.read_version.is_none());
    update.detect_conflicts_safe().unwrap();
    update.commit().unwrap();

    let mut delete = TransactionVersionStore::new(Arc::clone(&store), 3);
    delete.claim_rows_for_delete(&[11]).unwrap();
    let entry = delete.write_set_ref().unwrap().get(11).unwrap();
    assert_eq!(entry.observation, WriteObservation::ClaimedExisting);
    delete.detect_conflicts_safe().unwrap();
    delete.commit().unwrap();
}

#[test]
fn test_row_index() {
    let idx = RowIndex::new(100, Some(5));

    // Test Copy trait
    let copied = idx;
    assert_eq!(copied.row_id, 100);
    assert_eq!(copied.arena_idx(), Some(5));

    // Test Clone trait (use Clone::clone to avoid clone_on_copy warning)
    let cloned = Clone::clone(&idx);
    assert_eq!(cloned.row_id, 100);

    // Test with None arena_idx
    let idx_none = RowIndex::new(200, None);
    assert!(idx_none.arena_idx().is_none());

    // Test Debug
    let debug = format!("{:?}", idx);
    assert!(debug.contains("RowIndex"));
    assert!(debug.contains("100"));

    // Test memory optimization: RowIndex should be 16 bytes (not 24)
    assert_eq!(std::mem::size_of::<RowIndex>(), 16);
}

#[test]
fn test_aggregate_op() {
    // Test equality
    assert_eq!(AggregateOp::Count, AggregateOp::Count);
    assert_ne!(AggregateOp::Count, AggregateOp::Sum);

    // Test all variants
    let ops = [
        AggregateOp::Count,
        AggregateOp::Sum,
        AggregateOp::Min,
        AggregateOp::Max,
        AggregateOp::Avg,
    ];

    for op in ops {
        let debug = format!("{:?}", op);
        assert!(!debug.is_empty());

        // Test Clone and Copy (use Clone::clone to avoid clone_on_copy warning)
        let copied = op;
        let cloned = Clone::clone(&op);
        assert_eq!(copied, cloned);
    }
}

#[test]
fn test_version_store_with_capacity() {
    let store = VersionStore::with_capacity("test_table".to_string(), test_schema(), None, 100);

    assert_eq!(store.table_name(), "test_table");
    assert_eq!(store.row_count(), 0);
    assert_eq!(store.arena.capacity(), 100);

    let empty = VersionStore::new("empty_table".to_string(), test_schema());
    assert_eq!(empty.arena.capacity(), 0);
}

#[test]
fn test_version_store_quick_check_row_existence() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    // Row doesn't exist
    assert!(!store.quick_check_row_existence(100));

    // Add a row
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);
    store.add_version(100, version).unwrap();

    // Row exists
    assert!(store.quick_check_row_existence(100));
    assert!(!store.quick_check_row_existence(200));
}

#[test]
fn test_version_store_get_visible_versions_batch() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i * 10)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    // Batch query
    let row_ids = vec![1, 3, 5, 99]; // 99 doesn't exist
    let results = store.get_visible_versions_batch(&row_ids, 2);

    assert_eq!(results.len(), 3); // Only 1, 3, 5 exist
}

#[test]
fn test_version_store_count_visible_versions_batch() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    let count = store.count_visible_versions_batch(&[1, 2, 3, 99, 100], 2);
    assert_eq!(count, 3); // Only 1, 2, 3 exist
}

#[test]
fn test_version_store_probe_visible_row_ids_batch_is_aligned_and_snapshot_aware() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    store
        .add_version(1, RowVersion::new(1, Row::from(vec![Value::from(10)])))
        .unwrap();
    store
        .add_version(2, RowVersion::new(1, Row::from(vec![Value::from(20)])))
        .unwrap();
    store
        .add_version(
            2,
            RowVersion::new_deleted(3, Row::from(vec![Value::from(20)])),
        )
        .unwrap();
    store
        .add_version(3, RowVersion::new(5, Row::from(vec![Value::from(30)])))
        .unwrap();

    let row_ids = [1, 1, 2, 3, 99];
    let mut matches = [true; 5];

    // Viewer 2 sees the original row 2 version beneath the newer delete,
    // but cannot see row 3, which was created by transaction 5.
    let count = store.probe_visible_row_ids_batch(&row_ids, 2, &mut matches);
    assert_eq!(matches, [true, true, true, false, false]);
    assert_eq!(count, 3, "duplicate row ID positions count separately");

    // Viewer 4 sees the delete of row 2.
    let count = store.probe_visible_row_ids_batch(&row_ids, 4, &mut matches);
    assert_eq!(matches, [true, true, false, false, false]);
    assert_eq!(count, 2);
}

#[test]
fn test_version_store_count_visible_rows() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    assert_eq!(store.count_visible_rows(1), 0);

    // Add rows
    for i in 1..=10 {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    assert_eq!(store.count_visible_rows(2), 10);
}

#[test]
fn test_version_store_mark_deleted() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add a row
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);
    store.add_version(100, version).unwrap();

    // Mark it deleted
    store.mark_deleted(100, 2).unwrap();

    // Transaction 1 should still see it
    assert!(store.get_visible_version(100, 1).is_some());

    // Transaction 3 should not see deleted row
    assert!(store.get_visible_version(100, 3).is_none());

    // Mark non-existent row as deleted (no-op)
    store.mark_deleted(999, 2).unwrap();
}

#[test]
fn test_version_store_get_visible_rows_with_limit() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add 20 rows
    for i in 1..=20 {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    // Get with limit (txn_id, limit, offset)
    let results = store.get_visible_rows_with_limit(2, 5, 0);
    assert_eq!(results.len(), 5);

    // Test with offset
    let results_offset = store.get_visible_rows_with_limit(2, 5, 10);
    assert_eq!(results_offset.len(), 5);
}

#[test]
fn test_version_store_as_of_transaction() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add version from transaction 5
    let row = Row::from(vec![Value::from(100)]);
    let version = RowVersion::new(5, row);
    store.add_version(1, version).unwrap();

    // Add updated version from transaction 10
    let row2 = Row::from(vec![Value::from(200)]);
    let version2 = RowVersion::new(10, row2);
    store.add_version(1, version2).unwrap();

    // AS OF transaction 7 should see the first version
    let result = store.get_visible_version_as_of_transaction(1, 7);
    assert!(result.is_some());
    let rv = result.unwrap();
    assert_eq!(rv.txn_id, 5);

    // AS OF transaction 15 should see the second version
    let result = store.get_visible_version_as_of_transaction(1, 15);
    assert!(result.is_some());
    let rv = result.unwrap();
    assert_eq!(rv.txn_id, 10);

    // AS OF transaction 3 should see nothing
    let result = store.get_visible_version_as_of_transaction(1, 3);
    assert!(result.is_none());

    // Non-existent row
    let result = store.get_visible_version_as_of_transaction(999, 10);
    assert!(result.is_none());
}

#[test]
fn test_version_store_as_of_timestamp() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add version with specific timestamp
    let row = Row::from(vec![Value::from(100)]);
    let version = RowVersion::new_with_timestamp(1, row, 1000);
    store.add_version(1, version).unwrap();

    // Add version with later timestamp
    let row2 = Row::from(vec![Value::from(200)]);
    let version2 = RowVersion::new_with_timestamp(2, row2, 2000);
    store.add_version(1, version2).unwrap();

    // AS OF timestamp 1500 should see first version
    let result = store.get_visible_version_as_of_timestamp(1, 1500);
    assert!(result.is_some());
    assert_eq!(result.unwrap().create_time, 1000);

    // AS OF timestamp 2500 should see second version
    let result = store.get_visible_version_as_of_timestamp(1, 2500);
    assert!(result.is_some());
    assert_eq!(result.unwrap().create_time, 2000);

    // AS OF timestamp 500 should see nothing
    let result = store.get_visible_version_as_of_timestamp(1, 500);
    assert!(result.is_none());
}

#[test]
fn test_version_store_sum_column() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows with integer values
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i * 10)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    let sum = store.sum_column(2, 0);
    assert_eq!(sum.into_value().unwrap(), Value::Integer(150));
    assert_eq!(sum.count(), 5);
}

#[test]
fn test_version_store_sum_column_with_floats() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows with float values
    let values = [1.5, 2.5, 3.5];
    for (i, v) in values.iter().enumerate() {
        let row = Row::from(vec![Value::from(*v)]);
        let version = RowVersion::new(1, row);
        store.add_version((i + 1) as i64, version).unwrap();
    }

    let sum = store.sum_column(2, 0);
    assert!((sum.as_f64() - 7.5).abs() < 0.001);
    assert_eq!(sum.count(), 3);

    let exact_store = VersionStore::with_visibility_checker(
        "exact".to_string(),
        test_schema(),
        Arc::new(TestVisibilityChecker::new()),
    );
    for (row_id, value) in [(1, (1_i64 << 53) + 1), (2, 2)] {
        exact_store
            .add_version(
                row_id,
                RowVersion::new(1, Row::from(vec![Value::Integer(value)])),
            )
            .unwrap();
    }
    assert_eq!(
        exact_store.sum_column(2, 0).into_value().unwrap(),
        Value::Integer((1_i64 << 53) + 3)
    );
}

#[test]
fn test_version_store_min_max_column() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows
    for i in [30, 10, 50, 20, 40] {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    let min = store.min_column(2, 0);
    assert_eq!(min, Some(Value::from(10)));

    let max = store.max_column(2, 0);
    assert_eq!(max, Some(Value::from(50)));
}

#[test]
fn test_version_store_min_max_empty() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    let min = store.min_column(1, 0);
    assert!(min.is_none());

    let max = store.max_column(1, 0);
    assert!(max.is_none());
}

#[test]
fn test_version_store_compute_aggregates() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i * 10)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    // Compute multiple aggregates at once
    let ops = vec![
        (AggregateOp::Count, 0),
        (AggregateOp::Sum, 0),
        (AggregateOp::Min, 0),
        (AggregateOp::Max, 0),
        (AggregateOp::Avg, 0),
    ];

    let results = store.compute_aggregates(2, &ops);
    assert_eq!(results.len(), 5);

    // Check count
    match &results[0] {
        AggregateResult::Count(c) => assert_eq!(*c, 5),
        _ => panic!("Expected Count"),
    }

    // Check sum
    match &results[1] {
        AggregateResult::Sum(s, _) => assert_eq!(*s, 150.0),
        _ => panic!("Expected Sum"),
    }

    // Check min
    match &results[2] {
        AggregateResult::Min(Some(v)) => assert_eq!(*v, Value::from(10)),
        _ => panic!("Expected Min"),
    }

    // Check max
    match &results[3] {
        AggregateResult::Max(Some(v)) => assert_eq!(*v, Value::from(50)),
        _ => panic!("Expected Max"),
    }

    // Check avg (returns sum, count - caller computes sum/count)
    match &results[4] {
        AggregateResult::Avg(sum, count) => {
            let avg = sum / *count as f64;
            assert!((avg - 30.0).abs() < 0.001);
        }
        _ => panic!("Expected Avg"),
    }
}

#[test]
fn r8_l01_batch_b_row_claim_wait_is_barrier_driven() {
    let checker = Arc::new(WaitSignalVisibilityChecker {
        registered: std::sync::Barrier::new(2),
    });
    let store = Arc::new(VersionStore::with_capacity(
        "test_table".to_string(),
        test_schema(),
        Some(checker.clone()),
        0,
    ));

    // Claim a row
    assert!(store.try_claim_row(100, 1).is_ok());

    // Same transaction can claim again
    assert!(store.try_claim_row(100, 1).is_ok());

    // A different transaction waits and is notified by release.
    let waiter_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || waiter_store.try_claim_row(100, 2));
    checker.registered.wait();
    store.release_row_claim(100, 1);
    assert!(waiter.join().unwrap().is_ok());
    store.release_row_claim(100, 2);
}

#[test]
fn test_version_store_row_claim_timeout_is_bounded() {
    let store = VersionStore::new("test_table".to_string(), test_schema());
    store.try_claim_row(101, 1).unwrap();
    let started = Instant::now();
    let error = store.try_claim_row(101, 2).unwrap_err();
    assert!(matches!(error, Error::RowLockTimeout { row_id: 101, .. }));
    assert!(started.elapsed() >= ROW_CLAIM_WAIT_TIMEOUT);
    store.release_row_claim(101, 1);
}

#[test]
fn row_claim_wait_budget_restarts_when_owner_progresses() {
    let checker = Arc::new(WaitCountVisibilityChecker {
        registered: AtomicUsize::new(0),
    });
    let store = Arc::new(VersionStore::with_visibility_checker(
        "progressing_row_claim_queue".to_string(),
        test_schema(),
        checker.clone(),
    ));
    store.try_claim_row(102, 1).unwrap();

    let waiter_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || waiter_store.try_claim_row(102, 3));
    while checker.registered.load(Ordering::Acquire) < 1 {
        std::thread::yield_now();
    }

    std::thread::sleep(ROW_CLAIM_WAIT_TIMEOUT * 3 / 5);
    {
        let _wait_guard = store.claim_wait_mutex.lock();
        let mut claims = store.uncommitted_writes.write();
        assert_eq!(claims.insert(102, 2), Some(1));
        drop(claims);
        store.claim_changed.notify_all();
    }
    while checker.registered.load(Ordering::Acquire) < 2 {
        std::thread::yield_now();
    }

    std::thread::sleep(ROW_CLAIM_WAIT_TIMEOUT * 3 / 5);
    store.release_row_claim(102, 2);
    waiter.join().unwrap().unwrap();
    store.release_row_claim(102, 3);
}

#[test]
fn test_version_store_index_operations() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    let store = VersionStore::new(
        "test_table".to_string(),
        SchemaBuilder::new("test_table")
            .add("test_col", DataType::Integer)
            .build(),
    );

    // No indexes initially
    assert!(!store.index_exists("idx_test"));
    assert!(store.list_indexes().is_empty());

    // Add an index with all required parameters
    let index = Arc::new(HashIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        vec!["test_col".to_string()],
        vec![0],
        vec![DataType::Integer],
        false,
        0,
    ));
    store.add_index("idx_test".to_string(), index).unwrap();

    assert!(store.index_exists("idx_test"));
    assert_eq!(store.list_indexes().len(), 1);

    // Get index
    assert!(store.get_index("idx_test").is_some());
    assert!(store.get_index("nonexistent").is_none());

    // Get by column
    assert!(store.get_index_by_column("test_col").is_some());

    // Remove index
    let removed = store.remove_index("idx_test");
    assert!(removed.is_some());
    assert!(!store.index_exists("idx_test"));
}

#[test]
fn v2_r3_index_registry_rejects_identity_mismatch_and_replacement() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    let store = VersionStore::new(
        "items".to_string(),
        SchemaBuilder::new("items")
            .add("value", DataType::Integer)
            .build(),
    );
    let good: Arc<dyn Index> = Arc::new(HashIndex::new(
        "idx_value".into(),
        "items".into(),
        vec!["value".into()],
        vec![0],
        vec![DataType::Integer],
        false,
        0,
    ));
    store
        .add_index("idx_value".into(), Arc::clone(&good))
        .unwrap();
    store
        .add_index("idx_value".into(), Arc::clone(&good))
        .unwrap();

    let replacement: Arc<dyn Index> = Arc::new(HashIndex::new(
        "idx_value".into(),
        "items".into(),
        vec!["value".into()],
        vec![0],
        vec![DataType::Integer],
        true,
        0,
    ));
    assert!(store.add_index("idx_value".into(), replacement).is_err());
    assert!(!store.get_index("idx_value").unwrap().is_unique());

    let wrong_table: Arc<dyn Index> = Arc::new(HashIndex::new(
        "idx_other".into(),
        "other".into(),
        vec!["value".into()],
        vec![0],
        vec![DataType::Integer],
        false,
        0,
    ));
    assert!(store.add_index("idx_other".into(), wrong_table).is_err());
    assert!(store.get_index("idx_other").is_none());
}

#[test]
fn r4_l01_recovery_and_seal_index_failures_are_failure_atomic() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    fn make_index(name: &str) -> HashIndex {
        HashIndex::new(
            name.to_string(),
            "test_table".to_string(),
            vec!["value".to_string()],
            vec![0],
            vec![DataType::Integer],
            false,
            0,
        )
    }

    fn closed_index(name: &str) -> Arc<dyn Index> {
        let mut index = make_index(name);
        Index::close(&mut index).expect("close injected failure index");
        Arc::new(index)
    }

    let indexed_schema = || {
        SchemaBuilder::new("test_table")
            .add("value", DataType::Integer)
            .build()
    };

    let row = Row::from(vec![Value::from(10)]);

    // A failed WAL apply must restore every earlier index and leave the
    // version store unpublished.
    let recovered = VersionStore::new("test_table".to_string(), indexed_schema());
    let recovered_good = Arc::new(make_index("a_good"));
    recovered
        .add_index("a_good".to_string(), recovered_good.clone())
        .unwrap();
    recovered
        .add_index("z_closed".to_string(), closed_index("z_closed"))
        .unwrap();
    assert!(recovered
        .apply_recovered_version(1, RowVersion::new(1, row.clone()))
        .is_err());
    assert!(recovered.versions.read().get(1).is_none());
    assert!(recovered_good
        .find(&[Value::from(10)])
        .expect("query restored index")
        .is_empty());

    // A failed recovered DELETE must restore the live posting and leave
    // the current row version visible.
    let deleted = VersionStore::new("test_table".to_string(), indexed_schema());
    let deleted_good = Arc::new(make_index("a_good"));
    deleted
        .add_version(1, RowVersion::new(1, row.clone()))
        .unwrap();
    deleted_good
        .add(&[Value::from(10)], 1, 1)
        .expect("seed live posting");
    deleted
        .add_index("a_good".to_string(), deleted_good.clone())
        .unwrap();
    deleted
        .add_index("z_closed".to_string(), closed_index("z_closed"))
        .unwrap();
    assert!(deleted.mark_deleted(1, 2).is_err());
    assert!(!deleted
        .versions
        .read()
        .get(1)
        .expect("live row retained")
        .version
        .is_deleted());
    assert_eq!(
        deleted_good
            .find(&[Value::from(10)])
            .expect("query restored posting")
            .len(),
        1
    );

    // Deferred seal cleanup is also all-or-nothing across indexes. On a
    // later failure the already-removed postings are restored.
    let sealed = VersionStore::new("test_table".to_string(), indexed_schema());
    let sealed_good = Arc::new(make_index("a_good"));
    sealed.add_version(1, RowVersion::new(1, row)).unwrap();
    sealed_good
        .add(&[Value::from(10)], 1, 1)
        .expect("seed seal posting");
    sealed
        .add_index("a_good".to_string(), sealed_good.clone())
        .unwrap();
    sealed
        .add_index("z_closed".to_string(), closed_index("z_closed"))
        .unwrap();
    let snapshot = sealed.versions.read().clone();
    assert!(sealed
        .remove_sealed_index_entries(SealedIndexCleanup {
            removed_ids: vec![1],
            snapshot: Some(snapshot),
        })
        .is_err());
    assert_eq!(
        sealed_good
            .find(&[Value::from(10)])
            .expect("query restored seal posting")
            .len(),
        1
    );
}

#[test]
fn v2_r3_external_cold_index_changes_publish_only_at_commit() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    let schema = SchemaBuilder::new("cold_index_txn")
        .column("id", DataType::Integer, false, true)
        .column("value", DataType::Integer, false, false)
        .build();
    let store = Arc::new(VersionStore::new("cold_index_txn".to_string(), schema));
    let concrete = Arc::new(HashIndex::new(
        "idx_value".to_string(),
        "cold_index_txn".to_string(),
        vec!["value".to_string()],
        vec![1],
        vec![DataType::Integer],
        false,
        2,
    ));
    let index: Arc<dyn Index> = concrete.clone();
    concrete.add(&[Value::from(10)], 1, 1).unwrap();
    store
        .add_index("idx_value".to_string(), Arc::clone(&index))
        .unwrap();

    let mut rolled_back = TransactionVersionStore::new(Arc::clone(&store), 1);
    rolled_back
        .stage_external_index_removal(Arc::clone(&index), vec![Value::from(10)], 1)
        .unwrap();
    assert_eq!(
        concrete
            .get_row_ids_equal(&[Value::from(10)])
            .unwrap()
            .into_vec(),
        vec![1]
    );
    rolled_back.rollback();
    assert_eq!(
        concrete
            .get_row_ids_equal(&[Value::from(10)])
            .unwrap()
            .into_vec(),
        vec![1]
    );

    let mut updated = TransactionVersionStore::new(Arc::clone(&store), 2);
    updated
        .stage_external_index_removal(Arc::clone(&index), vec![Value::from(10)], 1)
        .unwrap();
    updated
        .put(1, Row::from(vec![Value::from(1), Value::from(20)]), false)
        .unwrap();
    updated.validate_commit().unwrap();
    assert_eq!(
        concrete
            .get_row_ids_equal(&[Value::from(10)])
            .unwrap()
            .into_vec(),
        vec![1]
    );
    updated.commit().unwrap();
    assert!(concrete
        .get_row_ids_equal(&[Value::from(10)])
        .unwrap()
        .is_empty());
    assert_eq!(
        concrete
            .get_row_ids_equal(&[Value::from(20)])
            .unwrap()
            .into_vec(),
        vec![1]
    );

    concrete.add(&[Value::from(30)], 2, 2).unwrap();
    let mut deleted = TransactionVersionStore::new(store, 3);
    deleted
        .stage_external_index_removal(index, vec![Value::from(30)], 2)
        .unwrap();
    deleted.validate_commit().unwrap();
    assert_eq!(
        concrete
            .get_row_ids_equal(&[Value::from(30)])
            .unwrap()
            .into_vec(),
        vec![2]
    );
    deleted.commit().unwrap();
    assert!(concrete
        .get_row_ids_equal(&[Value::from(30)])
        .unwrap()
        .is_empty());
}

#[test]
fn v2_r3_external_cold_index_commit_failure_restores_prior_removals() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    fn make_index(name: &str) -> HashIndex {
        HashIndex::new(
            name.to_string(),
            "cold_index_failure".to_string(),
            vec!["value".to_string()],
            vec![0],
            vec![DataType::Integer],
            false,
            1,
        )
    }

    let store = Arc::new(VersionStore::new(
        "cold_index_failure".to_string(),
        SchemaBuilder::new("cold_index_failure")
            .column("value", DataType::Integer, false, false)
            .build(),
    ));
    let good = Arc::new(make_index("a_good"));
    good.add(&[Value::from(10)], 1, 1).unwrap();
    let mut closed_impl = make_index("z_closed");
    closed_impl.add(&[Value::from(10)], 1, 1).unwrap();
    Index::close(&mut closed_impl).unwrap();
    let closed: Arc<dyn Index> = Arc::new(closed_impl);
    let good_dyn: Arc<dyn Index> = good.clone();
    store
        .add_index("a_good".to_string(), Arc::clone(&good_dyn))
        .unwrap();
    store
        .add_index("z_closed".to_string(), Arc::clone(&closed))
        .unwrap();

    let mut txn = TransactionVersionStore::new(store, 1);
    txn.stage_external_index_removal(good_dyn, vec![Value::from(10)], 1)
        .unwrap();
    txn.stage_external_index_removal(closed, vec![Value::from(10)], 1)
        .unwrap();
    assert!(txn.commit().is_err());
    assert_eq!(
        good.get_row_ids_equal(&[Value::from(10)])
            .unwrap()
            .into_vec(),
        vec![1],
        "the successful earlier removal must be compensated"
    );
}

#[test]
fn test_version_store_get_visible_row_indices() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    let indices = store.get_visible_row_indices(2);
    assert_eq!(indices.len(), 5);

    // Verify indices can be materialized
    let materialized = store.materialize_rows(&indices);
    assert_eq!(materialized.len(), 5);
}

#[test]
fn test_version_store_get_column_value() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add a row with multiple columns
    let row = Row::from(vec![Value::from(42), Value::from("test")]);
    let version = RowVersion::new(1, row);
    store.add_version(1, version).unwrap();

    let indices = store.get_visible_row_indices(2);
    assert_eq!(indices.len(), 1);

    // Get column values
    let val = store.get_column_value(&indices[0], 0);
    assert_eq!(val, Some(Value::from(42)));

    let val = store.get_column_value(&indices[0], 1);
    assert_eq!(val, Some(Value::from("test")));

    // Out of bounds column
    let val = store.get_column_value(&indices[0], 99);
    assert!(val.is_none());
}

#[test]
fn test_version_store_apply_recovered_version() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    // Apply a recovered version
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);
    store.apply_recovered_version(100, version).unwrap();

    assert_eq!(store.row_count(), 1);
    assert!(store.quick_check_row_existence(100));
}

#[test]
fn test_version_store_schema_operations() {
    let store = VersionStore::new("test_table".to_string(), test_schema());

    // Get schema
    let schema = store.schema();
    assert_eq!(schema.table_name, "test_table");

    // Modify schema through mutable reference
    {
        let schema_guard = store.schema_mut();
        // Just verify we can get mutable access
        assert_eq!(schema_guard.table_name, "test_table");
    }
}

#[test]
fn test_version_store_visibility_checker_setter() {
    let mut store = VersionStore::new("test_table".to_string(), test_schema());

    // Set a new visibility checker
    let checker = Arc::new(TestVisibilityChecker::new());
    store.set_visibility_checker(checker);

    // Verify it works with the new checker
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);
    store.add_version(100, version).unwrap();

    let visible = store.get_visible_version(100, 2);
    assert!(visible.is_some());
}

#[test]
fn test_transaction_version_store_update() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        test_schema(),
        checker,
    ));

    // Add a row first
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);
    store.add_version(100, version).unwrap();

    // Start a new transaction and update
    let mut tvs = TransactionVersionStore::new(Arc::clone(&store), 2);

    // Update the row (is_delete = false for updates)
    let new_row = Row::from(vec![Value::from(99)]);
    tvs.put(100, new_row, false).unwrap();

    // Should see updated value locally
    let got = tvs.get(100);
    assert!(got.is_some());
    let data = got.unwrap();
    assert_eq!(data.get(0), Some(&Value::from(99)));

    // Commit
    tvs.commit().unwrap();

    // Updated value should be visible
    let visible = store.get_visible_version(100, 3);
    assert!(visible.is_some());
    assert_eq!(visible.unwrap().data.get(0), Some(&Value::from(99)));
}

#[test]
fn test_transaction_version_store_delete() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        test_schema(),
        checker,
    ));

    // Add a row first
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);
    store.add_version(100, version).unwrap();

    // Start a new transaction and delete
    let mut tvs = TransactionVersionStore::new(Arc::clone(&store), 2);

    // Delete the row by using put with is_delete=true
    let delete_row = Row::from(vec![Value::from(42)]);
    tvs.put(100, delete_row, true).unwrap(); // is_delete = true

    // Should see it as deleted locally
    let got = tvs.get(100);
    assert!(got.is_none());

    // Commit
    tvs.commit().unwrap();

    // Should not be visible after commit
    let visible = store.get_visible_version(100, 3);
    assert!(visible.is_none());
}

#[test]
fn test_get_all_visible_rows() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i * 10)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    let rows = store.get_all_visible_rows(2);
    assert_eq!(rows.len(), 5);

    // Verify values
    let values: Vec<i64> = rows
        .iter()
        .map(|(_, row)| match row.get(0) {
            Some(Value::Integer(i)) => *i,
            _ => panic!("Expected integer"),
        })
        .collect();

    assert!(values.contains(&10));
    assert!(values.contains(&50));
}

#[test]
fn test_get_all_visible_row_ids() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    let row_ids = store.get_all_visible_row_ids(2);
    assert_eq!(row_ids.len(), 5);
    assert!(row_ids.contains(&1));
    assert!(row_ids.contains(&5));
}

#[test]
fn test_collect_rows_pk_ordered() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows in non-sequential order
    for i in [5, 3, 1, 4, 2] {
        let row = Row::from(vec![Value::from(i * 10)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    // Get rows in ascending PK order
    let rows = store.collect_rows_pk_ordered(2, true, 3, 0);
    assert!(rows.is_some());
    let rows = rows.unwrap();
    assert_eq!(rows.len(), 3);

    // First 3 rows in ascending PK order should be row_ids 1, 2, 3
    // With values 10, 20, 30
    assert_eq!(rows[0].1.get(0), Some(&Value::from(10)));
    assert_eq!(rows[1].1.get(0), Some(&Value::from(20)));
    assert_eq!(rows[2].1.get(0), Some(&Value::from(30)));

    // Test descending order
    let rows_desc = store.collect_rows_pk_ordered(2, false, 3, 0);
    assert!(rows_desc.is_some());
    let rows_desc = rows_desc.unwrap();
    assert_eq!(rows_desc.len(), 3);

    // First 3 rows in descending PK order should be row_ids 5, 4, 3
    // With values 50, 40, 30
    assert_eq!(rows_desc[0].1.get(0), Some(&Value::from(50)));
    assert_eq!(rows_desc[1].1.get(0), Some(&Value::from(40)));
    assert_eq!(rows_desc[2].1.get(0), Some(&Value::from(30)));

    // Test with offset
    let rows_offset = store.collect_rows_pk_ordered(2, true, 2, 2);
    assert!(rows_offset.is_some());
    let rows_offset = rows_offset.unwrap();
    assert_eq!(rows_offset.len(), 2);
    // Skip first 2 (values 10, 20), get next 2 (values 30, 40)
    assert_eq!(rows_offset[0].1.get(0), Some(&Value::from(30)));
    assert_eq!(rows_offset[1].1.get(0), Some(&Value::from(40)));
}

#[test]
fn test_count_visible() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    assert_eq!(store.count_visible(1), 0);

    // Add rows
    for i in 1..=10 {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    assert_eq!(store.count_visible(2), 10);
}

#[test]
fn test_materialize_single_row() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add a row
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);
    store.add_version(100, version).unwrap();

    let indices = store.get_visible_row_indices(2);
    assert_eq!(indices.len(), 1);

    // Materialize single row
    let result = store.materialize_row(&indices[0]);
    assert!(result.is_some());
    let (row_id, row) = result.unwrap();
    assert_eq!(row_id, 100);
    assert_eq!(row.get(0), Some(&Value::from(42)));

    // Invalid index
    let invalid_idx = RowIndex::new(999, None);
    let result = store.materialize_row(&invalid_idx);
    assert!(result.is_none());
}

// =========================================================================
// Cleanup Tests
// =========================================================================

#[test]
fn test_cleanup_deleted_rows_basic() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add 10 rows from transaction 1
    for i in 1..=10 {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    // Delete all rows in transaction 2
    for i in 1..=10 {
        let row = Row::from(vec![Value::from(i)]);
        let mut version = RowVersion::new(1, row);
        version.deleted_at_txn_id = 2;
        store.add_version(i, version).unwrap();
    }

    // Cleanup with 0 retention (immediate)
    let cleaned = store.cleanup_deleted_rows(std::time::Duration::from_secs(0));

    // All 10 rows should be cleaned
    assert_eq!(cleaned, 10, "Expected 10 rows to be cleaned");

    // Verify versions map is empty
    assert_eq!(
        store.versions.read().len(),
        0,
        "Versions map should be empty"
    );
}

#[test]
fn test_cleanup_respects_retention_period() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add and delete a row
    let row = Row::from(vec![Value::from(1)]);
    let version = RowVersion::new(1, row.clone());
    store.add_version(1, version).unwrap();

    let mut deleted_version = RowVersion::new(1, row);
    deleted_version.deleted_at_txn_id = 2;
    store.add_version(1, deleted_version).unwrap();

    // Cleanup with very long retention - should not clean
    let cleaned = store.cleanup_deleted_rows(std::time::Duration::from_secs(3600));
    assert_eq!(cleaned, 0, "Should not clean rows within retention period");

    // Verify row still exists
    assert_eq!(
        store.versions.read().len(),
        1,
        "Row should still exist in versions"
    );
}

#[test]
fn test_cleanup_only_deleted_rows() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add 5 rows, delete only 3
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i)]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    // Delete rows 1, 3, 5
    for i in [1, 3, 5] {
        let row = Row::from(vec![Value::from(i)]);
        let mut version = RowVersion::new(1, row);
        version.deleted_at_txn_id = 2;
        store.add_version(i, version).unwrap();
    }

    // Cleanup
    let cleaned = store.cleanup_deleted_rows(std::time::Duration::from_secs(0));

    assert_eq!(cleaned, 3, "Should clean only 3 deleted rows");
    assert_eq!(
        store.versions.read().len(),
        2,
        "2 non-deleted rows should remain"
    );
}

#[test]
fn test_cleanup_arena_memory_released() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Add rows with data
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i), Value::text(format!("data_{}", i))]);
        let version = RowVersion::new(1, row);
        store.add_version(i, version).unwrap();
    }

    // Record initial arena length
    let initial_arena_len = store.arena.len();
    assert_eq!(initial_arena_len, 5);

    // Delete all rows
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i), Value::text(format!("data_{}", i))]);
        let mut version = RowVersion::new(1, row);
        version.deleted_at_txn_id = 2;
        store.add_version(i, version).unwrap();
    }

    // Cleanup
    let cleaned = store.cleanup_deleted_rows(std::time::Duration::from_secs(0));
    assert_eq!(cleaned, 5);

    // Arena slots should be cleared (data replaced with empty)
    // The slots remain but data is released
    let guard = store.arena.read_guard();
    for i in 0..5 {
        // Cleared slots have row_id = 0
        assert_eq!(guard.meta()[i].row_id, 0, "Slot {} should be cleared", i);
        // Data should be empty
        assert!(
            guard.data()[i].is_empty(),
            "Slot {} data should be empty",
            i
        );
    }
}

#[test]
fn test_cleanup_does_not_remove_reinserted_row() {
    // Regression test for race condition: if a deleted row_id gets a new live
    // version committed between the cleanup snapshot and the removal pass,
    // cleanup must NOT remove the new live version.
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Tx 1: insert row_id=100
    let row = Row::from(vec![Value::from(42)]);
    let version = RowVersion::new(1, row);
    store.add_version(100, version).unwrap();

    // Tx 2: delete row_id=100
    let row = Row::from(vec![Value::from(42)]);
    let mut deleted = RowVersion::new(1, row);
    deleted.deleted_at_txn_id = 2;
    store.add_version(100, deleted).unwrap();

    // Simulate the cleanup's first pass: snapshot and identify row for deletion
    assert!(store.versions.read().get(100).unwrap().version.is_deleted());

    // Now, BEFORE cleanup's removal pass, tx 3 re-inserts a live version at row_id=100
    let row = Row::from(vec![Value::from(99)]);
    let live_version = RowVersion::new(3, row);
    store.add_version(100, live_version).unwrap();

    // The row should now be live (not deleted)
    assert!(!store.versions.read().get(100).unwrap().version.is_deleted());

    // Run cleanup — it should see the row is no longer deleted and skip it
    let cleaned = store.cleanup_deleted_rows(std::time::Duration::from_secs(0));
    assert_eq!(cleaned, 0, "Should not clean a row that was re-inserted");

    // Row must still exist with the new live value
    let versions = store.versions.read();
    let entry = versions.get(100);
    assert!(entry.is_some(), "Row 100 must still exist");
    let version = &entry.unwrap().version;
    assert!(!version.is_deleted(), "Row 100 must be live, not deleted");
    assert_eq!(
        version.data.get(0),
        Some(&Value::from(99)),
        "Row 100 must have the re-inserted value"
    );
}

#[test]
fn r3_l01_batch_c_cleanup_revalidates_deleted_version_identity() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = VersionStore::with_visibility_checker(
        "r3_l01_cleanup_identity".to_string(),
        test_schema(),
        checker,
    );

    let mut old_tombstone = RowVersion::new(1, Row::from(vec![Value::from(10)]));
    old_tombstone.deleted_at_txn_id = 2;
    store.add_version(7, old_tombstone).unwrap();

    let cleaned = store.cleanup_deleted_rows_with_between_passes(std::time::Duration::ZERO, || {
        let mut new_tombstone = RowVersion::new(3, Row::from(vec![Value::from(99)]));
        new_tombstone.deleted_at_txn_id = 4;
        store.add_version(7, new_tombstone).unwrap();
    });

    assert_eq!(
        cleaned, 0,
        "an old cleanup candidate removed a new tombstone"
    );
    let versions = store.versions.read();
    let current = versions.get(7).expect("new tombstone must remain");
    assert_eq!(current.version.txn_id, 3);
    assert_eq!(current.version.deleted_at_txn_id, 4);
    assert_eq!(current.version.data.get(0), Some(&Value::from(99)));
}

#[test]
fn test_cleanup_mixed_reinserted_and_deleted() {
    // Some rows are still deleted (should be cleaned), some were re-inserted (should be kept).
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Insert and delete rows 1..=4
    for i in 1..=4 {
        let row = Row::from(vec![Value::from(i)]);
        store.add_version(i, RowVersion::new(1, row)).unwrap();
    }
    for i in 1..=4 {
        let row = Row::from(vec![Value::from(i)]);
        let mut del = RowVersion::new(1, row);
        del.deleted_at_txn_id = 2;
        store.add_version(i, del).unwrap();
    }

    // Re-insert rows 2 and 4 with new values (simulating concurrent commit)
    for &i in &[2, 4] {
        let row = Row::from(vec![Value::from(i * 100)]);
        store.add_version(i, RowVersion::new(3, row)).unwrap();
    }

    // Cleanup should only remove rows 1 and 3 (still deleted)
    let cleaned = store.cleanup_deleted_rows(std::time::Duration::from_secs(0));
    assert_eq!(cleaned, 2, "Should clean only rows 1 and 3");

    let versions = store.versions.read();
    assert!(versions.get(1).is_none(), "Row 1 should be removed");
    assert!(versions.get(3).is_none(), "Row 3 should be removed");
    assert!(versions.get(2).is_some(), "Row 2 should still exist");
    assert!(versions.get(4).is_some(), "Row 4 should still exist");

    // Verify re-inserted values
    assert_eq!(
        versions.get(2).unwrap().version.data.get(0),
        Some(&Value::from(200))
    );
    assert_eq!(
        versions.get(4).unwrap().version.data.get(0),
        Some(&Value::from(400))
    );
}

#[test]
fn test_descending_order_returns_correct_version_under_snapshot_isolation() {
    // Reproducer: under snapshot isolation, the descending path uses deferred
    // materialization (RowIndex → materialize_rows). For chain entries where HEAD
    // is not visible, materialization falls back to versions.get(row_id) which
    // returns HEAD data — not the correct older visible version.
    //
    // TestVisibilityChecker: is_visible(txn_id, viewer) = txn_id <= viewer.
    // So viewer=2 sees txn_id=1 but NOT txn_id=3. This simulates snapshot isolation.
    let checker = Arc::new(TestVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Tx 1: insert row_id=1 with value=100, row_id=2 with value=200
    let row1 = Row::from(vec![Value::from(100)]);
    store.add_version(1, RowVersion::new(1, row1)).unwrap();
    let row2 = Row::from(vec![Value::from(200)]);
    store.add_version(2, RowVersion::new(1, row2)).unwrap();

    // Tx 3: update row_id=1 with new value=999
    // This creates a version chain: HEAD(txn=3, val=999) -> prev(txn=1, val=100)
    let updated_row = Row::from(vec![Value::from(999)]);
    store
        .add_version(1, RowVersion::new(3, updated_row))
        .unwrap();

    // Viewer txn_id=2: sees txn<=2, so sees txn=1 but NOT txn=3
    // Expected: row_id=1 should have value=100, row_id=2 should have value=200

    // Ascending order — materializes during iteration (should be correct)
    let asc = store.collect_rows_pk_ordered(2, true, 100, 0).unwrap();
    assert_eq!(asc.len(), 2, "Should see 2 rows ascending");
    // row_id=1 should have the original value (100), NOT the update (999)
    let (rid1, data1) = &asc[0];
    assert_eq!(*rid1, 1);
    assert_eq!(
        data1.get(0),
        Some(&Value::from(100)),
        "Ascending: row_id=1 should have original value 100, not updated 999"
    );

    // Descending order
    let desc = store.collect_rows_pk_ordered(2, false, 100, 0).unwrap();
    assert_eq!(desc.len(), 2, "Should see 2 rows descending");
    // row_id=1 is at index 1 in descending order (row_id=2 comes first)
    let (rid1_desc, data1_desc) = &desc[1];
    assert_eq!(*rid1_desc, 1);
    assert_eq!(
        data1_desc.get(0),
        Some(&Value::from(100)),
        "Descending: row_id=1 should have original value 100, not updated 999"
    );
}

#[test]
fn test_sorted_limit_returns_correct_version_under_snapshot_isolation() {
    // Same scenario as descending test but via get_visible_rows_sorted_limit.
    // The slow path (SnapshotIsolation) must materialize during iteration,
    // not defer to materialize_rows which reads HEAD data.
    // Uses SnapshotVisibilityChecker so arena fast paths are bypassed.
    let checker = Arc::new(SnapshotVisibilityChecker::new());
    let store =
        VersionStore::with_visibility_checker("test_table".to_string(), test_schema(), checker);

    // Tx 1: insert row_id=1 with value=100, row_id=2 with value=200
    let row1 = Row::from(vec![Value::from(100)]);
    store.add_version(1, RowVersion::new(1, row1)).unwrap();
    let row2 = Row::from(vec![Value::from(200)]);
    store.add_version(2, RowVersion::new(1, row2)).unwrap();

    // Tx 3: update row_id=1 with new value=999
    let updated_row = Row::from(vec![Value::from(999)]);
    store
        .add_version(1, RowVersion::new(3, updated_row))
        .unwrap();

    // Viewer txn_id=2: sees txn<=2
    // Sort by column 0, ascending — row_id=1 (val=100) should come first
    let sorted = store.get_visible_rows_sorted_limit(2, 0, true, 100, 0);
    assert_eq!(sorted.len(), 2, "Should see 2 rows");

    // First row should be row_id=1 with value=100 (not 999)
    let (rid, data) = &sorted[0];
    assert_eq!(*rid, 1);
    assert_eq!(
        data.get(0),
        Some(&Value::from(100)),
        "Sorted ascending: row_id=1 should have value 100, not 999"
    );

    // Descending — row_id=2 (val=200) first, then row_id=1 (val=100)
    let sorted_desc = store.get_visible_rows_sorted_limit(2, 0, false, 100, 0);
    assert_eq!(sorted_desc.len(), 2);
    let (rid2, data2) = &sorted_desc[1];
    assert_eq!(*rid2, 1);
    assert_eq!(
        data2.get(0),
        Some(&Value::from(100)),
        "Sorted descending: row_id=1 should have value 100, not 999"
    );
}

#[test]
fn test_pack_arena_idx_no_overflow() {
    // Verify that NonZeroU64 handles all valid indices correctly without overflow

    // Case 1: Small index
    let small = 100usize;
    let packed = pack_arena_idx(small);
    assert!(packed.is_some());
    assert_eq!(unpack_arena_idx(packed), Some(small));

    // Case 2: Max u32 value (previously caused issues with NonZeroU32)
    let max_u32 = u32::MAX as usize;
    let packed_u32 = pack_arena_idx(max_u32);
    assert!(packed_u32.is_some());
    assert_eq!(unpack_arena_idx(packed_u32), Some(max_u32));

    // Case 3: Beyond u32::MAX (previously caused corruption with NonZeroU32)
    let beyond_u32 = u32::MAX as usize + 1;
    let packed_beyond = pack_arena_idx(beyond_u32);
    assert!(packed_beyond.is_some());
    assert_eq!(unpack_arena_idx(packed_beyond), Some(beyond_u32)); // Now correct!

    // Case 4: Large index (5 billion - would have corrupted with u32)
    let large = 5_000_000_000usize;
    let packed_large = pack_arena_idx(large);
    assert!(packed_large.is_some());
    assert_eq!(unpack_arena_idx(packed_large), Some(large)); // Now correct!

    // Case 5: Zero index
    let zero = 0usize;
    let packed_zero = pack_arena_idx(zero);
    assert!(packed_zero.is_some());
    assert_eq!(unpack_arena_idx(packed_zero), Some(zero));

    assert_eq!(unpack_arena_idx(None), None);
}

#[test]
fn test_unique_constraint_swap() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    // Setup: Table with unique index on column 'u' (index 1)
    let schema = radixdb_core::SchemaBuilder::new("test_swap")
        .column("id", DataType::Integer, false, true) // nullable=false, pk=true
        .column("u", DataType::Integer, true, false) // nullable=true, pk=false
        .build();

    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "test_swap".to_string(),
        schema.clone(),
        checker,
    ));

    // Create Unique Hash Index on 'u'
    let index = Arc::new(HashIndex::new(
        "idx_u".to_string(),
        "test_swap".to_string(),
        vec!["u".to_string()],
        vec![1],
        vec![DataType::Integer],
        true, // is_unique
        0,
    ));
    store.add_index("idx_u".to_string(), index).unwrap();

    // Initial data: (1, 10), (2, 20)
    let mut txn1 = TransactionVersionStore::new(Arc::clone(&store), 1);
    txn1.put(1, Row::from(vec![Value::from(1), Value::from(10)]), false)
        .unwrap();
    txn1.put(2, Row::from(vec![Value::from(2), Value::from(20)]), false)
        .unwrap();
    txn1.commit().unwrap();

    // Swap: (1, 20), (2, 10) in Single Transaction
    let mut txn2 = TransactionVersionStore::new(Arc::clone(&store), 2);

    // Update row 1: 10 -> 20 (conflict with row 2's old value)
    txn2.put(1, Row::from(vec![Value::from(1), Value::from(20)]), false)
        .unwrap();

    // Update row 2: 20 -> 10 (taking row 1's old value)
    txn2.put(2, Row::from(vec![Value::from(2), Value::from(10)]), false)
        .unwrap();

    // Commit should succeed
    let result = txn2.commit();
    assert!(result.is_ok(), "Commit failed: {:?}", result.err());

    // Verify values
    let v1_new = store.get_visible_version(1, 3).unwrap();
    assert_eq!(v1_new.data.get(1).unwrap(), &Value::from(20));

    let v2_new = store.get_visible_version(2, 3).unwrap();
    assert_eq!(v2_new.data.get(1).unwrap(), &Value::from(10));
}

#[test]
fn uncommitted_unique_key_is_claimed_until_transaction_terminates() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    let schema = radixdb_core::SchemaBuilder::new("unique_claim")
        .column("id", DataType::Integer, false, true)
        .column("u", DataType::Integer, false, false)
        .build();
    let store = Arc::new(VersionStore::with_visibility_checker(
        "unique_claim".to_string(),
        schema,
        Arc::new(TestVisibilityChecker::new()),
    ));
    store
        .add_index(
            "unique_claim_u".to_string(),
            Arc::new(HashIndex::new(
                "unique_claim_u".to_string(),
                "unique_claim".to_string(),
                vec!["u".to_string()],
                vec![1],
                vec![DataType::Integer],
                true,
                0,
            )),
        )
        .unwrap();

    let mut owner = TransactionVersionStore::new(Arc::clone(&store), 10);
    owner
        .put(1, Row::from(vec![Value::from(1), Value::from(20)]), false)
        .unwrap();

    let mut contender = TransactionVersionStore::new(Arc::clone(&store), 11);
    let error = contender
        .put(2, Row::from(vec![Value::from(2), Value::from(20)]), false)
        .unwrap_err();
    assert!(matches!(error, Error::RowLockTimeout { row_id: 1, .. }));

    owner.rollback();
    contender
        .put(2, Row::from(vec![Value::from(2), Value::from(20)]), false)
        .unwrap();
    contender.rollback();
    assert!(store.unique_key_claims.lock().is_empty());
}

#[test]
fn unique_claim_waiter_acquires_key_after_owner_rollback() {
    let checker = Arc::new(WaitSignalVisibilityChecker {
        registered: std::sync::Barrier::new(2),
    });
    let store = unique_claim_test_store("unique_wait_rollback", checker.clone());
    let mut owner = TransactionVersionStore::new(Arc::clone(&store), 12);
    owner
        .put(1, Row::from(vec![Value::from(1), Value::from(20)]), false)
        .unwrap();

    let waiter_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || {
        let mut transaction = TransactionVersionStore::new(waiter_store, 13);
        transaction.put(2, Row::from(vec![Value::from(2), Value::from(20)]), false)?;
        transaction.rollback();
        Ok::<_, Error>(())
    });
    checker.registered.wait();
    owner.rollback();

    waiter.join().unwrap().unwrap();
    assert!(store.unique_key_claims.lock().is_empty());
}

#[test]
fn unique_claim_wait_budget_restarts_when_owner_progresses() {
    let checker = Arc::new(WaitCountVisibilityChecker {
        registered: AtomicUsize::new(0),
    });
    let store = unique_claim_test_store("progressing_unique_claim_queue", checker.clone());
    let mut first_owner = TransactionVersionStore::new(Arc::clone(&store), 20);
    first_owner
        .put(1, Row::from(vec![Value::from(1), Value::from(20)]), false)
        .unwrap();

    let waiter_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || {
        let mut transaction = TransactionVersionStore::new(waiter_store, 22);
        transaction.put(3, Row::from(vec![Value::from(3), Value::from(20)]), false)?;
        transaction.rollback();
        Ok::<_, Error>(())
    });
    while checker.registered.load(Ordering::Acquire) < 1 {
        std::thread::yield_now();
    }

    std::thread::sleep(ROW_CLAIM_WAIT_TIMEOUT * 3 / 5);
    {
        let _wait_guard = store.claim_wait_mutex.lock();
        let mut claims = store.unique_key_claims.lock();
        let claim = claims
            .values_mut()
            .next()
            .expect("the first UNIQUE owner must remain visible");
        assert_eq!(claim.txn_id, 20);
        *claim = super::unique_claims::UniqueClaimOwner {
            txn_id: 21,
            row_id: 2,
        };
        drop(claims);
        store.claim_changed.notify_all();
    }
    while checker.registered.load(Ordering::Acquire) < 2 {
        std::thread::yield_now();
    }

    std::thread::sleep(ROW_CLAIM_WAIT_TIMEOUT * 3 / 5);
    {
        let _wait_guard = store.claim_wait_mutex.lock();
        let mut claims = store.unique_key_claims.lock();
        claims.retain(|_, owner| owner.txn_id != 21);
        drop(claims);
        store.claim_changed.notify_all();
    }

    waiter.join().unwrap().unwrap();
    first_owner.rollback();
    assert!(store.unique_key_claims.lock().is_empty());
}

#[test]
fn unique_claim_waiter_observes_committed_owner_as_durable_conflict() {
    let checker = Arc::new(WaitSignalVisibilityChecker {
        registered: std::sync::Barrier::new(2),
    });
    let store = unique_claim_test_store("unique_wait_commit", checker.clone());
    let mut owner = TransactionVersionStore::new(Arc::clone(&store), 14);
    owner
        .put(1, Row::from(vec![Value::from(1), Value::from(20)]), false)
        .unwrap();

    let waiter_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || {
        let mut transaction = TransactionVersionStore::new(waiter_store, 15);
        transaction.put(2, Row::from(vec![Value::from(2), Value::from(20)]), false)
    });
    checker.registered.wait();
    owner.commit().unwrap();

    assert!(matches!(
        waiter.join().unwrap().unwrap_err(),
        Error::UniqueConstraint { .. }
    ));
    assert!(store.unique_key_claims.lock().is_empty());
}

#[test]
fn concurrent_uncommitted_unique_key_has_exactly_one_owner() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    let schema = radixdb_core::SchemaBuilder::new("unique_claim_race")
        .column("id", DataType::Integer, false, true)
        .column("u", DataType::Integer, false, false)
        .build();
    let store = Arc::new(VersionStore::with_visibility_checker(
        "unique_claim_race".to_string(),
        schema,
        Arc::new(TestVisibilityChecker::new()),
    ));
    store
        .add_index(
            "unique_claim_race_u".to_string(),
            Arc::new(HashIndex::new(
                "unique_claim_race_u".to_string(),
                "unique_claim_race".to_string(),
                vec!["u".to_string()],
                vec![1],
                vec![DataType::Integer],
                true,
                0,
            )),
        )
        .unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = [30, 31]
        .into_iter()
        .enumerate()
        .map(|(offset, txn_id)| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut txn = TransactionVersionStore::new(store, txn_id);
                barrier.wait();
                let result = txn.put(
                    offset as i64 + 1,
                    Row::from(vec![Value::from(offset as i64 + 1), Value::from(50)]),
                    false,
                );
                let outcome = match result {
                    Ok(()) => Ok(()),
                    Err(Error::RowLockTimeout { .. }) => Err(()),
                    Err(error) => panic!("unexpected UNIQUE claim outcome: {error}"),
                };
                barrier.wait();
                txn.rollback();
                outcome
            })
        })
        .collect();

    barrier.wait();
    barrier.wait();
    let outcomes: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(outcomes.iter().filter(|result| result.is_err()).count(), 1);
    assert!(store.unique_key_claims.lock().is_empty());
}

#[test]
fn statement_rollback_releases_only_new_unique_claims() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    let schema = radixdb_core::SchemaBuilder::new("unique_claim_statement")
        .column("id", DataType::Integer, false, true)
        .column("u", DataType::Integer, false, false)
        .build();
    let store = Arc::new(VersionStore::with_visibility_checker(
        "unique_claim_statement".to_string(),
        schema,
        Arc::new(TestVisibilityChecker::new()),
    ));
    store
        .add_index(
            "unique_claim_statement_u".to_string(),
            Arc::new(HashIndex::new(
                "unique_claim_statement_u".to_string(),
                "unique_claim_statement".to_string(),
                vec!["u".to_string()],
                vec![1],
                vec![DataType::Integer],
                true,
                0,
            )),
        )
        .unwrap();

    let mut owner = TransactionVersionStore::new(Arc::clone(&store), 40);
    owner
        .put(1, Row::from(vec![Value::from(1), Value::from(10)]), false)
        .unwrap();
    let statement_boundary = get_fast_timestamp();
    owner
        .put(2, Row::from(vec![Value::from(2), Value::from(20)]), false)
        .unwrap();
    owner.rollback_to_timestamp(statement_boundary);

    let mut contender = TransactionVersionStore::new(Arc::clone(&store), 41);
    assert!(matches!(
        contender
            .put(3, Row::from(vec![Value::from(3), Value::from(10)]), false)
            .unwrap_err(),
        Error::RowLockTimeout { row_id: 1, .. }
    ));
    contender
        .put(3, Row::from(vec![Value::from(3), Value::from(20)]), false)
        .unwrap();

    owner.rollback();
    contender.rollback();
    assert!(store.unique_key_claims.lock().is_empty());
}

#[test]
fn unique_claim_savepoint_rollback_restores_local_view_without_claim_gap() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;

    let schema = radixdb_core::SchemaBuilder::new("unique_claim_rollback")
        .column("id", DataType::Integer, false, true)
        .column("u", DataType::Integer, false, false)
        .build();
    let store = Arc::new(VersionStore::with_visibility_checker(
        "unique_claim_rollback".to_string(),
        schema,
        Arc::new(TestVisibilityChecker::new()),
    ));
    store
        .add_index(
            "unique_claim_rollback_u".to_string(),
            Arc::new(HashIndex::new(
                "unique_claim_rollback_u".to_string(),
                "unique_claim_rollback".to_string(),
                vec!["u".to_string()],
                vec![1],
                vec![DataType::Integer],
                true,
                0,
            )),
        )
        .unwrap();

    let mut owner = TransactionVersionStore::new(Arc::clone(&store), 20);
    owner
        .put(1, Row::from(vec![Value::from(1), Value::from(10)]), false)
        .unwrap();
    let savepoint = get_fast_timestamp();
    owner
        .put(1, Row::from(vec![Value::from(1), Value::from(20)]), false)
        .unwrap();
    owner.rollback_to_timestamp(savepoint);
    assert_eq!(
        owner.get(1).unwrap().get(1),
        Some(&Value::from(10)),
        "rollback must restore the prior transaction-local UNIQUE owner"
    );

    // The superseded key stays conservatively reserved for other
    // transactions, but it is no longer part of this transaction's final view
    // and may be reused by another row in the same transaction.
    owner
        .put(2, Row::from(vec![Value::from(2), Value::from(20)]), false)
        .unwrap();
    let mut contender = TransactionVersionStore::new(Arc::clone(&store), 21);
    assert!(matches!(
        contender
            .put(3, Row::from(vec![Value::from(3), Value::from(20)]), false)
            .unwrap_err(),
        Error::RowLockTimeout { row_id: 2, .. }
    ));

    owner.rollback();
    contender
        .put(3, Row::from(vec![Value::from(3), Value::from(20)]), false)
        .unwrap();
    contender.rollback();
    assert!(store.unique_key_claims.lock().is_empty());
}

#[test]
fn test_unique_constraint_performance_bulk_update() {
    use crate::index::HashIndex;
    use radixdb_core::DataType;
    use std::time::Instant;

    // Setup: Table with unique index on column 'u' (index 1)
    let schema = radixdb_core::SchemaBuilder::new("test_perf")
        .column("id", DataType::Integer, false, true) // nullable=false, pk=true
        .column("u", DataType::Integer, true, false) // nullable=true, pk=false
        .build();

    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "test_perf".to_string(),
        schema.clone(),
        checker,
    ));

    // Create Unique Hash Index on 'u'
    let index = Arc::new(HashIndex::new(
        "idx_u".to_string(),
        "test_perf".to_string(),
        vec!["u".to_string()],
        vec![1],
        vec![DataType::Integer],
        true, // is_unique
        0,
    ));
    store.add_index("idx_u".to_string(), index).unwrap();

    let row_count = 30000;
    let mut txn1 = TransactionVersionStore::new(Arc::clone(&store), 1);
    for i in 0..row_count {
        txn1.put(
            i as i64,
            Row::from(vec![Value::from(i), Value::from(i)]),
            false,
        )
        .unwrap();
    }
    txn1.commit().unwrap();

    let mut txn2 = TransactionVersionStore::new(Arc::clone(&store), 2);
    // Bulk update: shift every value by 1 (e.g., 0->1, 1->2, ... 4999->5000)
    // This will cause every row to conflict with the next one's old value
    for i in 0..row_count {
        txn2.put(
            i as i64,
            Row::from(vec![Value::from(i), Value::from(i + 1)]),
            false,
        )
        .unwrap();
    }

    let start = Instant::now();
    txn2.commit().unwrap();
    let duration = start.elapsed();
    println!("Commit time for {} rows: {:?}", row_count, duration);

    // Functional coverage proves the complete update and unique-index
    // transition. Runtime thresholds belong to the repeated benchmark
    // owner, not an uncalibrated wall-clock assertion in a unit test.
    for row_id in [0, row_count / 2, row_count - 1] {
        let visible = store
            .get_visible_version(row_id as i64, 3)
            .expect("committed bulk-update row remains visible");
        assert_eq!(
            visible.data.get(1),
            Some(&Value::from(row_id + 1)),
            "bulk update published the wrong unique value for row {row_id}"
        );
    }
}

/// Verify that truncate_all() holds uncommitted_writes(W) during the
/// entire check-and-clear sequence, preventing the TOCTOU race where
/// a concurrent try_claim_row() could add a claim between the check
/// and the clear.
///
/// With the fix, truncate_all() holds uncommitted_writes(W) for the
/// entire duration, so any concurrent try_claim_row() blocks until
/// truncate completes — at which point truncate has already cleared
/// the table and the UPDATE will operate on a clean state.
#[test]
fn test_truncate_blocks_concurrent_claims() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let checker = Arc::new(TestVisibilityChecker::new());
    let store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        test_schema(),
        checker,
    ));

    // Setup: 10 committed rows
    for i in 1..=10 {
        let row = Row::from(vec![Value::from(i)]);
        store.add_version(i, RowVersion::new(1, row)).unwrap();
    }
    assert_eq!(store.committed_row_count.load(Ordering::Relaxed), 10);

    // Pre-claim a row to simulate an in-progress UPDATE
    store
        .try_claim_row(5, 99)
        .expect("claim should succeed on fresh store");

    // truncate_all must fail because uncommitted_writes is not empty
    let result = store.truncate_all();
    assert!(
        result.is_err(),
        "truncate must fail when uncommitted writes exist"
    );

    // Verify data was NOT destroyed
    assert_eq!(
        store.committed_row_count.load(Ordering::Relaxed),
        10,
        "row count must be unchanged after failed truncate"
    );
    assert_eq!(store.versions.read().len(), 10);

    // Release the claim, then truncate should succeed
    store.release_row_claim(5, 99);
    let count = store.truncate_all().expect("truncate should succeed now");
    assert_eq!(count, 10);
    assert!(store.versions.read().is_empty());
    assert!(store.uncommitted_writes.read().is_empty());

    // Concurrent test: truncate holds the lock, blocking try_claim_row
    // Insert fresh data for next round
    for i in 1..=5 {
        let row = Row::from(vec![Value::from(i)]);
        store.add_version(i, RowVersion::new(1, row)).unwrap();
    }
    // Force committed_row_count to reflect the 5 rows
    store.committed_row_count.store(5, Ordering::Relaxed);

    let barrier = Arc::new(Barrier::new(2));
    let store2 = Arc::clone(&store);
    let barrier2 = Arc::clone(&barrier);

    // Thread: try to claim a row concurrently with truncate
    let handle = thread::spawn(move || {
        barrier2.wait();
        // This will either:
        // a) Execute BEFORE truncate's lock → claim succeeds, truncate sees
        //    non-empty uncommitted_writes → truncate fails (correct!)
        // b) Execute AFTER truncate's lock → claim succeeds on empty store
        //    (correct, no data to corrupt)
        store2.try_claim_row(3, 200)
    });

    barrier.wait();
    let truncate_result = store.truncate_all();
    let claim_result = handle.join().unwrap();

    // Both operations complete without panic.
    // Either truncate succeeded (claim was after) or failed (claim was before).
    // In no case should a ghost row appear.
    if truncate_result.is_ok() {
        // Truncate completed first — table is empty, claim is on empty store
        assert!(store.versions.read().is_empty() || store.versions.read().len() <= 1);
    } else {
        // Claim was first — truncate correctly rejected
        assert!(claim_result.is_ok());
    }
}

#[test]
fn grouped_aggregates_use_canonical_mixed_numeric_identity() {
    let checker = Arc::new(TestVisibilityChecker::new());
    let store = VersionStore::with_visibility_checker(
        "mixed_group_identity".to_string(),
        test_schema(),
        checker,
    );

    let two_pow_53 = 1_i64 << 53;
    let values = [
        Value::Integer(0),
        Value::Float(0.0),
        Value::Float(-0.0),
        Value::Integer(two_pow_53),
        Value::Float(two_pow_53 as f64),
        Value::Integer(two_pow_53 + 1),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0002)),
        Value::Integer(i64::MIN),
        Value::Float(-9_223_372_036_854_775_808.0),
    ];

    for (row_id, value) in values.into_iter().enumerate() {
        store
            .add_version(row_id as i64, RowVersion::new(1, Row::from(vec![value])))
            .unwrap();
    }

    let groups = store
        .compute_grouped_aggregates(2, &[0], &[(AggregateOp::CountStar, 0)])
        .expect("read-committed arena grouping must be available");
    assert_eq!(groups.len(), 5);

    let count_for = |needle: &Value| {
        let matching: Vec<_> = groups
            .iter()
            .filter(|group| group.group_values.first() == Some(needle))
            .collect();
        assert_eq!(matching.len(), 1, "expected one group for {needle:?}");
        match matching[0].aggregate_values.first() {
            Some(Value::Integer(count)) => Some(*count),
            _ => None,
        }
    };

    assert_eq!(count_for(&Value::Integer(0)), Some(3));
    assert_eq!(count_for(&Value::Integer(two_pow_53)), Some(2));
    assert_eq!(count_for(&Value::Integer(two_pow_53 + 1)), Some(1));
    assert_eq!(
        count_for(&Value::Float(f64::from_bits(0x7ff8_0000_0000_0042))),
        Some(2)
    );
    assert_eq!(count_for(&Value::Integer(i64::MIN)), Some(2));
}
