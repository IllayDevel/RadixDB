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

//! MVCC Transaction implementation
//!
//! Provides transaction semantics with two-phase commit protocol.
//!

use rustc_hash::FxHashMap;
use std::sync::Arc;

use radixdb_catalog::CatalogMutationSet;
use radixdb_core::{Error, IsolationLevel, Result, Schema, SchemaColumn};

use crate::mvcc::TransactionRegistry;
use crate::timestamp::get_fast_timestamp;
use crate::traits::{
    PendingIndexDefinition, PendingIndexDrop, PendingIndexRename, PendingSchemaChange,
    PendingTableRename, QueryResult, SchemaPhysicalTransition, Table, Transaction,
};
use crate::Expression;

/// DDL state captured at savepoint creation time.
/// Used to rollback CREATE/DROP TABLE operations when rolling back to a savepoint.
#[derive(Debug, Clone)]
struct SavepointDdlState {
    /// Number of created_tables entries at savepoint time
    created_tables_len: usize,
    /// Number of dropped_tables entries at savepoint time
    dropped_tables_len: usize,
    /// Number of staged CREATE INDEX definitions at savepoint time.
    pending_indexes_len: usize,
    /// Number of staged DROP INDEX definitions at savepoint time.
    pending_index_drops_len: usize,
    /// Number of staged ALTER INDEX RENAME definitions at savepoint time.
    pending_index_renames_len: usize,
    /// Number of staged ALTER TABLE RENAME definitions at savepoint time.
    pending_table_renames_len: usize,
    /// Number of staged ALTER TABLE schema transitions at savepoint time.
    pending_schema_changes_len: usize,
    /// Coalesced catalog mutation, if a low-level caller attached it before
    /// creating the savepoint.
    catalog_mutation: Option<CatalogMutationSet>,
}

/// State captured when a savepoint is created.
#[derive(Debug, Clone)]
struct SavepointState {
    /// Timestamp for rolling back DML changes
    timestamp: i64,
    /// DDL state for rolling back CREATE/DROP TABLE operations
    ddl_state: SavepointDdlState,
}

/// MVCC Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and can perform operations
    Active,
    /// Transaction is being committed (two-phase commit)
    Committing,
    /// Transaction has been committed
    Committed,
    /// Transaction has been rolled back
    RolledBack,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PublishedDdlOwners {
    tables: u64,
    indexes: u64,
    index_drops: u64,
    schema_changes: u64,
    constraint_changes: u64,
}

/// Named transaction-private inputs for the fallible DDL preparation phase.
pub struct TransactionalDdlPreparation<'a> {
    pub txn_id: i64,
    pub created_tables: &'a [String],
    pub dropped_tables: &'a [(String, Schema)],
    pub pending_indexes: &'a [PendingIndexDefinition],
    pub pending_index_drops: &'a [PendingIndexDrop],
    pub pending_index_renames: &'a [PendingIndexRename],
    pub pending_table_renames: &'a [PendingTableRename],
    pub pending_schema_changes: &'a [PendingSchemaChange],
    pub catalog_mutation: Option<&'a CatalogMutationSet>,
}

/// Named transaction-private inputs for the post-commit DDL publication phase.
pub struct TransactionalDdlPublication<'a> {
    pub txn_id: i64,
    pub created_tables: &'a [String],
    pub dropped_tables: &'a [(String, Schema)],
    pub pending_indexes: &'a [PendingIndexDefinition],
    pub pending_index_drops: &'a [PendingIndexDrop],
    pub pending_index_renames: &'a [PendingIndexRename],
    pub pending_table_renames: &'a [PendingTableRename],
    pub pending_schema_changes: &'a [PendingSchemaChange],
    pub catalog_mutation: Option<&'a CatalogMutationSet>,
    pub commit_lsn: u64,
}

/// MVCC Transaction implementation
pub struct MvccTransaction {
    /// Transaction ID
    id: i64,
    /// Transaction state
    state: TransactionState,
    /// Transaction-specific isolation level (if different from engine default)
    isolation_level: Option<IsolationLevel>,
    /// Reference to the transaction registry
    registry: Arc<TransactionRegistry>,
    /// Begin sequence number (for snapshot isolation)
    begin_seq: i64,
    /// Engine reference for table operations (will be set by Engine)
    engine_operations: Option<Arc<dyn TransactionEngineOperations>>,
    /// Savepoints: maps savepoint name to state (timestamp + DDL snapshot)
    savepoints: FxHashMap<String, SavepointState>,
    /// Tables created in this transaction (for rollback)
    created_tables: Vec<String>,
    /// Tables dropped in this transaction (for rollback - stores name and schema)
    dropped_tables: Vec<(String, Schema)>,
    /// CREATE INDEX definitions applied only under the commit publication fence.
    pending_indexes: Vec<PendingIndexDefinition>,
    /// Existing indexes removed only at the durable commit boundary.
    pending_index_drops: Vec<PendingIndexDrop>,
    /// Existing indexes renamed only at the durable commit boundary.
    pending_index_renames: Vec<PendingIndexRename>,
    /// Existing tables renamed only at the durable commit boundary.
    pending_table_renames: Vec<PendingTableRename>,
    /// ALTER TABLE schema transitions kept private until commit publication.
    pending_schema_changes: Vec<PendingSchemaChange>,
    /// The only typed logical-catalog delta admitted for this transaction.
    pending_catalog_mutation: Option<CatalogMutationSet>,
    /// Last contribution made by this transaction to the process-wide bounded
    /// DDL owner gauges.
    published_ddl_owners: PublishedDdlOwners,
}

/// Operations that require engine access
///
/// This trait allows the transaction to call back into the engine
/// without creating circular dependencies.
pub trait TransactionEngineOperations: Send + Sync {
    /// Get a table by name, initializing transaction-local version store
    fn get_table_for_transaction(&self, txn_id: i64, table_name: &str) -> Result<Box<dyn Table>>;

    /// Create a new table
    fn create_table(&self, txn_id: i64, name: &str, schema: Schema) -> Result<Box<dyn Table>>;

    /// Recreate a previously published table while undoing DROP TABLE.
    fn restore_table(&self, name: &str, schema: Schema) -> Result<()>;

    /// Drop a table
    fn drop_table(&self, name: &str) -> Result<()>;

    /// List all tables
    fn list_tables(&self) -> Result<Vec<String>>;

    /// Rename a table
    fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()>;

    /// Record commit in WAL
    fn record_commit(&self, txn_id: i64) -> Result<u64>;

    /// Record rollback in WAL
    fn record_rollback(&self, txn_id: i64) -> Result<()>;

    /// Get all tables with pending changes for a transaction
    fn get_tables_with_pending_changes(&self, txn_id: i64) -> Result<Vec<Box<dyn Table>>>;

    /// Check if transaction has any pending DML changes (without allocating)
    fn has_pending_dml_changes(&self, txn_id: i64) -> bool;

    /// Validate all touched tables before any table publishes index/version
    /// changes. Writer preflight is serialized against other writers, while
    /// readers remain available until the short publication interval.
    fn validate_transaction_commit(&self, _txn_id: i64) -> Result<()> {
        Ok(())
    }

    /// Record every pending DML operation before the transaction commit marker.
    /// This phase must not publish hot versions, indexes or cold tombstones, so
    /// any ordinary WAL error still has the single safe outcome: rollback.
    fn record_transaction_dml(&self, txn_id: i64) -> Result<()>;

    /// Publish every prepared table after the shared commit marker is durable.
    /// Validation and table membership fences are already held. Any unexpected
    /// error here is an explicit recovery-owned outcome, never a rollback.
    fn publish_transaction_dml(&self, txn_id: i64) -> Result<()>;

    /// Publish scalar cold-storage state after registry completion at the
    /// atomically reserved storage visibility point. The caller still owns the
    /// global visibility and touched-table membership fences, so no statement
    /// can observe the physical publication interval.
    fn publish_committed_transaction_storage(&self, _txn_id: i64, _visibility_seq: u64) {}

    /// Drop transaction-local stores and release row claims only after the
    /// registry has published the commit as visible.
    fn finalize_transaction_commit(&self, _txn_id: i64) {}

    /// Rollback all tables for a transaction at once
    /// This cleans up the transaction's entries in txn_version_stores
    fn rollback_all_tables(&self, txn_id: i64);

    /// Defer table cleanup to background thread (avoids synchronous deallocation)
    /// Default implementation drops synchronously
    fn defer_table_cleanup(&self, _tables: Vec<Box<dyn Table>>) {
        // Default: just drop synchronously (tables dropped when _tables goes out of scope)
    }

    /// Apply storage backpressure before the commit owns any DDL, visibility,
    /// seal, or table-membership fence. A hard rejection is rollback-safe and
    /// leaves the transaction active for retry.
    fn wait_for_storage_pressure(&self, _txn_id: i64) -> Result<()> {
        Ok(())
    }

    /// Acquire the shared global seal fence for commit publication. This is
    /// intentionally separate from table-membership locking: a commit must
    /// wait for an active checkpoint before taking the visibility fence, so
    /// readers remain available while maintenance owns the seal boundary.
    /// Returns None for in-memory engines (no persistence, no seal fence needed).
    fn acquire_seal_fence(&self, _txn_id: i64) -> Option<SealFenceGuard> {
        None
    }

    /// Acquire ordered exclusive table-membership fences after the caller owns
    /// both the shared seal fence and exclusive visibility fence.
    fn lock_commit_membership_fences(&self, _txn_id: i64, _guard: &mut SealFenceGuard) {}

    /// Acquire exclusive catalog publication ownership for commit/rollback.
    fn acquire_ddl_fence(&self) -> Option<DdlFenceGuard> {
        None
    }

    /// Acquire exclusive ownership of the logical commit visibility point.
    ///
    /// SELECT statements hold the shared side while constructing one result.
    /// A writer takes the exclusive side only while prepared table changes and
    /// the registry outcome become visible as one cross-table epoch.
    fn acquire_commit_visibility_fence(&self) -> Option<VisibilityFenceGuard> {
        None
    }

    /// Install a transaction-private schema overlay for ALTER TABLE.
    fn stage_schema_change(&self, _txn_id: i64, _change: &PendingSchemaChange) -> Result<()> {
        Err(Error::NotSupported(
            "transactional ALTER TABLE is not supported by this storage engine".to_string(),
        ))
    }

    /// Rebuild transaction-private schema overlays after savepoint rollback.
    fn reset_schema_changes(&self, _txn_id: i64, _changes: &[PendingSchemaChange]) -> Result<()> {
        Ok(())
    }

    /// Hide a staged DROP INDEX only from the owning transaction.
    fn stage_index_drop(&self, _txn_id: i64, _drop: &PendingIndexDrop) -> Result<()> {
        Ok(())
    }

    /// Rebuild transaction-private DROP INDEX visibility after savepoint rollback.
    fn reset_index_drops(&self, _txn_id: i64, _drops: &[PendingIndexDrop]) -> Result<()> {
        Ok(())
    }

    /// Build staged indexes and write CREATE TABLE/CREATE INDEX WAL records
    /// under the owning user transaction ID. No catalog object is published by
    /// this call.
    fn prepare_transactional_ddl(
        &self,
        _preparation: TransactionalDdlPreparation<'_>,
    ) -> Result<()> {
        Ok(())
    }

    /// Make fully prepared transaction-private index generations visible only
    /// after all commit validation has succeeded and before WAL publication.
    fn activate_transactional_indexes(&self, _txn_id: i64) -> Result<()> {
        Ok(())
    }

    /// Publish private CREATE TABLE stores after the transaction commit marker
    /// and MVCC visibility point are durable.
    fn publish_transactional_ddl(
        &self,
        _publication: TransactionalDdlPublication<'_>,
    ) -> Result<()> {
        Ok(())
    }
}

/// RAII guard for the engine-wide DDL publication fence.
///
/// The raw parking_lot API lets a transaction own this guard without borrowing
/// the engine object. `exclusive=false` is used by statement execution;
/// explicit transaction commit uses the exclusive form.
pub struct DdlFenceGuard {
    lock: Arc<parking_lot::RwLock<()>>,
    exclusive: bool,
}

unsafe impl Send for DdlFenceGuard {}
unsafe impl Sync for DdlFenceGuard {}

impl DdlFenceGuard {
    pub fn try_shared(lock: Arc<parking_lot::RwLock<()>>) -> Option<Self> {
        use parking_lot::lock_api::RawRwLock;
        if unsafe { lock.raw().try_lock_shared() } {
            Some(Self {
                lock,
                exclusive: false,
            })
        } else {
            None
        }
    }

    pub fn shared(lock: Arc<parking_lot::RwLock<()>>) -> Self {
        use parking_lot::lock_api::RawRwLock;
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        unsafe { lock.raw().lock_shared() };
        #[cfg(feature = "bench-harness")]
        crate::instrumentation::record_runtime_wait(
            crate::instrumentation::RuntimeWaitKind::DdlShared,
            started.elapsed(),
        );
        Self {
            lock,
            exclusive: false,
        }
    }

    pub fn exclusive(lock: Arc<parking_lot::RwLock<()>>) -> Self {
        use parking_lot::lock_api::RawRwLock;
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        unsafe { lock.raw().lock_exclusive() };
        #[cfg(feature = "bench-harness")]
        crate::instrumentation::record_runtime_wait(
            crate::instrumentation::RuntimeWaitKind::DdlExclusive,
            started.elapsed(),
        );
        Self {
            lock,
            exclusive: true,
        }
    }
}

impl Drop for DdlFenceGuard {
    fn drop(&mut self) {
        use parking_lot::lock_api::RawRwLock;
        unsafe {
            if self.exclusive {
                self.lock.raw().unlock_exclusive();
            } else {
                self.lock.raw().unlock_shared();
            }
        }
    }
}

/// RAII guard that admits one auto-commit catalog writer at a time.
///
/// This fence deliberately differs from [`DdlFenceGuard`]. Storage operations
/// take the DDL fence internally while preparing and publishing physical
/// objects, so holding that fence across a complete SQL statement would be
/// recursive. The catalog-writer fence instead spans catalog pinning through
/// commit and prevents a second auto-commit DDL statement from building a
/// mutation against the same predecessor generation.
pub struct CatalogWriteFenceGuard {
    lock: Arc<parking_lot::Mutex<()>>,
}

unsafe impl Send for CatalogWriteFenceGuard {}
unsafe impl Sync for CatalogWriteFenceGuard {}

impl CatalogWriteFenceGuard {
    pub fn exclusive(lock: Arc<parking_lot::Mutex<()>>) -> Self {
        use parking_lot::lock_api::RawMutex;
        unsafe { lock.raw().lock() };
        Self { lock }
    }
}

impl Drop for CatalogWriteFenceGuard {
    fn drop(&mut self) {
        use parking_lot::lock_api::RawMutex;
        unsafe { self.lock.raw().unlock() };
    }
}

/// RAII guard for the engine-wide logical commit visibility fence.
pub struct VisibilityFenceGuard {
    lock: Arc<parking_lot::RwLock<()>>,
    exclusive: bool,
}

unsafe impl Send for VisibilityFenceGuard {}
unsafe impl Sync for VisibilityFenceGuard {}

impl VisibilityFenceGuard {
    pub fn shared(lock: Arc<parking_lot::RwLock<()>>) -> Self {
        use parking_lot::lock_api::RawRwLock;
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        unsafe { lock.raw().lock_shared() };
        #[cfg(feature = "bench-harness")]
        crate::instrumentation::record_runtime_wait(
            crate::instrumentation::RuntimeWaitKind::VisibilityShared,
            started.elapsed(),
        );
        Self {
            lock,
            exclusive: false,
        }
    }

    pub fn exclusive(lock: Arc<parking_lot::RwLock<()>>) -> Self {
        use parking_lot::lock_api::RawRwLock;
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        unsafe { lock.raw().lock_exclusive() };
        #[cfg(feature = "bench-harness")]
        crate::instrumentation::record_runtime_wait(
            crate::instrumentation::RuntimeWaitKind::VisibilityExclusive,
            started.elapsed(),
        );
        Self {
            lock,
            exclusive: true,
        }
    }
}

impl Drop for VisibilityFenceGuard {
    fn drop(&mut self) {
        use parking_lot::lock_api::RawRwLock;
        unsafe {
            if self.exclusive {
                self.lock.raw().unlock_exclusive();
            } else {
                self.lock.raw().unlock_shared();
            }
        }
    }
}

/// RAII guard that holds a seal fence read lock. When dropped, the read lock
/// is released. The checkpoint micro-seal acquires the write lock, which
/// blocks until all SealFenceGuards are dropped.
pub struct SealFenceGuard {
    /// Keep the Arc alive so the lock outlives the guard.
    _lock: Arc<parking_lot::RwLock<()>>,
    /// Raw pointer to avoid lifetime issues with RwLockReadGuard.
    /// SAFETY: The Arc above keeps the RwLock alive.
    _raw: *const (),
    /// Exclusive per-table publication locks. These are acquired in stable
    /// table-name order and released before the global seal lock.
    membership_locks: Vec<Arc<parking_lot::RwLock<()>>>,
}

// SAFETY: SealFenceGuard only holds an Arc (Send+Sync) and a raw read-lock
// that is released on drop. The guard is created and dropped on the same
// thread (the commit thread). The raw pointer is not dereferenced.
unsafe impl Send for SealFenceGuard {}
unsafe impl Sync for SealFenceGuard {}

impl SealFenceGuard {
    pub fn new(lock: Arc<parking_lot::RwLock<()>>) -> Self {
        // Acquire the read lock via the raw API so we can control the lifetime.
        // parking_lot::RawRwLock::lock_shared is balanced by unlock_shared in Drop.
        use parking_lot::lock_api::RawRwLock;
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        // SAFETY: lock_shared() is always safe to call. We balance it with
        // unlock_shared() in Drop. The Arc keeps the RwLock alive.
        unsafe { lock.raw().lock_shared() };
        #[cfg(feature = "bench-harness")]
        crate::instrumentation::record_runtime_wait(
            crate::instrumentation::RuntimeWaitKind::SealShared,
            started.elapsed(),
        );
        Self {
            _raw: std::ptr::null(),
            _lock: lock,
            membership_locks: Vec::new(),
        }
    }

    /// Add an exclusive per-table publication lock while the global shared
    /// seal guard is already held. Callers must supply locks in a deterministic
    /// order to keep multi-table commits deadlock-free.
    pub fn lock_membership_fence(&mut self, lock: Arc<parking_lot::RwLock<()>>) {
        use parking_lot::lock_api::RawRwLock;
        #[cfg(feature = "bench-harness")]
        let started = std::time::Instant::now();
        // SAFETY: lock_exclusive() is paired with unlock_exclusive() in Drop.
        unsafe { lock.raw().lock_exclusive() };
        #[cfg(feature = "bench-harness")]
        crate::instrumentation::record_runtime_wait(
            crate::instrumentation::RuntimeWaitKind::MembershipExclusive,
            started.elapsed(),
        );
        self.membership_locks.push(lock);
    }
}

impl Drop for SealFenceGuard {
    fn drop(&mut self) {
        use parking_lot::lock_api::RawRwLock;
        for lock in self.membership_locks.drain(..).rev() {
            // SAFETY: each lock was acquired once by lock_membership_fence.
            unsafe { lock.raw().unlock_exclusive() };
        }
        // SAFETY: We acquired lock_shared() in new(), this is the balancing release.
        unsafe { self._lock.raw().unlock_shared() };
    }
}

impl MvccTransaction {
    /// Creates a new MVCC transaction
    pub fn new(id: i64, begin_seq: i64, registry: Arc<TransactionRegistry>) -> Self {
        let isolation_level = registry.get_isolation_level(id);
        Self {
            id,
            state: TransactionState::Active,
            isolation_level: Some(isolation_level),
            registry,
            begin_seq,
            engine_operations: None,
            savepoints: FxHashMap::default(),
            created_tables: Vec::new(),
            dropped_tables: Vec::new(),
            pending_indexes: Vec::new(),
            pending_index_drops: Vec::new(),
            pending_index_renames: Vec::new(),
            pending_table_renames: Vec::new(),
            pending_schema_changes: Vec::new(),
            pending_catalog_mutation: None,
            published_ddl_owners: PublishedDdlOwners::default(),
        }
    }

    /// Sets the engine operations callback
    pub fn set_engine_operations(&mut self, ops: Arc<dyn TransactionEngineOperations>) {
        self.engine_operations = Some(ops);
    }

    /// Returns the begin sequence number
    pub fn begin_seq(&self) -> i64 {
        self.begin_seq
    }

    /// Returns the current transaction state
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Returns the isolation level for this transaction
    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.isolation_level
            .unwrap_or_else(|| self.registry.get_global_isolation_level())
    }

    /// Check if transaction is active
    fn check_active(&self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(Error::TransactionClosed);
        }
        Ok(())
    }

    /// Get engine operations, returning error if not set
    fn get_engine_ops(&self) -> Result<&Arc<dyn TransactionEngineOperations>> {
        self.engine_operations
            .as_ref()
            .ok_or_else(|| Error::internal("engine operations not set"))
    }

    /// Clean up transaction resources
    fn cleanup(&mut self) {
        // Clear DDL tracking
        self.created_tables.clear();
        self.dropped_tables.clear();
        self.pending_indexes.clear();
        self.pending_index_drops.clear();
        self.pending_index_renames.clear();
        self.pending_table_renames.clear();
        self.pending_schema_changes.clear();
        self.pending_catalog_mutation = None;
        self.publish_ddl_owners();

        // Remove transaction isolation level from registry
        self.registry.remove_transaction_isolation_level(self.id);
    }

    fn ddl_owners(&self) -> PublishedDdlOwners {
        PublishedDdlOwners {
            tables: self
                .created_tables
                .len()
                .saturating_add(self.dropped_tables.len())
                .saturating_add(self.pending_table_renames.len()) as u64,
            indexes: self
                .pending_indexes
                .len()
                .saturating_add(self.pending_index_renames.len()) as u64,
            index_drops: self.pending_index_drops.len() as u64,
            schema_changes: self.pending_schema_changes.len() as u64,
            constraint_changes: self
                .pending_schema_changes
                .iter()
                .filter(|change| schema_constraints_changed(change))
                .count() as u64,
        }
    }

    fn publish_ddl_owners(&mut self) {
        let next = self.ddl_owners();
        let before = self.published_ddl_owners;
        crate::instrumentation::replace_transaction_ddl_owners(
            before.tables,
            next.tables,
            before.indexes,
            next.indexes,
            before.index_drops,
            next.index_drops,
            before.schema_changes,
            next.schema_changes,
            before.constraint_changes,
            next.constraint_changes,
        );
        self.published_ddl_owners = next;
    }

    fn clear_ddl_owner_publication(&mut self) {
        let before = self.published_ddl_owners;
        if before == PublishedDdlOwners::default() {
            return;
        }
        crate::instrumentation::replace_transaction_ddl_owners(
            before.tables,
            0,
            before.indexes,
            0,
            before.index_drops,
            0,
            before.schema_changes,
            0,
            before.constraint_changes,
            0,
        );
        self.published_ddl_owners = PublishedDdlOwners::default();
    }

    /// Finish a commit whose table versions or WAL outcome have crossed the
    /// publication boundary. This helper is intentionally terminal even when
    /// catalog publication reports an error: callers must never leave a handle
    /// in `Committing` and later attempt a contradictory rollback.
    fn finish_committed_transaction(&mut self, has_ddl: bool, commit_lsn: u64) -> Result<()> {
        let ops = self.engine_operations.clone();
        let storage_visibility_seq = match self
            .registry
            .complete_commit_with_storage_publication(self.id)
        {
            Ok(sequence) => sequence as u64,
            Err(error) => {
                // A caller reaches this helper only after the shared commit marker
                // (or the read-only start_commit transition). Never leave the Rust
                // object in Committing: release claims/local stores and expose the
                // outcome as recovery-owned uncertainty instead of a rollbackable
                // zombie handle.
                if let Some(ops) = &ops {
                    ops.finalize_transaction_commit(self.id);
                }
                self.state = TransactionState::Committed;
                self.cleanup();
                return Err(error);
            }
        };

        if let Some(ops) = &ops {
            ops.publish_committed_transaction_storage(self.id, storage_visibility_seq);
        }

        let publish_result = if has_ddl {
            if let Some(ops) = &ops {
                ops.publish_transactional_ddl(TransactionalDdlPublication {
                    txn_id: self.id,
                    created_tables: &self.created_tables,
                    dropped_tables: &self.dropped_tables,
                    pending_indexes: &self.pending_indexes,
                    pending_index_drops: &self.pending_index_drops,
                    pending_index_renames: &self.pending_index_renames,
                    pending_table_renames: &self.pending_table_renames,
                    pending_schema_changes: &self.pending_schema_changes,
                    catalog_mutation: self.pending_catalog_mutation.as_ref(),
                    commit_lsn,
                })
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };

        if let Some(ops) = &ops {
            ops.finalize_transaction_commit(self.id);
        }
        self.state = TransactionState::Committed;
        self.cleanup();
        publish_result
    }

    /// Roll back DDL operations (CREATE TABLE / DROP TABLE) in reverse order.
    /// Used by both explicit rollback() and implicit Drop.
    fn rollback_ddl(&self, ops: &dyn TransactionEngineOperations) -> Result<()> {
        let mut failures = Vec::new();

        // Staged indexes may already have been built by commit preparation when
        // a later WAL/DML phase fails. Clean them before private CREATE TABLEs,
        // because an index can belong to a table created by this transaction.
        // A definition that was never built is already clean; every other
        // failure remains visible to the caller.
        for index in self.pending_indexes.iter().rev() {
            match ops.get_table_for_transaction(self.id, &index.table_name) {
                Ok(table) => match table.drop_index(&index.index_name) {
                    Ok(()) | Err(Error::IndexNotFound(_)) => {}
                    Err(error) => {
                        failures.push(format!(
                            "drop index '{}.{}': {error}",
                            index.table_name, index.index_name
                        ));
                    }
                },
                Err(error) => failures.push(format!(
                    "open table '{}' to drop staged index '{}': {error}",
                    index.table_name, index.index_name
                )),
            }
        }

        // Drop tables that were created in this transaction only after their
        // staged/built indexes have been cleaned up.
        for table_name in self.created_tables.iter().rev() {
            if let Err(e) = ops.drop_table(table_name) {
                failures.push(format!("drop table '{table_name}': {e}"));
            }
        }

        // DROP TABLE intents remain private until commit publication, so
        // rollback has no shared catalog or physical state to restore.
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::internal(format!(
                "transaction {} DDL rollback failed: {}",
                self.id,
                failures.join("; ")
            )))
        }
    }

    /// Check if this is a read-only transaction
    fn is_read_only(&self) -> bool {
        // Check for DDL changes
        if !self.created_tables.is_empty()
            || !self.dropped_tables.is_empty()
            || !self.pending_indexes.is_empty()
            || !self.pending_index_drops.is_empty()
            || !self.pending_index_renames.is_empty()
            || !self.pending_table_renames.is_empty()
            || !self.pending_schema_changes.is_empty()
            || self.pending_catalog_mutation.is_some()
        {
            return false;
        }
        // Check for DML changes via engine operations
        if let Some(ops) = &self.engine_operations {
            if ops.has_pending_dml_changes(self.id) {
                return false;
            }
        }
        true
    }

    /// Creates a savepoint with the given name
    ///
    /// Records the current timestamp and DDL state so we can rollback to this point later.
    /// If a savepoint with this name already exists, it is overwritten.
    pub fn create_savepoint(&mut self, name: &str) -> Result<()> {
        self.check_active()?;
        let timestamp = get_fast_timestamp();
        let ddl_state = SavepointDdlState {
            created_tables_len: self.created_tables.len(),
            dropped_tables_len: self.dropped_tables.len(),
            pending_indexes_len: self.pending_indexes.len(),
            pending_index_drops_len: self.pending_index_drops.len(),
            pending_index_renames_len: self.pending_index_renames.len(),
            pending_table_renames_len: self.pending_table_renames.len(),
            pending_schema_changes_len: self.pending_schema_changes.len(),
            catalog_mutation: self.pending_catalog_mutation.clone(),
        };
        self.savepoints.insert(
            name.to_string(),
            SavepointState {
                timestamp,
                ddl_state,
            },
        );
        Ok(())
    }

    /// Releases (removes) a savepoint without rolling back
    ///
    /// The changes made after the savepoint remain intact.
    /// Returns an error if the savepoint doesn't exist.
    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        self.check_active()?;
        if self.savepoints.remove(name).is_none() {
            return Err(Error::invalid_argument(format!(
                "savepoint '{}' does not exist",
                name
            )));
        }
        Ok(())
    }

    /// Rolls back to a savepoint, discarding all changes made after it
    ///
    /// All local DML changes with timestamps after the savepoint are discarded.
    /// DDL operations (CREATE/DROP TABLE) after the savepoint are also reversed.
    /// The target savepoint is retained; only savepoints created after it are
    /// removed, matching SQL ROLLBACK TO semantics.
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        self.check_active()?;

        let sp_state = self.savepoints.get(name).cloned().ok_or_else(|| {
            Error::invalid_argument(format!("savepoint '{}' does not exist", name))
        })?;

        // Rollback DML changes via engine operations (not self.tables which is empty)
        if let Some(ops) = &self.engine_operations {
            let tables = ops.get_tables_with_pending_changes(self.id)?;
            for table in &tables {
                table.rollback_to_timestamp(sp_state.timestamp);
            }
        }

        // Rollback DDL: undo CREATE TABLEs after savepoint
        if let Some(ops) = &self.engine_operations {
            // Tables created after savepoint need to be dropped
            while self.created_tables.len() > sp_state.ddl_state.created_tables_len {
                let table_name = self.created_tables.last().cloned().ok_or_else(|| {
                    Error::internal("created-table savepoint journal became inconsistent")
                })?;
                // Keep the journal entry until the physical reversal succeeds,
                // so the caller can retry or perform full rollback.
                ops.drop_table(&table_name)?;
                self.created_tables.pop();
            }

            // Tables dropped after savepoint need to be recreated
            while self.dropped_tables.len() > sp_state.ddl_state.dropped_tables_len {
                self.dropped_tables.pop();
            }

            while self.pending_indexes.len() > sp_state.ddl_state.pending_indexes_len {
                self.pending_indexes.pop();
            }
            self.pending_index_drops
                .truncate(sp_state.ddl_state.pending_index_drops_len);
            self.pending_index_renames
                .truncate(sp_state.ddl_state.pending_index_renames_len);
            self.pending_table_renames
                .truncate(sp_state.ddl_state.pending_table_renames_len);
            ops.reset_index_drops(self.id, &self.pending_index_drops)?;
            let retained_changes =
                &self.pending_schema_changes[..sp_state.ddl_state.pending_schema_changes_len];
            ops.reset_schema_changes(self.id, retained_changes)?;
            self.pending_schema_changes
                .truncate(sp_state.ddl_state.pending_schema_changes_len);
            self.pending_catalog_mutation = sp_state.ddl_state.catalog_mutation;
        }
        self.publish_ddl_owners();

        // Retain the target savepoint and remove only savepoints created after it.
        self.savepoints
            .retain(|_, sp| sp.timestamp <= sp_state.timestamp);

        Ok(())
    }

    /// Check if a savepoint exists
    pub fn has_savepoint(&self, name: &str) -> bool {
        self.savepoints.contains_key(name)
    }

    /// Gets the timestamp associated with a savepoint
    pub fn get_savepoint_ts(&self, name: &str) -> Option<i64> {
        self.savepoints.get(name).map(|sp| sp.timestamp)
    }
}

fn schema_constraints_changed(change: &PendingSchemaChange) -> bool {
    let before = &change.expected_catalog_schema;
    let after = &change.schema;

    before.constraints() != after.constraints()
        || before.foreign_keys() != after.foreign_keys()
        || before.table_checks() != after.table_checks()
        || before
            .columns()
            .iter()
            .map(column_constraint_state)
            .collect::<Vec<_>>()
            != after
                .columns()
                .iter()
                .map(column_constraint_state)
                .collect::<Vec<_>>()
}

fn column_constraint_state(column: &SchemaColumn) -> (&str, bool, bool, Option<&str>) {
    (
        column.name.as_str(),
        column.nullable,
        column.primary_key,
        column.check_expr.as_deref(),
    )
}

impl Transaction for MvccTransaction {
    fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    fn id(&self) -> i64 {
        self.id
    }

    fn begin(&mut self) -> Result<()> {
        // No-op for compatibility - transaction is initialized in new()
        self.check_active()
    }

    fn commit(&mut self) -> Result<()> {
        self.check_active()?;

        let has_ddl = !self.created_tables.is_empty()
            || !self.dropped_tables.is_empty()
            || !self.pending_indexes.is_empty()
            || !self.pending_index_drops.is_empty()
            || !self.pending_index_renames.is_empty()
            || !self.pending_table_renames.is_empty()
            || !self.pending_schema_changes.is_empty()
            || self.pending_catalog_mutation.is_some();
        let has_dml_changes = self
            .engine_operations
            .as_ref()
            .is_some_and(|ops| ops.has_pending_dml_changes(self.id));
        let is_read_only = !has_ddl && !has_dml_changes;

        if !is_read_only {
            if let Some(ops) = &self.engine_operations {
                ops.wait_for_storage_pressure(self.id)?;
            }
        }

        // No statement may plan against the catalog between physical index
        // preparation and the single publication point below.
        let _ddl_guard = if has_ddl {
            self.engine_operations
                .as_ref()
                .and_then(|ops| ops.acquire_ddl_fence())
        } else {
            None
        };

        if has_ddl {
            if let Some(ops) = &self.engine_operations {
                if let Err(error) = ops.prepare_transactional_ddl(TransactionalDdlPreparation {
                    txn_id: self.id,
                    created_tables: &self.created_tables,
                    dropped_tables: &self.dropped_tables,
                    pending_indexes: &self.pending_indexes,
                    pending_index_drops: &self.pending_index_drops,
                    pending_index_renames: &self.pending_index_renames,
                    pending_table_renames: &self.pending_table_renames,
                    pending_schema_changes: &self.pending_schema_changes,
                    catalog_mutation: self.pending_catalog_mutation.as_ref(),
                }) {
                    self.registry.abort_transaction(self.id);
                    ops.rollback_all_tables(self.id);
                    let cleanup_error = self.rollback_ddl(ops.as_ref()).err();
                    self.state = TransactionState::RolledBack;
                    self.cleanup();
                    return Err(match cleanup_error {
                        Some(cleanup) => Error::internal(format!(
                            "transactional DDL preparation failed: {error}; rollback cleanup failed: {cleanup}"
                        )),
                        None => error,
                    });
                }
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::interleave(
                    crate::test_failpoints::InterleavePoint::IndexPrepared,
                    self.id,
                );
            }
        }

        // Validate every table before changing transaction state or publishing
        // the first row/index. A constraint failure therefore leaves the handle
        // active and explicitly rollback-capable.
        if !is_read_only {
            if let Some(ops) = &self.engine_operations {
                ops.validate_transaction_commit(self.id)?;
            }
            #[cfg(any(test, feature = "test-failpoints"))]
            crate::test_failpoints::interleave(
                crate::test_failpoints::InterleavePoint::ConstraintsValidated,
                self.id,
            );
        }

        // The first validation above is deliberately reader-friendly and may
        // run concurrently with other preflights. Publication needs a second
        // certification under one ordered boundary. Every ready writer joins
        // the visibility queue directly, so long SELECTs cannot reacquire the
        // shared side between commits and starve a hidden global writer queue.
        // Maintenance is still acquired first: a writer waiting for checkpoint
        // never owns visibility and therefore never blocks ordinary reads.
        let mut _seal_guard = if !is_read_only {
            self.engine_operations
                .as_ref()
                .and_then(|ops| ops.acquire_seal_fence(self.id))
        } else {
            None
        };
        let _visibility_guard = if !is_read_only {
            self.engine_operations
                .as_ref()
                .and_then(|ops| ops.acquire_commit_visibility_fence())
        } else {
            None
        };
        if !is_read_only {
            if let Some(ops) = &self.engine_operations {
                ops.validate_transaction_commit(self.id)?;
            }
        }
        // Constraint certification may read parent/child tables through their
        // ordinary shared membership paths. Take table-local publication locks
        // only after certification, while global visibility still excludes any
        // competing commit between that proof and publication.
        if let (Some(ops), Some(seal_guard)) =
            (self.engine_operations.as_ref(), _seal_guard.as_mut())
        {
            ops.lock_commit_membership_fences(self.id, seal_guard);
        }

        self.registry.start_commit(self.id)?;
        self.state = TransactionState::Committing;

        if has_ddl {
            if let Some(ops) = &self.engine_operations {
                if let Err(error) = ops.activate_transactional_indexes(self.id) {
                    self.registry.abort_transaction(self.id);
                    ops.rollback_all_tables(self.id);
                    let cleanup_error = self.rollback_ddl(ops.as_ref()).err();
                    self.state = TransactionState::RolledBack;
                    self.cleanup();
                    return Err(match cleanup_error {
                        Some(cleanup) => Error::internal(format!(
                            "transactional index activation failed: {error}; rollback cleanup failed: {cleanup}"
                        )),
                        None => error,
                    });
                }
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::interleave(
                    crate::test_failpoints::InterleavePoint::IndexPublished,
                    self.id,
                );
            }
        }

        // Two-phase commit protocol
        if !is_read_only {
            // Phase 1: record the complete DML unit before publishing any table.
            // A late WAL failure therefore cannot leave an earlier table visible.
            if let Some(ops) = self.engine_operations.clone() {
                if let Err(error) = ops.record_transaction_dml(self.id) {
                    self.registry.abort_transaction(self.id);
                    ops.rollback_all_tables(self.id);
                    let cleanup_error = has_ddl
                        .then(|| self.rollback_ddl(ops.as_ref()))
                        .and_then(Result::err);
                    self.state = TransactionState::RolledBack;
                    self.cleanup();
                    return Err(match cleanup_error {
                        Some(cleanup) => Error::internal(format!(
                            "transaction DML recording failed: {error}; rollback cleanup failed: {cleanup}"
                        )),
                        None => error,
                    });
                }
            }

            // Phase 2: Record the one commit marker BEFORE making changes visible.
            // This ensures crash recovery sees the COMMIT marker even if we crash
            // before complete_commit(). WAL is only read during recovery, so writing
            // the marker before visibility doesn't affect normal operation.
            if let Some(ops) = self.engine_operations.clone() {
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::interleave(
                    crate::test_failpoints::InterleavePoint::WalBeforeCommitMarker,
                    self.id,
                );
                let mut commit_lsn = 0;
                let marker_uncertainty = match ops.record_commit(self.id) {
                    Ok(lsn) => {
                        commit_lsn = lsn;
                        #[cfg(any(test, feature = "test-failpoints"))]
                        crate::test_failpoints::interleave(
                            crate::test_failpoints::InterleavePoint::WalCommitMarkerDurable,
                            self.id,
                        );
                        None
                    }
                    Err(Error::WalDurabilityUncertain { detail }) => Some(detail),
                    Err(e) => {
                        // No marker bytes crossed the append boundary, so rollback
                        // remains the only safe outcome.
                        self.registry.abort_transaction(self.id);
                        ops.rollback_all_tables(self.id);
                        let cleanup_error = has_ddl
                            .then(|| self.rollback_ddl(ops.as_ref()))
                            .and_then(Result::err);
                        self.state = TransactionState::RolledBack;
                        self.cleanup();
                        return Err(match cleanup_error {
                            Some(cleanup) => Error::internal(format!(
                                "commit marker append failed: {e}; rollback cleanup failed: {cleanup}"
                            )),
                            None => e,
                        });
                    }
                };

                // The marker is the point of no return. Complete every table
                // under the same membership fences. A post-marker invariant
                // failure is reported only as explicit recovery uncertainty.
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::interleave(
                    crate::test_failpoints::InterleavePoint::VisibilityBeforePublish,
                    self.id,
                );
                let dml_publish = ops.publish_transaction_dml(self.id);
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::interleave(
                    crate::test_failpoints::InterleavePoint::VisibilityDmlPublished,
                    self.id,
                );
                let finish = self.finish_committed_transaction(has_ddl, commit_lsn);
                #[cfg(any(test, feature = "test-failpoints"))]
                crate::test_failpoints::interleave(
                    crate::test_failpoints::InterleavePoint::VisibilityPublished,
                    self.id,
                );
                if marker_uncertainty.is_some() || dml_publish.is_err() || finish.is_err() {
                    let mut details = Vec::new();
                    if let Some(detail) = marker_uncertainty {
                        details.push(detail);
                    }
                    if let Err(error) = dml_publish {
                        details.push(format!(
                            "transaction {} is durably committed but DML publication requires recovery: {}",
                            self.id, error
                        ));
                    }
                    if let Err(error) = finish {
                        details.push(format!(
                            "transaction {} is durably committed but catalog publication requires recovery: {}",
                            self.id, error
                        ));
                    }
                    return Err(Error::WalDurabilityUncertain {
                        detail: details.join("; "),
                    });
                }
            }

            return Ok(());
        }

        self.finish_committed_transaction(has_ddl, 0)
    }

    fn rollback(&mut self) -> Result<()> {
        self.check_active()?;

        // Check if read-only before rolling back
        let is_read_only = self.is_read_only();

        // Mark transaction as aborted in registry
        self.registry.abort_transaction(self.id);

        let mut cleanup_failures = Vec::new();

        // Rollback DDL operations (CREATE TABLE / DROP TABLE) in reverse order
        if let Some(ops) = &self.engine_operations {
            if let Err(error) = self.rollback_ddl(ops.as_ref()) {
                cleanup_failures.push(error.to_string());
            }
        }

        // Notify engine of rollback.
        if let Some(ops) = &self.engine_operations {
            // Clean up txn_version_stores entry to prevent memory leak
            ops.rollback_all_tables(self.id);
        }

        // Record in WAL if not read-only
        if !is_read_only {
            if let Some(ops) = &self.engine_operations {
                if let Err(error) = ops.record_rollback(self.id) {
                    cleanup_failures.push(format!("WAL rollback record: {error}"));
                }
            }
        }

        // Mark as rolled back
        self.state = TransactionState::RolledBack;
        self.cleanup();
        if cleanup_failures.is_empty() {
            Ok(())
        } else {
            Err(Error::internal(format!(
                "transaction {} rolled back with cleanup failures: {}",
                self.id,
                cleanup_failures.join("; ")
            )))
        }
    }

    fn create_savepoint(&mut self, name: &str) -> Result<()> {
        // Delegate to the inherent method
        MvccTransaction::create_savepoint(self, name)
    }

    fn release_savepoint(&mut self, name: &str) -> Result<()> {
        // Delegate to the inherent method
        MvccTransaction::release_savepoint(self, name)
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        // Delegate to the inherent method
        MvccTransaction::rollback_to_savepoint(self, name)
    }

    fn stage_catalog_mutation(&mut self, mutation: CatalogMutationSet) -> Result<()> {
        self.check_active()?;
        if let Some(existing) = &self.pending_catalog_mutation {
            return if existing == &mutation {
                Ok(())
            } else {
                Err(Error::invalid_argument(
                    "transaction already owns a different catalog mutation",
                ))
            };
        }
        self.pending_catalog_mutation = Some(mutation);
        Ok(())
    }

    fn get_savepoint_timestamp(&self, name: &str) -> Option<i64> {
        // Delegate to the inherent method
        MvccTransaction::get_savepoint_ts(self, name)
    }

    fn set_isolation_level(&mut self, level: IsolationLevel) -> Result<()> {
        self.check_active()?;
        if self
            .registry
            .set_transaction_isolation_level(self.id, level)
        {
            self.isolation_level = Some(level);
            Ok(())
        } else {
            Err(Error::invalid_argument(
                "transaction isolation level is fixed at begin",
            ))
        }
    }

    fn create_table(&mut self, name: &str, schema: Schema) -> Result<Box<dyn Table>> {
        self.check_active()?;

        let ops = self.get_engine_ops()?;
        let table = ops.create_table(self.id, name, schema)?;

        // Track for rollback - store the table name
        self.created_tables.push(name.to_lowercase());
        self.publish_ddl_owners();

        Ok(table)
    }

    /// Stage a table drop. Shared catalog/data publication happens only after
    /// this transaction's commit marker is durable.
    fn drop_table(&mut self, name: &str) -> Result<()> {
        self.check_active()?;

        // Before dropping, get the schema so we can recreate on rollback
        // We need to get the table to access its schema
        // Scope the borrow to allow later mutable operations
        let schema = {
            let ops = self.get_engine_ops()?;
            let table = ops.get_table_for_transaction(self.id, name)?;
            table.schema().clone()
        };

        let lower = name.to_lowercase();
        if let Some(position) = self
            .created_tables
            .iter()
            .position(|created| created == &lower)
        {
            // CREATE followed by DROP in the same transaction cancels the
            // unpublished reservation and emits neither durable unit.
            self.get_engine_ops()?.drop_table(name)?;
            self.created_tables.remove(position);
            self.publish_ddl_owners();
            return Ok(());
        }
        if self
            .dropped_tables
            .iter()
            .any(|(dropped, _)| dropped == &lower)
        {
            return Err(Error::TableNotFound(lower));
        }

        // The table remains globally visible until the transaction marker is
        // durable. This vector is the private DROP intent and preflight schema.
        self.dropped_tables.push((lower, schema));
        self.publish_ddl_owners();

        Ok(())
    }

    fn get_table(&self, name: &str) -> Result<Box<dyn Table>> {
        self.check_active()?;

        if self
            .pending_table_renames
            .iter()
            .any(|rename| rename.old_name.eq_ignore_ascii_case(name))
        {
            return Err(Error::TableNotFound(name.to_lowercase()));
        }
        let mut physical_name = name.to_lowercase();
        for rename in self.pending_table_renames.iter().rev() {
            if rename.new_name.eq_ignore_ascii_case(&physical_name) {
                physical_name = rename.old_name.clone();
            }
        }

        if self
            .dropped_tables
            .iter()
            .any(|(table_name, _)| table_name.eq_ignore_ascii_case(&physical_name))
        {
            return Err(Error::TableNotFound(name.to_lowercase()));
        }

        // Get from engine
        let ops = self.get_engine_ops()?;
        ops.get_table_for_transaction(self.id, &physical_name)
    }

    fn list_tables(&self) -> Result<Vec<String>> {
        self.check_active()?;

        let ops = self.get_engine_ops()?;
        let mut tables = ops.list_tables()?;
        tables.retain(|table| {
            !self
                .dropped_tables
                .iter()
                .any(|(dropped, _)| dropped.eq_ignore_ascii_case(table))
        });
        for created in &self.created_tables {
            if !tables
                .iter()
                .any(|table| table.eq_ignore_ascii_case(created))
            {
                tables.push(created.clone());
            }
        }
        for rename in &self.pending_table_renames {
            if let Some(position) = tables
                .iter()
                .position(|table| table.eq_ignore_ascii_case(&rename.old_name))
            {
                tables[position] = rename.new_name.clone();
            }
        }
        tables.sort_unstable();
        Ok(tables)
    }

    fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        self.check_active()?;
        let old_name = old_name.to_lowercase();
        let new_name = new_name.to_lowercase();
        self.get_table(&old_name)?;
        if self.get_table(&new_name).is_ok()
            || self
                .pending_table_renames
                .iter()
                .any(|rename| rename.new_name.eq_ignore_ascii_case(&new_name))
        {
            return Err(Error::TableAlreadyExists(new_name));
        }
        if let Some(position) = self
            .pending_table_renames
            .iter()
            .position(|rename| rename.new_name == old_name)
        {
            if self.pending_table_renames[position].old_name == new_name {
                self.pending_table_renames.remove(position);
            } else {
                self.pending_table_renames[position].new_name = new_name;
            }
        } else {
            self.pending_table_renames
                .push(PendingTableRename { old_name, new_name });
        }
        self.publish_ddl_owners();
        Ok(())
    }

    fn create_table_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        columns: &[String],
        is_unique: bool,
    ) -> Result<()> {
        self.stage_create_index(PendingIndexDefinition {
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            columns: columns.to_vec(),
            is_unique,
            index_type: None,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
            hnsw_distance_metric: None,
            partial_predicate: None,
            key_encoder: None,
        })
    }

    fn stage_create_index(&mut self, definition: PendingIndexDefinition) -> Result<()> {
        self.check_active()?;
        let table = self.get_table(&definition.table_name)?;
        for column in &definition.columns {
            if table.schema().find_column(column).is_none() {
                return Err(Error::ColumnNotFound(column.clone()));
            }
        }
        if table.get_index(&definition.index_name).is_some()
            || self.pending_indexes.iter().any(|pending| {
                pending
                    .table_name
                    .eq_ignore_ascii_case(&definition.table_name)
                    && pending
                        .index_name
                        .eq_ignore_ascii_case(&definition.index_name)
            })
        {
            return Err(Error::IndexAlreadyExists(definition.index_name));
        }
        self.pending_indexes.push(definition);
        self.publish_ddl_owners();
        Ok(())
    }

    fn staged_index_definitions(&self, table_name: &str) -> Vec<PendingIndexDefinition> {
        self.pending_indexes
            .iter()
            .filter(|definition| definition.table_name.eq_ignore_ascii_case(table_name))
            .cloned()
            .collect()
    }

    fn stage_drop_index(&mut self, drop: PendingIndexDrop) -> Result<()> {
        self.check_active()?;
        if let Some(index) = self.pending_indexes.iter().position(|pending| {
            pending.table_name.eq_ignore_ascii_case(&drop.table_name)
                && pending.index_name.eq_ignore_ascii_case(&drop.index_name)
        }) {
            self.pending_indexes.remove(index);
            self.publish_ddl_owners();
            return Ok(());
        }
        let table = self.get_table(&drop.table_name)?;
        if table.get_index(&drop.index_name).is_none() {
            return Err(Error::IndexNotFound(drop.index_name));
        }
        if self.pending_index_drops.iter().any(|pending| {
            pending.table_name.eq_ignore_ascii_case(&drop.table_name)
                && pending.index_name.eq_ignore_ascii_case(&drop.index_name)
        }) {
            return Err(Error::InvalidArgument(format!(
                "index '{}.{}' is already staged for drop",
                drop.table_name, drop.index_name
            )));
        }
        self.get_engine_ops()?.stage_index_drop(self.id, &drop)?;
        self.pending_index_drops.push(drop);
        self.publish_ddl_owners();
        Ok(())
    }

    fn stage_rename_index(&mut self, rename: PendingIndexRename) -> Result<()> {
        self.check_active()?;
        let table = self.get_table(&rename.table_name)?;
        if table.get_index(&rename.old_index_name).is_none() {
            return Err(Error::IndexNotFound(rename.old_index_name));
        }
        if table.get_index(&rename.new_index_name).is_some() {
            return Err(Error::IndexAlreadyExists(rename.new_index_name));
        }
        if self.pending_index_renames.iter().any(|pending| {
            pending.table_name.eq_ignore_ascii_case(&rename.table_name)
                && (pending
                    .old_index_name
                    .eq_ignore_ascii_case(&rename.old_index_name)
                    || pending
                        .new_index_name
                        .eq_ignore_ascii_case(&rename.new_index_name))
        }) {
            return Err(Error::InvalidArgument(format!(
                "index rename '{}.{}' is already staged",
                rename.table_name, rename.old_index_name
            )));
        }
        self.pending_index_renames.push(rename);
        self.publish_ddl_owners();
        Ok(())
    }

    fn drop_table_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        self.check_active()?;
        Err(Error::NotSupported(format!(
            "DROP INDEX '{table_name}.{index_name}' is not supported inside an explicit transaction"
        )))
    }

    fn create_table_btree_index(
        &mut self,
        table_name: &str,
        column_name: &str,
        is_unique: bool,
        custom_name: Option<&str>,
    ) -> Result<()> {
        self.check_active()?;
        let index_name = custom_name.unwrap_or(column_name);
        Err(Error::NotSupported(format!(
            "CREATE BTREE INDEX '{table_name}.{index_name}' is not supported inside an explicit transaction (unique={is_unique})"
        )))
    }

    fn drop_table_btree_index(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        self.check_active()?;
        Err(Error::NotSupported(format!(
            "DROP BTREE INDEX '{table_name}.{column_name}' is not supported inside an explicit transaction"
        )))
    }

    fn add_table_column(&mut self, table_name: &str, column: SchemaColumn) -> Result<()> {
        self.check_active()?;
        let mut schema = self.get_table(table_name)?.schema().clone();
        let mut column = column;
        column.id = schema.columns.len();
        schema.add_column(column)?;
        self.stage_table_schema_change(table_name, schema, true)
    }

    fn stage_table_schema_change(
        &mut self,
        table_name: &str,
        schema: Schema,
        requires_row_normalization: bool,
    ) -> Result<()> {
        self.check_active()?;
        let table_name = table_name.to_lowercase();
        let current_schema = self.get_table(&table_name)?.schema().clone();
        let expected_catalog_schema = self
            .pending_schema_changes
            .iter()
            .find(|pending| pending.table_name.eq_ignore_ascii_case(&table_name))
            .map(|pending| pending.expected_catalog_schema.clone())
            .unwrap_or(current_schema);
        let requires_row_normalization = requires_row_normalization
            || self
                .pending_schema_changes
                .iter()
                .filter(|pending| pending.table_name.eq_ignore_ascii_case(&table_name))
                .any(|pending| pending.requires_row_normalization);
        let change = PendingSchemaChange {
            table_name,
            schema,
            expected_catalog_schema,
            requires_row_normalization,
            physical_transition: None,
        };
        self.get_engine_ops()?
            .stage_schema_change(self.id, &change)?;
        self.pending_schema_changes.push(change);
        self.publish_ddl_owners();
        Ok(())
    }

    fn stage_table_schema_transition(
        &mut self,
        table_name: &str,
        schema: Schema,
        requires_row_normalization: bool,
        transition: SchemaPhysicalTransition,
    ) -> Result<()> {
        self.check_active()?;
        let table_name = table_name.to_lowercase();
        let current_schema = self.get_table(&table_name)?.schema().clone();
        let expected_catalog_schema = self
            .pending_schema_changes
            .iter()
            .find(|pending| pending.table_name.eq_ignore_ascii_case(&table_name))
            .map(|pending| pending.expected_catalog_schema.clone())
            .unwrap_or(current_schema);
        let change = PendingSchemaChange {
            table_name,
            schema,
            expected_catalog_schema,
            requires_row_normalization: requires_row_normalization
                || self
                    .pending_schema_changes
                    .iter()
                    .any(|pending| pending.requires_row_normalization),
            physical_transition: Some(transition),
        };
        self.get_engine_ops()?
            .stage_schema_change(self.id, &change)?;
        self.pending_schema_changes.push(change);
        self.publish_ddl_owners();
        Ok(())
    }

    fn drop_table_column(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        self.check_active()?;
        Err(Error::NotSupported(format!(
            "ALTER TABLE '{table_name}' DROP COLUMN '{column_name}' is not supported inside an explicit transaction"
        )))
    }

    fn rename_table_column(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        self.check_active()?;
        Err(Error::NotSupported(format!(
            "ALTER TABLE '{table_name}' RENAME COLUMN '{old_name}' TO '{new_name}' is not supported inside an explicit transaction"
        )))
    }

    fn modify_table_column(&mut self, table_name: &str, column: SchemaColumn) -> Result<()> {
        self.check_active()?;
        Err(Error::NotSupported(format!(
            "ALTER TABLE '{table_name}' MODIFY COLUMN '{}' is not supported inside an explicit transaction",
            column.name
        )))
    }

    fn select(
        &self,
        table_name: &str,
        columns_to_fetch: &[String],
        expr: Option<&dyn Expression>,
        _original_columns: Option<&[String]>,
    ) -> Result<Box<dyn QueryResult>> {
        self.check_active()?;

        let table = self.get_table(table_name)?;
        let col_refs: Vec<&str> = columns_to_fetch.iter().map(|s| s.as_str()).collect();
        table.select(&col_refs, expr)
    }

    fn select_with_aliases(
        &self,
        table_name: &str,
        columns_to_fetch: &[String],
        expr: Option<&dyn Expression>,
        aliases: &FxHashMap<String, String>,
        _original_columns: Option<&[String]>,
    ) -> Result<Box<dyn QueryResult>> {
        self.check_active()?;

        let table = self.get_table(table_name)?;
        let col_refs: Vec<&str> = columns_to_fetch.iter().map(|s| s.as_str()).collect();
        table.select_with_aliases(&col_refs, expr, aliases)
    }

    fn select_as_of(
        &self,
        table_name: &str,
        columns_to_fetch: &[String],
        expr: Option<&dyn Expression>,
        temporal_type: &str,
        temporal_value: i64,
        _original_columns: Option<&[String]>,
    ) -> Result<Box<dyn QueryResult>> {
        self.check_active()?;

        let table = self.get_table(table_name)?;
        let col_refs: Vec<&str> = columns_to_fetch.iter().map(|s| s.as_str()).collect();
        table.select_as_of(&col_refs, expr, temporal_type, temporal_value)
    }
}

impl std::fmt::Debug for MvccTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvccTransaction")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("begin_seq", &self.begin_seq)
            .finish()
    }
}

// Ensure transaction is rolled back on drop if still active
impl Drop for MvccTransaction {
    fn drop(&mut self) {
        if self.state == TransactionState::Active {
            // Silent rollback on drop
            self.registry.abort_transaction(self.id);

            if let Some(ops) = &self.engine_operations {
                // Roll back DDL operations (CREATE TABLE / DROP TABLE)
                if let Err(error) = self.rollback_ddl(ops.as_ref()) {
                    eprintln!(
                        "transaction {} drop-time DDL rollback failed: {}",
                        self.id, error
                    );
                }

                // Clean up txn_version_stores to prevent memory leak
                // This is critical for read-only transactions that call get_table()
                // but are dropped without explicit commit/rollback
                ops.rollback_all_tables(self.id);
            }

            self.cleanup();
        }
        self.clear_ddl_owner_publication();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_creation() {
        let registry = Arc::new(TransactionRegistry::new());
        let (txn_id, begin_seq) = registry.begin_transaction();
        let txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&registry));

        assert_eq!(txn.id(), txn_id);
        assert_eq!(txn.begin_seq(), begin_seq);
        assert_eq!(txn.state(), TransactionState::Active);
    }

    #[test]
    fn ddl_owner_publication_tracks_private_indexes_and_constraints() {
        let registry = Arc::new(TransactionRegistry::new());
        let (txn_id, begin_seq) = registry.begin_transaction();
        let mut txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&registry));
        txn.created_tables.push("new_table".into());
        txn.pending_indexes.push(PendingIndexDefinition {
            table_name: "new_table".into(),
            index_name: "new_table_value".into(),
            columns: vec!["value".into()],
            is_unique: false,
            index_type: None,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
            hnsw_distance_metric: None,
            partial_predicate: None,
            key_encoder: None,
        });
        let before = radixdb_core::SchemaBuilder::new("new_table")
            .column("id", radixdb_core::DataType::Integer, false, false)
            .build();
        let after = radixdb_core::SchemaBuilder::new("new_table")
            .column("id", radixdb_core::DataType::Integer, false, true)
            .build();
        txn.pending_schema_changes.push(PendingSchemaChange {
            table_name: "new_table".into(),
            schema: after,
            expected_catalog_schema: before,
            requires_row_normalization: false,
            physical_transition: None,
        });

        txn.publish_ddl_owners();
        assert_eq!(
            txn.published_ddl_owners,
            PublishedDdlOwners {
                tables: 1,
                indexes: 1,
                index_drops: 0,
                schema_changes: 1,
                constraint_changes: 1,
            }
        );

        registry.abort_transaction(txn_id);
        txn.state = TransactionState::RolledBack;
        txn.cleanup();
        assert_eq!(txn.published_ddl_owners, PublishedDdlOwners::default());
    }

    #[test]
    fn test_transaction_state_transitions() {
        let registry = Arc::new(TransactionRegistry::new());
        let (txn_id, begin_seq) = registry.begin_transaction();
        let mut txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&registry));

        assert_eq!(txn.state(), TransactionState::Active);

        // Begin should be no-op
        txn.begin().unwrap();
        assert_eq!(txn.state(), TransactionState::Active);

        // Commit
        txn.commit().unwrap();
        assert_eq!(txn.state(), TransactionState::Committed);

        // Should fail to begin after commit
        assert!(txn.begin().is_err());
    }

    #[test]
    fn test_transaction_rollback() {
        let registry = Arc::new(TransactionRegistry::new());
        let (txn_id, begin_seq) = registry.begin_transaction();
        let mut txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&registry));

        assert_eq!(txn.state(), TransactionState::Active);

        // Rollback
        txn.rollback().unwrap();
        assert_eq!(txn.state(), TransactionState::RolledBack);

        // Should fail to begin after rollback
        assert!(txn.begin().is_err());
    }

    #[test]
    fn test_transaction_isolation_level() {
        let registry = Arc::new(TransactionRegistry::new());
        let (txn_id, begin_seq) =
            registry.begin_transaction_with_isolation(IsolationLevel::SnapshotIsolation);
        let mut txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&registry));

        assert_eq!(txn.get_isolation_level(), IsolationLevel::SnapshotIsolation);
        // Confirming the captured level is idempotent.
        txn.set_isolation_level(IsolationLevel::SnapshotIsolation)
            .unwrap();
        assert_eq!(txn.get_isolation_level(), IsolationLevel::SnapshotIsolation);
        // Changing the snapshot contract after begin is forbidden.
        assert!(txn
            .set_isolation_level(IsolationLevel::ReadCommitted)
            .is_err());
    }

    #[test]
    fn test_transaction_double_commit() {
        let registry = Arc::new(TransactionRegistry::new());
        let (txn_id, begin_seq) = registry.begin_transaction();
        let mut txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&registry));

        // First commit should succeed
        txn.commit().unwrap();

        // Second commit should fail
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_transaction_commit_after_rollback() {
        let registry = Arc::new(TransactionRegistry::new());
        let (txn_id, begin_seq) = registry.begin_transaction();
        let mut txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&registry));

        // Rollback first
        txn.rollback().unwrap();

        // Commit should fail
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_transaction_debug() {
        let registry = Arc::new(TransactionRegistry::new());
        let (txn_id, begin_seq) = registry.begin_transaction();
        let txn = MvccTransaction::new(txn_id, begin_seq, Arc::clone(&registry));

        let debug_str = format!("{:?}", txn);
        assert!(debug_str.contains("MvccTransaction"));
        assert!(debug_str.contains("Active"));
    }
}
