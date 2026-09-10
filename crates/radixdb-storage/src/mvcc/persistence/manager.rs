use super::*;

/// Persistence metadata for tracking state
#[derive(Debug, Default)]
pub struct PersistenceMeta {
    /// Last checkpoint time (Unix nanoseconds)
    pub last_checkpoint_time: AtomicI64,
}

/// Persistence Manager coordinates all disk operations
pub struct PersistenceManager {
    /// Base path for persistence files
    path: PathBuf,
    /// WAL manager
    wal: Option<WALManager>,
    /// Persistence metadata
    meta: PersistenceMeta,
    /// Runtime checkpoint scheduling is monotonic even when wall time moves.
    last_checkpoint_monotonic: Mutex<Instant>,
    /// Start time of the last scheduled checkpoint attempt.
    ///
    /// This is deliberately independent from `last_checkpoint_monotonic`.
    /// A scheduled cycle may safely defer WAL-floor publication when commits
    /// keep the hot set non-empty; that outcome must still consume one cadence
    /// slot instead of retriggering maintenance on every coordinator tick.
    last_checkpoint_attempt_monotonic: Mutex<Instant>,
    /// Whether persistence is enabled
    enabled: AtomicBool,
    /// Next identity for one auto-commit DDL entry + commit-marker pair.
    ///
    /// Internal IDs descend below the reserved DDL identity. The restart seed also incorporates the
    /// validated WAL LSN, so no identity emitted before restart can be reused.
    next_auto_ddl_txn_id: AtomicI64,
    /// Checkpoint interval
    checkpoint_interval: Duration,
    /// Number of snapshots to keep
    keep_count: usize,
    /// Running flag for background tasks
    running: AtomicBool,
}

impl PersistenceManager {
    const FIRST_AUTO_DDL_TXN_ID: i64 = DDL_TXN_ID - 1;

    fn initial_auto_ddl_txn_id(wal_lsn: u64) -> Result<i64> {
        let wal_lsn = i64::try_from(wal_lsn).map_err(|_| {
            Error::internal(format!(
                "WAL LSN {wal_lsn} exhausted the auto-DDL transaction identity space"
            ))
        })?;
        Self::FIRST_AUTO_DDL_TXN_ID
            .checked_sub(wal_lsn)
            .ok_or_else(|| Error::internal("auto-DDL transaction identity space exhausted"))
    }

    fn allocate_auto_ddl_txn_id(&self) -> Result<i64> {
        self.next_auto_ddl_txn_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(1)
            })
            .map_err(|_| Error::internal("auto-DDL transaction identity space exhausted"))
    }

    /// Create a new persistence manager
    #[doc(hidden)]
    pub fn new(path: Option<&Path>, config: &PersistenceConfig) -> Result<Self> {
        let replay_floor = crate::v6::WalReplayFloor::new(
            crate::v6::WalGeneration::new(1).map_err(|error| Error::internal(error.to_string()))?,
            0,
        );
        Self::new_with_replay_floor(path, config, replay_floor)
    }

    pub(crate) fn new_with_replay_floor(
        path: Option<&Path>,
        config: &PersistenceConfig,
        replay_floor: crate::v6::WalReplayFloor,
    ) -> Result<Self> {
        // Memory-only mode if no path provided
        if path.is_none() || !config.enabled {
            return Ok(Self {
                path: PathBuf::new(),
                wal: None,
                meta: PersistenceMeta::default(),
                last_checkpoint_monotonic: Mutex::new(Instant::now()),
                last_checkpoint_attempt_monotonic: Mutex::new(Instant::now()),
                enabled: AtomicBool::new(false),
                next_auto_ddl_txn_id: AtomicI64::new(Self::FIRST_AUTO_DDL_TXN_ID),
                checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
                keep_count: DEFAULT_KEEP_SNAPSHOTS,
                running: AtomicBool::new(false),
            });
        }

        let path = path.unwrap();

        // Create base directory
        fs::create_dir_all(path).map_err(|e| {
            Error::internal(format!("failed to create persistence directory: {}", e))
        })?;

        // Initialize WAL with config (including fast sync settings)
        let wal_path = path.join("wal");
        let wal =
            WALManager::with_replay_floor(&wal_path, config.sync_mode, Some(config), replay_floor)?;
        let next_auto_ddl_txn_id = Self::initial_auto_ddl_txn_id(wal.current_lsn())?;

        // Configure intervals
        let checkpoint_interval = if config.checkpoint_interval > 0 {
            Duration::from_secs(config.checkpoint_interval as u64)
        } else {
            DEFAULT_CHECKPOINT_INTERVAL
        };

        let keep_count = if config.keep_snapshots > 0 {
            config.keep_snapshots as usize
        } else {
            DEFAULT_KEEP_SNAPSHOTS
        };

        Ok(Self {
            path: path.to_path_buf(),
            wal: Some(wal),
            meta: PersistenceMeta::default(),
            last_checkpoint_monotonic: Mutex::new(Instant::now()),
            last_checkpoint_attempt_monotonic: Mutex::new(Instant::now()),
            enabled: AtomicBool::new(true),
            next_auto_ddl_txn_id: AtomicI64::new(next_auto_ddl_txn_id),
            checkpoint_interval,
            keep_count,
            running: AtomicBool::new(false),
        })
    }

    pub(crate) fn read_catalog_transactions_at(
        path: &Path,
        config: &PersistenceConfig,
        replay_floor: crate::v6::WalReplayFloor,
        byte_budget: u64,
    ) -> Result<Vec<u8>> {
        let wal = WALManager::with_replay_floor(
            path.join("wal"),
            config.sync_mode,
            Some(config),
            replay_floor,
        )?;
        wal.read_catalog_transactions(replay_floor.lsn(), byte_budget)
    }

    /// Check if persistence is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Check whether the persistence facade and its WAL still admit work.
    fn is_running(&self) -> bool {
        !self.is_enabled()
            || (self.running.load(Ordering::Acquire)
                && self.wal.as_ref().is_some_and(|wal| wal.is_running()))
    }

    fn ensure_running(&self) -> Result<()> {
        if !self.is_enabled() || self.is_running() {
            Ok(())
        } else {
            Err(Error::WalNotRunning)
        }
    }

    fn ensure_manager_running(&self) -> Result<()> {
        if !self.is_enabled() || self.running.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Error::WalNotRunning)
        }
    }

    /// Installs an append hook on this persistence instance only.
    ///
    /// Transaction IDs are engine-local, so a process-global test hook can
    /// accidentally match an unrelated database running in parallel.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn install_wal_append_test_hook(
        &self,
        hook: WalAppendTestHook,
    ) -> Result<WalAppendTestHookGuard<'_>> {
        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;
        Ok(WalAppendTestHookGuard::install(wal, hook))
    }

    /// Start persistence operations
    pub fn start(&self) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;
        if !wal.is_running() {
            return Err(Error::WalNotRunning);
        }

        self.running.store(true, Ordering::Release);
        Ok(())
    }

    /// Stop persistence operations
    pub fn stop(&self) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        self.running.store(false, Ordering::Release);

        // Close WAL
        if let Some(ref wal) = self.wal {
            wal.close()?;
        }

        Ok(())
    }

    /// Record one auto-committed operation whose target is a stable catalog
    /// table identity. This is used for table-scoped state transitions such
    /// as TRUNCATE: unlike logical DDL they must survive a metadata-only table
    /// rename without addressing the row state by its mutable name.
    pub fn record_table_operation(
        &self,
        table_id: radixdb_catalog::ObjectId,
        op: WALOperationType,
        data: &[u8],
    ) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.ensure_running()?;

        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;
        let txn_id = self.allocate_auto_ddl_txn_id()?;
        wal.append_entry(WALEntry::new(txn_id, Some(table_id), 0, op, data.to_vec()))?;
        wal.write_commit_marker(txn_id)?;
        Self::handle_rotation_result(wal.maybe_rotate())?;
        Ok(())
    }

    /// Record a DML operation (INSERT, UPDATE, DELETE)
    pub fn record_dml_operation(
        &self,
        txn_id: i64,
        table_id: radixdb_catalog::ObjectId,
        row_id: i64,
        op: WALOperationType,
        version: &RowVersion,
    ) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.ensure_running()?;

        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;

        // DELETE recovery needs only the durable envelope (transaction, table
        // and row identity). The transaction-local old row remains available
        // to index maintenance and rollback, but copying it into WAL made hot
        // wide deletes proportional to row width even though REDO ignores the
        // payload. INSERT/UPDATE retain the full RowVersion encoding.
        let data = if op == WALOperationType::Delete {
            Vec::new()
        } else {
            serialize_row_version(version)?
        };

        let mut entry = WALEntry::new(txn_id, Some(table_id), row_id, op, data);
        entry.timestamp = version.create_time;

        wal.append_entry(entry)?;
        Ok(())
    }

    /// Record the complete DML portion of one transaction as an ordered WAL
    /// batch. Row serialization and WAL framing are independent per row and
    /// use the database-local CPU budget; LSN assignment and durable admission
    /// remain serial in WAL order.
    #[doc(hidden)]
    pub fn record_dml_operations(
        &self,
        txn_id: i64,
        operations: Vec<PendingDmlWalOperation>,
        cpu_runtime: &Arc<StorageCpuRuntime>,
    ) -> Result<()> {
        if operations.is_empty() || !self.is_enabled() {
            return Ok(());
        }
        self.ensure_running()?;
        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;
        let cpu = cpu_runtime.acquire(operations.len());

        #[cfg(feature = "parallel")]
        let entries = if cpu.workers() > 1 && operations.len() > 1 {
            // One ordered partition per admitted worker avoids scheduling and
            // instrumentation traffic per row. The partitions are indexed, so
            // flattening their results preserves the transaction's WAL order.
            let partition_len = operations.len().div_ceil(cpu.workers());
            operations
                .par_chunks(partition_len)
                .map(|partition| {
                    let _activity = cpu.activate();
                    partition
                        .iter()
                        .map(|operation| operation.to_wal_entry(txn_id))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect()
        } else {
            let _activity = cpu.activate();
            operations
                .iter()
                .map(|operation| operation.to_wal_entry(txn_id))
                .collect::<Result<Vec<_>>>()?
        };

        #[cfg(not(feature = "parallel"))]
        let entries = {
            let _activity = cpu.activate();
            operations
                .iter()
                .map(|operation| operation.to_wal_entry(txn_id))
                .collect::<Result<Vec<_>>>()?
        };

        wal.append_entries(entries, &cpu)?;
        Ok(())
    }

    /// Record a transaction commit
    ///
    /// Uses commit_marker() which sets the COMMIT_MARKER flag for two-phase recovery
    pub fn record_commit(&self, txn_id: i64) -> Result<u64> {
        if !self.is_enabled() {
            return Ok(0);
        }
        self.ensure_running()?;

        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;

        // Use commit_marker to set COMMIT_MARKER flag for two-phase recovery
        let commit_lsn = wal.write_commit_marker(txn_id)?;

        Self::handle_rotation_result(wal.maybe_rotate())?;

        Ok(commit_lsn)
    }

    /// Record the transaction's typed catalog mutation and its shared commit
    /// marker in the same WAL transition. DML entries, when present, have
    /// already been appended under `txn_id`; the marker commits both streams.
    pub fn record_catalog_commit(
        &self,
        txn_id: i64,
        successor_catalog_id: [u8; 16],
        created_unix_ns: u64,
        mutation: &radixdb_catalog::CatalogMutationSet,
    ) -> Result<u64> {
        if !self.is_enabled() {
            return Ok(0);
        }
        self.ensure_running()?;

        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;
        let commit_lsn =
            wal.write_catalog_commit(txn_id, successor_catalog_id, created_unix_ns, mutation)?;
        Self::handle_rotation_result(wal.maybe_rotate())?;
        Ok(commit_lsn)
    }

    /// Record a transaction rollback
    ///
    /// Uses abort_marker() which sets the ABORT_MARKER flag for two-phase recovery
    pub fn record_rollback(&self, txn_id: i64) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.ensure_running()?;

        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;

        // Use abort_marker to set ABORT_MARKER flag for two-phase recovery
        wal.write_abort_marker(txn_id)?;

        Self::handle_rotation_result(wal.maybe_rotate())?;

        Ok(())
    }

    /// Replay WAL entries using two-phase recovery
    ///
    /// This method ensures crash consistency by:
    /// 1. Scanning to identify committed/aborted transactions
    /// 2. Only applying entries from committed transactions
    pub fn replay_two_phase<F>(
        &self,
        from_lsn: u64,
        callback: F,
    ) -> Result<crate::mvcc::wal_manager::TwoPhaseRecoveryInfo>
    where
        F: FnMut(crate::mvcc::wal_manager::WALEntry) -> Result<()>,
    {
        self.ensure_running()?;
        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;

        wal.replay_two_phase(from_lsn, callback)
    }

    /// Read committed catalog transactions from the same WAL files used by
    /// ordinary DML recovery, preserving their commit order and byte bound.
    pub fn read_catalog_transactions(&self, from_lsn: u64, byte_budget: u64) -> Result<Vec<u8>> {
        self.ensure_running()?;
        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;
        wal.read_catalog_transactions(from_lsn, byte_budget)
    }

    #[doc(hidden)]
    pub fn replay_two_phase_with_outcome_observer<F, O>(
        &self,
        from_lsn: u64,
        callback: F,
        observe_uncommitted: O,
    ) -> Result<crate::mvcc::wal_manager::TwoPhaseRecoveryInfo>
    where
        F: FnMut(crate::mvcc::wal_manager::WALEntry) -> Result<()>,
        O: FnMut(i64) -> Result<()>,
    {
        self.ensure_running()?;
        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;
        wal.replay_two_phase_with_outcome_observer(from_lsn, callback, observe_uncommitted)
    }

    /// Create a checkpoint and return the LSN at the checkpoint point
    ///
    /// Returns the LSN that represents the checkpoint point. All data up to
    /// this LSN is guaranteed to be durably written to disk when this returns.
    /// Returns 0 if persistence is not enabled.
    pub fn create_checkpoint(&self) -> Result<u64> {
        if !self.is_enabled() {
            return Ok(0);
        }
        self.ensure_manager_running()?;

        let wal = self.wal.as_ref().ok_or(Error::WalNotInitialized)?;

        let checkpoint_lsn = wal.create_checkpoint()?;

        // Update last checkpoint time
        let now = system_time_now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        self.meta.last_checkpoint_time.store(now, Ordering::Release);
        let now_monotonic = Instant::now();
        *self
            .last_checkpoint_monotonic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = now_monotonic;
        *self
            .last_checkpoint_attempt_monotonic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = now_monotonic;

        Ok(checkpoint_lsn)
    }

    /// Get the persistence path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the current WAL LSN
    pub fn current_lsn(&self) -> u64 {
        self.wal.as_ref().map(|w| w.current_lsn()).unwrap_or(0)
    }

    /// Lock-free WAL ownership used by the bounded runtime diagnostics.
    #[doc(hidden)]
    pub fn wal_runtime_state(&self) -> (u64, u64, bool, Option<u64>) {
        self.wal.as_ref().map_or((0, 0, false, Some(0)), |wal| {
            (
                wal.current_file_size(),
                wal.max_file_size(),
                wal.is_running(),
                wal.pending_durability_bytes(),
            )
        })
    }

    pub(crate) fn transaction_high_water(&self) -> i64 {
        self.wal
            .as_ref()
            .map_or(0, WALManager::transaction_high_water)
    }

    #[doc(hidden)]
    pub fn prepare_checkpoint_retention(&self, up_to_lsn: u64) -> Result<crate::v6::WalGeneration> {
        self.ensure_running()?;
        self.wal
            .as_ref()
            .ok_or(Error::WalNotInitialized)?
            .prepare_checkpoint_retention(up_to_lsn)
    }

    pub(crate) fn checkpoint_retirement_candidates(
        &self,
        retained_floor: crate::v6::WalGeneration,
    ) -> Result<Vec<crate::v6::WalGeneration>> {
        self.ensure_running()?;
        self.wal
            .as_ref()
            .ok_or(Error::WalNotInitialized)?
            .checkpoint_retirement_candidates(retained_floor)
    }

    pub(crate) fn freeze_snapshot_generations(
        &self,
        floor: crate::v6::WalReplayFloor,
    ) -> Result<Vec<crate::mvcc::wal_manager::SnapshotWalGeneration>> {
        self.ensure_running()?;
        self.wal
            .as_ref()
            .ok_or(Error::WalNotInitialized)?
            .freeze_snapshot_generations(floor)
    }

    pub(crate) fn confirm_checkpoint_publication(
        &self,
        floor: crate::v6::WalReplayFloor,
        retired: &[crate::v6::WalGeneration],
    ) {
        if let Some(wal) = &self.wal {
            wal.confirm_checkpoint_publication(floor, retired);
        }
    }

    fn handle_rotation_result(result: Result<bool>) -> Result<()> {
        match result {
            Ok(_) => Ok(()),
            Err(error @ Error::WalDurabilityUncertain { .. }) => Err(error),
            // Before metadata publication, rotation failure leaves the old
            // generation fully owned and the transaction marker durable.
            Err(_) => Ok(()),
        }
    }

    /// Get the checkpoint interval
    pub fn checkpoint_interval(&self) -> Duration {
        self.checkpoint_interval
    }

    /// Get the last checkpoint time in Unix nanoseconds
    pub fn last_checkpoint_time(&self) -> i64 {
        self.meta.last_checkpoint_time.load(Ordering::Acquire)
    }

    /// Runtime duration since the last successful checkpoint. This is never
    /// derived from wall time and therefore survives clock rollback safely.
    pub fn checkpoint_elapsed(&self) -> Duration {
        self.last_checkpoint_monotonic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .elapsed()
    }

    /// Atomically claim one scheduled checkpoint cadence slot.
    ///
    /// The claim is recorded before maintenance starts, so a cycle that
    /// deliberately defers durable checkpoint publication cannot create a
    /// tight retry loop. Manual checkpoints reset this clock when their WAL
    /// boundary is created.
    pub(crate) fn try_begin_scheduled_checkpoint(&self, interval: Duration) -> bool {
        let mut last_attempt = self
            .last_checkpoint_attempt_monotonic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if last_attempt.elapsed() < interval {
            return false;
        }
        *last_attempt = Instant::now();
        true
    }

    /// Get the number of snapshots to keep
    pub fn keep_count(&self) -> usize {
        self.keep_count
    }
}

impl Drop for PersistenceManager {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
