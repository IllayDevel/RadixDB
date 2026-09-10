use super::*;

impl WALManager {
    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::Acquire)
    }

    /// Append a WAL entry
    pub fn append_entry(&self, mut entry: WALEntry) -> Result<u64> {
        #[cfg(any(test, feature = "test-hooks"))]
        let append_test_hook = {
            self.append_test_hook
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        };
        #[cfg(any(test, feature = "test-hooks"))]
        if let Some(hook) = append_test_hook {
            hook(&entry);
        }

        let mut transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;
        if entry.txn_id == i64::MAX {
            return Err(Error::internal(
                "WAL transaction ID exhausts the recoverable domain",
            ));
        }
        if !crate::timestamp::observe_persisted_timestamp(entry.timestamp) {
            return Err(Error::internal(
                "WAL timestamp exhausts the MVCC timestamp domain",
            ));
        }

        let current = self.current_lsn.load(Ordering::Acquire);
        let next_lsn = current.checked_add(1).ok_or_else(|| {
            Error::internal(
                "WAL LSN overflow: maximum sequence number reached. Database requires maintenance.",
            )
        })?;
        entry.previous_lsn = self.previous_lsn.load(Ordering::Acquire);
        entry.lsn = next_lsn;

        let prepared = PreparedWalEntry::encode(&entry)?;
        self.append_prepared_entry(&mut transition, prepared)
    }

    /// Append one transaction's already-materialized DML records while owning
    /// the WAL transition exactly once. LSN assignment stays serial and ordered;
    /// independent framing/compression/CRC work may use the shared database CPU
    /// lease. Durable bytes are still admitted through the ordinary per-record
    /// path, so partial-write and uncertainty semantics remain unchanged.
    pub(crate) fn append_entries(
        &self,
        mut entries: Vec<WALEntry>,
        cpu: &StorageCpuLease,
    ) -> Result<Vec<u64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Hooks deliberately retain the historical one-entry interleave
        // contract. Production has no hook and uses the bounded batch path.
        #[cfg(any(test, feature = "test-hooks"))]
        if self
            .append_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
        {
            return entries
                .into_iter()
                .map(|entry| self.append_entry(entry))
                .collect();
        }

        let mut transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;
        let base_lsn = self.current_lsn.load(Ordering::Acquire);
        let mut previous_lsn = self.previous_lsn.load(Ordering::Acquire);
        for (index, entry) in entries.iter_mut().enumerate() {
            if entry.txn_id == i64::MAX {
                return Err(Error::internal(
                    "WAL transaction ID exhausts the recoverable domain",
                ));
            }
            if !crate::timestamp::observe_persisted_timestamp(entry.timestamp) {
                return Err(Error::internal(
                    "WAL timestamp exhausts the MVCC timestamp domain",
                ));
            }
            let offset = u64::try_from(index)
                .map_err(|_| Error::internal("WAL batch entry count exceeds u64"))?
                .checked_add(1)
                .ok_or_else(|| Error::internal("WAL batch LSN offset overflow"))?;
            let lsn = base_lsn.checked_add(offset).ok_or_else(|| {
                Error::internal(
                    "WAL LSN overflow: maximum sequence number reached. Database requires maintenance.",
                )
            })?;
            entry.previous_lsn = previous_lsn;
            entry.lsn = lsn;
            previous_lsn = lsn;
        }

        let prepared = Self::prepare_entries(entries, cpu)?;
        let mut lsns = Vec::with_capacity(prepared.len());
        for entry in prepared {
            lsns.push(self.append_prepared_entry(&mut transition, entry)?);
        }
        Ok(lsns)
    }

    /// Append one typed catalog mutation followed by the transaction's shared
    /// commit marker while owning the WAL transition once. The catalog frame
    /// records the exact LSN of that marker; no caller observes or predicts an
    /// LSN outside the serialized WAL authority.
    pub(crate) fn write_catalog_commit(
        &self,
        txn_id: i64,
        successor_catalog_id: [u8; 16],
        created_unix_ns: u64,
        mutation: &radixdb_catalog::CatalogMutationSet,
    ) -> Result<u64> {
        #[cfg(any(test, feature = "test-hooks"))]
        let append_test_hook = self
            .append_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        #[cfg(any(test, feature = "test-hooks"))]
        if let Some(hook) = append_test_hook.as_ref() {
            // Hooks are an admission interleave and therefore run before the
            // transition mutex, matching append_entry(). Their contract is the
            // logical operation/transaction identity, not an assigned LSN.
            hook(&WALEntry::new(
                txn_id,
                None,
                0,
                WALOperationType::CatalogMutation,
                Vec::new(),
            ));
            hook(&WALEntry::commit_marker(txn_id));
        }

        let mut transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;
        if txn_id == i64::MAX {
            return Err(Error::internal(
                "WAL transaction ID exhausts the recoverable domain",
            ));
        }

        let base_lsn = self.current_lsn.load(Ordering::Acquire);
        let mutation_lsn = base_lsn
            .checked_add(1)
            .ok_or_else(|| Error::internal("WAL LSN overflow before catalog mutation"))?;
        let commit_lsn = mutation_lsn
            .checked_add(1)
            .ok_or_else(|| Error::internal("WAL LSN overflow before commit marker"))?;

        let mut transaction_identity = [0_u8; 16];
        transaction_identity[..8].copy_from_slice(&txn_id.to_le_bytes());
        transaction_identity[8..].copy_from_slice(&commit_lsn.to_le_bytes());
        let transaction_identity = crate::v6::CatalogWalTransactionId::from_bytes(
            transaction_identity,
        )
        .map_err(|error| Error::internal(format!("catalog WAL identity rejected: {error}")))?;
        let catalog_transaction = crate::v6::CatalogWalTransaction::new(
            transaction_identity,
            successor_catalog_id,
            commit_lsn,
            created_unix_ns,
            mutation.clone(),
        )
        .map_err(|error| Error::internal(format!("catalog WAL transaction rejected: {error}")))?;
        let catalog_payload = crate::v6::encode_catalog_wal_transaction(&catalog_transaction)
            .map_err(|error| Error::internal(format!("catalog WAL encoding failed: {error}")))?;

        let mut catalog_entry = WALEntry::new(
            txn_id,
            None,
            0,
            WALOperationType::CatalogMutation,
            catalog_payload,
        );
        catalog_entry.lsn = mutation_lsn;
        catalog_entry.previous_lsn = self.previous_lsn.load(Ordering::Acquire);
        let mut commit_entry = WALEntry::commit_marker(txn_id);
        commit_entry.lsn = commit_lsn;
        commit_entry.previous_lsn = mutation_lsn;
        if !crate::timestamp::observe_persisted_timestamp(catalog_entry.timestamp)
            || !crate::timestamp::observe_persisted_timestamp(commit_entry.timestamp)
        {
            return Err(Error::internal(
                "catalog commit exhausted the MVCC timestamp domain",
            ));
        }

        let catalog_entry = PreparedWalEntry::encode(&catalog_entry)?;
        let commit_entry = PreparedWalEntry::encode(&commit_entry)?;
        self.append_prepared_entry(&mut transition, catalog_entry)?;
        self.append_prepared_entry(&mut transition, commit_entry)?;
        Ok(commit_lsn)
    }

    pub(super) fn prepare_entries(
        entries: Vec<WALEntry>,
        cpu: &StorageCpuLease,
    ) -> Result<Vec<PreparedWalEntry>> {
        #[cfg(feature = "parallel")]
        if cpu.workers() > 1 && entries.len() > 1 {
            let partition_len = entries.len().div_ceil(cpu.workers());
            return entries
                .par_chunks(partition_len)
                .map(|partition| {
                    let _activity = cpu.activate();
                    partition
                        .iter()
                        .map(PreparedWalEntry::encode)
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()
                .map(|partitions| partitions.into_iter().flatten().collect());
        }

        let _activity = cpu.activate();
        entries.iter().map(PreparedWalEntry::encode).collect()
    }

    pub(super) fn append_prepared_entry(
        &self,
        transition: &mut WalTransitionState,
        prepared: PreparedWalEntry,
    ) -> Result<u64> {
        let PreparedWalEntry {
            lsn,
            txn_id,
            operation,
            encoded,
        } = prepared;
        let encoded_len = encoded.len() as u64;

        let (needs_write, buffer_data, current_record_start) = {
            let mut buffer = self.buffer.lock().unwrap();
            let current_record_start = buffer.len();
            buffer.extend_from_slice(&encoded);

            let needs_flush = buffer.len() >= self.flush_trigger as usize;
            let force_flush = self.sync_mode == SyncMode::Full
                || (self.sync_mode == SyncMode::Normal
                    && (operation.is_transaction_end() || operation.is_ddl()));

            if needs_flush || force_flush {
                (true, std::mem::take(&mut *buffer), current_record_start)
            } else {
                (false, Vec::new(), current_record_start)
            }
        };

        if needs_write {
            let write_result = self.write_to_file(&buffer_data);

            match write_result {
                Ok(()) => {
                    self.current_file_position
                        .fetch_add(buffer_data.len() as u64, Ordering::Relaxed);
                    self.publish_admitted_fields(lsn, txn_id);

                    if self.should_sync(operation) {
                        if let Err(error) = self.sync_locked() {
                            instrumentation::record_wal_append_count(1, encoded_len);
                            return Err(self.mark_transition_uncertain(
                                transition,
                                format!(
                                    "entry LSN {} was written but fsync failed: {}",
                                    lsn, error
                                ),
                            ));
                        }
                    }
                }
                Err(failure) => {
                    self.current_file_position
                        .fetch_add(failure.written as u64, Ordering::Relaxed);

                    if failure.written <= current_record_start {
                        self.prepend_to_buffer(&buffer_data[failure.written..current_record_start]);
                        return Err(failure.error);
                    }

                    self.prepend_to_buffer(&buffer_data[failure.written..]);
                    self.publish_admitted_fields(lsn, txn_id);
                    instrumentation::record_wal_append_count(1, encoded_len);
                    return Err(self.mark_transition_uncertain(
                        transition,
                        format!(
                            "entry LSN {} was partially written ({} of {} bytes in the shared batch): {}",
                            lsn,
                            failure.written,
                            buffer_data.len(),
                            failure.error
                        ),
                    ));
                }
            }
        } else {
            self.publish_admitted_fields(lsn, txn_id);
        }

        // Track appended entries/bytes without timing every WAL append. The
        // hot path can execute once per inserted row, so detailed timing belongs
        // to the lower-frequency write/sync counters.
        instrumentation::record_wal_append_count(1, encoded_len);

        Ok(lsn)
    }

    pub(super) fn require_running_transition(transition: &WalTransitionState) -> Result<()> {
        match transition.lifecycle {
            WalLifecycle::Running => Ok(()),
            WalLifecycle::Failed => Err(Error::WalDurabilityUncertain {
                detail: "WAL is in a terminal failed state".to_string(),
            }),
            WalLifecycle::Closing | WalLifecycle::Closed => Err(Error::WalNotRunning),
        }
    }

    pub(super) fn mark_transition_uncertain(
        &self,
        transition: &mut WalTransitionState,
        detail: String,
    ) -> Error {
        transition.lifecycle = WalLifecycle::Failed;
        self.running.store(false, Ordering::Release);
        Error::WalDurabilityUncertain { detail }
    }

    pub(super) fn publish_admitted_fields(&self, lsn: u64, txn_id: i64) {
        self.current_lsn.store(lsn, Ordering::Release);
        self.previous_lsn.store(lsn, Ordering::Release);
        if txn_id > 0 {
            self.transaction_high_water
                .fetch_max(txn_id, Ordering::AcqRel);
        }
    }

    pub(crate) fn transaction_high_water(&self) -> i64 {
        self.transaction_high_water.load(Ordering::Acquire)
    }

    pub(super) fn prepend_to_buffer(&self, prefix: &[u8]) {
        if prefix.is_empty() {
            return;
        }
        let mut buffer = self.buffer.lock().unwrap();
        let existing = std::mem::take(&mut *buffer);
        buffer.reserve(prefix.len() + existing.len());
        buffer.extend_from_slice(prefix);
        buffer.extend_from_slice(&existing);
    }

    /// Get previous LSN (last written entry's LSN)
    pub fn previous_lsn(&self) -> u64 {
        self.previous_lsn.load(Ordering::Acquire)
    }

    /// Write a commit marker for two-phase recovery
    pub fn write_commit_marker(&self, txn_id: i64) -> Result<u64> {
        let entry = WALEntry::commit_marker(txn_id);
        self.append_entry(entry)
    }

    /// Write an abort marker for two-phase recovery
    pub fn write_abort_marker(&self, txn_id: i64) -> Result<u64> {
        let entry = WALEntry::abort_marker(txn_id);
        self.append_entry(entry)
    }

    /// Write data to WAL file
    #[allow(clippy::result_large_err)]
    pub(super) fn write_to_file(&self, data: &[u8]) -> std::result::Result<(), WalWriteFailure> {
        if data.is_empty() {
            return Ok(());
        }
        let started = Instant::now();

        #[cfg(any(test, feature = "test-failpoints"))]
        if crate::test_failpoints::WAL_WRITE_FAIL.load(std::sync::atomic::Ordering::Acquire) {
            return Err(WalWriteFailure {
                error: Error::internal("failpoint: WAL write"),
                written: 0,
            });
        }
        #[cfg(any(test, feature = "test-failpoints"))]
        if crate::test_failpoints::FILESYSTEM_FULL_FAIL.load(std::sync::atomic::Ordering::Acquire) {
            return Err(WalWriteFailure {
                error: Error::internal("failpoint: filesystem capacity exhausted (ENOSPC)"),
                written: 0,
            });
        }

        let mut wal_file = self.wal_file.lock().unwrap();
        let file = wal_file.as_mut().ok_or(WalWriteFailure {
            error: Error::WalFileClosed,
            written: 0,
        })?;
        let mut written = 0usize;
        while written < data.len() {
            match file.write(&data[written..]) {
                Ok(0) => {
                    return Err(WalWriteFailure {
                        error: Error::internal("failed to write to WAL: write returned zero bytes"),
                        written,
                    });
                }
                Ok(count) => written += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    if written > 0 {
                        instrumentation::record_wal_write(written as u64, started.elapsed());
                    }
                    return Err(WalWriteFailure {
                        error: Error::internal(format!("failed to write to WAL: {}", error)),
                        written,
                    });
                }
            }
        }
        instrumentation::record_wal_write(written as u64, started.elapsed());

        Ok(())
    }

    /// Sync WAL to disk (assumes lock is held)
    pub(super) fn sync_locked(&self) -> Result<()> {
        let started = Instant::now();

        #[cfg(any(test, feature = "test-failpoints"))]
        if crate::test_failpoints::WAL_SYNC_FAIL.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::internal("failpoint: WAL sync"));
        }

        let wal_file = self.wal_file.lock().unwrap();
        let file = wal_file.as_ref().ok_or(Error::WalFileClosed)?;
        file.sync_all()
            .map_err(|e| Error::internal(format!("failed to sync WAL: {}", e)))?;
        instrumentation::record_wal_sync(started.elapsed());

        self.last_synced_file_position.store(
            self.current_file_position.load(Ordering::Acquire),
            Ordering::Release,
        );

        let elapsed = self
            .sync_clock_origin
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        self.last_sync_elapsed_nanos
            .store(elapsed, Ordering::Relaxed);

        Ok(())
    }
}
