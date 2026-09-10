use super::*;

impl MVCCEngine {
    /// Closes the engine (inherent method)
    pub fn close_engine(&self) -> Result<()> {
        const SHUTDOWN_ACTIVE_TXN_DRAIN_TIMEOUT: std::time::Duration =
            std::time::Duration::from_secs(30);
        let _startup_guard = self
            .startup_mutex
            .lock()
            .map_err(|_| Error::LockAcquisitionFailed("engine lifecycle".to_string()))?;

        let retrying_close = matches!(
            &*self.lifecycle.read().unwrap(),
            EngineLifecycleState::Closing | EngineLifecycleState::CloseFailed(_)
        );
        if !retrying_close {
            // Use CAS to atomically close admission. A failed shutdown remains
            // `Closing`, so a later call retries the drain instead of claiming
            // that the engine is already closed.
            if self
                .open
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                if matches!(
                    &*self.lifecycle.read().unwrap(),
                    EngineLifecycleState::Closed
                ) {
                    return Ok(());
                }
                return Err(Error::internal(
                    "engine is not open and has no retryable close transition",
                ));
            }
            *self.lifecycle.write().unwrap() = EngineLifecycleState::Closing;
            self.shutdown_checkpoint_complete
                .store(false, Ordering::Release);
        } else {
            *self.lifecycle.write().unwrap() = EngineLifecycleState::Closing;
        }

        // Stop background warmup before final checkpoint can replace its
        // immutable generation inputs.
        {
            let mut warmup_handle = self.page_cache_warmup_handle.lock().unwrap();
            if let Some(mut handle) = warmup_handle.take() {
                if let Err(error) = handle.stop() {
                    return Err(self.record_close_failure(Error::internal(format!(
                        "page-cache warmup worker failed: {error}"
                    ))));
                }
            }
        }

        // Stop the cleanup thread first (before stopping transactions)
        {
            let mut cleanup_handle = self.cleanup_handle.lock().unwrap();
            if let Some(mut handle) = cleanup_handle.take() {
                if let Err(error) = handle.stop() {
                    return Err(self.record_close_failure(error));
                }
            }
        }

        // Stop accepting new transactions
        self.registry.stop_accepting_transactions();
        let remaining_active = self
            .registry
            .wait_for_active_transactions(SHUTDOWN_ACTIVE_TXN_DRAIN_TIMEOUT);
        if remaining_active > 0 {
            let error = Error::internal(format!(
                "close_engine timed out with {} active transaction(s) after {:?}; resources remain owned and shutdown may be retried",
                remaining_active, SHUTDOWN_ACTIVE_TXN_DRAIN_TIMEOUT
            ));
            return Err(self.record_close_failure(error));
        }

        // Run a final checkpoint to seal ALL remaining hot rows into volumes.
        // Use force_seal=true to bypass thresholds — on close, we want all data
        // in volumes so startup is fast and doesn't depend on WAL replay.
        // Skipped when checkpoint_on_close is false (crash simulation in tests).
        let checkpoint_on_close = self.config.read().unwrap().persistence.checkpoint_on_close;
        if !self.shutdown_checkpoint_complete.load(Ordering::Acquire) {
            if checkpoint_on_close {
                if let Some(pm) = self.persistence() {
                    if pm.is_enabled() {
                        // Retry checkpoint until all hot buffers are empty.
                        // After stop_accepting_transactions(), no new writes can start,
                        // but in-flight commits may still add rows between seal passes.
                        // Retry ensures WAL truncation happens and startup is fast.
                        let mut drained = false;
                        for attempt in 0..5 {
                            self.checkpoint_cycle_inner(true)
                                .map_err(|error| self.record_close_failure(error))?;
                            let pass_empty = self
                                .version_stores
                                .read()
                                .unwrap()
                                .values()
                                .all(|s| s.committed_row_count() == 0);
                            if pass_empty {
                                drained = true;
                                break;
                            }
                            if attempt < 4 {
                                std::thread::sleep(std::time::Duration::from_millis(10));
                            }
                        }
                        if !drained {
                            let error = Error::internal(
                                "final checkpoint did not drain all committed hot rows; resources remain owned and shutdown may be retried",
                            );
                            return Err(self.record_close_failure(error));
                        }
                        self.compact_after_checkpoint_forced()
                            .map_err(|error| self.record_close_failure(error))?;
                    }
                }
            } // checkpoint_on_close
            self.shutdown_checkpoint_complete
                .store(true, Ordering::Release);
        }

        // Wait for any background compaction to finish before releasing
        // resources. Without this, a detached compaction thread could still
        // be rewriting manifests and deleting volume files after close returns.
        while self.compaction_running.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Stop persistence before closing version stores. If WAL drain/fsync
        // fails, close_engine returns with stores and file lock still owned so
        // the same close transition can be retried safely.
        if let Some(pm) = self.persistence() {
            if pm.is_enabled() {
                pm.stop()
                    .map_err(|error| self.record_close_failure(error))?;
            }
        }

        // Close all version stores only after persistence is durably closed.
        let stores = self.version_stores.read().unwrap();
        for store in stores.values() {
            store.close();
        }
        drop(stores);

        // The publisher owns a clone of the same lock. Drop it before the
        // engine's final clone so close really releases writer ownership.
        // Revoke the shared publisher first: a retained internal Arc must not
        // keep mutation authority or the OS lock alive beyond close.
        if let Some(publisher) = self.physical_generation.load_full() {
            publisher.release_writer_lock();
        }
        self.physical_generation.store(None);

        // Release file lock (drops the final lock owner, allowing another open)
        {
            let mut file_lock = self.file_lock.lock().unwrap();
            *file_lock = None;
        }

        *self.lifecycle.write().unwrap() = EngineLifecycleState::Closed;

        Ok(())
    }

    /// Returns whether the engine is open
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Per-table, per-volume statistics for PRAGMA VOLUME_STATS.
    /// Returns table identity, total resident ownership, its disjoint
    /// components, idle cycles and tombstones.
    #[allow(clippy::type_complexity)]
    pub fn volume_stats(
        &self,
    ) -> Vec<(
        String,
        u64,
        &'static str,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        usize,
        u64,
        usize,
    )> {
        let mgrs = self.segment_managers.read().unwrap();
        let mut result = Vec::new();
        let mut table_names: Vec<&String> = mgrs.keys().collect();
        table_names.sort();
        for table_name in table_names {
            if let Some(mgr) = mgrs.get(table_name) {
                let tombstone_count = mgr.tombstone_count();
                for (
                    seg_id,
                    tier,
                    row_count,
                    mem,
                    metadata,
                    row_ids,
                    exact_indices,
                    ordered_indices,
                    descriptor,
                    column_payload,
                    idle,
                ) in mgr.volume_stats()
                {
                    result.push((
                        table_name.clone(),
                        seg_id,
                        tier,
                        row_count,
                        mem,
                        metadata,
                        row_ids,
                        exact_indices,
                        ordered_indices,
                        descriptor,
                        column_payload,
                        idle,
                        tombstone_count,
                    ));
                }
            }
        }
        result
    }

    /// Returns the database path
    pub fn get_path(&self) -> &str {
        &self.path
    }

    /// Returns a copy of the configuration
    pub fn config(&self) -> Config {
        self.config.read().unwrap().clone()
    }

    /// Updates the engine configuration
    pub fn update_engine_config(&self, config: Config) -> Result<()> {
        let current = self.config.read().unwrap();
        if config.path != current.path {
            return Err(Error::internal("cannot change database path after opening"));
        }
        if config.persistence.storage_cpu_workers != current.persistence.storage_cpu_workers {
            return Err(Error::invalid_argument(
                "storage_cpu_workers is fixed when the database opens; restart with the new value",
            ));
        }
        drop(current);

        *self.config.write().unwrap() = config;
        Ok(())
    }

    /// Returns the transaction registry
    pub fn registry(&self) -> Arc<TransactionRegistry> {
        Arc::clone(&self.registry)
    }

    /// Replay an ALTER TABLE operation from WAL
    /// Check if we should skip WAL writes (during recovery replay)
    pub(super) fn should_skip_wal(&self) -> bool {
        self.loading_from_disk.load(Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn truncate_table_under_ddl_fence(&self, table_name: &str, txn_id: i64) -> Result<i32> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }
        let table_name_lower = table_name.to_lowercase();
        let store = self.get_version_store(&table_name_lower)?;
        // Commits hold the same table-local fence from index mutation through
        // hot-version publication. TRUNCATE must own it exclusively as well;
        // otherwise a pure INSERT (which has no pre-existing row claim) can
        // publish an index entry on one side of the clear and its row version
        // on the other.
        let membership_fence = store.membership_fence();
        let _membership_guard = membership_fence.write();
        let manager = self
            .segment_managers
            .read()
            .unwrap()
            .get(&table_name_lower)
            .cloned();
        let cold_rows = manager
            .as_ref()
            .map(|manager| manager.total_row_count() as i32)
            .unwrap_or(0);

        let hot_rows = store.truncate_all_after(|| {
            if let Some(manager) = manager.as_ref() {
                manager.rollback_pending_tombstones(txn_id);
            }
            <Self as Engine>::record_truncate_table(self, &table_name_lower)
        })?;
        Ok(hot_rows.saturating_add(cold_rows))
    }
}
