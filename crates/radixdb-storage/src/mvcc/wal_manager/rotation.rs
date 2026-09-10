use super::*;

impl WALManager {
    /// Check if WAL file should be rotated based on size
    ///
    /// Returns true if rotation occurred
    pub fn maybe_rotate(&self) -> Result<bool> {
        let mut transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;
        let current_size = self.current_file_position.load(Ordering::Relaxed);
        if current_size < self.max_wal_size {
            return Ok(false);
        }

        self.flush_under_transition()?;
        if let Err(error) = self.sync_locked() {
            return Err(self.mark_transition_uncertain(
                &mut transition,
                format!("WAL rotation pre-publish fsync failed: {}", error),
            ));
        }
        self.rotate_wal_under_transition(&mut transition)?;

        Ok(true)
    }

    /// Rotate WAL to a new file
    ///
    /// This:
    /// 1. Syncs and closes the current WAL file
    /// 2. Creates a new WAL file with incremented sequence number
    /// 3. Durably publishes the canonical successor generation
    pub(super) fn rotate_wal_under_transition(
        &self,
        transition: &mut WalTransitionState,
    ) -> Result<()> {
        let current_lsn = self.current_lsn.load(Ordering::Acquire);
        let current_name = self.current_wal_file.lock().unwrap().clone();
        let current_sequence = self.wal_sequence.load(Ordering::Acquire);
        let replay_floor = *self
            .replay_floor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current_start_lsn = self
            .validated_closed_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last()
            .map_or(replay_floor.lsn(), |generation| generation.end_lsn);
        let closed_generation = Self::validate_generation(
            self.path.join(&current_name),
            current_sequence,
            current_start_lsn,
        )?;
        if closed_generation.name != current_name || closed_generation.end_lsn != current_lsn {
            return Err(Error::internal(format!(
                "current WAL generation {} validated through LSN {}, expected {}",
                closed_generation.name, closed_generation.end_lsn, current_lsn
            )));
        }
        #[cfg(any(test, feature = "test-hooks"))]
        self.runtime_generation_validation_bytes
            .fetch_add(closed_generation.identity.len, Ordering::AcqRel);
        let new_sequence = self
            .wal_sequence
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| Error::internal("WAL generation sequence overflow"))?;
        let new_filename = Self::canonical_filename(new_sequence);
        let new_path = self.path.join(&new_filename);

        let new_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| Error::internal(format!("failed to create rotated WAL file: {}", e)))?;
        if let Err(error) = new_file
            .sync_all()
            .map_err(|error| Error::internal(format!("failed to sync rotated WAL file: {}", error)))
        {
            drop(new_file);
            return match Self::remove_generation_durably(&new_path) {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(self.mark_transition_uncertain(
                    transition,
                    format!(
                        "rotation file sync failed ({error}) and orphan cleanup failed: {}",
                        cleanup_error
                    ),
                )),
            };
        }
        if let Err(error) = Self::sync_directory(&self.path) {
            drop(new_file);
            return match Self::remove_generation_durably(&new_path) {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(self.mark_transition_uncertain(
                    transition,
                    format!(
                        "rotation directory sync failed ({error}) and orphan cleanup failed: {}",
                        cleanup_error
                    ),
                )),
            };
        }

        *self.wal_file.lock().unwrap() = Some(new_file);
        *self.current_wal_file.lock().unwrap() = new_filename;
        self.current_file_position.store(0, Ordering::Release);
        self.last_synced_file_position.store(0, Ordering::Release);
        self.wal_sequence.store(new_sequence, Ordering::Release);
        self.validated_closed_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(closed_generation);

        Ok(())
    }

    /// Get current WAL file size
    pub fn current_file_size(&self) -> u64 {
        self.current_file_position.load(Ordering::Relaxed)
    }

    /// Bytes admitted but not yet covered by a successful fsync. Reading the
    /// buffered component is non-blocking so diagnostics cannot join WAL's
    /// append queue.
    pub fn pending_durability_bytes(&self) -> Option<u64> {
        let buffer = self.buffer.try_lock().ok()?;
        let written = self.current_file_position.load(Ordering::Acquire);
        let synced = self.last_synced_file_position.load(Ordering::Acquire);
        Some(
            written
                .saturating_sub(synced)
                .saturating_add(buffer.len() as u64),
        )
    }

    /// Get maximum WAL file size
    pub fn max_file_size(&self) -> u64 {
        self.max_wal_size
    }

    /// Get current WAL sequence number
    pub fn current_sequence(&self) -> u64 {
        self.wal_sequence.load(Ordering::Relaxed)
    }

    /// Public sync method
    pub fn sync(&self) -> Result<()> {
        let mut transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;
        self.flush_under_transition()?;
        if let Err(error) = self.sync_locked() {
            return Err(self.mark_transition_uncertain(
                &mut transition,
                format!("WAL fsync failed: {}", error),
            ));
        }
        Ok(())
    }

    /// Flush buffer to disk without syncing
    pub fn flush(&self) -> Result<()> {
        let transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;
        self.flush_under_transition()
    }

    pub(super) fn flush_under_transition(&self) -> Result<()> {
        let buffer_data = {
            let mut buffer = self.buffer.lock().unwrap();
            if buffer.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buffer)
        };

        let write_result = self.write_to_file(&buffer_data);
        match write_result {
            Ok(()) => {
                self.current_file_position
                    .fetch_add(buffer_data.len() as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(failure) => {
                self.current_file_position
                    .fetch_add(failure.written as u64, Ordering::Relaxed);
                self.prepend_to_buffer(&buffer_data[failure.written..]);
                Err(failure.error)
            }
        }
    }

    /// Check if we should sync based on operation type
    pub(super) fn should_sync(&self, op: WALOperationType) -> bool {
        match self.sync_mode {
            SyncMode::None => false,
            SyncMode::Normal => {
                // A COMMIT marker and DDL have an immediate durability
                // deadline. A ROLLBACK marker is still force-flushed so later
                // recovery can classify it when present, but it does not need
                // an fsync: if the marker is lost, the transaction has no
                // durable COMMIT marker and recovery rejects its data anyway.
                if op == WALOperationType::Commit || op.is_ddl() {
                    return true;
                }
                if op == WALOperationType::Rollback {
                    return false;
                }
                // Time-based sync: fsync at most once per second.
                // Committed data survives in the OS buffer cache for most crashes
                // (power failure is the exception). Checkpoint (every 60s) moves
                // data to fsynced volume files for full durability.
                // Max data loss on power failure: ~1 second of commits.
                let now = self
                    .sync_clock_origin
                    .elapsed()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64;
                let last = self.last_sync_elapsed_nanos.load(Ordering::Relaxed);
                now.saturating_sub(last) >= self.sync_interval_nanos
            }
            SyncMode::Full => true,
        }
    }
}
