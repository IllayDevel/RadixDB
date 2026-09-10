use super::*;

impl WALManager {
    /// Extract the committed catalog substream from this WAL authority. Every
    /// embedded catalog transaction is checked against the shared outer commit
    /// marker before its bytes are admitted to the result.
    pub(crate) fn read_catalog_transactions(
        &self,
        from_lsn: u64,
        byte_budget: u64,
    ) -> Result<Vec<u8>> {
        let mut pending = rustc_hash::FxHashMap::<i64, Vec<u8>>::default();
        let mut output = Vec::new();
        self.replay_two_phase(from_lsn, |entry| {
            if entry.operation == WALOperationType::CatalogMutation {
                if pending.insert(entry.txn_id, entry.data).is_some() {
                    return Err(Error::internal(format!(
                        "transaction {} contains more than one catalog mutation record",
                        entry.txn_id
                    )));
                }
                return Ok(());
            }
            if !entry.is_commit_marker() {
                return Ok(());
            }
            let Some(encoded) = pending.remove(&entry.txn_id) else {
                return Ok(());
            };
            let decoded =
                crate::v6::decode_catalog_wal(&encoded, crate::v6::CatalogWalReplayLimits::hard())
                    .map_err(|error| {
                        Error::internal(format!(
                            "catalog WAL record for transaction {} is invalid: {error}",
                            entry.txn_id
                        ))
                    })?;
            if decoded.incomplete_tail_bytes() != 0 || decoded.transactions().len() != 1 {
                return Err(Error::internal(format!(
                    "catalog WAL record for transaction {} is not one complete transaction",
                    entry.txn_id
                )));
            }
            let catalog_transaction = &decoded.transactions()[0];
            if catalog_transaction.commit_lsn() != entry.lsn {
                return Err(Error::internal(format!(
                    "catalog WAL transaction {} binds commit LSN {}, shared marker is {}",
                    entry.txn_id,
                    catalog_transaction.commit_lsn(),
                    entry.lsn
                )));
            }
            let mut expected_identity = [0_u8; 16];
            expected_identity[..8].copy_from_slice(&entry.txn_id.to_le_bytes());
            expected_identity[8..].copy_from_slice(&entry.lsn.to_le_bytes());
            if catalog_transaction.transaction_id().as_bytes() != expected_identity {
                return Err(Error::internal(format!(
                    "catalog WAL transaction {} has a foreign embedded identity",
                    entry.txn_id
                )));
            }
            let next_len = output
                .len()
                .checked_add(encoded.len())
                .ok_or_else(|| Error::internal("catalog WAL replay byte count overflow"))?;
            if next_len as u64 > byte_budget {
                return Err(Error::internal(format!(
                    "catalog WAL replay exceeds byte budget: {} bytes (maximum {})",
                    next_len, byte_budget
                )));
            }
            output.extend_from_slice(&encoded);
            Ok(())
        })?;
        if !pending.is_empty() {
            return Err(Error::internal(
                "committed catalog WAL mutation is missing its shared commit callback",
            ));
        }
        Ok(output)
    }

    /// Two-phase WAL replay for crash recovery
    ///
    /// Phase 1 (Analysis): Scan all entries to identify committed/aborted transactions.
    ///                     Outcomes use a fixed memory budget and spill to a
    ///                     temporary disk hash index for long retained history.
    /// Phase 2 (REDO): Re-read WAL and apply only entries from committed transactions
    ///
    /// This ensures that after a crash, only committed transactions are visible.
    /// Uncommitted transactions (those without a COMMIT_MARKER) are discarded.
    ///
    /// The common path uses two streaming passes. If the declared outcome
    /// memory budget is exceeded, analysis performs one additional validated
    /// pass to build the disk index; process RSS remains independent of the
    /// number of retained transactions.
    pub fn replay_two_phase<F>(&self, from_lsn: u64, callback: F) -> Result<TwoPhaseRecoveryInfo>
    where
        F: FnMut(WALEntry) -> Result<()>,
    {
        self.replay_two_phase_with_outcome_observer(from_lsn, callback, |_| Ok(()))
    }

    pub(crate) fn replay_two_phase_with_outcome_observer<F, O>(
        &self,
        from_lsn: u64,
        mut callback: F,
        mut observe_uncommitted: O,
    ) -> Result<TwoPhaseRecoveryInfo>
    where
        F: FnMut(WALEntry) -> Result<()>,
        O: FnMut(i64) -> Result<()>,
    {
        // Flush buffer first
        self.flush()?;

        // The caller owns the replay floor after validating the corresponding
        // manifest/snapshot generation. In particular, an explicit zero must
        // remain zero when any table manifest has no committed checkpoint;
        // A CONTROL slot alone cannot prove table-artifact completeness.
        let replay_floor = *self
            .replay_floor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // A caller may request a historical boundary, but CONTROL has already
        // made every earlier record redundant and eligible for retirement.
        // Clamp instead of requiring a file which the selected generation no
        // longer owns.
        let from_lsn = from_lsn.max(replay_floor.lsn());
        let generations = Self::collect_validated_generations(
            &self.path,
            replay_floor.generation().get(),
            replay_floor.lsn(),
        )?;
        Self::validate_required_generation_suffix(&generations, from_lsn)?;

        // =====================================================
        // Phase 1: Analysis - Identify transaction outcomes.
        // Keep a bounded common-case map. Once it crosses the declared
        // budget, discard it and build an exact disk-backed index from a
        // second validated scan.
        // =====================================================
        let mut memory_outcomes = rustc_hash::FxHashMap::default();
        let mut outcome_markers = 0usize;
        let mut needs_spill = false;
        let mut last_lsn = from_lsn;
        let mut max_transaction_id = self.transaction_high_water.load(Ordering::Acquire);

        for generation in &generations {
            let mut reader = ValidatedWalReader::open(&generation.path)?;
            while let Some(entry) = reader.next_entry()? {
                if entry.txn_id == i64::MAX {
                    return Err(Error::internal(format!(
                        "WAL transaction ID domain exhausted at LSN {}",
                        entry.lsn
                    )));
                }
                if entry.txn_id > max_transaction_id {
                    max_transaction_id = entry.txn_id;
                }
                if !crate::timestamp::observe_persisted_timestamp(entry.timestamp) {
                    return Err(Error::internal(format!(
                        "WAL timestamp domain exhausted at LSN {}",
                        entry.lsn
                    )));
                }
                if entry.lsn <= from_lsn {
                    continue;
                }
                last_lsn = last_lsn.max(entry.lsn);
                let Some(outcome) = Self::marker_outcome(&entry) else {
                    continue;
                };
                outcome_markers = outcome_markers
                    .checked_add(1)
                    .ok_or_else(|| Error::internal("WAL recovery marker count overflow"))?;
                if needs_spill {
                    continue;
                }
                match memory_outcomes.get(&entry.txn_id) {
                    Some(stored) if *stored != outcome => {
                        return Err(Error::internal(format!(
                            "WAL transaction {} has both commit and abort outcomes",
                            entry.txn_id
                        )))
                    }
                    Some(_) => {}
                    None => {
                        memory_outcomes.insert(entry.txn_id, outcome);
                        if memory_outcomes.len() > RECOVERY_OUTCOME_MEMORY_LIMIT {
                            needs_spill = true;
                            memory_outcomes.clear();
                            memory_outcomes.shrink_to_fit();
                        }
                    }
                }
            }
        }
        self.transaction_high_water
            .fetch_max(max_transaction_id, Ordering::AcqRel);

        let mut outcomes = if needs_spill {
            let mut disk = DiskRecoveryOutcomes::create(&self.path, outcome_markers)?;
            for generation in &generations {
                Self::scan_wal_outcomes(&generation.path, from_lsn, |txn_id, outcome| {
                    disk.insert(txn_id, outcome)
                })?;
            }
            RecoveryOutcomes::Disk(disk)
        } else {
            RecoveryOutcomes::Memory(memory_outcomes)
        };
        let (committed_transactions, aborted_transactions) = outcomes.counts();

        // =====================================================
        // Phase 2: REDO - Re-read WAL and apply committed entries
        // Streaming approach: read and apply one entry at a time
        // =====================================================
        let mut applied_count = 0u64;
        let mut skipped_count = 0u64;

        for generation in &generations {
            let mut reader = ValidatedWalReader::open(&generation.path)?;
            while let Some(entry) = reader.next_entry()? {
                if entry.lsn <= from_lsn {
                    continue;
                }

                // Abort markers do not apply data, but the registry must
                // retain their non-committed identity and high-water.
                if entry.is_abort_marker() {
                    observe_uncommitted(entry.txn_id)?;
                    continue;
                }

                // For commit markers: pass to callback so registry can be updated.
                if entry.is_commit_marker() {
                    if outcomes.get(entry.txn_id)? == Some(RECOVERY_OUTCOME_COMMITTED) {
                        callback(entry)?;
                    }
                    continue;
                }

                // Apply only committed transactions' data entries.
                if outcomes.get(entry.txn_id)? == Some(RECOVERY_OUTCOME_COMMITTED) {
                    callback(entry)?;
                    applied_count += 1;
                } else {
                    // Transaction is aborted or in-doubt (no commit marker).
                    observe_uncommitted(entry.txn_id)?;
                    skipped_count += 1;
                }
            }
        }

        // Update current LSN if we replayed entries
        if last_lsn > self.current_lsn.load(Ordering::Acquire) {
            self.current_lsn.store(last_lsn, Ordering::Release);
        }

        Ok(TwoPhaseRecoveryInfo {
            last_lsn,
            committed_transactions,
            aborted_transactions,
            applied_entries: applied_count,
            skipped_entries: skipped_count,
            max_transaction_id,
        })
    }

    #[inline]
    pub(super) fn marker_outcome(entry: &WALEntry) -> Option<u8> {
        if entry.is_commit_marker() {
            Some(RECOVERY_OUTCOME_COMMITTED)
        } else if entry.is_abort_marker() {
            Some(RECOVERY_OUTCOME_ABORTED)
        } else {
            None
        }
    }

    pub(super) fn scan_wal_outcomes<F>(
        wal_path: &Path,
        from_lsn: u64,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(i64, u8) -> Result<()>,
    {
        let mut reader = ValidatedWalReader::open(wal_path)?;
        while let Some(entry) = reader.next_entry()? {
            if entry.lsn <= from_lsn {
                continue;
            }
            if let Some(outcome) = Self::marker_outcome(&entry) {
                callback(entry.txn_id, outcome)?;
            }
        }
        Ok(())
    }
}
