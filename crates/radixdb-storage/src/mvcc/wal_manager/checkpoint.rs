use super::*;

use std::collections::BTreeSet;

impl WALManager {
    /// Create a checkpoint and return the LSN at the checkpoint point
    ///
    /// Returns the LSN that represents the checkpoint point. All data up to
    /// this LSN is guaranteed to be durably written to disk when this returns.
    /// This LSN should be used for snapshot creation to ensure consistency.
    pub fn create_checkpoint(&self) -> Result<u64> {
        let mut transition = self.transition.lock().unwrap();
        match transition.lifecycle {
            WalLifecycle::Running | WalLifecycle::Failed => {}
            WalLifecycle::Closing | WalLifecycle::Closed => return Err(Error::WalNotRunning),
        }
        self.flush_under_transition()?;
        if let Err(error) = self.sync_locked() {
            return Err(self.mark_transition_uncertain(
                &mut transition,
                format!("checkpoint WAL fsync failed: {}", error),
            ));
        }

        // `Failed` blocks ordinary admission because a partial write or fsync
        // error left durability uncertain. A complete flush followed by a
        // successful fsync is the explicit reconciliation barrier: every
        // retained byte (including a previously unwritten record suffix) is
        // now whole and durable, so checkpoint retention may safely continue.
        if transition.lifecycle == WalLifecycle::Failed {
            transition.lifecycle = WalLifecycle::Running;
            self.running.store(true, Ordering::Release);
        }

        // Admission, flush, fsync, LSN capture and checkpoint publication are
        // one transition. No append can reserve or publish an LSN in between.
        let checkpoint_lsn = self.current_lsn.load(Ordering::Acquire);
        #[cfg(any(test, feature = "test-failpoints"))]
        crate::test_failpoints::interleave(
            crate::test_failpoints::InterleavePoint::CheckpointBeforePublish,
            0,
        );

        #[cfg(any(test, feature = "test-failpoints"))]
        if crate::test_failpoints::CHECKPOINT_WRITE_FAIL.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::internal("failpoint: checkpoint write"));
        }

        self.last_checkpoint
            .store(checkpoint_lsn, Ordering::Release);

        Ok(checkpoint_lsn)
    }

    /// Close the WAL manager
    pub fn close(&self) -> Result<()> {
        let mut transition = self.transition.lock().unwrap();
        match transition.lifecycle {
            WalLifecycle::Closed => return Ok(()),
            WalLifecycle::Closing => {
                return Err(Error::internal("WAL close is already in progress"));
            }
            WalLifecycle::Running | WalLifecycle::Failed => {
                transition.lifecycle = WalLifecycle::Closing;
                self.running.store(false, Ordering::Release);
            }
        }
        drop(transition);

        #[cfg(test)]
        let close_test_hook = {
            self.close_test_hook
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        };
        #[cfg(test)]
        if let Some(hook) = close_test_hook {
            hook(&self.path);
        }

        let mut transition = self.transition.lock().unwrap();
        if let Err(error) = self.flush_under_transition() {
            transition.lifecycle = WalLifecycle::Failed;
            return Err(Error::WalDurabilityUncertain {
                detail: format!("WAL close could not drain accepted bytes: {}", error),
            });
        }
        if let Err(error) = self.sync_locked() {
            return Err(self.mark_transition_uncertain(
                &mut transition,
                format!("WAL close fsync failed: {}", error),
            ));
        }

        // Close file
        let mut wal_file = self.wal_file.lock().unwrap();
        *wal_file = None;
        transition.lifecycle = WalLifecycle::Closed;

        Ok(())
    }

    /// Get the WAL directory path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get maximum WAL file size before rotation
    pub fn max_wal_size(&self) -> u64 {
        self.max_wal_size
    }

    /// Get last checkpoint LSN
    pub fn last_checkpoint_lsn(&self) -> u64 {
        self.last_checkpoint.load(Ordering::Acquire)
    }

    /// Get current WAL file name
    pub fn current_wal_file(&self) -> String {
        self.current_wal_file.lock().unwrap().clone()
    }

    /// Publish an empty successor generation when the current generation is
    /// fully covered by a durable checkpoint. Retirement remains owned by the
    /// CONTROL publication transaction and cannot be requested independently.
    pub(crate) fn prepare_checkpoint_retention(
        &self,
        up_to_lsn: u64,
    ) -> Result<crate::v6::WalGeneration> {
        let mut transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;

        if up_to_lsn == 0 {
            return Err(Error::internal(
                "WAL retention requires a non-zero checkpoint LSN",
            ));
        }
        let current_lsn = self.current_lsn.load(Ordering::Acquire);
        let checkpoint_lsn = self.last_checkpoint.load(Ordering::Acquire);
        if up_to_lsn > current_lsn || up_to_lsn > checkpoint_lsn {
            return Err(Error::internal(format!(
                "WAL retention boundary {} is not owned by durable checkpoint {} at current LSN {}",
                up_to_lsn, checkpoint_lsn, current_lsn
            )));
        }

        self.flush_under_transition()?;
        if let Err(error) = self.sync_locked() {
            return Err(self.mark_transition_uncertain(
                &mut transition,
                format!("WAL retention seed preflight fsync failed: {}", error),
            ));
        }

        if self.current_file_position.load(Ordering::Acquire) > 0 && current_lsn <= up_to_lsn {
            self.rotate_wal_under_transition(&mut transition)?;
        }

        crate::v6::WalGeneration::new(self.wal_sequence.load(Ordering::Acquire))
            .map_err(|error| Error::internal(error.to_string()))
    }

    /// Durably close the current append generation and return the exact
    /// immutable WAL suffix required by one CONTROL-selected physical root.
    ///
    /// The caller is responsible for preventing checkpoint retention while it
    /// copies the returned files. Ordinary writers may continue immediately in
    /// the newly published successor generation.
    pub(crate) fn freeze_snapshot_generations(
        &self,
        floor: crate::v6::WalReplayFloor,
    ) -> Result<Vec<SnapshotWalGeneration>> {
        let mut transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;
        let replay_floor = *self
            .replay_floor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if floor != replay_floor {
            return Err(Error::internal(format!(
                "snapshot WAL floor {}:{} differs from the active WAL owner {}:{}",
                floor.generation().get(),
                floor.lsn(),
                replay_floor.generation().get(),
                replay_floor.lsn()
            )));
        }

        self.flush_under_transition()?;
        if let Err(error) = self.sync_locked() {
            return Err(self.mark_transition_uncertain(
                &mut transition,
                format!("snapshot WAL boundary fsync failed: {error}"),
            ));
        }

        // Always rotate, including an empty generation. This turns every
        // returned source into an immutable file while allowing new commits to
        // continue in the successor as the snapshot copy proceeds.
        let last_snapshot_generation = self.wal_sequence.load(Ordering::Acquire);
        self.rotate_wal_under_transition(&mut transition)?;

        let closed = self
            .validated_closed_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let selected = closed
            .iter()
            .filter(|generation| {
                generation.sequence >= floor.generation().get()
                    && generation.sequence <= last_snapshot_generation
            })
            .collect::<Vec<_>>();
        let expected_count = last_snapshot_generation
            .checked_sub(floor.generation().get())
            .and_then(|distance| distance.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| Error::internal("snapshot WAL generation range exceeds usize"))?;
        if selected.len() != expected_count
            || selected.iter().enumerate().any(|(offset, generation)| {
                generation.sequence
                    != floor
                        .generation()
                        .get()
                        .checked_add(offset as u64)
                        .unwrap_or(0)
            })
        {
            return Err(Error::internal(
                "snapshot WAL suffix is not contiguous from the CONTROL replay floor",
            ));
        }

        let mut sources = Vec::with_capacity(selected.len());
        for generation in selected {
            let identity = WalFileIdentity::read(&generation.path)?;
            if identity != generation.identity {
                return Err(Error::internal(format!(
                    "snapshot WAL generation {} changed after validation",
                    generation.path.display()
                )));
            }
            sources.push(SnapshotWalGeneration {
                generation: crate::v6::WalGeneration::new(generation.sequence)
                    .map_err(|error| Error::internal(error.to_string()))?,
                path: generation.path.clone(),
                byte_length: identity.len,
            });
        }
        Ok(sources)
    }

    /// Enumerate every canonical WAL generation that is no longer reachable
    /// once `retained_floor` becomes the older of the two durable CONTROL
    /// roots.  Enumerating existing files instead of the numeric generation
    /// range keeps the work bounded when size rotation advanced the sequence
    /// by many generations between checkpoints.
    pub(crate) fn checkpoint_retirement_candidates(
        &self,
        retained_floor: crate::v6::WalGeneration,
    ) -> Result<Vec<crate::v6::WalGeneration>> {
        let transition = self.transition.lock().unwrap();
        Self::require_running_transition(&transition)?;

        let mut generations = BTreeSet::new();
        collect_retirement_candidates(&self.path, retained_floor.get(), &mut generations)?;

        let retired = self.path.join("retired");
        if retired.exists() {
            collect_retirement_candidates(&retired, retained_floor.get(), &mut generations)?;
        }

        generations
            .into_iter()
            .map(|generation| {
                crate::v6::WalGeneration::new(generation)
                    .map_err(|error| Error::internal(error.to_string()))
            })
            .collect()
    }

    /// Reconcile the live WAL owner after CONTROL selected a new checkpoint.
    /// The replay floor advances even when post-publication retirement is
    /// deferred; only the immutable-generation cache depends on which files
    /// were actually retired.
    pub(crate) fn confirm_checkpoint_publication(
        &self,
        floor: crate::v6::WalReplayFloor,
        retired: &[crate::v6::WalGeneration],
    ) {
        *self
            .replay_floor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = floor;
        if retired.is_empty() {
            return;
        }
        let retired = retired
            .iter()
            .map(|generation| generation.get())
            .collect::<BTreeSet<_>>();
        self.validated_closed_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|generation| !retired.contains(&generation.sequence));
    }
}

fn collect_retirement_candidates(
    directory: &Path,
    retained_floor: u64,
    generations: &mut BTreeSet<u64>,
) -> Result<()> {
    let metadata = fs::symlink_metadata(directory).map_err(|error| {
        Error::internal(format!(
            "failed to inspect WAL retirement directory {}: {}",
            directory.display(),
            error
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::internal(format!(
            "WAL retirement path {} is not a regular directory",
            directory.display()
        )));
    }

    for entry in fs::read_dir(directory).map_err(|error| {
        Error::internal(format!(
            "failed to enumerate WAL retirement directory {}: {}",
            directory.display(),
            error
        ))
    })? {
        let entry = entry.map_err(|error| {
            Error::internal(format!("failed to read WAL retirement entry: {}", error))
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "retired" && entry.path() == directory.join("retired") {
            continue;
        }
        let generation = WALManager::parse_canonical_generation(&name).ok_or_else(|| {
            Error::internal(format!(
                "non-canonical file '{}' exists in WAL retirement directory {}",
                name,
                directory.display()
            ))
        })?;
        let entry_type = entry.file_type().map_err(|error| {
            Error::internal(format!(
                "failed to inspect WAL retirement entry '{}': {}",
                name, error
            ))
        })?;
        if entry_type.is_symlink() || !entry_type.is_file() {
            return Err(Error::internal(format!(
                "WAL retirement entry '{}' is not a regular file",
                name
            )));
        }
        if generation < retained_floor {
            generations.insert(generation);
        }
    }
    Ok(())
}

impl Drop for WALManager {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
