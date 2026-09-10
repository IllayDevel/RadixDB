use super::*;

const SEAL_STALE_REPLAN_LIMIT: usize = 2;

impl MVCCEngine {
    /// Checkpoint-to-volume cycle: replaces the old snapshot-based persistence.
    ///
    /// Instead of serializing the hot buffer or re-seeding DDL, this cycle seals
    /// committed rows into immutable DATA artifacts and publishes catalog,
    /// manifests and the next WAL replay floor as one physical generation.
    ///
    /// WAL truncation is only safe when ALL committed hot rows have been sealed.
    /// Otherwise, unsealed rows' INSERT entries must survive in the WAL.
    pub(super) fn checkpoint_cycle(&self) -> Result<()> {
        self.checkpoint_cycle_inner(false)?;
        // Durability publication and WAL retention are complete at this
        // point. Compaction is independent maintenance: request it without
        // making checkpoint latency or success depend on rewriting cold data.
        self.compaction_requested.store(true, Ordering::Release);
        Ok(())
    }

    /// Inner checkpoint implementation. When `force` is true, seals ALL hot rows
    /// regardless of threshold (used by PRAGMA CHECKPOINT and close_engine).
    /// When false, respects the normal seal thresholds (used by background thread).
    pub(super) fn checkpoint_cycle_inner(&self, force: bool) -> Result<()> {
        if let Some(pm) = self.persistence() {
            if !pm.is_enabled() {
                return Ok(());
            }
        } else {
            return Ok(());
        }
        // A checkpoint publishes sealed payloads, checkpoint metadata and the
        // database-wide manifest generation for one immutable catalog view.
        // Direct Rust callers and the background worker enter through here;
        // the SQL coordinator deliberately does not take an outer DDL fence.
        // Acquire DDL before checkpoint_mutex to match SNAPSHOT/RESTORE and
        // avoid the inverse-order deadlock (DDL(W) -> checkpoint mutex).
        let _ddl_generation_guard = DdlFenceGuard::shared(Arc::clone(&self.ddl_fence));
        // Serialize the entire cycle: prevent concurrent seal+compact from
        // the background thread and explicit PRAGMA CHECKPOINT.
        let _checkpoint_guard = self.lock_checkpoint_mutex_profiled();
        let checkpoint_runtime = self.runtime_maintenance.checkpoint.start();
        checkpoint_runtime.set_reason(if force {
            "forced_checkpoint"
        } else {
            "scheduled_checkpoint"
        });
        if let Ok(stores) = self.version_stores.try_read() {
            let (rows, bytes) = stores
                .values()
                .fold((0_u64, 0_u64), |(rows, bytes), store| {
                    (
                        rows.saturating_add(store.committed_row_count() as u64),
                        bytes.saturating_add(store.committed_hot_bytes() as u64),
                    )
                });
            checkpoint_runtime.add_input(rows, bytes);
        }

        // Step 1: Seal hot rows into frozen volumes (the actual checkpoint).
        // Sealed rows are published as immutable DATA/INDEX artifacts and then
        // removed from the hot buffer.
        // When force=true, bypass thresholds so ALL hot rows are sealed.
        if force {
            self.force_seal_all.store(true, Ordering::Release);
        }
        let initial_seal = self.seal_checkpoint_hot_buffers(force);
        if force {
            self.force_seal_all.store(false, Ordering::Release);
        }
        initial_seal?;
        // Step 2: Force-seal any remaining small tables so all hot buffers
        // are empty. The first seal pass (Step 1) uses incremental thresholds
        // and may leave small tables (metrics, logs, etc.) unsealed. Without
        // draining them, all_hot_empty is never true and WAL never truncates.
        let all_hot_empty = {
            let stores = self.version_stores.read().unwrap();
            stores
                .values()
                .all(|store| store.committed_row_count() == 0)
        };

        if !all_hot_empty && !force {
            // Force-seal the stragglers (small tables below threshold)
            self.force_seal_all.store(true, Ordering::Release);
            let straggler_seal = self.seal_hot_buffers_under_catalog_generation();
            self.force_seal_all.store(false, Ordering::Release);
            straggler_seal?;
        }

        // Step 3: Brief fence — block commits just long enough to check if all
        // hot buffers are empty and capture checkpoint_lsn. NO disk I/O inside
        // the fence. Previously this ran a full seal_hot_buffers() (with volume
        // building + disk writes) while blocking all commits, causing 1-2s INSERT
        // stalls. Now the fence is held for microseconds (atomic counter reads).
        // If hot buffers aren't empty after steps 1-2, we skip WAL truncation
        // this cycle and let the next cycle's bulk seal drain them.
        let checkpoint_lsn = match self
            .seal_fence
            .try_write_for(std::time::Duration::from_secs(15))
        {
            Some(_fence) => {
                // Fence acquired — no new commits can start. In-flight commits
                // finished (they held the read lock, which is now released).
                // Just check if bulk seal (steps 1-2) drained everything.
                let all_hot_empty = {
                    let stores = self.version_stores.read().unwrap();
                    stores
                        .values()
                        .all(|store| store.committed_row_count() == 0)
                };

                if all_hot_empty {
                    // All data is in volumes. Safe to advance the WAL checkpoint.
                    if let Some(pm) = self.persistence() {
                        let checkpoint_lsn = pm.create_checkpoint()?;
                        if checkpoint_lsn > 0 {
                            // Ensure the next replay-floor member exists and is
                            // immutable for the duration of the publication.
                            // If WAL size rotation already created it, this is
                            // only a durability preflight.
                            let wal_generation = pm.prepare_checkpoint_retention(checkpoint_lsn)?;
                            self.publish_checkpoint_generation(checkpoint_lsn, wal_generation)?;
                        }
                        checkpoint_lsn
                    } else {
                        0
                    }
                } else {
                    if force {
                        return Err(Error::internal(FORCED_CHECKPOINT_HOT_ROWS_UNSEALED));
                    }
                    // A background checkpoint may defer publication when
                    // continuous writes kept hot buffers non-empty.
                    0
                }
                // _fence dropped here — commits resume
            }
            None => {
                return Err(Error::internal(
                    "checkpoint timed out acquiring the commit fence",
                ));
            }
        };

        checkpoint_runtime.set_result_marker(checkpoint_lsn);
        checkpoint_runtime.success();
        self.request_page_cache_warmup();

        Ok(())
    }

    fn seal_checkpoint_hot_buffers(&self, retry_stale_publication: bool) -> Result<()> {
        for replan in 0..=SEAL_STALE_REPLAN_LIMIT {
            let visibility_before_seal = self.registry.get_current_sequence();
            let outcome = self.seal_hot_buffers_under_catalog_generation()?;
            if !retry_stale_publication
                || !outcome.stale_publication
                // A concurrent DML visibility point changes which hot rows a
                // second extraction would own. Preserve the established
                // forced-checkpoint race contract and defer that newer work
                // instead of silently widening this checkpoint's boundary.
                || self.registry.get_current_sequence() != visibility_before_seal
                || replan == SEAL_STALE_REPLAN_LIMIT
            {
                return Ok(());
            }
        }
        unreachable!("bounded seal replan loop always returns")
    }
}
