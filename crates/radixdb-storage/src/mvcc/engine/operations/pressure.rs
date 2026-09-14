use super::*;

impl EngineOperations {
    pub(super) fn touched_l0_pressure(
        &self,
        txn_id: i64,
    ) -> Option<(
        String,
        crate::volume::manifest::L0DebtSnapshot,
        L0PressureLevel,
    )> {
        let touched: SmallVec<[SmartString; 4]> = self
            .txn_version_stores
            .read()
            .unwrap()
            .get(txn_id)
            .map(|tables| tables.iter().map(|(name, _)| name.clone()).collect())
            .unwrap_or_default();
        if touched.is_empty() {
            return None;
        }

        let managers = self.segment_managers.read().unwrap();
        let mut soft = None;
        for table_name in touched {
            let Some(manager) = managers.get(table_name.as_str()) else {
                continue;
            };
            let debt = manager.l0_debt_snapshot();
            match classify_l0_pressure(debt, self.l0_pressure_limits) {
                L0PressureLevel::Hard => {
                    return Some((table_name.to_string(), debt, L0PressureLevel::Hard));
                }
                L0PressureLevel::Soft if soft.is_none() => {
                    soft = Some((table_name.to_string(), debt, L0PressureLevel::Soft));
                }
                L0PressureLevel::Normal | L0PressureLevel::Soft => {}
            }
        }
        soft
    }

    pub(super) fn wait_for_compaction_pressure(&self, txn_id: i64) -> Result<()> {
        let started = Instant::now();
        let mut soft_wait_recorded = false;
        loop {
            let Some((table, debt, level)) = self.touched_l0_pressure(txn_id) else {
                if soft_wait_recorded {
                    self.compaction_soft_backpressure_wait_millis.fetch_add(
                        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                }
                return Ok(());
            };
            self.compaction_requested.store(true, Ordering::Release);
            if level == L0PressureLevel::Hard {
                if soft_wait_recorded {
                    self.compaction_soft_backpressure_wait_millis.fetch_add(
                        started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                        Ordering::Relaxed,
                    );
                }
                self.compaction_hard_backpressure_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Error::CompactionBackpressure {
                    table,
                    segments: debt.segments,
                    physical_bytes: debt.physical_bytes,
                    hard_segments: self.l0_pressure_limits.hard_segments,
                    hard_bytes: self.l0_pressure_limits.hard_bytes,
                });
            }
            if !soft_wait_recorded {
                self.compaction_soft_backpressure_waits
                    .fetch_add(1, Ordering::Relaxed);
                soft_wait_recorded = true;
            }
            if self.l0_pressure_limits.soft_wait.is_zero()
                || started.elapsed() >= self.l0_pressure_limits.soft_wait
            {
                self.compaction_soft_backpressure_wait_millis.fetch_add(
                    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    Ordering::Relaxed,
                );
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub(super) fn touched_tables_exceed_seal_pressure(&self, table_names: &[SmartString]) -> bool {
        if table_names.is_empty() {
            return false;
        }
        let stores = self.version_stores.read().unwrap();
        let total_hot_bytes = stores.values().fold(0usize, |total, store| {
            total.saturating_add(store.committed_hot_bytes())
        });
        let candidates: SmallVec<[(SmartString, Arc<VersionStore>); 4]> = table_names
            .iter()
            .filter_map(|name| {
                stores
                    .get(name.as_str())
                    .map(|store| (name.clone(), Arc::clone(store)))
            })
            .collect();
        drop(stores);

        // Per-table thresholds alone do not bound a database with many small
        // hot tables.  The aggregate soft watermark starts the independent
        // worker before producers reach the hard admission watermark.
        if total_hot_bytes >= total_hot_soft_threshold(self.seal_hot_bytes_threshold) {
            return true;
        }

        let managers = self.segment_managers.read().unwrap();
        candidates.into_iter().any(|(name, store)| {
            let incremental = managers
                .get(name.as_str())
                .is_some_and(|manager| manager.has_segments());
            let byte_threshold = if incremental {
                self.seal_incremental_hot_bytes_threshold
            } else {
                self.seal_hot_bytes_threshold
            };
            store.committed_hot_bytes() >= byte_threshold
        })
    }

    /// Soft aggregate pressure runs in the background.  Foreground commits
    /// wait only when a table crossed its configured owner budget or the whole
    /// database reached the larger hard watermark; otherwise a small-table
    /// workload would pay one artifact publication and fsync on every commit.
    pub(super) fn hot_storage_requires_backpressure(&self) -> bool {
        let stores = self.version_stores.read().unwrap();
        let total_hot_bytes = stores.values().fold(0usize, |total, store| {
            total.saturating_add(store.committed_hot_bytes())
        });
        if total_hot_bytes >= total_hot_hard_threshold(self.seal_hot_bytes_threshold) {
            return true;
        }
        let candidates = stores
            .iter()
            .map(|(name, store)| (name.clone(), Arc::clone(store)))
            .collect::<Vec<_>>();
        drop(stores);

        let managers = self.segment_managers.read().unwrap();
        candidates.into_iter().any(|(name, store)| {
            let incremental = managers
                .get(name.as_str())
                .is_some_and(|manager| manager.has_segments());
            let byte_threshold = if incremental {
                self.seal_incremental_hot_bytes_threshold
            } else {
                self.seal_hot_bytes_threshold
            };
            store.committed_hot_bytes() >= byte_threshold
        })
    }
}
