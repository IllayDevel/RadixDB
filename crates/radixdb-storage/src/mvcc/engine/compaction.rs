use super::*;

struct CompactionStageContext<'a> {
    publication: &'a mut super::physical_publication::CompactionPublication,
    row_refs: &'a mut CompactionRowRefSpool,
    volumes: &'a [(u64, Arc<crate::volume::writer::FrozenVolume>)],
    vol_mappings: &'a [crate::volume::writer::ColumnMapping],
    block_cache: &'a mut crate::volume::writer::CompactionBlockCache,
    compress: bool,
    execution_budget: &'a mut CompactionExecutionBudget,
    output_headroom_path: &'a Path,
    ensure_live: &'a (dyn Fn() -> Result<()> + Send + Sync),
}

/// Return snapshot tombstones whose every currently published physical row
/// copy belongs to the selected compaction inputs.
///
/// A row id can legitimately occur in more than one immutable segment while
/// visibility selects its newest copy. Retiring only one such segment does not
/// resolve the tombstone: clearing it would expose a predecessor left in an
/// unselected segment. Candidate ids are bounded by the selected input rows;
/// unselected segments are consulted through resident sorted row-id metadata,
/// never through DATA payload reads or a table-wide row scan.
fn fully_retired_tombstone_rows(
    tombstone_snapshot: &FxHashMap<i64, u64>,
    snapshot_boundary: Option<u64>,
    selected_volumes: &[(u64, Arc<crate::volume::writer::FrozenVolume>)],
    current_segments: &FxHashMap<u64, crate::volume::manifest::ColdSegment>,
) -> FxHashSet<i64> {
    if tombstone_snapshot.is_empty() || selected_volumes.is_empty() {
        return FxHashSet::default();
    }

    let selected_segment_ids = selected_volumes
        .iter()
        .map(|(segment_id, _)| *segment_id)
        .collect::<FxHashSet<_>>();
    if selected_segment_ids
        .iter()
        .any(|segment_id| !current_segments.contains_key(segment_id))
    {
        // Publication must fail its live-token check before reaching this
        // state. Remain fail-closed if a future caller violates that order.
        return FxHashSet::default();
    }

    let mut candidates = selected_volumes
        .iter()
        .flat_map(|(_, volume)| volume.meta.row_ids.iter())
        .filter(|row_id| {
            tombstone_snapshot.get(row_id).is_some_and(|commit_seq| {
                snapshot_boundary.is_none_or(|boundary| *commit_seq < boundary)
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates.dedup();
    if candidates.is_empty() {
        return FxHashSet::default();
    }

    let mut retained_by_unselected_segment = FxHashSet::default();
    for (segment_id, segment) in current_segments {
        if selected_segment_ids.contains(segment_id) || segment.volume.meta.row_count == 0 {
            continue;
        }
        let row_ids = &segment.volume.meta.row_ids;
        let Some((minimum, maximum)) = row_ids.first().zip(row_ids.last()) else {
            continue;
        };
        let start = candidates.partition_point(|row_id| *row_id < minimum);
        let end = candidates.partition_point(|row_id| *row_id <= maximum);
        for row_id in &candidates[start..end] {
            if !retained_by_unselected_segment.contains(row_id)
                && row_ids.binary_search(row_id).is_ok()
            {
                retained_by_unselected_segment.insert(*row_id);
            }
        }
    }

    candidates
        .into_iter()
        .filter(|row_id| !retained_by_unselected_segment.contains(row_id))
        .collect()
}

impl MVCCEngine {
    /// Run compaction synchronously under the compaction_running flag.
    /// The flag prevents concurrent compaction from background and forced
    /// callers. Clears the flag on exit (including panics via drop guard).
    pub(super) fn run_compaction_guarded(&self) -> Result<()> {
        let _guard = AtomicBoolGuard(&self.compaction_running);
        let result = self.compact_volumes();
        if result.is_ok() {
            self.request_page_cache_warmup();
        }
        result
    }

    /// Run one bounded background wave under the compaction-running flag.
    ///
    /// A background owner deliberately processes at most one table per
    /// configured worker. Returning `true` asks the coordinator to schedule a
    /// fresh wave, which re-reads pressure and prevents an old whole-database
    /// candidate list from starving a newly urgent table.
    fn run_background_compaction_guarded(&self) -> Result<bool> {
        let _guard = AtomicBoolGuard(&self.compaction_running);
        let max_tables = self
            .config
            .read()
            .map(|config| config.persistence.max_compaction_jobs)
            .unwrap_or(crate::config::DEFAULT_MAX_COMPACTION_JOBS)
            .clamp(1, crate::config::MAX_COMPACTION_JOBS);
        let result = self.compact_volumes_limited(Some(max_tables));
        if result.is_ok() {
            self.request_page_cache_warmup();
        }
        result
    }

    /// Evict resident volume payload cache to the configured global budget.
    ///
    /// This is storage-cache pressure, not a generic runtime governor. Segment
    /// metadata/descriptors remain resident; only materialized eager columns
    /// are demoted to metadata-only artifact-backed volumes.
    pub(super) fn evict_idle_volumes(&self) {
        let epoch = self.eviction_epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let max_cache_bytes = {
            let config = self.config.read().unwrap();
            config.persistence.volume_cache_bytes
        };
        let mut managers: Vec<_> = {
            let mgrs = self.segment_managers.read().unwrap();
            mgrs.values()
                .map(|mgr| (Arc::clone(mgr), mgr.volume_cache_bytes()))
                .collect()
        };

        let total_cache_bytes = managers
            .iter()
            .fold(0usize, |acc, (_, bytes)| acc.saturating_add(*bytes));
        if total_cache_bytes <= max_cache_bytes {
            // Still run a no-op pass so last_access_epoch sentinels are reset
            // and PRAGMA VOLUME_STATS observes the new epoch.
            for (mgr, _) in managers {
                mgr.evict_volumes_to_budget(epoch, usize::MAX);
            }
            return;
        }

        let mut bytes_to_free = total_cache_bytes.saturating_sub(max_cache_bytes);
        managers.sort_by(|(_, a), (_, b)| b.cmp(a));
        for (mgr, table_cache_bytes) in managers {
            if bytes_to_free == 0 {
                mgr.evict_volumes_to_budget(epoch, usize::MAX);
                continue;
            }
            let table_target = table_cache_bytes.saturating_sub(bytes_to_free);
            let freed = mgr.evict_volumes_to_budget(epoch, table_target);
            bytes_to_free = bytes_to_free.saturating_sub(freed);
        }
    }

    /// Spawn compaction on a background thread. If compaction is already
    /// running, this is a no-op (the next checkpoint cycle will retry).
    /// Called from the background cleanup thread which owns Arc<Self>.
    pub(super) fn spawn_compaction(self: &Arc<Self>) {
        // Try to claim the compaction slot. If already running, skip
        // compaction but still run eviction on this thread.
        if self
            .compaction_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.evict_idle_volumes();
            return;
        }
        let engine = Arc::clone(self);
        std::thread::spawn(move || {
            let more_candidates = match engine.run_background_compaction_guarded() {
                Ok(more_candidates) => more_candidates,
                Err(error) => {
                    eprintln!("Warning: background compact_volumes failed: {}", error);
                    false
                }
            };
            if more_candidates || engine.l0_soft_pressure_exceeded() {
                engine.compaction_requested.store(true, Ordering::Release);
            }
            engine.evict_idle_volumes();
        });
    }

    /// Run compaction synchronously, waiting for any in-flight background
    /// compaction to finish first. Used by close/restore paths that must retire
    /// all background ownership before returning. Public CHECKPOINT only
    /// requests independent maintenance after its durability boundary.
    pub(super) fn compact_after_checkpoint_forced(&self) -> Result<()> {
        // Claim the compaction slot, waiting for any background compaction.
        // CAS loop: if background thread holds the flag, spin until it clears.
        // Once we claim it, no background thread can start a new compaction.
        while self
            .compaction_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        self.run_compaction_guarded()
    }

    pub(super) fn validate_compaction_job_live(
        &self,
        table_name: &str,
        manager: &crate::volume::manifest::SegmentManager,
        token: &crate::volume::manifest::CompactionToken,
        planned_schema_epoch: u64,
    ) -> Result<()> {
        let current_schema_epoch = self.schema_epoch.load(Ordering::Acquire);
        if current_schema_epoch != planned_schema_epoch {
            return Err(compaction_abort(
                "schema_epoch_changed",
                format_args!(
                    "table '{table_name}' planned at {planned_schema_epoch}, current {current_schema_epoch}"
                ),
            ));
        }
        manager
            .validate_compaction_token_live(token, current_schema_epoch)
            .map_err(|error| {
                compaction_abort(
                    "input_snapshot_changed",
                    format_args!("table '{table_name}': {error}"),
                )
            })
    }

    fn stage_compaction_rows_spooled_adaptively(
        context: CompactionStageContext<'_>,
    ) -> Result<Vec<super::physical_publication::StagedCompactionSegment>> {
        let CompactionStageContext {
            publication,
            row_refs,
            volumes,
            vol_mappings,
            block_cache,
            compress,
            execution_budget,
            output_headroom_path,
            ensure_live,
        } = context;
        let mut pending: Vec<Range<usize>> = std::iter::once(0..row_refs.len()).collect();
        let mut staged = Vec::new();
        while let Some(range) = pending.pop() {
            ensure_live()?;
            execution_budget.ensure_output_headroom(output_headroom_path)?;
            match publication.stage_rows(
                row_refs,
                range.clone(),
                volumes,
                vol_mappings,
                block_cache,
                compress,
            ) {
                Ok(output) => {
                    execution_budget.account_output_bytes(output.physical_bytes, ensure_live)?;
                    staged.push(output);
                    ensure_live()?;
                    std::thread::yield_now();
                }
                Err(error) if is_artifact_adaptive_capacity_error(&error) && range.len() > 1 => {
                    let half = range.len() / 2;
                    let aligned = (half / ROW_GROUP_SIZE) * ROW_GROUP_SIZE;
                    let split_at = range.start + if aligned == 0 { half } else { aligned };
                    pending.push(split_at..range.end);
                    pending.push(range.start..split_at);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(staged)
    }

    pub(super) fn compact_volumes(&self) -> Result<()> {
        self.compact_volumes_limited(None).map(|_| ())
    }

    fn compact_volumes_limited(&self, max_tables: Option<usize>) -> Result<bool> {
        // Compaction replaces the current physical owner bitmap. A Snapshot
        // beginning during that rewrite could otherwise pin the logical
        // frontier but read two different physical generations. Admission is
        // linearized by the exclusive side in begin_transaction_with_level(); an
        // already active Snapshot makes this maintenance wave a no-op.
        let _snapshot_maintenance = self.snapshot_maintenance_fence.read();
        if self.registry.get_min_snapshot_begin_seq().is_some() {
            return Ok(false);
        }

        // Planning and output construction deliberately do not hold the DDL
        // fence. The immutable schema/input token is revalidated at bounded
        // yield points; only final manifest publication takes the shared side.
        let compaction_started = Instant::now();
        let pm = match self.persistence() {
            Some(pm) if pm.is_enabled() => pm,
            _ => return Ok(false),
        };
        let compaction_runtime = self.runtime_maintenance.compaction.start();

        let (
            compact_threshold,
            target_volume_rows,
            max_compaction_jobs,
            max_compaction_input_segments,
            max_compaction_input_bytes,
            max_compaction_output_bytes,
            compaction_job_time_budget_ms,
            compaction_io_bytes_per_sec,
            compaction_disk_reserve_bytes,
            compaction_retry_cooldown_ms,
        ) = self
            .config
            .read()
            .map(|c| {
                (
                    c.persistence.compact_threshold as usize,
                    c.persistence.target_volume_rows,
                    c.persistence.max_compaction_jobs,
                    c.persistence.max_compaction_input_segments,
                    c.persistence.max_compaction_input_bytes,
                    c.persistence.max_compaction_output_bytes,
                    c.persistence.compaction_job_time_budget_ms,
                    c.persistence.compaction_io_bytes_per_sec,
                    c.persistence.compaction_disk_reserve_bytes,
                    c.persistence.compaction_retry_cooldown_ms,
                )
            })
            .unwrap_or((
                4,
                1_048_576,
                crate::config::DEFAULT_MAX_COMPACTION_JOBS,
                crate::config::DEFAULT_MAX_COMPACTION_INPUT_SEGMENTS,
                crate::config::DEFAULT_MAX_COMPACTION_INPUT_BYTES,
                crate::config::DEFAULT_MAX_COMPACTION_OUTPUT_BYTES,
                crate::config::DEFAULT_COMPACTION_JOB_TIME_BUDGET_MS,
                crate::config::DEFAULT_COMPACTION_IO_BYTES_PER_SEC,
                crate::config::DEFAULT_COMPACTION_DISK_RESERVE_BYTES,
                crate::config::DEFAULT_COMPACTION_RETRY_COOLDOWN_MS,
            ));
        let compact_threshold = compact_threshold.max(2);
        let tier_target_rows = size_tier_target_rows(target_volume_rows, compact_threshold);

        let pressure_limits = self.l0_pressure_limits();
        let mut tables_to_compact: Vec<String> = {
            let mgrs = self.segment_managers.read().unwrap();
            let mut candidates = mgrs
                .iter()
                .filter(|(_, mgr)| {
                    let manifest = mgr.manifest();
                    // L0 is always eligible for promotion. L1 remains eligible
                    // only below the size tier; artifact writers independently
                    // enforce their bounded physical ceilings.
                    let mut consecutive_mergeable = 0usize;
                    for segment in &manifest.segments {
                        let mergeable = match segment.level {
                            SegmentLevel::Unleveled | SegmentLevel::L0 => true,
                            SegmentLevel::L1 => {
                                compaction_segment_is_mergeable(segment.row_count, tier_target_rows)
                            }
                        };
                        if mergeable {
                            consecutive_mergeable = consecutive_mergeable.saturating_add(1);
                            if consecutive_mergeable >= compact_threshold {
                                return true;
                            }
                        } else {
                            consecutive_mergeable = 0;
                        }
                    }
                    // Any durable tombstone may dirty a small, at-target or
                    // oversized segment. The bounded planner below performs
                    // the exact row-id intersection before selecting work.
                    if !mgr.is_tombstone_set_empty() && mgr.segment_count() >= 1 {
                        return true;
                    }
                    // Split oversized volumes that exceed 150% of target.
                    let oversized_threshold = tier_target_rows.saturating_mul(3) / 2;
                    if mgr.max_segment_row_count() > oversized_threshold {
                        return true;
                    }
                    false
                })
                .map(|(name, manager)| (name.clone(), manager.l0_debt_snapshot()))
                .collect::<Vec<_>>();
            // One conservative worker must not inherit HashMap iteration as
            // its scheduling policy. Retire the table nearest a hard writer
            // stop first; otherwise a large but non-urgent job can let an
            // unrelated high-churn table cross its bounded L0 limit.
            prioritize_compaction_tables(&mut candidates, pressure_limits);
            candidates.into_iter().map(|(name, _)| name).collect()
        };

        if tables_to_compact.is_empty() {
            compaction_runtime.success();
            return Ok(false);
        }
        let more_candidates = limit_compaction_wave(&mut tables_to_compact, max_tables);
        let compaction_job_slots = max_compaction_jobs
            .clamp(1, crate::config::MAX_COMPACTION_JOBS)
            .min(tables_to_compact.len());
        // The configured rate is a process-wide maintenance budget, not a
        // per-worker multiplier. A lone table retains the complete budget;
        // concurrent table owners divide it evenly.
        let compaction_io_bytes_per_job =
            compaction_io_rate_per_job(compaction_io_bytes_per_sec, compaction_job_slots);
        let max_compaction_input_bytes = effective_compaction_input_budget(
            max_compaction_input_bytes,
            max_compaction_output_bytes,
            compaction_job_time_budget_ms,
            compaction_io_bytes_per_job,
        );
        let compaction_table_count = tables_to_compact.len() as u64;
        let disk_reservations = Arc::new(CompactionDiskReservationPool::default());

        let compact_table = |table_name: &str| -> Result<()> {
            let (schema, schema_version) = {
                let schema_version = self.schema_epoch.load(Ordering::Acquire);
                let schemas = self.schemas.read().unwrap();
                match schemas.get(table_name) {
                    Some(s) => (s.clone(), schema_version),
                    None => return Ok(()),
                }
            };

            let mgr = self.get_or_create_segment_manager(table_name);
            let artifact_bounded_target_rows = tier_target_rows;

            // Per-table snapshot gating: capture the current min snapshot begin_seq
            // for each table to close the TOCTOU window. A snapshot starting between
            // tables must not cause earlier compacted tables to lose visible rows.
            let compact_seal_seq_limit =
                self.registry.get_min_snapshot_begin_seq().map(|s| s as u64);

            // Targeted compaction: only rewrite volumes that need work.
            // At-target volumes are left untouched to minimize disk I/O.
            //
            // Categories:
            // - Mergeable: sub-target immutable DATA artifacts
            // - Oversized (> target * 3/2): large volumes to split
            // - At-target: properly sized, never rewrite
            let (old_ids, volumes, tombstones, compaction_token) = {
                let segment_generation = mgr.segment_generation();
                // Use segments_raw for planning — only metadata (row_ids,
                // row_count) is needed. Avoids reloading ALL cold volumes
                // for tables where only sub-target volumes need compaction.
                let segs = mgr.segments_raw();
                let manifest = mgr.manifest();
                let ts = mgr.tombstone_set_arc();

                let oversized_threshold = artifact_bounded_target_rows.saturating_mul(3) / 2;

                let mut candidates = Vec::new();
                for (idx, seg) in manifest.segments.iter().enumerate() {
                    // Skip volumes sealed after the earliest snapshot began.
                    // seal_seq = cutoff used during extraction. A volume with
                    // seal_seq <= limit contains only pre-snapshot data (safe).
                    // seal_seq > limit means the volume may have post-snapshot data.
                    if let Some(limit) = compact_seal_seq_limit {
                        if seg.seal_seq > 0 && seg.seal_seq > limit {
                            continue;
                        }
                    }
                    let Some(cold) = segs.get(&seg.segment_id) else {
                        continue;
                    };
                    let ordinary_tier_merge = match seg.level {
                        SegmentLevel::Unleveled | SegmentLevel::L0 => true,
                        SegmentLevel::L1 => compaction_segment_is_mergeable(
                            seg.row_count,
                            artifact_bounded_target_rows,
                        ),
                    };
                    let oversized = seg.row_count > oversized_threshold;
                    let mut tombstone_dirty = false;
                    let mut tombstone_blocked = false;
                    if !ts.is_empty() {
                        for row_id in cold.volume.meta.row_ids.iter() {
                            let Some(&commit_seq) = ts.get(&row_id) else {
                                continue;
                            };
                            if compact_seal_seq_limit.is_some_and(|limit| commit_seq >= limit) {
                                tombstone_blocked = true;
                                break;
                            }
                            tombstone_dirty = true;
                        }
                    }
                    // A snapshot older than this tombstone still needs the old
                    // physical row. Merging that row with a later reuse of the
                    // same UNIQUE key would create an invalid single-artifact
                    // posting. Keep the complete segment out of this contiguous
                    // run until every tombstone it owns is safe to resolve.
                    if tombstone_blocked {
                        continue;
                    }
                    let must_rewrite = oversized || tombstone_dirty;
                    if !ordinary_tier_merge && !must_rewrite {
                        continue;
                    }

                    candidates.push(CompactionCandidate {
                        manifest_index: idx,
                        physical_bytes: frozen_volume_physical_bytes(&cold.volume),
                        must_rewrite,
                    });
                }

                let merge_indices = select_bounded_compaction_run(
                    &candidates,
                    max_compaction_input_segments,
                    max_compaction_input_bytes,
                );
                if merge_indices.is_empty() {
                    return Ok(());
                }

                // Keep only merge-candidate volume metadata (not the entire table).
                // artifact-backed cold volumes stay metadata-only; compaction materializes rows
                // through the retained block source.
                let old_ids: Vec<u64> = merge_indices
                    .iter()
                    .map(|&i| manifest.segments[i].segment_id)
                    .collect();
                let compaction_token = mgr.capture_compaction_token_from_snapshot(
                    &manifest,
                    &segs,
                    &old_ids,
                    schema_version,
                    compact_seal_seq_limit,
                    crate::volume::manifest::SegmentLevel::L1,
                )?;
                let mut vols: Vec<(u64, Arc<crate::volume::writer::FrozenVolume>)> = merge_indices
                    .iter()
                    .filter_map(|&i| {
                        let seg = &manifest.segments[i];
                        let vol = segs.get(&seg.segment_id)?;
                        Some((seg.segment_id, Arc::clone(&vol.volume)))
                    })
                    .collect();
                drop(segs);

                // Every manifest entry must have a loaded volume.
                if vols.len() != old_ids.len() {
                    return Err(Error::internal(format!(
                        "cannot compact '{}': {} of {} required manifest segments are loaded",
                        table_name,
                        vols.len(),
                        old_ids.len()
                    )));
                }
                // Manifest position is the authoritative recency order. Walk
                // selected indices newest-first; segment IDs are identities,
                // not chronology after restore/replace operations.
                let rank: FxHashMap<u64, usize> = merge_indices
                    .iter()
                    .enumerate()
                    .map(|(rank, &index)| (manifest.segments[index].segment_id, rank))
                    .collect();
                vols.sort_by_key(|entry| {
                    std::cmp::Reverse(rank.get(&entry.0).copied().unwrap_or_default())
                });
                let ts = mgr.tombstone_set_arc();
                if mgr.segment_generation() != segment_generation {
                    return Err(Error::internal(format!(
                        "cannot plan compaction for '{}': segment topology changed during snapshot",
                        table_name
                    )));
                }
                (old_ids, Arc::new(vols), ts, compaction_token)
            };
            debug_assert_eq!(
                compaction_token.tombstone_boundary(),
                compact_seal_seq_limit
            );
            let compaction_reason = if volumes
                .iter()
                .any(|(_, volume)| volume.meta.row_count > target_volume_rows.saturating_mul(3) / 2)
            {
                "oversized_segment_split"
            } else if !tombstones.is_empty() {
                "tombstone_cleanup"
            } else {
                "sub_target_merge"
            };
            self.runtime_maintenance
                .compaction_cost
                .selected(compaction_reason);
            let retry_signature = compaction_job_signature(table_name, schema_version, &old_ids);
            if self.compaction_retry_cooldown.should_defer(retry_signature) {
                compaction_runtime.set_detail_with_reason(table_name, &old_ids, "retry_cooldown");
                self.runtime_maintenance
                    .compaction_cost
                    .deferred("retry_cooldown");
                return Ok(());
            }
            let mut retry_job = CompactionRetryJobGuard::new(
                &self.compaction_retry_cooldown,
                retry_signature,
                compaction_retry_cooldown_ms,
            );
            let ensure_live = || {
                self.validate_compaction_job_live(
                    table_name,
                    &mgr,
                    &compaction_token,
                    schema_version,
                )
            };
            compaction_runtime.set_detail_with_reason(table_name, &old_ids, compaction_reason);
            let compaction_input_rows = volumes.iter().fold(0_u64, |rows, (_, volume)| {
                rows.saturating_add(volume.meta.row_count as u64)
            });
            let compaction_input_bytes = volumes.iter().fold(0_u64, |bytes, (_, volume)| {
                bytes.saturating_add(frozen_volume_physical_bytes(volume))
            });
            compaction_runtime.add_input(compaction_input_rows, compaction_input_bytes);
            let _job_concurrency = self.compaction_job_concurrency.start_job();
            let mut compaction_cost_job = self
                .runtime_maintenance
                .compaction_cost
                .start_job(compaction_input_rows, compaction_input_bytes);
            let storage_root = pm.path();
            let mut publication = self.begin_compaction_publication(table_name, &volumes)?;
            let scratch_root = publication.scratch_root();
            let disk_reservation = match disk_reservations.reserve(
                storage_root,
                max_compaction_output_bytes,
                compaction_disk_reserve_bytes,
            ) {
                Ok(reservation) => reservation,
                Err(error) => {
                    let reason =
                        compaction_abort_reason(&error).unwrap_or("budget_admission_failed");
                    retry_job.set_reason(reason);
                    compaction_cost_job.invalidated(reason);
                    return Err(error);
                }
            };
            let mut execution_budget = CompactionExecutionBudget::new(
                compaction_job_time_budget_ms,
                compaction_io_bytes_per_job,
                max_compaction_output_bytes,
                compaction_disk_reserve_bytes,
            );
            execution_budget.attach_disk_reservation(disk_reservation);
            if let Err(error) = execution_budget.ensure_output_headroom(storage_root) {
                let reason = compaction_abort_reason(&error).unwrap_or("budget_admission_failed");
                retry_job.set_reason(reason);
                compaction_cost_job.invalidated(reason);
                return Err(error);
            }
            #[cfg(any(test, feature = "test-failpoints"))]
            crate::test_failpoints::interleave_scoped(
                self.schema_scope_id,
                crate::test_failpoints::InterleavePoint::CompactionBeforeOutput,
                old_ids
                    .first()
                    .copied()
                    .and_then(|segment_id| i64::try_from(segment_id).ok())
                    .unwrap_or(i64::MAX),
            );

            // Build compacted volumes with a k-way row_id stream. Each input
            // volume is already sorted by row_id (artifact-backed seal enforces that
            // invariant). The heap emits row_ids in ascending order without
            // collecting every `(row_id, volume, row)` reference and sorting it.
            //
            // Volumes are ordered newest-first above, so for duplicate row_ids
            // the smallest vol_idx wins.
            let compress = self
                .config
                .read()
                .map(|c| c.persistence.volume_compression)
                .unwrap_or(true);
            // Precompute column mapping per volume (once each, not per row).
            let vol_mappings: Vec<crate::volume::writer::ColumnMapping> = volumes
                .iter()
                .map(|(seg_id, _volume)| mgr.get_volume_mapping(*seg_id, &schema))
                .collect();

            // Split the streamed output rows into target-sized chunks and stage
            // one canonical DATA/INDEX pair per chunk. Row refs for the current
            // output chunk are spooled inside this publication's private staging
            // directory instead of retained as a target-sized Vec in RAM.
            // Row-group aligned split: round to 64K boundary so every volume
            // has complete row groups. Optimal for LZ4 compression and zone maps.
            let row_group_size = 65_536usize;
            let chunk_size =
                (artifact_bounded_target_rows / row_group_size).max(1) * row_group_size;
            let mut staged_outputs = Vec::new();
            let mut write_error: Option<Error> = None;
            let mut compaction_block_cache = crate::volume::writer::CompactionBlockCache::default();
            let mut heap =
                std::collections::BinaryHeap::<std::cmp::Reverse<(i64, usize, usize)>>::new();
            let mut examined_rows = 0_u64;

            for (vol_idx, (_seg_id, vol)) in volumes.iter().enumerate() {
                if let Some(row_id) = vol.meta.row_ids.first() {
                    heap.push(std::cmp::Reverse((row_id, vol_idx, 0usize)));
                }
            }

            let mut compaction_row_refs = CompactionRowRefSpool::create(&scratch_root, table_name)
                .map_err(|error| {
                    Error::internal(format!(
                        "failed to create compacted row ref spool for '{}': {}",
                        table_name, error
                    ))
                })?;

            while let Some(std::cmp::Reverse((row_id, vol_idx, row_idx))) = heap.pop() {
                examined_rows = examined_rows.saturating_add(1);
                if examined_rows.is_multiple_of(ROW_GROUP_SIZE as u64) {
                    let accounted_read_bytes = (u128::from(compaction_input_bytes)
                        .saturating_mul(u128::from(examined_rows.min(compaction_input_rows)))
                        / u128::from(compaction_input_rows.max(1)))
                    .min(u128::from(u64::MAX))
                        as u64;
                    if let Err(error) =
                        execution_budget.account_read_bytes(accounted_read_bytes, &ensure_live)
                    {
                        write_error = Some(error);
                        break;
                    }
                    std::thread::yield_now();
                }
                let mut best = (vol_idx, row_idx);

                let next_idx = row_idx + 1;
                let vol = &volumes[vol_idx].1;
                if next_idx < vol.meta.row_count {
                    heap.push(std::cmp::Reverse((
                        vol.meta.row_ids.at(next_idx),
                        vol_idx,
                        next_idx,
                    )));
                }

                while matches!(heap.peek(), Some(std::cmp::Reverse((next_row_id, _, _))) if *next_row_id == row_id)
                {
                    let std::cmp::Reverse((_, same_vol_idx, same_row_idx)) =
                        heap.pop().expect("peeked heap entry");
                    if same_vol_idx < best.0 {
                        best = (same_vol_idx, same_row_idx);
                    }

                    let next_idx = same_row_idx + 1;
                    let vol = &volumes[same_vol_idx].1;
                    if next_idx < vol.meta.row_count {
                        heap.push(std::cmp::Reverse((
                            vol.meta.row_ids.at(next_idx),
                            same_vol_idx,
                            next_idx,
                        )));
                    }
                }

                if tombstones.get(&row_id).is_some_and(|commit_seq| {
                    compact_seal_seq_limit.is_none_or(|limit| *commit_seq < limit)
                }) {
                    continue;
                }

                let row_ref = match CompactionRowRef::new(row_id, best.0, best.1) {
                    Ok(row_ref) => row_ref,
                    Err(e) => {
                        write_error = Some(Error::internal(format!(
                            "failed to build compacted row reference for '{}': {}",
                            table_name, e
                        )));
                        break;
                    }
                };
                if let Err(e) = compaction_row_refs.append(row_ref) {
                    write_error = Some(Error::internal(format!(
                        "failed to append compacted row reference for '{}': {}",
                        table_name, e
                    )));
                    break;
                }

                if compaction_row_refs.len() >= chunk_size {
                    match Self::stage_compaction_rows_spooled_adaptively(CompactionStageContext {
                        publication: &mut publication,
                        row_refs: &mut compaction_row_refs,
                        volumes: volumes.as_slice(),
                        vol_mappings: &vol_mappings,
                        block_cache: &mut compaction_block_cache,
                        compress,
                        execution_budget: &mut execution_budget,
                        output_headroom_path: storage_root,
                        ensure_live: &ensure_live,
                    }) {
                        Ok(staged) => {
                            staged_outputs.extend(staged);
                            compaction_row_refs = match CompactionRowRefSpool::create(
                                &scratch_root,
                                table_name,
                            ) {
                                Ok(spool) => spool,
                                Err(e) => {
                                    write_error = Some(Error::internal(format!(
                                        "failed to create next compacted row ref spool for '{}': {}",
                                        table_name, e
                                    )));
                                    break;
                                }
                            };
                        }
                        Err(e) => {
                            write_error = Some(Error::internal(format!(
                                "failed to write compacted volume for '{}': {}",
                                table_name, e
                            )));
                            break;
                        }
                    }
                }
            }

            if write_error.is_none() {
                if let Err(error) =
                    execution_budget.account_read_bytes(compaction_input_bytes, &ensure_live)
                {
                    write_error = Some(error);
                }
            }

            if write_error.is_none() && !compaction_row_refs.is_empty() {
                match Self::stage_compaction_rows_spooled_adaptively(CompactionStageContext {
                    publication: &mut publication,
                    row_refs: &mut compaction_row_refs,
                    volumes: volumes.as_slice(),
                    vol_mappings: &vol_mappings,
                    block_cache: &mut compaction_block_cache,
                    compress,
                    execution_budget: &mut execution_budget,
                    output_headroom_path: storage_root,
                    ensure_live: &ensure_live,
                }) {
                    Ok(staged) => {
                        staged_outputs.extend(staged);
                    }
                    Err(error) => {
                        write_error = Some(Error::internal(format!(
                            "failed to write compacted volume for '{}': {}",
                            table_name, error
                        )));
                    }
                }
            }

            if let Some(error) = write_error {
                let partial_output_rows = staged_outputs
                    .iter()
                    .fold(0_u64, |rows, output| rows.saturating_add(output.row_count));
                let partial_output_bytes = staged_outputs.iter().fold(0_u64, |bytes, output| {
                    bytes.saturating_add(output.physical_bytes)
                });
                let partial_posting_outputs = staged_outputs
                    .iter()
                    .filter(|output| output.has_index)
                    .count() as u64;
                compaction_cost_job.set_generated_output(
                    partial_output_rows,
                    partial_output_bytes,
                    partial_posting_outputs,
                );
                if let Some(reason) = compaction_abort_reason(&error) {
                    retry_job.set_reason(reason);
                    compaction_cost_job.invalidated(reason);
                } else {
                    retry_job.set_reason("output_write_failed");
                    compaction_cost_job.failed("output_write_failed");
                }
                return Err(error);
            }

            let compaction_output_rows = staged_outputs
                .iter()
                .fold(0_u64, |rows, output| rows.saturating_add(output.row_count));
            let compaction_output_bytes = staged_outputs.iter().fold(0_u64, |bytes, output| {
                bytes.saturating_add(output.physical_bytes)
            });
            let compaction_posting_outputs = staged_outputs
                .iter()
                .filter(|output| output.has_index)
                .count() as u64;
            compaction_cost_job.set_generated_output(
                compaction_output_rows,
                compaction_output_bytes,
                compaction_posting_outputs,
            );

            if let Err(error) = ensure_live() {
                let reason = compaction_abort_reason(&error).unwrap_or("prepublication_cancelled");
                retry_job.set_reason(reason);
                compaction_cost_job.invalidated(reason);
                return Err(error);
            }

            if staged_outputs.is_empty() {
                compaction_runtime.add_reclaimed(compaction_input_rows, compaction_input_bytes);
            } else {
                compaction_runtime.add_output(compaction_output_rows, compaction_output_bytes);
                compaction_runtime.add_reclaimed(
                    compaction_input_rows.saturating_sub(compaction_output_rows),
                    compaction_input_bytes.saturating_sub(compaction_output_bytes),
                );
            }

            // Reserve process-local runtime identities before the durable commit.
            // The physical SegmentId remains the authority across reopen; harmless
            // gaps in this allocator are preferable to a post-CONTROL allocation
            // failure while installing the already committed runtime graph.
            let mut replacement_segment_ids = Vec::with_capacity(staged_outputs.len());
            for _ in 0..staged_outputs.len() {
                replacement_segment_ids.push(mgr.reserve_runtime_segment_id());
            }
            drop(compaction_row_refs);
            let manifest_publication_started = Instant::now();
            #[cfg(any(test, feature = "test-failpoints"))]
            crate::test_failpoints::interleave_scoped(
                self.schema_scope_id,
                crate::test_failpoints::InterleavePoint::CompactionBeforePublish,
                old_ids
                    .first()
                    .copied()
                    .and_then(|segment_id| i64::try_from(segment_id).ok())
                    .unwrap_or(i64::MAX),
            );
            let _ddl_generation_guard = DdlFenceGuard::shared(Arc::clone(&self.ddl_fence));
            // Artifact construction is deliberately optimistic and runs
            // outside the checkpoint coordinator. Its short commit phase must
            // nevertheless serialize with checkpoint and pressure-seal
            // publication. Otherwise a completed compaction can advance
            // CONTROL between their source pin and publication, invalidating
            // an entire seal pass or a frozen WAL checkpoint. Taking the
            // coordinator only here preserves concurrent build work while the
            // existing live-token check turns a superseded compaction into a
            // bounded replan.
            let _checkpoint_guard = self.lock_checkpoint_mutex_profiled();
            let _segment_publication_guard = mgr.acquire_seal_write();
            if let Err(error) = ensure_live() {
                let reason = compaction_abort_reason(&error).unwrap_or("prepublication_cancelled");
                retry_job.set_reason(reason);
                compaction_cost_job.invalidated(reason);
                return Err(error);
            }

            // The replacement DATA omits tombstoned rows from the selected
            // inputs. Publish the corresponding exact tombstone replacement in
            // the same CONTROL generation, otherwise a close/reopen between
            // compaction and the next checkpoint can resurrect the obsolete
            // durable tombstone set. The seal-write fence keeps commits from
            // changing this map until the physical and runtime graphs agree.
            let current_tombstones = mgr.tombstone_set_arc();
            let current_segments = mgr.segments_raw();
            let resolved_tombstone_rows = fully_retired_tombstone_rows(
                &tombstones,
                compact_seal_seq_limit,
                volumes.as_slice(),
                &current_segments,
            );
            let tombstone_was_resolved = |row_id: i64, commit_seq: u64| {
                matches!(
                    tombstones.get(&row_id),
                    Some(snapshot_seq) if *snapshot_seq == commit_seq
                ) && compact_seal_seq_limit.is_none_or(|limit| commit_seq < limit)
                    && resolved_tombstone_rows.contains(&row_id)
            };
            let mut post_compaction_tombstones = (*current_tombstones).clone();
            post_compaction_tombstones
                .retain(|row_id, commit_seq| !tombstone_was_resolved(*row_id, *commit_seq));
            let tombstones_changed = post_compaction_tombstones.len() != current_tombstones.len();
            let tombstone_target = tombstones_changed.then_some(&post_compaction_tombstones);
            #[cfg(any(test, feature = "test-failpoints"))]
            crate::test_failpoints::interleave_scoped(
                self.schema_scope_id,
                crate::test_failpoints::InterleavePoint::CompactionTombstonesPrepared,
                old_ids
                    .first()
                    .copied()
                    .and_then(|segment_id| i64::try_from(segment_id).ok())
                    .unwrap_or(i64::MAX),
            );

            let published_segments = match publication.publish(self, tombstone_target) {
                Ok(segments) => segments,
                Err(error) => {
                    if compaction_publication_is_stale(&error) {
                        // This is an expected optimistic publication race, not
                        // a persistent failure of the selected inputs. Do not
                        // poison their retry signature with a cooldown: the
                        // scheduler will immediately re-plan from current
                        // CONTROL within its bounded retry allowance.
                        retry_job.success();
                        compaction_cost_job.invalidated(COMPACTION_STALE_PUBLICATION_REASON);
                    } else {
                        retry_job.set_reason("physical_publication_rejected");
                        compaction_cost_job.invalidated("physical_publication_rejected");
                    }
                    return Err(error);
                }
            };
            if published_segments.len() != replacement_segment_ids.len() {
                retry_job.set_reason("runtime_output_cardinality_mismatch");
                compaction_cost_job.failed("runtime_output_cardinality_mismatch");
                return Err(Error::internal(
                    "published compaction output count differs from reserved runtime identities",
                ));
            }
            let new_volumes = replacement_segment_ids
                .into_iter()
                .zip(published_segments)
                .map(|(segment_id, published)| {
                    let mut registration = self.segment_registration(
                        table_name,
                        published.volume,
                        segment_id,
                        0,
                        schema_version,
                    );
                    registration.meta.level = SegmentLevel::L1;
                    (segment_id, registration.volume, registration.meta)
                })
                .collect::<Vec<_>>();
            let topology_result = if new_volumes.is_empty() {
                mgr.replace_segments_atomic_remove_only_compaction_checked(
                    &compaction_token,
                    schema_version,
                )
            } else {
                mgr.replace_segments_atomic_multi_compaction_checked(
                    new_volumes,
                    &compaction_token,
                    schema_version,
                )
            };
            if let Err(error) = topology_result {
                retry_job.set_reason("topology_publication_rejected");
                compaction_cost_job.failed("topology_publication_rejected");
                return Err(Error::internal(format!(
                    "physical compaction committed but runtime topology installation failed for '{}': {}",
                    table_name, error
                )));
            }

            // Clear only tombstones that existed at snapshot time for
            // row_ids whose every physical copy was retired by this bounded
            // replacement.
            mgr.remove_tombstones_matching_snapshot_where(&tombstones, |row_id| {
                tombstones.get(&row_id).is_some_and(|commit_seq| {
                    compact_seal_seq_limit.is_none_or(|limit| *commit_seq < limit)
                }) && resolved_tombstone_rows.contains(&row_id)
            });

            let installed_tombstones = mgr.tombstone_set_arc();
            if installed_tombstones.as_ref() != &post_compaction_tombstones {
                retry_job.set_reason("runtime_tombstone_replacement_mismatch");
                compaction_cost_job.failed("runtime_tombstone_replacement_mismatch");
                return Err(Error::internal(format!(
                    "physical compaction committed but runtime tombstone replacement differed for '{}'",
                    table_name
                )));
            }
            if tombstones_changed {
                let published = mgr.tombstone_publication_snapshot().ok_or_else(|| {
                    Error::internal(format!(
                        "physical compaction committed but tombstone generation was not dirty for '{}'",
                        table_name
                    ))
                })?;
                if published.tombstones() != &post_compaction_tombstones {
                    retry_job.set_reason("runtime_tombstone_snapshot_mismatch");
                    compaction_cost_job.failed("runtime_tombstone_snapshot_mismatch");
                    return Err(Error::internal(format!(
                        "physical compaction committed but runtime tombstone snapshot differed for '{}'",
                        table_name
                    )));
                }
                mgr.confirm_tombstone_publication(published.generation());
            }

            compaction_cost_job.published(manifest_publication_started.elapsed());
            retry_job.success();
            Ok(())
        };

        run_bounded_compaction_jobs(&tables_to_compact, compaction_job_slots, &compact_table)?;

        instrumentation::record_compaction(compaction_table_count, compaction_started.elapsed());
        compaction_runtime.success();
        Ok(more_candidates)
    }
}
