use super::*;

pub(super) struct PendingTable {
    pub(super) owner_txn_id: i64,
    pub(super) version_store: Arc<VersionStore>,
}

/// Authoritative engine startup/shutdown state exposed to lifecycle consumers.
#[derive(Clone, Debug)]
pub enum EngineLifecycleState {
    Closed,
    Opening,
    Ready,
    Closing,
    CloseFailed(Error),
    Failed(Error),
}

/// Bounded identity of the physical owners currently touched by one
/// maintenance operation. Table names are represented by stable numeric IDs;
/// segment IDs are already non-secret physical identities.
#[derive(Clone, Debug, Default, Serialize)]
pub struct EngineRuntimeOperationDetail {
    pub table_id: Option<u64>,
    pub segment_ids: Vec<u64>,
    pub segment_ids_truncated: bool,
    pub reason: String,
}

/// One operation class in the engine runtime snapshot.
#[derive(Clone, Debug, Default, Serialize)]
pub struct EngineRuntimeOperationSnapshot {
    pub active: bool,
    pub epoch: u64,
    pub started_unix_millis: u64,
    pub elapsed_millis: u64,
    pub calls: u64,
    pub completed: u64,
    pub failed: u64,
    pub last_success_unix_millis: u64,
    pub last_failure_unix_millis: u64,
    pub last_duration_nanos: u64,
    pub current_input_rows: u64,
    pub current_input_bytes: u64,
    pub current_output_rows: u64,
    pub current_output_bytes: u64,
    pub current_reclaimed_rows: u64,
    pub current_reclaimed_bytes: u64,
    pub current_rewrite_amplification: f64,
    pub last_input_rows: u64,
    pub last_input_bytes: u64,
    pub last_output_rows: u64,
    pub last_output_bytes: u64,
    pub last_reclaimed_rows: u64,
    pub last_reclaimed_bytes: u64,
    pub last_rewrite_amplification: f64,
    pub total_input_rows: u64,
    pub total_input_bytes: u64,
    pub total_output_rows: u64,
    pub total_output_bytes: u64,
    pub total_reclaimed_rows: u64,
    pub total_reclaimed_bytes: u64,
    /// Operation-specific monotonic marker. Checkpoint publishes its LSN here.
    pub last_result_marker: u64,
    pub detail: EngineRuntimeOperationDetail,
}

/// Logical cost ledger for table-local compaction jobs.
///
/// `EngineRuntimeOperationSnapshot` describes whole maintenance cycles. This
/// ledger deliberately separates useful publication from work discarded by a
/// stale topology/schema decision, which is the distinction needed to diagnose
/// size-dependent ingest stalls.
#[derive(Clone, Debug, Default, Serialize)]
pub struct EngineCompactionCostSnapshot {
    pub jobs_selected_sub_target_merge: u64,
    pub jobs_selected_tombstone_cleanup: u64,
    pub jobs_selected_oversized_segment_split: u64,
    pub jobs_planned: u64,
    pub jobs_deferred: u64,
    pub jobs_waited_retry_cooldown: u64,
    pub jobs_waited_other: u64,
    pub jobs_published: u64,
    pub jobs_invalidated: u64,
    pub jobs_invalidated_topology: u64,
    pub jobs_invalidated_other: u64,
    pub jobs_cancelled_schema_epoch: u64,
    pub jobs_cancelled_input_snapshot: u64,
    pub jobs_cancelled_budget: u64,
    pub jobs_cancelled_other: u64,
    pub jobs_failed: u64,
    pub total_logical_input_rows: u64,
    pub total_logical_input_bytes: u64,
    pub total_generated_output_rows: u64,
    pub total_generated_output_bytes: u64,
    pub total_published_input_rows: u64,
    pub total_published_input_bytes: u64,
    pub total_published_output_rows: u64,
    pub total_published_output_bytes: u64,
    pub total_wasted_input_rows: u64,
    pub total_wasted_input_bytes: u64,
    pub total_wasted_output_rows: u64,
    pub total_wasted_output_bytes: u64,
    pub posting_outputs_generated: u64,
    pub posting_outputs_published: u64,
    pub posting_outputs_wasted: u64,
    pub manifest_publications: u64,
    pub total_manifest_publication_nanos: u64,
    pub last_manifest_publication_nanos: u64,
    pub last_selection_reason: String,
    pub last_wait_reason: String,
    pub last_cancellation_reason: String,
    pub last_invalidation_reason: String,
    pub last_outcome: String,
}

#[derive(Debug)]
pub(super) struct EngineCompactionCostState {
    jobs_selected_sub_target_merge: AtomicU64,
    jobs_selected_tombstone_cleanup: AtomicU64,
    jobs_selected_oversized_segment_split: AtomicU64,
    jobs_planned: AtomicU64,
    jobs_deferred: AtomicU64,
    jobs_waited_retry_cooldown: AtomicU64,
    jobs_waited_other: AtomicU64,
    jobs_published: AtomicU64,
    jobs_invalidated: AtomicU64,
    jobs_invalidated_topology: AtomicU64,
    jobs_invalidated_other: AtomicU64,
    jobs_cancelled_schema_epoch: AtomicU64,
    jobs_cancelled_input_snapshot: AtomicU64,
    jobs_cancelled_budget: AtomicU64,
    jobs_cancelled_other: AtomicU64,
    jobs_failed: AtomicU64,
    total_logical_input_rows: AtomicU64,
    total_logical_input_bytes: AtomicU64,
    total_generated_output_rows: AtomicU64,
    total_generated_output_bytes: AtomicU64,
    total_published_input_rows: AtomicU64,
    total_published_input_bytes: AtomicU64,
    total_published_output_rows: AtomicU64,
    total_published_output_bytes: AtomicU64,
    total_wasted_input_rows: AtomicU64,
    total_wasted_input_bytes: AtomicU64,
    total_wasted_output_rows: AtomicU64,
    total_wasted_output_bytes: AtomicU64,
    posting_outputs_generated: AtomicU64,
    posting_outputs_published: AtomicU64,
    posting_outputs_wasted: AtomicU64,
    manifest_publications: AtomicU64,
    total_manifest_publication_nanos: AtomicU64,
    last_manifest_publication_nanos: AtomicU64,
    last_selection_reason: ArcSwap<String>,
    last_wait_reason: ArcSwap<String>,
    last_cancellation_reason: ArcSwap<String>,
    last_invalidation_reason: ArcSwap<String>,
    last_outcome: ArcSwap<String>,
}

impl EngineCompactionCostState {
    pub(super) fn new() -> Self {
        Self {
            jobs_selected_sub_target_merge: AtomicU64::new(0),
            jobs_selected_tombstone_cleanup: AtomicU64::new(0),
            jobs_selected_oversized_segment_split: AtomicU64::new(0),
            jobs_planned: AtomicU64::new(0),
            jobs_deferred: AtomicU64::new(0),
            jobs_waited_retry_cooldown: AtomicU64::new(0),
            jobs_waited_other: AtomicU64::new(0),
            jobs_published: AtomicU64::new(0),
            jobs_invalidated: AtomicU64::new(0),
            jobs_invalidated_topology: AtomicU64::new(0),
            jobs_invalidated_other: AtomicU64::new(0),
            jobs_cancelled_schema_epoch: AtomicU64::new(0),
            jobs_cancelled_input_snapshot: AtomicU64::new(0),
            jobs_cancelled_budget: AtomicU64::new(0),
            jobs_cancelled_other: AtomicU64::new(0),
            jobs_failed: AtomicU64::new(0),
            total_logical_input_rows: AtomicU64::new(0),
            total_logical_input_bytes: AtomicU64::new(0),
            total_generated_output_rows: AtomicU64::new(0),
            total_generated_output_bytes: AtomicU64::new(0),
            total_published_input_rows: AtomicU64::new(0),
            total_published_input_bytes: AtomicU64::new(0),
            total_published_output_rows: AtomicU64::new(0),
            total_published_output_bytes: AtomicU64::new(0),
            total_wasted_input_rows: AtomicU64::new(0),
            total_wasted_input_bytes: AtomicU64::new(0),
            total_wasted_output_rows: AtomicU64::new(0),
            total_wasted_output_bytes: AtomicU64::new(0),
            posting_outputs_generated: AtomicU64::new(0),
            posting_outputs_published: AtomicU64::new(0),
            posting_outputs_wasted: AtomicU64::new(0),
            manifest_publications: AtomicU64::new(0),
            total_manifest_publication_nanos: AtomicU64::new(0),
            last_manifest_publication_nanos: AtomicU64::new(0),
            last_selection_reason: ArcSwap::from_pointee(String::new()),
            last_wait_reason: ArcSwap::from_pointee(String::new()),
            last_cancellation_reason: ArcSwap::from_pointee(String::new()),
            last_invalidation_reason: ArcSwap::from_pointee(String::new()),
            last_outcome: ArcSwap::from_pointee(String::new()),
        }
    }

    pub(super) fn selected(&self, reason: &str) {
        match reason {
            "sub_target_merge" => &self.jobs_selected_sub_target_merge,
            "tombstone_cleanup" => &self.jobs_selected_tombstone_cleanup,
            "oversized_segment_split" => &self.jobs_selected_oversized_segment_split,
            _ => return,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.set_reason(&self.last_selection_reason, reason);
    }

    pub(super) fn start_job(
        &self,
        input_rows: u64,
        input_bytes: u64,
    ) -> CompactionCostJobGuard<'_> {
        self.jobs_planned.fetch_add(1, Ordering::Relaxed);
        self.total_logical_input_rows
            .fetch_add(input_rows, Ordering::Relaxed);
        self.total_logical_input_bytes
            .fetch_add(input_bytes, Ordering::Relaxed);
        CompactionCostJobGuard {
            state: self,
            input_rows,
            input_bytes,
            output_rows: 0,
            output_bytes: 0,
            posting_outputs: 0,
            finished: false,
        }
    }

    pub(super) fn deferred(&self, reason: &str) {
        self.jobs_deferred.fetch_add(1, Ordering::Relaxed);
        match reason {
            "retry_cooldown" => &self.jobs_waited_retry_cooldown,
            _ => &self.jobs_waited_other,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.set_reason(&self.last_wait_reason, reason);
        self.set_outcome(reason);
    }

    pub(super) fn snapshot(&self) -> EngineCompactionCostSnapshot {
        EngineCompactionCostSnapshot {
            jobs_selected_sub_target_merge: self
                .jobs_selected_sub_target_merge
                .load(Ordering::Relaxed),
            jobs_selected_tombstone_cleanup: self
                .jobs_selected_tombstone_cleanup
                .load(Ordering::Relaxed),
            jobs_selected_oversized_segment_split: self
                .jobs_selected_oversized_segment_split
                .load(Ordering::Relaxed),
            jobs_planned: self.jobs_planned.load(Ordering::Relaxed),
            jobs_deferred: self.jobs_deferred.load(Ordering::Relaxed),
            jobs_waited_retry_cooldown: self.jobs_waited_retry_cooldown.load(Ordering::Relaxed),
            jobs_waited_other: self.jobs_waited_other.load(Ordering::Relaxed),
            jobs_published: self.jobs_published.load(Ordering::Relaxed),
            jobs_invalidated: self.jobs_invalidated.load(Ordering::Relaxed),
            jobs_invalidated_topology: self.jobs_invalidated_topology.load(Ordering::Relaxed),
            jobs_invalidated_other: self.jobs_invalidated_other.load(Ordering::Relaxed),
            jobs_cancelled_schema_epoch: self.jobs_cancelled_schema_epoch.load(Ordering::Relaxed),
            jobs_cancelled_input_snapshot: self
                .jobs_cancelled_input_snapshot
                .load(Ordering::Relaxed),
            jobs_cancelled_budget: self.jobs_cancelled_budget.load(Ordering::Relaxed),
            jobs_cancelled_other: self.jobs_cancelled_other.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            total_logical_input_rows: self.total_logical_input_rows.load(Ordering::Relaxed),
            total_logical_input_bytes: self.total_logical_input_bytes.load(Ordering::Relaxed),
            total_generated_output_rows: self.total_generated_output_rows.load(Ordering::Relaxed),
            total_generated_output_bytes: self.total_generated_output_bytes.load(Ordering::Relaxed),
            total_published_input_rows: self.total_published_input_rows.load(Ordering::Relaxed),
            total_published_input_bytes: self.total_published_input_bytes.load(Ordering::Relaxed),
            total_published_output_rows: self.total_published_output_rows.load(Ordering::Relaxed),
            total_published_output_bytes: self.total_published_output_bytes.load(Ordering::Relaxed),
            total_wasted_input_rows: self.total_wasted_input_rows.load(Ordering::Relaxed),
            total_wasted_input_bytes: self.total_wasted_input_bytes.load(Ordering::Relaxed),
            total_wasted_output_rows: self.total_wasted_output_rows.load(Ordering::Relaxed),
            total_wasted_output_bytes: self.total_wasted_output_bytes.load(Ordering::Relaxed),
            posting_outputs_generated: self.posting_outputs_generated.load(Ordering::Relaxed),
            posting_outputs_published: self.posting_outputs_published.load(Ordering::Relaxed),
            posting_outputs_wasted: self.posting_outputs_wasted.load(Ordering::Relaxed),
            manifest_publications: self.manifest_publications.load(Ordering::Relaxed),
            total_manifest_publication_nanos: self
                .total_manifest_publication_nanos
                .load(Ordering::Relaxed),
            last_manifest_publication_nanos: self
                .last_manifest_publication_nanos
                .load(Ordering::Relaxed),
            last_selection_reason: self.last_selection_reason.load_full().as_ref().clone(),
            last_wait_reason: self.last_wait_reason.load_full().as_ref().clone(),
            last_cancellation_reason: self.last_cancellation_reason.load_full().as_ref().clone(),
            last_invalidation_reason: self.last_invalidation_reason.load_full().as_ref().clone(),
            last_outcome: self.last_outcome.load_full().as_ref().clone(),
        }
    }

    pub(super) fn set_outcome(&self, outcome: &str) {
        self.set_reason(&self.last_outcome, outcome);
    }

    pub(super) fn set_reason(&self, target: &ArcSwap<String>, reason: &str) {
        target.store(Arc::new(reason.chars().take(96).collect::<String>()));
    }

    pub(super) fn record_invalidation(&self, reason: &str) {
        match reason {
            "topology_publication_rejected" => &self.jobs_invalidated_topology,
            _ => &self.jobs_invalidated_other,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.set_reason(&self.last_invalidation_reason, reason);

        let cancellation = match reason {
            "schema_epoch_changed" => Some(&self.jobs_cancelled_schema_epoch),
            "input_snapshot_changed" => Some(&self.jobs_cancelled_input_snapshot),
            "time_budget_exceeded"
            | "output_budget_exceeded"
            | "disk_reserve_exhausted"
            | "budget_admission_failed" => Some(&self.jobs_cancelled_budget),
            "prepublication_cancelled" => Some(&self.jobs_cancelled_other),
            _ => None,
        };
        if let Some(counter) = cancellation {
            counter.fetch_add(1, Ordering::Relaxed);
            self.set_reason(&self.last_cancellation_reason, reason);
        }
    }
}

pub(super) struct CompactionCostJobGuard<'a> {
    state: &'a EngineCompactionCostState,
    input_rows: u64,
    input_bytes: u64,
    output_rows: u64,
    output_bytes: u64,
    posting_outputs: u64,
    finished: bool,
}

impl CompactionCostJobGuard<'_> {
    pub(super) fn set_generated_output(&mut self, rows: u64, bytes: u64, posting_outputs: u64) {
        self.output_rows = rows;
        self.output_bytes = bytes;
        self.posting_outputs = posting_outputs;
        self.state
            .total_generated_output_rows
            .fetch_add(rows, Ordering::Relaxed);
        self.state
            .total_generated_output_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.state
            .posting_outputs_generated
            .fetch_add(posting_outputs, Ordering::Relaxed);
    }

    pub(super) fn published(mut self, manifest_publication_elapsed: Duration) {
        self.state.jobs_published.fetch_add(1, Ordering::Relaxed);
        self.state
            .total_published_input_rows
            .fetch_add(self.input_rows, Ordering::Relaxed);
        self.state
            .total_published_input_bytes
            .fetch_add(self.input_bytes, Ordering::Relaxed);
        self.state
            .total_published_output_rows
            .fetch_add(self.output_rows, Ordering::Relaxed);
        self.state
            .total_published_output_bytes
            .fetch_add(self.output_bytes, Ordering::Relaxed);
        self.state
            .posting_outputs_published
            .fetch_add(self.posting_outputs, Ordering::Relaxed);
        self.state
            .manifest_publications
            .fetch_add(1, Ordering::Relaxed);
        let nanos = manifest_publication_elapsed
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        self.state
            .total_manifest_publication_nanos
            .fetch_add(nanos, Ordering::Relaxed);
        self.state
            .last_manifest_publication_nanos
            .store(nanos, Ordering::Relaxed);
        self.state.set_outcome("published");
        self.finished = true;
    }

    pub(super) fn invalidated(mut self, reason: &str) {
        self.state.jobs_invalidated.fetch_add(1, Ordering::Relaxed);
        self.state.record_invalidation(reason);
        self.state
            .total_wasted_input_rows
            .fetch_add(self.input_rows, Ordering::Relaxed);
        self.state
            .total_wasted_input_bytes
            .fetch_add(self.input_bytes, Ordering::Relaxed);
        self.state
            .total_wasted_output_rows
            .fetch_add(self.output_rows, Ordering::Relaxed);
        self.state
            .total_wasted_output_bytes
            .fetch_add(self.output_bytes, Ordering::Relaxed);
        self.state
            .posting_outputs_wasted
            .fetch_add(self.posting_outputs, Ordering::Relaxed);
        self.state.set_outcome(reason);
        self.finished = true;
    }

    pub(super) fn failed(mut self, reason: &str) {
        self.state.jobs_failed.fetch_add(1, Ordering::Relaxed);
        self.state.set_outcome(reason);
        self.finished = true;
    }
}

impl Drop for CompactionCostJobGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.state.jobs_failed.fetch_add(1, Ordering::Relaxed);
            self.state.set_outcome("failed_unclassified");
            self.finished = true;
        }
    }
}

#[derive(Debug)]
pub(super) struct RuntimeOperationState {
    active: AtomicBool,
    epoch: AtomicU64,
    started_unix_millis: AtomicU64,
    calls: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    last_success_unix_millis: AtomicU64,
    last_failure_unix_millis: AtomicU64,
    last_duration_nanos: AtomicU64,
    current_input_rows: AtomicU64,
    current_input_bytes: AtomicU64,
    current_output_rows: AtomicU64,
    current_output_bytes: AtomicU64,
    current_reclaimed_rows: AtomicU64,
    current_reclaimed_bytes: AtomicU64,
    last_input_rows: AtomicU64,
    last_input_bytes: AtomicU64,
    last_output_rows: AtomicU64,
    last_output_bytes: AtomicU64,
    last_reclaimed_rows: AtomicU64,
    last_reclaimed_bytes: AtomicU64,
    total_input_rows: AtomicU64,
    total_input_bytes: AtomicU64,
    total_output_rows: AtomicU64,
    total_output_bytes: AtomicU64,
    total_reclaimed_rows: AtomicU64,
    total_reclaimed_bytes: AtomicU64,
    last_result_marker: AtomicU64,
    detail: ArcSwap<EngineRuntimeOperationDetail>,
}

impl RuntimeOperationState {
    pub(super) fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            started_unix_millis: AtomicU64::new(0),
            calls: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            last_success_unix_millis: AtomicU64::new(0),
            last_failure_unix_millis: AtomicU64::new(0),
            last_duration_nanos: AtomicU64::new(0),
            current_input_rows: AtomicU64::new(0),
            current_input_bytes: AtomicU64::new(0),
            current_output_rows: AtomicU64::new(0),
            current_output_bytes: AtomicU64::new(0),
            current_reclaimed_rows: AtomicU64::new(0),
            current_reclaimed_bytes: AtomicU64::new(0),
            last_input_rows: AtomicU64::new(0),
            last_input_bytes: AtomicU64::new(0),
            last_output_rows: AtomicU64::new(0),
            last_output_bytes: AtomicU64::new(0),
            last_reclaimed_rows: AtomicU64::new(0),
            last_reclaimed_bytes: AtomicU64::new(0),
            total_input_rows: AtomicU64::new(0),
            total_input_bytes: AtomicU64::new(0),
            total_output_rows: AtomicU64::new(0),
            total_output_bytes: AtomicU64::new(0),
            total_reclaimed_rows: AtomicU64::new(0),
            total_reclaimed_bytes: AtomicU64::new(0),
            last_result_marker: AtomicU64::new(0),
            detail: ArcSwap::from_pointee(EngineRuntimeOperationDetail::default()),
        }
    }

    pub(super) fn start(&self) -> RuntimeOperationGuard<'_> {
        self.current_input_rows.store(0, Ordering::Relaxed);
        self.current_input_bytes.store(0, Ordering::Relaxed);
        self.current_output_rows.store(0, Ordering::Relaxed);
        self.current_output_bytes.store(0, Ordering::Relaxed);
        self.current_reclaimed_rows.store(0, Ordering::Relaxed);
        self.current_reclaimed_bytes.store(0, Ordering::Relaxed);
        self.detail
            .store(Arc::new(EngineRuntimeOperationDetail::default()));
        self.calls.fetch_add(1, Ordering::Relaxed);
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        self.started_unix_millis
            .store(runtime_unix_millis(), Ordering::Release);
        self.active.store(true, Ordering::Release);
        RuntimeOperationGuard {
            state: self,
            epoch,
            started: Instant::now(),
            finished: false,
        }
    }

    pub(super) fn snapshot(&self, now_unix_millis: u64) -> EngineRuntimeOperationSnapshot {
        let active = self.active.load(Ordering::Acquire);
        let started_unix_millis = self.started_unix_millis.load(Ordering::Acquire);
        let current_input_bytes = self.current_input_bytes.load(Ordering::Relaxed);
        let current_output_bytes = self.current_output_bytes.load(Ordering::Relaxed);
        let last_input_bytes = self.last_input_bytes.load(Ordering::Relaxed);
        let last_output_bytes = self.last_output_bytes.load(Ordering::Relaxed);
        EngineRuntimeOperationSnapshot {
            active,
            epoch: self.epoch.load(Ordering::Acquire),
            started_unix_millis,
            elapsed_millis: if active {
                now_unix_millis.saturating_sub(started_unix_millis)
            } else {
                0
            },
            calls: self.calls.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            last_success_unix_millis: self.last_success_unix_millis.load(Ordering::Acquire),
            last_failure_unix_millis: self.last_failure_unix_millis.load(Ordering::Acquire),
            last_duration_nanos: self.last_duration_nanos.load(Ordering::Relaxed),
            current_input_rows: self.current_input_rows.load(Ordering::Relaxed),
            current_input_bytes,
            current_output_rows: self.current_output_rows.load(Ordering::Relaxed),
            current_output_bytes,
            current_reclaimed_rows: self.current_reclaimed_rows.load(Ordering::Relaxed),
            current_reclaimed_bytes: self.current_reclaimed_bytes.load(Ordering::Relaxed),
            current_rewrite_amplification: current_output_bytes as f64
                / current_input_bytes.max(1) as f64,
            last_input_rows: self.last_input_rows.load(Ordering::Relaxed),
            last_input_bytes,
            last_output_rows: self.last_output_rows.load(Ordering::Relaxed),
            last_output_bytes,
            last_reclaimed_rows: self.last_reclaimed_rows.load(Ordering::Relaxed),
            last_reclaimed_bytes: self.last_reclaimed_bytes.load(Ordering::Relaxed),
            last_rewrite_amplification: last_output_bytes as f64 / last_input_bytes.max(1) as f64,
            total_input_rows: self.total_input_rows.load(Ordering::Relaxed),
            total_input_bytes: self.total_input_bytes.load(Ordering::Relaxed),
            total_output_rows: self.total_output_rows.load(Ordering::Relaxed),
            total_output_bytes: self.total_output_bytes.load(Ordering::Relaxed),
            total_reclaimed_rows: self.total_reclaimed_rows.load(Ordering::Relaxed),
            total_reclaimed_bytes: self.total_reclaimed_bytes.load(Ordering::Relaxed),
            last_result_marker: self.last_result_marker.load(Ordering::Relaxed),
            detail: (*self.detail.load_full()).clone(),
        }
    }
}

pub(super) struct RuntimeOperationGuard<'a> {
    state: &'a RuntimeOperationState,
    epoch: u64,
    started: Instant,
    finished: bool,
}

impl RuntimeOperationGuard<'_> {
    pub(super) fn set_detail_with_reason(
        &self,
        table_name: &str,
        segment_ids: &[u64],
        reason: &str,
    ) {
        let mut bounded = segment_ids
            .iter()
            .copied()
            .take(RUNTIME_STATS_MAX_ACTIVE_SEGMENT_IDS)
            .collect::<Vec<_>>();
        bounded.sort_unstable();
        self.state
            .detail
            .store(Arc::new(EngineRuntimeOperationDetail {
                table_id: Some(runtime_owner_id(table_name)),
                segment_ids: bounded,
                segment_ids_truncated: segment_ids.len() > RUNTIME_STATS_MAX_ACTIVE_SEGMENT_IDS,
                reason: reason.chars().take(64).collect(),
            }));
    }

    pub(super) fn set_reason(&self, reason: &str) {
        let mut detail = (*self.state.detail.load_full()).clone();
        detail.reason = reason.chars().take(64).collect();
        self.state.detail.store(Arc::new(detail));
    }

    pub(super) fn add_input(&self, rows: u64, bytes: u64) {
        self.state
            .current_input_rows
            .fetch_add(rows, Ordering::Relaxed);
        self.state
            .current_input_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn add_output(&self, rows: u64, bytes: u64) {
        self.state
            .current_output_rows
            .fetch_add(rows, Ordering::Relaxed);
        self.state
            .current_output_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn add_reclaimed(&self, rows: u64, bytes: u64) {
        self.state
            .current_reclaimed_rows
            .fetch_add(rows, Ordering::Relaxed);
        self.state
            .current_reclaimed_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn set_result_marker(&self, marker: u64) {
        self.state
            .last_result_marker
            .store(marker, Ordering::Release);
    }

    pub(super) fn success(mut self) {
        self.finish(true);
    }

    pub(super) fn failure(mut self) {
        self.finish(false);
    }

    pub(super) fn finish(&mut self, success: bool) {
        if self.finished {
            return;
        }
        let current_epoch = self.state.epoch.load(Ordering::Acquire);
        if current_epoch == self.epoch {
            let input_rows = self.state.current_input_rows.load(Ordering::Relaxed);
            let input_bytes = self.state.current_input_bytes.load(Ordering::Relaxed);
            let output_rows = self.state.current_output_rows.load(Ordering::Relaxed);
            let output_bytes = self.state.current_output_bytes.load(Ordering::Relaxed);
            let reclaimed_rows = self.state.current_reclaimed_rows.load(Ordering::Relaxed);
            let reclaimed_bytes = self.state.current_reclaimed_bytes.load(Ordering::Relaxed);
            self.state
                .last_input_rows
                .store(input_rows, Ordering::Relaxed);
            self.state
                .last_input_bytes
                .store(input_bytes, Ordering::Relaxed);
            self.state
                .last_output_rows
                .store(output_rows, Ordering::Relaxed);
            self.state
                .last_output_bytes
                .store(output_bytes, Ordering::Relaxed);
            self.state
                .last_reclaimed_rows
                .store(reclaimed_rows, Ordering::Relaxed);
            self.state
                .last_reclaimed_bytes
                .store(reclaimed_bytes, Ordering::Relaxed);
            self.state
                .total_input_rows
                .fetch_add(input_rows, Ordering::Relaxed);
            self.state
                .total_input_bytes
                .fetch_add(input_bytes, Ordering::Relaxed);
            self.state
                .total_output_rows
                .fetch_add(output_rows, Ordering::Relaxed);
            self.state
                .total_output_bytes
                .fetch_add(output_bytes, Ordering::Relaxed);
            self.state
                .total_reclaimed_rows
                .fetch_add(reclaimed_rows, Ordering::Relaxed);
            self.state
                .total_reclaimed_bytes
                .fetch_add(reclaimed_bytes, Ordering::Relaxed);
            self.state.last_duration_nanos.store(
                self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
            let finished_at = runtime_unix_millis();
            if success {
                self.state.completed.fetch_add(1, Ordering::Relaxed);
                self.state
                    .last_success_unix_millis
                    .store(finished_at, Ordering::Release);
            } else {
                self.state.failed.fetch_add(1, Ordering::Relaxed);
                self.state
                    .last_failure_unix_millis
                    .store(finished_at, Ordering::Release);
            }
            self.state.active.store(false, Ordering::Release);
        }
        self.finished = true;
    }
}

impl Drop for RuntimeOperationGuard<'_> {
    fn drop(&mut self) {
        self.finish(false);
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct EngineMaintenanceSnapshot {
    pub background_worker_alive: bool,
    pub background_loop_epoch: u64,
    pub background_last_success_unix_millis: u64,
    pub seal: EngineRuntimeOperationSnapshot,
    pub compaction: EngineRuntimeOperationSnapshot,
    pub compaction_cost: EngineCompactionCostSnapshot,
    pub checkpoint: EngineRuntimeOperationSnapshot,
}

#[derive(Debug)]
pub(super) struct EngineMaintenanceState {
    pub(super) background_worker_alive: AtomicBool,
    pub(super) background_loop_epoch: AtomicU64,
    background_last_success_unix_millis: AtomicU64,
    pub(super) seal: RuntimeOperationState,
    pub(super) compaction: RuntimeOperationState,
    pub(super) compaction_cost: EngineCompactionCostState,
    pub(super) checkpoint: RuntimeOperationState,
}

impl EngineMaintenanceState {
    pub(super) fn new() -> Self {
        Self {
            background_worker_alive: AtomicBool::new(false),
            background_loop_epoch: AtomicU64::new(0),
            background_last_success_unix_millis: AtomicU64::new(0),
            seal: RuntimeOperationState::new(),
            compaction: RuntimeOperationState::new(),
            compaction_cost: EngineCompactionCostState::new(),
            checkpoint: RuntimeOperationState::new(),
        }
    }

    pub(super) fn snapshot(&self, now_unix_millis: u64) -> EngineMaintenanceSnapshot {
        EngineMaintenanceSnapshot {
            background_worker_alive: self.background_worker_alive.load(Ordering::Acquire),
            background_loop_epoch: self.background_loop_epoch.load(Ordering::Relaxed),
            background_last_success_unix_millis: self
                .background_last_success_unix_millis
                .load(Ordering::Acquire),
            seal: self.seal.snapshot(now_unix_millis),
            compaction: self.compaction.snapshot(now_unix_millis),
            compaction_cost: self.compaction_cost.snapshot(),
            checkpoint: self.checkpoint.snapshot(now_unix_millis),
        }
    }

    pub(super) fn record_background_success(&self) {
        self.background_last_success_unix_millis
            .store(runtime_unix_millis(), Ordering::Release);
    }
}

pub(super) struct BackgroundWorkerRuntimeGuard(pub(super) Arc<EngineMaintenanceState>);

impl Drop for BackgroundWorkerRuntimeGuard {
    fn drop(&mut self) {
        self.0
            .background_worker_alive
            .store(false, Ordering::Release);
    }
}

/// Versioned, bounded read-only runtime snapshot returned by
/// `PRAGMA RUNTIME_STATS`.
///
/// Every potentially contended engine owner is sampled with `try_read` or an
/// atomic/ArcSwap snapshot. A missing owner is explicit; diagnostics never
/// waits behind a workload lock and never opens a payload file, scans rows, or
/// builds an index. Owner totals become lower bounds when a structural visit
/// limit is reached.
#[derive(Clone, Debug, Serialize)]
pub struct EngineRuntimeStatsV2 {
    pub format: u32,
    pub sequence: u64,
    pub captured_unix_millis: u64,
    pub snapshot_nanos: u64,
    pub complete: bool,
    pub missing_evidence: Vec<String>,
    pub truncated_owners: Vec<String>,
    pub lifecycle: String,
    pub schema_epoch: u64,
    pub active_transactions: u64,
    pub accepting_transactions: bool,
    pub oldest_transaction_begin_sequence: Option<i64>,
    pub oldest_transaction_age_millis: Option<u64>,
    pub transaction_wait_edges: Option<u64>,
    pub hot_tables: u64,
    pub hot_rows: u64,
    pub hot_bytes: u64,
    pub staging_transactions: u64,
    pub staging_tables: u64,
    pub staging_rows: u64,
    pub cold_tables: u64,
    pub cold_segments: u64,
    pub cold_unleveled_segments: u64,
    pub cold_l0_segments: u64,
    pub cold_l1_segments: u64,
    pub cold_l0_debt_physical_bytes: u64,
    pub cold_rows: u64,
    pub cold_resident_bytes: u64,
    pub cold_metadata_bytes: u64,
    pub cold_row_id_bytes: u64,
    pub cold_exact_index_bytes: u64,
    pub cold_ordered_index_bytes: u64,
    pub cold_descriptor_bytes: u64,
    pub cold_column_payload_bytes: u64,
    pub cold_tombstones: u64,
    pub pressure_seal_requested: bool,
    pub compaction_requested: bool,
    pub max_compaction_jobs: u64,
    pub compaction_active_jobs: u64,
    pub compaction_peak_active_jobs: u64,
    pub max_compaction_input_segments: u64,
    pub max_compaction_input_bytes: u64,
    pub max_compaction_output_bytes: u64,
    pub compaction_job_time_budget_ms: u64,
    pub compaction_io_bytes_per_sec: u64,
    pub compaction_disk_reserve_bytes: u64,
    pub compaction_retry_cooldown_ms: u64,
    pub compaction_retry_cooldown_active: bool,
    pub compaction_retry_cooldown_until_unix_millis: u64,
    pub compaction_retry_suppressed: u64,
    pub compaction_retry_last_reason: String,
    pub l0_soft_limit_segments: u64,
    pub l0_hard_limit_segments: u64,
    pub l0_soft_limit_bytes: u64,
    pub l0_hard_limit_bytes: u64,
    pub compaction_soft_backpressure_waits: u64,
    pub compaction_soft_backpressure_wait_millis: u64,
    pub compaction_hard_backpressure_rejections: u64,
    pub seal_running: bool,
    pub compaction_running: bool,
    pub checkpoint_running: bool,
    pub checkpoint_mutex_busy: bool,
    pub wal_current_lsn: u64,
    pub wal_current_file_bytes: u64,
    pub wal_max_file_bytes: u64,
    pub wal_pending_durability_bytes: u64,
    pub wal_running: bool,
    pub last_checkpoint_unix_nanos: i64,
    pub page_cache_warmup: Option<crate::PageCacheWarmupSnapshot>,
    pub read_queue_depth: u64,
    pub storage_cpu_workers_configured: u64,
    pub storage_cpu_workers_effective: u64,
    pub storage_cpu_workers_in_use: u64,
    pub storage_cpu_peak_workers_in_use: u64,
    pub storage_cpu_workers_reserved: u64,
    pub storage_cpu_peak_workers_reserved: u64,
    pub storage_cpu_leases: u64,
    pub storage_cpu_parallel_leases: u64,
    pub maintenance: EngineMaintenanceSnapshot,
    pub runtime_owners: instrumentation::RuntimeOwnerSnapshot,
    pub owner_visit_limits: EngineRuntimeVisitLimits,
    pub counters: instrumentation::EngineCountersSnapshot,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct EngineRuntimeVisitLimits {
    pub max_tables: u64,
    pub max_segments: u64,
    pub max_transactions: u64,
    pub max_staging_tables: u64,
    pub max_active_segment_ids: u64,
}
