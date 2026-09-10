pub fn reset() {
    ROW_MATERIALIZATION_PENDING.with(|pending| pending.0.set(RowMaterializationPending::EMPTY));
    WAL_APPEND_PENDING.with(|pending| pending.0.set(WalAppendPending::EMPTY));
    PROTOCOL_ROW_ADAPTER_PENDING.with(|pending| pending.0.set(ProtocolRowAdapterPending::EMPTY));
    runtime_profile::reset_runtime_profile();
    COUNTERS.reset();
}

pub fn snapshot() -> EngineCountersSnapshot {
    flush_thread_local_counters();
    COUNTERS.snapshot()
}

pub fn runtime_owner_snapshot() -> RuntimeOwnerSnapshot {
    RUNTIME_OWNERS.snapshot()
}

#[doc(hidden)]
pub fn record_server_connection_opened() {
    add(&RUNTIME_OWNERS.server_connections, 1);
}

#[doc(hidden)]
pub fn record_server_connection_closed() {
    subtract_gauge(&RUNTIME_OWNERS.server_connections, 1);
}

#[doc(hidden)]
pub fn replace_server_session_owners(
    before_sessions: u64,
    after_sessions: u64,
    before_cursors: u64,
    after_cursors: u64,
    before_prepared: u64,
    after_prepared: u64,
) {
    replace_gauge(
        &RUNTIME_OWNERS.authenticated_sessions,
        before_sessions,
        after_sessions,
    );
    replace_gauge(
        &RUNTIME_OWNERS.active_cursors,
        before_cursors,
        after_cursors,
    );
    replace_gauge(
        &RUNTIME_OWNERS.prepared_statements,
        before_prepared,
        after_prepared,
    );
}

#[doc(hidden)]
pub fn record_server_execution_started() {
    add(&RUNTIME_OWNERS.active_executions, 1);
}

#[doc(hidden)]
pub fn record_server_execution_finished() {
    subtract_gauge(&RUNTIME_OWNERS.active_executions, 1);
}

#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub fn replace_transaction_ddl_owners(
    before_tables: u64,
    after_tables: u64,
    before_indexes: u64,
    after_indexes: u64,
    before_index_drops: u64,
    after_index_drops: u64,
    before_schema_changes: u64,
    after_schema_changes: u64,
    before_constraint_changes: u64,
    after_constraint_changes: u64,
) {
    replace_gauge(&RUNTIME_OWNERS.staged_tables, before_tables, after_tables);
    replace_gauge(
        &RUNTIME_OWNERS.staged_indexes,
        before_indexes,
        after_indexes,
    );
    replace_gauge(
        &RUNTIME_OWNERS.staged_index_drops,
        before_index_drops,
        after_index_drops,
    );
    replace_gauge(
        &RUNTIME_OWNERS.staged_schema_changes,
        before_schema_changes,
        after_schema_changes,
    );
    replace_gauge(
        &RUNTIME_OWNERS.staged_constraint_changes,
        before_constraint_changes,
        after_constraint_changes,
    );
}

#[inline]
pub fn record_runtime_wait(kind: RuntimeWaitKind, elapsed: Duration) {
    runtime_profile::record_runtime_wait(kind, elapsed);
}

#[inline]
pub fn record_hash_build(rows: usize, elapsed: Duration) {
    runtime_profile::record_hash_build(rows, elapsed);
}

#[inline]
pub fn record_index_lookup(keys: usize, hits: usize, elapsed: Duration) {
    runtime_profile::record_index_lookup(keys, hits, elapsed);
}

#[inline]
pub fn record_protocol_round_trip(elapsed: Duration) {
    runtime_profile::record_protocol_round_trip(elapsed);
}

#[inline]
pub fn record_metadata_pk_count_attempt() {
    add(&COUNTERS.metadata_pk_count_attempts, 1);
    record_metadata_pk_count_probe(|probe| {
        probe.attempts = probe.attempts.saturating_add(1);
    });
}

#[inline]
pub fn record_metadata_pk_count_interval() {
    add(&COUNTERS.metadata_pk_count_intervals, 1);
    record_metadata_pk_count_probe(|probe| {
        probe.intervals = probe.intervals.saturating_add(1);
    });
}

#[inline]
pub fn record_metadata_pk_count_hot_candidates(count: usize) {
    add(&COUNTERS.metadata_pk_count_hot_candidates, count as u64);
    record_metadata_pk_count_probe(|probe| {
        probe.hot_candidates = probe.hot_candidates.saturating_add(count as u64);
    });
}

#[inline]
pub fn record_metadata_pk_count_applied(candidate_rows: usize, visible_rows: usize) {
    add(&COUNTERS.metadata_pk_count_applied, 1);
    add(
        &COUNTERS.metadata_pk_count_candidate_rows,
        candidate_rows as u64,
    );
    add(
        &COUNTERS.metadata_pk_count_visible_rows,
        visible_rows as u64,
    );
    add(
        &COUNTERS.metadata_pk_count_visibility_exclusions,
        candidate_rows.saturating_sub(visible_rows) as u64,
    );
    record_metadata_pk_count_probe(|probe| {
        probe.applied = probe.applied.saturating_add(1);
        probe.candidate_rows = probe.candidate_rows.saturating_add(candidate_rows as u64);
        probe.visible_rows = probe.visible_rows.saturating_add(visible_rows as u64);
        probe.visibility_exclusions = probe
            .visibility_exclusions
            .saturating_add(candidate_rows.saturating_sub(visible_rows) as u64);
    });
}

#[inline]
pub fn record_metadata_pk_count_fallback(reason: MetadataPkCountFallback) {
    add(&COUNTERS.metadata_pk_count_fallbacks, 1);
    match reason {
        MetadataPkCountFallback::Snapshot => add(&COUNTERS.metadata_pk_count_fallback_snapshot, 1),
        MetadataPkCountFallback::SealOverlap => {
            add(&COUNTERS.metadata_pk_count_fallback_seal_overlap, 1)
        }
        MetadataPkCountFallback::Unsupported => {
            add(&COUNTERS.metadata_pk_count_fallback_unsupported, 1)
        }
        MetadataPkCountFallback::CandidateLimit => {
            add(&COUNTERS.metadata_pk_count_fallback_candidate_limit, 1)
        }
    }
    record_metadata_pk_count_probe(|probe| {
        probe.fallbacks = probe.fallbacks.saturating_add(1);
        match reason {
            MetadataPkCountFallback::Snapshot => {
                probe.fallback_snapshot = probe.fallback_snapshot.saturating_add(1);
            }
            MetadataPkCountFallback::SealOverlap => {
                probe.fallback_seal_overlap = probe.fallback_seal_overlap.saturating_add(1);
            }
            MetadataPkCountFallback::Unsupported => {
                probe.fallback_unsupported = probe.fallback_unsupported.saturating_add(1);
            }
            MetadataPkCountFallback::CandidateLimit => {
                probe.fallback_candidate_limit = probe.fallback_candidate_limit.saturating_add(1);
            }
        }
    });
}

/// Flush counters accumulated in the calling thread into the process-wide
/// totals.
///
/// The TCP server uses this at every request boundary. Without that boundary,
/// an external benchmark can attribute buffered row-materialization work to a
/// later request or miss it until the session thread exits.
#[doc(hidden)]
pub fn flush_thread_local_counters() {
    flush_row_materialization_pending_current_thread();
    flush_wal_append_pending_current_thread();
    flush_protocol_row_adapter_pending_current_thread();
}

pub fn begin_volume_read_probe() {
    VOLUME_READ_PROBE.with(|probe| probe.set(Some(VolumeReadProbeSnapshot::default())));
}

pub fn end_volume_read_probe() -> VolumeReadProbeSnapshot {
    VOLUME_READ_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

/// Begin a request-local probe for metadata-only INTEGER primary-key count.
///
/// The probe is intentionally thread-local. The operator is synchronous on
/// the executor thread, whereas process-wide totals may include unrelated
/// client requests in a running server.
#[doc(hidden)]
pub fn begin_metadata_pk_count_probe() {
    METADATA_PK_COUNT_PROBE.with(|probe| probe.set(Some(MetadataPkCountProbeSnapshot::default())));
}

/// Finish the current request-local metadata count probe.
#[doc(hidden)]
pub fn end_metadata_pk_count_probe() -> MetadataPkCountProbeSnapshot {
    METADATA_PK_COUNT_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

/// Begin a request-local probe for derived table source execution.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn begin_derived_subquery_probe() {
    DERIVED_SUBQUERY_PROBE.with(|probe| probe.set(Some(DerivedSubqueryProbeSnapshot::default())));
}

/// Finish the current request-local derived table source execution probe.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_derived_subquery_probe() -> DerivedSubqueryProbeSnapshot {
    DERIVED_SUBQUERY_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

#[doc(hidden)]
pub fn begin_artifact_columnar_group_probe() {
    ARTIFACT_COLUMNAR_GROUP_PROBE.with(|probe| probe.set(Some(ArtifactColumnarGroupProbeSnapshot::default())));
}

#[doc(hidden)]
pub fn end_artifact_columnar_group_probe() -> ArtifactColumnarGroupProbeSnapshot {
    ARTIFACT_COLUMNAR_GROUP_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

/// Begin a request-local aggregate of physical JOIN executions.
#[doc(hidden)]
pub fn begin_join_execution_probe() {
    JOIN_EXECUTION_PROBE.with(|probe| probe.set(Some(JoinExecutionProbeSnapshot::default())));
    JOIN_EXECUTION_TRACE.with(|trace| {
        *trace.borrow_mut() = Some(JoinExecutionTraceSnapshot::default());
    });
}

/// Finish the current request-local physical JOIN aggregate.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_join_execution_probe() -> JoinExecutionProbeSnapshot {
    end_join_execution_probe_with_trace().0
}

/// Finish the aggregate together with its bounded per-operator trace.
#[doc(hidden)]
pub fn end_join_execution_probe_with_trace(
) -> (JoinExecutionProbeSnapshot, JoinExecutionTraceSnapshot) {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        let trace =
            JOIN_EXECUTION_TRACE.with(|trace| trace.borrow_mut().take().unwrap_or_default());
        (snapshot, trace)
    })
}

/// Begin a request-local, bounded physical planning trace.
#[doc(hidden)]
pub fn begin_join_planning_probe() {
    JOIN_PLANNING_PROBE.with(|probe| {
        *probe.borrow_mut() = Some(JoinPlanningProbeSnapshot::default());
    });
}

#[inline]
#[doc(hidden)]
pub fn join_planning_probe_active() -> bool {
    JOIN_PLANNING_PROBE.with(|probe| probe.borrow().is_some())
}

/// Finish the request-local physical planning trace.
#[doc(hidden)]
pub fn end_join_planning_probe() -> JoinPlanningProbeSnapshot {
    JOIN_PLANNING_PROBE.with(|probe| probe.borrow_mut().take().unwrap_or_default())
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn begin_protocol_component_probe() {
    PROTOCOL_COMPONENT_PROBE
        .with(|probe| probe.set(Some(ProtocolComponentProbeSnapshot::default())));
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_protocol_component_probe() -> ProtocolComponentProbeSnapshot {
    PROTOCOL_COMPONENT_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn begin_protocol_pending_column_batch_probe() {
    PROTOCOL_PENDING_COLUMN_BATCH_PROBE
        .with(|probe| probe.set(Some(ProtocolPendingColumnBatchProbeSnapshot::default())));
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_protocol_pending_column_batch_probe() -> ProtocolPendingColumnBatchProbeSnapshot {
    PROTOCOL_PENDING_COLUMN_BATCH_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

#[inline]
#[doc(hidden)]
pub fn record_derived_subquery_execute() {
    add(&COUNTERS.derived_subquery_executes, 1);
    DERIVED_SUBQUERY_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.executes = snapshot.executes.saturating_add(1);
        probe.set(Some(snapshot));
    });
}

#[inline]
fn record_metadata_pk_count_probe(update: impl FnOnce(&mut MetadataPkCountProbeSnapshot)) {
    METADATA_PK_COUNT_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        update(&mut snapshot);
        probe.set(Some(snapshot));
    });
}

#[inline]
#[doc(hidden)]
pub fn record_artifact_columnar_group_aggregate(record: ArtifactColumnarGroupRecord) {
    add(&COUNTERS.artifact_columnar_group_applies, 1);
    add(&COUNTERS.artifact_columnar_group_row_groups, record.row_groups);
    add(
        &COUNTERS.artifact_columnar_group_selected_blocks,
        record.selected_blocks,
    );
    add(&COUNTERS.artifact_columnar_group_input_rows, record.input_rows);
    add(
        &COUNTERS.artifact_columnar_group_output_groups,
        record.output_groups,
    );
    add(
        &COUNTERS.artifact_columnar_group_direct_accumulators,
        record.direct_accumulators,
    );
    add(
        &COUNTERS.artifact_columnar_group_hash_accumulators,
        record.hash_accumulators,
    );
    add(
        &COUNTERS.artifact_columnar_group_local_merges,
        record.local_merges,
    );
    add(
        &COUNTERS.artifact_columnar_group_merged_groups,
        record.merged_groups,
    );
    add(
        &COUNTERS.artifact_columnar_group_scheduler_runs,
        record.scheduler_runs,
    );
    add(
        &COUNTERS.artifact_columnar_group_scheduled_segments,
        record.scheduled_segments,
    );

    ARTIFACT_COLUMNAR_GROUP_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.applies = snapshot.applies.saturating_add(1);
        snapshot.row_groups = snapshot.row_groups.saturating_add(record.row_groups);
        snapshot.selected_blocks = snapshot
            .selected_blocks
            .saturating_add(record.selected_blocks);
        snapshot.input_rows = snapshot.input_rows.saturating_add(record.input_rows);
        snapshot.output_groups = snapshot.output_groups.saturating_add(record.output_groups);
        snapshot.direct_accumulators = snapshot
            .direct_accumulators
            .saturating_add(record.direct_accumulators);
        snapshot.hash_accumulators = snapshot
            .hash_accumulators
            .saturating_add(record.hash_accumulators);
        snapshot.local_merges = snapshot.local_merges.saturating_add(record.local_merges);
        snapshot.merged_groups = snapshot.merged_groups.saturating_add(record.merged_groups);
        snapshot.scheduler_runs = snapshot
            .scheduler_runs
            .saturating_add(record.scheduler_runs);
        snapshot.scheduled_segments = snapshot
            .scheduled_segments
            .saturating_add(record.scheduled_segments);
        probe.set(Some(snapshot));
    });
}

#[inline]
#[doc(hidden)]
pub fn record_artifact_columnar_group_fallback(reason: ArtifactColumnarGroupFallback) {
    add(&COUNTERS.artifact_columnar_group_fallbacks, 1);
    match reason {
        ArtifactColumnarGroupFallback::RowState => {
            add(&COUNTERS.artifact_columnar_group_fallback_row_state, 1);
        }
        ArtifactColumnarGroupFallback::NoColdArtifact => {
            add(&COUNTERS.artifact_columnar_group_fallback_no_cold_artifact, 1);
        }
        ArtifactColumnarGroupFallback::Schema => {
            add(&COUNTERS.artifact_columnar_group_fallback_schema, 1);
        }
        ArtifactColumnarGroupFallback::GroupKey => {
            add(&COUNTERS.artifact_columnar_group_fallback_group_key, 1);
        }
        ArtifactColumnarGroupFallback::Aggregate => {
            add(&COUNTERS.artifact_columnar_group_fallback_aggregate, 1);
        }
        ArtifactColumnarGroupFallback::Visibility => {
            add(&COUNTERS.artifact_columnar_group_fallback_visibility, 1);
        }
        ArtifactColumnarGroupFallback::Storage => {
            add(&COUNTERS.artifact_columnar_group_fallback_storage, 1);
        }
        ArtifactColumnarGroupFallback::ColumnShape => {
            add(&COUNTERS.artifact_columnar_group_fallback_column_shape, 1);
        }
        ArtifactColumnarGroupFallback::Accumulator => {
            add(&COUNTERS.artifact_columnar_group_fallback_accumulator, 1);
        }
    }

    ARTIFACT_COLUMNAR_GROUP_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.fallbacks = snapshot.fallbacks.saturating_add(1);
        match reason {
            ArtifactColumnarGroupFallback::RowState => {
                snapshot.fallback_row_state = snapshot.fallback_row_state.saturating_add(1);
            }
            ArtifactColumnarGroupFallback::NoColdArtifact => {
                snapshot.fallback_no_cold_artifact = snapshot.fallback_no_cold_artifact.saturating_add(1);
            }
            ArtifactColumnarGroupFallback::Schema => {
                snapshot.fallback_schema = snapshot.fallback_schema.saturating_add(1);
            }
            ArtifactColumnarGroupFallback::GroupKey => {
                snapshot.fallback_group_key = snapshot.fallback_group_key.saturating_add(1);
            }
            ArtifactColumnarGroupFallback::Aggregate => {
                snapshot.fallback_aggregate = snapshot.fallback_aggregate.saturating_add(1);
            }
            ArtifactColumnarGroupFallback::Visibility => {
                snapshot.fallback_visibility = snapshot.fallback_visibility.saturating_add(1);
            }
            ArtifactColumnarGroupFallback::Storage => {
                snapshot.fallback_storage = snapshot.fallback_storage.saturating_add(1);
            }
            ArtifactColumnarGroupFallback::ColumnShape => {
                snapshot.fallback_column_shape = snapshot.fallback_column_shape.saturating_add(1);
            }
            ArtifactColumnarGroupFallback::Accumulator => {
                snapshot.fallback_accumulator = snapshot.fallback_accumulator.saturating_add(1);
            }
        }
        probe.set(Some(snapshot));
    });
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn begin_row_materialization_probe() {
    ROW_MATERIALIZATION_PROBE
        .with(|probe| probe.set(Some(RowMaterializationProbeSnapshot::default())));
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_row_materialization_probe() -> RowMaterializationProbeSnapshot {
    ROW_MATERIALIZATION_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn begin_decompression_probe() {
    DECOMPRESSION_PROBE.with(|probe| probe.set(Some(DecompressionProbeSnapshot::default())));
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_decompression_probe() -> DecompressionProbeSnapshot {
    DECOMPRESSION_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn begin_artifact_io_probe() {
    ARTIFACT_IO_PROBE.with(|probe| probe.set(Some(ArtifactIoProbeSnapshot::default())));
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_artifact_io_probe() -> ArtifactIoProbeSnapshot {
    ARTIFACT_IO_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn begin_ram_accelerator_probe() {
    RAM_ACCELERATOR_PROBE.with(|probe| probe.set(Some(RamAcceleratorProbeSnapshot::default())));
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_ram_accelerator_probe() -> RamAcceleratorProbeSnapshot {
    RAM_ACCELERATOR_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn begin_posting_lookup_probe() {
    POSTING_LOOKUP_PROBE.with(|probe| probe.set(Some(PostingLookupProbeSnapshot::default())));
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn end_posting_lookup_probe() -> PostingLookupProbeSnapshot {
    POSTING_LOOKUP_PROBE.with(|probe| {
        let snapshot = probe.get().unwrap_or_default();
        probe.set(None);
        snapshot
    })
}

pub fn record_volume_read(bytes: u64, elapsed: Duration) {
    add(&COUNTERS.volume_read_calls, 1);
    add(&COUNTERS.volume_read_bytes, bytes);
    add(&COUNTERS.volume_read_nanos, duration_nanos(elapsed));
    VOLUME_READ_PROBE.with(|probe| {
        if let Some(mut snapshot) = probe.get() {
            snapshot.calls = snapshot.calls.saturating_add(1);
            snapshot.bytes = snapshot.bytes.saturating_add(bytes);
            probe.set(Some(snapshot));
        }
    });
}

pub fn record_decompression(compressed_bytes: u64, raw_bytes: u64, elapsed: Duration) {
    add(&COUNTERS.decompression_calls, 1);
    add(&COUNTERS.decompression_compressed_bytes, compressed_bytes);
    add(&COUNTERS.decompression_raw_bytes, raw_bytes);
    add(&COUNTERS.decompression_nanos, duration_nanos(elapsed));
    record_decompression_probe(compressed_bytes, raw_bytes);
}

pub fn record_row_materialization(rows: u64, values: u64, elapsed: Duration) {
    add(&COUNTERS.row_materialization_calls, 1);
    add(&COUNTERS.row_materialization_rows, rows);
    add(&COUNTERS.row_materialization_values, values);
    add(&COUNTERS.row_materialization_nanos, duration_nanos(elapsed));
    record_row_materialization_probe(1, rows, values);
}

#[inline]
pub fn record_row_materialization_count(rows: u64, values: u64) {
    record_row_materialization_probe(1, rows, values);
    ROW_MATERIALIZATION_PENDING.with(|pending| {
        let mut next = pending.0.get();
        next.calls = next.calls.saturating_add(1);
        next.rows = next.rows.saturating_add(rows);
        next.values = next.values.saturating_add(values);

        if next.rows >= 16_384 {
            flush_row_materialization_pending(next);
            pending.0.set(RowMaterializationPending::EMPTY);
        } else {
            pending.0.set(next);
        }
    });
}

/// Record a successful foreground artifact-backed payload-file open.
///
/// This intentionally excludes descriptor/header reads performed while a
/// volume is opened: query-path gates are about payload I/O after startup.
#[inline]
pub fn record_artifact_file_open() {
    add(&COUNTERS.artifact_file_open_calls, 1);
    record_artifact_io_probe(|probe| {
        probe.file_open_calls = probe.file_open_calls.saturating_add(1);
    });
}

/// Record a successful artifact-backed descriptor/metadata file open.
#[inline]
pub fn record_artifact_descriptor_open() {
    add(&COUNTERS.artifact_descriptor_open_calls, 1);
}

#[inline]
pub fn record_artifact_file_stat() {
    add(&COUNTERS.artifact_file_stat_calls, 1);
}

#[inline]
pub fn record_artifact_file_identity_check() {
    add(&COUNTERS.artifact_file_identity_checks, 1);
}

#[inline]
pub fn record_artifact_fadvise(success: bool) {
    add(&COUNTERS.artifact_fadvise_calls, 1);
    if !success {
        add(&COUNTERS.artifact_fadvise_errors, 1);
    }
}

/// Record one successful foreground artifact-backed payload `pread` syscall.
#[inline]
pub fn record_artifact_pread(bytes: u64, elapsed: Duration) {
    add(&COUNTERS.artifact_pread_calls, 1);
    add(&COUNTERS.artifact_pread_bytes, bytes);
    add(&COUNTERS.artifact_pread_nanos, duration_nanos(elapsed));
    record_artifact_io_probe(|probe| {
        probe.pread_calls = probe.pread_calls.saturating_add(1);
        probe.pread_bytes = probe.pread_bytes.saturating_add(bytes);
    });
}

#[inline]
pub fn record_artifact_singleflight_leader() {
    add(&COUNTERS.artifact_singleflight_leaders, 1);
}

#[inline]
pub fn record_artifact_singleflight_follower() {
    add(&COUNTERS.artifact_singleflight_followers, 1);
}

#[inline]
pub fn record_artifact_payload_decompress(compressed_bytes: u64, raw_bytes: u64, elapsed: Duration) {
    add(&COUNTERS.artifact_payload_decompress_calls, 1);
    add(
        &COUNTERS.artifact_payload_decompress_compressed_bytes,
        compressed_bytes,
    );
    add(&COUNTERS.artifact_payload_decompress_raw_bytes, raw_bytes);
    add(
        &COUNTERS.artifact_payload_decompress_nanos,
        duration_nanos(elapsed),
    );
}

#[inline]
pub fn record_artifact_column_deserialize(bytes: u64, rows: u64, elapsed: Duration) {
    add(&COUNTERS.artifact_column_deserialize_calls, 1);
    add(&COUNTERS.artifact_column_deserialize_bytes, bytes);
    add(&COUNTERS.artifact_column_deserialize_rows, rows);
    add(
        &COUNTERS.artifact_column_deserialize_nanos,
        duration_nanos(elapsed),
    );
    // Compatibility aggregate for existing benchmark reports/tests. Exact artifact-backed
    // split counters above are the authoritative contract for new analysis.
    record_decompression(bytes, bytes, elapsed);
}

#[inline]
fn record_row_materialization_probe(calls: u64, rows: u64, values: u64) {
    #[cfg(any(test, feature = "test-hooks"))]
    ROW_MATERIALIZATION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.calls = snapshot.calls.saturating_add(calls);
        snapshot.rows = snapshot.rows.saturating_add(rows);
        snapshot.values = snapshot.values.saturating_add(values);
        probe.set(Some(snapshot));
    });

    #[cfg(not(test))]
    {
        let _ = (calls, rows, values);
    }
}

#[inline]
fn record_decompression_probe(compressed_bytes: u64, raw_bytes: u64) {
    #[cfg(any(test, feature = "test-hooks"))]
    DECOMPRESSION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.calls = snapshot.calls.saturating_add(1);
        snapshot.compressed_bytes = snapshot.compressed_bytes.saturating_add(compressed_bytes);
        snapshot.raw_bytes = snapshot.raw_bytes.saturating_add(raw_bytes);
        probe.set(Some(snapshot));
    });

    #[cfg(not(test))]
    {
        let _ = (compressed_bytes, raw_bytes);
    }
}

#[inline]
fn record_artifact_io_probe(update: impl FnOnce(&mut ArtifactIoProbeSnapshot)) {
    #[cfg(any(test, feature = "test-hooks"))]
    ARTIFACT_IO_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        update(&mut snapshot);
        probe.set(Some(snapshot));
    });

    #[cfg(not(test))]
    {
        let _ = update;
    }
}

/// Record the filtered outer stream consumed by a physical join.
pub fn record_join_outer_rows(rows: u64, rows_with_key: u64) {
    add(&COUNTERS.join_outer_rows, rows);
    add(&COUNTERS.join_key_rows, rows_with_key);
}

/// Record one deduplicated physical lookup-key batch. Repeated outer rows
/// remain in the SQL join stream; only redundant index probes are eliminated.
#[doc(hidden)]
pub fn record_join_lookup_key_batch(key_rows: u64, distinct_keys: u64) {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.lookup_key_rows = snapshot.lookup_key_rows.saturating_add(key_rows);
        snapshot.lookup_distinct_keys = snapshot.lookup_distinct_keys.saturating_add(distinct_keys);
        snapshot.lookup_repeated_keys_eliminated = snapshot
            .lookup_repeated_keys_eliminated
            .saturating_add(key_rows.saturating_sub(distinct_keys));
        probe.set(Some(snapshot));
    });
}

/// Record one bounded PK-membership/fetch batch used by a physical join.
pub fn record_join_pk_probe(keys: u64, hits: u64, parent_payload_rows: u64) {
    add(&COUNTERS.join_pk_probe_batches, 1);
    add(&COUNTERS.join_pk_probe_keys, keys);
    add(&COUNTERS.join_pk_probe_hits, hits);
    add(&COUNTERS.join_parent_payload_rows, parent_payload_rows);
}

/// Record rows physically constructed at a join boundary.
pub fn record_join_rows_constructed(rows: u64) {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.owned_rows_constructed = snapshot.owned_rows_constructed.saturating_add(rows);
        probe.set(Some(snapshot));
    });
    add(&COUNTERS.join_rows_constructed, rows);
}

/// Record virtual projection transport between physical JOIN edges.
pub fn record_join_deferred_rows(produced: u64, consumed: u64) {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.deferred_rows = snapshot.deferred_rows.saturating_add(produced);
        snapshot.deferred_rows_consumed = snapshot.deferred_rows_consumed.saturating_add(consumed);
        probe.set(Some(snapshot));
    });
    add(&COUNTERS.join_rows_deferred, produced);
    add(&COUNTERS.join_deferred_rows_consumed, consumed);
}

/// Record rows collected into an intermediate deferred-result Vec. This is a
/// request-local diagnostic only: it exists to prove that a physical JOIN
/// graph did not silently reintroduce a RowVec boundary between operators.
#[doc(hidden)]
pub fn record_join_deferred_boundary_rows(rows: u64) {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.deferred_boundary_rows = snapshot.deferred_boundary_rows.saturating_add(rows);
        probe.set(Some(snapshot));
    });
}

/// Publish one bounded post-JOIN Top-N execution. The update happens once per
/// blocking owner, never once per candidate, so diagnostics do not perturb the
/// hot comparison loop.
#[doc(hidden)]
pub fn record_join_top_n(input_rows: u64, peak_candidates: u64, peak_bytes: u64, output_rows: u64) {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.top_n_calls = snapshot.top_n_calls.saturating_add(1);
        snapshot.top_n_input_rows = snapshot.top_n_input_rows.saturating_add(input_rows);
        snapshot.top_n_output_rows = snapshot.top_n_output_rows.saturating_add(output_rows);
        snapshot.top_n_peak_candidates = snapshot.top_n_peak_candidates.max(peak_candidates);
        snapshot.top_n_peak_bytes = snapshot.top_n_peak_bytes.max(peak_bytes);
        probe.set(Some(snapshot));
    });
}

/// Publish one bounded full ORDER BY execution. Unlike Top-N this owner may
/// spill sorted runs, but its resident row set must remain bounded regardless
/// of the input cardinality.
#[doc(hidden)]
pub fn record_join_ordered_sort(
    input_rows: u64,
    spill_runs: u64,
    peak_rows: u64,
    peak_bytes: u64,
    collect_elapsed: Duration,
    finalize_elapsed: Duration,
) {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.ordered_sort_calls = snapshot.ordered_sort_calls.saturating_add(1);
        snapshot.ordered_sort_input_rows =
            snapshot.ordered_sort_input_rows.saturating_add(input_rows);
        snapshot.ordered_sort_spill_runs =
            snapshot.ordered_sort_spill_runs.saturating_add(spill_runs);
        snapshot.ordered_sort_peak_rows = snapshot.ordered_sort_peak_rows.max(peak_rows);
        snapshot.ordered_sort_peak_bytes = snapshot.ordered_sort_peak_bytes.max(peak_bytes);
        snapshot.ordered_collect_nanos = snapshot
            .ordered_collect_nanos
            .saturating_add(duration_nanos(collect_elapsed));
        snapshot.ordered_finalize_nanos = snapshot
            .ordered_finalize_nanos
            .saturating_add(duration_nanos(finalize_elapsed));
        probe.set(Some(snapshot));
    });
}

/// Record that a certified index order survived the complete JOIN prefix and
/// allowed the final ORDER BY sort/Top-N owner to be skipped.
#[doc(hidden)]
pub fn record_join_ordering_skip() {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.ordering_skip_calls = snapshot.ordering_skip_calls.saturating_add(1);
        probe.set(Some(snapshot));
    });
}

#[doc(hidden)]
pub fn record_join_hash_state_build() {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.hash_state_builds = snapshot.hash_state_builds.saturating_add(1);
        probe.set(Some(snapshot));
    });
}

#[doc(hidden)]
pub fn record_join_hash_state_reuse() {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.hash_state_reuses = snapshot.hash_state_reuses.saturating_add(1);
        probe.set(Some(snapshot));
    });
}

/// Stable, label-free buckets for physical JOIN implementations. Keeping the
/// enum in-process avoids an unbounded metric label/cardinality surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum JoinExecutionKind {
    HashStreaming,
    HashParallel,
    Merge,
    NestedLoop,
    IndexNestedLoop,
    BatchIndexNestedLoop,
}

/// One aggregate physical JOIN observation.
///
/// Callers accumulate row-level work locally and publish once when an operator
/// closes. This keeps observability overhead independent of result cardinality.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct JoinExecutionRecord {
    pub left_rows: u64,
    pub right_rows: u64,
    pub output_rows: u64,
    pub left_width: u64,
    pub right_width: u64,
    pub output_width: u64,
    pub candidate_pairs: u64,
    pub lookup_calls: u64,
    pub lookup_candidate_rows: u64,
    pub wall_nanos: u64,
    /// Time spent pulling a bounded outer batch. For a chain this deliberately
    /// includes the child edge which produced those rows.
    pub outer_pull_nanos: u64,
    /// Local key extraction and de-duplication for batched indexed JOINs.
    pub key_prepare_nanos: u64,
    /// Storage/index resolution and projected candidate fetch.
    pub lookup_nanos: u64,
    /// Construction of the local key-to-candidate map for non-unique edges.
    pub candidate_map_nanos: u64,
}

/// One completed physical operator. The trace is request-local, contains no
/// keys or SQL values and is capped, so EXPLAIN cannot create unbounded metric
/// labels or per-row history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct JoinExecutionTraceRecord {
    pub execution_slot: u64,
    pub kind: JoinExecutionKind,
    pub record: JoinExecutionRecord,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct JoinExecutionTraceSnapshot {
    pub records: Vec<JoinExecutionTraceRecord>,
    pub truncated_operators: u64,
}

/// Request-local aggregate of physical JOIN work.
///
/// `EXPLAIN ANALYZE` must describe the request it executed, not process-wide
/// counters that may include concurrent sessions. The first completed
/// operator is also retained: for the current left-deep executor it is the
/// selective root edge and provides a bounded proof that predicate pushdown
/// reached the intended starting relation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct JoinExecutionProbeSnapshot {
    pub operator_calls: u64,
    pub hash_streaming_calls: u64,
    pub hash_parallel_calls: u64,
    pub merge_calls: u64,
    pub nested_loop_calls: u64,
    pub index_nested_loop_calls: u64,
    pub batch_index_nested_loop_calls: u64,
    pub left_input_rows: u64,
    pub right_input_rows: u64,
    pub output_rows: u64,
    pub input_values: u64,
    pub output_values: u64,
    pub max_output_width: u64,
    pub candidate_pairs: u64,
    pub deferred_rows: u64,
    pub deferred_rows_consumed: u64,
    pub deferred_boundary_rows: u64,
    pub owned_rows_constructed: u64,
    /// Values cloned while a deferred JOIN row is materialized. Moving owned
    /// values does not contribute to this counter.
    pub copied_values: u64,
    /// Conservative in-memory bytes corresponding to `copied_values`.
    pub copied_bytes: u64,
    /// Opens of streaming QueryResult sources inside the JOIN graph.
    pub source_open_calls: u64,
    /// Re-opening an already opened source is a rescan and must remain zero for
    /// the depth/first-row pipeline.
    pub source_rescans: u64,
    pub top_n_calls: u64,
    pub top_n_input_rows: u64,
    pub top_n_output_rows: u64,
    pub top_n_peak_candidates: u64,
    pub top_n_peak_bytes: u64,
    pub ordered_sort_calls: u64,
    pub ordered_sort_input_rows: u64,
    pub ordered_sort_spill_runs: u64,
    pub ordered_sort_peak_rows: u64,
    pub ordered_sort_peak_bytes: u64,
    /// Time spent draining/materializing the input of the blocking ORDER BY.
    /// This includes the upstream operator work needed to produce those rows.
    pub ordered_collect_nanos: u64,
    /// Time spent finalizing the ordered owner after input collection. For an
    /// in-memory run this is the pure sort; for a spilled owner it is the
    /// bounded external-merge setup (run creation remains part of collect).
    pub ordered_finalize_nanos: u64,
    pub ordering_skip_calls: u64,
    pub hash_state_builds: u64,
    pub hash_state_reuses: u64,
    pub lookup_calls: u64,
    pub lookup_candidate_rows: u64,
    pub lookup_key_rows: u64,
    pub lookup_distinct_keys: u64,
    pub lookup_repeated_keys_eliminated: u64,
    pub wall_nanos: u64,
    pub outer_pull_nanos: u64,
    pub key_prepare_nanos: u64,
    pub lookup_nanos: u64,
    pub candidate_map_nanos: u64,
    pub first_kind: Option<JoinExecutionKind>,
    pub first_left_input_rows: u64,
    pub first_right_input_rows: u64,
    pub first_output_rows: u64,
}

/// One bounded whole-component planning decision. Alias/order strings are
/// already owned by the parsed statement; this request-local copy is discarded
/// when EXPLAIN ANALYZE finishes and never enters process-wide metric labels.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct JoinPlanningRecord {
    pub original_order: Vec<String>,
    pub planned_order: Vec<String>,
    pub root_estimated_rows: u64,
    pub estimated_cost: u64,
    pub safe_limit: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct JoinPlanningProbeSnapshot {
    pub components: Vec<JoinPlanningRecord>,
    pub logical_planning_calls: u64,
    pub logical_planning_nanos: u64,
    pub physical_planning_calls: u64,
    pub physical_planning_nanos: u64,
}

#[doc(hidden)]
pub fn record_join_logical_planning(elapsed: Duration) {
    JOIN_PLANNING_PROBE.with(|probe| {
        let mut probe = probe.borrow_mut();
        let Some(snapshot) = probe.as_mut() else {
            return;
        };
        snapshot.logical_planning_calls = snapshot.logical_planning_calls.saturating_add(1);
        snapshot.logical_planning_nanos = snapshot
            .logical_planning_nanos
            .saturating_add(duration_nanos(elapsed));
    });
}

#[doc(hidden)]
pub fn record_join_physical_planning(elapsed: Duration) {
    JOIN_PLANNING_PROBE.with(|probe| {
        let mut probe = probe.borrow_mut();
        let Some(snapshot) = probe.as_mut() else {
            return;
        };
        snapshot.physical_planning_calls = snapshot.physical_planning_calls.saturating_add(1);
        snapshot.physical_planning_nanos = snapshot
            .physical_planning_nanos
            .saturating_add(duration_nanos(elapsed));
    });
}

#[doc(hidden)]
pub fn record_join_planning(record: JoinPlanningRecord) {
    const MAX_COMPONENTS: usize = 32;
    JOIN_PLANNING_PROBE.with(|probe| {
        let mut probe = probe.borrow_mut();
        let Some(snapshot) = probe.as_mut() else {
            return;
        };
        if snapshot
            .components
            .iter()
            .any(|existing| existing.planned_order.starts_with(&record.planned_order))
        {
            return;
        }
        if snapshot.components.len() < MAX_COMPONENTS {
            snapshot.components.push(record);
        }
    });
}

impl JoinExecutionKind {
    #[doc(hidden)]
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::HashStreaming => "hash_streaming",
            Self::HashParallel => "hash_parallel",
            Self::Merge => "merge",
            Self::NestedLoop => "nested_loop",
            Self::IndexNestedLoop => "index_nested_loop",
            Self::BatchIndexNestedLoop => "batch_index_nested_loop",
        }
    }
}

#[doc(hidden)]
pub fn record_join_execution(kind: JoinExecutionKind, record: JoinExecutionRecord) {
    const MAX_TRACED_OPERATORS: usize = 64;
    JOIN_EXECUTION_TRACE.with(|trace| {
        let mut trace = trace.borrow_mut();
        let Some(snapshot) = trace.as_mut() else {
            return;
        };
        if snapshot.records.len() < MAX_TRACED_OPERATORS {
            snapshot.records.push(JoinExecutionTraceRecord {
                execution_slot: u64::try_from(snapshot.records.len()).unwrap_or(u64::MAX),
                kind,
                record,
            });
        } else {
            snapshot.truncated_operators = snapshot.truncated_operators.saturating_add(1);
        }
    });
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        if snapshot.operator_calls == 0 {
            snapshot.first_kind = Some(kind);
            snapshot.first_left_input_rows = record.left_rows;
            snapshot.first_right_input_rows = record.right_rows;
            snapshot.first_output_rows = record.output_rows;
        }
        snapshot.operator_calls = snapshot.operator_calls.saturating_add(1);
        match kind {
            JoinExecutionKind::HashStreaming => {
                snapshot.hash_streaming_calls = snapshot.hash_streaming_calls.saturating_add(1)
            }
            JoinExecutionKind::HashParallel => {
                snapshot.hash_parallel_calls = snapshot.hash_parallel_calls.saturating_add(1)
            }
            JoinExecutionKind::Merge => {
                snapshot.merge_calls = snapshot.merge_calls.saturating_add(1)
            }
            JoinExecutionKind::NestedLoop => {
                snapshot.nested_loop_calls = snapshot.nested_loop_calls.saturating_add(1)
            }
            JoinExecutionKind::IndexNestedLoop => {
                snapshot.index_nested_loop_calls =
                    snapshot.index_nested_loop_calls.saturating_add(1)
            }
            JoinExecutionKind::BatchIndexNestedLoop => {
                snapshot.batch_index_nested_loop_calls =
                    snapshot.batch_index_nested_loop_calls.saturating_add(1)
            }
        }
        snapshot.left_input_rows = snapshot.left_input_rows.saturating_add(record.left_rows);
        snapshot.right_input_rows = snapshot.right_input_rows.saturating_add(record.right_rows);
        snapshot.output_rows = snapshot.output_rows.saturating_add(record.output_rows);
        snapshot.input_values = snapshot
            .input_values
            .saturating_add(record.left_rows.saturating_mul(record.left_width))
            .saturating_add(record.right_rows.saturating_mul(record.right_width));
        snapshot.output_values = snapshot
            .output_values
            .saturating_add(record.output_rows.saturating_mul(record.output_width));
        snapshot.max_output_width = snapshot.max_output_width.max(record.output_width);
        snapshot.candidate_pairs = snapshot
            .candidate_pairs
            .saturating_add(record.candidate_pairs);
        snapshot.lookup_calls = snapshot.lookup_calls.saturating_add(record.lookup_calls);
        snapshot.lookup_candidate_rows = snapshot
            .lookup_candidate_rows
            .saturating_add(record.lookup_candidate_rows);
        snapshot.wall_nanos = snapshot.wall_nanos.saturating_add(record.wall_nanos);
        snapshot.outer_pull_nanos = snapshot
            .outer_pull_nanos
            .saturating_add(record.outer_pull_nanos);
        snapshot.key_prepare_nanos = snapshot
            .key_prepare_nanos
            .saturating_add(record.key_prepare_nanos);
        snapshot.lookup_nanos = snapshot.lookup_nanos.saturating_add(record.lookup_nanos);
        snapshot.candidate_map_nanos = snapshot
            .candidate_map_nanos
            .saturating_add(record.candidate_map_nanos);
        probe.set(Some(snapshot));
    });

    add(&COUNTERS.join_operator_calls, 1);
    match kind {
        JoinExecutionKind::HashStreaming => add(&COUNTERS.join_hash_streaming_calls, 1),
        JoinExecutionKind::HashParallel => add(&COUNTERS.join_hash_parallel_calls, 1),
        JoinExecutionKind::Merge => add(&COUNTERS.join_merge_calls, 1),
        JoinExecutionKind::NestedLoop => add(&COUNTERS.join_nested_loop_calls, 1),
        JoinExecutionKind::IndexNestedLoop => add(&COUNTERS.join_index_nested_loop_calls, 1),
        JoinExecutionKind::BatchIndexNestedLoop => {
            add(&COUNTERS.join_batch_index_nested_loop_calls, 1)
        }
    }
    add(&COUNTERS.join_left_input_rows, record.left_rows);
    add(&COUNTERS.join_right_input_rows, record.right_rows);
    add(&COUNTERS.join_output_rows, record.output_rows);
    add(
        &COUNTERS.join_input_values,
        record
            .left_rows
            .saturating_mul(record.left_width)
            .saturating_add(record.right_rows.saturating_mul(record.right_width)),
    );
    add(
        &COUNTERS.join_output_values,
        record.output_rows.saturating_mul(record.output_width),
    );
    add(&COUNTERS.join_candidate_pairs, record.candidate_pairs);
    add(&COUNTERS.join_lookup_calls, record.lookup_calls);
    add(
        &COUNTERS.join_lookup_candidate_rows,
        record.lookup_candidate_rows,
    );
    add(&COUNTERS.join_wall_nanos, record.wall_nanos);
    update_max(&COUNTERS.join_max_left_input_rows, record.left_rows);
    update_max(&COUNTERS.join_max_right_input_rows, record.right_rows);
    update_max(&COUNTERS.join_max_output_rows, record.output_rows);
    update_max(&COUNTERS.join_max_output_width, record.output_width);
}

/// Record values that were actually cloned while materializing a deferred
/// JOIN row. The probe is request-local and inactive in ordinary execution, so
/// this adds no global per-row metric stream.
#[doc(hidden)]
pub fn record_join_value_copies(values: &[Value]) {
    #[cfg(any(test, feature = "test-hooks"))]
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        let bytes = values.iter().fold(0_u64, |total, value| {
            let payload = match value {
                Value::Text(text) => text.len(),
                Value::Extension(bytes) => bytes.len(),
                _ => 0,
            };
            total.saturating_add(
                u64::try_from(std::mem::size_of::<Value>().saturating_add(payload))
                    .unwrap_or(u64::MAX),
            )
        });
        snapshot.copied_values = snapshot
            .copied_values
            .saturating_add(u64::try_from(values.len()).unwrap_or(u64::MAX));
        snapshot.copied_bytes = snapshot.copied_bytes.saturating_add(bytes);
        probe.set(Some(snapshot));
    });

    #[cfg(not(test))]
    let _ = values;
}

/// Record opening a streaming source. `reopen=true` means the same adapter was
/// started again and therefore the already traversed prefix was rescanned.
#[doc(hidden)]
pub fn record_join_source_open(reopen: bool) {
    JOIN_EXECUTION_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        snapshot.source_open_calls = snapshot.source_open_calls.saturating_add(1);
        if reopen {
            snapshot.source_rescans = snapshot.source_rescans.saturating_add(1);
        }
        probe.set(Some(snapshot));
    });
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct NavigationQueryCounters {
    pub paths_planned: u64,
    pub paths_executed: u64,
    pub source_rows: u64,
    pub distinct_source_keys: u64,
    pub repeated_keys_eliminated: u64,
    pub lookup_batches: u64,
    pub lookup_hits: u64,
    pub lookup_misses: u64,
    pub direct_edges: u64,
    pub index_nested_loop_edges: u64,
    pub batch_edges: u64,
    pub hash_edges: u64,
    pub merge_edges: u64,
    pub fallback_edges: u64,
    pub planner_left_join_edges: u64,
}

/// Record one completed navigation statement. Counts are aggregate only;
/// source/target keys and SQL parameters never enter instrumentation.
#[doc(hidden)]
pub fn record_navigation_query(counters: NavigationQueryCounters) {
    add(&COUNTERS.navigation_paths_planned, counters.paths_planned);
    add(&COUNTERS.navigation_paths_executed, counters.paths_executed);
    add(&COUNTERS.navigation_source_rows, counters.source_rows);
    add(
        &COUNTERS.navigation_distinct_source_keys,
        counters.distinct_source_keys,
    );
    add(
        &COUNTERS.navigation_repeated_keys_eliminated,
        counters.repeated_keys_eliminated,
    );
    add(&COUNTERS.navigation_lookup_batches, counters.lookup_batches);
    add(&COUNTERS.navigation_lookup_hits, counters.lookup_hits);
    add(&COUNTERS.navigation_lookup_misses, counters.lookup_misses);
    add(&COUNTERS.navigation_direct_edges, counters.direct_edges);
    add(
        &COUNTERS.navigation_index_nested_loop_edges,
        counters.index_nested_loop_edges,
    );
    add(&COUNTERS.navigation_batch_edges, counters.batch_edges);
    add(&COUNTERS.navigation_hash_edges, counters.hash_edges);
    add(&COUNTERS.navigation_merge_edges, counters.merge_edges);
    add(&COUNTERS.navigation_fallback_edges, counters.fallback_edges);
    add(
        &COUNTERS.navigation_planner_left_join_edges,
        counters.planner_left_join_edges,
    );
}

#[doc(hidden)]
pub fn record_navigation_integrity_failure() {
    add(&COUNTERS.navigation_integrity_failures, 1);
}

#[doc(hidden)]
pub fn record_navigation_cancellation(timed_out: bool) {
    if timed_out {
        add(&COUNTERS.navigation_timeouts, 1);
    } else {
        add(&COUNTERS.navigation_cancellations, 1);
    }
}

/// Record rows delivered through the server cursor protocol.
pub fn record_protocol_result_rows(rows: u64) {
    add(&COUNTERS.protocol_result_rows, rows);
}

/// Record work caused by adapting one storage row to the legacy row protocol.
#[inline]
pub fn record_protocol_row_adapter(values: u64) {
    PROTOCOL_ROW_ADAPTER_PENDING.with(|pending| {
        let mut next = pending.0.get();
        next.values = next.values.saturating_add(values);
        if next.values >= 16_384 {
            flush_protocol_row_adapter_pending(next);
            pending.0.set(ProtocolRowAdapterPending::EMPTY);
        } else {
            pending.0.set(next);
        }
    });
}

/// Record a completed prepared legacy row batch.
#[inline]
pub fn record_protocol_row_batch(rows: u64) {
    if rows == 0 {
        return;
    }
    add(&COUNTERS.protocol_row_batch_frames, 1);
}

/// Record one ColumnBatchV1 request that deliberately used the row protocol.
#[inline]
pub fn record_protocol_column_batch_fallback(reason: ProtocolColumnBatchFallback) {
    add(&COUNTERS.protocol_column_batch_fallbacks, 1);
    match reason {
        ProtocolColumnBatchFallback::RowState => {
            add(&COUNTERS.protocol_column_batch_fallback_row_state, 1)
        }
        ProtocolColumnBatchFallback::QueryShape => {
            add(&COUNTERS.protocol_column_batch_fallback_query_shape, 1)
        }
        ProtocolColumnBatchFallback::StorageShape => {
            add(&COUNTERS.protocol_column_batch_fallback_storage_shape, 1)
        }
        ProtocolColumnBatchFallback::Schema => {
            add(&COUNTERS.protocol_column_batch_fallback_schema, 1)
        }
        ProtocolColumnBatchFallback::Unknown => {
            add(&COUNTERS.protocol_column_batch_fallback_unknown, 1)
        }
    }
}

/// Record an oversized typed group retained between `FetchColumnBatch`
/// requests. Normal-size groups never hit this counter: they move directly
/// into one prepared response.
#[inline]
pub fn record_protocol_column_batch_pending_opened(rows: u64, retained_bytes: u64) {
    add(&COUNTERS.protocol_column_batch_pending_opened, 1);
    let current = COUNTERS
        .protocol_column_batch_pending_current
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    let rows_current = COUNTERS
        .protocol_column_batch_pending_rows_current
        .fetch_add(rows, Ordering::Relaxed)
        .saturating_add(rows);
    let bytes_current = COUNTERS
        .protocol_column_batch_pending_bytes_current
        .fetch_add(retained_bytes, Ordering::Relaxed)
        .saturating_add(retained_bytes);
    update_max(&COUNTERS.protocol_column_batch_pending_max, current);
    update_max(
        &COUNTERS.protocol_column_batch_pending_rows_max,
        rows_current,
    );
    update_max(
        &COUNTERS.protocol_column_batch_pending_bytes_max,
        bytes_current,
    );
    record_protocol_pending_column_batch_probe(|probe| {
        probe.opened = probe.opened.saturating_add(1);
        probe.current = probe.current.saturating_add(1);
        probe.max = probe.max.max(probe.current);
        probe.rows_current = probe.rows_current.saturating_add(rows);
        probe.rows_max = probe.rows_max.max(probe.rows_current);
        probe.bytes_current = probe.bytes_current.saturating_add(retained_bytes);
        probe.bytes_max = probe.bytes_max.max(probe.bytes_current);
    });
}

#[inline]
pub fn record_protocol_column_batch_pending_completed(rows: u64, retained_bytes: u64) {
    add(&COUNTERS.protocol_column_batch_pending_completed, 1);
    close_protocol_column_batch_pending(rows, retained_bytes, |probe| {
        probe.completed = probe.completed.saturating_add(1);
    });
}

#[inline]
pub fn record_protocol_column_batch_pending_dropped(rows: u64, retained_bytes: u64) {
    add(&COUNTERS.protocol_column_batch_pending_dropped, 1);
    close_protocol_column_batch_pending(rows, retained_bytes, |probe| {
        probe.dropped = probe.dropped.saturating_add(1);
    });
}

#[inline]
pub fn record_protocol_encode(bytes: u64, elapsed: Duration) {
    add(&COUNTERS.protocol_encode_calls, 1);
    add(&COUNTERS.protocol_encode_bytes, bytes);
    add(&COUNTERS.protocol_encode_nanos, duration_nanos(elapsed));
    record_protocol_component_probe(|probe| {
        probe.encode_calls = probe.encode_calls.saturating_add(1);
        probe.encode_bytes = probe.encode_bytes.saturating_add(bytes);
    });
}

#[inline]
pub fn record_protocol_socket_write(bytes: u64, elapsed: Duration) {
    add(&COUNTERS.protocol_socket_write_calls, 1);
    add(&COUNTERS.protocol_socket_write_bytes, bytes);
    add(
        &COUNTERS.protocol_socket_write_nanos,
        duration_nanos(elapsed),
    );
    record_protocol_component_probe(|probe| {
        probe.socket_write_calls = probe.socket_write_calls.saturating_add(1);
        probe.socket_write_bytes = probe.socket_write_bytes.saturating_add(bytes);
    });
}

#[inline]
fn record_protocol_component_probe(update: impl FnOnce(&mut ProtocolComponentProbeSnapshot)) {
    #[cfg(any(test, feature = "test-hooks"))]
    PROTOCOL_COMPONENT_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        update(&mut snapshot);
        probe.set(Some(snapshot));
    });

    #[cfg(not(test))]
    {
        let _ = update;
    }
}

#[inline]
fn record_protocol_pending_column_batch_probe(
    update: impl FnOnce(&mut ProtocolPendingColumnBatchProbeSnapshot),
) {
    #[cfg(any(test, feature = "test-hooks"))]
    PROTOCOL_PENDING_COLUMN_BATCH_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        update(&mut snapshot);
        probe.set(Some(snapshot));
    });

    #[cfg(not(test))]
    {
        let _ = update;
    }
}

pub fn record_ram_accelerator_build(entries: u64, bytes: u64, elapsed: Duration) {
    add(&COUNTERS.ram_accelerator_builds, 1);
    add(&COUNTERS.ram_accelerator_build_entries, entries);
    add(&COUNTERS.ram_accelerator_build_bytes, bytes);
    add(
        &COUNTERS.ram_accelerator_build_nanos,
        duration_nanos(elapsed),
    );
    record_ram_accelerator_probe(|probe| {
        probe.builds = probe.builds.saturating_add(1);
        probe.build_entries = probe.build_entries.saturating_add(entries);
        probe.build_bytes = probe.build_bytes.saturating_add(bytes);
    });
}

pub fn record_ram_accelerator_hit() {
    add(&COUNTERS.ram_accelerator_hits, 1);
    record_ram_accelerator_probe(|probe| {
        probe.hits = probe.hits.saturating_add(1);
    });
}

pub fn record_ram_accelerator_miss() {
    add(&COUNTERS.ram_accelerator_misses, 1);
    record_ram_accelerator_probe(|probe| {
        probe.misses = probe.misses.saturating_add(1);
    });
}

pub fn record_ram_accelerator_fallback() {
    add(&COUNTERS.ram_accelerator_fallbacks, 1);
    record_ram_accelerator_probe(|probe| {
        probe.fallbacks = probe.fallbacks.saturating_add(1);
    });
}

pub fn record_ram_accelerator_eviction(bytes: u64) {
    add(&COUNTERS.ram_accelerator_evictions, 1);
    add(&COUNTERS.ram_accelerator_eviction_bytes, bytes);
    record_ram_accelerator_probe(|probe| {
        probe.evictions = probe.evictions.saturating_add(1);
        probe.eviction_bytes = probe.eviction_bytes.saturating_add(bytes);
    });
}

#[inline]
fn record_ram_accelerator_probe(update: impl FnOnce(&mut RamAcceleratorProbeSnapshot)) {
    #[cfg(any(test, feature = "test-hooks"))]
    RAM_ACCELERATOR_PROBE.with(|probe| {
        let Some(mut snapshot) = probe.get() else {
            return;
        };
        update(&mut snapshot);
        probe.set(Some(snapshot));
    });

    #[cfg(not(test))]
    {
        let _ = update;
    }
}

#[inline]
pub fn record_wal_append_count(entries: u64, bytes: u64) {
    WAL_APPEND_PENDING.with(|pending| {
        let mut next = pending.0.get();
        next.entries = next.entries.saturating_add(entries);
        next.bytes = next.bytes.saturating_add(bytes);

        if next.entries >= 16_384 || next.bytes >= 8 * 1024 * 1024 {
            flush_wal_append_pending(next);
            pending.0.set(WalAppendPending::EMPTY);
        } else {
            pending.0.set(next);
        }
    });
}

pub fn record_wal_write(bytes: u64, elapsed: Duration) {
    add(&COUNTERS.wal_write_calls, 1);
    add(&COUNTERS.wal_write_bytes, bytes);
    add(&COUNTERS.wal_write_nanos, duration_nanos(elapsed));
}

pub fn record_wal_sync(elapsed: Duration) {
    add(&COUNTERS.wal_sync_calls, 1);
    add(&COUNTERS.wal_sync_nanos, duration_nanos(elapsed));
}

pub fn record_wal_generation_validation(bytes: u64, elapsed: Duration) {
    add(&COUNTERS.wal_generation_validation_calls, 1);
    add(&COUNTERS.wal_generation_validation_bytes, bytes);
    add(
        &COUNTERS.wal_generation_validation_nanos,
        duration_nanos(elapsed),
    );
}

pub fn record_wal_retention(identity_checks: u64, files_deleted: u64, elapsed: Duration) {
    add(&COUNTERS.wal_retention_calls, 1);
    add(&COUNTERS.wal_retention_identity_checks, identity_checks);
    add(&COUNTERS.wal_retention_files_deleted, files_deleted);
    add(&COUNTERS.wal_retention_nanos, duration_nanos(elapsed));
}

#[doc(hidden)]
pub fn record_copy(
    rows: u64,
    parse_elapsed: Duration,
    commit_elapsed: Duration,
    total_elapsed: Duration,
) {
    add(&COUNTERS.copy_calls, 1);
    add(&COUNTERS.copy_rows, rows);
    add(&COUNTERS.copy_parse_nanos, duration_nanos(parse_elapsed));
    add(&COUNTERS.copy_commit_nanos, duration_nanos(commit_elapsed));
    add(&COUNTERS.copy_total_nanos, duration_nanos(total_elapsed));
}

#[doc(hidden)]
pub fn record_cold_constraint_batch(
    rows: usize,
    segments: usize,
    pk_elapsed: Duration,
    total_elapsed: Duration,
) {
    add(&COUNTERS.cold_constraint_batch_calls, 1);
    add(&COUNTERS.cold_constraint_batch_rows, rows as u64);
    add(&COUNTERS.cold_constraint_batch_segments, segments as u64);
    add(
        &COUNTERS.cold_constraint_batch_nanos,
        duration_nanos(total_elapsed),
    );
    add(&COUNTERS.cold_pk_batch_nanos, duration_nanos(pk_elapsed));
}

pub fn record_seal(rows: u64, bytes: u64, elapsed: Duration) {
    add(&COUNTERS.seal_calls, 1);
    add(&COUNTERS.seal_rows, rows);
    add(&COUNTERS.seal_bytes, bytes);
    add(&COUNTERS.seal_nanos, duration_nanos(elapsed));
}

pub fn record_seal_output(bytes: u64) {
    add(&COUNTERS.seal_output_bytes, bytes);
}


#[doc(hidden)]
pub fn record_posting_exact_lookup_fanout(segments: usize) {
    let segments = segments as u64;
    add(&COUNTERS.posting_exact_lookup_calls, 1);
    add(&COUNTERS.posting_exact_lookup_segments, segments);
    COUNTERS
        .posting_exact_lookup_max_segments
        .fetch_max(segments, Ordering::Relaxed);
    #[cfg(any(test, feature = "test-hooks"))]
    POSTING_LOOKUP_PROBE.with(|probe| {
        if let Some(mut snapshot) = probe.get() {
            snapshot.exact_calls = snapshot.exact_calls.saturating_add(1);
            snapshot.exact_segments = snapshot.exact_segments.saturating_add(segments);
            probe.set(Some(snapshot));
        }
    });
}

#[doc(hidden)]
pub fn record_posting_ordered_lookup_fanout(segments: usize) {
    let segments = segments as u64;
    add(&COUNTERS.posting_ordered_lookup_calls, 1);
    add(&COUNTERS.posting_ordered_lookup_segments, segments);
    COUNTERS
        .posting_ordered_lookup_max_segments
        .fetch_max(segments, Ordering::Relaxed);
    #[cfg(any(test, feature = "test-hooks"))]
    POSTING_LOOKUP_PROBE.with(|probe| {
        if let Some(mut snapshot) = probe.get() {
            snapshot.ordered_calls = snapshot.ordered_calls.saturating_add(1);
            snapshot.ordered_segments = snapshot.ordered_segments.saturating_add(segments);
            probe.set(Some(snapshot));
        }
    });
}

pub fn record_compaction(tables: u64, elapsed: Duration) {
    add(&COUNTERS.compaction_calls, 1);
    add(&COUNTERS.compaction_tables, tables);
    add(&COUNTERS.compaction_nanos, duration_nanos(elapsed));
}

#[doc(hidden)]
pub fn record_compaction_spool_write(bytes: u64, elapsed: Duration) {
    add(&COUNTERS.compaction_spool_write_calls, 1);
    add(&COUNTERS.compaction_spool_write_bytes, bytes);
    add(
        &COUNTERS.compaction_spool_write_nanos,
        duration_nanos(elapsed),
    );
}

#[doc(hidden)]
pub fn record_compaction_spool_read(bytes: u64, elapsed: Duration) {
    add(&COUNTERS.compaction_spool_read_calls, 1);
    add(&COUNTERS.compaction_spool_read_bytes, bytes);
    add(
        &COUNTERS.compaction_spool_read_nanos,
        duration_nanos(elapsed),
    );
}

#[inline]
fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

#[inline]
fn subtract_saturating(counter: &AtomicU64, value: u64) {
    if value == 0 {
        return;
    }
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(value))
    });
}

#[inline]
fn subtract_gauge(counter: &AtomicU64, value: u64) {
    if value == 0 {
        return;
    }
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(value))
    });
}

#[inline]
fn replace_gauge(counter: &AtomicU64, before: u64, after: u64) {
    if after >= before {
        add(counter, after - before);
    } else {
        subtract_gauge(counter, before - after);
    }
}

#[inline]
fn update_max(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    while value > current {
        match counter.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

#[inline]
fn close_protocol_column_batch_pending(
    rows: u64,
    retained_bytes: u64,
    update_probe: impl FnOnce(&mut ProtocolPendingColumnBatchProbeSnapshot),
) {
    subtract_saturating(&COUNTERS.protocol_column_batch_pending_current, 1);
    subtract_saturating(&COUNTERS.protocol_column_batch_pending_rows_current, rows);
    subtract_saturating(
        &COUNTERS.protocol_column_batch_pending_bytes_current,
        retained_bytes,
    );
    record_protocol_pending_column_batch_probe(|probe| {
        update_probe(probe);
        probe.current = probe.current.saturating_sub(1);
        probe.rows_current = probe.rows_current.saturating_sub(rows);
        probe.bytes_current = probe.bytes_current.saturating_sub(retained_bytes);
    });
}

#[inline]
fn flush_row_materialization_pending_current_thread() {
    ROW_MATERIALIZATION_PENDING.with(|pending| {
        let current = pending.0.get();
        flush_row_materialization_pending(current);
        pending.0.set(RowMaterializationPending::EMPTY);
    });
}

#[inline]
fn flush_wal_append_pending_current_thread() {
    WAL_APPEND_PENDING.with(|pending| {
        let current = pending.0.get();
        flush_wal_append_pending(current);
        pending.0.set(WalAppendPending::EMPTY);
    });
}

#[inline]
fn flush_protocol_row_adapter_pending_current_thread() {
    PROTOCOL_ROW_ADAPTER_PENDING.with(|pending| {
        let current = pending.0.get();
        flush_protocol_row_adapter_pending(current);
        pending.0.set(ProtocolRowAdapterPending::EMPTY);
    });
}

#[inline]
fn flush_row_materialization_pending(pending: RowMaterializationPending) {
    if pending.calls == 0 {
        return;
    }

    add(&COUNTERS.row_materialization_calls, pending.calls);
    add(&COUNTERS.row_materialization_rows, pending.rows);
    add(&COUNTERS.row_materialization_values, pending.values);
}

#[inline]
fn flush_wal_append_pending(pending: WalAppendPending) {
    if pending.entries == 0 {
        return;
    }

    add(&COUNTERS.wal_append_entries, pending.entries);
    add(&COUNTERS.wal_append_bytes, pending.bytes);
}

#[inline]
fn flush_protocol_row_adapter_pending(pending: ProtocolRowAdapterPending) {
    if pending.values == 0 {
        return;
    }
    add(&COUNTERS.protocol_row_to_wire_values, pending.values);
}

#[inline]
fn duration_nanos(elapsed: Duration) -> u64 {
    elapsed.as_nanos().min(u128::from(u64::MAX)) as u64
}
