fn measure_server_case<T>(
    report: &mut BenchReport,
    case: &str,
    measure: impl FnOnce() -> BenchResult<(T, u64, Option<String>)>,
) -> BenchResult<T> {
    let engine_before = instrumentation::snapshot();
    let client_protocol_before = client_protocol_counters_snapshot();
    let start = Instant::now();
    let measured = measure();
    let elapsed = start.elapsed();
    let client_protocol_after = client_protocol_counters_snapshot();
    let engine_after = instrumentation::snapshot();
    let counters = engine_counter_delta(&engine_after, &engine_before);
    report.engine.push(EngineMetric {
        participant: Participant::Server.as_str().to_string(),
        phase: format!("case.{case}"),
        counters,
    });
    report.client_protocol.push(ClientProtocolMetric {
        participant: Participant::Server.as_str().to_string(),
        phase: format!("case.{case}"),
        counters: client_protocol_counter_delta(&client_protocol_after, &client_protocol_before),
    });
    let (value, rows, checksum) = measured?;
    report.metrics.push(Metric::measured(
        Participant::Server,
        case,
        elapsed,
        rows,
        checksum,
    ));
    record_engine_snapshot(report, Participant::Server, format!("case.{case}.after"));
    Ok(value)
}

fn record_engine_snapshot(
    report: &mut BenchReport,
    participant: Participant,
    phase: impl Into<String>,
) {
    let counters = instrumentation::snapshot();
    let diagnostics = engine_counter_diagnostics(&counters);
    report.engine_snapshots.push(EngineSnapshotMetric {
        participant: participant.as_str().to_string(),
        phase: phase.into(),
        counters,
        diagnostics,
    });
}

fn engine_counter_diagnostics(counters: &EngineCountersSnapshot) -> Vec<EngineCounterDiagnostic> {
    let mut diagnostics = Vec::new();

    if counters.artifact_pread_bytes > 0 && counters.artifact_pread_calls == 0 {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "artifact-pread-bytes-without-calls",
            "artifact_pread_bytes is non-zero while artifact_pread_calls is zero",
        );
    }
    if counters.artifact_pread_nanos > 0 && counters.artifact_pread_calls == 0 {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "artifact-pread-time-without-calls",
            "artifact_pread_nanos is non-zero while artifact_pread_calls is zero",
        );
    }
    if counters.artifact_payload_decompress_raw_bytes > 0 && counters.artifact_payload_decompress_calls == 0 {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "artifact-decompress-bytes-without-calls",
            "artifact_payload_decompress_raw_bytes is non-zero while artifact_payload_decompress_calls is zero",
        );
    }
    if counters.artifact_column_deserialize_rows > 0 && counters.artifact_column_deserialize_calls == 0 {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "artifact-deserialize-rows-without-calls",
            "artifact_column_deserialize_rows is non-zero while artifact_column_deserialize_calls is zero",
        );
    }
    if counters.protocol_encode_bytes > 0 && counters.protocol_encode_calls == 0 {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-encode-bytes-without-calls",
            "protocol_encode_bytes is non-zero while protocol_encode_calls is zero",
        );
    }
    if counters.protocol_socket_write_bytes > 0 && counters.protocol_socket_write_calls == 0 {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-socket-bytes-without-calls",
            "protocol_socket_write_bytes is non-zero while protocol_socket_write_calls is zero",
        );
    }
    if counters.metadata_pk_count_applied > counters.metadata_pk_count_attempts {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "metadata-pk-applied-over-attempts",
            "metadata_pk_count_applied is greater than metadata_pk_count_attempts",
        );
    }
    let metadata_fallback_reasons = counters
        .metadata_pk_count_fallback_snapshot
        .saturating_add(counters.metadata_pk_count_fallback_seal_overlap)
        .saturating_add(counters.metadata_pk_count_fallback_unsupported)
        .saturating_add(counters.metadata_pk_count_fallback_candidate_limit);
    if counters.metadata_pk_count_fallbacks != metadata_fallback_reasons {
        push_counter_diagnostic(
            &mut diagnostics,
            "warn",
            "metadata-pk-fallback-reason-mismatch",
            format!(
                "metadata_pk_count_fallbacks={} but reason sum={metadata_fallback_reasons}",
                counters.metadata_pk_count_fallbacks
            ),
        );
    }
    let column_batch_fallback_reasons = counters
        .protocol_column_batch_fallback_row_state
        .saturating_add(counters.protocol_column_batch_fallback_query_shape)
        .saturating_add(counters.protocol_column_batch_fallback_storage_shape)
        .saturating_add(counters.protocol_column_batch_fallback_schema)
        .saturating_add(counters.protocol_column_batch_fallback_unknown);
    if counters.protocol_column_batch_fallbacks != column_batch_fallback_reasons {
        push_counter_diagnostic(
            &mut diagnostics,
            "warn",
            "protocol-column-batch-fallback-reason-mismatch",
            format!(
                "protocol_column_batch_fallbacks={} but reason sum={column_batch_fallback_reasons}",
                counters.protocol_column_batch_fallbacks
            ),
        );
    }
    let column_batch_pending_closed = counters
        .protocol_column_batch_pending_completed
        .saturating_add(counters.protocol_column_batch_pending_dropped);
    if column_batch_pending_closed > counters.protocol_column_batch_pending_opened {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-close-over-open",
            format!(
                "protocol column pending completed+dropped={column_batch_pending_closed} but opened={}",
                counters.protocol_column_batch_pending_opened
            ),
        );
    }
    if counters.protocol_column_batch_pending_current
        > counters.protocol_column_batch_pending_opened
    {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-current-over-open",
            format!(
                "protocol column pending current={} but opened={}",
                counters.protocol_column_batch_pending_current,
                counters.protocol_column_batch_pending_opened
            ),
        );
    }
    if counters.protocol_column_batch_pending_current > counters.protocol_column_batch_pending_max {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-current-over-max",
            format!(
                "protocol column pending current={} but max={}",
                counters.protocol_column_batch_pending_current,
                counters.protocol_column_batch_pending_max
            ),
        );
    }
    if counters.protocol_column_batch_pending_rows_current
        > counters.protocol_column_batch_pending_rows_max
    {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-rows-current-over-max",
            format!(
                "protocol column pending rows current={} but max={}",
                counters.protocol_column_batch_pending_rows_current,
                counters.protocol_column_batch_pending_rows_max
            ),
        );
    }
    if counters.protocol_column_batch_pending_bytes_current
        > counters.protocol_column_batch_pending_bytes_max
    {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-bytes-current-over-max",
            format!(
                "protocol column pending bytes current={} but max={}",
                counters.protocol_column_batch_pending_bytes_current,
                counters.protocol_column_batch_pending_bytes_max
            ),
        );
    }
    if counters.protocol_column_batch_pending_current == 0
        && counters.protocol_column_batch_pending_rows_current > 0
    {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-rows-without-current",
            "protocol column pending retained rows are non-zero while current pending batches is zero",
        );
    }
    if counters.protocol_column_batch_pending_current == 0
        && counters.protocol_column_batch_pending_bytes_current > 0
    {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-bytes-without-current",
            "protocol column pending retained bytes are non-zero while current pending batches is zero",
        );
    }
    if counters.protocol_column_batch_pending_max == 0
        && counters.protocol_column_batch_pending_rows_max > 0
    {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-rows-max-without-batches",
            "protocol column pending retained row high-water is non-zero while pending batch high-water is zero",
        );
    }
    if counters.protocol_column_batch_pending_max == 0
        && counters.protocol_column_batch_pending_bytes_max > 0
    {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "protocol-column-batch-pending-bytes-max-without-batches",
            "protocol column pending retained byte high-water is non-zero while pending batch high-water is zero",
        );
    }
    if counters.join_pk_probe_hits > counters.join_pk_probe_keys {
        push_counter_diagnostic(
            &mut diagnostics,
            "error",
            "join-pk-hits-over-keys",
            "join_pk_probe_hits is greater than join_pk_probe_keys",
        );
    }
    if counters.artifact_singleflight_followers > 0 && counters.artifact_singleflight_leaders == 0 {
        push_counter_diagnostic(
            &mut diagnostics,
            "warn",
            "artifact-singleflight-followers-without-leaders",
            "artifact_singleflight_followers is non-zero while artifact_singleflight_leaders is zero in the same instrumentation epoch",
        );
    }

    diagnostics
}

fn push_counter_diagnostic(
    diagnostics: &mut Vec<EngineCounterDiagnostic>,
    level: &str,
    code: &str,
    message: impl Into<String>,
) {
    diagnostics.push(EngineCounterDiagnostic {
        level: level.to_string(),
        code: code.to_string(),
        message: message.into(),
    });
}

fn engine_counter_delta(
    after: &EngineCountersSnapshot,
    before: &EngineCountersSnapshot,
) -> EngineCountersSnapshot {
    EngineCountersSnapshot {
        runtime_profile: after.runtime_profile.delta(before.runtime_profile),
        volume_read_calls: after
            .volume_read_calls
            .saturating_sub(before.volume_read_calls),
        volume_read_bytes: after
            .volume_read_bytes
            .saturating_sub(before.volume_read_bytes),
        volume_read_nanos: after
            .volume_read_nanos
            .saturating_sub(before.volume_read_nanos),
        decompression_calls: after
            .decompression_calls
            .saturating_sub(before.decompression_calls),
        decompression_compressed_bytes: after
            .decompression_compressed_bytes
            .saturating_sub(before.decompression_compressed_bytes),
        decompression_raw_bytes: after
            .decompression_raw_bytes
            .saturating_sub(before.decompression_raw_bytes),
        decompression_nanos: after
            .decompression_nanos
            .saturating_sub(before.decompression_nanos),
        row_materialization_calls: after
            .row_materialization_calls
            .saturating_sub(before.row_materialization_calls),
        row_materialization_rows: after
            .row_materialization_rows
            .saturating_sub(before.row_materialization_rows),
        row_materialization_values: after
            .row_materialization_values
            .saturating_sub(before.row_materialization_values),
        row_materialization_nanos: after
            .row_materialization_nanos
            .saturating_sub(before.row_materialization_nanos),
        derived_subquery_executes: after
            .derived_subquery_executes
            .saturating_sub(before.derived_subquery_executes),
        artifact_descriptor_open_calls: after
            .artifact_descriptor_open_calls
            .saturating_sub(before.artifact_descriptor_open_calls),
        artifact_file_open_calls: after
            .artifact_file_open_calls
            .saturating_sub(before.artifact_file_open_calls),
        artifact_file_stat_calls: after
            .artifact_file_stat_calls
            .saturating_sub(before.artifact_file_stat_calls),
        artifact_file_identity_checks: after
            .artifact_file_identity_checks
            .saturating_sub(before.artifact_file_identity_checks),
        artifact_fadvise_calls: after
            .artifact_fadvise_calls
            .saturating_sub(before.artifact_fadvise_calls),
        artifact_fadvise_errors: after
            .artifact_fadvise_errors
            .saturating_sub(before.artifact_fadvise_errors),
        artifact_pread_calls: after.artifact_pread_calls.saturating_sub(before.artifact_pread_calls),
        artifact_pread_bytes: after.artifact_pread_bytes.saturating_sub(before.artifact_pread_bytes),
        artifact_pread_nanos: after.artifact_pread_nanos.saturating_sub(before.artifact_pread_nanos),
        artifact_singleflight_leaders: after
            .artifact_singleflight_leaders
            .saturating_sub(before.artifact_singleflight_leaders),
        artifact_singleflight_followers: after
            .artifact_singleflight_followers
            .saturating_sub(before.artifact_singleflight_followers),
        artifact_payload_decompress_calls: after
            .artifact_payload_decompress_calls
            .saturating_sub(before.artifact_payload_decompress_calls),
        artifact_payload_decompress_compressed_bytes: after
            .artifact_payload_decompress_compressed_bytes
            .saturating_sub(before.artifact_payload_decompress_compressed_bytes),
        artifact_payload_decompress_raw_bytes: after
            .artifact_payload_decompress_raw_bytes
            .saturating_sub(before.artifact_payload_decompress_raw_bytes),
        artifact_payload_decompress_nanos: after
            .artifact_payload_decompress_nanos
            .saturating_sub(before.artifact_payload_decompress_nanos),
        artifact_column_deserialize_calls: after
            .artifact_column_deserialize_calls
            .saturating_sub(before.artifact_column_deserialize_calls),
        artifact_column_deserialize_bytes: after
            .artifact_column_deserialize_bytes
            .saturating_sub(before.artifact_column_deserialize_bytes),
        artifact_column_deserialize_rows: after
            .artifact_column_deserialize_rows
            .saturating_sub(before.artifact_column_deserialize_rows),
        artifact_column_deserialize_nanos: after
            .artifact_column_deserialize_nanos
            .saturating_sub(before.artifact_column_deserialize_nanos),
        artifact_columnar_group_applies: after
            .artifact_columnar_group_applies
            .saturating_sub(before.artifact_columnar_group_applies),
        artifact_columnar_group_row_groups: after
            .artifact_columnar_group_row_groups
            .saturating_sub(before.artifact_columnar_group_row_groups),
        artifact_columnar_group_selected_blocks: after
            .artifact_columnar_group_selected_blocks
            .saturating_sub(before.artifact_columnar_group_selected_blocks),
        artifact_columnar_group_input_rows: after
            .artifact_columnar_group_input_rows
            .saturating_sub(before.artifact_columnar_group_input_rows),
        artifact_columnar_group_output_groups: after
            .artifact_columnar_group_output_groups
            .saturating_sub(before.artifact_columnar_group_output_groups),
        artifact_columnar_group_direct_accumulators: after
            .artifact_columnar_group_direct_accumulators
            .saturating_sub(before.artifact_columnar_group_direct_accumulators),
        artifact_columnar_group_hash_accumulators: after
            .artifact_columnar_group_hash_accumulators
            .saturating_sub(before.artifact_columnar_group_hash_accumulators),
        artifact_columnar_group_local_merges: after
            .artifact_columnar_group_local_merges
            .saturating_sub(before.artifact_columnar_group_local_merges),
        artifact_columnar_group_merged_groups: after
            .artifact_columnar_group_merged_groups
            .saturating_sub(before.artifact_columnar_group_merged_groups),
        artifact_columnar_group_scheduler_runs: after
            .artifact_columnar_group_scheduler_runs
            .saturating_sub(before.artifact_columnar_group_scheduler_runs),
        artifact_columnar_group_scheduled_segments: after
            .artifact_columnar_group_scheduled_segments
            .saturating_sub(before.artifact_columnar_group_scheduled_segments),
        artifact_columnar_group_fallbacks: after
            .artifact_columnar_group_fallbacks
            .saturating_sub(before.artifact_columnar_group_fallbacks),
        artifact_columnar_group_fallback_row_state: after
            .artifact_columnar_group_fallback_row_state
            .saturating_sub(before.artifact_columnar_group_fallback_row_state),
        artifact_columnar_group_fallback_no_cold_artifact: after
            .artifact_columnar_group_fallback_no_cold_artifact
            .saturating_sub(before.artifact_columnar_group_fallback_no_cold_artifact),
        artifact_columnar_group_fallback_schema: after
            .artifact_columnar_group_fallback_schema
            .saturating_sub(before.artifact_columnar_group_fallback_schema),
        artifact_columnar_group_fallback_group_key: after
            .artifact_columnar_group_fallback_group_key
            .saturating_sub(before.artifact_columnar_group_fallback_group_key),
        artifact_columnar_group_fallback_aggregate: after
            .artifact_columnar_group_fallback_aggregate
            .saturating_sub(before.artifact_columnar_group_fallback_aggregate),
        artifact_columnar_group_fallback_visibility: after
            .artifact_columnar_group_fallback_visibility
            .saturating_sub(before.artifact_columnar_group_fallback_visibility),
        artifact_columnar_group_fallback_storage: after
            .artifact_columnar_group_fallback_storage
            .saturating_sub(before.artifact_columnar_group_fallback_storage),
        artifact_columnar_group_fallback_column_shape: after
            .artifact_columnar_group_fallback_column_shape
            .saturating_sub(before.artifact_columnar_group_fallback_column_shape),
        artifact_columnar_group_fallback_accumulator: after
            .artifact_columnar_group_fallback_accumulator
            .saturating_sub(before.artifact_columnar_group_fallback_accumulator),
        metadata_pk_count_attempts: after
            .metadata_pk_count_attempts
            .saturating_sub(before.metadata_pk_count_attempts),
        metadata_pk_count_applied: after
            .metadata_pk_count_applied
            .saturating_sub(before.metadata_pk_count_applied),
        metadata_pk_count_intervals: after
            .metadata_pk_count_intervals
            .saturating_sub(before.metadata_pk_count_intervals),
        metadata_pk_count_candidate_rows: after
            .metadata_pk_count_candidate_rows
            .saturating_sub(before.metadata_pk_count_candidate_rows),
        metadata_pk_count_visible_rows: after
            .metadata_pk_count_visible_rows
            .saturating_sub(before.metadata_pk_count_visible_rows),
        metadata_pk_count_visibility_exclusions: after
            .metadata_pk_count_visibility_exclusions
            .saturating_sub(before.metadata_pk_count_visibility_exclusions),
        metadata_pk_count_hot_candidates: after
            .metadata_pk_count_hot_candidates
            .saturating_sub(before.metadata_pk_count_hot_candidates),
        metadata_pk_count_fallbacks: after
            .metadata_pk_count_fallbacks
            .saturating_sub(before.metadata_pk_count_fallbacks),
        metadata_pk_count_fallback_snapshot: after
            .metadata_pk_count_fallback_snapshot
            .saturating_sub(before.metadata_pk_count_fallback_snapshot),
        metadata_pk_count_fallback_seal_overlap: after
            .metadata_pk_count_fallback_seal_overlap
            .saturating_sub(before.metadata_pk_count_fallback_seal_overlap),
        metadata_pk_count_fallback_unsupported: after
            .metadata_pk_count_fallback_unsupported
            .saturating_sub(before.metadata_pk_count_fallback_unsupported),
        metadata_pk_count_fallback_candidate_limit: after
            .metadata_pk_count_fallback_candidate_limit
            .saturating_sub(before.metadata_pk_count_fallback_candidate_limit),
        join_outer_rows: after.join_outer_rows.saturating_sub(before.join_outer_rows),
        join_key_rows: after.join_key_rows.saturating_sub(before.join_key_rows),
        join_pk_probe_batches: after
            .join_pk_probe_batches
            .saturating_sub(before.join_pk_probe_batches),
        join_pk_probe_keys: after
            .join_pk_probe_keys
            .saturating_sub(before.join_pk_probe_keys),
        join_pk_probe_hits: after
            .join_pk_probe_hits
            .saturating_sub(before.join_pk_probe_hits),
        join_parent_payload_rows: after
            .join_parent_payload_rows
            .saturating_sub(before.join_parent_payload_rows),
        join_rows_constructed: after
            .join_rows_constructed
            .saturating_sub(before.join_rows_constructed),
        join_rows_deferred: after
            .join_rows_deferred
            .saturating_sub(before.join_rows_deferred),
        join_deferred_rows_consumed: after
            .join_deferred_rows_consumed
            .saturating_sub(before.join_deferred_rows_consumed),
        join_operator_calls: after
            .join_operator_calls
            .saturating_sub(before.join_operator_calls),
        join_hash_streaming_calls: after
            .join_hash_streaming_calls
            .saturating_sub(before.join_hash_streaming_calls),
        join_hash_parallel_calls: after
            .join_hash_parallel_calls
            .saturating_sub(before.join_hash_parallel_calls),
        join_merge_calls: after
            .join_merge_calls
            .saturating_sub(before.join_merge_calls),
        join_nested_loop_calls: after
            .join_nested_loop_calls
            .saturating_sub(before.join_nested_loop_calls),
        join_index_nested_loop_calls: after
            .join_index_nested_loop_calls
            .saturating_sub(before.join_index_nested_loop_calls),
        join_batch_index_nested_loop_calls: after
            .join_batch_index_nested_loop_calls
            .saturating_sub(before.join_batch_index_nested_loop_calls),
        join_left_input_rows: after
            .join_left_input_rows
            .saturating_sub(before.join_left_input_rows),
        join_right_input_rows: after
            .join_right_input_rows
            .saturating_sub(before.join_right_input_rows),
        join_output_rows: after
            .join_output_rows
            .saturating_sub(before.join_output_rows),
        join_input_values: after
            .join_input_values
            .saturating_sub(before.join_input_values),
        join_output_values: after
            .join_output_values
            .saturating_sub(before.join_output_values),
        join_candidate_pairs: after
            .join_candidate_pairs
            .saturating_sub(before.join_candidate_pairs),
        join_lookup_calls: after
            .join_lookup_calls
            .saturating_sub(before.join_lookup_calls),
        join_lookup_candidate_rows: after
            .join_lookup_candidate_rows
            .saturating_sub(before.join_lookup_candidate_rows),
        join_wall_nanos: after.join_wall_nanos.saturating_sub(before.join_wall_nanos),
        join_max_left_input_rows: after.join_max_left_input_rows,
        join_max_right_input_rows: after.join_max_right_input_rows,
        join_max_output_rows: after.join_max_output_rows,
        join_max_output_width: after.join_max_output_width,
        navigation_paths_planned: after
            .navigation_paths_planned
            .saturating_sub(before.navigation_paths_planned),
        navigation_paths_executed: after
            .navigation_paths_executed
            .saturating_sub(before.navigation_paths_executed),
        navigation_source_rows: after
            .navigation_source_rows
            .saturating_sub(before.navigation_source_rows),
        navigation_distinct_source_keys: after
            .navigation_distinct_source_keys
            .saturating_sub(before.navigation_distinct_source_keys),
        navigation_repeated_keys_eliminated: after
            .navigation_repeated_keys_eliminated
            .saturating_sub(before.navigation_repeated_keys_eliminated),
        navigation_lookup_batches: after
            .navigation_lookup_batches
            .saturating_sub(before.navigation_lookup_batches),
        navigation_lookup_hits: after
            .navigation_lookup_hits
            .saturating_sub(before.navigation_lookup_hits),
        navigation_lookup_misses: after
            .navigation_lookup_misses
            .saturating_sub(before.navigation_lookup_misses),
        navigation_direct_edges: after
            .navigation_direct_edges
            .saturating_sub(before.navigation_direct_edges),
        navigation_index_nested_loop_edges: after
            .navigation_index_nested_loop_edges
            .saturating_sub(before.navigation_index_nested_loop_edges),
        navigation_batch_edges: after
            .navigation_batch_edges
            .saturating_sub(before.navigation_batch_edges),
        navigation_hash_edges: after
            .navigation_hash_edges
            .saturating_sub(before.navigation_hash_edges),
        navigation_merge_edges: after
            .navigation_merge_edges
            .saturating_sub(before.navigation_merge_edges),
        navigation_fallback_edges: after
            .navigation_fallback_edges
            .saturating_sub(before.navigation_fallback_edges),
        navigation_planner_left_join_edges: after
            .navigation_planner_left_join_edges
            .saturating_sub(before.navigation_planner_left_join_edges),
        navigation_integrity_failures: after
            .navigation_integrity_failures
            .saturating_sub(before.navigation_integrity_failures),
        navigation_cancellations: after
            .navigation_cancellations
            .saturating_sub(before.navigation_cancellations),
        navigation_timeouts: after
            .navigation_timeouts
            .saturating_sub(before.navigation_timeouts),
        protocol_result_rows: after
            .protocol_result_rows
            .saturating_sub(before.protocol_result_rows),
        protocol_row_to_wire_values: after
            .protocol_row_to_wire_values
            .saturating_sub(before.protocol_row_to_wire_values),
        protocol_row_size_probe_calls: after
            .protocol_row_size_probe_calls
            .saturating_sub(before.protocol_row_size_probe_calls),
        protocol_row_size_probe_bytes: after
            .protocol_row_size_probe_bytes
            .saturating_sub(before.protocol_row_size_probe_bytes),
        protocol_row_batch_frames: after
            .protocol_row_batch_frames
            .saturating_sub(before.protocol_row_batch_frames),
        protocol_row_batch_probe_bytes: after
            .protocol_row_batch_probe_bytes
            .saturating_sub(before.protocol_row_batch_probe_bytes),
        protocol_column_batch_fallbacks: after
            .protocol_column_batch_fallbacks
            .saturating_sub(before.protocol_column_batch_fallbacks),
        protocol_column_batch_fallback_row_state: after
            .protocol_column_batch_fallback_row_state
            .saturating_sub(before.protocol_column_batch_fallback_row_state),
        protocol_column_batch_fallback_query_shape: after
            .protocol_column_batch_fallback_query_shape
            .saturating_sub(before.protocol_column_batch_fallback_query_shape),
        protocol_column_batch_fallback_storage_shape: after
            .protocol_column_batch_fallback_storage_shape
            .saturating_sub(before.protocol_column_batch_fallback_storage_shape),
        protocol_column_batch_fallback_schema: after
            .protocol_column_batch_fallback_schema
            .saturating_sub(before.protocol_column_batch_fallback_schema),
        protocol_column_batch_fallback_unknown: after
            .protocol_column_batch_fallback_unknown
            .saturating_sub(before.protocol_column_batch_fallback_unknown),
        protocol_column_batch_pending_opened: after
            .protocol_column_batch_pending_opened
            .saturating_sub(before.protocol_column_batch_pending_opened),
        protocol_column_batch_pending_completed: after
            .protocol_column_batch_pending_completed
            .saturating_sub(before.protocol_column_batch_pending_completed),
        protocol_column_batch_pending_dropped: after
            .protocol_column_batch_pending_dropped
            .saturating_sub(before.protocol_column_batch_pending_dropped),
        protocol_column_batch_pending_current: after.protocol_column_batch_pending_current,
        protocol_column_batch_pending_max: after.protocol_column_batch_pending_max,
        protocol_column_batch_pending_rows_current: after
            .protocol_column_batch_pending_rows_current,
        protocol_column_batch_pending_rows_max: after.protocol_column_batch_pending_rows_max,
        protocol_column_batch_pending_bytes_current: after
            .protocol_column_batch_pending_bytes_current,
        protocol_column_batch_pending_bytes_max: after.protocol_column_batch_pending_bytes_max,
        protocol_encode_calls: after
            .protocol_encode_calls
            .saturating_sub(before.protocol_encode_calls),
        protocol_encode_bytes: after
            .protocol_encode_bytes
            .saturating_sub(before.protocol_encode_bytes),
        protocol_encode_nanos: after
            .protocol_encode_nanos
            .saturating_sub(before.protocol_encode_nanos),
        protocol_socket_write_calls: after
            .protocol_socket_write_calls
            .saturating_sub(before.protocol_socket_write_calls),
        protocol_socket_write_bytes: after
            .protocol_socket_write_bytes
            .saturating_sub(before.protocol_socket_write_bytes),
        protocol_socket_write_nanos: after
            .protocol_socket_write_nanos
            .saturating_sub(before.protocol_socket_write_nanos),
        ram_accelerator_builds: after
            .ram_accelerator_builds
            .saturating_sub(before.ram_accelerator_builds),
        ram_accelerator_build_entries: after
            .ram_accelerator_build_entries
            .saturating_sub(before.ram_accelerator_build_entries),
        ram_accelerator_build_bytes: after
            .ram_accelerator_build_bytes
            .saturating_sub(before.ram_accelerator_build_bytes),
        ram_accelerator_build_nanos: after
            .ram_accelerator_build_nanos
            .saturating_sub(before.ram_accelerator_build_nanos),
        ram_accelerator_hits: after
            .ram_accelerator_hits
            .saturating_sub(before.ram_accelerator_hits),
        ram_accelerator_misses: after
            .ram_accelerator_misses
            .saturating_sub(before.ram_accelerator_misses),
        ram_accelerator_fallbacks: after
            .ram_accelerator_fallbacks
            .saturating_sub(before.ram_accelerator_fallbacks),
        ram_accelerator_evictions: after
            .ram_accelerator_evictions
            .saturating_sub(before.ram_accelerator_evictions),
        ram_accelerator_eviction_bytes: after
            .ram_accelerator_eviction_bytes
            .saturating_sub(before.ram_accelerator_eviction_bytes),
        wal_append_entries: after
            .wal_append_entries
            .saturating_sub(before.wal_append_entries),
        wal_append_bytes: after
            .wal_append_bytes
            .saturating_sub(before.wal_append_bytes),
        wal_write_calls: after.wal_write_calls.saturating_sub(before.wal_write_calls),
        wal_write_bytes: after.wal_write_bytes.saturating_sub(before.wal_write_bytes),
        wal_write_nanos: after.wal_write_nanos.saturating_sub(before.wal_write_nanos),
        wal_sync_calls: after.wal_sync_calls.saturating_sub(before.wal_sync_calls),
        wal_sync_nanos: after.wal_sync_nanos.saturating_sub(before.wal_sync_nanos),
        wal_generation_validation_calls: after
            .wal_generation_validation_calls
            .saturating_sub(before.wal_generation_validation_calls),
        wal_generation_validation_bytes: after
            .wal_generation_validation_bytes
            .saturating_sub(before.wal_generation_validation_bytes),
        wal_generation_validation_nanos: after
            .wal_generation_validation_nanos
            .saturating_sub(before.wal_generation_validation_nanos),
        wal_retention_calls: after
            .wal_retention_calls
            .saturating_sub(before.wal_retention_calls),
        wal_retention_identity_checks: after
            .wal_retention_identity_checks
            .saturating_sub(before.wal_retention_identity_checks),
        wal_retention_files_deleted: after
            .wal_retention_files_deleted
            .saturating_sub(before.wal_retention_files_deleted),
        wal_retention_nanos: after
            .wal_retention_nanos
            .saturating_sub(before.wal_retention_nanos),
        copy_calls: after.copy_calls.saturating_sub(before.copy_calls),
        copy_rows: after.copy_rows.saturating_sub(before.copy_rows),
        copy_parse_nanos: after
            .copy_parse_nanos
            .saturating_sub(before.copy_parse_nanos),
        copy_commit_nanos: after
            .copy_commit_nanos
            .saturating_sub(before.copy_commit_nanos),
        copy_total_nanos: after
            .copy_total_nanos
            .saturating_sub(before.copy_total_nanos),
        cold_constraint_batch_calls: after
            .cold_constraint_batch_calls
            .saturating_sub(before.cold_constraint_batch_calls),
        cold_constraint_batch_rows: after
            .cold_constraint_batch_rows
            .saturating_sub(before.cold_constraint_batch_rows),
        cold_constraint_batch_segments: after
            .cold_constraint_batch_segments
            .saturating_sub(before.cold_constraint_batch_segments),
        cold_constraint_batch_nanos: after
            .cold_constraint_batch_nanos
            .saturating_sub(before.cold_constraint_batch_nanos),
        cold_pk_batch_nanos: after
            .cold_pk_batch_nanos
            .saturating_sub(before.cold_pk_batch_nanos),
        compaction_spool_write_calls: after
            .compaction_spool_write_calls
            .saturating_sub(before.compaction_spool_write_calls),
        compaction_spool_write_bytes: after
            .compaction_spool_write_bytes
            .saturating_sub(before.compaction_spool_write_bytes),
        compaction_spool_write_nanos: after
            .compaction_spool_write_nanos
            .saturating_sub(before.compaction_spool_write_nanos),
        compaction_spool_read_calls: after
            .compaction_spool_read_calls
            .saturating_sub(before.compaction_spool_read_calls),
        compaction_spool_read_bytes: after
            .compaction_spool_read_bytes
            .saturating_sub(before.compaction_spool_read_bytes),
        compaction_spool_read_nanos: after
            .compaction_spool_read_nanos
            .saturating_sub(before.compaction_spool_read_nanos),
        posting_exact_lookup_calls: after
            .posting_exact_lookup_calls
            .saturating_sub(before.posting_exact_lookup_calls),
        posting_exact_lookup_segments: after
            .posting_exact_lookup_segments
            .saturating_sub(before.posting_exact_lookup_segments),
        posting_exact_lookup_max_segments: after.posting_exact_lookup_max_segments,
        posting_ordered_lookup_calls: after
            .posting_ordered_lookup_calls
            .saturating_sub(before.posting_ordered_lookup_calls),
        posting_ordered_lookup_segments: after
            .posting_ordered_lookup_segments
            .saturating_sub(before.posting_ordered_lookup_segments),
        posting_ordered_lookup_max_segments: after.posting_ordered_lookup_max_segments,
        seal_calls: after.seal_calls.saturating_sub(before.seal_calls),
        seal_rows: after.seal_rows.saturating_sub(before.seal_rows),
        seal_bytes: after.seal_bytes.saturating_sub(before.seal_bytes),
        seal_output_bytes: after
            .seal_output_bytes
            .saturating_sub(before.seal_output_bytes),
        seal_nanos: after.seal_nanos.saturating_sub(before.seal_nanos),
        compaction_calls: after
            .compaction_calls
            .saturating_sub(before.compaction_calls),
        compaction_tables: after
            .compaction_tables
            .saturating_sub(before.compaction_tables),
        compaction_nanos: after
            .compaction_nanos
            .saturating_sub(before.compaction_nanos),
    }
}

fn client_protocol_counter_delta(
    after: &ClientProtocolCountersSnapshot,
    before: &ClientProtocolCountersSnapshot,
) -> ClientProtocolCountersSnapshot {
    ClientProtocolCountersSnapshot {
        frame_decode_calls: after
            .frame_decode_calls
            .saturating_sub(before.frame_decode_calls),
        frame_decode_bytes: after
            .frame_decode_bytes
            .saturating_sub(before.frame_decode_bytes),
        frame_decode_nanos: after
            .frame_decode_nanos
            .saturating_sub(before.frame_decode_nanos),
    }
}

#[derive(Debug, Clone, Serialize)]
struct StorageMetric {
    participant: String,
    phase: String,
    path: String,
    elapsed_ms: f64,
    samples: u64,
    final_logical_bytes: u64,
    peak_logical_bytes: u64,
    final_allocated_bytes: u64,
    peak_allocated_bytes: u64,
    final_files: u64,
    final_dirs: u64,
    sample_errors: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct StorageUsage {
    logical_bytes: u64,
    allocated_bytes: u64,
    files: u64,
    dirs: u64,
}

struct StorageSampler {
    stop: mpsc::Sender<()>,
    handle: JoinHandle<StorageMetric>,
}

#[derive(Debug, Clone, Serialize)]
struct ResourceMetric {
    participant: String,
    phase: String,
    process_scope: String,
    root_pid: Option<i32>,
    device: Option<String>,
    elapsed_ms: f64,
    samples: u64,
    sample_errors: u64,
    process_missing_samples: u64,
    final_rss_bytes: u64,
    peak_rss_bytes: u64,
    peak_vsize_bytes: u64,
    peak_threads: u64,
    peak_open_fds: u64,
    peak_socket_fds: u64,
    final_open_fds: u64,
    final_socket_fds: u64,
    cpu_user_seconds: f64,
    cpu_system_seconds: f64,
    cpu_total_percent: f64,
    minor_faults_delta: u64,
    major_faults_delta: u64,
    voluntary_context_switches_delta: u64,
    involuntary_context_switches_delta: u64,
    process_read_bytes_delta: u64,
    process_write_bytes_delta: u64,
    process_read_bytes_per_sec: f64,
    process_write_bytes_per_sec: f64,
    process_rchar_delta: u64,
    process_wchar_delta: u64,
    process_read_syscalls_delta: u64,
    process_write_syscalls_delta: u64,
    process_cancelled_write_bytes_delta: u64,
    device_read_bytes_delta: u64,
    device_write_bytes_delta: u64,
    device_read_bytes_per_sec: f64,
    device_write_bytes_per_sec: f64,
    device_reads_delta: u64,
    device_writes_delta: u64,
    device_busy_ms_delta: u64,
    device_weighted_io_ms_delta: u64,
    peak_device_in_progress: u64,
    notes: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
enum ProcessScope {
    Single(i32),
    Tree(i32),
}

impl ProcessScope {
    fn root_pid(self) -> i32 {
        match self {
            Self::Single(pid) | Self::Tree(pid) => pid,
        }
    }

    fn label(self) -> String {
        match self {
            Self::Single(pid) => format!("single:{pid}"),
            Self::Tree(pid) => format!("tree:{pid}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ProcessAggregate {
    rss_bytes: u64,
    vsize_bytes: u64,
    user_ticks: u64,
    system_ticks: u64,
    minor_faults: u64,
    major_faults: u64,
    threads: u64,
    open_fds: u64,
    socket_fds: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
    read_bytes: u64,
    write_bytes: u64,
    rchar: u64,
    wchar: u64,
    read_syscalls: u64,
    write_syscalls: u64,
    cancelled_write_bytes: u64,
}

#[derive(Debug, Clone)]
struct ProcessSample {
    pid: i32,
    ppid: i32,
    values: ProcessAggregate,
}

#[derive(Debug, Clone, Copy, Default)]
struct DeviceAggregate {
    reads: u64,
    sectors_read: u64,
    writes: u64,
    sectors_written: u64,
    in_progress: u64,
    time_in_progress: u64,
    weighted_time_in_progress: u64,
}

struct ResourceSampler {
    stop: mpsc::Sender<()>,
    handle: JoinHandle<ResourceMetric>,
}

impl ResourceSampler {
    fn start(
        participant: Participant,
        phase: impl Into<String>,
        process_scope: ProcessScope,
        device_probe_path: &Path,
    ) -> Self {
        let (stop, rx) = mpsc::channel();
        let participant_name = participant.as_str().to_string();
        let phase = phase.into();
        let process_label = process_scope.label();
        let root_pid = process_scope.root_pid();
        let device = resolve_block_device(device_probe_path);
        let notes = resource_notes(participant, process_scope, device.as_deref());
        let handle = thread::spawn(move || {
            let started = Instant::now();
            let ticks_per_second = procfs::ticks_per_second() as f64;
            let page_size = procfs::page_size();

            let mut samples = 0_u64;
            let mut errors = 0_u64;
            let mut missing = 0_u64;
            let mut first_process = None;
            let mut last_process = None;
            let mut first_device = None;
            let mut last_device = None;
            let mut peak_rss = 0_u64;
            let mut peak_vsize = 0_u64;
            let mut peak_threads = 0_u64;
            let mut peak_open_fds = 0_u64;
            let mut peak_socket_fds = 0_u64;
            let mut peak_in_progress = 0_u64;

            loop {
                match sample_resources(process_scope, page_size, device.as_deref()) {
                    Ok(sample) => {
                        samples += 1;
                        if let Some(process) = sample.process {
                            if first_process.is_none() {
                                first_process = Some(process);
                            }
                            peak_rss = peak_rss.max(process.rss_bytes);
                            peak_vsize = peak_vsize.max(process.vsize_bytes);
                            peak_threads = peak_threads.max(process.threads);
                            peak_open_fds = peak_open_fds.max(process.open_fds);
                            peak_socket_fds = peak_socket_fds.max(process.socket_fds);
                            last_process = Some(process);
                        } else {
                            missing += 1;
                        }

                        if let Some(device_sample) = sample.device {
                            if first_device.is_none() {
                                first_device = Some(device_sample);
                            }
                            peak_in_progress = peak_in_progress.max(device_sample.in_progress);
                            last_device = Some(device_sample);
                        }
                    }
                    Err(_) => errors += 1,
                }

                match rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }

            if let Ok(sample) = sample_resources(process_scope, page_size, device.as_deref()) {
                samples += 1;
                if let Some(process) = sample.process {
                    if first_process.is_none() {
                        first_process = Some(process);
                    }
                    peak_rss = peak_rss.max(process.rss_bytes);
                    peak_vsize = peak_vsize.max(process.vsize_bytes);
                    peak_threads = peak_threads.max(process.threads);
                    peak_open_fds = peak_open_fds.max(process.open_fds);
                    peak_socket_fds = peak_socket_fds.max(process.socket_fds);
                    last_process = Some(process);
                } else {
                    missing += 1;
                }
                if let Some(device_sample) = sample.device {
                    if first_device.is_none() {
                        first_device = Some(device_sample);
                    }
                    peak_in_progress = peak_in_progress.max(device_sample.in_progress);
                    last_device = Some(device_sample);
                }
            } else {
                errors += 1;
            }

            let elapsed = started.elapsed();
            let process_delta = first_process
                .zip(last_process)
                .map(|(first, last)| subtract_process(last, first))
                .unwrap_or_default();
            let device_delta = first_device
                .zip(last_device)
                .map(|(first, last)| subtract_device(last, first))
                .unwrap_or_default();
            let final_process = last_process.unwrap_or_default();
            let cpu_user_seconds = process_delta.user_ticks as f64 / ticks_per_second;
            let cpu_system_seconds = process_delta.system_ticks as f64 / ticks_per_second;
            let elapsed_seconds = elapsed.as_secs_f64();
            let cpu_total_percent = if elapsed_seconds > 0.0 {
                (cpu_user_seconds + cpu_system_seconds) * 100.0 / elapsed_seconds
            } else {
                0.0
            };
            let process_read_bytes_delta = process_delta.read_bytes;
            let process_write_bytes_delta = process_delta.write_bytes;
            let device_read_bytes_delta = device_delta.sectors_read.saturating_mul(512);
            let device_write_bytes_delta = device_delta.sectors_written.saturating_mul(512);
            let bytes_per_second = |bytes: u64| {
                if elapsed_seconds > 0.0 {
                    bytes as f64 / elapsed_seconds
                } else {
                    0.0
                }
            };

            ResourceMetric {
                participant: participant_name,
                phase,
                process_scope: process_label,
                root_pid: Some(root_pid),
                device,
                elapsed_ms: elapsed_seconds * 1000.0,
                samples,
                sample_errors: errors,
                process_missing_samples: missing,
                final_rss_bytes: final_process.rss_bytes,
                peak_rss_bytes: peak_rss,
                peak_vsize_bytes: peak_vsize,
                peak_threads,
                peak_open_fds,
                peak_socket_fds,
                final_open_fds: final_process.open_fds,
                final_socket_fds: final_process.socket_fds,
                cpu_user_seconds,
                cpu_system_seconds,
                cpu_total_percent,
                minor_faults_delta: process_delta.minor_faults,
                major_faults_delta: process_delta.major_faults,
                voluntary_context_switches_delta: process_delta.voluntary_context_switches,
                involuntary_context_switches_delta: process_delta.involuntary_context_switches,
                process_read_bytes_delta,
                process_write_bytes_delta,
                process_read_bytes_per_sec: bytes_per_second(process_read_bytes_delta),
                process_write_bytes_per_sec: bytes_per_second(process_write_bytes_delta),
                process_rchar_delta: process_delta.rchar,
                process_wchar_delta: process_delta.wchar,
                process_read_syscalls_delta: process_delta.read_syscalls,
                process_write_syscalls_delta: process_delta.write_syscalls,
                process_cancelled_write_bytes_delta: process_delta.cancelled_write_bytes,
                device_read_bytes_delta,
                device_write_bytes_delta,
                device_read_bytes_per_sec: bytes_per_second(device_read_bytes_delta),
                device_write_bytes_per_sec: bytes_per_second(device_write_bytes_delta),
                device_reads_delta: device_delta.reads,
                device_writes_delta: device_delta.writes,
                device_busy_ms_delta: device_delta.time_in_progress,
                device_weighted_io_ms_delta: device_delta.weighted_time_in_progress,
                peak_device_in_progress: peak_in_progress,
                notes,
            }
        });
        Self { stop, handle }
    }

    fn stop(self) -> BenchResult<ResourceMetric> {
        let _ = self.stop.send(());
        self.handle
            .join()
            .map_err(|_| "resource sampler thread panicked".into())
    }
}

struct ResourceSample {
    process: Option<ProcessAggregate>,
    device: Option<DeviceAggregate>,
}

impl StorageSampler {
    fn start(participant: Participant, phase: impl Into<String>, path: PathBuf) -> Self {
        let (stop, rx) = mpsc::channel();
        let participant_name = participant.as_str().to_string();
        let phase = phase.into();
        let path_for_thread = path.clone();
        let path_label = path.display().to_string();
        let handle = thread::spawn(move || {
            let started = Instant::now();
            let mut samples = 0_u64;
            let mut errors = 0_u64;
            let mut peak_logical = 0_u64;
            let mut peak_allocated = 0_u64;
            let mut final_usage = StorageUsage::default();

            loop {
                match storage_usage(&path_for_thread) {
                    Ok(usage) => {
                        samples += 1;
                        peak_logical = peak_logical.max(usage.logical_bytes);
                        peak_allocated = peak_allocated.max(usage.allocated_bytes);
                        final_usage = usage;
                    }
                    Err(_) => errors += 1,
                }

                match rx.recv_timeout(Duration::from_secs(2)) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }

            match storage_usage(&path_for_thread) {
                Ok(usage) => {
                    samples += 1;
                    peak_logical = peak_logical.max(usage.logical_bytes);
                    peak_allocated = peak_allocated.max(usage.allocated_bytes);
                    final_usage = usage;
                }
                Err(_) => errors += 1,
            }

            StorageMetric {
                participant: participant_name,
                phase,
                path: path_label,
                elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
                samples,
                final_logical_bytes: final_usage.logical_bytes,
                peak_logical_bytes: peak_logical,
                final_allocated_bytes: final_usage.allocated_bytes,
                peak_allocated_bytes: peak_allocated,
                final_files: final_usage.files,
                final_dirs: final_usage.dirs,
                sample_errors: errors,
            }
        });
        Self { stop, handle }
    }

    fn stop(self) -> BenchResult<StorageMetric> {
        let _ = self.stop.send(());
        self.handle
            .join()
            .map_err(|_| "storage sampler thread panicked".into())
    }
}

fn storage_usage(path: &Path) -> BenchResult<StorageUsage> {
    let mut total = StorageUsage::default();
    if !path.exists() {
        return Ok(total);
    }

    let mut stack = vec![path.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = fs::symlink_metadata(&path)?;
        total.logical_bytes = total.logical_bytes.saturating_add(metadata.len());
        total.allocated_bytes = total
            .allocated_bytes
            .saturating_add(metadata.blocks().saturating_mul(512));

        if metadata.is_dir() {
            total.dirs += 1;
            for entry in fs::read_dir(&path)? {
                stack.push(entry?.path());
            }
        } else if metadata.is_file() {
            total.files += 1;
        }
    }
    Ok(total)
}

fn sample_resources(
    process_scope: ProcessScope,
    page_size: u64,
    device: Option<&str>,
) -> BenchResult<ResourceSample> {
    Ok(ResourceSample {
        process: sample_process_scope(process_scope, page_size)?,
        device: device.and_then(sample_device),
    })
}

fn sample_process_scope(
    process_scope: ProcessScope,
    page_size: u64,
) -> BenchResult<Option<ProcessAggregate>> {
    match process_scope {
        ProcessScope::Single(pid) => {
            let process = procfs::process::Process::new(pid)?;
            Ok(Some(process_sample(&process, page_size)?.values))
        }
        ProcessScope::Tree(root_pid) => sample_process_tree(root_pid, page_size),
    }
}

fn sample_process_tree(root_pid: i32, page_size: u64) -> BenchResult<Option<ProcessAggregate>> {
    let mut samples = Vec::new();
    for process in procfs::process::all_processes()? {
        let Ok(process) = process else {
            continue;
        };
        if let Ok(sample) = process_sample(&process, page_size) {
            samples.push(sample);
        }
    }

    let parents: HashMap<i32, i32> = samples
        .iter()
        .map(|sample| (sample.pid, sample.ppid))
        .collect();
    let included = descendant_pids(root_pid, &parents);
    if included.is_empty() {
        return Ok(None);
    }

    let mut aggregate = ProcessAggregate::default();
    for sample in samples {
        if included.contains(&sample.pid) {
            add_process(&mut aggregate, sample.values);
        }
    }
    Ok(Some(aggregate))
}

fn process_sample(
    process: &procfs::process::Process,
    page_size: u64,
) -> BenchResult<ProcessSample> {
    let stat = process.stat()?;
    let status = process.status().ok();
    let io = process.io().ok();
    let mut open_fds = 0_u64;
    let mut socket_fds = 0_u64;
    if let Ok(fds) = process.fd() {
        for fd in fds.flatten() {
            open_fds = open_fds.saturating_add(1);
            if matches!(fd.target, procfs::process::FDTarget::Socket(_)) {
                socket_fds = socket_fds.saturating_add(1);
            }
        }
    }
    Ok(ProcessSample {
        pid: stat.pid,
        ppid: stat.ppid,
        values: ProcessAggregate {
            rss_bytes: stat.rss.saturating_mul(page_size),
            vsize_bytes: stat.vsize,
            user_ticks: stat.utime,
            system_ticks: stat.stime,
            minor_faults: stat.minflt,
            major_faults: stat.majflt,
            threads: stat.num_threads.max(0) as u64,
            open_fds,
            socket_fds,
            voluntary_context_switches: status
                .as_ref()
                .and_then(|status| status.voluntary_ctxt_switches)
                .unwrap_or(0),
            involuntary_context_switches: status
                .as_ref()
                .and_then(|status| status.nonvoluntary_ctxt_switches)
                .unwrap_or(0),
            read_bytes: io.as_ref().map(|io| io.read_bytes).unwrap_or(0),
            write_bytes: io.as_ref().map(|io| io.write_bytes).unwrap_or(0),
            rchar: io.as_ref().map(|io| io.rchar).unwrap_or(0),
            wchar: io.as_ref().map(|io| io.wchar).unwrap_or(0),
            read_syscalls: io.as_ref().map(|io| io.syscr).unwrap_or(0),
            write_syscalls: io.as_ref().map(|io| io.syscw).unwrap_or(0),
            cancelled_write_bytes: io.as_ref().map(|io| io.cancelled_write_bytes).unwrap_or(0),
        },
    })
}

fn descendant_pids(root_pid: i32, parents: &HashMap<i32, i32>) -> HashSet<i32> {
    let mut included = HashSet::new();
    if !parents.contains_key(&root_pid) {
        return included;
    }
    included.insert(root_pid);

    let mut changed = true;
    while changed {
        changed = false;
        for (&pid, &ppid) in parents {
            if !included.contains(&pid) && included.contains(&ppid) {
                included.insert(pid);
                changed = true;
            }
        }
    }
    included
}

fn add_process(target: &mut ProcessAggregate, value: ProcessAggregate) {
    target.rss_bytes = target.rss_bytes.saturating_add(value.rss_bytes);
    target.vsize_bytes = target.vsize_bytes.saturating_add(value.vsize_bytes);
    target.user_ticks = target.user_ticks.saturating_add(value.user_ticks);
    target.system_ticks = target.system_ticks.saturating_add(value.system_ticks);
    target.minor_faults = target.minor_faults.saturating_add(value.minor_faults);
    target.major_faults = target.major_faults.saturating_add(value.major_faults);
    target.threads = target.threads.saturating_add(value.threads);
    target.open_fds = target.open_fds.saturating_add(value.open_fds);
    target.socket_fds = target.socket_fds.saturating_add(value.socket_fds);
    target.voluntary_context_switches = target
        .voluntary_context_switches
        .saturating_add(value.voluntary_context_switches);
    target.involuntary_context_switches = target
        .involuntary_context_switches
        .saturating_add(value.involuntary_context_switches);
    target.read_bytes = target.read_bytes.saturating_add(value.read_bytes);
    target.write_bytes = target.write_bytes.saturating_add(value.write_bytes);
    target.rchar = target.rchar.saturating_add(value.rchar);
    target.wchar = target.wchar.saturating_add(value.wchar);
    target.read_syscalls = target.read_syscalls.saturating_add(value.read_syscalls);
    target.write_syscalls = target.write_syscalls.saturating_add(value.write_syscalls);
    target.cancelled_write_bytes = target
        .cancelled_write_bytes
        .saturating_add(value.cancelled_write_bytes);
}

fn subtract_process(last: ProcessAggregate, first: ProcessAggregate) -> ProcessAggregate {
    ProcessAggregate {
        rss_bytes: last.rss_bytes,
        vsize_bytes: last.vsize_bytes,
        user_ticks: last.user_ticks.saturating_sub(first.user_ticks),
        system_ticks: last.system_ticks.saturating_sub(first.system_ticks),
        minor_faults: last.minor_faults.saturating_sub(first.minor_faults),
        major_faults: last.major_faults.saturating_sub(first.major_faults),
        threads: last.threads,
        open_fds: last.open_fds,
        socket_fds: last.socket_fds,
        voluntary_context_switches: last
            .voluntary_context_switches
            .saturating_sub(first.voluntary_context_switches),
        involuntary_context_switches: last
            .involuntary_context_switches
            .saturating_sub(first.involuntary_context_switches),
        read_bytes: last.read_bytes.saturating_sub(first.read_bytes),
        write_bytes: last.write_bytes.saturating_sub(first.write_bytes),
        rchar: last.rchar.saturating_sub(first.rchar),
        wchar: last.wchar.saturating_sub(first.wchar),
        read_syscalls: last.read_syscalls.saturating_sub(first.read_syscalls),
        write_syscalls: last.write_syscalls.saturating_sub(first.write_syscalls),
        cancelled_write_bytes: last
            .cancelled_write_bytes
            .saturating_sub(first.cancelled_write_bytes),
    }
}

fn sample_device(device: &str) -> Option<DeviceAggregate> {
    procfs::diskstats()
        .ok()?
        .into_iter()
        .find(|stat| stat.name == device)
        .map(|stat| DeviceAggregate {
            reads: stat.reads,
            sectors_read: stat.sectors_read,
            writes: stat.writes,
            sectors_written: stat.sectors_written,
            in_progress: stat.in_progress,
            time_in_progress: stat.time_in_progress,
            weighted_time_in_progress: stat.weighted_time_in_progress,
        })
}

fn subtract_device(last: DeviceAggregate, first: DeviceAggregate) -> DeviceAggregate {
    DeviceAggregate {
        reads: last.reads.saturating_sub(first.reads),
        sectors_read: last.sectors_read.saturating_sub(first.sectors_read),
        writes: last.writes.saturating_sub(first.writes),
        sectors_written: last.sectors_written.saturating_sub(first.sectors_written),
        in_progress: last.in_progress,
        time_in_progress: last.time_in_progress.saturating_sub(first.time_in_progress),
        weighted_time_in_progress: last
            .weighted_time_in_progress
            .saturating_sub(first.weighted_time_in_progress),
    }
}

fn resolve_block_device(path: &Path) -> Option<String> {
    let path_arg = path.to_string_lossy();
    let source = command_output(
        "findmnt",
        &["--noheadings", "--output", "SOURCE", "--target", &path_arg],
    )
    .ok()?;
    let source = source.lines().next()?.trim();
    let device_path = source.strip_prefix("/dev/")?;
    let parent = command_output("lsblk", &["--noheadings", "--output", "PKNAME", source])
        .ok()
        .and_then(|value| value.lines().next().map(str::trim).map(str::to_string))
        .filter(|value| !value.is_empty());
    parent.or_else(|| {
        Path::new(device_path)
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
    })
}

fn resource_notes(
    participant: Participant,
    process_scope: ProcessScope,
    device: Option<&str>,
) -> Vec<String> {
    let mut notes = Vec::new();
    match (participant, process_scope) {
        (Participant::Postgres, ProcessScope::Tree(_)) => notes.push(
            "PostgreSQL metrics aggregate postmaster and current descendants; RSS sums shared pages and can overstate unique memory.".to_string(),
        ),
        (Participant::Server, ProcessScope::Single(_)) => notes.push(
            "RadixDB server runs inside the benchmark harness process; process metrics include client/harness sampler overhead.".to_string(),
        ),
        _ => {}
    }
    if device.is_some() {
        notes.push(
            "Device counters are host-wide for the resolved block device, so unrelated activity on the same NVMe is included.".to_string(),
        );
    }
    notes
}

fn make_run_id() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("run-{seconds}")
}

fn validate_run_id(run_id: &str) -> BenchResult<()> {
    let portable = !run_id.is_empty()
        && run_id.len() <= 128
        && !run_id.starts_with('.')
        && run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !portable || run_id == "." || run_id == ".." {
        return Err(format!(
            "invalid --run-id `{run_id}`; expected one portable filename component (ASCII letters, digits, '.', '_' or '-', not starting with '.', maximum 128 bytes)"
        )
        .into());
    }
    Ok(())
}

fn write_server_config(config: &BenchConfig, layout: &BenchLayout, port: u16) -> BenchResult<()> {
    let config_path = layout.rd_root.join("server.toml");
    let toml = format!(
        r#"[server]
bind_ip = "127.0.0.1"
port = {port}
data_dir = "{}"
max_connections = 151
connect_timeout_secs = 10
connection_idle_timeout_secs = 28800
net_read_timeout_secs = 30
net_write_timeout_secs = 60
cursor_batch_max_rows = 1024
cursor_batch_max_bytes = 8388608
max_frame_bytes = 67108864
copy_max_transaction_bytes = {}
max_compaction_jobs = 1
storage_cpu_workers = {}
page_cache_level = {}
page_cache_max_bytes = {}
page_cache_memory_reserve = {}
target_volume_rows = {}
seal_hot_bytes_threshold = {}
seal_incremental_hot_bytes_threshold = {}
read_queue_depth = {}
"#,
        layout.server_data.display(),
        BENCH_COPY_TRANSACTION_BYTES,
        config.storage_cpu_workers,
        config.page_cache_level,
        config.page_cache_max_bytes,
        config.page_cache_memory_reserve,
        config.target_volume_rows,
        config.seal_hot_bytes_threshold,
        config.seal_incremental_hot_bytes_threshold,
        config.read_queue_depth
    );
    fs::write(config_path, toml)?;
    Ok(())
}

fn write_environment(
    run_dir: &Path,
    config: &BenchConfig,
    layout: &BenchLayout,
    argv: &[String],
) -> BenchResult<()> {
    let server_config_path = layout.rd_root.join("server.toml");
    let postgres_config_path = layout.pg_root.join("data/postgresql.conf");
    let postgres_hba_path = layout.pg_root.join("data/pg_hba.conf");
    let postgres_opts_path = layout.pg_root.join("data/postmaster.opts");
    let postgres_version_path = layout.pg_root.join("data/PG_VERSION");
    let current_exe = std::env::current_exe().ok();
    let sanitized_argv = sanitize_argv(argv);
    let binary_identity = radixdb::server::build_identity();

    let environment = serde_json::json!({
        "manifest_version": 3,
        "captured_unix_seconds": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        "argv": sanitized_argv,
        "scale": config.scale.as_str(),
        "total_rows": config.scale.total_rows(),
        "participants": config.participants.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
        "run": config.run,
        "allow_large": config.allow_large,
        "keep_seed": config.keep_seed,
        "verify_existing": config.verify_existing,
        "expected_checksum": config.expected_checksum,
        "only_case": config.only_case.map(QueryCase::as_str),
        "warmup_runs": config.warmup_runs,
        "repeat_runs": config.repeat_runs,
        "case_profile": CaseProfile::for_config(config).as_str(),
        "ports": {
            "postgres": config.pg_port,
            "radixdb_server": config.server_port,
        },
        "postgres_password_configured": config.pg_password.is_some(),
        "postgres_profile": config.pg_profile.as_str(),
        "postgres_profile_settings": config.pg_profile.settings(),
        "root": layout.root,
        "pg_root": layout.pg_root,
        "rd_root": layout.rd_root,
        "server_data": layout.server_data,
        "source_checkout_context": {
            "git_head": command_output("git", &["rev-parse", "HEAD"]).ok(),
            "git_status_porcelain": command_output("git", &["status", "--porcelain=v1"]).ok(),
            "git_branch": command_output("git", &["branch", "--show-current"]).ok(),
        },
        "rustc": command_output("rustc", &["--version"]).ok(),
        "cargo": command_output("cargo", &["--version"]).ok(),
        "uname": command_output("uname", &["-a"]).ok(),
        "build": {
            "package_version": binary_identity.semantic_version,
            "git_revision": binary_identity.git_revision,
            "protocol_version": binary_identity.protocol_version,
            "profile": binary_identity.build_profile,
            "target": binary_identity.target,
            "debug_assertions": cfg!(debug_assertions),
            "executable": current_exe.as_ref().map(|path| path.display().to_string()),
            "executable_sha256": current_exe.as_deref().and_then(|path| sha256_file(path).ok()),
            "cargo_lock_sha256": env!("RADIXDB_CARGO_LOCK_SHA256"),
        },
        "server_config": file_snapshot(&server_config_path),
        "postgres": {
            "version": file_snapshot(&postgres_version_path),
            "postgresql_conf": file_snapshot(&postgres_config_path),
            "pg_hba_conf": file_snapshot(&postgres_hba_path),
            "postmaster_opts": file_snapshot(&postgres_opts_path),
        },
        "hardware": {
            "lscpu_json": command_output("lscpu", &["--json"]).ok(),
            "lsblk_json": command_output(
                "lsblk",
                &["--json", "-d", "-o", "NAME,MODEL,ROTA,TYPE,SIZE,LOG-SEC,PHY-SEC,SCHED"]
            ).ok(),
            "filesystem_json": command_output(
                "findmnt",
                &["--json", "-T", layout.root.to_string_lossy().as_ref()]
            ).ok(),
            "meminfo": read_text_if_exists(Path::new("/proc/meminfo")),
            "cpu_online": read_text_if_exists(Path::new("/sys/devices/system/cpu/online")),
        },
    });
    fs::write(
        run_dir.join("environment.json"),
        serde_json::to_string_pretty(&environment)?,
    )?;
    Ok(())
}

fn sanitize_argv(argv: &[String]) -> Vec<String> {
    let mut sanitized = Vec::with_capacity(argv.len());
    let mut redact_next = false;
    for arg in argv {
        if redact_next {
            sanitized.push("<redacted>".to_string());
            redact_next = false;
            continue;
        }
        if arg == "--pg-password" {
            sanitized.push(arg.clone());
            redact_next = true;
        } else if let Some((flag, _value)) = arg.split_once('=') {
            if flag == "--pg-password" {
                sanitized.push(format!("{flag}=<redacted>"));
            } else {
                sanitized.push(arg.clone());
            }
        } else {
            sanitized.push(arg.clone());
        }
    }
    sanitized
}

fn file_snapshot(path: &Path) -> serde_json::Value {
    serde_json::json!({
        "path": path,
        "sha256": sha256_file(path).ok(),
        "content": read_text_if_exists(path),
    })
}

fn read_text_if_exists(path: &Path) -> Option<String> {
    fs::read_to_string(path).ok()
}

fn sha256_file(path: &Path) -> BenchResult<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
}

fn command_output(program: &str, args: &[&str]) -> BenchResult<String> {
    let output = Command::new(program).args(args).output()?;
    if !output.status.success() {
        return Err(format!("{program} {:?} failed", args).into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn write_report_files(run_dir: &Path, report: &BenchReport) -> BenchResult<()> {
    let report_json = report_json_with_engine_high_water(report)?;
    fs::write(
        run_dir.join("results.json"),
        serde_json::to_string_pretty(&report_json)?,
    )?;

    let mut markdown = String::new();
    markdown.push_str("# RadixDB benchmark report\n\n");
    markdown.push_str(&format!("- run_id: `{}`\n", report.run_id));
    markdown.push_str(&format!("- scale: `{}`\n", report.scale.as_str()));
    markdown.push_str(&format!("- total rows: `{}`\n", report.total_rows));
    markdown.push_str(&format!("- dry run: `{}`\n", report.dry_run));
    markdown.push_str(&format!(
        "- PostgreSQL: `127.0.0.1:{}` data `{}`\n",
        report.resolved.postgres.port, report.resolved.postgres.data_dir
    ));
    markdown.push_str(&format!(
        "- SQLite: database `{}` ({})\n",
        report.resolved.sqlite.database_path, report.resolved.sqlite.adapter
    ));
    markdown.push_str(&format!(
        "- RadixDB server: `{}:{}` data `{}` max_frame `{}` cursor rows `{}` cursor bytes `{}`\n",
        report.resolved.radixdb_server.bind_ip,
        report.resolved.radixdb_server.port,
        report.resolved.radixdb_server.data_dir.display(),
        human_bytes(report.resolved.radixdb_server.max_frame_bytes as u64),
        report.resolved.radixdb_server.cursor_batch_max_rows,
        human_bytes(report.resolved.radixdb_server.cursor_batch_max_bytes as u64),
    ));
    if let Some(seed) = &report.seed {
        markdown.push_str(&format!(
            "- seed source: `{}`\n- seed dir: `{}`\n- seed CSV bytes: `{}`\n- seed elapsed ms: `{:.3}`\n",
            if seed.generated { "generated" } else { "reused" },
            seed.artifact_dir,
            seed.generated_csv_bytes,
            seed.elapsed_ms
        ));
    }
    if !report.readiness.is_empty() {
        markdown.push_str(
            "\n## RadixDB readiness\n\n| Phase | ms | server lifecycle | server ready | database | database lifecycle | database ready | artifacts |\n",
        );
        markdown.push_str("|---|---:|---|---:|---|---|---:|---|\n");
        for readiness in &report.readiness {
            let database = readiness.status.databases.first();
            markdown.push_str(&format!(
                "| {} | {:.3} | {:?} | {} | {} | {} | {} | {} |\n",
                readiness.phase,
                readiness.elapsed_ms,
                readiness.status.lifecycle,
                readiness.status.ready,
                database
                    .map(|database| database.name.as_str())
                    .unwrap_or("-"),
                database
                    .map(|database| format!("{:?}", database.lifecycle))
                    .unwrap_or_else(|| "-".to_string()),
                database.map(|database| database.ready).unwrap_or(false),
                database
                    .map(|database| format!(
                        "tables={} wal={} volumes={} snapshots={} checkpoints={} manifests={} other={}",
                        database.artifacts.table_dirs,
                        database.artifacts.wal_files,
                        database.artifacts.artifact_files,
                        database.artifacts.snapshot_files,
                        database.artifacts.checkpoint_files,
                        database.artifacts.manifest_files,
                        database.artifacts.other_files
                    ))
                    .unwrap_or_else(|| "-".to_string()),
            ));
        }
    }
    if !report.page_cache.is_empty() {
        markdown.push_str(
            "\n## Page-cache warmup\n\n| Phase | Level | State | Wait ms | Wall ms | Generation | Total | Safe budget | Target | Read | Resident estimate | Worker ms | Throughput | Limited by | Error |\n",
        );
        markdown
            .push_str("|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|\n");
        for metric in &report.page_cache {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {:.3} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                metric.phase,
                metric.requested_level,
                metric.state,
                metric.wait_limit_millis,
                metric.elapsed_ms,
                metric.generation_fingerprint,
                human_bytes(metric.total_generation_bytes),
                human_bytes(metric.safe_budget_bytes),
                human_bytes(metric.target_bytes),
                human_bytes(metric.warmed_bytes),
                human_bytes(metric.resident_estimate_bytes),
                metric.worker_duration_millis,
                human_bytes(metric.read_bytes_per_second),
                metric.limited_by,
                if metric.last_error.is_empty() {
                    "-"
                } else {
                    metric.last_error.as_str()
                },
            ));
        }
    }
    markdown.push_str("\n| Participant | Case | ms | rows | rows/sec | checksum |\n");
    markdown.push_str("|---|---:|---:|---:|---:|---|\n");
    for metric in &report.metrics {
        markdown.push_str(&format!(
            "| {} | {} | {:.3} | {} | {} | {} |\n",
            metric.participant,
            metric.case,
            metric.elapsed_ms,
            metric.rows,
            metric
                .rows_per_sec
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "-".to_string()),
            metric.checksum.as_deref().unwrap_or("-"),
        ));
    }
    let repeated_metrics = report
        .metrics
        .iter()
        .filter_map(|metric| {
            metric
                .repeat_summary
                .as_ref()
                .map(|summary| (metric, summary))
        })
        .collect::<Vec<_>>();
    if !repeated_metrics.is_empty() {
        markdown.push_str(
            "\nFor repeated cases the main `ms` value is the median of measured runs; warm-up runs are excluded from median/min.\n\n",
        );
        markdown.push_str(
            "For `restart-hot`, `Restart/cold first` is the first query after reopening the database; median/min are measured only after that warm-up. The harness does not clear the Linux page cache, so this is engine-cold, not guaranteed OS-cache-cold.\n\n",
        );
        markdown.push_str(
            "Repeated wall time is end-to-end `execute`/`fetch` through the binary protocol. Engine counters are stored separately as `case.<name>.warmup.<n>` and `case.<name>.measured.<n>`; EXPLAIN is outside all timed/counter windows. `protocol.result` reports delivered rows.\n\n",
        );
        markdown.push_str("## Repeated Case Timings\n\n");
        markdown.push_str(
            "| Participant | Case | Profile | Restart/cold first ms | Warm-up ms | Measured ms | Median ms | Min ms |\n",
        );
        markdown.push_str("|---|---|---|---:|---|---|---:|---:|\n");
        for (metric, summary) in repeated_metrics {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {:.3} | {:.3} |\n",
                metric.participant,
                metric.case,
                summary.profile.as_str(),
                summary
                    .restart_cold_first_ms
                    .map(|value| format!("{value:.3}"))
                    .unwrap_or_else(|| "-".to_string()),
                format_ms_samples(&summary.warmup_ms),
                format_ms_samples(&summary.measured_ms),
                summary.median_ms,
                summary.min_ms,
            ));
        }
    }
    if !report.membership.is_empty() {
        markdown.push_str("\n## PK Membership Gate\n\n");
        markdown.push_str(
            "This is a direct storage microbenchmark on parent IDs selected through the TCP client. Each sample alternates sequential `has_row_id()` and batch `probe_visible_row_ids()` order; raw paired samples and batch-only engine counters are retained in `results.json`.\n\n",
        );
        markdown.push_str(
            "| Profile | Input keys | Expected hits | Inner iterations | Sequential median ms | Batch median ms | Batch / sequential |\n",
        );
        markdown.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
        for metric in &report.membership {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {:.3} | {:.3} | {:.3} |\n",
                metric.profile.as_str(),
                metric.input_keys,
                metric.expected_hits,
                metric.inner_iterations,
                metric.sequential_median_ms,
                metric.batch_median_ms,
                metric.median_batch_over_sequential,
            ));
        }
    }
    if !report.access_paths.is_empty() {
        markdown.push_str("\n## Access Paths\n\n");
        markdown.push_str("Access paths are captured with non-ANALYZE `EXPLAIN` outside the timed case window, so EXPLAIN overhead is not mixed into elapsed/counter deltas.\n\n");
        markdown.push_str("| Participant | Case | Access paths | Error | SQL |\n");
        markdown.push_str("|---|---|---|---|---|\n");
        for access_path in &report.access_paths {
            let paths = if access_path.access_paths.is_empty() {
                "-".to_string()
            } else {
                access_path.access_paths.join("<br>")
            };
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | `{}` |\n",
                markdown_table_cell(&access_path.participant),
                markdown_table_cell(&access_path.case),
                markdown_table_cell(&paths),
                markdown_table_cell(access_path.error.as_deref().unwrap_or("-")),
                markdown_table_cell(&access_path.sql.replace('`', "\\`")),
            ));
        }
        markdown.push_str("\n### Full EXPLAIN output\n\n");
        for access_path in &report.access_paths {
            markdown.push_str(&format!(
                "#### {} — `{}`\n\n",
                access_path.participant, access_path.case
            ));
            if let Some(error) = &access_path.error {
                markdown.push_str(&format!("EXPLAIN error: `{error}`\n\n"));
            } else {
                markdown.push_str("```text\n");
                markdown.push_str(&access_path.explain_lines.join("\n"));
                markdown.push_str("\n```\n\n");
            }
        }
    }
    if !report.storage.is_empty() {
        markdown.push_str("\n## Storage\n\n");
        markdown.push_str("| Participant | Phase | Path | ms | samples | final allocated | peak allocated | final logical | peak logical | files | dirs | errors |\n");
        markdown.push_str("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for storage in &report.storage {
            markdown.push_str(&format!(
                "| {} | {} | {} | {:.3} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                storage.participant,
                storage.phase,
                storage.path,
                storage.elapsed_ms,
                storage.samples,
                human_bytes(storage.final_allocated_bytes),
                human_bytes(storage.peak_allocated_bytes),
                human_bytes(storage.final_logical_bytes),
                human_bytes(storage.peak_logical_bytes),
                storage.final_files,
                storage.final_dirs,
                storage.sample_errors,
            ));
        }
    }
    if !report.resources.is_empty() {
        markdown.push_str("\n## Resources\n\n");
        markdown.push_str("`participant.run` covers the full participant lifecycle, including external seed CSV reads and load/index work. For database-read comparisons use `query.phase`, which starts after schema/load/index creation and excludes seed CSV input.\n\n");
        markdown.push_str("| Participant | Phase | Scope | Device | ms | samples | CPU % | CPU user s | CPU system s | peak RSS | final RSS | peak virtual | threads | peak FDs | final FDs | peak sockets | final sockets | process read | process write | process write/s | device read | device write | device write/s | ctx voluntary | ctx involuntary | errors | notes |\n");
        markdown.push_str("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
        for resource in &report.resources {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {:.3} | {} | {:.2} | {:.3} | {:.3} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                resource.participant,
                resource.phase,
                resource.process_scope,
                resource.device.as_deref().unwrap_or("-"),
                resource.elapsed_ms,
                resource.samples,
                resource.cpu_total_percent,
                resource.cpu_user_seconds,
                resource.cpu_system_seconds,
                human_bytes(resource.peak_rss_bytes),
                human_bytes(resource.final_rss_bytes),
                human_bytes(resource.peak_vsize_bytes),
                resource.peak_threads,
                resource.peak_open_fds,
                resource.final_open_fds,
                resource.peak_socket_fds,
                resource.final_socket_fds,
                human_bytes(resource.process_read_bytes_delta),
                human_bytes(resource.process_write_bytes_delta),
                human_bytes(resource.process_write_bytes_per_sec as u64),
                human_bytes(resource.device_read_bytes_delta),
                human_bytes(resource.device_write_bytes_delta),
                human_bytes(resource.device_write_bytes_per_sec as u64),
                resource.voluntary_context_switches_delta,
                resource.involuntary_context_switches_delta,
                resource.sample_errors + resource.process_missing_samples,
                resource.notes.join("<br>"),
            ));
        }
    }
    if !report.engine.is_empty() {
        markdown.push_str("\n## Engine Counters\n\n");
        markdown.push_str("| Participant | Phase | Counter | Calls/entries | Rows | Values | Bytes in | Bytes out | ms |\n");
        markdown.push_str("|---|---|---|---:|---:|---:|---:|---:|---:|\n");
        for engine in &report.engine {
            let c = &engine.counters;
            push_engine_counter_row(
                &mut markdown,
                engine,
                "volume.read",
                c.volume_read_calls,
                0,
                c.volume_read_bytes,
                0,
                c.volume_read_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "decompress",
                c.decompression_calls,
                0,
                c.decompression_compressed_bytes,
                c.decompression_raw_bytes,
                c.decompression_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "row.materialize",
                c.row_materialization_calls,
                c.row_materialization_rows,
                0,
                0,
                c.row_materialization_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.payload.open",
                c.artifact_file_open_calls,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.payload.pread",
                c.artifact_pread_calls,
                0,
                c.artifact_pread_bytes,
                0,
                c.artifact_pread_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.columnar_group.input",
                c.artifact_columnar_group_applies,
                c.artifact_columnar_group_input_rows,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.columnar_group.output",
                0,
                c.artifact_columnar_group_output_groups,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.columnar_group.accumulator.direct",
                c.artifact_columnar_group_direct_accumulators,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.columnar_group.accumulator.hash",
                c.artifact_columnar_group_hash_accumulators,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.columnar_group.merge",
                c.artifact_columnar_group_local_merges,
                c.artifact_columnar_group_merged_groups,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.columnar_group.scheduler",
                c.artifact_columnar_group_scheduler_runs,
                c.artifact_columnar_group_scheduled_segments,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "artifact.columnar_group.fallback",
                c.artifact_columnar_group_fallbacks,
                0,
                0,
                0,
                0,
            );
            for (name, value) in [
                (
                    "artifact.columnar_group.fallback.row_state",
                    c.artifact_columnar_group_fallback_row_state,
                ),
                (
                    "artifact.columnar_group.fallback.no_cold_artifact",
                    c.artifact_columnar_group_fallback_no_cold_artifact,
                ),
                (
                    "artifact.columnar_group.fallback.schema",
                    c.artifact_columnar_group_fallback_schema,
                ),
                (
                    "artifact.columnar_group.fallback.group_key",
                    c.artifact_columnar_group_fallback_group_key,
                ),
                (
                    "artifact.columnar_group.fallback.aggregate",
                    c.artifact_columnar_group_fallback_aggregate,
                ),
                (
                    "artifact.columnar_group.fallback.visibility",
                    c.artifact_columnar_group_fallback_visibility,
                ),
                (
                    "artifact.columnar_group.fallback.storage",
                    c.artifact_columnar_group_fallback_storage,
                ),
                (
                    "artifact.columnar_group.fallback.column_shape",
                    c.artifact_columnar_group_fallback_column_shape,
                ),
                (
                    "artifact.columnar_group.fallback.accumulator",
                    c.artifact_columnar_group_fallback_accumulator,
                ),
            ] {
                push_engine_counter_row(&mut markdown, engine, name, value, 0, 0, 0, 0);
            }
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count",
                c.metadata_pk_count_attempts,
                c.metadata_pk_count_candidate_rows,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.applied",
                c.metadata_pk_count_applied,
                c.metadata_pk_count_visible_rows,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.interval",
                c.metadata_pk_count_intervals,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.visibility_excluded",
                0,
                c.metadata_pk_count_visibility_exclusions,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.hot_candidate",
                0,
                c.metadata_pk_count_hot_candidates,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.fallback",
                c.metadata_pk_count_fallbacks,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.fallback.snapshot",
                c.metadata_pk_count_fallback_snapshot,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.fallback.seal_overlap",
                c.metadata_pk_count_fallback_seal_overlap,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.fallback.unsupported",
                c.metadata_pk_count_fallback_unsupported,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "pk_metadata_count.fallback.candidate_limit",
                c.metadata_pk_count_fallback_candidate_limit,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "join.outer",
                0,
                c.join_outer_rows,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "join.key",
                0,
                c.join_key_rows,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "join.pk_probe",
                c.join_pk_probe_batches,
                c.join_pk_probe_keys,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "join.pk_hit",
                0,
                c.join_pk_probe_hits,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "join.parent_payload",
                0,
                c.join_parent_payload_rows,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "join.constructed",
                0,
                c.join_rows_constructed,
                0,
                0,
                0,
            );
            for (name, value) in [
                ("navigation.paths.planned", c.navigation_paths_planned),
                ("navigation.paths.executed", c.navigation_paths_executed),
                ("navigation.source_rows", c.navigation_source_rows),
                (
                    "navigation.distinct_source_keys",
                    c.navigation_distinct_source_keys,
                ),
                (
                    "navigation.repeated_keys_eliminated",
                    c.navigation_repeated_keys_eliminated,
                ),
                ("navigation.lookup_batches", c.navigation_lookup_batches),
                ("navigation.lookup_hits", c.navigation_lookup_hits),
                ("navigation.lookup_misses", c.navigation_lookup_misses),
                ("navigation.strategy.direct", c.navigation_direct_edges),
                (
                    "navigation.strategy.index_nested_loop",
                    c.navigation_index_nested_loop_edges,
                ),
                ("navigation.strategy.batch", c.navigation_batch_edges),
                ("navigation.strategy.hash", c.navigation_hash_edges),
                ("navigation.strategy.merge", c.navigation_merge_edges),
                ("navigation.strategy.fallback", c.navigation_fallback_edges),
                (
                    "navigation.strategy.planner_left_join",
                    c.navigation_planner_left_join_edges,
                ),
                (
                    "navigation.integrity_failures",
                    c.navigation_integrity_failures,
                ),
                ("navigation.cancellations", c.navigation_cancellations),
                ("navigation.timeouts", c.navigation_timeouts),
            ] {
                push_engine_counter_row(&mut markdown, engine, name, value, 0, 0, 0, 0);
            }
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.result",
                0,
                c.protocol_result_rows,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.row_to_wire",
                0,
                c.protocol_row_to_wire_values,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.row_size_probe",
                c.protocol_row_size_probe_calls,
                0,
                c.protocol_row_size_probe_bytes,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.row_batch",
                c.protocol_row_batch_frames,
                c.protocol_result_rows,
                c.protocol_row_batch_probe_bytes,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch_fallback",
                c.protocol_column_batch_fallbacks,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch_fallback.row_state",
                c.protocol_column_batch_fallback_row_state,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch_fallback.query_shape",
                c.protocol_column_batch_fallback_query_shape,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch_fallback.storage_shape",
                c.protocol_column_batch_fallback_storage_shape,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch_fallback.schema",
                c.protocol_column_batch_fallback_schema,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch_fallback.unknown",
                c.protocol_column_batch_fallback_unknown,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch.pending.opened",
                c.protocol_column_batch_pending_opened,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch.pending.completed",
                c.protocol_column_batch_pending_completed,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch.pending.dropped",
                c.protocol_column_batch_pending_dropped,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch.pending.current",
                c.protocol_column_batch_pending_current,
                c.protocol_column_batch_pending_rows_current,
                0,
                c.protocol_column_batch_pending_bytes_current,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "protocol.column_batch.pending.max",
                c.protocol_column_batch_pending_max,
                c.protocol_column_batch_pending_rows_max,
                0,
                c.protocol_column_batch_pending_bytes_max,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "ram.accel.build",
                c.ram_accelerator_builds,
                c.ram_accelerator_build_entries,
                c.ram_accelerator_build_bytes,
                0,
                c.ram_accelerator_build_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "ram.accel.hit",
                c.ram_accelerator_hits,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "ram.accel.miss",
                c.ram_accelerator_misses,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "ram.accel.fallback",
                c.ram_accelerator_fallbacks,
                0,
                0,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "ram.accel.eviction",
                c.ram_accelerator_evictions,
                0,
                0,
                c.ram_accelerator_eviction_bytes,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "wal.append",
                c.wal_append_entries,
                0,
                c.wal_append_bytes,
                0,
                0,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "wal.write",
                c.wal_write_calls,
                0,
                0,
                c.wal_write_bytes,
                c.wal_write_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "wal.sync",
                c.wal_sync_calls,
                0,
                0,
                0,
                c.wal_sync_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "wal.generation_validation",
                c.wal_generation_validation_calls,
                0,
                0,
                c.wal_generation_validation_bytes,
                c.wal_generation_validation_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "wal.retention",
                c.wal_retention_calls,
                c.wal_retention_files_deleted,
                c.wal_retention_identity_checks,
                0,
                c.wal_retention_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "seal",
                c.seal_calls,
                c.seal_rows,
                c.seal_bytes,
                c.seal_output_bytes,
                c.seal_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "compaction",
                c.compaction_calls,
                c.compaction_tables,
                0,
                0,
                c.compaction_nanos,
            );
            let profile = c.runtime_profile;
            push_engine_counter_row(
                &mut markdown,
                engine,
                "runtime.wait",
                profile.wait_calls,
                0,
                0,
                0,
                profile.wait_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "runtime.hash_build",
                profile.hash_build_calls,
                profile.hash_build_rows,
                0,
                0,
                profile.hash_build_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "runtime.index_lookup",
                profile.index_lookup_calls,
                profile.index_lookup_keys,
                0,
                profile.index_lookup_hits,
                profile.index_lookup_nanos,
            );
            push_engine_counter_row(
                &mut markdown,
                engine,
                "runtime.protocol_round_trip",
                profile.protocol_round_trips,
                0,
                0,
                0,
                profile.protocol_round_trip_nanos,
            );
        }
    }
    if !report.client_protocol.is_empty() {
        markdown.push_str("\n## Client Protocol Counters\n\n");
        markdown.push_str(
            "These counters are measured inside the reusable RadixDB binary client decoder. They cover bincode frame decode work after the payload has been read from the socket; server execution and socket wait time are not included.\n\n",
        );
        markdown.push_str("| Participant | Phase | Decode calls | Decode bytes | Decode ms |\n");
        markdown.push_str("|---|---|---:|---:|---:|\n");
        for metric in &report.client_protocol {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {:.3} |\n",
                metric.participant,
                metric.phase,
                metric.counters.frame_decode_calls,
                human_bytes(metric.counters.frame_decode_bytes),
                metric.counters.frame_decode_nanos as f64 / 1_000_000.0,
            ));
        }
    }
    if !report.engine_snapshots.is_empty() {
        markdown.push_str("\n## Engine Absolute Snapshots\n\n");
        markdown.push_str("Snapshots are absolute within the current instrumentation epoch. `engine_high_water` in `results.json` stores per-counter maxima across these snapshots, so benchmark readers do not have to infer high-water values from per-case deltas.\n\n");
        markdown.push_str("| Participant | Phase | Diagnostics | artifact-backed pread | artifact-backed pread bytes | artifact-backed payload decompress | artifact-backed column deserialize rows | Row materialized | Protocol rows | Protocol encode bytes | Socket write bytes |\n");
        markdown.push_str("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for snapshot in &report.engine_snapshots {
            let c = &snapshot.counters;
            let diagnostics = if snapshot.diagnostics.is_empty() {
                "-".to_string()
            } else {
                snapshot
                    .diagnostics
                    .iter()
                    .map(|diagnostic| {
                        format!(
                            "{}:{}:{}",
                            diagnostic.level, diagnostic.code, diagnostic.message
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("<br>")
            };
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                markdown_table_cell(&snapshot.participant),
                markdown_table_cell(&snapshot.phase),
                markdown_table_cell(&diagnostics),
                c.artifact_pread_calls,
                human_bytes(c.artifact_pread_bytes),
                c.artifact_payload_decompress_calls,
                c.artifact_column_deserialize_rows,
                c.row_materialization_rows,
                c.protocol_result_rows,
                human_bytes(c.protocol_encode_bytes),
                human_bytes(c.protocol_socket_write_bytes),
            ));
        }
    }
    fs::write(run_dir.join("REPORT.md"), markdown)?;
    Ok(())
}

fn report_json_with_engine_high_water(report: &BenchReport) -> BenchResult<serde_json::Value> {
    let mut value = serde_json::to_value(report)?;
    if let serde_json::Value::Object(ref mut object) = value {
        object.insert(
            "engine_high_water".to_string(),
            engine_high_water_json(&report.engine_snapshots),
        );
    }
    Ok(value)
}

fn engine_high_water_json(snapshots: &[EngineSnapshotMetric]) -> serde_json::Value {
    let mut high_water = serde_json::Map::new();
    for snapshot in snapshots {
        let Ok(serde_json::Value::Object(counters)) = serde_json::to_value(&snapshot.counters)
        else {
            continue;
        };
        for (name, value) in counters {
            merge_engine_high_water(&mut high_water, &name, value);
        }
    }
    serde_json::Value::Object(high_water)
}

fn merge_engine_high_water(
    high_water: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    value: serde_json::Value,
) {
    if let serde_json::Value::Object(children) = value {
        for (child, value) in children {
            merge_engine_high_water(high_water, &format!("{name}.{child}"), value);
        }
        return;
    }
    let Some(candidate) = value.as_u64() else {
        return;
    };
    let current = high_water
        .get(name)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if !high_water.contains_key(name) || candidate > current {
        high_water.insert(name.to_string(), value);
    }
}

fn format_ms_samples(samples: &[f64]) -> String {
    samples
        .iter()
        .map(|value| format!("{value:.3}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn markdown_table_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', "<br>")
}

// Markdown report rows keep all rendered counter fields explicit; a struct
// wrapper here would only move column order away from the table writer.
#[allow(clippy::too_many_arguments)]
fn push_engine_counter_row(
    markdown: &mut String,
    engine: &EngineMetric,
    counter: &str,
    calls: u64,
    rows: u64,
    bytes_in: u64,
    bytes_out: u64,
    nanos: u64,
) {
    let values = match counter {
        "row.materialize" => engine.counters.row_materialization_values,
        "artifact.columnar_group.input" => engine.counters.artifact_columnar_group_selected_blocks,
        "artifact.columnar_group.output" => engine.counters.artifact_columnar_group_row_groups,
        _ => 0,
    };
    markdown.push_str(&format!(
        "| {} | {} | {} | {} | {} | {} | {} | {} | {:.3} |\n",
        engine.participant,
        engine.phase,
        counter,
        calls,
        rows,
        values,
        human_bytes(bytes_in),
        human_bytes(bytes_out),
        nanos as f64 / 1_000_000.0,
    ));
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}
