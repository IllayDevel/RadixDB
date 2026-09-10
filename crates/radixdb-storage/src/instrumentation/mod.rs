use serde::Serialize;
use std::{
    cell::{Cell, RefCell},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use radixdb_core::Value;
mod runtime_profile;

pub use runtime_profile::{
    MetadataPkCountFallback, ProtocolColumnBatchFallback, RuntimeProfileSnapshot, RuntimeWaitKind,
};

/// Always-on gauges for runtime owners that live above one storage engine.
///
/// A server process can host more than one database, therefore these values
/// are process-wide by design. They are kept separate from resettable
/// cumulative counters: resetting benchmark counters while sessions are live
/// must not manufacture a negative or wrapped owner count.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct RuntimeOwnerSnapshot {
    pub server_connections: u64,
    pub authenticated_sessions: u64,
    pub active_cursors: u64,
    pub prepared_statements: u64,
    pub active_executions: u64,
    pub staged_tables: u64,
    pub staged_indexes: u64,
    pub staged_index_drops: u64,
    pub staged_schema_changes: u64,
    pub staged_constraint_changes: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct EngineCountersSnapshot {
    pub runtime_profile: RuntimeProfileSnapshot,
    pub volume_read_calls: u64,
    pub volume_read_bytes: u64,
    pub volume_read_nanos: u64,
    pub decompression_calls: u64,
    pub decompression_compressed_bytes: u64,
    pub decompression_raw_bytes: u64,
    pub decompression_nanos: u64,
    pub row_materialization_calls: u64,
    pub row_materialization_rows: u64,
    pub row_materialization_values: u64,
    pub row_materialization_nanos: u64,
    /// Derived table source executions. This counts source starts, not rows.
    pub derived_subquery_executes: u64,
    /// Successful artifact-backed descriptor/metadata file opens. Payload opens are counted
    /// separately in `artifact_file_open_calls`.
    pub artifact_descriptor_open_calls: u64,
    /// Successful foreground artifact-backed payload-file opens. Descriptor/metadata open
    /// at startup deliberately does not contribute to this counter.
    pub artifact_file_open_calls: u64,
    /// Successful `metadata()` calls for artifact-backed descriptor or payload sessions.
    pub artifact_file_stat_calls: u64,
    /// Successful immutable file identity checks for artifact-backed descriptor or payload sessions.
    pub artifact_file_identity_checks: u64,
    /// artifact-backed `posix_fadvise`/advisory hint calls attempted by descriptor/payload paths.
    pub artifact_fadvise_calls: u64,
    /// artifact-backed advisory hint calls rejected by the OS.
    pub artifact_fadvise_errors: u64,
    /// Actual foreground artifact-backed payload `pread` syscalls, not logical blocks.
    pub artifact_pread_calls: u64,
    pub artifact_pread_bytes: u64,
    pub artifact_pread_nanos: u64,
    pub artifact_singleflight_leaders: u64,
    pub artifact_singleflight_followers: u64,
    pub artifact_payload_decompress_calls: u64,
    pub artifact_payload_decompress_compressed_bytes: u64,
    pub artifact_payload_decompress_raw_bytes: u64,
    pub artifact_payload_decompress_nanos: u64,
    pub artifact_column_deserialize_calls: u64,
    pub artifact_column_deserialize_bytes: u64,
    pub artifact_column_deserialize_rows: u64,
    pub artifact_column_deserialize_nanos: u64,
    /// artifact-backed columnar grouped-aggregate operator activity.
    pub artifact_columnar_group_applies: u64,
    pub artifact_columnar_group_row_groups: u64,
    pub artifact_columnar_group_selected_blocks: u64,
    pub artifact_columnar_group_input_rows: u64,
    pub artifact_columnar_group_output_groups: u64,
    pub artifact_columnar_group_direct_accumulators: u64,
    pub artifact_columnar_group_hash_accumulators: u64,
    pub artifact_columnar_group_local_merges: u64,
    pub artifact_columnar_group_merged_groups: u64,
    pub artifact_columnar_group_scheduler_runs: u64,
    pub artifact_columnar_group_scheduled_segments: u64,
    pub artifact_columnar_group_fallbacks: u64,
    pub artifact_columnar_group_fallback_row_state: u64,
    pub artifact_columnar_group_fallback_no_cold_artifact: u64,
    pub artifact_columnar_group_fallback_schema: u64,
    pub artifact_columnar_group_fallback_group_key: u64,
    pub artifact_columnar_group_fallback_aggregate: u64,
    pub artifact_columnar_group_fallback_visibility: u64,
    pub artifact_columnar_group_fallback_storage: u64,
    pub artifact_columnar_group_fallback_column_shape: u64,
    pub artifact_columnar_group_fallback_accumulator: u64,
    pub metadata_pk_count_attempts: u64,
    pub metadata_pk_count_applied: u64,
    pub metadata_pk_count_intervals: u64,
    pub metadata_pk_count_candidate_rows: u64,
    pub metadata_pk_count_visible_rows: u64,
    pub metadata_pk_count_visibility_exclusions: u64,
    pub metadata_pk_count_hot_candidates: u64,
    pub metadata_pk_count_fallbacks: u64,
    pub metadata_pk_count_fallback_snapshot: u64,
    pub metadata_pk_count_fallback_seal_overlap: u64,
    pub metadata_pk_count_fallback_unsupported: u64,
    pub metadata_pk_count_fallback_candidate_limit: u64,
    pub join_outer_rows: u64,
    pub join_key_rows: u64,
    pub join_pk_probe_batches: u64,
    pub join_pk_probe_keys: u64,
    pub join_pk_probe_hits: u64,
    pub join_parent_payload_rows: u64,
    pub join_rows_constructed: u64,
    /// Virtual projected rows produced without copying retained values.
    pub join_rows_deferred: u64,
    /// Virtual projected rows consumed directly by a subsequent JOIN edge.
    pub join_deferred_rows_consumed: u64,
    pub join_operator_calls: u64,
    pub join_hash_streaming_calls: u64,
    pub join_hash_parallel_calls: u64,
    pub join_merge_calls: u64,
    pub join_nested_loop_calls: u64,
    pub join_index_nested_loop_calls: u64,
    pub join_batch_index_nested_loop_calls: u64,
    pub join_left_input_rows: u64,
    pub join_right_input_rows: u64,
    pub join_output_rows: u64,
    pub join_input_values: u64,
    pub join_output_values: u64,
    pub join_candidate_pairs: u64,
    pub join_lookup_calls: u64,
    pub join_lookup_candidate_rows: u64,
    pub join_wall_nanos: u64,
    pub join_max_left_input_rows: u64,
    pub join_max_right_input_rows: u64,
    pub join_max_output_rows: u64,
    pub join_max_output_width: u64,
    pub navigation_paths_planned: u64,
    pub navigation_paths_executed: u64,
    pub navigation_source_rows: u64,
    pub navigation_distinct_source_keys: u64,
    pub navigation_repeated_keys_eliminated: u64,
    pub navigation_lookup_batches: u64,
    pub navigation_lookup_hits: u64,
    pub navigation_lookup_misses: u64,
    pub navigation_direct_edges: u64,
    pub navigation_index_nested_loop_edges: u64,
    pub navigation_batch_edges: u64,
    pub navigation_hash_edges: u64,
    pub navigation_merge_edges: u64,
    pub navigation_fallback_edges: u64,
    pub navigation_planner_left_join_edges: u64,
    pub navigation_integrity_failures: u64,
    pub navigation_cancellations: u64,
    pub navigation_timeouts: u64,
    pub protocol_result_rows: u64,
    /// Values converted from storage `Value` to protocol `WireValue`.
    pub protocol_row_to_wire_values: u64,
    /// Legacy counters for exact per-row bincode probes. A non-zero value is
    /// now a regression: prepared RowBatch payloads enforce limits without
    /// serialising every row separately.
    pub protocol_row_size_probe_calls: u64,
    pub protocol_row_size_probe_bytes: u64,
    /// Cursor batches constructed by the legacy row protocol.
    pub protocol_row_batch_frames: u64,
    /// Legacy sum of individually probed row payload bytes. Must remain zero.
    pub protocol_row_batch_probe_bytes: u64,
    /// ColumnBatchV1 requests that safely fell back to the row protocol.
    pub protocol_column_batch_fallbacks: u64,
    pub protocol_column_batch_fallback_row_state: u64,
    pub protocol_column_batch_fallback_query_shape: u64,
    pub protocol_column_batch_fallback_storage_shape: u64,
    pub protocol_column_batch_fallback_schema: u64,
    pub protocol_column_batch_fallback_unknown: u64,
    /// Split ColumnBatchV1 groups retained by the server after an oversized
    /// typed frame had to be sliced.
    pub protocol_column_batch_pending_opened: u64,
    pub protocol_column_batch_pending_completed: u64,
    pub protocol_column_batch_pending_dropped: u64,
    /// Current/high-water pending split batches, retained rows and approximate
    /// retained payload bytes. Current values are gauges; max values are
    /// process high-water marks.
    pub protocol_column_batch_pending_current: u64,
    pub protocol_column_batch_pending_max: u64,
    pub protocol_column_batch_pending_rows_current: u64,
    pub protocol_column_batch_pending_rows_max: u64,
    pub protocol_column_batch_pending_bytes_current: u64,
    pub protocol_column_batch_pending_bytes_max: u64,
    pub protocol_encode_calls: u64,
    pub protocol_encode_bytes: u64,
    pub protocol_encode_nanos: u64,
    pub protocol_socket_write_calls: u64,
    pub protocol_socket_write_bytes: u64,
    pub protocol_socket_write_nanos: u64,
    pub ram_accelerator_builds: u64,
    pub ram_accelerator_build_entries: u64,
    pub ram_accelerator_build_bytes: u64,
    pub ram_accelerator_build_nanos: u64,
    pub ram_accelerator_hits: u64,
    pub ram_accelerator_misses: u64,
    pub ram_accelerator_fallbacks: u64,
    pub ram_accelerator_evictions: u64,
    pub ram_accelerator_eviction_bytes: u64,
    pub wal_append_entries: u64,
    pub wal_append_bytes: u64,
    pub wal_write_calls: u64,
    pub wal_write_bytes: u64,
    pub wal_write_nanos: u64,
    pub wal_sync_calls: u64,
    pub wal_sync_nanos: u64,
    /// Full payload validation performed once when a WAL generation becomes
    /// immutable (or during startup/recovery).
    pub wal_generation_validation_calls: u64,
    pub wal_generation_validation_bytes: u64,
    pub wal_generation_validation_nanos: u64,
    /// Metadata-only retirement of already validated immutable generations.
    pub wal_retention_calls: u64,
    pub wal_retention_identity_checks: u64,
    pub wal_retention_files_deleted: u64,
    pub wal_retention_nanos: u64,
    /// COPY owner boundaries. `parse_nanos` includes streamed decode,
    /// materialization and transaction-local batch insertion before commit.
    pub copy_calls: u64,
    pub copy_rows: u64,
    pub copy_parse_nanos: u64,
    pub copy_commit_nanos: u64,
    pub copy_total_nanos: u64,
    /// Cold constraint certification owned by INSERT batches. These counters
    /// make database-size-dependent validation visible in long bulk imports.
    pub cold_constraint_batch_calls: u64,
    pub cold_constraint_batch_rows: u64,
    pub cold_constraint_batch_segments: u64,
    pub cold_constraint_batch_nanos: u64,
    pub cold_pk_batch_nanos: u64,
    /// Physical I/O performed by the bounded compaction row-reference spool.
    /// These bytes are temporary pipeline traffic, not durable artifact bytes.
    pub compaction_spool_write_calls: u64,
    pub compaction_spool_write_bytes: u64,
    pub compaction_spool_write_nanos: u64,
    pub compaction_spool_read_calls: u64,
    pub compaction_spool_read_bytes: u64,
    pub compaction_spool_read_nanos: u64,
    /// Segment-local persisted posting fanout. Maxima make accidental growth
    /// visible without unbounded per-table metric labels.
    pub posting_exact_lookup_calls: u64,
    pub posting_exact_lookup_segments: u64,
    pub posting_exact_lookup_max_segments: u64,
    pub posting_ordered_lookup_calls: u64,
    pub posting_ordered_lookup_segments: u64,
    pub posting_ordered_lookup_max_segments: u64,
    pub seal_calls: u64,
    pub seal_rows: u64,
    pub seal_bytes: u64,
    pub seal_output_bytes: u64,
    pub seal_nanos: u64,
    pub compaction_calls: u64,
    pub compaction_tables: u64,
    pub compaction_nanos: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct VolumeReadProbeSnapshot {
    pub calls: u64,
    pub bytes: u64,
}

/// Metadata-only INTEGER primary-key count work performed by the current
/// execution thread while a caller-owned probe is active.
///
/// Process-wide counters remain the source for benchmark reports.  This small
/// probe exists for request-local diagnostics such as `EXPLAIN ANALYZE`: those
/// diagnostics must not accidentally describe a different concurrent request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct MetadataPkCountProbeSnapshot {
    pub attempts: u64,
    pub applied: u64,
    pub intervals: u64,
    pub candidate_rows: u64,
    pub visible_rows: u64,
    pub visibility_exclusions: u64,
    pub hot_candidates: u64,
    pub fallbacks: u64,
    pub fallback_snapshot: u64,
    pub fallback_seal_overlap: u64,
    pub fallback_unsupported: u64,
    pub fallback_candidate_limit: u64,
}

/// Derived table executions observed by the current execution thread while a
/// caller-owned probe is active.
///
/// This is intentionally request-local: correctness tests need to prove that a
/// rejected derived-table optimization keeps consuming the same source instead
/// of executing the subquery again. Process-wide counters would make that proof
/// flaky under parallel tests or concurrent client sessions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct DerivedSubqueryProbeSnapshot {
    pub executes: u64,
}

/// Request-local proof for the artifact-backed columnar grouped-aggregate operator.
///
/// The production benchmark ultimately needs process-wide counters, but unit
/// correctness must stay independent from unrelated tests or client sessions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
#[doc(hidden)]
pub struct ArtifactColumnarGroupProbeSnapshot {
    pub applies: u64,
    pub row_groups: u64,
    pub selected_blocks: u64,
    pub input_rows: u64,
    pub output_groups: u64,
    pub direct_accumulators: u64,
    pub hash_accumulators: u64,
    pub local_merges: u64,
    pub merged_groups: u64,
    pub scheduler_runs: u64,
    pub scheduled_segments: u64,
    pub fallbacks: u64,
    pub fallback_row_state: u64,
    pub fallback_no_cold_artifact: u64,
    pub fallback_schema: u64,
    pub fallback_group_key: u64,
    pub fallback_aggregate: u64,
    pub fallback_visibility: u64,
    pub fallback_storage: u64,
    pub fallback_column_shape: u64,
    pub fallback_accumulator: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct ArtifactColumnarGroupRecord {
    pub row_groups: u64,
    pub selected_blocks: u64,
    pub input_rows: u64,
    pub output_groups: u64,
    pub direct_accumulators: u64,
    pub hash_accumulators: u64,
    pub local_merges: u64,
    pub merged_groups: u64,
    pub scheduler_runs: u64,
    pub scheduled_segments: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum ArtifactColumnarGroupFallback {
    RowState,
    NoColdArtifact,
    Schema,
    GroupKey,
    Aggregate,
    Visibility,
    Storage,
    ColumnShape,
    Accumulator,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
#[doc(hidden)]
pub struct ProtocolComponentProbeSnapshot {
    pub encode_calls: u64,
    pub encode_bytes: u64,
    pub socket_write_calls: u64,
    pub socket_write_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
#[doc(hidden)]
pub struct ProtocolPendingColumnBatchProbeSnapshot {
    pub opened: u64,
    pub completed: u64,
    pub dropped: u64,
    pub current: u64,
    pub max: u64,
    pub rows_current: u64,
    pub rows_max: u64,
    pub bytes_current: u64,
    pub bytes_max: u64,
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct RowMaterializationProbeSnapshot {
    pub calls: u64,
    pub rows: u64,
    pub values: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
#[doc(hidden)]
pub struct DecompressionProbeSnapshot {
    pub calls: u64,
    pub compressed_bytes: u64,
    pub raw_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct ArtifactIoProbeSnapshot {
    pub file_open_calls: u64,
    pub pread_calls: u64,
    pub pread_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct RamAcceleratorProbeSnapshot {
    pub builds: u64,
    pub build_entries: u64,
    pub build_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub fallbacks: u64,
    pub evictions: u64,
    pub eviction_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct PostingLookupProbeSnapshot {
    pub exact_calls: u64,
    pub exact_segments: u64,
    pub ordered_calls: u64,
    pub ordered_segments: u64,
}

struct EngineCounters {
    volume_read_calls: AtomicU64,
    volume_read_bytes: AtomicU64,
    volume_read_nanos: AtomicU64,
    decompression_calls: AtomicU64,
    decompression_compressed_bytes: AtomicU64,
    decompression_raw_bytes: AtomicU64,
    decompression_nanos: AtomicU64,
    row_materialization_calls: AtomicU64,
    row_materialization_rows: AtomicU64,
    row_materialization_values: AtomicU64,
    row_materialization_nanos: AtomicU64,
    derived_subquery_executes: AtomicU64,
    artifact_descriptor_open_calls: AtomicU64,
    artifact_file_open_calls: AtomicU64,
    artifact_file_stat_calls: AtomicU64,
    artifact_file_identity_checks: AtomicU64,
    artifact_fadvise_calls: AtomicU64,
    artifact_fadvise_errors: AtomicU64,
    artifact_pread_calls: AtomicU64,
    artifact_pread_bytes: AtomicU64,
    artifact_pread_nanos: AtomicU64,
    artifact_singleflight_leaders: AtomicU64,
    artifact_singleflight_followers: AtomicU64,
    artifact_payload_decompress_calls: AtomicU64,
    artifact_payload_decompress_compressed_bytes: AtomicU64,
    artifact_payload_decompress_raw_bytes: AtomicU64,
    artifact_payload_decompress_nanos: AtomicU64,
    artifact_column_deserialize_calls: AtomicU64,
    artifact_column_deserialize_bytes: AtomicU64,
    artifact_column_deserialize_rows: AtomicU64,
    artifact_column_deserialize_nanos: AtomicU64,
    artifact_columnar_group_applies: AtomicU64,
    artifact_columnar_group_row_groups: AtomicU64,
    artifact_columnar_group_selected_blocks: AtomicU64,
    artifact_columnar_group_input_rows: AtomicU64,
    artifact_columnar_group_output_groups: AtomicU64,
    artifact_columnar_group_direct_accumulators: AtomicU64,
    artifact_columnar_group_hash_accumulators: AtomicU64,
    artifact_columnar_group_local_merges: AtomicU64,
    artifact_columnar_group_merged_groups: AtomicU64,
    artifact_columnar_group_scheduler_runs: AtomicU64,
    artifact_columnar_group_scheduled_segments: AtomicU64,
    artifact_columnar_group_fallbacks: AtomicU64,
    artifact_columnar_group_fallback_row_state: AtomicU64,
    artifact_columnar_group_fallback_no_cold_artifact: AtomicU64,
    artifact_columnar_group_fallback_schema: AtomicU64,
    artifact_columnar_group_fallback_group_key: AtomicU64,
    artifact_columnar_group_fallback_aggregate: AtomicU64,
    artifact_columnar_group_fallback_visibility: AtomicU64,
    artifact_columnar_group_fallback_storage: AtomicU64,
    artifact_columnar_group_fallback_column_shape: AtomicU64,
    artifact_columnar_group_fallback_accumulator: AtomicU64,
    metadata_pk_count_attempts: AtomicU64,
    metadata_pk_count_applied: AtomicU64,
    metadata_pk_count_intervals: AtomicU64,
    metadata_pk_count_candidate_rows: AtomicU64,
    metadata_pk_count_visible_rows: AtomicU64,
    metadata_pk_count_visibility_exclusions: AtomicU64,
    metadata_pk_count_hot_candidates: AtomicU64,
    metadata_pk_count_fallbacks: AtomicU64,
    metadata_pk_count_fallback_snapshot: AtomicU64,
    metadata_pk_count_fallback_seal_overlap: AtomicU64,
    metadata_pk_count_fallback_unsupported: AtomicU64,
    metadata_pk_count_fallback_candidate_limit: AtomicU64,
    join_outer_rows: AtomicU64,
    join_key_rows: AtomicU64,
    join_pk_probe_batches: AtomicU64,
    join_pk_probe_keys: AtomicU64,
    join_pk_probe_hits: AtomicU64,
    join_parent_payload_rows: AtomicU64,
    join_rows_constructed: AtomicU64,
    join_rows_deferred: AtomicU64,
    join_deferred_rows_consumed: AtomicU64,
    join_operator_calls: AtomicU64,
    join_hash_streaming_calls: AtomicU64,
    join_hash_parallel_calls: AtomicU64,
    join_merge_calls: AtomicU64,
    join_nested_loop_calls: AtomicU64,
    join_index_nested_loop_calls: AtomicU64,
    join_batch_index_nested_loop_calls: AtomicU64,
    join_left_input_rows: AtomicU64,
    join_right_input_rows: AtomicU64,
    join_output_rows: AtomicU64,
    join_input_values: AtomicU64,
    join_output_values: AtomicU64,
    join_candidate_pairs: AtomicU64,
    join_lookup_calls: AtomicU64,
    join_lookup_candidate_rows: AtomicU64,
    join_wall_nanos: AtomicU64,
    join_max_left_input_rows: AtomicU64,
    join_max_right_input_rows: AtomicU64,
    join_max_output_rows: AtomicU64,
    join_max_output_width: AtomicU64,
    navigation_paths_planned: AtomicU64,
    navigation_paths_executed: AtomicU64,
    navigation_source_rows: AtomicU64,
    navigation_distinct_source_keys: AtomicU64,
    navigation_repeated_keys_eliminated: AtomicU64,
    navigation_lookup_batches: AtomicU64,
    navigation_lookup_hits: AtomicU64,
    navigation_lookup_misses: AtomicU64,
    navigation_direct_edges: AtomicU64,
    navigation_index_nested_loop_edges: AtomicU64,
    navigation_batch_edges: AtomicU64,
    navigation_hash_edges: AtomicU64,
    navigation_merge_edges: AtomicU64,
    navigation_fallback_edges: AtomicU64,
    navigation_planner_left_join_edges: AtomicU64,
    navigation_integrity_failures: AtomicU64,
    navigation_cancellations: AtomicU64,
    navigation_timeouts: AtomicU64,
    protocol_result_rows: AtomicU64,
    protocol_row_to_wire_values: AtomicU64,
    protocol_row_size_probe_calls: AtomicU64,
    protocol_row_size_probe_bytes: AtomicU64,
    protocol_row_batch_frames: AtomicU64,
    protocol_row_batch_probe_bytes: AtomicU64,
    protocol_column_batch_fallbacks: AtomicU64,
    protocol_column_batch_fallback_row_state: AtomicU64,
    protocol_column_batch_fallback_query_shape: AtomicU64,
    protocol_column_batch_fallback_storage_shape: AtomicU64,
    protocol_column_batch_fallback_schema: AtomicU64,
    protocol_column_batch_fallback_unknown: AtomicU64,
    protocol_column_batch_pending_opened: AtomicU64,
    protocol_column_batch_pending_completed: AtomicU64,
    protocol_column_batch_pending_dropped: AtomicU64,
    protocol_column_batch_pending_current: AtomicU64,
    protocol_column_batch_pending_max: AtomicU64,
    protocol_column_batch_pending_rows_current: AtomicU64,
    protocol_column_batch_pending_rows_max: AtomicU64,
    protocol_column_batch_pending_bytes_current: AtomicU64,
    protocol_column_batch_pending_bytes_max: AtomicU64,
    protocol_encode_calls: AtomicU64,
    protocol_encode_bytes: AtomicU64,
    protocol_encode_nanos: AtomicU64,
    protocol_socket_write_calls: AtomicU64,
    protocol_socket_write_bytes: AtomicU64,
    protocol_socket_write_nanos: AtomicU64,
    ram_accelerator_builds: AtomicU64,
    ram_accelerator_build_entries: AtomicU64,
    ram_accelerator_build_bytes: AtomicU64,
    ram_accelerator_build_nanos: AtomicU64,
    ram_accelerator_hits: AtomicU64,
    ram_accelerator_misses: AtomicU64,
    ram_accelerator_fallbacks: AtomicU64,
    ram_accelerator_evictions: AtomicU64,
    ram_accelerator_eviction_bytes: AtomicU64,
    wal_append_entries: AtomicU64,
    wal_append_bytes: AtomicU64,
    wal_write_calls: AtomicU64,
    wal_write_bytes: AtomicU64,
    wal_write_nanos: AtomicU64,
    wal_sync_calls: AtomicU64,
    wal_sync_nanos: AtomicU64,
    wal_generation_validation_calls: AtomicU64,
    wal_generation_validation_bytes: AtomicU64,
    wal_generation_validation_nanos: AtomicU64,
    wal_retention_calls: AtomicU64,
    wal_retention_identity_checks: AtomicU64,
    wal_retention_files_deleted: AtomicU64,
    wal_retention_nanos: AtomicU64,
    copy_calls: AtomicU64,
    copy_rows: AtomicU64,
    copy_parse_nanos: AtomicU64,
    copy_commit_nanos: AtomicU64,
    copy_total_nanos: AtomicU64,
    cold_constraint_batch_calls: AtomicU64,
    cold_constraint_batch_rows: AtomicU64,
    cold_constraint_batch_segments: AtomicU64,
    cold_constraint_batch_nanos: AtomicU64,
    cold_pk_batch_nanos: AtomicU64,
    compaction_spool_write_calls: AtomicU64,
    compaction_spool_write_bytes: AtomicU64,
    compaction_spool_write_nanos: AtomicU64,
    compaction_spool_read_calls: AtomicU64,
    compaction_spool_read_bytes: AtomicU64,
    compaction_spool_read_nanos: AtomicU64,
    posting_exact_lookup_calls: AtomicU64,
    posting_exact_lookup_segments: AtomicU64,
    posting_exact_lookup_max_segments: AtomicU64,
    posting_ordered_lookup_calls: AtomicU64,
    posting_ordered_lookup_segments: AtomicU64,
    posting_ordered_lookup_max_segments: AtomicU64,
    seal_calls: AtomicU64,
    seal_rows: AtomicU64,
    seal_bytes: AtomicU64,
    seal_output_bytes: AtomicU64,
    seal_nanos: AtomicU64,
    compaction_calls: AtomicU64,
    compaction_tables: AtomicU64,
    compaction_nanos: AtomicU64,
}

#[derive(Clone, Copy)]
struct RowMaterializationPending {
    calls: u64,
    rows: u64,
    values: u64,
}

#[derive(Clone, Copy)]
struct WalAppendPending {
    entries: u64,
    bytes: u64,
}

/// Per-row protocol adaptation is as hot as row materialization. Keep its
/// telemetry thread-local and flush at request boundaries so observability
/// never adds an atomic operation for every returned row.
#[derive(Clone, Copy)]
struct ProtocolRowAdapterPending {
    values: u64,
}

impl RowMaterializationPending {
    const EMPTY: Self = Self {
        calls: 0,
        rows: 0,
        values: 0,
    };
}

impl WalAppendPending {
    const EMPTY: Self = Self {
        entries: 0,
        bytes: 0,
    };
}

impl ProtocolRowAdapterPending {
    const EMPTY: Self = Self { values: 0 };
}

struct RowMaterializationPendingCell(Cell<RowMaterializationPending>);
struct WalAppendPendingCell(Cell<WalAppendPending>);
struct ProtocolRowAdapterPendingCell(Cell<ProtocolRowAdapterPending>);

impl Drop for RowMaterializationPendingCell {
    fn drop(&mut self) {
        flush_row_materialization_pending(self.0.get());
        self.0.set(RowMaterializationPending::EMPTY);
    }
}

impl Drop for WalAppendPendingCell {
    fn drop(&mut self) {
        flush_wal_append_pending(self.0.get());
        self.0.set(WalAppendPending::EMPTY);
    }
}

impl Drop for ProtocolRowAdapterPendingCell {
    fn drop(&mut self) {
        flush_protocol_row_adapter_pending(self.0.get());
        self.0.set(ProtocolRowAdapterPending::EMPTY);
    }
}

impl EngineCounters {
    const fn new() -> Self {
        Self {
            volume_read_calls: AtomicU64::new(0),
            volume_read_bytes: AtomicU64::new(0),
            volume_read_nanos: AtomicU64::new(0),
            decompression_calls: AtomicU64::new(0),
            decompression_compressed_bytes: AtomicU64::new(0),
            decompression_raw_bytes: AtomicU64::new(0),
            decompression_nanos: AtomicU64::new(0),
            row_materialization_calls: AtomicU64::new(0),
            row_materialization_rows: AtomicU64::new(0),
            row_materialization_values: AtomicU64::new(0),
            row_materialization_nanos: AtomicU64::new(0),
            derived_subquery_executes: AtomicU64::new(0),
            artifact_descriptor_open_calls: AtomicU64::new(0),
            artifact_file_open_calls: AtomicU64::new(0),
            artifact_file_stat_calls: AtomicU64::new(0),
            artifact_file_identity_checks: AtomicU64::new(0),
            artifact_fadvise_calls: AtomicU64::new(0),
            artifact_fadvise_errors: AtomicU64::new(0),
            artifact_pread_calls: AtomicU64::new(0),
            artifact_pread_bytes: AtomicU64::new(0),
            artifact_pread_nanos: AtomicU64::new(0),
            artifact_singleflight_leaders: AtomicU64::new(0),
            artifact_singleflight_followers: AtomicU64::new(0),
            artifact_payload_decompress_calls: AtomicU64::new(0),
            artifact_payload_decompress_compressed_bytes: AtomicU64::new(0),
            artifact_payload_decompress_raw_bytes: AtomicU64::new(0),
            artifact_payload_decompress_nanos: AtomicU64::new(0),
            artifact_column_deserialize_calls: AtomicU64::new(0),
            artifact_column_deserialize_bytes: AtomicU64::new(0),
            artifact_column_deserialize_rows: AtomicU64::new(0),
            artifact_column_deserialize_nanos: AtomicU64::new(0),
            artifact_columnar_group_applies: AtomicU64::new(0),
            artifact_columnar_group_row_groups: AtomicU64::new(0),
            artifact_columnar_group_selected_blocks: AtomicU64::new(0),
            artifact_columnar_group_input_rows: AtomicU64::new(0),
            artifact_columnar_group_output_groups: AtomicU64::new(0),
            artifact_columnar_group_direct_accumulators: AtomicU64::new(0),
            artifact_columnar_group_hash_accumulators: AtomicU64::new(0),
            artifact_columnar_group_local_merges: AtomicU64::new(0),
            artifact_columnar_group_merged_groups: AtomicU64::new(0),
            artifact_columnar_group_scheduler_runs: AtomicU64::new(0),
            artifact_columnar_group_scheduled_segments: AtomicU64::new(0),
            artifact_columnar_group_fallbacks: AtomicU64::new(0),
            artifact_columnar_group_fallback_row_state: AtomicU64::new(0),
            artifact_columnar_group_fallback_no_cold_artifact: AtomicU64::new(0),
            artifact_columnar_group_fallback_schema: AtomicU64::new(0),
            artifact_columnar_group_fallback_group_key: AtomicU64::new(0),
            artifact_columnar_group_fallback_aggregate: AtomicU64::new(0),
            artifact_columnar_group_fallback_visibility: AtomicU64::new(0),
            artifact_columnar_group_fallback_storage: AtomicU64::new(0),
            artifact_columnar_group_fallback_column_shape: AtomicU64::new(0),
            artifact_columnar_group_fallback_accumulator: AtomicU64::new(0),
            metadata_pk_count_attempts: AtomicU64::new(0),
            metadata_pk_count_applied: AtomicU64::new(0),
            metadata_pk_count_intervals: AtomicU64::new(0),
            metadata_pk_count_candidate_rows: AtomicU64::new(0),
            metadata_pk_count_visible_rows: AtomicU64::new(0),
            metadata_pk_count_visibility_exclusions: AtomicU64::new(0),
            metadata_pk_count_hot_candidates: AtomicU64::new(0),
            metadata_pk_count_fallbacks: AtomicU64::new(0),
            metadata_pk_count_fallback_snapshot: AtomicU64::new(0),
            metadata_pk_count_fallback_seal_overlap: AtomicU64::new(0),
            metadata_pk_count_fallback_unsupported: AtomicU64::new(0),
            metadata_pk_count_fallback_candidate_limit: AtomicU64::new(0),
            join_outer_rows: AtomicU64::new(0),
            join_key_rows: AtomicU64::new(0),
            join_pk_probe_batches: AtomicU64::new(0),
            join_pk_probe_keys: AtomicU64::new(0),
            join_pk_probe_hits: AtomicU64::new(0),
            join_parent_payload_rows: AtomicU64::new(0),
            join_rows_constructed: AtomicU64::new(0),
            join_rows_deferred: AtomicU64::new(0),
            join_deferred_rows_consumed: AtomicU64::new(0),
            join_operator_calls: AtomicU64::new(0),
            join_hash_streaming_calls: AtomicU64::new(0),
            join_hash_parallel_calls: AtomicU64::new(0),
            join_merge_calls: AtomicU64::new(0),
            join_nested_loop_calls: AtomicU64::new(0),
            join_index_nested_loop_calls: AtomicU64::new(0),
            join_batch_index_nested_loop_calls: AtomicU64::new(0),
            join_left_input_rows: AtomicU64::new(0),
            join_right_input_rows: AtomicU64::new(0),
            join_output_rows: AtomicU64::new(0),
            join_input_values: AtomicU64::new(0),
            join_output_values: AtomicU64::new(0),
            join_candidate_pairs: AtomicU64::new(0),
            join_lookup_calls: AtomicU64::new(0),
            join_lookup_candidate_rows: AtomicU64::new(0),
            join_wall_nanos: AtomicU64::new(0),
            join_max_left_input_rows: AtomicU64::new(0),
            join_max_right_input_rows: AtomicU64::new(0),
            join_max_output_rows: AtomicU64::new(0),
            join_max_output_width: AtomicU64::new(0),
            navigation_paths_planned: AtomicU64::new(0),
            navigation_paths_executed: AtomicU64::new(0),
            navigation_source_rows: AtomicU64::new(0),
            navigation_distinct_source_keys: AtomicU64::new(0),
            navigation_repeated_keys_eliminated: AtomicU64::new(0),
            navigation_lookup_batches: AtomicU64::new(0),
            navigation_lookup_hits: AtomicU64::new(0),
            navigation_lookup_misses: AtomicU64::new(0),
            navigation_direct_edges: AtomicU64::new(0),
            navigation_index_nested_loop_edges: AtomicU64::new(0),
            navigation_batch_edges: AtomicU64::new(0),
            navigation_hash_edges: AtomicU64::new(0),
            navigation_merge_edges: AtomicU64::new(0),
            navigation_fallback_edges: AtomicU64::new(0),
            navigation_planner_left_join_edges: AtomicU64::new(0),
            navigation_integrity_failures: AtomicU64::new(0),
            navigation_cancellations: AtomicU64::new(0),
            navigation_timeouts: AtomicU64::new(0),
            protocol_result_rows: AtomicU64::new(0),
            protocol_row_to_wire_values: AtomicU64::new(0),
            protocol_row_size_probe_calls: AtomicU64::new(0),
            protocol_row_size_probe_bytes: AtomicU64::new(0),
            protocol_row_batch_frames: AtomicU64::new(0),
            protocol_row_batch_probe_bytes: AtomicU64::new(0),
            protocol_column_batch_fallbacks: AtomicU64::new(0),
            protocol_column_batch_fallback_row_state: AtomicU64::new(0),
            protocol_column_batch_fallback_query_shape: AtomicU64::new(0),
            protocol_column_batch_fallback_storage_shape: AtomicU64::new(0),
            protocol_column_batch_fallback_schema: AtomicU64::new(0),
            protocol_column_batch_fallback_unknown: AtomicU64::new(0),
            protocol_column_batch_pending_opened: AtomicU64::new(0),
            protocol_column_batch_pending_completed: AtomicU64::new(0),
            protocol_column_batch_pending_dropped: AtomicU64::new(0),
            protocol_column_batch_pending_current: AtomicU64::new(0),
            protocol_column_batch_pending_max: AtomicU64::new(0),
            protocol_column_batch_pending_rows_current: AtomicU64::new(0),
            protocol_column_batch_pending_rows_max: AtomicU64::new(0),
            protocol_column_batch_pending_bytes_current: AtomicU64::new(0),
            protocol_column_batch_pending_bytes_max: AtomicU64::new(0),
            protocol_encode_calls: AtomicU64::new(0),
            protocol_encode_bytes: AtomicU64::new(0),
            protocol_encode_nanos: AtomicU64::new(0),
            protocol_socket_write_calls: AtomicU64::new(0),
            protocol_socket_write_bytes: AtomicU64::new(0),
            protocol_socket_write_nanos: AtomicU64::new(0),
            ram_accelerator_builds: AtomicU64::new(0),
            ram_accelerator_build_entries: AtomicU64::new(0),
            ram_accelerator_build_bytes: AtomicU64::new(0),
            ram_accelerator_build_nanos: AtomicU64::new(0),
            ram_accelerator_hits: AtomicU64::new(0),
            ram_accelerator_misses: AtomicU64::new(0),
            ram_accelerator_fallbacks: AtomicU64::new(0),
            ram_accelerator_evictions: AtomicU64::new(0),
            ram_accelerator_eviction_bytes: AtomicU64::new(0),
            wal_append_entries: AtomicU64::new(0),
            wal_append_bytes: AtomicU64::new(0),
            wal_write_calls: AtomicU64::new(0),
            wal_write_bytes: AtomicU64::new(0),
            wal_write_nanos: AtomicU64::new(0),
            wal_sync_calls: AtomicU64::new(0),
            wal_sync_nanos: AtomicU64::new(0),
            wal_generation_validation_calls: AtomicU64::new(0),
            wal_generation_validation_bytes: AtomicU64::new(0),
            wal_generation_validation_nanos: AtomicU64::new(0),
            wal_retention_calls: AtomicU64::new(0),
            wal_retention_identity_checks: AtomicU64::new(0),
            wal_retention_files_deleted: AtomicU64::new(0),
            wal_retention_nanos: AtomicU64::new(0),
            copy_calls: AtomicU64::new(0),
            copy_rows: AtomicU64::new(0),
            copy_parse_nanos: AtomicU64::new(0),
            copy_commit_nanos: AtomicU64::new(0),
            copy_total_nanos: AtomicU64::new(0),
            cold_constraint_batch_calls: AtomicU64::new(0),
            cold_constraint_batch_rows: AtomicU64::new(0),
            cold_constraint_batch_segments: AtomicU64::new(0),
            cold_constraint_batch_nanos: AtomicU64::new(0),
            cold_pk_batch_nanos: AtomicU64::new(0),
            compaction_spool_write_calls: AtomicU64::new(0),
            compaction_spool_write_bytes: AtomicU64::new(0),
            compaction_spool_write_nanos: AtomicU64::new(0),
            compaction_spool_read_calls: AtomicU64::new(0),
            compaction_spool_read_bytes: AtomicU64::new(0),
            compaction_spool_read_nanos: AtomicU64::new(0),
            posting_exact_lookup_calls: AtomicU64::new(0),
            posting_exact_lookup_segments: AtomicU64::new(0),
            posting_exact_lookup_max_segments: AtomicU64::new(0),
            posting_ordered_lookup_calls: AtomicU64::new(0),
            posting_ordered_lookup_segments: AtomicU64::new(0),
            posting_ordered_lookup_max_segments: AtomicU64::new(0),
            seal_calls: AtomicU64::new(0),
            seal_rows: AtomicU64::new(0),
            seal_bytes: AtomicU64::new(0),
            seal_output_bytes: AtomicU64::new(0),
            seal_nanos: AtomicU64::new(0),
            compaction_calls: AtomicU64::new(0),
            compaction_tables: AtomicU64::new(0),
            compaction_nanos: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.volume_read_calls.store(0, Ordering::Relaxed);
        self.volume_read_bytes.store(0, Ordering::Relaxed);
        self.volume_read_nanos.store(0, Ordering::Relaxed);
        self.decompression_calls.store(0, Ordering::Relaxed);
        self.decompression_compressed_bytes
            .store(0, Ordering::Relaxed);
        self.decompression_raw_bytes.store(0, Ordering::Relaxed);
        self.decompression_nanos.store(0, Ordering::Relaxed);
        self.row_materialization_calls.store(0, Ordering::Relaxed);
        self.row_materialization_rows.store(0, Ordering::Relaxed);
        self.row_materialization_values.store(0, Ordering::Relaxed);
        self.row_materialization_nanos.store(0, Ordering::Relaxed);
        self.derived_subquery_executes.store(0, Ordering::Relaxed);
        self.artifact_descriptor_open_calls
            .store(0, Ordering::Relaxed);
        self.artifact_file_open_calls.store(0, Ordering::Relaxed);
        self.artifact_file_stat_calls.store(0, Ordering::Relaxed);
        self.artifact_file_identity_checks
            .store(0, Ordering::Relaxed);
        self.artifact_fadvise_calls.store(0, Ordering::Relaxed);
        self.artifact_fadvise_errors.store(0, Ordering::Relaxed);
        self.artifact_pread_calls.store(0, Ordering::Relaxed);
        self.artifact_pread_bytes.store(0, Ordering::Relaxed);
        self.artifact_pread_nanos.store(0, Ordering::Relaxed);
        self.artifact_singleflight_leaders
            .store(0, Ordering::Relaxed);
        self.artifact_singleflight_followers
            .store(0, Ordering::Relaxed);
        self.artifact_payload_decompress_calls
            .store(0, Ordering::Relaxed);
        self.artifact_payload_decompress_compressed_bytes
            .store(0, Ordering::Relaxed);
        self.artifact_payload_decompress_raw_bytes
            .store(0, Ordering::Relaxed);
        self.artifact_payload_decompress_nanos
            .store(0, Ordering::Relaxed);
        self.artifact_column_deserialize_calls
            .store(0, Ordering::Relaxed);
        self.artifact_column_deserialize_bytes
            .store(0, Ordering::Relaxed);
        self.artifact_column_deserialize_rows
            .store(0, Ordering::Relaxed);
        self.artifact_column_deserialize_nanos
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_applies
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_row_groups
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_selected_blocks
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_input_rows
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_output_groups
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_direct_accumulators
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_hash_accumulators
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_local_merges
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_merged_groups
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_scheduler_runs
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_scheduled_segments
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallbacks
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_row_state
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_no_cold_artifact
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_schema
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_group_key
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_aggregate
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_visibility
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_storage
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_column_shape
            .store(0, Ordering::Relaxed);
        self.artifact_columnar_group_fallback_accumulator
            .store(0, Ordering::Relaxed);
        self.metadata_pk_count_attempts.store(0, Ordering::Relaxed);
        self.metadata_pk_count_applied.store(0, Ordering::Relaxed);
        self.metadata_pk_count_intervals.store(0, Ordering::Relaxed);
        self.metadata_pk_count_candidate_rows
            .store(0, Ordering::Relaxed);
        self.metadata_pk_count_visible_rows
            .store(0, Ordering::Relaxed);
        self.metadata_pk_count_visibility_exclusions
            .store(0, Ordering::Relaxed);
        self.metadata_pk_count_hot_candidates
            .store(0, Ordering::Relaxed);
        self.metadata_pk_count_fallbacks.store(0, Ordering::Relaxed);
        self.metadata_pk_count_fallback_snapshot
            .store(0, Ordering::Relaxed);
        self.metadata_pk_count_fallback_seal_overlap
            .store(0, Ordering::Relaxed);
        self.metadata_pk_count_fallback_unsupported
            .store(0, Ordering::Relaxed);
        self.metadata_pk_count_fallback_candidate_limit
            .store(0, Ordering::Relaxed);
        self.join_outer_rows.store(0, Ordering::Relaxed);
        self.join_key_rows.store(0, Ordering::Relaxed);
        self.join_pk_probe_batches.store(0, Ordering::Relaxed);
        self.join_pk_probe_keys.store(0, Ordering::Relaxed);
        self.join_pk_probe_hits.store(0, Ordering::Relaxed);
        self.join_parent_payload_rows.store(0, Ordering::Relaxed);
        self.join_rows_constructed.store(0, Ordering::Relaxed);
        self.join_rows_deferred.store(0, Ordering::Relaxed);
        self.join_deferred_rows_consumed.store(0, Ordering::Relaxed);
        self.join_operator_calls.store(0, Ordering::Relaxed);
        self.join_hash_streaming_calls.store(0, Ordering::Relaxed);
        self.join_hash_parallel_calls.store(0, Ordering::Relaxed);
        self.join_merge_calls.store(0, Ordering::Relaxed);
        self.join_nested_loop_calls.store(0, Ordering::Relaxed);
        self.join_index_nested_loop_calls
            .store(0, Ordering::Relaxed);
        self.join_batch_index_nested_loop_calls
            .store(0, Ordering::Relaxed);
        self.join_left_input_rows.store(0, Ordering::Relaxed);
        self.join_right_input_rows.store(0, Ordering::Relaxed);
        self.join_output_rows.store(0, Ordering::Relaxed);
        self.join_input_values.store(0, Ordering::Relaxed);
        self.join_output_values.store(0, Ordering::Relaxed);
        self.join_candidate_pairs.store(0, Ordering::Relaxed);
        self.join_lookup_calls.store(0, Ordering::Relaxed);
        self.join_lookup_candidate_rows.store(0, Ordering::Relaxed);
        self.join_wall_nanos.store(0, Ordering::Relaxed);
        self.join_max_left_input_rows.store(0, Ordering::Relaxed);
        self.join_max_right_input_rows.store(0, Ordering::Relaxed);
        self.join_max_output_rows.store(0, Ordering::Relaxed);
        self.join_max_output_width.store(0, Ordering::Relaxed);
        self.navigation_paths_planned.store(0, Ordering::Relaxed);
        self.navigation_paths_executed.store(0, Ordering::Relaxed);
        self.navigation_source_rows.store(0, Ordering::Relaxed);
        self.navigation_distinct_source_keys
            .store(0, Ordering::Relaxed);
        self.navigation_repeated_keys_eliminated
            .store(0, Ordering::Relaxed);
        self.navigation_lookup_batches.store(0, Ordering::Relaxed);
        self.navigation_lookup_hits.store(0, Ordering::Relaxed);
        self.navigation_lookup_misses.store(0, Ordering::Relaxed);
        self.navigation_direct_edges.store(0, Ordering::Relaxed);
        self.navigation_index_nested_loop_edges
            .store(0, Ordering::Relaxed);
        self.navigation_batch_edges.store(0, Ordering::Relaxed);
        self.navigation_hash_edges.store(0, Ordering::Relaxed);
        self.navigation_merge_edges.store(0, Ordering::Relaxed);
        self.navigation_fallback_edges.store(0, Ordering::Relaxed);
        self.navigation_planner_left_join_edges
            .store(0, Ordering::Relaxed);
        self.navigation_integrity_failures
            .store(0, Ordering::Relaxed);
        self.navigation_cancellations.store(0, Ordering::Relaxed);
        self.navigation_timeouts.store(0, Ordering::Relaxed);
        self.protocol_result_rows.store(0, Ordering::Relaxed);
        self.protocol_row_to_wire_values.store(0, Ordering::Relaxed);
        self.protocol_row_size_probe_calls
            .store(0, Ordering::Relaxed);
        self.protocol_row_size_probe_bytes
            .store(0, Ordering::Relaxed);
        self.protocol_row_batch_frames.store(0, Ordering::Relaxed);
        self.protocol_row_batch_probe_bytes
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_fallbacks
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_fallback_row_state
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_fallback_query_shape
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_fallback_storage_shape
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_fallback_schema
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_fallback_unknown
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_opened
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_completed
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_dropped
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_current
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_max
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_rows_current
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_rows_max
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_bytes_current
            .store(0, Ordering::Relaxed);
        self.protocol_column_batch_pending_bytes_max
            .store(0, Ordering::Relaxed);
        self.protocol_encode_calls.store(0, Ordering::Relaxed);
        self.protocol_encode_bytes.store(0, Ordering::Relaxed);
        self.protocol_encode_nanos.store(0, Ordering::Relaxed);
        self.protocol_socket_write_calls.store(0, Ordering::Relaxed);
        self.protocol_socket_write_bytes.store(0, Ordering::Relaxed);
        self.protocol_socket_write_nanos.store(0, Ordering::Relaxed);
        self.ram_accelerator_builds.store(0, Ordering::Relaxed);
        self.ram_accelerator_build_entries
            .store(0, Ordering::Relaxed);
        self.ram_accelerator_build_bytes.store(0, Ordering::Relaxed);
        self.ram_accelerator_build_nanos.store(0, Ordering::Relaxed);
        self.ram_accelerator_hits.store(0, Ordering::Relaxed);
        self.ram_accelerator_misses.store(0, Ordering::Relaxed);
        self.ram_accelerator_fallbacks.store(0, Ordering::Relaxed);
        self.ram_accelerator_evictions.store(0, Ordering::Relaxed);
        self.ram_accelerator_eviction_bytes
            .store(0, Ordering::Relaxed);
        self.wal_append_entries.store(0, Ordering::Relaxed);
        self.wal_append_bytes.store(0, Ordering::Relaxed);
        self.wal_write_calls.store(0, Ordering::Relaxed);
        self.wal_write_bytes.store(0, Ordering::Relaxed);
        self.wal_write_nanos.store(0, Ordering::Relaxed);
        self.wal_sync_calls.store(0, Ordering::Relaxed);
        self.wal_sync_nanos.store(0, Ordering::Relaxed);
        self.wal_generation_validation_calls
            .store(0, Ordering::Relaxed);
        self.wal_generation_validation_bytes
            .store(0, Ordering::Relaxed);
        self.wal_generation_validation_nanos
            .store(0, Ordering::Relaxed);
        self.wal_retention_calls.store(0, Ordering::Relaxed);
        self.wal_retention_identity_checks
            .store(0, Ordering::Relaxed);
        self.wal_retention_files_deleted.store(0, Ordering::Relaxed);
        self.wal_retention_nanos.store(0, Ordering::Relaxed);
        self.copy_calls.store(0, Ordering::Relaxed);
        self.copy_rows.store(0, Ordering::Relaxed);
        self.copy_parse_nanos.store(0, Ordering::Relaxed);
        self.copy_commit_nanos.store(0, Ordering::Relaxed);
        self.copy_total_nanos.store(0, Ordering::Relaxed);
        self.cold_constraint_batch_calls.store(0, Ordering::Relaxed);
        self.cold_constraint_batch_rows.store(0, Ordering::Relaxed);
        self.cold_constraint_batch_segments
            .store(0, Ordering::Relaxed);
        self.cold_constraint_batch_nanos.store(0, Ordering::Relaxed);
        self.cold_pk_batch_nanos.store(0, Ordering::Relaxed);
        self.compaction_spool_write_calls
            .store(0, Ordering::Relaxed);
        self.compaction_spool_write_bytes
            .store(0, Ordering::Relaxed);
        self.compaction_spool_write_nanos
            .store(0, Ordering::Relaxed);
        self.compaction_spool_read_calls.store(0, Ordering::Relaxed);
        self.compaction_spool_read_bytes.store(0, Ordering::Relaxed);
        self.compaction_spool_read_nanos.store(0, Ordering::Relaxed);
        self.posting_exact_lookup_calls.store(0, Ordering::Relaxed);
        self.posting_exact_lookup_segments
            .store(0, Ordering::Relaxed);
        self.posting_exact_lookup_max_segments
            .store(0, Ordering::Relaxed);
        self.posting_ordered_lookup_calls
            .store(0, Ordering::Relaxed);
        self.posting_ordered_lookup_segments
            .store(0, Ordering::Relaxed);
        self.posting_ordered_lookup_max_segments
            .store(0, Ordering::Relaxed);
        self.seal_calls.store(0, Ordering::Relaxed);
        self.seal_rows.store(0, Ordering::Relaxed);
        self.seal_bytes.store(0, Ordering::Relaxed);
        self.seal_output_bytes.store(0, Ordering::Relaxed);
        self.seal_nanos.store(0, Ordering::Relaxed);
        self.compaction_calls.store(0, Ordering::Relaxed);
        self.compaction_tables.store(0, Ordering::Relaxed);
        self.compaction_nanos.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> EngineCountersSnapshot {
        EngineCountersSnapshot {
            runtime_profile: runtime_profile::runtime_profile_snapshot(),
            volume_read_calls: self.volume_read_calls.load(Ordering::Relaxed),
            volume_read_bytes: self.volume_read_bytes.load(Ordering::Relaxed),
            volume_read_nanos: self.volume_read_nanos.load(Ordering::Relaxed),
            decompression_calls: self.decompression_calls.load(Ordering::Relaxed),
            decompression_compressed_bytes: self
                .decompression_compressed_bytes
                .load(Ordering::Relaxed),
            decompression_raw_bytes: self.decompression_raw_bytes.load(Ordering::Relaxed),
            decompression_nanos: self.decompression_nanos.load(Ordering::Relaxed),
            row_materialization_calls: self.row_materialization_calls.load(Ordering::Relaxed),
            row_materialization_rows: self.row_materialization_rows.load(Ordering::Relaxed),
            row_materialization_values: self.row_materialization_values.load(Ordering::Relaxed),
            row_materialization_nanos: self.row_materialization_nanos.load(Ordering::Relaxed),
            derived_subquery_executes: self.derived_subquery_executes.load(Ordering::Relaxed),
            artifact_descriptor_open_calls: self
                .artifact_descriptor_open_calls
                .load(Ordering::Relaxed),
            artifact_file_open_calls: self.artifact_file_open_calls.load(Ordering::Relaxed),
            artifact_file_stat_calls: self.artifact_file_stat_calls.load(Ordering::Relaxed),
            artifact_file_identity_checks: self
                .artifact_file_identity_checks
                .load(Ordering::Relaxed),
            artifact_fadvise_calls: self.artifact_fadvise_calls.load(Ordering::Relaxed),
            artifact_fadvise_errors: self.artifact_fadvise_errors.load(Ordering::Relaxed),
            artifact_pread_calls: self.artifact_pread_calls.load(Ordering::Relaxed),
            artifact_pread_bytes: self.artifact_pread_bytes.load(Ordering::Relaxed),
            artifact_pread_nanos: self.artifact_pread_nanos.load(Ordering::Relaxed),
            artifact_singleflight_leaders: self
                .artifact_singleflight_leaders
                .load(Ordering::Relaxed),
            artifact_singleflight_followers: self
                .artifact_singleflight_followers
                .load(Ordering::Relaxed),
            artifact_payload_decompress_calls: self
                .artifact_payload_decompress_calls
                .load(Ordering::Relaxed),
            artifact_payload_decompress_compressed_bytes: self
                .artifact_payload_decompress_compressed_bytes
                .load(Ordering::Relaxed),
            artifact_payload_decompress_raw_bytes: self
                .artifact_payload_decompress_raw_bytes
                .load(Ordering::Relaxed),
            artifact_payload_decompress_nanos: self
                .artifact_payload_decompress_nanos
                .load(Ordering::Relaxed),
            artifact_column_deserialize_calls: self
                .artifact_column_deserialize_calls
                .load(Ordering::Relaxed),
            artifact_column_deserialize_bytes: self
                .artifact_column_deserialize_bytes
                .load(Ordering::Relaxed),
            artifact_column_deserialize_rows: self
                .artifact_column_deserialize_rows
                .load(Ordering::Relaxed),
            artifact_column_deserialize_nanos: self
                .artifact_column_deserialize_nanos
                .load(Ordering::Relaxed),
            artifact_columnar_group_applies: self
                .artifact_columnar_group_applies
                .load(Ordering::Relaxed),
            artifact_columnar_group_row_groups: self
                .artifact_columnar_group_row_groups
                .load(Ordering::Relaxed),
            artifact_columnar_group_selected_blocks: self
                .artifact_columnar_group_selected_blocks
                .load(Ordering::Relaxed),
            artifact_columnar_group_input_rows: self
                .artifact_columnar_group_input_rows
                .load(Ordering::Relaxed),
            artifact_columnar_group_output_groups: self
                .artifact_columnar_group_output_groups
                .load(Ordering::Relaxed),
            artifact_columnar_group_direct_accumulators: self
                .artifact_columnar_group_direct_accumulators
                .load(Ordering::Relaxed),
            artifact_columnar_group_hash_accumulators: self
                .artifact_columnar_group_hash_accumulators
                .load(Ordering::Relaxed),
            artifact_columnar_group_local_merges: self
                .artifact_columnar_group_local_merges
                .load(Ordering::Relaxed),
            artifact_columnar_group_merged_groups: self
                .artifact_columnar_group_merged_groups
                .load(Ordering::Relaxed),
            artifact_columnar_group_scheduler_runs: self
                .artifact_columnar_group_scheduler_runs
                .load(Ordering::Relaxed),
            artifact_columnar_group_scheduled_segments: self
                .artifact_columnar_group_scheduled_segments
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallbacks: self
                .artifact_columnar_group_fallbacks
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_row_state: self
                .artifact_columnar_group_fallback_row_state
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_no_cold_artifact: self
                .artifact_columnar_group_fallback_no_cold_artifact
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_schema: self
                .artifact_columnar_group_fallback_schema
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_group_key: self
                .artifact_columnar_group_fallback_group_key
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_aggregate: self
                .artifact_columnar_group_fallback_aggregate
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_visibility: self
                .artifact_columnar_group_fallback_visibility
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_storage: self
                .artifact_columnar_group_fallback_storage
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_column_shape: self
                .artifact_columnar_group_fallback_column_shape
                .load(Ordering::Relaxed),
            artifact_columnar_group_fallback_accumulator: self
                .artifact_columnar_group_fallback_accumulator
                .load(Ordering::Relaxed),
            metadata_pk_count_attempts: self.metadata_pk_count_attempts.load(Ordering::Relaxed),
            metadata_pk_count_applied: self.metadata_pk_count_applied.load(Ordering::Relaxed),
            metadata_pk_count_intervals: self.metadata_pk_count_intervals.load(Ordering::Relaxed),
            metadata_pk_count_candidate_rows: self
                .metadata_pk_count_candidate_rows
                .load(Ordering::Relaxed),
            metadata_pk_count_visible_rows: self
                .metadata_pk_count_visible_rows
                .load(Ordering::Relaxed),
            metadata_pk_count_visibility_exclusions: self
                .metadata_pk_count_visibility_exclusions
                .load(Ordering::Relaxed),
            metadata_pk_count_hot_candidates: self
                .metadata_pk_count_hot_candidates
                .load(Ordering::Relaxed),
            metadata_pk_count_fallbacks: self.metadata_pk_count_fallbacks.load(Ordering::Relaxed),
            metadata_pk_count_fallback_snapshot: self
                .metadata_pk_count_fallback_snapshot
                .load(Ordering::Relaxed),
            metadata_pk_count_fallback_seal_overlap: self
                .metadata_pk_count_fallback_seal_overlap
                .load(Ordering::Relaxed),
            metadata_pk_count_fallback_unsupported: self
                .metadata_pk_count_fallback_unsupported
                .load(Ordering::Relaxed),
            metadata_pk_count_fallback_candidate_limit: self
                .metadata_pk_count_fallback_candidate_limit
                .load(Ordering::Relaxed),
            join_outer_rows: self.join_outer_rows.load(Ordering::Relaxed),
            join_key_rows: self.join_key_rows.load(Ordering::Relaxed),
            join_pk_probe_batches: self.join_pk_probe_batches.load(Ordering::Relaxed),
            join_pk_probe_keys: self.join_pk_probe_keys.load(Ordering::Relaxed),
            join_pk_probe_hits: self.join_pk_probe_hits.load(Ordering::Relaxed),
            join_parent_payload_rows: self.join_parent_payload_rows.load(Ordering::Relaxed),
            join_rows_constructed: self.join_rows_constructed.load(Ordering::Relaxed),
            join_rows_deferred: self.join_rows_deferred.load(Ordering::Relaxed),
            join_deferred_rows_consumed: self.join_deferred_rows_consumed.load(Ordering::Relaxed),
            join_operator_calls: self.join_operator_calls.load(Ordering::Relaxed),
            join_hash_streaming_calls: self.join_hash_streaming_calls.load(Ordering::Relaxed),
            join_hash_parallel_calls: self.join_hash_parallel_calls.load(Ordering::Relaxed),
            join_merge_calls: self.join_merge_calls.load(Ordering::Relaxed),
            join_nested_loop_calls: self.join_nested_loop_calls.load(Ordering::Relaxed),
            join_index_nested_loop_calls: self.join_index_nested_loop_calls.load(Ordering::Relaxed),
            join_batch_index_nested_loop_calls: self
                .join_batch_index_nested_loop_calls
                .load(Ordering::Relaxed),
            join_left_input_rows: self.join_left_input_rows.load(Ordering::Relaxed),
            join_right_input_rows: self.join_right_input_rows.load(Ordering::Relaxed),
            join_output_rows: self.join_output_rows.load(Ordering::Relaxed),
            join_input_values: self.join_input_values.load(Ordering::Relaxed),
            join_output_values: self.join_output_values.load(Ordering::Relaxed),
            join_candidate_pairs: self.join_candidate_pairs.load(Ordering::Relaxed),
            join_lookup_calls: self.join_lookup_calls.load(Ordering::Relaxed),
            join_lookup_candidate_rows: self.join_lookup_candidate_rows.load(Ordering::Relaxed),
            join_wall_nanos: self.join_wall_nanos.load(Ordering::Relaxed),
            join_max_left_input_rows: self.join_max_left_input_rows.load(Ordering::Relaxed),
            join_max_right_input_rows: self.join_max_right_input_rows.load(Ordering::Relaxed),
            join_max_output_rows: self.join_max_output_rows.load(Ordering::Relaxed),
            join_max_output_width: self.join_max_output_width.load(Ordering::Relaxed),
            navigation_paths_planned: self.navigation_paths_planned.load(Ordering::Relaxed),
            navigation_paths_executed: self.navigation_paths_executed.load(Ordering::Relaxed),
            navigation_source_rows: self.navigation_source_rows.load(Ordering::Relaxed),
            navigation_distinct_source_keys: self
                .navigation_distinct_source_keys
                .load(Ordering::Relaxed),
            navigation_repeated_keys_eliminated: self
                .navigation_repeated_keys_eliminated
                .load(Ordering::Relaxed),
            navigation_lookup_batches: self.navigation_lookup_batches.load(Ordering::Relaxed),
            navigation_lookup_hits: self.navigation_lookup_hits.load(Ordering::Relaxed),
            navigation_lookup_misses: self.navigation_lookup_misses.load(Ordering::Relaxed),
            navigation_direct_edges: self.navigation_direct_edges.load(Ordering::Relaxed),
            navigation_index_nested_loop_edges: self
                .navigation_index_nested_loop_edges
                .load(Ordering::Relaxed),
            navigation_batch_edges: self.navigation_batch_edges.load(Ordering::Relaxed),
            navigation_hash_edges: self.navigation_hash_edges.load(Ordering::Relaxed),
            navigation_merge_edges: self.navigation_merge_edges.load(Ordering::Relaxed),
            navigation_fallback_edges: self.navigation_fallback_edges.load(Ordering::Relaxed),
            navigation_planner_left_join_edges: self
                .navigation_planner_left_join_edges
                .load(Ordering::Relaxed),
            navigation_integrity_failures: self
                .navigation_integrity_failures
                .load(Ordering::Relaxed),
            navigation_cancellations: self.navigation_cancellations.load(Ordering::Relaxed),
            navigation_timeouts: self.navigation_timeouts.load(Ordering::Relaxed),
            protocol_result_rows: self.protocol_result_rows.load(Ordering::Relaxed),
            protocol_row_to_wire_values: self.protocol_row_to_wire_values.load(Ordering::Relaxed),
            protocol_row_size_probe_calls: self
                .protocol_row_size_probe_calls
                .load(Ordering::Relaxed),
            protocol_row_size_probe_bytes: self
                .protocol_row_size_probe_bytes
                .load(Ordering::Relaxed),
            protocol_row_batch_frames: self.protocol_row_batch_frames.load(Ordering::Relaxed),
            protocol_row_batch_probe_bytes: self
                .protocol_row_batch_probe_bytes
                .load(Ordering::Relaxed),
            protocol_column_batch_fallbacks: self
                .protocol_column_batch_fallbacks
                .load(Ordering::Relaxed),
            protocol_column_batch_fallback_row_state: self
                .protocol_column_batch_fallback_row_state
                .load(Ordering::Relaxed),
            protocol_column_batch_fallback_query_shape: self
                .protocol_column_batch_fallback_query_shape
                .load(Ordering::Relaxed),
            protocol_column_batch_fallback_storage_shape: self
                .protocol_column_batch_fallback_storage_shape
                .load(Ordering::Relaxed),
            protocol_column_batch_fallback_schema: self
                .protocol_column_batch_fallback_schema
                .load(Ordering::Relaxed),
            protocol_column_batch_fallback_unknown: self
                .protocol_column_batch_fallback_unknown
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_opened: self
                .protocol_column_batch_pending_opened
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_completed: self
                .protocol_column_batch_pending_completed
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_dropped: self
                .protocol_column_batch_pending_dropped
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_current: self
                .protocol_column_batch_pending_current
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_max: self
                .protocol_column_batch_pending_max
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_rows_current: self
                .protocol_column_batch_pending_rows_current
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_rows_max: self
                .protocol_column_batch_pending_rows_max
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_bytes_current: self
                .protocol_column_batch_pending_bytes_current
                .load(Ordering::Relaxed),
            protocol_column_batch_pending_bytes_max: self
                .protocol_column_batch_pending_bytes_max
                .load(Ordering::Relaxed),
            protocol_encode_calls: self.protocol_encode_calls.load(Ordering::Relaxed),
            protocol_encode_bytes: self.protocol_encode_bytes.load(Ordering::Relaxed),
            protocol_encode_nanos: self.protocol_encode_nanos.load(Ordering::Relaxed),
            protocol_socket_write_calls: self.protocol_socket_write_calls.load(Ordering::Relaxed),
            protocol_socket_write_bytes: self.protocol_socket_write_bytes.load(Ordering::Relaxed),
            protocol_socket_write_nanos: self.protocol_socket_write_nanos.load(Ordering::Relaxed),
            ram_accelerator_builds: self.ram_accelerator_builds.load(Ordering::Relaxed),
            ram_accelerator_build_entries: self
                .ram_accelerator_build_entries
                .load(Ordering::Relaxed),
            ram_accelerator_build_bytes: self.ram_accelerator_build_bytes.load(Ordering::Relaxed),
            ram_accelerator_build_nanos: self.ram_accelerator_build_nanos.load(Ordering::Relaxed),
            ram_accelerator_hits: self.ram_accelerator_hits.load(Ordering::Relaxed),
            ram_accelerator_misses: self.ram_accelerator_misses.load(Ordering::Relaxed),
            ram_accelerator_fallbacks: self.ram_accelerator_fallbacks.load(Ordering::Relaxed),
            ram_accelerator_evictions: self.ram_accelerator_evictions.load(Ordering::Relaxed),
            ram_accelerator_eviction_bytes: self
                .ram_accelerator_eviction_bytes
                .load(Ordering::Relaxed),
            wal_append_entries: self.wal_append_entries.load(Ordering::Relaxed),
            wal_append_bytes: self.wal_append_bytes.load(Ordering::Relaxed),
            wal_write_calls: self.wal_write_calls.load(Ordering::Relaxed),
            wal_write_bytes: self.wal_write_bytes.load(Ordering::Relaxed),
            wal_write_nanos: self.wal_write_nanos.load(Ordering::Relaxed),
            wal_sync_calls: self.wal_sync_calls.load(Ordering::Relaxed),
            wal_sync_nanos: self.wal_sync_nanos.load(Ordering::Relaxed),
            wal_generation_validation_calls: self
                .wal_generation_validation_calls
                .load(Ordering::Relaxed),
            wal_generation_validation_bytes: self
                .wal_generation_validation_bytes
                .load(Ordering::Relaxed),
            wal_generation_validation_nanos: self
                .wal_generation_validation_nanos
                .load(Ordering::Relaxed),
            wal_retention_calls: self.wal_retention_calls.load(Ordering::Relaxed),
            wal_retention_identity_checks: self
                .wal_retention_identity_checks
                .load(Ordering::Relaxed),
            wal_retention_files_deleted: self.wal_retention_files_deleted.load(Ordering::Relaxed),
            wal_retention_nanos: self.wal_retention_nanos.load(Ordering::Relaxed),
            copy_calls: self.copy_calls.load(Ordering::Relaxed),
            copy_rows: self.copy_rows.load(Ordering::Relaxed),
            copy_parse_nanos: self.copy_parse_nanos.load(Ordering::Relaxed),
            copy_commit_nanos: self.copy_commit_nanos.load(Ordering::Relaxed),
            copy_total_nanos: self.copy_total_nanos.load(Ordering::Relaxed),
            cold_constraint_batch_calls: self.cold_constraint_batch_calls.load(Ordering::Relaxed),
            cold_constraint_batch_rows: self.cold_constraint_batch_rows.load(Ordering::Relaxed),
            cold_constraint_batch_segments: self
                .cold_constraint_batch_segments
                .load(Ordering::Relaxed),
            cold_constraint_batch_nanos: self.cold_constraint_batch_nanos.load(Ordering::Relaxed),
            cold_pk_batch_nanos: self.cold_pk_batch_nanos.load(Ordering::Relaxed),
            compaction_spool_write_calls: self.compaction_spool_write_calls.load(Ordering::Relaxed),
            compaction_spool_write_bytes: self.compaction_spool_write_bytes.load(Ordering::Relaxed),
            compaction_spool_write_nanos: self.compaction_spool_write_nanos.load(Ordering::Relaxed),
            compaction_spool_read_calls: self.compaction_spool_read_calls.load(Ordering::Relaxed),
            compaction_spool_read_bytes: self.compaction_spool_read_bytes.load(Ordering::Relaxed),
            compaction_spool_read_nanos: self.compaction_spool_read_nanos.load(Ordering::Relaxed),
            posting_exact_lookup_calls: self.posting_exact_lookup_calls.load(Ordering::Relaxed),
            posting_exact_lookup_segments: self
                .posting_exact_lookup_segments
                .load(Ordering::Relaxed),
            posting_exact_lookup_max_segments: self
                .posting_exact_lookup_max_segments
                .load(Ordering::Relaxed),
            posting_ordered_lookup_calls: self.posting_ordered_lookup_calls.load(Ordering::Relaxed),
            posting_ordered_lookup_segments: self
                .posting_ordered_lookup_segments
                .load(Ordering::Relaxed),
            posting_ordered_lookup_max_segments: self
                .posting_ordered_lookup_max_segments
                .load(Ordering::Relaxed),
            seal_calls: self.seal_calls.load(Ordering::Relaxed),
            seal_rows: self.seal_rows.load(Ordering::Relaxed),
            seal_bytes: self.seal_bytes.load(Ordering::Relaxed),
            seal_output_bytes: self.seal_output_bytes.load(Ordering::Relaxed),
            seal_nanos: self.seal_nanos.load(Ordering::Relaxed),
            compaction_calls: self.compaction_calls.load(Ordering::Relaxed),
            compaction_tables: self.compaction_tables.load(Ordering::Relaxed),
            compaction_nanos: self.compaction_nanos.load(Ordering::Relaxed),
        }
    }
}

static COUNTERS: EngineCounters = EngineCounters::new();
static RUNTIME_OWNERS: RuntimeOwnerGauges = RuntimeOwnerGauges::new();

struct RuntimeOwnerGauges {
    server_connections: AtomicU64,
    authenticated_sessions: AtomicU64,
    active_cursors: AtomicU64,
    prepared_statements: AtomicU64,
    active_executions: AtomicU64,
    staged_tables: AtomicU64,
    staged_indexes: AtomicU64,
    staged_index_drops: AtomicU64,
    staged_schema_changes: AtomicU64,
    staged_constraint_changes: AtomicU64,
}

impl RuntimeOwnerGauges {
    const fn new() -> Self {
        Self {
            server_connections: AtomicU64::new(0),
            authenticated_sessions: AtomicU64::new(0),
            active_cursors: AtomicU64::new(0),
            prepared_statements: AtomicU64::new(0),
            active_executions: AtomicU64::new(0),
            staged_tables: AtomicU64::new(0),
            staged_indexes: AtomicU64::new(0),
            staged_index_drops: AtomicU64::new(0),
            staged_schema_changes: AtomicU64::new(0),
            staged_constraint_changes: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> RuntimeOwnerSnapshot {
        RuntimeOwnerSnapshot {
            server_connections: self.server_connections.load(Ordering::Acquire),
            authenticated_sessions: self.authenticated_sessions.load(Ordering::Acquire),
            active_cursors: self.active_cursors.load(Ordering::Acquire),
            prepared_statements: self.prepared_statements.load(Ordering::Acquire),
            active_executions: self.active_executions.load(Ordering::Acquire),
            staged_tables: self.staged_tables.load(Ordering::Acquire),
            staged_indexes: self.staged_indexes.load(Ordering::Acquire),
            staged_index_drops: self.staged_index_drops.load(Ordering::Acquire),
            staged_schema_changes: self.staged_schema_changes.load(Ordering::Acquire),
            staged_constraint_changes: self.staged_constraint_changes.load(Ordering::Acquire),
        }
    }
}

thread_local! {
    static ROW_MATERIALIZATION_PENDING: RowMaterializationPendingCell =
        const { RowMaterializationPendingCell(Cell::new(RowMaterializationPending::EMPTY)) };
    static WAL_APPEND_PENDING: WalAppendPendingCell =
        const { WalAppendPendingCell(Cell::new(WalAppendPending::EMPTY)) };
    static PROTOCOL_ROW_ADAPTER_PENDING: ProtocolRowAdapterPendingCell =
        const { ProtocolRowAdapterPendingCell(Cell::new(ProtocolRowAdapterPending::EMPTY)) };
    static VOLUME_READ_PROBE: Cell<Option<VolumeReadProbeSnapshot>> = const { Cell::new(None) };
    static METADATA_PK_COUNT_PROBE: Cell<Option<MetadataPkCountProbeSnapshot>> =
        const { Cell::new(None) };
    static DERIVED_SUBQUERY_PROBE: Cell<Option<DerivedSubqueryProbeSnapshot>> =
        const { Cell::new(None) };
    #[cfg(any(test, feature = "test-hooks"))]
    static ROW_MATERIALIZATION_PROBE: Cell<Option<RowMaterializationProbeSnapshot>> =
        const { Cell::new(None) };
    #[cfg(any(test, feature = "test-hooks"))]
    static DECOMPRESSION_PROBE: Cell<Option<DecompressionProbeSnapshot>> =
        const { Cell::new(None) };
    #[cfg(any(test, feature = "test-hooks"))]
    static ARTIFACT_IO_PROBE: Cell<Option<ArtifactIoProbeSnapshot>> =
        const { Cell::new(None) };
    #[cfg(any(test, feature = "test-hooks"))]
    static RAM_ACCELERATOR_PROBE: Cell<Option<RamAcceleratorProbeSnapshot>> =
        const { Cell::new(None) };
    #[cfg(any(test, feature = "test-hooks"))]
    static POSTING_LOOKUP_PROBE: Cell<Option<PostingLookupProbeSnapshot>> =
        const { Cell::new(None) };
    static ARTIFACT_COLUMNAR_GROUP_PROBE: Cell<Option<ArtifactColumnarGroupProbeSnapshot>> =
        const { Cell::new(None) };
    static JOIN_EXECUTION_PROBE: Cell<Option<JoinExecutionProbeSnapshot>> =
        const { Cell::new(None) };
    static JOIN_EXECUTION_TRACE: RefCell<Option<JoinExecutionTraceSnapshot>> =
        const { RefCell::new(None) };
    static JOIN_PLANNING_PROBE: RefCell<Option<JoinPlanningProbeSnapshot>> =
        const { RefCell::new(None) };
    #[cfg(any(test, feature = "test-hooks"))]
    static PROTOCOL_COMPONENT_PROBE: Cell<Option<ProtocolComponentProbeSnapshot>> =
        const { Cell::new(None) };
    #[cfg(any(test, feature = "test-hooks"))]
    static PROTOCOL_PENDING_COLUMN_BATCH_PROBE: Cell<Option<ProtocolPendingColumnBatchProbeSnapshot>> =
        const { Cell::new(None) };
}

include!("record.rs");
