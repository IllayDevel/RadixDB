//! Canonical storage identity, artifact and durable-generation contracts.
//!
//! The module boundary names the persisted format generation. Owners below it
//! use role-based names and form the sole production storage authority.

mod artifact;
mod catalog;
mod catalog_wal;
mod control;
mod crash_point;
mod data;
mod error;
mod fault;
mod gc;
mod identity;
mod index;
mod manifest;
mod publication;
mod reachability;
mod recovery;
mod reference;
mod root;
mod runtime;
mod scrub;
mod snapshot;
mod source;
mod staging;

pub use artifact::{
    ArtifactKind, ArtifactLocator, ArtifactRef, ArtifactSliceRef, ArtifactSuffix, IndexSectionKind,
    IndexSectionRef, ARTIFACT_CODEC_VERSION, MAX_ARTIFACT_FILE_BYTES,
};
pub use catalog::{encode_catalog_artifact, publish_catalog_mutation};
pub use catalog_wal::{
    append_catalog_wal_transaction, decode_catalog_wal, encode_catalog_wal_transaction,
    replay_catalog_wal, replay_catalog_wal_after, CatalogWalReplay, CatalogWalReplayLimits,
    CatalogWalTransaction, CatalogWalTransactionId, CATALOG_WAL_FORMAT_MAJOR,
    CATALOG_WAL_FORMAT_MINOR, CATALOG_WAL_RECORD_HEADER_BYTES, MAX_CATALOG_WAL_RECORD_BYTES,
    MAX_CATALOG_WAL_REPLAY_BYTES, MAX_CATALOG_WAL_REPLAY_TRANSACTIONS,
};
pub use control::{
    decode_control_slot, encode_control_slot, select_control_slots, CatalogRootRef, ControlRecord,
    ControlSlotIndex, DatabaseManifestRootRef, CONTROL_RECORD_BYTES,
};
pub use crash_point::{GenerationCrashPoint, PhysicalGenerationExpectation, SemanticExpectation};
pub use data::{
    decode_data_artifact_layout, encode_data_artifact, open_data_artifact_metadata,
    open_data_artifact_metadata_with_limits, read_data_block, read_data_block_from_source,
    read_data_bloom, read_data_bloom_from_source, read_data_column, read_data_column_from_source,
    read_data_row_ids, read_data_row_ids_from_source, DataArtifactHeader, DataArtifactInput,
    DataArtifactLayout, DataBlockKind, DataBlockRef, DataBlockSpec, DataBloom, DataBloomConfig,
    DataColumn, DataColumnSpec, DataLayout, DataOpenLimits, DataOpenMetrics, DataPhysicalCodec,
    DataRowGroup, DataSectionKind, DataSectionRef, DataStatistics, DataStatisticsSpec,
    DataValueEncoding, OpenedDataArtifact, DATA_BLOCK_REF_BYTES, DATA_FOOTER_BYTES,
    DATA_HEADER_BYTES, DATA_SECTION_COUNT, DATA_SECTION_REF_BYTES, MAX_BLOCKS_PER_DATA_ARTIFACT,
    MAX_BLOOM_BITS_PER_GROUP_COLUMN, MAX_BYTES_PER_VALUE, MAX_COLUMNS_PER_TABLE,
    MAX_DATA_DIRECTORY_BYTES, MAX_DATA_OPEN_METADATA_BYTES, MAX_DICTIONARY_ITEMS_PER_BLOCK,
    MAX_LOGICAL_BYTES_PER_BLOCK, MAX_ROWS_PER_GROUP, MAX_ROW_GROUPS_PER_DATA_ARTIFACT,
    MAX_STATISTICS_VALUES_BYTES, MAX_STATISTIC_VALUE_BYTES, MAX_STORED_BYTES_PER_BLOCK,
};
pub(crate) use data::{decode_runtime_row_id, encode_runtime_row_id};
pub(crate) use data::{read_data_typed_column_from_source, DecodedColumn};
pub use error::{FormatError, FormatResult};
#[cfg(any(test, feature = "test-failpoints"))]
pub use fault::{GenerationFaultGuard, GenerationFaultMode};
pub use gc::{
    ArtifactCleanupLimits, ArtifactCleanupReport, ArtifactGarbageCollector, CleanupGeneration,
    MAX_CLEANUP_ACCOUNTED_BYTES, MAX_CLEANUP_FILES, MAX_CLEANUP_MUTATIONS, MAX_CLEANUP_WALL_TIME,
};
pub(crate) use gc::{ArtifactReachability, ImmutableMemberLocator, ImmutableMemberRef};
pub use identity::{
    ArtifactId, CatalogGeneration, CatalogId, DatabaseGeneration, DatabaseId, ManifestGeneration,
    ManifestId, PublicationId, SegmentId, SnapshotId, WalGeneration, WalReplayFloor,
    WriterInstanceId,
};
pub(crate) use index::scan_ordered_non_null_index_from_source;
pub use index::{
    admit_constraint_mutation, decode_exact_index_page, decode_hnsw_index,
    decode_hnsw_index_from_source, decode_index_artifact_layout, decode_ordered_index_page,
    encode_exact_index_pages, encode_hnsw_index_sections, encode_index_artifact,
    encode_ordered_index_pages, lookup_exact_index, lookup_exact_index_from_source,
    open_index_artifact_metadata, open_index_artifact_metadata_with_limits, read_index_page,
    read_index_page_from_source, read_index_section, read_index_section_from_source,
    scan_ordered_index, scan_ordered_index_from_source, visit_exact_index_or_scan,
    ConstraintMutationAdmission, ConstraintMutationError, ConstraintMutationGuard,
    ConstraintMutationKind, ExactFallbackReason, ExactIndexEntry, ExactIndexKey, ExactIndexPage,
    ExactIndexPageEntry, ExactLookupDefinition, ExactLookupPath, ExactLookupReport,
    ExactPageBuildLimits, HnswBuildParameters, HnswDecodeLimits, HnswGraphNode, HnswIndexGraph,
    HnswPageBuildLimits, IndexAccelerator, IndexAcceleratorKind, IndexAcceleratorSpec,
    IndexAccessState, IndexArtifactHeader, IndexArtifactInput, IndexArtifactLayout, IndexKeyColumn,
    IndexNullsOrder, IndexOpenLimits, IndexOpenMetrics, IndexPage, IndexPageCodec, IndexPageSpec,
    IndexRebuildRequest, IndexScanDirection, IndexSection, IndexSectionSpec, IndexSortDirection,
    OpenedIndexArtifact, OrderedIndexBound, OrderedIndexEntry, OrderedIndexKey, OrderedIndexPage,
    OrderedIndexPageEntry, OrderedPageBuildLimits, RebuildRequestSink, RebuildRequestStatus,
    DEFAULT_EXACT_PAGE_BYTES, DEFAULT_EXACT_PAGE_ENTRIES, DEFAULT_HNSW_PAGE_BYTES,
    DEFAULT_HNSW_PAGE_ENTRIES, DEFAULT_ORDERED_PAGE_BYTES, DEFAULT_ORDERED_PAGE_ENTRIES,
    INDEX_ACCELERATOR_ENTRY_BYTES, INDEX_FOOTER_BYTES, INDEX_HEADER_BYTES, INDEX_PAGE_ENTRY_BYTES,
    INDEX_SECTION_ENTRY_BYTES, MAX_ACCELERATORS_PER_INDEX_ARTIFACT, MAX_ENTRIES_PER_INDEX_PAGE,
    MAX_EXACT_PAGE_DECODE_BYTES, MAX_HNSW_GRAPH_DECODE_BYTES, MAX_HNSW_LEVELS,
    MAX_HNSW_NEIGHBORS_PER_LEVEL, MAX_INDEX_COMPRESSION_RATIO, MAX_INDEX_DIRECTORY_BYTES,
    MAX_INDEX_KEY_BYTES, MAX_INDEX_OPEN_METADATA_BYTES, MAX_INDEX_PAGES, MAX_INDEX_SECTIONS,
    MAX_KEY_COLUMNS, MAX_LOGICAL_BYTES_PER_INDEX_PAGE, MAX_ORDERED_PAGE_DECODE_BYTES,
    MAX_STORED_BYTES_PER_INDEX_PAGE,
};
pub(crate) use manifest::encoded_database_manifest_length;
pub use manifest::{
    decode_database_manifest, decode_table_manifest, encode_database_manifest,
    encode_table_manifest, DatabaseManifest, SegmentDescriptor, SegmentKind, SegmentTier,
    TableManifest, TableManifestRef, MAX_ROWS_PER_DATA_ARTIFACT, MAX_SEGMENTS_PER_TABLE_MANIFEST,
    MAX_TABLES_PER_DATABASE,
};
pub(crate) use publication::filesystem::source::{
    catalog_path, database_manifest_path, table_manifest_path, wal_path,
};
pub(crate) use publication::write_index_replacement_reusing;
pub use publication::{
    build_artifact_pair, build_data_artifact, stage_index_replacement, write_artifact_pair,
    write_data_artifact, write_index_replacement, AcceleratorBuildSpec, ActiveLeaseSet,
    ArtifactBuildKind, ArtifactBuildLease, ArtifactPairBuildRequest, BuiltArtifactPair,
    BuiltDataArtifact, CheckpointOutcome, ColumnBuildPolicy, DataArtifactBuildRequest,
    FanoutBuildLimits, FrozenCheckpoint, FrozenMaintenance, IndexReplacementBuildRequest,
    IndexReplacementPublicationSink, LeaseLimits, MaintenanceKind, MaintenanceOutcome,
    PhysicalGenerationLease, PhysicalGenerationPublisher, PhysicalGenerationSnapshot,
    PreparedIndexReplacement, SourceRow, StagedIndexReplacement, WalRetirementReport,
    WalRetirementStatus, WrittenArtifactPair, WrittenDataArtifact, WrittenIndexReplacement,
    DEFAULT_FANOUT_RESIDENT_BYTES, DEFAULT_FANOUT_SPILL_BYTES, DEFAULT_MERGE_FAN_IN,
    DEFAULT_SORT_RUN_BYTES, DEFAULT_SORT_RUN_RECORDS, MAX_ACCELERATOR_PREPARATION_WORKERS,
    MAX_ACTIVE_ARTIFACT_BUILDS, MAX_ACTIVE_GENERATION_LEASES, MAX_FANOUT_RESIDENT_BYTES,
    MAX_FANOUT_SPILL_BYTES, MAX_MERGE_FAN_IN, MAX_SORT_RUN_BYTES, MAX_SORT_RUN_DESCRIPTOR_SLOTS,
    MAX_SORT_RUN_FILES, MAX_SORT_RUN_RECORDS,
};
#[cfg(feature = "test-hooks")]
pub use publication::{
    publication_diagnostics, reset_publication_diagnostics, PublicationDiagnostics,
};
pub use reachability::{
    validate_control_generation, validate_control_generation_with_limits, ArtifactInspection,
    ArtifactMetadata, DataArtifactMetadata, IndexArtifactMetadata, ReachabilityAllowance,
    ReachabilityError, ReachabilityLimits, ReachabilityResult, ReachabilitySource,
    ReachableNodeKind, UnavailableIndex, UnavailableIndexReason, ValidatedGeneration,
    MAX_OPEN_METADATA_BYTES, MAX_REACHABILITY_BYTES, MAX_REACHABLE_IDENTITIES,
    MAX_TOTAL_MANIFESTS_PER_OPEN, MAX_TOTAL_SEGMENTS_PER_OPEN,
};
pub use recovery::{
    CatalogRecoveryReport, DataWalRecoveryContext, DataWalRecoveryOutcome, DataWalRecoveryReport,
    DatabaseRecovery, RecoveredDatabase, RecoveryLimits, WalRecovery,
};
pub use reference::{
    CatalogRef, FormatVersion, ManifestKind, ManifestRef, FORMAT_VERSION, MAX_CATALOG_FILE_BYTES,
    MAX_MANIFEST_FILE_BYTES,
};
pub use root::{DatabaseRoot, DatabaseRootState};
pub use runtime::{
    ArtifactColumnBatch, ArtifactDataSource, ArtifactIndexSource, ArtifactRowIdBatch,
    OpenMetadataBudget,
};
pub use scrub::{scrub_generation_artifacts, ArtifactScrubReport};
pub use snapshot::{
    commit_snapshot_manifest, decode_snapshot_manifest, encode_snapshot_manifest,
    open_snapshot_manifest, restore_snapshot, SnapshotIndexPolicy, SnapshotLocatorSuffix,
    SnapshotManifest, SnapshotMember, SnapshotMemberKind, SnapshotRestoreOutcome,
    MAX_SNAPSHOT_MANIFEST_BYTES, MAX_SNAPSHOT_MEMBERS, MAX_SNAPSHOT_WAL_BYTES,
    SNAPSHOT_MANIFEST_FILE, SNAPSHOT_MEMBER_BYTES,
};
pub use source::{ArtifactFile, ArtifactSource};
pub use staging::{
    decode_staging_complete, decode_staging_owner, discover_staging_publications,
    encode_staging_complete, encode_staging_owner, CompleteStagingSet, StagedArtifactSet,
    StagedMemberRole, StagingComplete, StagingCompletion, StagingDiscovery, StagingDiscoveryLimits,
    StagingDisposition, StagingOwner, StagingPublication, MAX_STAGED_FILES_PER_PUBLICATION,
    MAX_STAGED_PATH_BYTES, MAX_STAGED_PATH_COMPONENT_BYTES, MAX_STAGING_DISCOVERY_BYTES,
    MAX_STAGING_PUBLICATIONS, MAX_STAGING_RECURSION_DEPTH, STAGING_COMPLETE_BYTES,
    STAGING_OWNER_BYTES,
};
