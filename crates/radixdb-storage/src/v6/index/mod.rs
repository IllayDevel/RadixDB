//! Optional page-addressable accelerator container.
//!
//! The module owns the byte format below the explicit `storage::v6` namespace.
//! Its public implementation names remain stable role names.

mod access;
mod codec;
mod exact;
mod exact_validation;
mod hnsw;
mod key;
mod model;
mod ordered;
mod posting;
mod source;
mod writer;

pub use access::{
    admit_constraint_mutation, visit_exact_index_or_scan, ConstraintMutationAdmission,
    ConstraintMutationError, ConstraintMutationGuard, ConstraintMutationKind, ExactFallbackReason,
    ExactLookupDefinition, ExactLookupPath, ExactLookupReport, IndexAccessState,
    IndexRebuildRequest, RebuildRequestSink, RebuildRequestStatus,
};
pub use codec::{
    decode_index_artifact_layout, encode_index_artifact, open_index_artifact_metadata,
    open_index_artifact_metadata_with_limits, read_index_page, read_index_page_from_source,
    read_index_section, read_index_section_from_source,
};
pub(crate) use exact::{count_exact_index_pages_from_sorted, visit_exact_index_pages_from_sorted};
pub use exact::{
    decode_exact_index_page, encode_exact_index_pages, lookup_exact_index,
    lookup_exact_index_from_source, ExactIndexEntry, ExactIndexKey, ExactIndexPage,
    ExactIndexPageEntry, ExactPageBuildLimits, DEFAULT_EXACT_PAGE_BYTES,
    DEFAULT_EXACT_PAGE_ENTRIES, MAX_EXACT_PAGE_DECODE_BYTES, MAX_INDEX_KEY_BYTES,
};
pub use hnsw::{
    decode_hnsw_index, decode_hnsw_index_from_source, encode_hnsw_index_sections,
    HnswBuildParameters, HnswDecodeLimits, HnswGraphNode, HnswIndexGraph, HnswPageBuildLimits,
    DEFAULT_HNSW_PAGE_BYTES, DEFAULT_HNSW_PAGE_ENTRIES, MAX_HNSW_GRAPH_DECODE_BYTES,
    MAX_HNSW_LEVELS, MAX_HNSW_NEIGHBORS_PER_LEVEL,
};
pub use model::{
    IndexAccelerator, IndexAcceleratorKind, IndexAcceleratorSpec, IndexArtifactHeader,
    IndexArtifactInput, IndexArtifactLayout, IndexKeyColumn, IndexNullsOrder, IndexPage,
    IndexPageCodec, IndexPageSpec, IndexSection, IndexSectionSpec, IndexSortDirection,
    INDEX_ACCELERATOR_ENTRY_BYTES, INDEX_FOOTER_BYTES, INDEX_HEADER_BYTES, INDEX_PAGE_ENTRY_BYTES,
    INDEX_SECTION_ENTRY_BYTES, MAX_ACCELERATORS_PER_INDEX_ARTIFACT, MAX_ENTRIES_PER_INDEX_PAGE,
    MAX_INDEX_COMPRESSION_RATIO, MAX_INDEX_DIRECTORY_BYTES, MAX_INDEX_PAGES, MAX_INDEX_SECTIONS,
    MAX_KEY_COLUMNS, MAX_LOGICAL_BYTES_PER_INDEX_PAGE, MAX_STORED_BYTES_PER_INDEX_PAGE,
};
pub(crate) use ordered::{
    compare_ordered_keys_trusted, count_ordered_index_pages_from_sorted,
    scan_ordered_non_null_index_from_source, visit_ordered_index_pages_from_sorted,
};
pub use ordered::{
    decode_ordered_index_page, encode_ordered_index_pages, scan_ordered_index,
    scan_ordered_index_from_source, IndexScanDirection, OrderedIndexBound, OrderedIndexEntry,
    OrderedIndexKey, OrderedIndexPage, OrderedIndexPageEntry, OrderedPageBuildLimits,
    DEFAULT_ORDERED_PAGE_BYTES, DEFAULT_ORDERED_PAGE_ENTRIES, MAX_ORDERED_PAGE_DECODE_BYTES,
};
pub(crate) use posting::PostingFragmentState;
pub use source::{
    IndexOpenLimits, IndexOpenMetrics, OpenedIndexArtifact, MAX_INDEX_OPEN_METADATA_BYTES,
};
pub(crate) use writer::{write_index_artifact_stream, IndexPageSource};
