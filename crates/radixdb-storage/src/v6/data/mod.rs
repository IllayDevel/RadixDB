//! Immutable authoritative data-artifact container.
//!
//! This module owns the generation-specific byte format below the explicit
//! `storage::v6` namespace. Its implementation names describe durable roles;
//! persisted version identity remains in magic/version fields only.

mod bloom;
mod codec;
mod column;
mod column_model;
mod model;
mod physical;
mod row_ids;
mod source;
mod statistics;
mod writer;

pub use bloom::{DataBloom, DataBloomConfig};
pub(crate) use codec::read_data_typed_column_from_source;
pub use codec::{
    decode_data_artifact_layout, encode_data_artifact, open_data_artifact_metadata,
    open_data_artifact_metadata_with_limits, read_data_block, read_data_block_from_source,
    read_data_bloom, read_data_bloom_from_source, read_data_column, read_data_column_from_source,
    read_data_row_ids, read_data_row_ids_from_source,
};
pub(crate) use column::{
    append_non_null_value, decode_non_null_value, DecodedColumn, ValueByteBuffer,
};
pub use column::{DataValueEncoding, MAX_BYTES_PER_VALUE, MAX_DICTIONARY_ITEMS_PER_BLOCK};
pub use column_model::{DataColumn, DataColumnSpec};
pub use model::{
    DataArtifactHeader, DataArtifactInput, DataArtifactLayout, DataBlockKind, DataBlockRef,
    DataBlockSpec, DataLayout, DataPhysicalCodec, DataRowGroup, DataSectionKind, DataSectionRef,
    DATA_BLOCK_REF_BYTES, DATA_FOOTER_BYTES, DATA_HEADER_BYTES, DATA_SECTION_COUNT,
    DATA_SECTION_REF_BYTES, MAX_BLOCKS_PER_DATA_ARTIFACT, MAX_BLOOM_BITS_PER_GROUP_COLUMN,
    MAX_COLUMNS_PER_TABLE, MAX_DATA_DIRECTORY_BYTES, MAX_LOGICAL_BYTES_PER_BLOCK,
    MAX_ROWS_PER_GROUP, MAX_ROW_GROUPS_PER_DATA_ARTIFACT, MAX_STATISTICS_VALUES_BYTES,
    MAX_STATISTIC_VALUE_BYTES, MAX_STORED_BYTES_PER_BLOCK,
};
pub(crate) use row_ids::{decode_runtime_row_id, encode_runtime_row_id};
pub use source::{
    DataOpenLimits, DataOpenMetrics, OpenedDataArtifact, MAX_DATA_OPEN_METADATA_BYTES,
};
pub use statistics::{DataStatistics, DataStatisticsSpec};
pub(crate) use writer::DataStreamWriter;
