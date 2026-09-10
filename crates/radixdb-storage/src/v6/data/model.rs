use radixdb_catalog::ObjectId;
use radixdb_core::{DataType, Value};

#[cfg(test)]
#[path = "model_tests.rs"]
mod tests;

use super::super::{
    ArtifactId, ArtifactRef, CatalogGeneration, DatabaseGeneration, DatabaseId, FormatError,
    FormatResult, SegmentId, SegmentKind,
};
use super::bloom::{encode_bloom_payload, DataBloomConfig};
use super::column::{encode_column_payload, DataValueEncoding};
use super::column_model::{DataColumn, DataColumnSpec};
use super::physical::encode_physical;
use super::row_ids::encode_row_id_payload;
use super::statistics::{build_statistics, DataStatistics, DataStatisticsSpec};

pub const DATA_HEADER_BYTES: usize = 256;
pub const DATA_FOOTER_BYTES: usize = 48;
pub const DATA_SECTION_REF_BYTES: usize = 48;
pub const DATA_SECTION_COUNT: usize = 5;
pub const DATA_BLOCK_REF_BYTES: usize = 64;

pub const MAX_COLUMNS_PER_TABLE: u32 = 4_096;
pub const MAX_ROW_GROUPS_PER_DATA_ARTIFACT: u32 = 65_536;
pub const MAX_ROWS_PER_GROUP: u32 = 65_536;
pub const MAX_BLOCKS_PER_DATA_ARTIFACT: u64 = 4_194_304;
pub const MAX_DATA_DIRECTORY_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_STORED_BYTES_PER_BLOCK: u64 = 256 * 1024 * 1024;
pub const MAX_LOGICAL_BYTES_PER_BLOCK: u64 = 512 * 1024 * 1024;
pub const MAX_STATISTICS_VALUES_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_STATISTIC_VALUE_BYTES: u64 = 1024 * 1024;
pub const MAX_BLOOM_BITS_PER_GROUP_COLUMN: u64 = 67_108_864;
pub const MAX_COMPRESSION_RATIO: u64 = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataArtifactHeader {
    artifact_id: ArtifactId,
    database_id: DatabaseId,
    table_id: ObjectId,
    segment_id: SegmentId,
    creation_generation: DatabaseGeneration,
    catalog_generation: CatalogGeneration,
    min_transaction_id: u64,
    max_transaction_id: u64,
    row_count: u64,
    column_count: u32,
    row_group_count: u32,
    segment_kind: SegmentKind,
    created_unix_ns: u64,
}

impl DataArtifactHeader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        artifact_id: ArtifactId,
        database_id: DatabaseId,
        table_id: ObjectId,
        segment_id: SegmentId,
        creation_generation: DatabaseGeneration,
        catalog_generation: CatalogGeneration,
        min_transaction_id: u64,
        max_transaction_id: u64,
        row_count: u64,
        column_count: u32,
        row_group_count: u32,
        segment_kind: SegmentKind,
        created_unix_ns: u64,
    ) -> FormatResult<Self> {
        if max_transaction_id < min_transaction_id {
            return Err(invalid("transaction range is reversed"));
        }
        if row_count > u64::from(u32::MAX) {
            return Err(limit("row count", row_count, u64::from(u32::MAX)));
        }
        if column_count > MAX_COLUMNS_PER_TABLE {
            return Err(limit(
                "column count",
                u64::from(column_count),
                u64::from(MAX_COLUMNS_PER_TABLE),
            ));
        }
        if row_group_count > MAX_ROW_GROUPS_PER_DATA_ARTIFACT {
            return Err(limit(
                "row-group count",
                u64::from(row_group_count),
                u64::from(MAX_ROW_GROUPS_PER_DATA_ARTIFACT),
            ));
        }
        if (row_count == 0) != (row_group_count == 0) {
            return Err(invalid(
                "row count and row-group count must be empty together",
            ));
        }
        let row_capacity = u64::from(row_group_count)
            .checked_mul(u64::from(MAX_ROWS_PER_GROUP))
            .ok_or_else(|| invalid("row-group capacity multiplication overflows"))?;
        if row_count > row_capacity {
            return Err(invalid("row count exceeds row-group capacity"));
        }
        if row_count != 0 && min_transaction_id == 0 {
            return Err(invalid(
                "non-empty artifact has zero minimum transaction ID",
            ));
        }
        if segment_kind == SegmentKind::Tombstones && column_count != 0 {
            return Err(invalid("tombstone artifact has columns"));
        }
        Ok(Self {
            artifact_id,
            database_id,
            table_id,
            segment_id,
            creation_generation,
            catalog_generation,
            min_transaction_id,
            max_transaction_id,
            row_count,
            column_count,
            row_group_count,
            segment_kind,
            created_unix_ns,
        })
    }

    pub const fn artifact_id(self) -> ArtifactId {
        self.artifact_id
    }

    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    pub const fn table_id(self) -> ObjectId {
        self.table_id
    }

    pub const fn segment_id(self) -> SegmentId {
        self.segment_id
    }

    pub const fn creation_generation(self) -> DatabaseGeneration {
        self.creation_generation
    }

    pub const fn catalog_generation(self) -> CatalogGeneration {
        self.catalog_generation
    }

    pub const fn min_transaction_id(self) -> u64 {
        self.min_transaction_id
    }

    pub const fn max_transaction_id(self) -> u64 {
        self.max_transaction_id
    }

    pub const fn row_count(self) -> u64 {
        self.row_count
    }

    pub const fn column_count(self) -> u32 {
        self.column_count
    }

    pub const fn row_group_count(self) -> u32 {
        self.row_group_count
    }

    pub const fn segment_kind(self) -> SegmentKind {
        self.segment_kind
    }

    pub const fn created_unix_ns(self) -> u64 {
        self.created_unix_ns
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u16)]
pub enum DataSectionKind {
    ColumnDirectory = 1,
    RowGroupDirectory = 2,
    BlockDirectory = 3,
    StatisticsDirectory = 4,
    StatisticsValues = 5,
}

impl DataSectionKind {
    pub const ALL: [Self; DATA_SECTION_COUNT] = [
        Self::ColumnDirectory,
        Self::RowGroupDirectory,
        Self::BlockDirectory,
        Self::StatisticsDirectory,
        Self::StatisticsValues,
    ];

    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataSectionRef {
    kind: DataSectionKind,
    offset: u64,
    stored_length: u64,
    item_count: u64,
    stored_crc32: u32,
}

impl DataSectionRef {
    pub(crate) const fn new(
        kind: DataSectionKind,
        offset: u64,
        stored_length: u64,
        item_count: u64,
        stored_crc32: u32,
    ) -> Self {
        Self {
            kind,
            offset,
            stored_length,
            item_count,
            stored_crc32,
        }
    }

    pub const fn kind(self) -> DataSectionKind {
        self.kind
    }

    pub const fn offset(self) -> u64 {
        self.offset
    }

    pub const fn stored_length(self) -> u64 {
        self.stored_length
    }

    pub const fn logical_length(self) -> u64 {
        self.stored_length
    }

    pub const fn item_count(self) -> u64 {
        self.item_count
    }

    pub const fn stored_crc32(self) -> u32 {
        self.stored_crc32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u16)]
pub enum DataBlockKind {
    RowIds = 1,
    Column = 2,
    Bloom = 3,
}

impl DataBlockKind {
    pub(crate) fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::RowIds),
            2 => Ok(Self::Column),
            3 => Ok(Self::Bloom),
            _ => Err(invalid("unknown block kind")),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum DataPhysicalCodec {
    None = 0,
    Lz4 = 1,
}

impl DataPhysicalCodec {
    pub(crate) fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            _ => Err(invalid("unknown block physical codec")),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataLayout {
    RowIdsDelta = 1,
    PlainValues = 2,
    DictionaryValues = 3,
    Bloom = 4,
}

impl DataLayout {
    pub(crate) fn from_tag(tag: u8) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::RowIdsDelta),
            2 => Ok(Self::PlainValues),
            3 => Ok(Self::DictionaryValues),
            4 => Ok(Self::Bloom),
            _ => Err(invalid("unknown block logical layout")),
        }
    }

    pub const fn tag(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataBlockSpec {
    kind: DataBlockKind,
    codec: DataPhysicalCodec,
    column_ordinal: u32,
    row_group_ordinal: u32,
    layout: DataLayout,
    logical_length: u64,
    item_count: u64,
    stored_bytes: Vec<u8>,
    row_id_bounds: Option<(u64, u64)>,
    column_spec: Option<DataColumnSpec>,
    source_value_count: Option<u64>,
}

impl DataBlockSpec {
    pub fn row_ids(
        row_group_ordinal: u32,
        row_ids: &[u64],
        codec: DataPhysicalCodec,
    ) -> FormatResult<Self> {
        let logical = encode_row_id_payload(row_ids)?;
        let stored_bytes = encode_physical(&logical, codec);
        validate_block_lengths(codec, stored_bytes.len() as u64, logical.len() as u64)?;
        Ok(Self {
            kind: DataBlockKind::RowIds,
            codec,
            column_ordinal: u32::MAX,
            row_group_ordinal,
            layout: DataLayout::RowIdsDelta,
            logical_length: logical.len() as u64,
            item_count: row_ids.len() as u64,
            stored_bytes,
            row_id_bounds: Some((row_ids[0], row_ids[row_ids.len() - 1])),
            column_spec: None,
            source_value_count: None,
        })
    }

    pub fn column(
        row_group_ordinal: u32,
        column_ordinal: u32,
        column: DataColumnSpec,
        values: &[Value],
        encoding: DataValueEncoding,
        codec: DataPhysicalCodec,
    ) -> FormatResult<Self> {
        let encode = |candidate| -> FormatResult<(DataLayout, Vec<u8>, Vec<u8>)> {
            let (layout, logical) = encode_column_payload(column, values, candidate)?;
            let stored = encode_physical(&logical, codec);
            Ok((layout, logical, stored))
        };
        let (layout, logical, stored_bytes) = if encoding == DataValueEncoding::Adaptive {
            if !matches!(
                column.data_type().logical_type(),
                DataType::Text | DataType::Json | DataType::Bytes
            ) {
                return Err(invalid(
                    "adaptive encoding is allowed only for variable-width dictionary types",
                ));
            }
            // Build candidates sequentially. Holding only the first candidate's
            // stored bytes while building the second keeps the adaptive policy
            // inside the same bounded block budget as ordinary publication.
            let dictionary = encode(DataValueEncoding::Dictionary)?;
            let plain = encode(DataValueEncoding::Plain)?;
            let dictionary_rank = (dictionary.2.len(), dictionary.1.len(), dictionary.0.tag());
            let plain_rank = (plain.2.len(), plain.1.len(), plain.0.tag());
            if dictionary_rank < plain_rank {
                dictionary
            } else {
                plain
            }
        } else {
            encode(encoding)?
        };
        validate_block_lengths(codec, stored_bytes.len() as u64, logical.len() as u64)?;
        Ok(Self {
            kind: DataBlockKind::Column,
            codec,
            column_ordinal,
            row_group_ordinal,
            layout,
            logical_length: logical.len() as u64,
            item_count: values.len() as u64,
            stored_bytes,
            row_id_bounds: None,
            column_spec: Some(column),
            source_value_count: Some(values.len() as u64),
        })
    }

    pub fn bloom(
        row_group_ordinal: u32,
        column_ordinal: u32,
        column: DataColumnSpec,
        values: &[Value],
        config: DataBloomConfig,
        codec: DataPhysicalCodec,
    ) -> FormatResult<Self> {
        let logical = encode_bloom_payload(column, values, config)?;
        let stored_bytes = encode_physical(&logical, codec);
        validate_block_lengths(codec, stored_bytes.len() as u64, logical.len() as u64)?;
        Ok(Self {
            kind: DataBlockKind::Bloom,
            codec,
            column_ordinal,
            row_group_ordinal,
            layout: DataLayout::Bloom,
            logical_length: logical.len() as u64,
            item_count: u64::from(config.bit_count()),
            stored_bytes,
            row_id_bounds: None,
            column_spec: Some(column),
            source_value_count: Some(values.len() as u64),
        })
    }

    pub const fn kind(&self) -> DataBlockKind {
        self.kind
    }

    pub const fn codec(&self) -> DataPhysicalCodec {
        self.codec
    }

    pub const fn column_ordinal(&self) -> u32 {
        self.column_ordinal
    }

    pub const fn row_group_ordinal(&self) -> u32 {
        self.row_group_ordinal
    }

    pub const fn layout(&self) -> DataLayout {
        self.layout
    }

    pub const fn logical_length(&self) -> u64 {
        self.logical_length
    }

    pub const fn item_count(&self) -> u64 {
        self.item_count
    }

    pub fn stored_bytes(&self) -> &[u8] {
        &self.stored_bytes
    }

    pub(crate) const fn row_id_bounds(&self) -> Option<(u64, u64)> {
        self.row_id_bounds
    }

    pub(crate) const fn column_spec(&self) -> Option<DataColumnSpec> {
        self.column_spec
    }

    pub(crate) const fn source_value_count(&self) -> Option<u64> {
        self.source_value_count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataBlockRef {
    kind: DataBlockKind,
    codec: DataPhysicalCodec,
    column_ordinal: u32,
    row_group_ordinal: u32,
    layout: DataLayout,
    offset: u64,
    stored_length: u64,
    logical_length: u64,
    item_count: u64,
    stored_crc32: u32,
}

impl DataBlockRef {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: DataBlockKind,
        codec: DataPhysicalCodec,
        column_ordinal: u32,
        row_group_ordinal: u32,
        layout: DataLayout,
        offset: u64,
        stored_length: u64,
        logical_length: u64,
        item_count: u64,
        stored_crc32: u32,
    ) -> FormatResult<Self> {
        validate_kind_layout(kind, layout, column_ordinal)?;
        validate_block_lengths(codec, stored_length, logical_length)?;
        Ok(Self {
            kind,
            codec,
            column_ordinal,
            row_group_ordinal,
            layout,
            offset,
            stored_length,
            logical_length,
            item_count,
            stored_crc32,
        })
    }

    pub const fn kind(&self) -> DataBlockKind {
        self.kind
    }

    pub const fn codec(&self) -> DataPhysicalCodec {
        self.codec
    }

    pub const fn column_ordinal(&self) -> u32 {
        self.column_ordinal
    }

    pub const fn row_group_ordinal(&self) -> u32 {
        self.row_group_ordinal
    }

    pub const fn layout(&self) -> DataLayout {
        self.layout
    }

    pub const fn offset(&self) -> u64 {
        self.offset
    }

    pub const fn stored_length(&self) -> u64 {
        self.stored_length
    }

    pub const fn logical_length(&self) -> u64 {
        self.logical_length
    }

    pub const fn item_count(&self) -> u64 {
        self.item_count
    }

    pub const fn stored_crc32(&self) -> u32 {
        self.stored_crc32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataArtifactInput {
    header: DataArtifactHeader,
    columns: Vec<DataColumn>,
    row_groups: Vec<DataRowGroup>,
    statistics: Vec<DataStatistics>,
    statistics_directory: Vec<u8>,
    statistics_values: Vec<u8>,
    blocks: Vec<DataBlockSpec>,
}

impl DataArtifactInput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        header: DataArtifactHeader,
        column_specs: Vec<DataColumnSpec>,
        statistics_specs: Vec<DataStatisticsSpec>,
        blocks: Vec<DataBlockSpec>,
    ) -> FormatResult<Self> {
        if column_specs.len() as u64 != u64::from(header.column_count()) {
            return Err(invalid("column specs do not match header column count"));
        }
        if header.segment_kind() == SegmentKind::Tombstones && !column_specs.is_empty() {
            return Err(invalid("tombstone artifact has column specs"));
        }
        if blocks.len() as u64 > MAX_BLOCKS_PER_DATA_ARTIFACT {
            return Err(limit(
                "block count",
                blocks.len() as u64,
                MAX_BLOCKS_PER_DATA_ARTIFACT,
            ));
        }
        for block in &blocks {
            validate_block_coordinates(&header, block)?;
        }
        if blocks
            .windows(2)
            .any(|pair| block_key(&pair[0]) >= block_key(&pair[1]))
        {
            return Err(invalid("blocks are not in canonical directory order"));
        }
        let row_groups = derive_row_groups(&header, &blocks)?;
        let statistics = build_statistics(&column_specs, &row_groups, &blocks, statistics_specs)?;
        let columns = derive_columns(
            &header,
            &column_specs,
            &row_groups,
            &statistics.entries,
            &blocks,
        )?;
        let directory_bytes = ((DATA_SECTION_COUNT * DATA_SECTION_REF_BYTES) as u64)
            .checked_add((columns.len() * 64) as u64)
            .and_then(|value| value.checked_add((row_groups.len() * 48) as u64))
            .and_then(|value| value.checked_add(statistics.directory.len() as u64))
            .and_then(|value| value.checked_add((blocks.len() * DATA_BLOCK_REF_BYTES) as u64))
            .ok_or_else(|| invalid("directory byte count overflows"))?;
        if directory_bytes > MAX_DATA_DIRECTORY_BYTES {
            return Err(limit(
                "directory bytes",
                directory_bytes,
                MAX_DATA_DIRECTORY_BYTES,
            ));
        }
        Ok(Self {
            header,
            columns,
            row_groups,
            statistics: statistics.entries,
            statistics_directory: statistics.directory,
            statistics_values: statistics.values,
            blocks,
        })
    }

    pub const fn header(&self) -> DataArtifactHeader {
        self.header
    }

    pub fn columns(&self) -> &[DataColumn] {
        &self.columns
    }

    pub fn row_groups(&self) -> &[DataRowGroup] {
        &self.row_groups
    }

    pub fn statistics(&self) -> &[DataStatistics] {
        &self.statistics
    }

    pub(crate) fn statistics_directory(&self) -> &[u8] {
        &self.statistics_directory
    }

    pub(crate) fn statistics_values(&self) -> &[u8] {
        &self.statistics_values
    }

    pub fn blocks(&self) -> &[DataBlockSpec] {
        &self.blocks
    }
}

fn block_key(block: &DataBlockSpec) -> (u32, u16, u32) {
    (
        block.row_group_ordinal(),
        block.kind().tag(),
        block.column_ordinal(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataArtifactLayout {
    reference: ArtifactRef,
    header: DataArtifactHeader,
    sections: [DataSectionRef; DATA_SECTION_COUNT],
    columns: Vec<DataColumn>,
    row_groups: Vec<DataRowGroup>,
    statistics: Vec<DataStatistics>,
    blocks: Vec<DataBlockRef>,
}

impl DataArtifactLayout {
    pub(crate) const fn new(
        reference: ArtifactRef,
        header: DataArtifactHeader,
        sections: [DataSectionRef; DATA_SECTION_COUNT],
        columns: Vec<DataColumn>,
        row_groups: Vec<DataRowGroup>,
        statistics: Vec<DataStatistics>,
        blocks: Vec<DataBlockRef>,
    ) -> Self {
        Self {
            reference,
            header,
            sections,
            columns,
            row_groups,
            statistics,
            blocks,
        }
    }

    pub const fn reference(&self) -> ArtifactRef {
        self.reference
    }

    pub const fn header(&self) -> DataArtifactHeader {
        self.header
    }

    pub const fn sections(&self) -> &[DataSectionRef; DATA_SECTION_COUNT] {
        &self.sections
    }

    pub fn row_groups(&self) -> &[DataRowGroup] {
        &self.row_groups
    }

    pub fn columns(&self) -> &[DataColumn] {
        &self.columns
    }

    pub fn statistics(&self) -> &[DataStatistics] {
        &self.statistics
    }

    pub fn blocks(&self) -> &[DataBlockRef] {
        &self.blocks
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataRowGroup {
    group_ordinal: u32,
    row_count: u32,
    first_row_ordinal: u64,
    min_row_id: u64,
    max_row_id: u64,
    first_block_index: u32,
    block_count: u32,
}

impl DataRowGroup {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        group_ordinal: u32,
        row_count: u32,
        first_row_ordinal: u64,
        min_row_id: u64,
        max_row_id: u64,
        first_block_index: u32,
        block_count: u32,
    ) -> FormatResult<Self> {
        if row_count == 0 || row_count > MAX_ROWS_PER_GROUP {
            return Err(limit(
                "rows per group",
                u64::from(row_count),
                u64::from(MAX_ROWS_PER_GROUP),
            ));
        }
        if max_row_id < min_row_id {
            return Err(invalid("row-group row-ID range is reversed"));
        }
        if block_count == 0 {
            return Err(invalid("row group has no blocks"));
        }
        Ok(Self {
            group_ordinal,
            row_count,
            first_row_ordinal,
            min_row_id,
            max_row_id,
            first_block_index,
            block_count,
        })
    }

    pub const fn group_ordinal(self) -> u32 {
        self.group_ordinal
    }

    pub const fn row_count(self) -> u32 {
        self.row_count
    }

    pub const fn first_row_ordinal(self) -> u64 {
        self.first_row_ordinal
    }

    pub const fn min_row_id(self) -> u64 {
        self.min_row_id
    }

    pub const fn max_row_id(self) -> u64 {
        self.max_row_id
    }

    pub const fn first_block_index(self) -> u32 {
        self.first_block_index
    }

    pub const fn block_count(self) -> u32 {
        self.block_count
    }
}

fn derive_row_groups(
    header: &DataArtifactHeader,
    blocks: &[DataBlockSpec],
) -> FormatResult<Vec<DataRowGroup>> {
    let mut groups = Vec::with_capacity(header.row_group_count() as usize);
    let mut first_row_ordinal = 0_u64;
    let mut block_cursor = 0_usize;
    for group_ordinal in 0..header.row_group_count() {
        let first_block_index = block_cursor;
        while block_cursor < blocks.len()
            && blocks[block_cursor].row_group_ordinal() == group_ordinal
        {
            block_cursor += 1;
        }
        let group_blocks = &blocks[first_block_index..block_cursor];
        let mut row_id_blocks = group_blocks
            .iter()
            .filter(|block| block.kind() == DataBlockKind::RowIds);
        let row_ids = row_id_blocks
            .next()
            .ok_or_else(|| invalid("row group has no row-ID block"))?;
        if row_id_blocks.next().is_some() {
            return Err(invalid("row group has multiple row-ID blocks"));
        }
        let row_count = u32::try_from(row_ids.item_count())
            .map_err(|_| limit("rows per group", row_ids.item_count(), u64::from(u32::MAX)))?;
        let (min_row_id, max_row_id) = row_ids
            .row_id_bounds()
            .ok_or_else(|| invalid("row-ID block was not built from canonical row IDs"))?;
        let block_count = u32::try_from(group_blocks.len())
            .map_err(|_| invalid("row-group block count does not fit u32"))?;
        groups.push(DataRowGroup::new(
            group_ordinal,
            row_count,
            first_row_ordinal,
            min_row_id,
            max_row_id,
            u32::try_from(first_block_index)
                .map_err(|_| invalid("first block index does not fit u32"))?,
            block_count,
        )?);
        first_row_ordinal = first_row_ordinal
            .checked_add(u64::from(row_count))
            .ok_or_else(|| invalid("row ordinal prefix sum overflows"))?;
    }
    if block_cursor != blocks.len() {
        return Err(invalid("block references an undeclared row group"));
    }
    if first_row_ordinal != header.row_count() {
        return Err(invalid("row-group counts do not sum to header row count"));
    }
    Ok(groups)
}

fn derive_columns(
    header: &DataArtifactHeader,
    specs: &[DataColumnSpec],
    groups: &[DataRowGroup],
    statistics: &[DataStatistics],
    blocks: &[DataBlockSpec],
) -> FormatResult<Vec<DataColumn>> {
    validate_unique_column_ids(specs)?;
    let (first_block_indexes, block_counts) = collect_column_block_map(blocks, specs.len())?;
    for block in blocks
        .iter()
        .filter(|block| block.kind() == DataBlockKind::Column)
    {
        let ordinal = block.column_ordinal() as usize;
        validate_column_block_spec(specs[ordinal], block, groups)?;
    }
    let first_statistics_indexes = collect_first_statistics_indexes(statistics, specs.len())?;
    let mut columns = Vec::with_capacity(specs.len());
    for (ordinal, spec) in specs.iter().copied().enumerate() {
        if block_counts[ordinal] != header.row_group_count() {
            return Err(invalid("column does not have one block per row group"));
        }
        columns.push(DataColumn::new(
            spec.column_id(),
            ordinal as u32,
            spec.data_type(),
            spec.nullable(),
            first_block_indexes[ordinal],
            block_counts[ordinal],
            first_statistics_indexes[ordinal],
        ));
    }
    Ok(columns)
}

pub(crate) fn derive_columns_from_refs(
    header: &DataArtifactHeader,
    specs: &[DataColumnSpec],
    groups: &[DataRowGroup],
    statistics: &[DataStatistics],
    blocks: &[DataBlockRef],
) -> FormatResult<Vec<DataColumn>> {
    validate_unique_column_ids(specs)?;
    let (first_block_indexes, block_counts) = collect_column_block_map(blocks, specs.len())?;
    for block in blocks
        .iter()
        .filter(|block| block.kind() == DataBlockKind::Column)
    {
        validate_column_block_ref(block, groups)?;
    }
    let first_statistics_indexes = collect_first_statistics_indexes(statistics, specs.len())?;
    let mut columns = Vec::with_capacity(specs.len());
    for (ordinal, spec) in specs.iter().copied().enumerate() {
        if block_counts[ordinal] != header.row_group_count() {
            return Err(invalid("column does not have one block per row group"));
        }
        columns.push(DataColumn::new(
            spec.column_id(),
            ordinal as u32,
            spec.data_type(),
            spec.nullable(),
            first_block_indexes[ordinal],
            block_counts[ordinal],
            first_statistics_indexes[ordinal],
        ));
    }
    Ok(columns)
}

fn validate_unique_column_ids(specs: &[DataColumnSpec]) -> FormatResult<()> {
    let mut identities = specs
        .iter()
        .map(|spec| spec.column_id())
        .collect::<Vec<_>>();
    identities.sort_unstable();
    if identities.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("column IDs are not unique"));
    }
    Ok(())
}

fn collect_first_statistics_indexes(
    statistics: &[DataStatistics],
    column_count: usize,
) -> FormatResult<Vec<u32>> {
    let mut first = vec![u32::MAX; column_count];
    for (index, entry) in statistics.iter().enumerate() {
        let slot = first
            .get_mut(entry.column_ordinal() as usize)
            .ok_or_else(|| invalid("statistics column ordinal is out of range"))?;
        if *slot == u32::MAX {
            *slot = u32::try_from(index)
                .map_err(|_| invalid("statistics entry index does not fit u32"))?;
        }
    }
    Ok(first)
}

pub(crate) trait ColumnBlockDescriptor {
    fn kind(&self) -> DataBlockKind;
    fn column_ordinal(&self) -> u32;
}

impl ColumnBlockDescriptor for DataBlockSpec {
    fn kind(&self) -> DataBlockKind {
        self.kind()
    }

    fn column_ordinal(&self) -> u32 {
        self.column_ordinal()
    }
}

impl ColumnBlockDescriptor for DataBlockRef {
    fn kind(&self) -> DataBlockKind {
        self.kind()
    }

    fn column_ordinal(&self) -> u32 {
        self.column_ordinal()
    }
}

pub(crate) fn collect_column_block_map<B: ColumnBlockDescriptor>(
    blocks: &[B],
    column_count: usize,
) -> FormatResult<(Vec<u32>, Vec<u32>)> {
    let mut first = vec![u32::MAX; column_count];
    let mut counts = vec![0_u32; column_count];
    for (index, block) in blocks.iter().enumerate() {
        if block.kind() != DataBlockKind::Column {
            continue;
        }
        let ordinal = block.column_ordinal() as usize;
        let first_slot = first
            .get_mut(ordinal)
            .ok_or_else(|| invalid("column block ordinal is out of range"))?;
        if *first_slot == u32::MAX {
            *first_slot =
                u32::try_from(index).map_err(|_| invalid("first block index does not fit u32"))?;
        }
        counts[ordinal] = counts[ordinal]
            .checked_add(1)
            .ok_or_else(|| invalid("column block count does not fit u32"))?;
    }
    Ok((first, counts))
}

fn validate_column_block_ref(block: &DataBlockRef, groups: &[DataRowGroup]) -> FormatResult<()> {
    let group = groups
        .get(block.row_group_ordinal() as usize)
        .ok_or_else(|| invalid("column block row group is out of range"))?;
    if block.item_count() != u64::from(group.row_count()) {
        return Err(invalid("column block count differs from row group"));
    }
    Ok(())
}

fn validate_column_block_spec(
    expected: DataColumnSpec,
    block: &DataBlockSpec,
    groups: &[DataRowGroup],
) -> FormatResult<()> {
    if block.column_spec() != Some(expected) {
        return Err(invalid("column block descriptor differs from column spec"));
    }
    let group = groups
        .get(block.row_group_ordinal() as usize)
        .ok_or_else(|| invalid("column block row group is out of range"))?;
    if block.item_count() != u64::from(group.row_count()) {
        return Err(invalid("column block count differs from row group"));
    }
    Ok(())
}

pub(crate) fn validate_block_coordinates(
    header: &DataArtifactHeader,
    block: &DataBlockSpec,
) -> FormatResult<()> {
    if block.row_group_ordinal() >= header.row_group_count() {
        return Err(invalid("block row-group ordinal is out of range"));
    }
    if block.kind() != DataBlockKind::RowIds && block.column_ordinal() >= header.column_count() {
        return Err(invalid("block column ordinal is out of range"));
    }
    Ok(())
}

pub(crate) fn validate_kind_layout(
    kind: DataBlockKind,
    layout: DataLayout,
    column_ordinal: u32,
) -> FormatResult<()> {
    let valid = match kind {
        DataBlockKind::RowIds => column_ordinal == u32::MAX && layout == DataLayout::RowIdsDelta,
        DataBlockKind::Column => {
            column_ordinal != u32::MAX
                && matches!(
                    layout,
                    DataLayout::PlainValues | DataLayout::DictionaryValues
                )
        }
        DataBlockKind::Bloom => column_ordinal != u32::MAX && layout == DataLayout::Bloom,
    };
    if !valid {
        return Err(invalid("block kind/layout/column combination is invalid"));
    }
    Ok(())
}

pub(crate) fn validate_block_lengths(
    codec: DataPhysicalCodec,
    stored_length: u64,
    logical_length: u64,
) -> FormatResult<()> {
    if stored_length == 0 || logical_length == 0 {
        return Err(invalid("block range cannot be empty"));
    }
    if stored_length > MAX_STORED_BYTES_PER_BLOCK {
        return Err(limit(
            "stored block bytes",
            stored_length,
            MAX_STORED_BYTES_PER_BLOCK,
        ));
    }
    if logical_length > MAX_LOGICAL_BYTES_PER_BLOCK {
        return Err(limit(
            "logical block bytes",
            logical_length,
            MAX_LOGICAL_BYTES_PER_BLOCK,
        ));
    }
    if codec == DataPhysicalCodec::None && logical_length != stored_length {
        return Err(invalid("uncompressed block lengths differ"));
    }
    let maximum_logical = stored_length.saturating_mul(MAX_COMPRESSION_RATIO);
    if codec == DataPhysicalCodec::Lz4 && logical_length > maximum_logical {
        return Err(limit(
            "block compression ratio",
            logical_length,
            maximum_logical,
        ));
    }
    Ok(())
}

pub(crate) const fn invalid(detail: &'static str) -> FormatError {
    FormatError::InvalidDataArtifact { detail }
}

pub(crate) const fn limit(field: &'static str, actual: u64, limit: u64) -> FormatError {
    FormatError::DataArtifactLimitExceeded {
        field,
        actual,
        limit,
    }
}
