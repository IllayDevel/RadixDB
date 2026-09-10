use radixdb_catalog::ObjectId;
use radixdb_core::DataType;

use super::exact_validation::ValidatedExactPages;

use super::super::{
    ArtifactId, ArtifactKind, ArtifactRef, ArtifactSliceRef, CatalogGeneration, DataArtifactLayout,
    DatabaseGeneration, DatabaseId, FormatError, FormatResult, IndexSectionKind, SegmentId,
};

pub const INDEX_HEADER_BYTES: usize = 256;
pub const INDEX_FOOTER_BYTES: usize = 48;
pub const INDEX_ACCELERATOR_ENTRY_BYTES: usize = 128;
pub const INDEX_SECTION_ENTRY_BYTES: usize = 64;
pub const INDEX_PAGE_ENTRY_BYTES: usize = 64;

pub const MAX_ACCELERATORS_PER_INDEX_ARTIFACT: u32 = 4_096;
pub const MAX_INDEX_SECTIONS: u32 = 16_384;
pub const MAX_INDEX_PAGES: u64 = 4_194_304;
pub const MAX_INDEX_DIRECTORY_BYTES: u64 = 320 * 1024 * 1024;
pub const MAX_KEY_COLUMNS: u32 = 64;
pub const MAX_ENTRIES_PER_INDEX_PAGE: u64 = 1_048_576;
pub const MAX_STORED_BYTES_PER_INDEX_PAGE: u64 = 64 * 1024 * 1024;
pub const MAX_LOGICAL_BYTES_PER_INDEX_PAGE: u64 = 256 * 1024 * 1024;
pub const MAX_INDEX_COMPRESSION_RATIO: u64 = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexArtifactHeader {
    artifact_id: ArtifactId,
    database_id: DatabaseId,
    table_id: ObjectId,
    segment_id: SegmentId,
    data_artifact_id: ArtifactId,
    data_body_sha256: [u8; 32],
    creation_generation: DatabaseGeneration,
    catalog_generation: CatalogGeneration,
    source_row_count: u64,
}

impl IndexArtifactHeader {
    pub fn for_data(
        artifact_id: ArtifactId,
        creation_generation: DatabaseGeneration,
        catalog_generation: CatalogGeneration,
        data: &DataArtifactLayout,
    ) -> FormatResult<Self> {
        if data.reference().kind() != ArtifactKind::Data {
            return Err(invalid("source artifact is not data"));
        }
        Ok(Self {
            artifact_id,
            database_id: data.header().database_id(),
            table_id: data.header().table_id(),
            segment_id: data.header().segment_id(),
            data_artifact_id: data.reference().id(),
            data_body_sha256: *data.reference().body_sha256(),
            creation_generation,
            catalog_generation,
            source_row_count: data.header().row_count(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn decoded(
        artifact_id: ArtifactId,
        database_id: DatabaseId,
        table_id: ObjectId,
        segment_id: SegmentId,
        data_artifact_id: ArtifactId,
        data_body_sha256: [u8; 32],
        creation_generation: DatabaseGeneration,
        catalog_generation: CatalogGeneration,
        source_row_count: u64,
    ) -> Self {
        Self {
            artifact_id,
            database_id,
            table_id,
            segment_id,
            data_artifact_id,
            data_body_sha256,
            creation_generation,
            catalog_generation,
            source_row_count,
        }
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

    pub const fn data_artifact_id(self) -> ArtifactId {
        self.data_artifact_id
    }

    pub const fn data_body_sha256(&self) -> &[u8; 32] {
        &self.data_body_sha256
    }

    pub const fn creation_generation(self) -> DatabaseGeneration {
        self.creation_generation
    }

    pub const fn catalog_generation(self) -> CatalogGeneration {
        self.catalog_generation
    }

    pub const fn source_row_count(self) -> u64 {
        self.source_row_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum IndexAcceleratorKind {
    Exact = 1,
    Ordered = 2,
    Hnsw = 3,
}

impl IndexAcceleratorKind {
    pub(crate) fn from_tag(tag: u16) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::Exact),
            2 => Ok(Self::Ordered),
            3 => Ok(Self::Hnsw),
            _ => Err(FormatError::UnknownIndexAcceleratorKind { tag }),
        }
    }

    pub const fn tag(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexSortDirection {
    Ascending = 1,
    Descending = 2,
}

impl IndexSortDirection {
    pub(crate) fn from_tag(tag: u8) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::Ascending),
            2 => Ok(Self::Descending),
            _ => Err(invalid("key sort direction is unknown")),
        }
    }

    pub const fn tag(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexNullsOrder {
    First = 1,
    Last = 2,
}

impl IndexNullsOrder {
    pub(crate) fn from_tag(tag: u8) -> FormatResult<Self> {
        match tag {
            1 => Ok(Self::First),
            2 => Ok(Self::Last),
            _ => Err(invalid("key null ordering is unknown")),
        }
    }

    pub const fn tag(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexKeyColumn {
    column_id: ObjectId,
    logical_type: DataType,
    direction: IndexSortDirection,
    nulls_order: IndexNullsOrder,
}

impl IndexKeyColumn {
    pub const fn new(
        column_id: ObjectId,
        logical_type: DataType,
        direction: IndexSortDirection,
        nulls_order: IndexNullsOrder,
    ) -> Self {
        Self {
            column_id,
            logical_type,
            direction,
            nulls_order,
        }
    }

    pub const fn column_id(self) -> ObjectId {
        self.column_id
    }

    pub const fn logical_type(self) -> DataType {
        self.logical_type
    }

    pub const fn direction(self) -> IndexSortDirection {
        self.direction
    }

    pub const fn nulls_order(self) -> IndexNullsOrder {
        self.nulls_order
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum IndexPageCodec {
    None = 0,
    Lz4 = 1,
}

impl IndexPageCodec {
    pub(crate) fn from_flags(flags: u32) -> FormatResult<Self> {
        match flags {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            _ => Err(invalid("index page has unknown flags")),
        }
    }

    pub const fn flags(self) -> u32 {
        self as u32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPageSpec {
    logical_bytes: Vec<u8>,
    codec: IndexPageCodec,
    item_count: u64,
    minimum_key_hash: u64,
    maximum_key_hash: u64,
}

impl IndexPageSpec {
    pub fn new(
        logical_bytes: Vec<u8>,
        codec: IndexPageCodec,
        item_count: u64,
        minimum_key_hash: u64,
        maximum_key_hash: u64,
    ) -> FormatResult<Self> {
        if logical_bytes.is_empty() {
            return Err(invalid("index page cannot be empty"));
        }
        if logical_bytes.len() as u64 > MAX_LOGICAL_BYTES_PER_INDEX_PAGE {
            return Err(limit(
                "logical page bytes",
                logical_bytes.len() as u64,
                MAX_LOGICAL_BYTES_PER_INDEX_PAGE,
            ));
        }
        if item_count == 0 || item_count > MAX_ENTRIES_PER_INDEX_PAGE {
            return Err(limit(
                "entries per page",
                item_count,
                MAX_ENTRIES_PER_INDEX_PAGE,
            ));
        }
        if maximum_key_hash < minimum_key_hash {
            return Err(invalid("index page key-hash range is reversed"));
        }
        Ok(Self {
            logical_bytes,
            codec,
            item_count,
            minimum_key_hash,
            maximum_key_hash,
        })
    }

    pub(crate) fn empty_exact(logical_bytes: Vec<u8>, codec: IndexPageCodec) -> FormatResult<Self> {
        if logical_bytes.is_empty() {
            return Err(invalid("empty exact page payload cannot be empty"));
        }
        if logical_bytes.len() as u64 > MAX_LOGICAL_BYTES_PER_INDEX_PAGE {
            return Err(limit(
                "logical page bytes",
                logical_bytes.len() as u64,
                MAX_LOGICAL_BYTES_PER_INDEX_PAGE,
            ));
        }
        Ok(Self {
            logical_bytes,
            codec,
            item_count: 0,
            minimum_key_hash: 0,
            maximum_key_hash: 0,
        })
    }

    pub fn logical_bytes(&self) -> &[u8] {
        &self.logical_bytes
    }

    pub const fn codec(&self) -> IndexPageCodec {
        self.codec
    }

    pub const fn item_count(&self) -> u64 {
        self.item_count
    }

    pub const fn minimum_key_hash(&self) -> u64 {
        self.minimum_key_hash
    }

    pub const fn maximum_key_hash(&self) -> u64 {
        self.maximum_key_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSectionSpec {
    kind: IndexSectionKind,
    metadata: Vec<u8>,
    pages: Vec<IndexPageSpec>,
    item_count: u64,
}

impl IndexSectionSpec {
    pub fn metadata(kind: IndexSectionKind, bytes: Vec<u8>, item_count: u64) -> FormatResult<Self> {
        if kind != IndexSectionKind::HnswMetadata {
            return Err(invalid(
                "only HNSW metadata is an external metadata section",
            ));
        }
        if bytes.is_empty() {
            return Err(invalid("index metadata section cannot be empty"));
        }
        Ok(Self {
            kind,
            metadata: bytes,
            pages: Vec::new(),
            item_count,
        })
    }

    pub fn pages(kind: IndexSectionKind, pages: Vec<IndexPageSpec>) -> FormatResult<Self> {
        if !matches!(
            kind,
            IndexSectionKind::ExactPages
                | IndexSectionKind::OrderedPages
                | IndexSectionKind::HnswNodes
                | IndexSectionKind::HnswAdjacency
        ) {
            return Err(invalid("section kind does not own index pages"));
        }
        if pages.is_empty() {
            return Err(invalid("paged index section cannot be empty"));
        }
        let item_count = pages.iter().try_fold(0_u64, |total, page| {
            total
                .checked_add(page.item_count())
                .ok_or_else(|| invalid("section item count overflows"))
        })?;
        Ok(Self {
            kind,
            metadata: Vec::new(),
            pages,
            item_count,
        })
    }

    pub const fn kind(&self) -> IndexSectionKind {
        self.kind
    }

    pub fn metadata_bytes(&self) -> &[u8] {
        &self.metadata
    }

    pub fn page_specs(&self) -> &[IndexPageSpec] {
        &self.pages
    }

    pub const fn item_count(&self) -> u64 {
        self.item_count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexAcceleratorSpec {
    logical_index_id: ObjectId,
    kind: IndexAcceleratorKind,
    unique: bool,
    constraint_owned: bool,
    definition_sha256: [u8; 32],
    key_columns: Vec<IndexKeyColumn>,
    indexed_item_count: u64,
    sections: Vec<IndexSectionSpec>,
}

impl IndexAcceleratorSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        logical_index_id: ObjectId,
        kind: IndexAcceleratorKind,
        unique: bool,
        constraint_owned: bool,
        definition_sha256: [u8; 32],
        key_columns: Vec<IndexKeyColumn>,
        indexed_item_count: u64,
        mut sections: Vec<IndexSectionSpec>,
    ) -> FormatResult<Self> {
        if key_columns.is_empty() || key_columns.len() as u64 > u64::from(MAX_KEY_COLUMNS) {
            return Err(limit(
                "key columns",
                key_columns.len() as u64,
                u64::from(MAX_KEY_COLUMNS),
            ));
        }
        if key_columns.iter().enumerate().any(|(index, column)| {
            key_columns[..index]
                .iter()
                .any(|previous| previous.column_id() == column.column_id())
        }) {
            return Err(invalid("index key columns are not unique"));
        }
        validate_accelerator_contract(kind, unique, constraint_owned, &key_columns)?;
        sections.sort_by_key(|section| section.kind().tag());
        validate_section_shape(kind, &sections)?;
        if indexed_item_count == 0 && !is_canonical_empty_exact(kind, unique, &sections) {
            return Err(invalid(
                "zero-entry accelerator is not canonical nullable UNIQUE exact state",
            ));
        }
        if sections.iter().any(|section| {
            matches!(
                section.kind(),
                IndexSectionKind::ExactPages
                    | IndexSectionKind::OrderedPages
                    | IndexSectionKind::HnswNodes
            ) && section.item_count() > indexed_item_count
        }) {
            return Err(invalid(
                "primary index section item count exceeds indexed items",
            ));
        }
        Ok(Self {
            logical_index_id,
            kind,
            unique,
            constraint_owned,
            definition_sha256,
            key_columns,
            indexed_item_count,
            sections,
        })
    }

    pub const fn logical_index_id(&self) -> ObjectId {
        self.logical_index_id
    }

    pub const fn kind(&self) -> IndexAcceleratorKind {
        self.kind
    }

    pub const fn unique(&self) -> bool {
        self.unique
    }

    pub const fn constraint_owned(&self) -> bool {
        self.constraint_owned
    }

    pub const fn definition_sha256(&self) -> &[u8; 32] {
        &self.definition_sha256
    }

    pub fn key_columns(&self) -> &[IndexKeyColumn] {
        &self.key_columns
    }

    pub const fn indexed_item_count(&self) -> u64 {
        self.indexed_item_count
    }

    pub fn sections(&self) -> &[IndexSectionSpec] {
        &self.sections
    }
}

fn validate_section_shape(
    kind: IndexAcceleratorKind,
    sections: &[IndexSectionSpec],
) -> FormatResult<()> {
    let expected: &[IndexSectionKind] = match kind {
        IndexAcceleratorKind::Exact => &[IndexSectionKind::ExactPages],
        IndexAcceleratorKind::Ordered => &[IndexSectionKind::OrderedPages],
        IndexAcceleratorKind::Hnsw => &[
            IndexSectionKind::HnswMetadata,
            IndexSectionKind::HnswNodes,
            IndexSectionKind::HnswAdjacency,
        ],
    };
    if sections.len() != expected.len()
        || sections
            .iter()
            .zip(expected)
            .any(|(section, expected)| section.kind() != *expected)
    {
        return Err(invalid("accelerator section set does not match its kind"));
    }
    Ok(())
}

fn is_canonical_empty_exact(
    kind: IndexAcceleratorKind,
    unique: bool,
    sections: &[IndexSectionSpec],
) -> bool {
    kind == IndexAcceleratorKind::Exact
        && unique
        && sections.len() == 1
        && sections[0].kind() == IndexSectionKind::ExactPages
        && sections[0].item_count() == 0
        && sections[0].page_specs().len() == 1
        && sections[0].page_specs()[0].item_count() == 0
        && sections[0].page_specs()[0].minimum_key_hash() == 0
        && sections[0].page_specs()[0].maximum_key_hash() == 0
}

pub(crate) const fn is_ordered_key_type(data_type: DataType) -> bool {
    matches!(
        data_type,
        DataType::Integer
            | DataType::Float
            | DataType::Text
            | DataType::Boolean
            | DataType::Timestamp
            | DataType::Uuid
            | DataType::Decimal
            | DataType::Date
            | DataType::Bytes
    )
}

pub(crate) fn validate_accelerator_contract(
    kind: IndexAcceleratorKind,
    unique: bool,
    constraint_owned: bool,
    key_columns: &[IndexKeyColumn],
) -> FormatResult<()> {
    if constraint_owned && !unique {
        return Err(invalid(
            "constraint-owned accelerator must enforce UNIQUE semantics",
        ));
    }
    if kind == IndexAcceleratorKind::Ordered
        && key_columns
            .iter()
            .any(|column| !is_ordered_key_type(column.logical_type()))
    {
        return Err(invalid(
            "ordered index contains a logical type without total B-tree order",
        ));
    }
    if kind == IndexAcceleratorKind::Hnsw {
        if key_columns.len() != 1 || key_columns[0].logical_type() != DataType::Vector {
            return Err(invalid(
                "HNSW accelerator must reference exactly one VECTOR column",
            ));
        }
        if unique || constraint_owned {
            return Err(invalid(
                "HNSW accelerator cannot own UNIQUE or constraint semantics",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexArtifactInput {
    header: IndexArtifactHeader,
    accelerators: Vec<IndexAcceleratorSpec>,
}

impl IndexArtifactInput {
    pub fn new(
        header: IndexArtifactHeader,
        data: &DataArtifactLayout,
        mut accelerators: Vec<IndexAcceleratorSpec>,
    ) -> FormatResult<Self> {
        validate_data_binding(header, data)?;
        if accelerators.is_empty() {
            return Err(invalid("empty index artifact is forbidden"));
        }
        if accelerators.len() as u64 > u64::from(MAX_ACCELERATORS_PER_INDEX_ARTIFACT) {
            return Err(limit(
                "accelerator count",
                accelerators.len() as u64,
                u64::from(MAX_ACCELERATORS_PER_INDEX_ARTIFACT),
            ));
        }
        accelerators.sort_by_key(IndexAcceleratorSpec::logical_index_id);
        if accelerators
            .windows(2)
            .any(|pair| pair[0].logical_index_id() == pair[1].logical_index_id())
        {
            return Err(invalid("logical index IDs are not unique"));
        }
        let mut section_count = 0_u64;
        let mut page_count = 0_u64;
        for accelerator in &accelerators {
            if accelerator.indexed_item_count() > header.source_row_count() {
                return Err(invalid("indexed item count exceeds source rows"));
            }
            let mut has_nullable_key_column = false;
            for key in accelerator.key_columns() {
                let source = data
                    .columns()
                    .iter()
                    .find(|column| column.column_id() == key.column_id())
                    .ok_or_else(|| invalid("index key column is absent from source data"))?;
                if source.data_type().logical_type() != key.logical_type() {
                    return Err(invalid("index key type differs from source data"));
                }
                has_nullable_key_column |= source.nullable();
            }
            if accelerator.indexed_item_count() == 0 && !has_nullable_key_column {
                return Err(invalid(
                    "zero-entry UNIQUE accelerator has no nullable source key column",
                ));
            }
            section_count = section_count
                .checked_add(1 + accelerator.sections().len() as u64)
                .ok_or_else(|| invalid("section count overflows"))?;
            for section in accelerator.sections() {
                page_count = page_count
                    .checked_add(section.page_specs().len() as u64)
                    .ok_or_else(|| invalid("page count overflows"))?;
            }
        }
        if section_count > u64::from(MAX_INDEX_SECTIONS) {
            return Err(limit(
                "section count",
                section_count,
                u64::from(MAX_INDEX_SECTIONS),
            ));
        }
        if page_count > MAX_INDEX_PAGES {
            return Err(limit("page count", page_count, MAX_INDEX_PAGES));
        }
        let directory_bytes = (accelerators.len() as u64)
            .checked_mul(INDEX_ACCELERATOR_ENTRY_BYTES as u64)
            .and_then(|value| value.checked_add(section_count * INDEX_SECTION_ENTRY_BYTES as u64))
            .and_then(|value| value.checked_add(page_count * INDEX_PAGE_ENTRY_BYTES as u64))
            .ok_or_else(|| invalid("index directory byte count overflows"))?;
        if directory_bytes > MAX_INDEX_DIRECTORY_BYTES {
            return Err(limit(
                "directory bytes",
                directory_bytes,
                MAX_INDEX_DIRECTORY_BYTES,
            ));
        }
        Ok(Self {
            header,
            accelerators,
        })
    }

    pub const fn header(&self) -> IndexArtifactHeader {
        self.header
    }

    pub fn accelerators(&self) -> &[IndexAcceleratorSpec] {
        &self.accelerators
    }
}

pub(super) fn validate_data_binding(
    header: IndexArtifactHeader,
    data: &DataArtifactLayout,
) -> FormatResult<()> {
    if data.reference().kind() != ArtifactKind::Data
        || header.database_id() != data.header().database_id()
        || header.table_id() != data.header().table_id()
        || header.segment_id() != data.header().segment_id()
        || header.data_artifact_id() != data.reference().id()
        || header.data_body_sha256() != data.reference().body_sha256()
        || header.source_row_count() != data.header().row_count()
    {
        return Err(invalid("index header does not match source data artifact"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexAccelerator {
    logical_index_id: ObjectId,
    kind: IndexAcceleratorKind,
    unique: bool,
    constraint_owned: bool,
    definition_sha256: [u8; 32],
    key_columns: Vec<IndexKeyColumn>,
    first_section_index: u32,
    section_count: u32,
    indexed_item_count: u64,
}

impl IndexAccelerator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        logical_index_id: ObjectId,
        kind: IndexAcceleratorKind,
        unique: bool,
        constraint_owned: bool,
        definition_sha256: [u8; 32],
        key_columns: Vec<IndexKeyColumn>,
        first_section_index: u32,
        section_count: u32,
        indexed_item_count: u64,
    ) -> Self {
        Self {
            logical_index_id,
            kind,
            unique,
            constraint_owned,
            definition_sha256,
            key_columns,
            first_section_index,
            section_count,
            indexed_item_count,
        }
    }

    pub const fn logical_index_id(&self) -> ObjectId {
        self.logical_index_id
    }

    pub const fn kind(&self) -> IndexAcceleratorKind {
        self.kind
    }

    pub const fn unique(&self) -> bool {
        self.unique
    }

    pub const fn constraint_owned(&self) -> bool {
        self.constraint_owned
    }

    pub const fn definition_sha256(&self) -> &[u8; 32] {
        &self.definition_sha256
    }

    pub fn key_columns(&self) -> &[IndexKeyColumn] {
        &self.key_columns
    }

    pub const fn first_section_index(&self) -> u32 {
        self.first_section_index
    }

    pub const fn section_count(&self) -> u32 {
        self.section_count
    }

    pub const fn indexed_item_count(&self) -> u64 {
        self.indexed_item_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexSection {
    reference: ArtifactSliceRef,
    accelerator_ordinal: u32,
    first_page_index: u32,
    page_count: u32,
}

impl IndexSection {
    pub(crate) const fn new(
        reference: ArtifactSliceRef,
        accelerator_ordinal: u32,
        first_page_index: u32,
        page_count: u32,
    ) -> Self {
        Self {
            reference,
            accelerator_ordinal,
            first_page_index,
            page_count,
        }
    }

    pub const fn reference(self) -> ArtifactSliceRef {
        self.reference
    }

    pub const fn kind(self) -> IndexSectionKind {
        self.reference.section_kind()
    }

    pub const fn accelerator_ordinal(self) -> u32 {
        self.accelerator_ordinal
    }

    pub const fn first_page_index(self) -> u32 {
        self.first_page_index
    }

    pub const fn page_count(self) -> u32 {
        self.page_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexPage {
    section_index: u32,
    page_ordinal: u32,
    offset: u64,
    stored_length: u64,
    logical_length: u64,
    item_count: u64,
    minimum_key_hash: u64,
    maximum_key_hash: u64,
    stored_crc32: u32,
    codec: IndexPageCodec,
}

impl IndexPage {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        section_index: u32,
        page_ordinal: u32,
        offset: u64,
        stored_length: u64,
        logical_length: u64,
        item_count: u64,
        minimum_key_hash: u64,
        maximum_key_hash: u64,
        stored_crc32: u32,
        codec: IndexPageCodec,
    ) -> Self {
        Self {
            section_index,
            page_ordinal,
            offset,
            stored_length,
            logical_length,
            item_count,
            minimum_key_hash,
            maximum_key_hash,
            stored_crc32,
            codec,
        }
    }

    pub const fn section_index(self) -> u32 {
        self.section_index
    }

    pub const fn page_ordinal(self) -> u32 {
        self.page_ordinal
    }

    pub const fn offset(self) -> u64 {
        self.offset
    }

    pub const fn stored_length(self) -> u64 {
        self.stored_length
    }

    pub const fn logical_length(self) -> u64 {
        self.logical_length
    }

    pub const fn item_count(self) -> u64 {
        self.item_count
    }

    pub const fn minimum_key_hash(self) -> u64 {
        self.minimum_key_hash
    }

    pub const fn maximum_key_hash(self) -> u64 {
        self.maximum_key_hash
    }

    pub const fn stored_crc32(self) -> u32 {
        self.stored_crc32
    }

    pub const fn codec(self) -> IndexPageCodec {
        self.codec
    }
}

#[derive(Debug, Clone)]
pub struct IndexArtifactLayout {
    reference: ArtifactRef,
    header: IndexArtifactHeader,
    accelerators: Vec<IndexAccelerator>,
    sections: Vec<IndexSection>,
    pages: Vec<IndexPage>,
    pub(super) validated_exact_pages: ValidatedExactPages,
}

// Validation cache capacity and warmth do not change immutable layout identity.
impl PartialEq for IndexArtifactLayout {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.header == other.header
            && self.accelerators == other.accelerators
            && self.sections == other.sections
            && self.pages == other.pages
    }
}

impl Eq for IndexArtifactLayout {}

impl IndexArtifactLayout {
    pub(crate) fn new(
        reference: ArtifactRef,
        header: IndexArtifactHeader,
        accelerators: Vec<IndexAccelerator>,
        sections: Vec<IndexSection>,
        pages: Vec<IndexPage>,
        cache_pages: usize,
    ) -> Self {
        let validated_exact_pages = ValidatedExactPages::new(cache_pages);
        Self {
            reference,
            header,
            accelerators,
            sections,
            pages,
            validated_exact_pages,
        }
    }

    pub const fn reference(&self) -> ArtifactRef {
        self.reference
    }

    pub const fn header(&self) -> IndexArtifactHeader {
        self.header
    }

    pub fn accelerators(&self) -> &[IndexAccelerator] {
        &self.accelerators
    }

    pub fn sections(&self) -> &[IndexSection] {
        &self.sections
    }

    pub fn pages(&self) -> &[IndexPage] {
        &self.pages
    }
}

pub(crate) fn validate_page_lengths(
    codec: IndexPageCodec,
    stored_length: u64,
    logical_length: u64,
) -> FormatResult<()> {
    if stored_length == 0 || logical_length == 0 {
        return Err(invalid("index page range cannot be empty"));
    }
    if stored_length > MAX_STORED_BYTES_PER_INDEX_PAGE {
        return Err(limit(
            "stored page bytes",
            stored_length,
            MAX_STORED_BYTES_PER_INDEX_PAGE,
        ));
    }
    if logical_length > MAX_LOGICAL_BYTES_PER_INDEX_PAGE {
        return Err(limit(
            "logical page bytes",
            logical_length,
            MAX_LOGICAL_BYTES_PER_INDEX_PAGE,
        ));
    }
    if codec == IndexPageCodec::None && stored_length != logical_length {
        return Err(invalid("uncompressed index page lengths differ"));
    }
    if codec == IndexPageCodec::Lz4
        && logical_length > stored_length.saturating_mul(MAX_INDEX_COMPRESSION_RATIO)
    {
        return Err(limit(
            "page compression ratio",
            logical_length,
            stored_length.saturating_mul(MAX_INDEX_COMPRESSION_RATIO),
        ));
    }
    Ok(())
}

pub(crate) const fn invalid(detail: &'static str) -> FormatError {
    FormatError::InvalidIndexArtifact { detail }
}

pub(crate) const fn limit(field: &'static str, actual: u64, limit: u64) -> FormatError {
    FormatError::IndexArtifactLimitExceeded {
        field,
        actual,
        limit,
    }
}
