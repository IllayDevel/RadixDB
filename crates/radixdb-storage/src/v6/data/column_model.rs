use radixdb_catalog::{CatalogDataType, ObjectId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataColumnSpec {
    column_id: ObjectId,
    data_type: CatalogDataType,
    nullable: bool,
}

impl DataColumnSpec {
    pub const fn new(column_id: ObjectId, data_type: CatalogDataType, nullable: bool) -> Self {
        Self {
            column_id,
            data_type,
            nullable,
        }
    }

    pub const fn column_id(self) -> ObjectId {
        self.column_id
    }

    pub const fn data_type(self) -> CatalogDataType {
        self.data_type
    }

    pub const fn nullable(self) -> bool {
        self.nullable
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataColumn {
    column_id: ObjectId,
    ordinal: u32,
    data_type: CatalogDataType,
    nullable: bool,
    first_block_index: u32,
    block_count: u32,
    statistics_entry_index: u32,
}

impl DataColumn {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        column_id: ObjectId,
        ordinal: u32,
        data_type: CatalogDataType,
        nullable: bool,
        first_block_index: u32,
        block_count: u32,
        statistics_entry_index: u32,
    ) -> Self {
        Self {
            column_id,
            ordinal,
            data_type,
            nullable,
            first_block_index,
            block_count,
            statistics_entry_index,
        }
    }

    pub const fn column_id(self) -> ObjectId {
        self.column_id
    }

    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }

    pub const fn data_type(self) -> CatalogDataType {
        self.data_type
    }

    pub const fn nullable(self) -> bool {
        self.nullable
    }

    pub const fn first_block_index(self) -> u32 {
        self.first_block_index
    }

    pub const fn block_count(self) -> u32 {
        self.block_count
    }

    pub const fn statistics_entry_index(self) -> u32 {
        self.statistics_entry_index
    }
}
