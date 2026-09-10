use crate::payload::common::{validate_version, CanonicalSql};
use crate::{CatalogDataType, CatalogError, CatalogResult};
use radixdb_core::DataType;

pub const COLUMN_FLAG_AUTO_INCREMENT: u64 = 1 << 0;
const KNOWN_COLUMN_FLAGS: u64 = COLUMN_FLAG_AUTO_INCREMENT;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPayload {
    ordinal: u32,
    data_type: CatalogDataType,
    nullable: bool,
    auto_increment: bool,
    default_sql: Option<CanonicalSql>,
    generated_sql: Option<CanonicalSql>,
}

impl ColumnPayload {
    pub fn new(
        ordinal: u32,
        data_type: CatalogDataType,
        nullable: bool,
        default_sql: Option<String>,
        generated_sql: Option<String>,
    ) -> CatalogResult<Self> {
        Self::new_with_auto_increment(
            ordinal,
            data_type,
            nullable,
            false,
            default_sql,
            generated_sql,
        )
    }

    pub fn new_with_auto_increment(
        ordinal: u32,
        data_type: CatalogDataType,
        nullable: bool,
        auto_increment: bool,
        default_sql: Option<String>,
        generated_sql: Option<String>,
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            u64::from(auto_increment) * COLUMN_FLAG_AUTO_INCREMENT,
            ordinal,
            data_type,
            nullable,
            default_sql,
            generated_sql,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        version: u16,
        flags: u64,
        ordinal: u32,
        data_type: CatalogDataType,
        nullable: bool,
        default_sql: Option<String>,
        generated_sql: Option<String>,
    ) -> CatalogResult<Self> {
        validate_version("column", version)?;
        if flags & !KNOWN_COLUMN_FLAGS != 0 {
            return Err(CatalogError::UnknownPayloadFlags {
                kind: "column",
                flags,
            });
        }
        let auto_increment = flags & COLUMN_FLAG_AUTO_INCREMENT != 0;
        let default_sql = default_sql
            .map(|sql| CanonicalSql::new("column.default_sql", sql))
            .transpose()?;
        let generated_sql = generated_sql
            .map(|sql| CanonicalSql::new("column.generated_sql", sql))
            .transpose()?;
        if default_sql.is_some() && generated_sql.is_some() {
            return Err(CatalogError::InvalidColumnPayload {
                detail: "a generated column cannot also have a default",
            });
        }
        if auto_increment && !matches!(data_type.logical_type(), DataType::Integer | DataType::Uuid)
        {
            return Err(CatalogError::InvalidColumnPayload {
                detail: "AUTO_INCREMENT requires INTEGER or UUID",
            });
        }
        if auto_increment && generated_sql.is_some() {
            return Err(CatalogError::InvalidColumnPayload {
                detail: "AUTO_INCREMENT column cannot also be generated",
            });
        }
        Ok(Self {
            ordinal,
            data_type,
            nullable,
            auto_increment,
            default_sql,
            generated_sql,
        })
    }

    pub const fn version(&self) -> u16 {
        super::PAYLOAD_VERSION
    }

    pub const fn flags(&self) -> u64 {
        if self.auto_increment {
            COLUMN_FLAG_AUTO_INCREMENT
        } else {
            0
        }
    }

    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub const fn data_type(&self) -> CatalogDataType {
        self.data_type
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    pub const fn auto_increment(&self) -> bool {
        self.auto_increment
    }

    pub fn default_sql(&self) -> Option<&CanonicalSql> {
        self.default_sql.as_ref()
    }

    pub fn generated_sql(&self) -> Option<&CanonicalSql> {
        self.generated_sql.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_increment_is_typed_and_fail_closed() {
        let integer = CatalogDataType::scalar(DataType::Integer).unwrap();
        let auto =
            ColumnPayload::new_with_auto_increment(0, integer, false, true, None, None).unwrap();
        assert!(auto.auto_increment());
        assert_eq!(auto.flags(), COLUMN_FLAG_AUTO_INCREMENT);

        let text = CatalogDataType::scalar(DataType::Text).unwrap();
        assert!(ColumnPayload::new_with_auto_increment(0, text, false, true, None, None).is_err());
        assert!(ColumnPayload::from_fields(
            super::super::PAYLOAD_VERSION,
            COLUMN_FLAG_AUTO_INCREMENT << 1,
            0,
            integer,
            false,
            None,
            None,
        )
        .is_err());
        assert!(ColumnPayload::new_with_auto_increment(
            0,
            integer,
            false,
            true,
            None,
            Some("id + 1".into()),
        )
        .is_err());
    }
}
