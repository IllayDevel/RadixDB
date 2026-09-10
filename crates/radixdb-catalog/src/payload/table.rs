use crate::payload::common::{
    ordered_unique_ids, sorted_unique_ids, validate_flags, validate_version,
};
use crate::{CatalogError, CatalogResult, ObjectId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePayload {
    column_ids: Vec<ObjectId>,
    constraint_ids: Vec<ObjectId>,
    index_ids: Vec<ObjectId>,
    primary_key_constraint_id: Option<ObjectId>,
    created_unix_ns: u64,
    updated_unix_ns: u64,
}

/// Decoded table fields before invariant validation and canonicalization.
///
/// Keeping the persisted envelope in one named value prevents callers from
/// accidentally swapping adjacent ID collections or timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePayloadFields {
    pub version: u16,
    pub flags: u64,
    pub column_ids: Vec<ObjectId>,
    pub constraint_ids: Vec<ObjectId>,
    pub index_ids: Vec<ObjectId>,
    pub primary_key_constraint_id: Option<ObjectId>,
    pub created_unix_ns: u64,
    pub updated_unix_ns: u64,
}

impl TablePayload {
    pub fn new(
        column_ids: Vec<ObjectId>,
        constraint_ids: Vec<ObjectId>,
        index_ids: Vec<ObjectId>,
        primary_key_constraint_id: Option<ObjectId>,
    ) -> CatalogResult<Self> {
        Self::new_with_timestamps(
            column_ids,
            constraint_ids,
            index_ids,
            primary_key_constraint_id,
            0,
            0,
        )
    }

    pub fn new_with_timestamps(
        column_ids: Vec<ObjectId>,
        constraint_ids: Vec<ObjectId>,
        index_ids: Vec<ObjectId>,
        primary_key_constraint_id: Option<ObjectId>,
        created_unix_ns: u64,
        updated_unix_ns: u64,
    ) -> CatalogResult<Self> {
        Self::from_fields(TablePayloadFields {
            version: super::PAYLOAD_VERSION,
            flags: 0,
            column_ids,
            constraint_ids,
            index_ids,
            primary_key_constraint_id,
            created_unix_ns,
            updated_unix_ns,
        })
    }

    pub fn from_fields(fields: TablePayloadFields) -> CatalogResult<Self> {
        let TablePayloadFields {
            version,
            flags,
            column_ids,
            constraint_ids,
            index_ids,
            primary_key_constraint_id,
            created_unix_ns,
            updated_unix_ns,
        } = fields;
        validate_version("table", version)?;
        validate_flags("table", flags)?;
        let column_ids = ordered_unique_ids("table.column_ids", column_ids, false)?;
        let constraint_ids = sorted_unique_ids("table.constraint_ids", constraint_ids, true)?;
        let index_ids = sorted_unique_ids("table.index_ids", index_ids, true)?;
        if let Some(primary_key) = primary_key_constraint_id {
            if constraint_ids.binary_search(&primary_key).is_err() {
                return Err(CatalogError::InvalidTablePayload {
                    detail: "primary key is absent from constraint_ids",
                });
            }
        }
        if (created_unix_ns == 0) != (updated_unix_ns == 0) {
            return Err(CatalogError::InvalidTablePayload {
                detail: "table timestamps must either both be zero or both be present",
            });
        }
        if updated_unix_ns < created_unix_ns {
            return Err(CatalogError::InvalidTablePayload {
                detail: "table update timestamp precedes its creation timestamp",
            });
        }
        Ok(Self {
            column_ids,
            constraint_ids,
            index_ids,
            primary_key_constraint_id,
            created_unix_ns,
            updated_unix_ns,
        })
    }

    pub const fn version(&self) -> u16 {
        super::PAYLOAD_VERSION
    }

    pub const fn flags(&self) -> u64 {
        0
    }

    pub fn column_ids(&self) -> &[ObjectId] {
        &self.column_ids
    }

    pub fn constraint_ids(&self) -> &[ObjectId] {
        &self.constraint_ids
    }

    pub fn index_ids(&self) -> &[ObjectId] {
        &self.index_ids
    }

    pub const fn primary_key_constraint_id(&self) -> Option<ObjectId> {
        self.primary_key_constraint_id
    }

    pub const fn created_unix_ns(&self) -> u64 {
        self.created_unix_ns
    }

    pub const fn updated_unix_ns(&self) -> u64 {
        self.updated_unix_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meaningful_column_order_is_preserved_while_sets_are_canonical() {
        let first = ObjectId::new();
        let second = ObjectId::new();
        let primary = ObjectId::new();
        let other = ObjectId::new();
        let payload = TablePayload::new(
            vec![second, first],
            vec![primary, other],
            vec![second, first],
            Some(primary),
        )
        .unwrap();
        assert_eq!(payload.column_ids(), &[second, first]);
        assert!(payload.constraint_ids().is_sorted());
        assert!(payload.index_ids().is_sorted());
    }

    #[test]
    fn missing_primary_key_and_duplicate_columns_are_rejected() {
        let column = ObjectId::new();
        assert!(TablePayload::new(vec![column, column], vec![], vec![], None).is_err());
        assert!(TablePayload::new(
            vec![column],
            vec![ObjectId::new()],
            vec![],
            Some(ObjectId::new())
        )
        .is_err());
    }
}
