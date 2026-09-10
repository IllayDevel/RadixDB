use crate::payload::common::{sorted_unique_ids, validate_flags, validate_version, CanonicalSql};
use crate::{CatalogResult, ObjectId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewPayload {
    canonical_sql: CanonicalSql,
    dependency_ids: Vec<ObjectId>,
    output_signature: [u8; 32],
}

impl ViewPayload {
    pub fn new(
        canonical_sql: impl Into<String>,
        dependency_ids: Vec<ObjectId>,
        output_signature: [u8; 32],
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            0,
            canonical_sql,
            dependency_ids,
            output_signature,
        )
    }

    pub fn from_fields(
        version: u16,
        flags: u64,
        canonical_sql: impl Into<String>,
        dependency_ids: Vec<ObjectId>,
        output_signature: [u8; 32],
    ) -> CatalogResult<Self> {
        validate_version("view", version)?;
        validate_flags("view", flags)?;
        Ok(Self {
            canonical_sql: CanonicalSql::new("view.canonical_sql", canonical_sql)?,
            dependency_ids: sorted_unique_ids("view.dependency_ids", dependency_ids, true)?,
            output_signature,
        })
    }

    pub const fn version(&self) -> u16 {
        super::PAYLOAD_VERSION
    }

    pub const fn flags(&self) -> u64 {
        0
    }

    pub fn canonical_sql(&self) -> &CanonicalSql {
        &self.canonical_sql
    }

    pub fn dependency_ids(&self) -> &[ObjectId] {
        &self.dependency_ids
    }

    pub const fn output_signature(&self) -> &[u8; 32] {
        &self.output_signature
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_keeps_only_rebindable_durable_contract() {
        let first = ObjectId::new();
        let second = ObjectId::new();
        let view =
            ViewPayload::new("SELECT id FROM messages", vec![second, first], [7; 32]).unwrap();
        assert_eq!(view.version(), 1);
        assert_eq!(view.canonical_sql().as_str(), "SELECT id FROM messages");
        assert!(view.dependency_ids().is_sorted());
        assert_eq!(view.output_signature(), &[7; 32]);
    }

    #[test]
    fn dependency_duplicates_are_rejected() {
        let dependency = ObjectId::new();
        assert!(ViewPayload::new("SELECT 1", vec![dependency, dependency], [0; 32]).is_err());
    }
}
