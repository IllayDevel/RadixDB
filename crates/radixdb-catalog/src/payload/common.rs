use std::collections::BTreeSet;
use std::fmt;

use crate::{CatalogError, CatalogResult, ObjectId};

pub const MAX_CANONICAL_SQL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_OBJECT_IDS_PER_FIELD: usize = 4096;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CanonicalSql(String);

impl CanonicalSql {
    /// Admit SQL already canonicalized by the SQL/executor owner.
    ///
    /// This crate intentionally validates only the persisted text contract. It
    /// neither parses SQL nor stores an AST or an executor handle.
    pub fn new(field: &'static str, value: impl Into<String>) -> CatalogResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(CatalogError::EmptyCanonicalSql { field });
        }
        if value.contains('\0') {
            return Err(CatalogError::EmbeddedSqlNul { field });
        }
        if value.len() > MAX_CANONICAL_SQL_BYTES {
            return Err(CatalogError::CanonicalSqlTooLong {
                field,
                actual: value.len(),
                limit: MAX_CANONICAL_SQL_BYTES,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CanonicalSql {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CanonicalSql")
            .field(&self.0)
            .finish()
    }
}

pub(crate) fn validate_version(kind: &'static str, version: u16) -> CatalogResult<()> {
    if version != super::PAYLOAD_VERSION {
        return Err(CatalogError::UnsupportedPayloadVersion { kind, version });
    }
    Ok(())
}

pub(crate) fn validate_flags(kind: &'static str, flags: u64) -> CatalogResult<()> {
    if flags != 0 {
        return Err(CatalogError::UnknownPayloadFlags { kind, flags });
    }
    Ok(())
}

pub(crate) fn ordered_unique_ids(
    field: &'static str,
    values: Vec<ObjectId>,
    allow_empty: bool,
) -> CatalogResult<Vec<ObjectId>> {
    validate_id_count(field, values.len(), allow_empty)?;
    let mut seen = BTreeSet::new();
    for id in &values {
        if !seen.insert(*id) {
            return Err(CatalogError::DuplicateObjectId {
                field,
                id: id.to_string(),
            });
        }
    }
    Ok(values)
}

pub(crate) fn sorted_unique_ids(
    field: &'static str,
    mut values: Vec<ObjectId>,
    allow_empty: bool,
) -> CatalogResult<Vec<ObjectId>> {
    validate_id_count(field, values.len(), allow_empty)?;
    values.sort_unstable();
    for pair in values.windows(2) {
        if pair[0] == pair[1] {
            return Err(CatalogError::DuplicateObjectId {
                field,
                id: pair[0].to_string(),
            });
        }
    }
    Ok(values)
}

fn validate_id_count(field: &'static str, count: usize, allow_empty: bool) -> CatalogResult<()> {
    if !allow_empty && count == 0 {
        return Err(CatalogError::EmptyObjectIdList { field });
    }
    if count > MAX_OBJECT_IDS_PER_FIELD {
        return Err(CatalogError::TooManyObjectIds {
            field,
            actual: count,
            limit: MAX_OBJECT_IDS_PER_FIELD,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_sql_is_bounded_and_has_no_nul() {
        assert!(CanonicalSql::new("query", "SELECT 1").is_ok());
        assert!(matches!(
            CanonicalSql::new("query", ""),
            Err(CatalogError::EmptyCanonicalSql { .. })
        ));
        assert!(matches!(
            CanonicalSql::new("query", "SELECT\0 1"),
            Err(CatalogError::EmbeddedSqlNul { .. })
        ));
        assert!(matches!(
            CanonicalSql::new("query", "x".repeat(MAX_CANONICAL_SQL_BYTES + 1)),
            Err(CatalogError::CanonicalSqlTooLong { .. })
        ));
    }
}
