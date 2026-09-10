use crate::payload::common::{ordered_unique_ids, sorted_unique_ids, validate_flags, CanonicalSql};
use crate::{CatalogError, CatalogResult, ObjectId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum AccessMethod {
    Btree = 1,
    Hash = 2,
    Bitmap = 3,
    Hnsw = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum HnswDistanceMetric {
    Cosine = 1,
    L2 = 2,
    Dot = 3,
}

impl HnswDistanceMetric {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for HnswDistanceMetric {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Cosine),
            2 => Ok(Self::L2),
            3 => Ok(Self::Dot),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "HNSW distance metric",
                tag,
            }),
        }
    }
}

pub const MAX_HNSW_M: u16 = 128;
pub const MAX_HNSW_EF_CONSTRUCTION: u16 = 4096;
pub const MAX_HNSW_EF_SEARCH: u16 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HnswParameters {
    m: u16,
    ef_construction: u16,
    ef_search: u16,
    distance_metric: HnswDistanceMetric,
}

impl HnswParameters {
    pub fn new(
        m: u16,
        ef_construction: u16,
        ef_search: u16,
        distance_metric: HnswDistanceMetric,
    ) -> CatalogResult<Self> {
        if !(2..=MAX_HNSW_M).contains(&m) {
            return Err(CatalogError::InvalidIndexPayload {
                detail: "HNSW m is outside 2..=128",
            });
        }
        if !(m..=MAX_HNSW_EF_CONSTRUCTION).contains(&ef_construction) {
            return Err(CatalogError::InvalidIndexPayload {
                detail: "HNSW ef_construction is outside m..=4096",
            });
        }
        if !(1..=MAX_HNSW_EF_SEARCH).contains(&ef_search) {
            return Err(CatalogError::InvalidIndexPayload {
                detail: "HNSW ef_search is outside 1..=4096",
            });
        }
        Ok(Self {
            m,
            ef_construction,
            ef_search,
            distance_metric,
        })
    }

    pub const fn m(self) -> u16 {
        self.m
    }

    pub const fn ef_construction(self) -> u16 {
        self.ef_construction
    }

    pub const fn ef_search(self) -> u16 {
        self.ef_search
    }

    pub const fn distance_metric(self) -> HnswDistanceMetric {
        self.distance_metric
    }
}

impl AccessMethod {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for AccessMethod {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Btree),
            2 => Ok(Self::Hash),
            3 => Ok(Self::Bitmap),
            4 => Ok(Self::Hnsw),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "index access method",
                tag,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPayload {
    access_method: AccessMethod,
    unique: bool,
    key_column_ids: Vec<ObjectId>,
    include_column_ids: Vec<ObjectId>,
    expression_sql: Option<CanonicalSql>,
    predicate_sql: Option<CanonicalSql>,
    hnsw_parameters: Option<HnswParameters>,
    operator_class_id: Option<ObjectId>,
}

impl IndexPayload {
    pub fn new(
        access_method: AccessMethod,
        unique: bool,
        key_column_ids: Vec<ObjectId>,
        include_column_ids: Vec<ObjectId>,
        expression_sql: Option<String>,
        predicate_sql: Option<String>,
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            0,
            access_method,
            unique,
            key_column_ids,
            include_column_ids,
            expression_sql,
            predicate_sql,
            None,
            None,
        )
    }

    pub fn new_hnsw(
        key_column_id: ObjectId,
        include_column_ids: Vec<ObjectId>,
        parameters: HnswParameters,
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            0,
            AccessMethod::Hnsw,
            false,
            vec![key_column_id],
            include_column_ids,
            None,
            None,
            Some(parameters),
            None,
        )
    }

    pub fn new_external(
        access_method: AccessMethod,
        unique: bool,
        key_column_id: ObjectId,
        predicate_sql: Option<String>,
        operator_class_id: ObjectId,
    ) -> CatalogResult<Self> {
        Self::from_fields(
            2,
            0,
            access_method,
            unique,
            vec![key_column_id],
            vec![],
            None,
            predicate_sql,
            None,
            Some(operator_class_id),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        version: u16,
        flags: u64,
        access_method: AccessMethod,
        unique: bool,
        key_column_ids: Vec<ObjectId>,
        include_column_ids: Vec<ObjectId>,
        expression_sql: Option<String>,
        predicate_sql: Option<String>,
        hnsw_parameters: Option<HnswParameters>,
        operator_class_id: Option<ObjectId>,
    ) -> CatalogResult<Self> {
        if version != super::PAYLOAD_VERSION && version != 2 {
            return Err(CatalogError::UnsupportedPayloadVersion {
                kind: "index",
                version,
            });
        }
        validate_flags("index", flags)?;
        if (version == 2) != operator_class_id.is_some() {
            return Err(CatalogError::InvalidIndexPayload {
                detail:
                    "index payload version 2 is required exactly when an operator class is bound",
            });
        }
        let key_column_ids = ordered_unique_ids(
            "index.key_column_ids",
            key_column_ids,
            expression_sql.is_some(),
        )?;
        if key_column_ids.is_empty() == expression_sql.is_none() {
            return Err(CatalogError::InvalidIndexPayload {
                detail: "exactly one column-key list or expression is required",
            });
        }
        let include_column_ids =
            sorted_unique_ids("index.include_column_ids", include_column_ids, true)?;
        if include_column_ids
            .iter()
            .any(|id| key_column_ids.contains(id))
        {
            return Err(CatalogError::InvalidIndexPayload {
                detail: "include columns overlap key columns",
            });
        }
        if access_method == AccessMethod::Hnsw
            && (unique
                || expression_sql.is_some()
                || predicate_sql.is_some()
                || key_column_ids.len() != 1)
        {
            return Err(CatalogError::InvalidIndexPayload {
                detail: "HNSW requires one non-expression, non-partial, non-unique key",
            });
        }
        if (access_method == AccessMethod::Hnsw) != hnsw_parameters.is_some() {
            return Err(CatalogError::InvalidIndexPayload {
                detail: "HNSW parameters are required exactly for HNSW indexes",
            });
        }
        if operator_class_id.is_some()
            && (key_column_ids.len() != 1
                || expression_sql.is_some()
                || access_method == AccessMethod::Hnsw)
        {
            return Err(CatalogError::InvalidIndexPayload {
                detail: "operator-class indexes require one non-expression, non-HNSW key",
            });
        }
        let expression_sql = expression_sql
            .map(|sql| CanonicalSql::new("index.expression_sql", sql))
            .transpose()?;
        let predicate_sql = predicate_sql
            .map(|sql| CanonicalSql::new("index.predicate_sql", sql))
            .transpose()?;
        Ok(Self {
            access_method,
            unique,
            key_column_ids,
            include_column_ids,
            expression_sql,
            predicate_sql,
            hnsw_parameters,
            operator_class_id,
        })
    }

    pub const fn version(&self) -> u16 {
        if self.operator_class_id.is_some() {
            2
        } else {
            super::PAYLOAD_VERSION
        }
    }

    pub const fn flags(&self) -> u64 {
        0
    }

    pub const fn access_method(&self) -> AccessMethod {
        self.access_method
    }

    pub const fn unique(&self) -> bool {
        self.unique
    }

    pub fn key_column_ids(&self) -> &[ObjectId] {
        &self.key_column_ids
    }

    pub fn include_column_ids(&self) -> &[ObjectId] {
        &self.include_column_ids
    }

    pub fn expression_sql(&self) -> Option<&CanonicalSql> {
        self.expression_sql.as_ref()
    }

    pub fn predicate_sql(&self) -> Option<&CanonicalSql> {
        self.predicate_sql.as_ref()
    }

    pub const fn hnsw_parameters(&self) -> Option<HnswParameters> {
        self.hnsw_parameters
    }

    pub const fn operator_class_id(&self) -> Option<ObjectId> {
        self.operator_class_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_or_expression_is_exclusive_and_includes_do_not_overlap() {
        let key = ObjectId::new();
        assert!(
            IndexPayload::new(AccessMethod::Btree, false, vec![key], vec![], None, None).is_ok()
        );
        assert!(IndexPayload::new(
            AccessMethod::Btree,
            false,
            vec![],
            vec![],
            Some("lower(name)".into()),
            None
        )
        .is_ok());
        assert!(IndexPayload::new(
            AccessMethod::Btree,
            false,
            vec![key],
            vec![],
            Some("lower(name)".into()),
            None
        )
        .is_err());
        assert!(
            IndexPayload::new(AccessMethod::Btree, false, vec![key], vec![key], None, None)
                .is_err()
        );
    }

    #[test]
    fn hnsw_shape_is_closed() {
        let key = ObjectId::new();
        let parameters = HnswParameters::new(16, 200, 64, HnswDistanceMetric::Cosine).unwrap();
        assert!(IndexPayload::new_hnsw(key, vec![], parameters).is_ok());
        assert!(
            IndexPayload::new(AccessMethod::Hnsw, false, vec![key], vec![], None, None).is_err()
        );
        assert!(
            IndexPayload::new(AccessMethod::Hnsw, true, vec![key], vec![], None, None).is_err()
        );
        assert_eq!(AccessMethod::try_from(1).unwrap(), AccessMethod::Btree);
        assert_eq!(AccessMethod::Hnsw.tag(), 4);
        assert!(AccessMethod::try_from(5).is_err());
        assert_eq!(
            HnswDistanceMetric::try_from(3).unwrap(),
            HnswDistanceMetric::Dot
        );
        assert!(HnswDistanceMetric::try_from(4).is_err());
        assert!(HnswParameters::new(1, 200, 64, HnswDistanceMetric::L2).is_err());
        assert!(HnswParameters::new(16, 15, 64, HnswDistanceMetric::L2).is_err());
        assert!(HnswParameters::new(16, 200, 0, HnswDistanceMetric::L2).is_err());
        assert!(IndexPayload::from_fields(
            super::super::PAYLOAD_VERSION,
            0,
            AccessMethod::Btree,
            false,
            vec![key],
            vec![],
            None,
            None,
            Some(parameters),
            None,
        )
        .is_err());
    }
}
