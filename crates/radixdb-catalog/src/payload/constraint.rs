use crate::payload::common::{ordered_unique_ids, CanonicalSql};
use crate::{CatalogError, CatalogResult, ObjectId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ConstraintKind {
    PrimaryKey = 1,
    Unique = 2,
    ForeignKey = 3,
    Check = 4,
    NotNull = 5,
}

impl ConstraintKind {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for ConstraintKind {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::PrimaryKey),
            2 => Ok(Self::Unique),
            3 => Ok(Self::ForeignKey),
            4 => Ok(Self::Check),
            5 => Ok(Self::NotNull),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "constraint kind",
                tag,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ForeignKeyMatch {
    Simple = 1,
    Full = 2,
}

impl ForeignKeyMatch {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for ForeignKeyMatch {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Simple),
            2 => Ok(Self::Full),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "foreign-key match action",
                tag,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ForeignKeyAction {
    NoAction = 1,
    Restrict = 2,
    Cascade = 3,
    SetNull = 4,
    SetDefault = 5,
}

impl ForeignKeyAction {
    pub const fn tag(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for ForeignKeyAction {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::NoAction),
            2 => Ok(Self::Restrict),
            3 => Ok(Self::Cascade),
            4 => Ok(Self::SetNull),
            5 => Ok(Self::SetDefault),
            _ => Err(CatalogError::UnknownPayloadEnumTag {
                owner: "foreign-key referential action",
                tag,
            }),
        }
    }
}

/// The enum shape makes fields illegal for a constraint kind unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintPayload {
    PrimaryKey {
        local_column_ids: Vec<ObjectId>,
    },
    Unique {
        local_column_ids: Vec<ObjectId>,
    },
    ForeignKey {
        local_column_ids: Vec<ObjectId>,
        referenced_table_id: ObjectId,
        referenced_column_ids: Vec<ObjectId>,
        match_action: ForeignKeyMatch,
        on_update_action: ForeignKeyAction,
        on_delete_action: ForeignKeyAction,
    },
    Check {
        /// Present only for a column-level CHECK. `None` denotes a table CHECK.
        local_column_id: Option<ObjectId>,
        check_sql: CanonicalSql,
    },
    NotNull {
        local_column_id: ObjectId,
    },
}

impl ConstraintPayload {
    pub fn primary_key(local_column_ids: Vec<ObjectId>) -> CatalogResult<Self> {
        Ok(Self::PrimaryKey {
            local_column_ids: ordered_unique_ids(
                "constraint.local_column_ids",
                local_column_ids,
                false,
            )?,
        })
    }

    pub fn unique(local_column_ids: Vec<ObjectId>) -> CatalogResult<Self> {
        Ok(Self::Unique {
            local_column_ids: ordered_unique_ids(
                "constraint.local_column_ids",
                local_column_ids,
                false,
            )?,
        })
    }

    pub fn foreign_key(
        local_column_ids: Vec<ObjectId>,
        referenced_table_id: ObjectId,
        referenced_column_ids: Vec<ObjectId>,
        match_action: ForeignKeyMatch,
        on_update_action: ForeignKeyAction,
        on_delete_action: ForeignKeyAction,
    ) -> CatalogResult<Self> {
        let local_column_ids =
            ordered_unique_ids("constraint.local_column_ids", local_column_ids, false)?;
        let referenced_column_ids = ordered_unique_ids(
            "constraint.referenced_column_ids",
            referenced_column_ids,
            false,
        )?;
        if local_column_ids.len() != referenced_column_ids.len() {
            return Err(CatalogError::InvalidConstraintPayload {
                detail: "foreign-key local/referenced arity differs",
            });
        }
        Ok(Self::ForeignKey {
            local_column_ids,
            referenced_table_id,
            referenced_column_ids,
            match_action,
            on_update_action,
            on_delete_action,
        })
    }

    pub fn check(check_sql: impl Into<String>) -> CatalogResult<Self> {
        Self::check_for_column(None, check_sql)
    }

    pub fn column_check(
        local_column_id: ObjectId,
        check_sql: impl Into<String>,
    ) -> CatalogResult<Self> {
        Self::check_for_column(Some(local_column_id), check_sql)
    }

    fn check_for_column(
        local_column_id: Option<ObjectId>,
        check_sql: impl Into<String>,
    ) -> CatalogResult<Self> {
        Ok(Self::Check {
            local_column_id,
            check_sql: CanonicalSql::new("constraint.check_sql", check_sql)?,
        })
    }

    pub const fn not_null(local_column_id: ObjectId) -> Self {
        Self::NotNull { local_column_id }
    }

    pub const fn version(&self) -> u16 {
        super::PAYLOAD_VERSION
    }

    pub const fn flags(&self) -> u64 {
        0
    }

    pub const fn kind(&self) -> ConstraintKind {
        match self {
            Self::PrimaryKey { .. } => ConstraintKind::PrimaryKey,
            Self::Unique { .. } => ConstraintKind::Unique,
            Self::ForeignKey { .. } => ConstraintKind::ForeignKey,
            Self::Check { .. } => ConstraintKind::Check,
            Self::NotNull { .. } => ConstraintKind::NotNull,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_key_arity_and_duplicates_fail_closed() {
        let local = ObjectId::new();
        let remote = ObjectId::new();
        let table = ObjectId::new();
        assert!(ConstraintPayload::foreign_key(
            vec![local],
            table,
            vec![remote],
            ForeignKeyMatch::Simple,
            ForeignKeyAction::NoAction,
            ForeignKeyAction::Cascade,
        )
        .is_ok());
        assert!(ConstraintPayload::foreign_key(
            vec![local, ObjectId::new()],
            table,
            vec![remote],
            ForeignKeyMatch::Simple,
            ForeignKeyAction::NoAction,
            ForeignKeyAction::Cascade,
        )
        .is_err());
        assert!(ConstraintPayload::primary_key(vec![local, local]).is_err());
    }

    #[test]
    fn kind_specific_fields_are_not_shared_bags() {
        let check = ConstraintPayload::check("value > 0").unwrap();
        assert_eq!(check.kind(), ConstraintKind::Check);
        assert_eq!(
            ConstraintPayload::not_null(ObjectId::new()).kind(),
            ConstraintKind::NotNull
        );
    }

    #[test]
    fn stable_enum_tags_fail_closed() {
        for (tag, kind) in [
            (1, ConstraintKind::PrimaryKey),
            (2, ConstraintKind::Unique),
            (3, ConstraintKind::ForeignKey),
            (4, ConstraintKind::Check),
            (5, ConstraintKind::NotNull),
        ] {
            assert_eq!(ConstraintKind::try_from(tag).unwrap(), kind);
            assert_eq!(kind.tag(), tag);
        }
        assert!(ConstraintKind::try_from(0).is_err());
        assert!(ForeignKeyMatch::try_from(3).is_err());
        assert!(ForeignKeyAction::try_from(6).is_err());
    }
}
