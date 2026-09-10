use crate::{CatalogError, CatalogResult};

pub const BASELINE_CATALOG_MINOR: u16 = 0;
pub const PROCEDURAL_CATALOG_MINOR: u16 = 1;
pub const EXTENSION_CATALOG_MINOR: u16 = 2;
pub const LATEST_CATALOG_MINOR: u16 = EXTENSION_CATALOG_MINOR;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum ObjectKind {
    Namespace = 1,
    Table = 2,
    Column = 3,
    Constraint = 4,
    Index = 5,
    View = 6,
    Principal = 32,
    Role = 33,
    AclEntry = 34,
    Function = 36,
    Procedure = 37,
    Trigger = 38,
    Job = 39,
    Extension = 40,
    ExternalType = 41,
    Operator = 42,
    OperatorClass = 43,
    PlannerSupport = 44,
}

impl ObjectKind {
    pub const ALL: [Self; 18] = [
        Self::Namespace,
        Self::Table,
        Self::Column,
        Self::Constraint,
        Self::Index,
        Self::View,
        Self::Principal,
        Self::Role,
        Self::AclEntry,
        Self::Function,
        Self::Procedure,
        Self::Trigger,
        Self::Job,
        Self::Extension,
        Self::ExternalType,
        Self::Operator,
        Self::OperatorClass,
        Self::PlannerSupport,
    ];

    pub const fn tag(self) -> u16 {
        self as u16
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Namespace => "Namespace",
            Self::Table => "Table",
            Self::Column => "Column",
            Self::Constraint => "Constraint",
            Self::Index => "Index",
            Self::View => "View",
            Self::Principal => "Principal",
            Self::Role => "Role",
            Self::AclEntry => "AclEntry",
            Self::Function => "Function",
            Self::Procedure => "Procedure",
            Self::Trigger => "Trigger",
            Self::Job => "Job",
            Self::Extension => "Extension",
            Self::ExternalType => "ExternalType",
            Self::Operator => "Operator",
            Self::OperatorClass => "OperatorClass",
            Self::PlannerSupport => "PlannerSupport",
        }
    }

    pub const fn minimum_catalog_minor(self) -> u16 {
        match self {
            Self::Namespace
            | Self::Table
            | Self::Column
            | Self::Constraint
            | Self::Index
            | Self::View => BASELINE_CATALOG_MINOR,
            Self::Principal
            | Self::Role
            | Self::AclEntry
            | Self::Function
            | Self::Procedure
            | Self::Trigger
            | Self::Job => PROCEDURAL_CATALOG_MINOR,
            Self::Extension
            | Self::ExternalType
            | Self::Operator
            | Self::OperatorClass
            | Self::PlannerSupport => EXTENSION_CATALOG_MINOR,
        }
    }

    pub const fn object_class(self) -> ObjectClass {
        match self {
            Self::Namespace => ObjectClass::Namespace,
            Self::Table | Self::View => ObjectClass::Relation,
            Self::Column => ObjectClass::Column,
            Self::Constraint => ObjectClass::Constraint,
            Self::Index => ObjectClass::Index,
            Self::Principal | Self::Role => ObjectClass::SecuritySubject,
            Self::AclEntry => ObjectClass::AclEntry,
            Self::Function => ObjectClass::Function,
            Self::Procedure => ObjectClass::Procedure,
            Self::Trigger => ObjectClass::Trigger,
            Self::Job => ObjectClass::Job,
            Self::Extension => ObjectClass::Extension,
            Self::ExternalType => ObjectClass::Type,
            Self::Operator => ObjectClass::Operator,
            Self::OperatorClass => ObjectClass::OperatorClass,
            Self::PlannerSupport => ObjectClass::PlannerSupport,
        }
    }

    pub const fn requires_containment_parent(self) -> bool {
        !matches!(
            self,
            Self::Principal | Self::Role | Self::AclEntry | Self::Extension
        )
    }

    pub const fn is_executable(self) -> bool {
        matches!(
            self,
            Self::Function | Self::Procedure | Self::Trigger | Self::Job
        )
    }

    pub fn from_tag_for_minor(tag: u16, catalog_minor: u16) -> CatalogResult<Self> {
        if catalog_minor > LATEST_CATALOG_MINOR {
            return Err(CatalogError::UnsupportedCatalogMinor {
                major: 6,
                minor: catalog_minor,
            });
        }
        DecodedObjectKind::decode(tag)?.admit_for_minor(catalog_minor)
    }
}

impl TryFrom<u16> for ObjectKind {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        Self::from_tag_for_minor(tag, LATEST_CATALOG_MINOR)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum ReservedObjectKind {
    MaterializedView = 35,
}

impl ReservedObjectKind {
    pub const ALL: [Self; 1] = [Self::MaterializedView];
    pub const fn tag(self) -> u16 {
        self as u16
    }
    pub const fn name(self) -> &'static str {
        match self {
            Self::MaterializedView => "MaterializedView",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodedObjectKind {
    Supported(ObjectKind),
    Reserved(ReservedObjectKind),
}

impl DecodedObjectKind {
    pub fn decode(tag: u16) -> CatalogResult<Self> {
        Ok(match tag {
            1 => Self::Supported(ObjectKind::Namespace),
            2 => Self::Supported(ObjectKind::Table),
            3 => Self::Supported(ObjectKind::Column),
            4 => Self::Supported(ObjectKind::Constraint),
            5 => Self::Supported(ObjectKind::Index),
            6 => Self::Supported(ObjectKind::View),
            32 => Self::Supported(ObjectKind::Principal),
            33 => Self::Supported(ObjectKind::Role),
            34 => Self::Supported(ObjectKind::AclEntry),
            35 => Self::Reserved(ReservedObjectKind::MaterializedView),
            36 => Self::Supported(ObjectKind::Function),
            37 => Self::Supported(ObjectKind::Procedure),
            38 => Self::Supported(ObjectKind::Trigger),
            39 => Self::Supported(ObjectKind::Job),
            40 => Self::Supported(ObjectKind::Extension),
            41 => Self::Supported(ObjectKind::ExternalType),
            42 => Self::Supported(ObjectKind::Operator),
            43 => Self::Supported(ObjectKind::OperatorClass),
            44 => Self::Supported(ObjectKind::PlannerSupport),
            _ => return Err(CatalogError::UnknownObjectKind { tag }),
        })
    }

    pub fn admit(self) -> CatalogResult<ObjectKind> {
        self.admit_for_minor(LATEST_CATALOG_MINOR)
    }

    pub fn admit_for_minor(self, catalog_minor: u16) -> CatalogResult<ObjectKind> {
        match self {
            Self::Supported(kind) if kind.minimum_catalog_minor() <= catalog_minor => Ok(kind),
            Self::Supported(kind) => Err(CatalogError::ObjectKindRequiresCatalogMinor {
                tag: kind.tag(),
                name: kind.name(),
                required_minor: kind.minimum_catalog_minor(),
                actual_minor: catalog_minor,
            }),
            Self::Reserved(kind) => Err(CatalogError::ReservedObjectKind {
                tag: kind.tag(),
                name: kind.name(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectClass {
    Namespace,
    Relation,
    Column,
    Constraint,
    Index,
    SecuritySubject,
    AclEntry,
    Function,
    Procedure,
    Trigger,
    Job,
    Extension,
    Type,
    Operator,
    OperatorClass,
    PlannerSupport,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_tags_roundtrip_exactly_for_latest_minor() {
        for kind in ObjectKind::ALL {
            assert_eq!(ObjectKind::try_from(kind.tag()).unwrap(), kind);
            assert_eq!(
                DecodedObjectKind::decode(kind.tag()).unwrap(),
                DecodedObjectKind::Supported(kind)
            );
        }
    }

    #[test]
    fn baseline_minor_keeps_new_kinds_closed() {
        for kind in [
            ObjectKind::Principal,
            ObjectKind::Role,
            ObjectKind::AclEntry,
            ObjectKind::Function,
            ObjectKind::Procedure,
            ObjectKind::Trigger,
            ObjectKind::Job,
        ] {
            assert!(
                matches!(ObjectKind::from_tag_for_minor(kind.tag(), BASELINE_CATALOG_MINOR), Err(CatalogError::ObjectKindRequiresCatalogMinor { tag, .. }) if tag == kind.tag())
            );
        }
        assert!(matches!(
            ObjectKind::from_tag_for_minor(35, PROCEDURAL_CATALOG_MINOR),
            Err(CatalogError::ReservedObjectKind { tag: 35, .. })
        ));
    }

    #[test]
    fn unknown_and_future_minor_fail_closed() {
        for tag in [0, 7, 31, 45, u16::MAX] {
            assert_eq!(
                DecodedObjectKind::decode(tag),
                Err(CatalogError::UnknownObjectKind { tag })
            );
        }
        assert!(matches!(
            ObjectKind::from_tag_for_minor(1, LATEST_CATALOG_MINOR + 1),
            Err(CatalogError::UnsupportedCatalogMinor { major: 6, minor: 3 })
        ));
    }

    #[test]
    fn object_classes_match_identity_contract() {
        assert_eq!(ObjectKind::Table.object_class(), ObjectClass::Relation);
        assert_eq!(ObjectKind::View.object_class(), ObjectClass::Relation);
        assert_eq!(
            ObjectKind::Principal.object_class(),
            ObjectClass::SecuritySubject
        );
        assert_eq!(
            ObjectKind::Role.object_class(),
            ObjectClass::SecuritySubject
        );
        assert_ne!(
            ObjectKind::Function.object_class(),
            ObjectKind::Procedure.object_class()
        );
    }
}
