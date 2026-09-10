use crate::{CatalogError, CatalogResult, ObjectId};

pub const EDGE_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum EdgeKind {
    Contains = 1,
    OwnedBy = 2,
    DependsOn = 3,
    References = 4,
    GrantedTo = 6,
    GrantsOn = 7,
}

impl EdgeKind {
    pub const fn tag(self) -> u16 {
        self as u16
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Contains => "Contains",
            Self::OwnedBy => "OwnedBy",
            Self::DependsOn => "DependsOn",
            Self::References => "References",
            Self::GrantedTo => "GrantedTo",
            Self::GrantsOn => "GrantsOn",
        }
    }

    pub const fn is_dependency(self) -> bool {
        matches!(self, Self::DependsOn | Self::References)
    }

    pub const fn minimum_catalog_minor(self) -> u16 {
        match self {
            Self::Contains | Self::OwnedBy | Self::DependsOn | Self::References => 0,
            Self::GrantedTo | Self::GrantsOn => 1,
        }
    }

    pub fn from_tag_for_minor(tag: u16, catalog_minor: u16) -> CatalogResult<Self> {
        if catalog_minor > crate::LATEST_CATALOG_MINOR {
            return Err(CatalogError::UnsupportedCatalogMinor {
                major: 6,
                minor: catalog_minor,
            });
        }
        let decoded = DecodedEdgeKind::decode(tag)?;
        match decoded {
            DecodedEdgeKind::Supported(kind) if kind.minimum_catalog_minor() <= catalog_minor => {
                Ok(kind)
            }
            DecodedEdgeKind::Supported(kind) => Err(CatalogError::ReservedEdgeKind {
                tag: kind.tag(),
                name: kind.name(),
            }),
            DecodedEdgeKind::Reserved(kind) => Err(CatalogError::ReservedEdgeKind {
                tag: kind.tag(),
                name: kind.name(),
            }),
        }
    }
}

impl TryFrom<u16> for EdgeKind {
    type Error = CatalogError;

    fn try_from(tag: u16) -> CatalogResult<Self> {
        DecodedEdgeKind::decode(tag)?.admit()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum ReservedEdgeKind {
    BackedBy = 5,
}

impl ReservedEdgeKind {
    pub const ALL: [Self; 1] = [Self::BackedBy];

    pub const fn tag(self) -> u16 {
        self as u16
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::BackedBy => "BackedBy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodedEdgeKind {
    Supported(EdgeKind),
    Reserved(ReservedEdgeKind),
}

impl DecodedEdgeKind {
    pub fn decode(tag: u16) -> CatalogResult<Self> {
        match tag {
            1 => Ok(Self::Supported(EdgeKind::Contains)),
            2 => Ok(Self::Supported(EdgeKind::OwnedBy)),
            3 => Ok(Self::Supported(EdgeKind::DependsOn)),
            4 => Ok(Self::Supported(EdgeKind::References)),
            5 => Ok(Self::Reserved(ReservedEdgeKind::BackedBy)),
            6 => Ok(Self::Supported(EdgeKind::GrantedTo)),
            7 => Ok(Self::Supported(EdgeKind::GrantsOn)),
            _ => Err(CatalogError::UnknownEdgeKind { tag }),
        }
    }

    pub fn admit(self) -> CatalogResult<EdgeKind> {
        match self {
            Self::Supported(kind) => Ok(kind),
            Self::Reserved(kind) => Err(CatalogError::ReservedEdgeKind {
                tag: kind.tag(),
                name: kind.name(),
            }),
        }
    }
}

/// Canonical logical form of one 48-byte catalog edge entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogEdge {
    source_object_id: ObjectId,
    kind: EdgeKind,
    target_object_id: ObjectId,
    ordinal: u32,
}

impl CatalogEdge {
    pub const fn new(
        source_object_id: ObjectId,
        target_object_id: ObjectId,
        kind: EdgeKind,
        ordinal: u32,
    ) -> Self {
        Self {
            source_object_id,
            kind,
            target_object_id,
            ordinal,
        }
    }

    pub fn from_fields(
        source_object_id: ObjectId,
        target_object_id: ObjectId,
        kind_tag: u16,
        version: u16,
        flags: u32,
        ordinal: u32,
    ) -> CatalogResult<Self> {
        Self::from_fields_for_minor(
            source_object_id,
            target_object_id,
            kind_tag,
            version,
            flags,
            ordinal,
            crate::LATEST_CATALOG_MINOR,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields_for_minor(
        source_object_id: ObjectId,
        target_object_id: ObjectId,
        kind_tag: u16,
        version: u16,
        flags: u32,
        ordinal: u32,
        catalog_minor: u16,
    ) -> CatalogResult<Self> {
        if version != EDGE_VERSION {
            return Err(CatalogError::UnsupportedEdgeVersion { version });
        }
        if flags != 0 {
            return Err(CatalogError::UnknownEdgeFlags { flags });
        }
        Ok(Self::new(
            source_object_id,
            target_object_id,
            EdgeKind::from_tag_for_minor(kind_tag, catalog_minor)?,
            ordinal,
        ))
    }

    pub const fn source_object_id(self) -> ObjectId {
        self.source_object_id
    }

    pub const fn target_object_id(self) -> ObjectId {
        self.target_object_id
    }

    pub const fn kind(self) -> EdgeKind {
        self.kind
    }

    pub const fn version(self) -> u16 {
        EDGE_VERSION
    }

    pub const fn flags(self) -> u32 {
        0
    }

    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_tags_and_envelope_fail_closed() {
        for kind in [
            EdgeKind::Contains,
            EdgeKind::OwnedBy,
            EdgeKind::DependsOn,
            EdgeKind::References,
            EdgeKind::GrantedTo,
            EdgeKind::GrantsOn,
        ] {
            assert_eq!(EdgeKind::try_from(kind.tag()).unwrap(), kind);
        }
        for kind in ReservedEdgeKind::ALL {
            assert!(matches!(
                EdgeKind::try_from(kind.tag()),
                Err(CatalogError::ReservedEdgeKind { tag, .. }) if tag == kind.tag()
            ));
        }
        assert!(EdgeKind::try_from(8).is_err());
        assert!(EdgeKind::from_tag_for_minor(6, 0).is_err());
        assert_eq!(
            EdgeKind::from_tag_for_minor(6, 1).unwrap(),
            EdgeKind::GrantedTo
        );
        assert_eq!(
            EdgeKind::from_tag_for_minor(1, crate::EXTENSION_CATALOG_MINOR).unwrap(),
            EdgeKind::Contains
        );
        assert!(matches!(
            EdgeKind::from_tag_for_minor(1, crate::LATEST_CATALOG_MINOR + 1),
            Err(CatalogError::UnsupportedCatalogMinor { major: 6, minor: 3 })
        ));

        let source = ObjectId::new();
        let target = ObjectId::new();
        assert!(CatalogEdge::from_fields(source, target, 1, 1, 0, 0).is_ok());
        assert!(CatalogEdge::from_fields(source, target, 1, 2, 0, 0).is_err());
        assert!(CatalogEdge::from_fields(source, target, 1, 1, 1, 0).is_err());
    }
}
