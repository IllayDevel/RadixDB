use crate::{CatalogError, CatalogName, CatalogPayload, CatalogResult, ObjectId, ObjectKind};

/// Fully decoded catalog object before global graph admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogObject {
    id: ObjectId,
    kind: ObjectKind,
    namespace_id: Option<ObjectId>,
    parent_id: Option<ObjectId>,
    owner_principal_id: ObjectId,
    name: CatalogName,
    definition_revision: u64,
    payload: CatalogPayload,
}

impl CatalogObject {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: ObjectId,
        namespace_id: Option<ObjectId>,
        parent_id: Option<ObjectId>,
        owner_principal_id: ObjectId,
        name: CatalogName,
        definition_revision: u64,
        payload: CatalogPayload,
    ) -> CatalogResult<Self> {
        Self::from_fields(
            id,
            payload.kind(),
            0,
            namespace_id,
            parent_id,
            owner_principal_id,
            name,
            definition_revision,
            payload,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        id: ObjectId,
        kind: ObjectKind,
        flags: u32,
        namespace_id: Option<ObjectId>,
        parent_id: Option<ObjectId>,
        owner_principal_id: ObjectId,
        name: CatalogName,
        definition_revision: u64,
        payload: CatalogPayload,
    ) -> CatalogResult<Self> {
        if flags != 0 {
            return Err(CatalogError::InvalidCatalogObject {
                id: id.to_string(),
                detail: "unknown object flags",
            });
        }
        if definition_revision == 0 {
            return Err(CatalogError::InvalidCatalogObject {
                id: id.to_string(),
                detail: "definition revision must be non-zero",
            });
        }
        if kind != payload.kind() {
            return Err(CatalogError::PayloadKindMismatch {
                id: id.to_string(),
                header: object_kind_name(kind),
                payload: object_kind_name(payload.kind()),
            });
        }
        Ok(Self {
            id,
            kind,
            namespace_id,
            parent_id,
            owner_principal_id,
            name,
            definition_revision,
            payload,
        })
    }

    pub const fn id(&self) -> ObjectId {
        self.id
    }

    pub const fn kind(&self) -> ObjectKind {
        self.kind
    }

    pub const fn namespace_id(&self) -> Option<ObjectId> {
        self.namespace_id
    }

    pub const fn parent_id(&self) -> Option<ObjectId> {
        self.parent_id
    }

    pub const fn owner_principal_id(&self) -> ObjectId {
        self.owner_principal_id
    }

    pub const fn flags(&self) -> u32 {
        0
    }

    pub fn name(&self) -> &CatalogName {
        &self.name
    }

    pub const fn definition_revision(&self) -> u64 {
        self.definition_revision
    }

    pub const fn payload_version(&self) -> u16 {
        self.payload.version()
    }

    pub const fn payload(&self) -> &CatalogPayload {
        &self.payload
    }
}

pub(crate) const fn object_kind_name(kind: ObjectKind) -> &'static str {
    kind.name()
}
