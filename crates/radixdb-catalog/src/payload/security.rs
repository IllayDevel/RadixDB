use crate::payload::common::{ordered_unique_ids, validate_flags};
use crate::{CatalogError, CatalogResult, ObjectId};

pub const PRIVILEGE_CONNECT: u64 = 1 << 0;
pub const PRIVILEGE_USAGE: u64 = 1 << 1;
pub const PRIVILEGE_CREATE: u64 = 1 << 7;
pub const PRIVILEGE_SELECT: u64 = 1 << 2;
pub const PRIVILEGE_INSERT: u64 = 1 << 3;
pub const PRIVILEGE_UPDATE: u64 = 1 << 4;
pub const PRIVILEGE_DELETE: u64 = 1 << 5;
pub const PRIVILEGE_EXECUTE: u64 = 1 << 6;
pub const ALL_OBJECT_PRIVILEGES: u64 = PRIVILEGE_CONNECT
    | PRIVILEGE_USAGE
    | PRIVILEGE_CREATE
    | PRIVILEGE_SELECT
    | PRIVILEGE_INSERT
    | PRIVILEGE_UPDATE
    | PRIVILEGE_DELETE
    | PRIVILEGE_EXECUTE;
const LEGACY_OBJECT_PRIVILEGES: u64 = ALL_OBJECT_PRIVILEGES & !PRIVILEGE_CREATE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalPayload {
    login_enabled: bool,
    system: bool,
    credential: Option<CredentialVerifier>,
}

pub const CREDENTIAL_SCHEME_ARGON2ID_PHC_V1: u16 = 1;
pub const MAX_CREDENTIAL_VERIFIER_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialVerifier {
    scheme: u16,
    encoded: Vec<u8>,
}

impl CredentialVerifier {
    pub fn new(scheme: u16, encoded: Vec<u8>) -> CatalogResult<Self> {
        if scheme != CREDENTIAL_SCHEME_ARGON2ID_PHC_V1 {
            return Err(CatalogError::InvalidCatalogObject {
                id: "principal-credential".to_owned(),
                detail: "unknown credential verifier scheme",
            });
        }
        if encoded.is_empty() || encoded.len() > MAX_CREDENTIAL_VERIFIER_BYTES {
            return Err(CatalogError::InvalidCatalogObject {
                id: "principal-credential".to_owned(),
                detail: "credential verifier length is outside the admitted range",
            });
        }
        Ok(Self { scheme, encoded })
    }

    pub const fn scheme(&self) -> u16 {
        self.scheme
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }
}

impl PrincipalPayload {
    pub fn new(login_enabled: bool, system: bool) -> Self {
        Self {
            login_enabled,
            system,
            credential: None,
        }
    }

    pub fn from_fields(
        version: u16,
        flags: u64,
        login_enabled: bool,
        system: bool,
    ) -> CatalogResult<Self> {
        if !matches!(
            version,
            super::PAYLOAD_VERSION | super::SECURITY_PAYLOAD_VERSION
        ) {
            return Err(CatalogError::UnsupportedPayloadVersion {
                kind: "principal",
                version,
            });
        }
        validate_flags("principal", flags)?;
        Ok(Self::new(login_enabled, system))
    }
    pub fn from_fields_with_credential(
        version: u16,
        flags: u64,
        login_enabled: bool,
        system: bool,
        credential: Option<CredentialVerifier>,
    ) -> CatalogResult<Self> {
        let mut payload = Self::from_fields(version, flags, login_enabled, system)?;
        if version == super::PAYLOAD_VERSION && credential.is_some() {
            return Err(CatalogError::InvalidCatalogObject {
                id: "principal-payload".to_owned(),
                detail: "payload version 1 cannot encode a credential verifier",
            });
        }
        payload.credential = credential;
        Ok(payload)
    }

    pub const fn login_enabled(&self) -> bool {
        self.login_enabled
    }
    pub fn with_login_enabled(self, login_enabled: bool) -> Self {
        Self {
            login_enabled,
            ..self
        }
    }
    pub fn credential(&self) -> Option<&CredentialVerifier> {
        self.credential.as_ref()
    }
    pub fn with_credential(mut self, credential: Option<CredentialVerifier>) -> Self {
        self.credential = credential;
        self
    }
    pub const fn system(&self) -> bool {
        self.system
    }
    pub const fn flags(&self) -> u64 {
        0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolePayload {
    inheritable: bool,
    enabled: bool,
}

impl RolePayload {
    pub fn new(inheritable: bool) -> Self {
        Self {
            inheritable,
            enabled: true,
        }
    }
    pub fn from_fields(
        version: u16,
        flags: u64,
        inheritable: bool,
        enabled: bool,
    ) -> CatalogResult<Self> {
        if !matches!(
            version,
            super::PAYLOAD_VERSION | super::SECURITY_PAYLOAD_VERSION
        ) {
            return Err(CatalogError::UnsupportedPayloadVersion {
                kind: "role",
                version,
            });
        }
        validate_flags("role", flags)?;
        if version == super::PAYLOAD_VERSION && !enabled {
            return Err(CatalogError::InvalidCatalogObject {
                id: "role-payload".to_owned(),
                detail: "payload version 1 cannot encode disabled roles",
            });
        }
        Ok(Self {
            inheritable,
            enabled,
        })
    }
    pub const fn inheritable(&self) -> bool {
        self.inheritable
    }
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
    pub const fn with_enabled(self, enabled: bool) -> Self {
        Self { enabled, ..self }
    }
    pub const fn flags(&self) -> u64 {
        0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPrivilegeSet {
    privilege: u64,
    column_ids: Vec<ObjectId>,
}

impl ColumnPrivilegeSet {
    pub fn new(privilege: u64, column_ids: Vec<ObjectId>) -> CatalogResult<Self> {
        if !matches!(
            privilege,
            PRIVILEGE_SELECT | PRIVILEGE_INSERT | PRIVILEGE_UPDATE
        ) {
            return Err(CatalogError::InvalidCatalogObject {
                id: "acl-payload".to_owned(),
                detail: "column privilege is not SELECT, INSERT or UPDATE",
            });
        }
        Ok(Self {
            privilege,
            column_ids: ordered_unique_ids("acl.column_ids", column_ids, false)?,
        })
    }
    pub const fn privilege(&self) -> u64 {
        self.privilege
    }
    pub fn column_ids(&self) -> &[ObjectId] {
        &self.column_ids
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclEntryPayload {
    ObjectPrivileges {
        grantor_principal_id: ObjectId,
        privileges: u64,
        grant_option: u64,
        columns: Vec<ColumnPrivilegeSet>,
        column_grant_options: Vec<ColumnPrivilegeSet>,
    },
    RoleMembership {
        grantor_principal_id: ObjectId,
        admin_option: bool,
    },
}

impl AclEntryPayload {
    pub fn object_privileges(
        grantor_principal_id: ObjectId,
        privileges: u64,
        grant_option: u64,
        columns: Vec<ColumnPrivilegeSet>,
    ) -> CatalogResult<Self> {
        Self::object_privileges_with_column_options(
            grantor_principal_id,
            privileges,
            grant_option,
            columns,
            Vec::new(),
        )
    }

    pub fn object_privileges_with_column_options(
        grantor_principal_id: ObjectId,
        privileges: u64,
        grant_option: u64,
        mut columns: Vec<ColumnPrivilegeSet>,
        mut column_grant_options: Vec<ColumnPrivilegeSet>,
    ) -> CatalogResult<Self> {
        if privileges & !ALL_OBJECT_PRIVILEGES != 0 || grant_option & !privileges != 0 {
            return Err(CatalogError::InvalidCatalogObject {
                id: "acl-payload".to_owned(),
                detail: "object privilege bits are unknown or grant-option exceeds privileges",
            });
        }
        columns.sort_unstable_by_key(ColumnPrivilegeSet::privilege);
        if columns
            .windows(2)
            .any(|pair| pair[0].privilege == pair[1].privilege)
        {
            return Err(CatalogError::InvalidCatalogObject {
                id: "acl-payload".to_owned(),
                detail: "duplicate column privilege group",
            });
        }
        column_grant_options.sort_unstable_by_key(ColumnPrivilegeSet::privilege);
        if column_grant_options
            .windows(2)
            .any(|pair| pair[0].privilege == pair[1].privilege)
        {
            return Err(CatalogError::InvalidCatalogObject {
                id: "acl-payload".to_owned(),
                detail: "duplicate column grant-option privilege group",
            });
        }
        for option in &column_grant_options {
            let Some(granted) = columns
                .iter()
                .find(|group| group.privilege() == option.privilege())
            else {
                return Err(CatalogError::InvalidCatalogObject {
                    id: "acl-payload".to_owned(),
                    detail: "column grant option has no matching column privilege",
                });
            };
            if option
                .column_ids()
                .iter()
                .any(|id| !granted.column_ids().contains(id))
            {
                return Err(CatalogError::InvalidCatalogObject {
                    id: "acl-payload".to_owned(),
                    detail: "column grant option exceeds granted columns",
                });
            }
        }
        if privileges == 0 && columns.is_empty() {
            return Err(CatalogError::InvalidCatalogObject {
                id: "acl-payload".to_owned(),
                detail: "object and column privileges are both empty",
            });
        }
        Ok(Self::ObjectPrivileges {
            grantor_principal_id,
            privileges,
            grant_option,
            columns,
            column_grant_options,
        })
    }

    pub const fn role_membership(grantor_principal_id: ObjectId, admin_option: bool) -> Self {
        Self::RoleMembership {
            grantor_principal_id,
            admin_option,
        }
    }

    pub fn from_fields(version: u16, flags: u64, value: Self) -> CatalogResult<Self> {
        if !matches!(
            version,
            super::PAYLOAD_VERSION | super::SECURITY_PAYLOAD_VERSION
        ) {
            return Err(CatalogError::UnsupportedPayloadVersion {
                kind: "acl-entry",
                version,
            });
        }
        validate_flags("acl-entry", flags)?;
        match value {
            Self::ObjectPrivileges {
                grantor_principal_id,
                privileges,
                grant_option,
                columns,
                column_grant_options,
            } => {
                if version == super::PAYLOAD_VERSION
                    && ((privileges | grant_option) & !LEGACY_OBJECT_PRIVILEGES != 0
                        || !column_grant_options.is_empty())
                {
                    return Err(CatalogError::InvalidCatalogObject {
                        id: "acl-payload".to_owned(),
                        detail: "payload version 1 cannot encode CREATE or column grant options",
                    });
                }
                Self::object_privileges_with_column_options(
                    grantor_principal_id,
                    privileges,
                    grant_option,
                    columns,
                    column_grant_options,
                )
            }
            Self::RoleMembership {
                grantor_principal_id,
                admin_option,
            } => Ok(Self::role_membership(grantor_principal_id, admin_option)),
        }
    }

    pub const fn grantor_principal_id(&self) -> ObjectId {
        match self {
            Self::ObjectPrivileges {
                grantor_principal_id,
                ..
            }
            | Self::RoleMembership {
                grantor_principal_id,
                ..
            } => *grantor_principal_id,
        }
    }
    pub const fn flags(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(marker: u8) -> ObjectId {
        let mut bytes = [marker; 16];
        bytes[0] = 1;
        ObjectId::from_user_bytes(bytes).unwrap()
    }

    #[test]
    fn column_only_privilege_does_not_invent_object_privilege() {
        let payload = AclEntryPayload::object_privileges(
            id(1),
            0,
            0,
            vec![ColumnPrivilegeSet::new(PRIVILEGE_SELECT, vec![id(2)]).unwrap()],
        )
        .unwrap();
        assert!(matches!(
            payload,
            AclEntryPayload::ObjectPrivileges { privileges: 0, .. }
        ));
    }

    #[test]
    fn empty_privilege_entry_is_rejected() {
        assert!(AclEntryPayload::object_privileges(id(1), 0, 0, vec![]).is_err());
    }

    #[test]
    fn legacy_security_payload_version_cannot_claim_new_semantics() {
        assert!(RolePayload::from_fields(super::super::PAYLOAD_VERSION, 0, true, false).is_err());
        assert!(AclEntryPayload::from_fields(
            super::super::PAYLOAD_VERSION,
            0,
            AclEntryPayload::object_privileges(id(1), PRIVILEGE_CREATE, 0, vec![]).unwrap(),
        )
        .is_err());
        assert!(AclEntryPayload::from_fields(
            super::super::PAYLOAD_VERSION,
            0,
            AclEntryPayload::object_privileges_with_column_options(
                id(1),
                0,
                0,
                vec![ColumnPrivilegeSet::new(PRIVILEGE_SELECT, vec![id(2)]).unwrap()],
                vec![ColumnPrivilegeSet::new(PRIVILEGE_SELECT, vec![id(2)]).unwrap()],
            )
            .unwrap(),
        )
        .is_err());
    }
}
