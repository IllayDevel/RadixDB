//! Typed catalog binding and immutable authorization lookup.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use radixdb_catalog::{
    AclEntryPayload, CatalogEdge, CatalogGeneration, CatalogMutation, CatalogMutationSet,
    CatalogName, CatalogObject, CatalogPayload, ColumnPrivilegeSet, CredentialVerifier, EdgeKind,
    NamespacePayload, ObjectId, ObjectKind, ObjectPrecondition, PrincipalPayload, RolePayload,
    CREDENTIAL_SCHEME_ARGON2ID_PHC_V1, PRIVILEGE_CONNECT, PRIVILEGE_CREATE, PRIVILEGE_DELETE,
    PRIVILEGE_EXECUTE, PRIVILEGE_INSERT, PRIVILEGE_SELECT, PRIVILEGE_UPDATE, PRIVILEGE_USAGE,
};
use radixdb_core::{Error, Result};
use radixdb_sql::{
    AlterOwnerStatement, AlterSecuritySubjectActionSyntax, AlterSecuritySubjectStatement,
    CreatePrincipalStatement, CreateRoleStatement, CreateSchemaStatement, DropBehaviorSyntax,
    DropSecuritySubjectStatement, GrantStatement, GrantSyntax, ObjectName, ObjectPrivilegeSyntax,
    OwnershipTargetSyntax, PrivilegeSyntax, PrivilegeTargetSyntax, RevokeStatement, RevokeSyntax,
    RoutineSignatureSyntax, SecuritySubjectKindSyntax, Statement,
};

use super::procedural::{bind_durable_type, resolve_namespace, resolve_object_scope};
use super::transaction::{catalog_argument, DdlDelta, ObjectIdSource};

const MAX_AUTHORIZATION_SUBJECTS: usize = 65_536;

pub(super) fn require_stage_authority(
    generation: &CatalogGeneration,
    actor: ObjectId,
    statement: &Statement,
) -> Result<()> {
    require_principal(generation, actor)?;
    if actor == ObjectId::BOOTSTRAP_OWNER {
        return Ok(());
    }
    match statement {
        Statement::CreateTable(_) | Statement::CreateView(_) => require_object_privilege(
            generation,
            actor,
            ObjectId::BOOTSTRAP_NAMESPACE,
            PRIVILEGE_CREATE,
            "CREATE",
        ),
        Statement::CreateRoutine(statement) => {
            let (namespace, _) = resolve_object_scope(generation, &statement.name)?;
            require_object_privilege(generation, actor, namespace, PRIVILEGE_CREATE, "CREATE")
        }
        Statement::CreateExternalType(statement) => {
            require_external_type_create_authority(generation, actor, statement)
        }
        Statement::DropExternalType(statement) => {
            require_external_type_drop_authority(generation, actor, statement)
        }
        Statement::CreateOperator(_)
        | Statement::DropOperator(_)
        | Statement::CreateOperatorClass(_)
        | Statement::DropOperatorClass(_) => Err(Error::InvalidArgument(
            "plugin operator DDL currently requires the database owner".to_owned(),
        )),
        Statement::CreateIndex(statement) => {
            let table = resolve_unqualified_table(generation, statement.table_name.value())?;
            require_owner(generation, actor, table)
        }
        Statement::CreateTrigger(statement) => {
            let table = resolve_relation(generation, &statement.table)?;
            require_owner(generation, actor, table)
        }
        Statement::DropRoutine(statement) => {
            match super::procedural::resolve_optional_routine_signature(
                generation,
                statement.kind,
                &statement.signature,
            )? {
                Some(object) => require_owner(generation, actor, object),
                None if statement.if_exists => Ok(()),
                None => Err(Error::InvalidArgument(format!(
                    "routine '{}' does not exist",
                    statement.signature
                ))),
            }
        }
        Statement::DropTrigger(statement) => match super::procedural::resolve_optional_trigger(
            generation,
            &statement.name,
            &statement.table,
        )? {
            Some(object) => require_owner(generation, actor, object),
            None if statement.if_exists => Ok(()),
            None => Err(Error::InvalidArgument(format!(
                "trigger '{}' does not exist",
                statement.name
            ))),
        },
        Statement::DropJob(statement) => {
            match super::procedural::resolve_optional_job(generation, &statement.name)? {
                Some(object) => require_owner(generation, actor, object),
                None if statement.if_exists => Ok(()),
                None => Err(Error::InvalidArgument(format!(
                    "job '{}' does not exist",
                    statement.name
                ))),
            }
        }
        Statement::AlterJob(statement) => {
            let object = super::procedural::resolve_optional_job(generation, &statement.name)?
                .ok_or_else(|| {
                    Error::InvalidArgument(format!("job '{}' does not exist", statement.name))
                })?;
            require_owner(generation, actor, object)
        }
        Statement::DropTable(statement) => require_optional_relation_owner(
            generation,
            actor,
            statement.table_name.value(),
            statement.if_exists,
        ),
        Statement::AlterTable(statement) => {
            let table = resolve_unqualified_table(generation, statement.table_name.value())?;
            require_owner(generation, actor, table)
        }
        Statement::DropView(statement) => require_optional_relation_owner(
            generation,
            actor,
            statement.view_name.value(),
            statement.if_exists,
        ),
        Statement::DropIndex(statement) => require_optional_index_owner(
            generation,
            actor,
            statement.index_name.value(),
            statement.if_exists,
        ),
        Statement::AlterIndex(statement) => {
            let index = generation
                .find_index(ObjectId::BOOTSTRAP_NAMESPACE, statement.index_name.value())
                .map_err(catalog_argument)?
                .ok_or_else(|| Error::IndexNotFound(statement.index_name.value().to_owned()))?;
            require_owner(generation, actor, index)
        }
        Statement::CreateSchema(_)
        | Statement::CreateExtension(_)
        | Statement::DropExtension(_)
        | Statement::CreatePrincipal(_)
        | Statement::CreateRole(_)
        | Statement::AlterSecuritySubject(_)
        | Statement::DropSecuritySubject(_)
        | Statement::CreateJob(_) => Err(permission_denied(format!(
            "only the bootstrap owner may execute {statement}"
        ))),
        Statement::Grant(_) | Statement::Revoke(_) | Statement::AlterOwner(_) => Ok(()),
        _ => Err(Error::internal(format!(
            "non-DDL statement reached catalog authority check: {statement}"
        ))),
    }
}

pub(super) fn require_table_schema_authority(
    generation: &CatalogGeneration,
    actor: ObjectId,
    table_name: &str,
) -> Result<()> {
    require_principal(generation, actor)?;
    if actor == ObjectId::BOOTSTRAP_OWNER {
        return Ok(());
    }
    match generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, table_name)
        .map_err(catalog_argument)?
    {
        Some(table) => require_owner(generation, actor, table),
        None => require_object_privilege(
            generation,
            actor,
            ObjectId::BOOTSTRAP_NAMESPACE,
            PRIVILEGE_CREATE,
            "CREATE",
        ),
    }
}

pub(super) fn require_routine_create_authority(
    generation: &CatalogGeneration,
    actor: ObjectId,
    name: &ObjectName,
) -> Result<()> {
    require_principal(generation, actor)?;
    if actor == ObjectId::BOOTSTRAP_OWNER {
        return Ok(());
    }
    let (namespace, _) = resolve_object_scope(generation, name)?;
    require_object_privilege(generation, actor, namespace, PRIVILEGE_CREATE, "CREATE")
}

pub(crate) fn require_external_type_create_authority(
    generation: &CatalogGeneration,
    actor: ObjectId,
    statement: &radixdb_sql::CreateExternalTypeStatement,
) -> Result<()> {
    require_principal(generation, actor)?;
    let extension = generation
        .find_extension(statement.extension_name.value())
        .map_err(catalog_argument)?
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "extension '{}' does not exist",
                statement.extension_name
            ))
        })?;
    if actor != ObjectId::BOOTSTRAP_OWNER {
        require_owner(generation, actor, extension)?;
        let (namespace, _) = resolve_object_scope(generation, &statement.name)?;
        require_object_privilege(generation, actor, namespace, PRIVILEGE_CREATE, "CREATE")?;
    }
    Ok(())
}

pub(crate) fn require_external_type_drop_authority(
    generation: &CatalogGeneration,
    actor: ObjectId,
    statement: &radixdb_sql::DropExternalTypeStatement,
) -> Result<()> {
    require_principal(generation, actor)?;
    let (namespace, name) = resolve_object_scope(generation, &statement.name)?;
    match generation
        .find_external_type(namespace, name)
        .map_err(catalog_argument)?
    {
        Some(external_type) => require_owner(generation, actor, external_type),
        None if statement.if_exists => Ok(()),
        None => Err(Error::InvalidArgument(format!(
            "type '{}' does not exist",
            statement.name
        ))),
    }
}

fn require_optional_relation_owner(
    generation: &CatalogGeneration,
    actor: ObjectId,
    name: &str,
    if_exists: bool,
) -> Result<()> {
    match resolve_unqualified_relation(generation, name) {
        Ok(relation) => require_owner(generation, actor, relation),
        Err(error) if if_exists && error.is_not_found() => Ok(()),
        Err(error) => Err(error),
    }
}

fn require_optional_index_owner(
    generation: &CatalogGeneration,
    actor: ObjectId,
    name: &str,
    if_exists: bool,
) -> Result<()> {
    let index = generation
        .find_index(ObjectId::BOOTSTRAP_NAMESPACE, name)
        .map_err(catalog_argument)?;
    match index {
        Some(index) => require_owner(generation, actor, index),
        None if if_exists => Ok(()),
        None => Err(Error::IndexNotFound(name.to_owned())),
    }
}

pub(super) fn bind_create_schema(
    statement: &CreateSchemaStatement,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    let (last, parent_path) = statement
        .name
        .components
        .split_last()
        .ok_or_else(|| Error::InvalidArgument("schema name is empty".to_owned()))?;
    let parent = if parent_path.is_empty() {
        ObjectId::BOOTSTRAP_NAMESPACE
    } else {
        resolve_namespace(generation, parent_path)?
    };
    if generation
        .find_namespace(Some(parent), last.value.as_str())
        .map_err(catalog_argument)?
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "schema '{}' already exists",
            statement.name
        )));
    }
    let id = ids.next(generation)?;
    let object = CatalogObject::new(
        id,
        Some(parent),
        Some(parent),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(last.value.as_str()).map_err(catalog_argument)?,
        1,
        CatalogPayload::Namespace(NamespacePayload::new()),
    )
    .map_err(catalog_argument)?;
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::create(object)],
        edge_additions: vec![CatalogEdge::new(parent, id, EdgeKind::Contains, 0)],
        ..DdlDelta::default()
    })
}

pub(super) fn bind_create_principal(
    statement: &CreatePrincipalStatement,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    let credential = statement
        .password
        .as_deref()
        .map(hash_password)
        .transpose()?;
    create_security_subject(
        statement.name.value.as_str(),
        CatalogPayload::Principal(
            PrincipalPayload::new(credential.is_some(), false).with_credential(credential),
        ),
        generation,
        ids,
    )
}

pub(super) fn bind_create_role(
    statement: &CreateRoleStatement,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    create_security_subject(
        statement.name.value.as_str(),
        CatalogPayload::Role(RolePayload::new(true)),
        generation,
        ids,
    )
}

fn create_security_subject(
    name: &str,
    payload: CatalogPayload,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    if find_subject(generation, name)?.is_some() {
        return Err(Error::InvalidArgument(format!(
            "security subject '{name}' already exists"
        )));
    }
    let id = ids.next(generation)?;
    let object = CatalogObject::new(
        id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).map_err(catalog_argument)?,
        1,
        payload,
    )
    .map_err(catalog_argument)?;
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::create(object)],
        ..DdlDelta::default()
    })
}

pub(super) fn bind_alter_security_subject(
    statement: &AlterSecuritySubjectStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let kind = subject_kind(statement.kind);
    let subject = require_subject_kind(generation, statement.name.value.as_str(), kind)?;
    if subject.id() == ObjectId::BOOTSTRAP_OWNER {
        return Err(permission_denied(
            "the bootstrap principal cannot be altered through subject lifecycle DDL",
        ));
    }
    match &statement.action {
        AlterSecuritySubjectActionSyntax::RenameTo(name) => {
            if let Some(existing) = find_subject(generation, name.value.as_str())? {
                if existing.id() != subject.id() {
                    return Err(Error::InvalidArgument(format!(
                        "security subject '{}' already exists",
                        name.value
                    )));
                }
                return Ok(DdlDelta::default());
            }
            Ok(DdlDelta {
                mutations: vec![CatalogMutation::rename(
                    precondition(subject)?,
                    CatalogName::new(name.value.as_str()).map_err(catalog_argument)?,
                )],
                ..DdlDelta::default()
            })
        }
        AlterSecuritySubjectActionSyntax::Enable | AlterSecuritySubjectActionSyntax::Disable => {
            let enabled = matches!(statement.action, AlterSecuritySubjectActionSyntax::Enable);
            let payload = match subject.payload() {
                CatalogPayload::Principal(payload) => {
                    if payload.login_enabled() == enabled {
                        return Ok(DdlDelta::default());
                    }
                    CatalogPayload::Principal(payload.clone().with_login_enabled(enabled))
                }
                CatalogPayload::Role(payload) => {
                    if payload.enabled() == enabled {
                        return Ok(DdlDelta::default());
                    }
                    CatalogPayload::Role(payload.clone().with_enabled(enabled))
                }
                _ => unreachable!("subject kind was checked"),
            };
            Ok(DdlDelta {
                mutations: vec![CatalogMutation::alter(
                    precondition(subject)?,
                    replace_payload(subject, payload)?,
                )],
                ..DdlDelta::default()
            })
        }
        AlterSecuritySubjectActionSyntax::SetPassword(password) => {
            let CatalogPayload::Principal(payload) = subject.payload() else {
                return Err(Error::invalid_argument(
                    "PASSWORD is valid only for PRINCIPAL",
                ));
            };
            let credential = hash_password(password)?;
            Ok(DdlDelta {
                mutations: vec![CatalogMutation::alter(
                    precondition(subject)?,
                    replace_payload(
                        subject,
                        CatalogPayload::Principal(
                            payload.clone().with_credential(Some(credential)),
                        ),
                    )?,
                )],
                ..DdlDelta::default()
            })
        }
        AlterSecuritySubjectActionSyntax::ClearPassword => {
            let CatalogPayload::Principal(payload) = subject.payload() else {
                return Err(Error::invalid_argument(
                    "PASSWORD is valid only for PRINCIPAL",
                ));
            };
            if payload.credential().is_none() {
                return Ok(DdlDelta::default());
            }
            Ok(DdlDelta {
                mutations: vec![CatalogMutation::alter(
                    precondition(subject)?,
                    replace_payload(
                        subject,
                        CatalogPayload::Principal(payload.clone().with_credential(None)),
                    )?,
                )],
                ..DdlDelta::default()
            })
        }
    }
}

fn hash_password(password: &str) -> Result<CredentialVerifier> {
    let encoded = crate::credentials::hash_password_verifier(password).map_err(|error| {
        if matches!(error, Error::InvalidArgument(_)) {
            Error::invalid_argument("principal password length must be in 1..=1024 bytes")
        } else {
            error
        }
    })?;
    CredentialVerifier::new(CREDENTIAL_SCHEME_ARGON2ID_PHC_V1, encoded.into_bytes())
        .map_err(catalog_argument)
}

pub(crate) fn authenticate_catalog_principal(
    generation: &CatalogGeneration,
    login: &str,
    password: &str,
) -> Result<ObjectId> {
    let principal =
        find_subject(generation, login)?.filter(|object| object.kind() == ObjectKind::Principal);
    let valid = principal
        .and_then(|object| {
            let CatalogPayload::Principal(payload) = object.payload() else {
                return None;
            };
            Some((object, payload))
        })
        .is_some_and(|(object, payload)| {
            payload.login_enabled()
                && payload.credential().is_some_and(|credential| {
                    verify_password(credential, password)
                        && has_object_privilege(
                            generation,
                            object.id(),
                            ObjectId::BOOTSTRAP_NAMESPACE,
                            PRIVILEGE_CONNECT,
                        )
                        .unwrap_or(false)
                })
        });
    if !valid {
        // Do not disclose whether the login, verifier, enabled bit or CONNECT
        // grant caused rejection.
        return Err(permission_denied("authentication failed"));
    }
    Ok(principal
        .expect("valid authentication has a principal")
        .id())
}

fn verify_password(credential: &CredentialVerifier, password: &str) -> bool {
    if credential.scheme() != CREDENTIAL_SCHEME_ARGON2ID_PHC_V1 {
        return false;
    }
    let Ok(encoded) = std::str::from_utf8(credential.encoded()) else {
        return false;
    };
    crate::credentials::verify_password_verifier(encoded, password)
}

pub(super) fn bind_drop_security_subject(
    statement: &DropSecuritySubjectStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let kind = subject_kind(statement.kind);
    let subject = require_subject_kind(generation, statement.name.value.as_str(), kind)?;
    if subject.id() == ObjectId::BOOTSTRAP_OWNER {
        return Err(permission_denied(
            "the bootstrap principal cannot be dropped",
        ));
    }
    let subject_id = subject.id();
    let mut acl_ids = BTreeSet::new();
    for acl in generation.objects_of_kind(ObjectKind::AclEntry) {
        let connected = acl
            .payload()
            .as_acl()
            .is_some_and(|payload| payload.grantor_principal_id() == subject_id)
            || acl_endpoints(generation, acl.id())
                .is_some_and(|(grantee, target)| grantee == subject_id || target == subject_id);
        if connected {
            acl_ids.insert(acl.id());
        }
    }
    let owned = generation
        .graph()
        .objects()
        .filter(|object| object.id() != subject_id && object.owner_principal_id() == subject_id)
        .map(CatalogObject::id)
        .collect::<BTreeSet<_>>();
    let jobs = if kind == ObjectKind::Principal {
        generation
            .objects_of_kind(ObjectKind::Job)
            .filter(|job| {
                matches!(job.payload(), CatalogPayload::Job(payload) if payload.principal_id() == subject_id)
            })
            .map(CatalogObject::id)
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    if statement.behavior == DropBehaviorSyntax::Restrict
        && (!acl_ids.is_empty() || !owned.is_empty() || !jobs.is_empty())
    {
        return Err(Error::InvalidArgument(format!(
            "cannot drop {} '{}' with RESTRICT: {} ACL entries, {} owned objects, {} RUN AS jobs depend on it",
            statement.kind,
            statement.name.value,
            acl_ids.len(),
            owned.len(),
            jobs.len()
        )));
    }

    let mut delta = DdlDelta::default();
    let mut dropped = acl_ids;
    dropped.extend(jobs);
    dropped.insert(subject_id);
    for id in &dropped {
        let object = generation
            .object(*id)
            .ok_or_else(|| Error::internal("subject cascade dependency disappeared"))?;
        delta
            .mutations
            .push(CatalogMutation::drop(precondition(object)?));
    }
    delta.edge_removals.extend(
        generation
            .graph()
            .edges()
            .iter()
            .filter(|edge| {
                dropped.contains(&edge.source_object_id())
                    || dropped.contains(&edge.target_object_id())
            })
            .copied(),
    );
    for id in owned {
        if dropped.contains(&id) {
            continue;
        }
        let object = generation
            .object(id)
            .ok_or_else(|| Error::internal("owned subject dependency disappeared"))?;
        delta.mutations.push(CatalogMutation::alter(
            precondition(object)?,
            replace_owner(object, ObjectId::BOOTSTRAP_OWNER)?,
        ));
    }
    resolve_dependent_grants(generation, delta, statement.behavior)
}

const fn subject_kind(kind: SecuritySubjectKindSyntax) -> ObjectKind {
    match kind {
        SecuritySubjectKindSyntax::Principal => ObjectKind::Principal,
        SecuritySubjectKindSyntax::Role => ObjectKind::Role,
    }
}

pub(super) fn bind_grant(
    statement: &GrantStatement,
    actor: ObjectId,
    current_database: Option<&str>,
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
) -> Result<DdlDelta> {
    require_principal(generation, actor)?;
    match &statement.grant {
        GrantSyntax::RoleMembership {
            role,
            member,
            admin_option,
        } => {
            let role = require_subject_kind(generation, role.value.as_str(), ObjectKind::Role)?;
            let member = require_subject(generation, member.value.as_str())?;
            require_membership_grant_authority(generation, actor, role.id())?;
            merge_membership(
                generation,
                ids,
                actor,
                role.id(),
                member.id(),
                *admin_option,
            )
        }
        GrantSyntax::ObjectPrivileges {
            privileges,
            target,
            grantee,
            grant_option,
        } => {
            let grantee = require_subject(generation, grantee.value.as_str())?;
            let target = resolve_privilege_target(generation, target, current_database)?;
            validate_privilege_target(privileges, target)?;
            let requested = bind_privileges(generation, target, privileges)?;
            require_grant_authority(generation, actor, target.id(), &requested)?;
            merge_object_privileges(
                generation,
                ids,
                actor,
                grantee.id(),
                target.id(),
                requested,
                *grant_option,
            )
        }
    }
}

pub(super) fn bind_revoke(
    statement: &RevokeStatement,
    actor: ObjectId,
    current_database: Option<&str>,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    require_principal(generation, actor)?;
    match &statement.revoke {
        RevokeSyntax::RoleMembership {
            role,
            member,
            admin_option_only,
        } => {
            let role = require_subject_kind(generation, role.value.as_str(), ObjectKind::Role)?;
            let member = require_subject(generation, member.value.as_str())?;
            require_membership_grant_authority(generation, actor, role.id())?;
            revoke_membership(
                generation,
                actor,
                role.id(),
                member.id(),
                *admin_option_only,
                statement.behavior,
            )
        }
        RevokeSyntax::ObjectPrivileges {
            privileges,
            target,
            grantee,
            grant_option_only,
        } => {
            let grantee = require_subject(generation, grantee.value.as_str())?;
            let target = resolve_privilege_target(generation, target, current_database)?;
            validate_privilege_target(privileges, target)?;
            let requested = bind_privileges(generation, target, privileges)?;
            require_grant_authority(generation, actor, target.id(), &requested)?;
            revoke_object_privileges(
                generation,
                actor,
                grantee.id(),
                target.id(),
                requested,
                *grant_option_only,
                statement.behavior,
            )
        }
    }
}

pub(super) fn bind_alter_owner(
    statement: &AlterOwnerStatement,
    actor: ObjectId,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    require_principal(generation, actor)?;
    let target = resolve_ownership_target(generation, &statement.target)?;
    if actor != ObjectId::BOOTSTRAP_OWNER && target.owner_principal_id() != actor {
        return Err(permission_denied(format!(
            "principal {actor} does not own {}",
            target.name().display().as_str()
        )));
    }
    let owner = require_subject_kind(
        generation,
        statement.owner.value.as_str(),
        ObjectKind::Principal,
    )?;
    if target.owner_principal_id() == owner.id() {
        return Ok(DdlDelta::default());
    }
    let replacement = replace_owner(target, owner.id())?;
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::alter(precondition(target)?, replacement)],
        ..DdlDelta::default()
    })
}

pub(crate) fn require_principal(
    generation: &CatalogGeneration,
    principal: ObjectId,
) -> Result<&CatalogObject> {
    if principal == ObjectId::BOOTSTRAP_OWNER && generation.format_minor() == 0 {
        return generation
            .object(ObjectId::BOOTSTRAP_NAMESPACE)
            .ok_or_else(|| Error::internal("bootstrap catalog namespace disappeared"));
    }
    generation
        .object(principal)
        .filter(|object| object.kind() == ObjectKind::Principal)
        .ok_or_else(|| permission_denied(format!("principal {principal} does not exist")))
}

pub(crate) fn role_closure(
    generation: &CatalogGeneration,
    principal: ObjectId,
) -> Result<BTreeSet<ObjectId>> {
    require_principal(generation, principal)?;
    let mut closure = BTreeSet::from([principal]);
    let mut queue = VecDeque::from([principal]);
    while let Some(member) = queue.pop_front() {
        for acl in generation.objects_of_kind(ObjectKind::AclEntry) {
            if !matches!(
                acl.payload(),
                CatalogPayload::AclEntry(AclEntryPayload::RoleMembership { .. })
            ) {
                continue;
            }
            let Some((grantee, role)) = acl_endpoints(generation, acl.id()) else {
                continue;
            };
            if generation.object(role).is_some_and(|role| {
                matches!(role.payload(), CatalogPayload::Role(payload) if !payload.enabled())
            }) {
                continue;
            }
            if grantee == member && closure.insert(role) {
                if closure.len() > MAX_AUTHORIZATION_SUBJECTS {
                    return Err(permission_denied(
                        "authorization role closure exceeds bounded subject limit",
                    ));
                }
                queue.push_back(role);
            }
        }
    }
    Ok(closure)
}

pub(crate) fn has_object_privilege(
    generation: &CatalogGeneration,
    principal: ObjectId,
    target: ObjectId,
    privilege: u64,
) -> Result<bool> {
    if principal == ObjectId::BOOTSTRAP_OWNER {
        return Ok(true);
    }
    require_principal(generation, principal)?;
    let target_object = generation
        .object(target)
        .ok_or_else(|| Error::InvalidArgument(format!("catalog object {target} does not exist")))?;
    if target_object.owner_principal_id() == principal {
        return Ok(true);
    }
    let subjects = role_closure(generation, principal)?;
    Ok(
        object_acl_entries(generation, target).any(|(acl, grantee)| {
            subjects.contains(&grantee)
                && matches!(
                    acl.payload(),
                    CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges { privileges, .. })
                        if privileges & privilege != 0
                )
        }),
    )
}

pub(crate) fn has_column_privilege(
    generation: &CatalogGeneration,
    principal: ObjectId,
    table: ObjectId,
    column: ObjectId,
    privilege: u64,
) -> Result<bool> {
    if has_object_privilege(generation, principal, table, privilege)? {
        return Ok(true);
    }
    let subjects = role_closure(generation, principal)?;
    Ok(object_acl_entries(generation, table).any(|(acl, grantee)| {
        if !subjects.contains(&grantee) {
            return false;
        }
        matches!(
            acl.payload(),
            CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges { columns, .. })
                if columns.iter().any(|group| {
                    group.privilege() == privilege && group.column_ids().contains(&column)
                })
        )
    }))
}

pub(crate) fn require_object_privilege(
    generation: &CatalogGeneration,
    principal: ObjectId,
    target: ObjectId,
    privilege: u64,
    label: &str,
) -> Result<()> {
    if has_object_privilege(generation, principal, target, privilege)? {
        Ok(())
    } else {
        Err(permission_denied(format!(
            "principal {principal} lacks {label} on catalog object {target}"
        )))
    }
}

pub(crate) fn require_column_privilege(
    generation: &CatalogGeneration,
    principal: ObjectId,
    table: ObjectId,
    column: ObjectId,
    privilege: u64,
    label: &str,
) -> Result<()> {
    if has_column_privilege(generation, principal, table, column, privilege)? {
        Ok(())
    } else {
        Err(permission_denied(format!(
            "principal {principal} lacks {label} on column {column} of catalog object {table}"
        )))
    }
}

pub(crate) fn require_owner(
    generation: &CatalogGeneration,
    principal: ObjectId,
    object: &CatalogObject,
) -> Result<()> {
    require_principal(generation, principal)?;
    if principal == ObjectId::BOOTSTRAP_OWNER || object.owner_principal_id() == principal {
        Ok(())
    } else {
        Err(permission_denied(format!(
            "principal {principal} does not own {:?} '{}'",
            object.kind(),
            object.name().display().as_str()
        )))
    }
}

pub(crate) fn resolve_unqualified_relation<'a>(
    generation: &'a CatalogGeneration,
    name: &str,
) -> Result<&'a CatalogObject> {
    generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, name)
        .map_err(catalog_argument)?
        .filter(|object| matches!(object.kind(), ObjectKind::Table | ObjectKind::View))
        .ok_or_else(|| Error::TableOrViewNotFound(name.to_owned()))
}

pub(crate) fn resolve_unqualified_table<'a>(
    generation: &'a CatalogGeneration,
    name: &str,
) -> Result<&'a CatalogObject> {
    resolve_unqualified_relation(generation, name).and_then(|object| {
        if object.kind() == ObjectKind::Table {
            Ok(object)
        } else {
            Err(Error::TableNotFound(name.to_owned()))
        }
    })
}

pub(crate) fn require_namespace_usage(
    generation: &CatalogGeneration,
    principal: ObjectId,
    object: &CatalogObject,
) -> Result<()> {
    let namespace = object.namespace_id().ok_or_else(|| {
        Error::internal(format!(
            "securable {:?} {} has no namespace",
            object.kind(),
            object.id()
        ))
    })?;
    require_object_privilege(generation, principal, namespace, PRIVILEGE_USAGE, "USAGE")
}

pub(crate) fn find_subject<'a>(
    generation: &'a CatalogGeneration,
    name: &str,
) -> Result<Option<&'a CatalogObject>> {
    let normalized = CatalogName::new(name).map_err(catalog_argument)?;
    Ok([ObjectKind::Principal, ObjectKind::Role]
        .into_iter()
        .flat_map(|kind| generation.objects_of_kind(kind))
        .find(|object| object.name().normalized() == normalized.normalized()))
}

fn require_subject<'a>(generation: &'a CatalogGeneration, name: &str) -> Result<&'a CatalogObject> {
    find_subject(generation, name)?
        .ok_or_else(|| Error::InvalidArgument(format!("security subject '{name}' does not exist")))
}

fn require_subject_kind<'a>(
    generation: &'a CatalogGeneration,
    name: &str,
    kind: ObjectKind,
) -> Result<&'a CatalogObject> {
    let subject = require_subject(generation, name)?;
    if subject.kind() != kind {
        return Err(Error::InvalidArgument(format!(
            "security subject '{name}' is {:?}, expected {kind:?}",
            subject.kind()
        )));
    }
    Ok(subject)
}

fn resolve_privilege_target<'a>(
    generation: &'a CatalogGeneration,
    target: &PrivilegeTargetSyntax,
    current_database: Option<&str>,
) -> Result<&'a CatalogObject> {
    match target {
        PrivilegeTargetSyntax::Database(name) => {
            if name.components.len() != 1 {
                return Err(Error::InvalidArgument(
                    "database privilege target cannot be qualified".to_owned(),
                ));
            }
            if let Some(current) = current_database {
                let requested = name.components[0].value.as_str();
                if !current.eq_ignore_ascii_case(requested) {
                    return Err(Error::InvalidArgument(format!(
                        "database '{requested}' is not the current database '{current}'"
                    )));
                }
            }
            generation
                .object(ObjectId::BOOTSTRAP_NAMESPACE)
                .ok_or_else(|| Error::internal("bootstrap database root disappeared"))
        }
        PrivilegeTargetSyntax::Schema(name) => {
            let id = resolve_namespace(generation, &name.components)?;
            generation
                .object(id)
                .ok_or_else(|| Error::internal("resolved schema disappeared"))
        }
        PrivilegeTargetSyntax::Table(name) => resolve_relation(generation, name),
        PrivilegeTargetSyntax::Function(signature) => {
            resolve_routine(generation, signature, ObjectKind::Function)
        }
        PrivilegeTargetSyntax::Procedure(signature) => {
            resolve_routine(generation, signature, ObjectKind::Procedure)
        }
    }
}

fn resolve_ownership_target<'a>(
    generation: &'a CatalogGeneration,
    target: &OwnershipTargetSyntax,
) -> Result<&'a CatalogObject> {
    match target {
        OwnershipTargetSyntax::Table(name) => resolve_relation(generation, name),
        OwnershipTargetSyntax::Function(signature) => {
            resolve_routine(generation, signature, ObjectKind::Function)
        }
        OwnershipTargetSyntax::Procedure(signature) => {
            resolve_routine(generation, signature, ObjectKind::Procedure)
        }
    }
}

pub(crate) fn resolve_relation<'a>(
    generation: &'a CatalogGeneration,
    name: &ObjectName,
) -> Result<&'a CatalogObject> {
    let (namespace, relation_name) = resolve_object_scope(generation, name)?;
    generation
        .find_relation(namespace, relation_name)
        .map_err(catalog_argument)?
        .filter(|object| matches!(object.kind(), ObjectKind::Table | ObjectKind::View))
        .ok_or_else(|| Error::InvalidArgument(format!("relation '{name}' does not exist")))
}

fn resolve_routine<'a>(
    generation: &'a CatalogGeneration,
    signature: &RoutineSignatureSyntax,
    kind: ObjectKind,
) -> Result<&'a CatalogObject> {
    let (namespace, name) = resolve_object_scope(generation, &signature.name)?;
    let types = signature
        .argument_types
        .iter()
        .map(|data_type| bind_durable_type(data_type, generation))
        .collect::<Result<Vec<_>>>()?;
    generation
        .find_routine(namespace, kind, name, &types)
        .map_err(catalog_argument)?
        .ok_or_else(|| Error::InvalidArgument(format!("{kind:?} '{signature}' does not exist")))
}

fn validate_privilege_target(privileges: &[PrivilegeSyntax], target: &CatalogObject) -> Result<()> {
    for privilege in privileges {
        let valid = match privilege.kind {
            ObjectPrivilegeSyntax::Connect => target.id() == ObjectId::BOOTSTRAP_NAMESPACE,
            ObjectPrivilegeSyntax::Usage => target.kind() == ObjectKind::Namespace,
            ObjectPrivilegeSyntax::Create => target.kind() == ObjectKind::Namespace,
            ObjectPrivilegeSyntax::Select => {
                matches!(target.kind(), ObjectKind::Table | ObjectKind::View)
            }
            ObjectPrivilegeSyntax::Insert
            | ObjectPrivilegeSyntax::Update
            | ObjectPrivilegeSyntax::Delete => target.kind() == ObjectKind::Table,
            ObjectPrivilegeSyntax::Execute => {
                matches!(target.kind(), ObjectKind::Function | ObjectKind::Procedure)
            }
        };
        if !valid {
            return Err(Error::InvalidArgument(format!(
                "{} is not valid on {:?}",
                privilege.kind,
                target.kind()
            )));
        }
        if !privilege.columns.is_empty() && target.kind() != ObjectKind::Table {
            return Err(Error::InvalidArgument(
                "column privileges require a TABLE target".to_owned(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BoundPrivileges {
    object_bits: u64,
    columns: BTreeMap<u64, BTreeSet<ObjectId>>,
}

fn bind_privileges(
    generation: &CatalogGeneration,
    target: &CatalogObject,
    privileges: &[PrivilegeSyntax],
) -> Result<BoundPrivileges> {
    let mut bound = BoundPrivileges::default();
    for privilege in privileges {
        let bit = privilege_bit(privilege.kind);
        if privilege.columns.is_empty() {
            bound.object_bits |= bit;
            continue;
        }
        let ids = bound.columns.entry(bit).or_default();
        for column in &privilege.columns {
            let object = generation
                .find_column(target.id(), column.value.as_str())
                .map_err(catalog_argument)?
                .ok_or_else(|| Error::ColumnNotFound(column.value.to_string()))?;
            ids.insert(object.id());
        }
    }
    Ok(bound)
}

fn require_grant_authority(
    generation: &CatalogGeneration,
    actor: ObjectId,
    target: ObjectId,
    requested: &BoundPrivileges,
) -> Result<()> {
    if actor == ObjectId::BOOTSTRAP_OWNER
        || generation
            .object(target)
            .is_some_and(|object| object.owner_principal_id() == actor)
    {
        return Ok(());
    }
    let subjects = role_closure(generation, actor)?;
    let options = grant_option_coverage(generation, target, &subjects, &BTreeSet::new());
    if privileges_cover(&options, requested) {
        Ok(())
    } else {
        Err(permission_denied(format!(
            "principal {actor} cannot grant requested privileges on {target}"
        )))
    }
}

fn require_membership_grant_authority(
    generation: &CatalogGeneration,
    actor: ObjectId,
    role: ObjectId,
) -> Result<()> {
    if actor == ObjectId::BOOTSTRAP_OWNER
        || generation
            .object(role)
            .is_some_and(|object| object.owner_principal_id() == actor)
    {
        return Ok(());
    }
    let subjects = role_closure(generation, actor)?;
    let can_admin = generation.objects_of_kind(ObjectKind::AclEntry).any(|acl| {
        let CatalogPayload::AclEntry(AclEntryPayload::RoleMembership { admin_option, .. }) =
            acl.payload()
        else {
            return false;
        };
        let Some((member, target_role)) = acl_endpoints(generation, acl.id()) else {
            return false;
        };
        *admin_option && target_role == role && subjects.contains(&member)
    });
    if can_admin {
        Ok(())
    } else {
        Err(permission_denied(format!(
            "principal {actor} cannot grant or revoke role {role}"
        )))
    }
}

fn merge_membership(
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
    grantor: ObjectId,
    role: ObjectId,
    member: ObjectId,
    admin_option: bool,
) -> Result<DdlDelta> {
    if let Some(existing) = find_acl(generation, grantor, member, role, true) {
        let CatalogPayload::AclEntry(AclEntryPayload::RoleMembership {
            admin_option: current,
            ..
        }) = existing.payload()
        else {
            unreachable!("ACL discriminator was filtered")
        };
        if *current || !admin_option {
            return Ok(DdlDelta::default());
        }
        let replacement = replace_payload(
            existing,
            CatalogPayload::AclEntry(AclEntryPayload::role_membership(grantor, true)),
        )?;
        return Ok(DdlDelta {
            mutations: vec![CatalogMutation::alter(precondition(existing)?, replacement)],
            ..DdlDelta::default()
        });
    }
    create_acl(
        generation,
        ids,
        grantor,
        member,
        role,
        AclEntryPayload::role_membership(grantor, admin_option),
    )
}

fn revoke_membership(
    generation: &CatalogGeneration,
    grantor: ObjectId,
    role: ObjectId,
    member: ObjectId,
    admin_option_only: bool,
    behavior: DropBehaviorSyntax,
) -> Result<DdlDelta> {
    let Some(existing) = find_acl(generation, grantor, member, role, true) else {
        return Ok(DdlDelta::default());
    };
    if admin_option_only {
        let CatalogPayload::AclEntry(AclEntryPayload::RoleMembership { admin_option, .. }) =
            existing.payload()
        else {
            unreachable!("ACL discriminator was filtered")
        };
        if !admin_option {
            return Ok(DdlDelta::default());
        }
        let replacement = replace_payload(
            existing,
            CatalogPayload::AclEntry(AclEntryPayload::role_membership(grantor, false)),
        )?;
        return resolve_dependent_grants(
            generation,
            DdlDelta {
                mutations: vec![CatalogMutation::alter(precondition(existing)?, replacement)],
                ..DdlDelta::default()
            },
            behavior,
        );
    }
    resolve_dependent_grants(generation, drop_acl(generation, existing)?, behavior)
}

fn merge_object_privileges(
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
    grantor: ObjectId,
    grantee: ObjectId,
    target: ObjectId,
    requested: BoundPrivileges,
    with_grant_option: bool,
) -> Result<DdlDelta> {
    if let Some(existing) = find_acl(generation, grantor, grantee, target, false) {
        let CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges {
            privileges,
            grant_option,
            columns,
            column_grant_options,
            ..
        }) = existing.payload()
        else {
            unreachable!("ACL discriminator was filtered")
        };
        let mut merged = BoundPrivileges {
            object_bits: *privileges | requested.object_bits,
            columns: column_map(columns),
        };
        merge_column_map(&mut merged.columns, requested.columns.clone());
        let mut options = BoundPrivileges {
            object_bits: *grant_option,
            columns: column_map(column_grant_options),
        };
        if with_grant_option {
            options.object_bits |= requested.object_bits;
            merge_column_map(&mut options.columns, requested.columns);
        }
        let payload = bound_acl_payload(grantor, merged, options)?;
        if existing.payload() == &CatalogPayload::AclEntry(payload.clone()) {
            return Ok(DdlDelta::default());
        }
        let replacement = replace_payload(existing, CatalogPayload::AclEntry(payload))?;
        return Ok(DdlDelta {
            mutations: vec![CatalogMutation::alter(precondition(existing)?, replacement)],
            ..DdlDelta::default()
        });
    }
    let options = if with_grant_option {
        requested.clone()
    } else {
        BoundPrivileges::default()
    };
    let payload = bound_acl_payload(grantor, requested, options)?;
    create_acl(generation, ids, grantor, grantee, target, payload)
}

fn revoke_object_privileges(
    generation: &CatalogGeneration,
    grantor: ObjectId,
    grantee: ObjectId,
    target: ObjectId,
    requested: BoundPrivileges,
    grant_option_only: bool,
    behavior: DropBehaviorSyntax,
) -> Result<DdlDelta> {
    let Some(existing) = find_acl(generation, grantor, grantee, target, false) else {
        return Ok(DdlDelta::default());
    };
    let CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges {
        privileges,
        grant_option,
        columns,
        column_grant_options,
        ..
    }) = existing.payload()
    else {
        unreachable!("ACL discriminator was filtered")
    };
    let mut remaining = BoundPrivileges {
        object_bits: *privileges,
        columns: column_map(columns),
    };
    let mut options = BoundPrivileges {
        object_bits: *grant_option,
        columns: column_map(column_grant_options),
    };
    subtract_bound(&mut options, &requested);
    if !grant_option_only {
        subtract_bound(&mut remaining, &requested);
        options.object_bits &= remaining.object_bits;
        intersect_column_map(&mut options.columns, &remaining.columns);
    }
    if remaining.object_bits == 0 && remaining.columns.is_empty() {
        return resolve_dependent_grants(generation, drop_acl(generation, existing)?, behavior);
    }
    let payload = bound_acl_payload(grantor, remaining, options)?;
    if existing.payload() == &CatalogPayload::AclEntry(payload.clone()) {
        return Ok(DdlDelta::default());
    }
    let replacement = replace_payload(existing, CatalogPayload::AclEntry(payload))?;
    resolve_dependent_grants(
        generation,
        DdlDelta {
            mutations: vec![CatalogMutation::alter(precondition(existing)?, replacement)],
            ..DdlDelta::default()
        },
        behavior,
    )
}

fn create_acl(
    generation: &CatalogGeneration,
    ids: &mut ObjectIdSource,
    grantor: ObjectId,
    grantee: ObjectId,
    target: ObjectId,
    payload: AclEntryPayload,
) -> Result<DdlDelta> {
    let id = ids.next(generation)?;
    let object = CatalogObject::new(
        id,
        None,
        None,
        grantor,
        CatalogName::new(format!("acl_{id}")).map_err(catalog_argument)?,
        1,
        CatalogPayload::AclEntry(payload),
    )
    .map_err(catalog_argument)?;
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::create(object)],
        edge_additions: vec![
            CatalogEdge::new(id, grantee, EdgeKind::GrantedTo, 0),
            CatalogEdge::new(id, target, EdgeKind::GrantsOn, 0),
        ],
        ..DdlDelta::default()
    })
}

fn drop_acl(generation: &CatalogGeneration, acl: &CatalogObject) -> Result<DdlDelta> {
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::drop(precondition(acl)?)],
        edge_removals: generation
            .graph()
            .outgoing_edges(acl.id())
            .copied()
            .collect(),
        ..DdlDelta::default()
    })
}

fn resolve_dependent_grants(
    source: &CatalogGeneration,
    root: DdlDelta,
    behavior: DropBehaviorSyntax,
) -> Result<DdlDelta> {
    let root_generation = apply_delta_for_analysis(source, &root)?;
    let mut working = root_generation;
    let mut dependent_changes = 0_usize;
    // Every cleanup step removes at least one membership or one granted
    // privilege atom. An ACL can shrink more than once as upstream paths
    // disappear, so the number of ACL objects alone is not a valid bound.
    let limit = working
        .objects_of_kind(ObjectKind::AclEntry)
        .map(|acl| match acl.payload() {
            CatalogPayload::AclEntry(AclEntryPayload::RoleMembership { .. }) => 1,
            CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges {
                privileges,
                columns,
                ..
            }) => {
                (*privileges).count_ones() as usize
                    + columns
                        .iter()
                        .map(|group| group.column_ids().len())
                        .sum::<usize>()
            }
            _ => 0,
        })
        .fold(1_usize, usize::saturating_add);
    loop {
        let mut change = None;
        for acl in working.objects_of_kind(ObjectKind::AclEntry) {
            let Some((_, target)) = acl_endpoints(&working, acl.id()) else {
                continue;
            };
            match acl.payload() {
                CatalogPayload::AclEntry(AclEntryPayload::RoleMembership {
                    grantor_principal_id,
                    ..
                }) => {
                    if !has_membership_grant_authority_excluding(
                        &working,
                        *grantor_principal_id,
                        target,
                        acl.id(),
                    )? {
                        change = Some(drop_acl(&working, acl)?);
                        break;
                    }
                }
                CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges {
                    grantor_principal_id,
                    privileges,
                    grant_option,
                    columns,
                    column_grant_options,
                }) => {
                    if *grantor_principal_id == ObjectId::BOOTSTRAP_OWNER
                        || working.object(target).is_some_and(|object| {
                            object.owner_principal_id() == *grantor_principal_id
                        })
                    {
                        continue;
                    }
                    let subjects =
                        role_closure_excluding(&working, *grantor_principal_id, acl.id())?;
                    let available = grant_option_coverage(
                        &working,
                        target,
                        &subjects,
                        &BTreeSet::from([acl.id()]),
                    );
                    let granted = BoundPrivileges {
                        object_bits: *privileges,
                        columns: column_map(columns),
                    };
                    let retained = retain_covered(&granted, &available);
                    if retained == granted {
                        continue;
                    }
                    if retained.object_bits == 0 && retained.columns.is_empty() {
                        change = Some(drop_acl(&working, acl)?);
                    } else {
                        let mut options = BoundPrivileges {
                            object_bits: *grant_option,
                            columns: column_map(column_grant_options),
                        };
                        options.object_bits &= retained.object_bits;
                        intersect_column_map(&mut options.columns, &retained.columns);
                        let replacement = replace_payload(
                            acl,
                            CatalogPayload::AclEntry(bound_acl_payload(
                                *grantor_principal_id,
                                retained,
                                options,
                            )?),
                        )?;
                        change = Some(DdlDelta {
                            mutations: vec![CatalogMutation::alter(
                                precondition(acl)?,
                                replacement,
                            )],
                            ..DdlDelta::default()
                        });
                    }
                    break;
                }
                _ => {}
            }
        }
        let Some(change) = change else {
            break;
        };
        dependent_changes += 1;
        if dependent_changes > limit {
            return Err(Error::internal(
                "ACL dependency cleanup exceeded its bounded convergence limit",
            ));
        }
        working = apply_delta_for_analysis(&working, &change)?;
    }
    if dependent_changes != 0 && behavior == DropBehaviorSyntax::Restrict {
        return Err(Error::InvalidArgument(format!(
            "RESTRICT would invalidate {dependent_changes} dependent grant(s); use CASCADE"
        )));
    }
    if dependent_changes == 0 {
        return Ok(root);
    }
    diff_acl_generations(source, &working)
}

fn has_membership_grant_authority_excluding(
    generation: &CatalogGeneration,
    actor: ObjectId,
    role: ObjectId,
    excluded_acl: ObjectId,
) -> Result<bool> {
    if actor == ObjectId::BOOTSTRAP_OWNER
        || generation
            .object(role)
            .is_some_and(|object| object.owner_principal_id() == actor)
    {
        return Ok(true);
    }
    let subjects = role_closure_excluding(generation, actor, excluded_acl)?;
    Ok(generation.objects_of_kind(ObjectKind::AclEntry).any(|acl| {
        if acl.id() == excluded_acl {
            return false;
        }
        let CatalogPayload::AclEntry(AclEntryPayload::RoleMembership { admin_option, .. }) =
            acl.payload()
        else {
            return false;
        };
        let Some((member, target_role)) = acl_endpoints(generation, acl.id()) else {
            return false;
        };
        *admin_option && target_role == role && subjects.contains(&member)
    }))
}

fn role_closure_excluding(
    generation: &CatalogGeneration,
    principal: ObjectId,
    excluded_acl: ObjectId,
) -> Result<BTreeSet<ObjectId>> {
    require_principal(generation, principal)?;
    let mut closure = BTreeSet::from([principal]);
    let mut queue = VecDeque::from([principal]);
    while let Some(member) = queue.pop_front() {
        for acl in generation.objects_of_kind(ObjectKind::AclEntry) {
            if acl.id() == excluded_acl
                || !matches!(
                    acl.payload(),
                    CatalogPayload::AclEntry(AclEntryPayload::RoleMembership { .. })
                )
            {
                continue;
            }
            let Some((grantee, role)) = acl_endpoints(generation, acl.id()) else {
                continue;
            };
            if generation.object(role).is_some_and(|role| {
                matches!(role.payload(), CatalogPayload::Role(payload) if !payload.enabled())
            }) {
                continue;
            }
            if grantee == member && closure.insert(role) {
                if closure.len() > MAX_AUTHORIZATION_SUBJECTS {
                    return Err(permission_denied(
                        "authorization role closure exceeds bounded subject limit",
                    ));
                }
                queue.push_back(role);
            }
        }
    }
    Ok(closure)
}

fn retain_covered(granted: &BoundPrivileges, available: &BoundPrivileges) -> BoundPrivileges {
    let mut retained = BoundPrivileges {
        object_bits: granted.object_bits & available.object_bits,
        columns: BTreeMap::new(),
    };
    for (bit, columns) in &granted.columns {
        if available.object_bits & bit != 0 {
            retained.columns.insert(*bit, columns.clone());
            continue;
        }
        if let Some(available_columns) = available.columns.get(bit) {
            let intersection = columns
                .intersection(available_columns)
                .copied()
                .collect::<BTreeSet<_>>();
            if !intersection.is_empty() {
                retained.columns.insert(*bit, intersection);
            }
        }
    }
    retained
}

fn apply_delta_for_analysis(
    generation: &CatalogGeneration,
    delta: &DdlDelta,
) -> Result<CatalogGeneration> {
    let mut edge_removals = delta.edge_removals.clone();
    let mut edge_additions = delta.edge_additions.clone();
    for mutation in &delta.mutations {
        if let CatalogMutation::Alter {
            expected,
            replacement,
        } = mutation
        {
            let current = generation
                .object(expected.object_id())
                .ok_or_else(|| Error::internal("ACL analysis ALTER target disappeared"))?;
            if current.owner_principal_id() != replacement.owner_principal_id() {
                edge_removals.push(CatalogEdge::new(
                    current.id(),
                    current.owner_principal_id(),
                    EdgeKind::OwnedBy,
                    0,
                ));
                edge_additions.push(CatalogEdge::new(
                    replacement.id(),
                    replacement.owner_principal_id(),
                    EdgeKind::OwnedBy,
                    0,
                ));
            }
        }
    }
    edge_removals.sort_unstable();
    edge_removals.dedup();
    edge_additions.sort_unstable();
    edge_additions.dedup();
    let set = CatalogMutationSet::for_generation(
        generation,
        delta.mutations.clone(),
        edge_removals,
        edge_additions,
    )
    .map_err(catalog_argument)?;
    let graph = set.apply(generation).map_err(catalog_argument)?;
    Ok(CatalogGeneration::new(generation.meta(), graph))
}

fn diff_acl_generations(
    source: &CatalogGeneration,
    target: &CatalogGeneration,
) -> Result<DdlDelta> {
    let source_objects = source
        .graph()
        .objects()
        .map(|object| (object.id(), object))
        .collect::<BTreeMap<_, _>>();
    let target_objects = target
        .graph()
        .objects()
        .map(|object| (object.id(), object))
        .collect::<BTreeMap<_, _>>();
    let mut mutations = Vec::new();
    for (id, object) in &source_objects {
        match target_objects.get(id) {
            None => mutations.push(CatalogMutation::drop(precondition(object)?)),
            Some(replacement) if *object != *replacement => mutations.push(CatalogMutation::alter(
                precondition(object)?,
                (*replacement).clone(),
            )),
            _ => {}
        }
    }
    let source_edges = source
        .graph()
        .edges()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let target_edges = target
        .graph()
        .edges()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    Ok(DdlDelta {
        mutations,
        edge_removals: source_edges.difference(&target_edges).copied().collect(),
        edge_additions: target_edges.difference(&source_edges).copied().collect(),
    })
}

fn find_acl(
    generation: &CatalogGeneration,
    grantor: ObjectId,
    grantee: ObjectId,
    target: ObjectId,
    membership: bool,
) -> Option<&CatalogObject> {
    generation
        .objects_of_kind(ObjectKind::AclEntry)
        .find(|acl| {
            let Some((actual_grantee, actual_target)) = acl_endpoints(generation, acl.id()) else {
                return false;
            };
            acl.payload().as_acl().is_some_and(|payload| {
                payload.grantor_principal_id() == grantor
                    && matches!(payload, AclEntryPayload::RoleMembership { .. }) == membership
            }) && actual_grantee == grantee
                && actual_target == target
        })
}

fn acl_endpoints(generation: &CatalogGeneration, acl: ObjectId) -> Option<(ObjectId, ObjectId)> {
    let mut grantee = None;
    let mut target = None;
    for edge in generation.graph().outgoing_edges(acl) {
        match edge.kind() {
            EdgeKind::GrantedTo => grantee = Some(edge.target_object_id()),
            EdgeKind::GrantsOn => target = Some(edge.target_object_id()),
            _ => {}
        }
    }
    Some((grantee?, target?))
}

fn object_acl_entries(
    generation: &CatalogGeneration,
    target: ObjectId,
) -> impl Iterator<Item = (&CatalogObject, ObjectId)> {
    generation
        .objects_of_kind(ObjectKind::AclEntry)
        .filter_map(move |acl| {
            let (grantee, acl_target) = acl_endpoints(generation, acl.id())?;
            (acl_target == target
                && matches!(
                    acl.payload(),
                    CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges { .. })
                ))
            .then_some((acl, grantee))
        })
}

fn bound_acl_payload(
    grantor: ObjectId,
    privileges: BoundPrivileges,
    grant_options: BoundPrivileges,
) -> Result<AclEntryPayload> {
    let columns = privileges
        .columns
        .into_iter()
        .map(|(privilege, ids)| {
            ColumnPrivilegeSet::new(privilege, ids.into_iter().collect()).map_err(catalog_argument)
        })
        .collect::<Result<Vec<_>>>()?;
    let column_grant_options = grant_options
        .columns
        .into_iter()
        .map(|(privilege, ids)| {
            ColumnPrivilegeSet::new(privilege, ids.into_iter().collect()).map_err(catalog_argument)
        })
        .collect::<Result<Vec<_>>>()?;
    AclEntryPayload::object_privileges_with_column_options(
        grantor,
        privileges.object_bits,
        grant_options.object_bits & privileges.object_bits,
        columns,
        column_grant_options,
    )
    .map_err(catalog_argument)
}

fn column_map(columns: &[ColumnPrivilegeSet]) -> BTreeMap<u64, BTreeSet<ObjectId>> {
    columns
        .iter()
        .map(|group| {
            (
                group.privilege(),
                group.column_ids().iter().copied().collect(),
            )
        })
        .collect()
}

fn merge_column_map(
    target: &mut BTreeMap<u64, BTreeSet<ObjectId>>,
    source: BTreeMap<u64, BTreeSet<ObjectId>>,
) {
    for (privilege, columns) in source {
        target.entry(privilege).or_default().extend(columns);
    }
}

fn subtract_bound(target: &mut BoundPrivileges, requested: &BoundPrivileges) {
    target.object_bits &= !requested.object_bits;
    for (bit, requested_columns) in &requested.columns {
        if let Some(current) = target.columns.get_mut(bit) {
            current.retain(|column| !requested_columns.contains(column));
            if current.is_empty() {
                target.columns.remove(bit);
            }
        }
    }
}

fn intersect_column_map(
    target: &mut BTreeMap<u64, BTreeSet<ObjectId>>,
    allowed: &BTreeMap<u64, BTreeSet<ObjectId>>,
) {
    target.retain(|bit, columns| {
        let Some(allowed_columns) = allowed.get(bit) else {
            return false;
        };
        columns.retain(|column| allowed_columns.contains(column));
        !columns.is_empty()
    });
}

fn privileges_cover(available: &BoundPrivileges, requested: &BoundPrivileges) -> bool {
    if available.object_bits & requested.object_bits != requested.object_bits {
        return false;
    }
    requested.columns.iter().all(|(bit, columns)| {
        available.object_bits & bit != 0
            || available
                .columns
                .get(bit)
                .is_some_and(|granted| columns.iter().all(|column| granted.contains(column)))
    })
}

fn grant_option_coverage(
    generation: &CatalogGeneration,
    target: ObjectId,
    subjects: &BTreeSet<ObjectId>,
    excluded_acl_ids: &BTreeSet<ObjectId>,
) -> BoundPrivileges {
    let mut options = BoundPrivileges::default();
    for (acl, grantee) in object_acl_entries(generation, target) {
        if excluded_acl_ids.contains(&acl.id()) || !subjects.contains(&grantee) {
            continue;
        }
        let CatalogPayload::AclEntry(AclEntryPayload::ObjectPrivileges {
            grant_option,
            column_grant_options,
            ..
        }) = acl.payload()
        else {
            continue;
        };
        options.object_bits |= *grant_option;
        merge_column_map(&mut options.columns, column_map(column_grant_options));
    }
    options
}

fn replace_owner(object: &CatalogObject, owner: ObjectId) -> Result<CatalogObject> {
    CatalogObject::new(
        object.id(),
        object.namespace_id(),
        object.parent_id(),
        owner,
        object.name().clone(),
        object
            .definition_revision()
            .checked_add(1)
            .ok_or_else(|| Error::internal("catalog object revision overflow"))?,
        object.payload().clone(),
    )
    .map_err(catalog_argument)
}

fn replace_payload(object: &CatalogObject, payload: CatalogPayload) -> Result<CatalogObject> {
    CatalogObject::new(
        object.id(),
        object.namespace_id(),
        object.parent_id(),
        object.owner_principal_id(),
        object.name().clone(),
        object
            .definition_revision()
            .checked_add(1)
            .ok_or_else(|| Error::internal("catalog object revision overflow"))?,
        payload,
    )
    .map_err(catalog_argument)
}

fn precondition(object: &CatalogObject) -> Result<ObjectPrecondition> {
    ObjectPrecondition::new(object.id(), object.kind(), object.definition_revision())
        .map_err(catalog_argument)
}

const fn privilege_bit(privilege: ObjectPrivilegeSyntax) -> u64 {
    match privilege {
        ObjectPrivilegeSyntax::Connect => PRIVILEGE_CONNECT,
        ObjectPrivilegeSyntax::Usage => PRIVILEGE_USAGE,
        ObjectPrivilegeSyntax::Create => PRIVILEGE_CREATE,
        ObjectPrivilegeSyntax::Select => PRIVILEGE_SELECT,
        ObjectPrivilegeSyntax::Insert => PRIVILEGE_INSERT,
        ObjectPrivilegeSyntax::Update => PRIVILEGE_UPDATE,
        ObjectPrivilegeSyntax::Delete => PRIVILEGE_DELETE,
        ObjectPrivilegeSyntax::Execute => PRIVILEGE_EXECUTE,
    }
}

fn permission_denied(detail: impl Into<String>) -> Error {
    Error::authorization_denied(detail)
}

trait CatalogPayloadAclExt {
    fn as_acl(&self) -> Option<&AclEntryPayload>;
}

impl CatalogPayloadAclExt for CatalogPayload {
    fn as_acl(&self) -> Option<&AclEntryPayload> {
        match self {
            Self::AclEntry(payload) => Some(payload),
            _ => None,
        }
    }
}
