use std::sync::Arc;

use radixdb_catalog::{
    CatalogEdge, CatalogGeneration, CatalogMutation, CatalogName, CatalogObject, CatalogPayload,
    EdgeKind, ExternalStorageKind, ExternalTypePayload, ObjectId, ObjectKind, ObjectPrecondition,
};
use radixdb_core::{Error, Result};
use radixdb_plugin_host::{PluginRegistry, EXTERNAL_STORAGE_FIXED, EXTERNAL_STORAGE_VARIABLE};
use radixdb_sql::{CreateExternalTypeStatement, DropExternalTypeStatement};

use super::procedural::resolve_object_scope;
use super::transaction::{catalog_argument, DdlDelta};

pub(super) fn bind_create_external_type(
    statement: &CreateExternalTypeStatement,
    generation: &CatalogGeneration,
    registry: &Arc<PluginRegistry>,
) -> Result<DdlDelta> {
    let (namespace_id, sql_name) = resolve_object_scope(generation, &statement.name)?;
    if generation
        .find_external_type(namespace_id, sql_name)
        .map_err(catalog_argument)?
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "type '{}' already exists",
            statement.name
        )));
    }
    let extension = generation
        .find_extension(statement.extension_name.value())
        .map_err(catalog_argument)?
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "extension '{}' does not exist",
                statement.extension_name
            ))
        })?;
    let CatalogPayload::Extension(extension_payload) = extension.payload() else {
        return Err(Error::internal(
            "extension name resolved to another object kind",
        ));
    };
    let package_id = extension_payload.package_id().into_bytes();
    let descriptor = registry
        .external_type_by_package_and_local_id(&package_id, statement.local_id.as_str())
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "extension '{}' does not export external type '{}'",
                statement.extension_name, statement.local_id
            ))
        })?;
    let object_id = ObjectId::from_user_bytes(descriptor.object_id).map_err(catalog_argument)?;
    if generation.object(object_id).is_some() {
        return Err(Error::InvalidArgument(format!(
            "plugin external type '{}' is already bound under another SQL name",
            statement.local_id
        )));
    }
    let storage_kind = match descriptor.storage_kind {
        EXTERNAL_STORAGE_FIXED => ExternalStorageKind::Fixed,
        EXTERNAL_STORAGE_VARIABLE => ExternalStorageKind::Variable,
        _ => return Err(Error::internal("admitted plugin has invalid storage kind")),
    };
    let fixed_bytes =
        (storage_kind == ExternalStorageKind::Fixed).then_some(descriptor.fixed_bytes);
    let payload = ExternalTypePayload::new(
        extension.id(),
        descriptor.local_id.clone(),
        descriptor.codec_version,
        descriptor.semantic_revision,
        storage_kind,
        fixed_bytes,
        descriptor.max_bytes,
        descriptor.codec_fingerprint,
        descriptor.capabilities,
    )
    .map_err(catalog_argument)?;
    let object = CatalogObject::new(
        object_id,
        Some(namespace_id),
        Some(namespace_id),
        extension.owner_principal_id(),
        CatalogName::new(sql_name).map_err(catalog_argument)?,
        1,
        CatalogPayload::ExternalType(payload),
    )
    .map_err(catalog_argument)?;
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::create(object)],
        edge_additions: vec![
            CatalogEdge::new(namespace_id, object_id, EdgeKind::Contains, 0),
            CatalogEdge::new(object_id, extension.id(), EdgeKind::DependsOn, 0),
        ],
        ..DdlDelta::default()
    })
}

pub(super) fn bind_drop_external_type(
    statement: &DropExternalTypeStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let (namespace_id, name) = resolve_object_scope(generation, &statement.name)?;
    let Some(external_type) = generation
        .find_external_type(namespace_id, name)
        .map_err(catalog_argument)?
    else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::InvalidArgument(format!(
                "type '{}' does not exist",
                statement.name
            )))
        };
    };
    if let Some(dependent) = generation.graph().dependents(external_type.id()).next() {
        return Err(Error::InvalidArgument(format!(
            "cannot drop type '{}' with RESTRICT: catalog object '{}' ({}, {}) depends on it",
            statement.name,
            dependent.name().display().as_str(),
            dependent.kind().name(),
            dependent.id()
        )));
    }
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::drop(
            ObjectPrecondition::new(
                external_type.id(),
                ObjectKind::ExternalType,
                external_type.definition_revision(),
            )
            .map_err(catalog_argument)?,
        )],
        ..DdlDelta::default()
    })
}
