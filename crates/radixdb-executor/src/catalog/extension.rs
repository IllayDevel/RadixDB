use std::sync::Arc;

use radixdb_catalog::{
    CatalogGeneration, CatalogMutation, CatalogName, CatalogObject, CatalogPayload,
    ExtensionPayload, ObjectId, ObjectKind, ObjectPrecondition,
};
use radixdb_core::{Error, Result};
use radixdb_plugin_host::PluginRegistry;
use radixdb_sql::{CreateExtensionStatement, DropExtensionStatement};

use super::transaction::{catalog_argument, DdlDelta};

pub(super) fn bind_create_extension(
    statement: &CreateExtensionStatement,
    generation: &CatalogGeneration,
    registry: &Arc<PluginRegistry>,
) -> Result<DdlDelta> {
    let name = CatalogName::new(statement.name.value()).map_err(catalog_argument)?;
    let normalized = name.normalized().as_str();
    let package = registry
        .package_by_name_and_version(normalized, statement.version.as_str())
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "installed plugin package '{normalized}' at exact version '{}' is not active",
                statement.version
            ))
        })?;
    let object_id = ObjectId::from_user_bytes(package.package_id).map_err(catalog_argument)?;
    let payload = ExtensionPayload::new(
        object_id,
        package.version.to_string(),
        package.abi_major,
        package.abi_min_minor,
        package.abi_max_minor,
        package.descriptor_fingerprint,
    )
    .map_err(catalog_argument)?;
    let desired_payload = CatalogPayload::Extension(payload);

    if let Some(existing) = generation
        .find_extension(name.display().as_str())
        .map_err(catalog_argument)?
    {
        if statement.if_not_exists
            && existing.id() == object_id
            && existing.payload() == &desired_payload
        {
            return Ok(DdlDelta::default());
        }
        return Err(Error::InvalidArgument(format!(
            "extension '{}' already exists with a different binding",
            name.display().as_str()
        )));
    }
    if let Some(existing) = generation.object(object_id) {
        return Err(Error::InvalidArgument(format!(
            "plugin package identity {object_id} is already bound as catalog object '{}' ({})",
            existing.name().display().as_str(),
            existing.kind().name()
        )));
    }

    let object = CatalogObject::new(
        object_id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        name,
        1,
        desired_payload,
    )
    .map_err(catalog_argument)?;
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::create(object)],
        ..DdlDelta::default()
    })
}

pub(super) fn bind_drop_extension(
    statement: &DropExtensionStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let name = statement.name.value();
    let Some(extension) = generation.find_extension(name).map_err(catalog_argument)? else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::InvalidArgument(format!(
                "extension '{name}' does not exist"
            )))
        };
    };
    if let Some(dependent) = generation.graph().dependents(extension.id()).next() {
        return Err(Error::InvalidArgument(format!(
            "cannot drop extension '{name}' with RESTRICT: catalog object '{}' ({}, {}) depends on it",
            dependent.name().display().as_str(),
            dependent.kind().name(),
            dependent.id()
        )));
    }
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::drop(
            ObjectPrecondition::new(
                extension.id(),
                ObjectKind::Extension,
                extension.definition_revision(),
            )
            .map_err(catalog_argument)?,
        )],
        ..DdlDelta::default()
    })
}
