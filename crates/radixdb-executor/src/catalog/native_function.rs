use std::collections::BTreeSet;
use std::sync::Arc;

use radixdb_catalog::{
    ArgumentMode, CatalogDataType, CatalogEdge, CatalogGeneration, CatalogMutation, CatalogName,
    CatalogObject, CatalogPayload, EdgeKind, FunctionPayload, NativeFunctionDefinition, ObjectId,
    ObjectKind, RoutineArgument, RoutineResult, Volatility,
};
use radixdb_core::{DataType, Error, Result};
use radixdb_plugin_host::{PluginRegistry, RegisteredTypeRef};
use radixdb_sql::{
    CreateRoutineStatement, ProceduralType, RoutineArgumentMode, RoutineReturnSyntax,
};

use super::procedural::resolve_object_scope;
use super::table::bind_catalog_type_in_generation;
use super::transaction::{catalog_argument, DdlDelta};

pub(super) fn bind_create_native_function(
    statement: &CreateRoutineStatement,
    generation: &CatalogGeneration,
    registry: &Arc<PluginRegistry>,
) -> Result<DdlDelta> {
    let native = statement
        .native
        .as_ref()
        .ok_or_else(|| Error::internal("native function binder received a procedural function"))?;
    if statement.or_replace {
        return Err(Error::NotSupported(
            "CREATE OR REPLACE is not supported for native functions".to_owned(),
        ));
    }
    let [extension_name] = native.extension.components.as_slice() else {
        return Err(Error::InvalidArgument(
            "extension binding name must be unqualified".to_owned(),
        ));
    };
    let extension = generation
        .find_extension(extension_name.value.as_str())
        .map_err(catalog_argument)?
        .ok_or_else(|| {
            Error::InvalidArgument(format!("extension '{}' does not exist", native.extension))
        })?;
    let CatalogPayload::Extension(extension_payload) = extension.payload() else {
        return Err(Error::internal(
            "extension name resolved to another catalog object kind",
        ));
    };
    let package_id = extension_payload.package_id().into_bytes();
    let descriptor = registry
        .function_by_package_and_local_id(&package_id, native.local_id.as_str())
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "extension '{}' does not export native function '{}'",
                native.extension, native.local_id
            ))
        })?;

    let object_id = ObjectId::from_user_bytes(descriptor.object_id).map_err(catalog_argument)?;
    if generation.object(object_id).is_some() {
        return Err(Error::InvalidArgument(format!(
            "plugin native function '{}' is already bound under another SQL name",
            native.local_id
        )));
    }

    if statement.arguments.len() != descriptor.arguments.len() {
        return Err(Error::InvalidArgument(format!(
            "native function '{}' declares {} arguments but descriptor '{}' requires {}",
            statement.name,
            statement.arguments.len(),
            native.local_id,
            descriptor.arguments.len()
        )));
    }
    let mut dependencies = BTreeSet::from([extension.id()]);
    let arguments = statement
        .arguments
        .iter()
        .zip(&descriptor.arguments)
        .map(|(argument, expected)| {
            if argument.mode != RoutineArgumentMode::In || argument.default.is_some() {
                return Err(Error::InvalidArgument(
                    "native functions require IN-only arguments without DEFAULT".to_owned(),
                ));
            }
            if descriptor.strict && argument.nullable {
                return Err(Error::InvalidArgument(format!(
                    "STRICT native function argument '{}' must be NOT NULL",
                    argument.name
                )));
            }
            let actual = bind_native_type(&argument.data_type, generation)?;
            require_descriptor_type(
                expected,
                actual,
                generation,
                extension.id(),
                &mut dependencies,
            )?;
            RoutineArgument::new(
                CatalogName::new(argument.name.value.as_str()).map_err(catalog_argument)?,
                ArgumentMode::In,
                actual,
                argument.nullable,
                None,
            )
            .map_err(catalog_argument)
        })
        .collect::<Result<Vec<_>>>()?;

    let Some(RoutineReturnSyntax::Scalar {
        data_type,
        nullable,
    }) = &statement.returns
    else {
        return Err(Error::InvalidArgument(
            "native function requires one scalar RETURNS contract".to_owned(),
        ));
    };
    let result_type = bind_native_type(data_type, generation)?;
    require_descriptor_type(
        &descriptor.result,
        result_type,
        generation,
        extension.id(),
        &mut dependencies,
    )?;
    let result = RoutineResult::Scalar {
        data_type: result_type,
        nullable: *nullable,
    };
    let volatility = Volatility::try_from(descriptor.volatility).map_err(catalog_argument)?;
    let definition = NativeFunctionDefinition::new(
        arguments,
        result,
        volatility,
        descriptor.semantic_revision,
        dependencies.iter().copied().collect(),
        extension.id(),
        descriptor.local_id.clone(),
        descriptor.strict,
        descriptor.parallel_safe,
        descriptor.cost,
        descriptor.cancellation,
        descriptor.batch.is_some(),
        descriptor.max_output_bytes,
    )
    .map_err(catalog_argument)?;
    let (namespace_id, sql_name) = resolve_object_scope(generation, &statement.name)?;
    let input_types = definition
        .arguments()
        .iter()
        .map(RoutineArgument::data_type)
        .collect::<Vec<_>>();
    if generation
        .find_routine(namespace_id, ObjectKind::Function, sql_name, &input_types)
        .map_err(catalog_argument)?
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "Function overload '{}' already exists",
            statement.name
        )));
    }
    let object = CatalogObject::new(
        object_id,
        Some(namespace_id),
        Some(namespace_id),
        extension.owner_principal_id(),
        CatalogName::new(sql_name).map_err(catalog_argument)?,
        1,
        CatalogPayload::Function(FunctionPayload::new_native(definition)),
    )
    .map_err(catalog_argument)?;
    let mut edge_additions = vec![CatalogEdge::new(
        namespace_id,
        object_id,
        EdgeKind::Contains,
        0,
    )];
    for (ordinal, dependency) in dependencies.into_iter().enumerate() {
        edge_additions.push(CatalogEdge::new(
            object_id,
            dependency,
            if dependency == extension.id() {
                EdgeKind::DependsOn
            } else {
                EdgeKind::References
            },
            u32::try_from(ordinal)
                .map_err(|_| Error::invalid_argument("too many native function dependencies"))?,
        ));
    }
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::create(object)],
        edge_additions,
        ..DdlDelta::default()
    })
}

pub(super) fn bind_native_type(
    syntax: &ProceduralType,
    generation: &CatalogGeneration,
) -> Result<CatalogDataType> {
    match syntax {
        ProceduralType::Scalar(name) => bind_catalog_type_in_generation(name.as_str(), generation),
        ProceduralType::RowType(_) => Err(Error::NotSupported(
            "%ROWTYPE is not valid in a native function signature".to_owned(),
        )),
    }
}

pub(super) fn require_descriptor_type(
    expected: &RegisteredTypeRef,
    actual: CatalogDataType,
    generation: &CatalogGeneration,
    extension_id: ObjectId,
    dependencies: &mut BTreeSet<ObjectId>,
) -> Result<()> {
    let expected = match expected {
        RegisteredTypeRef::Builtin(tag) => {
            let tag = u8::try_from(*tag)
                .ok()
                .and_then(DataType::from_u8)
                .filter(|value| *value != DataType::Null)
                .ok_or_else(|| Error::internal("admitted plugin has an unknown built-in type"))?;
            CatalogDataType::scalar(tag).map_err(catalog_argument)?
        }
        RegisteredTypeRef::External {
            object_id,
            codec_version,
        } => {
            let type_id = ObjectId::from_user_bytes(*object_id).map_err(catalog_argument)?;
            let object = generation.object(type_id).ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "native function external type {type_id} is not bound in this database"
                ))
            })?;
            let CatalogPayload::ExternalType(payload) = object.payload() else {
                return Err(Error::InvalidArgument(format!(
                    "native function dependency {type_id} is not an external type"
                )));
            };
            if payload.extension_binding_id() != extension_id
                || payload.write_codec_version() != *codec_version
            {
                return Err(Error::InvalidArgument(format!(
                    "native function external type {type_id} belongs to another extension or codec"
                )));
            }
            dependencies.insert(type_id);
            CatalogDataType::external(type_id, *codec_version).map_err(catalog_argument)?
        }
    };
    if actual != expected {
        return Err(Error::InvalidArgument(
            "native function SQL signature differs from its admitted package descriptor".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn descriptor_catalog_type(
    expected: &RegisteredTypeRef,
    generation: &CatalogGeneration,
    extension_id: ObjectId,
    dependencies: &mut BTreeSet<ObjectId>,
) -> Result<CatalogDataType> {
    let actual = match expected {
        RegisteredTypeRef::Builtin(tag) => {
            let data_type = u8::try_from(*tag)
                .ok()
                .and_then(DataType::from_u8)
                .filter(|value| *value != DataType::Null)
                .ok_or_else(|| Error::internal("admitted plugin has an unknown built-in type"))?;
            CatalogDataType::scalar(data_type).map_err(catalog_argument)?
        }
        RegisteredTypeRef::External {
            object_id,
            codec_version,
        } => {
            let type_id = ObjectId::from_user_bytes(*object_id).map_err(catalog_argument)?;
            let object = generation.object(type_id).ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "plugin external type {type_id} is not bound in this database"
                ))
            })?;
            let CatalogPayload::ExternalType(payload) = object.payload() else {
                return Err(Error::InvalidArgument(format!(
                    "plugin dependency {type_id} is not an external type"
                )));
            };
            if payload.extension_binding_id() != extension_id
                || payload.write_codec_version() != *codec_version
            {
                return Err(Error::InvalidArgument(format!(
                    "plugin external type {type_id} belongs to another extension or codec"
                )));
            }
            dependencies.insert(type_id);
            CatalogDataType::external(type_id, *codec_version).map_err(catalog_argument)?
        }
    };
    Ok(actual)
}
