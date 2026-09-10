use std::collections::BTreeSet;
use std::sync::Arc;

use radixdb_catalog::{
    AccessMethod, CatalogEdge, CatalogGeneration, CatalogMutation, CatalogName, CatalogObject,
    CatalogPayload, EdgeKind, ObjectId, ObjectKind, ObjectPrecondition, OperatorBinding,
    OperatorClassPayload, OperatorPayload, PlannerRecheckPolicy, PlannerSupportPayload,
    RoutineResult, Volatility,
};
use radixdb_core::{DataType, Error, Result};
use radixdb_plugin_host::{PluginRegistry, RegisteredBinding};
use radixdb_sql::{
    CreateOperatorClassStatement, CreateOperatorStatement, CreatePlannerSupportStatement,
    DropOperatorClassStatement, DropOperatorStatement, DropPlannerSupportStatement, IndexMethod,
    RoutineKindSyntax,
};

use super::native_function::{bind_native_type, descriptor_catalog_type, require_descriptor_type};
use super::procedural::{
    resolve_namespace, resolve_object_scope, resolve_optional_routine_signature,
};
use super::transaction::{catalog_argument, DdlDelta};

pub(super) fn bind_create_operator(
    statement: &CreateOperatorStatement,
    generation: &CatalogGeneration,
    registry: &Arc<PluginRegistry>,
) -> Result<DdlDelta> {
    let namespace_id = resolve_namespace(generation, std::slice::from_ref(&statement.name.schema))?;
    let extension = require_extension(generation, statement.extension_name.value())?;
    let CatalogPayload::Extension(extension_payload) = extension.payload() else {
        unreachable!()
    };
    let package_id = extension_payload.package_id().into_bytes();
    let descriptor = registry
        .operator_by_package_and_local_id(&package_id, statement.local_id.as_str())
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "extension '{}' does not export operator '{}'",
                statement.extension_name, statement.local_id
            ))
        })?;
    if descriptor.symbol != statement.name.symbol {
        return Err(Error::InvalidArgument(
            "operator SQL symbol differs from its admitted descriptor".to_owned(),
        ));
    }

    let mut dependencies = BTreeSet::from([extension.id()]);
    let left = statement
        .left_argument
        .as_ref()
        .map(|syntax| bind_native_type(syntax, generation))
        .transpose()?;
    let right = bind_native_type(&statement.right_argument, generation)?;
    match (left, descriptor.left) {
        (Some(actual), Some(expected)) => require_descriptor_type(
            &expected,
            actual,
            generation,
            extension.id(),
            &mut dependencies,
        )?,
        (None, None) => {}
        _ => {
            return Err(Error::InvalidArgument(
                "operator SQL arity differs from its admitted descriptor".to_owned(),
            ))
        }
    }
    require_descriptor_type(
        &descriptor.right,
        right,
        generation,
        extension.id(),
        &mut dependencies,
    )?;
    let result = descriptor_catalog_type(
        &descriptor.result,
        generation,
        extension.id(),
        &mut dependencies,
    )?;
    let function = resolve_optional_routine_signature(
        generation,
        RoutineKindSyntax::Function,
        &statement.function,
    )?
    .ok_or_else(|| {
        Error::InvalidArgument(format!(
            "operator backing function '{}' does not exist",
            statement.function
        ))
    })?;
    if function.id().into_bytes() != descriptor.function_id {
        return Err(Error::InvalidArgument(
            "operator backing function identity differs from its admitted descriptor".to_owned(),
        ));
    }
    let CatalogPayload::Function(function_payload) = function.payload() else {
        unreachable!()
    };
    let Some(function_definition) = function_payload.native_definition() else {
        return Err(Error::InvalidArgument(
            "operator backing function must be native".to_owned(),
        ));
    };
    if function_definition.extension_binding_id() != extension.id() {
        return Err(Error::InvalidArgument(
            "operator and backing function belong to different extensions".to_owned(),
        ));
    }
    if function_definition.volatility() != Volatility::Immutable
        || !matches!(
            function_definition.result(),
            RoutineResult::Scalar { data_type, .. }
                if data_type.logical_type() == DataType::Boolean
        )
    {
        return Err(Error::InvalidArgument(
            "planner support target must be an IMMUTABLE BOOLEAN native function".to_owned(),
        ));
    }
    dependencies.insert(function.id());

    if generation
        .find_operator(
            namespace_id,
            statement.name.symbol.as_str(),
            left,
            Some(right),
        )
        .map_err(catalog_argument)?
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "operator overload '{}' already exists",
            statement.name
        )));
    }
    let object_id = ObjectId::from_user_bytes(descriptor.object_id).map_err(catalog_argument)?;
    if generation.object(object_id).is_some() {
        return Err(Error::InvalidArgument(format!(
            "plugin operator '{}' is already bound under another SQL identity",
            statement.local_id
        )));
    }
    let payload = OperatorPayload::new(
        extension.id(),
        descriptor.local_id.clone(),
        descriptor.semantic_revision,
        descriptor.symbol.clone(),
        left,
        Some(right),
        result,
        function.id(),
    )
    .map_err(catalog_argument)?;
    let object = CatalogObject::new(
        object_id,
        Some(namespace_id),
        Some(namespace_id),
        extension.owner_principal_id(),
        CatalogName::new(descriptor.symbol.as_str()).map_err(catalog_argument)?,
        1,
        CatalogPayload::Operator(payload),
    )
    .map_err(catalog_argument)?;
    Ok(plugin_delta(namespace_id, object, dependencies))
}

pub(super) fn bind_drop_operator(
    statement: &DropOperatorStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let namespace_id = resolve_namespace(generation, std::slice::from_ref(&statement.name.schema))?;
    let left = statement
        .left_argument
        .as_ref()
        .map(|value| bind_native_type(value, generation))
        .transpose()?;
    let right = statement
        .right_argument
        .as_ref()
        .map(|value| bind_native_type(value, generation))
        .transpose()?;
    let Some(object) = generation
        .find_operator(namespace_id, statement.name.symbol.as_str(), left, right)
        .map_err(catalog_argument)?
    else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::InvalidArgument(format!(
                "operator '{}' does not exist",
                statement.name
            )))
        };
    };
    restrict_drop(statement.name.to_string(), object, generation)
}

pub(super) fn bind_create_operator_class(
    statement: &CreateOperatorClassStatement,
    generation: &CatalogGeneration,
    registry: &Arc<PluginRegistry>,
) -> Result<DdlDelta> {
    let (namespace_id, sql_name) = resolve_object_scope(generation, &statement.name)?;
    if generation
        .find_operator_class(namespace_id, sql_name)
        .map_err(catalog_argument)?
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "operator class '{}' already exists",
            statement.name
        )));
    }
    let extension = require_extension(generation, statement.extension_name.value())?;
    let CatalogPayload::Extension(extension_payload) = extension.payload() else {
        unreachable!()
    };
    let package_id = extension_payload.package_id().into_bytes();
    let descriptor = registry
        .operator_class_by_package_and_local_id(&package_id, statement.local_id.as_str())
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "extension '{}' does not export operator class '{}'",
                statement.extension_name, statement.local_id
            ))
        })?;
    let access_method = access_method(statement.access_method);
    if descriptor.access_method != access_method.tag() {
        return Err(Error::InvalidArgument(
            "operator-class SQL access method differs from its admitted descriptor".to_owned(),
        ));
    }
    let mut dependencies = BTreeSet::from([extension.id()]);
    let input_type = bind_native_type(&statement.input_type, generation)?;
    require_descriptor_type(
        &descriptor.input_type,
        input_type,
        generation,
        extension.id(),
        &mut dependencies,
    )?;
    let key_type = descriptor_catalog_type(
        &descriptor.key_type,
        generation,
        extension.id(),
        &mut dependencies,
    )?;
    if key_type.is_external() {
        return Err(Error::InvalidArgument(
            "operator-class physical key type must be core-owned".to_owned(),
        ));
    }
    let strategies = bind_descriptor_objects(
        &descriptor.strategies,
        ObjectKind::Operator,
        "operator-class strategy",
        generation,
        &mut dependencies,
    )?;
    validate_operator_class_strategies(
        access_method,
        input_type,
        extension.id(),
        &strategies,
        generation,
    )?;
    for support in &descriptor.supports {
        let Some(registered) = registry.planner_support(&support.object_id) else {
            return Err(Error::InvalidArgument(
                "operator-class descriptor references an unregistered planner support".to_owned(),
            ));
        };
        if registered.target_operator_class_id != Some(descriptor.object_id) {
            return Err(Error::InvalidArgument(
                "planner support does not target its declaring operator class".to_owned(),
            ));
        }
    }
    let payload = OperatorClassPayload::new(
        extension.id(),
        descriptor.local_id.clone(),
        descriptor.semantic_revision,
        access_method,
        input_type,
        key_type,
        strategies,
        vec![],
        descriptor.key_codec_revision,
        descriptor.fingerprint,
    )
    .map_err(catalog_argument)?;
    let object_id = ObjectId::from_user_bytes(descriptor.object_id).map_err(catalog_argument)?;
    if generation.object(object_id).is_some() {
        return Err(Error::InvalidArgument(format!(
            "plugin operator class '{}' is already bound under another SQL name",
            statement.local_id
        )));
    }
    let object = CatalogObject::new(
        object_id,
        Some(namespace_id),
        Some(namespace_id),
        extension.owner_principal_id(),
        CatalogName::new(sql_name).map_err(catalog_argument)?,
        1,
        CatalogPayload::OperatorClass(payload),
    )
    .map_err(catalog_argument)?;
    Ok(plugin_delta(namespace_id, object, dependencies))
}

pub(super) fn bind_create_planner_support(
    statement: &CreatePlannerSupportStatement,
    generation: &CatalogGeneration,
    registry: &Arc<PluginRegistry>,
) -> Result<DdlDelta> {
    let (namespace_id, sql_name) = resolve_object_scope(generation, &statement.name)?;
    if generation
        .find_planner_support(namespace_id, sql_name)
        .map_err(catalog_argument)?
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "planner support '{}' already exists",
            statement.name
        )));
    }
    let extension = require_extension(generation, statement.extension_name.value())?;
    let CatalogPayload::Extension(extension_payload) = extension.payload() else {
        unreachable!()
    };
    let package_id = extension_payload.package_id().into_bytes();
    let descriptor = registry
        .planner_support_by_package_and_local_id(&package_id, statement.local_id.as_str())
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "extension '{}' does not export planner support '{}'",
                statement.extension_name, statement.local_id
            ))
        })?;
    let target_function = resolve_optional_routine_signature(
        generation,
        RoutineKindSyntax::Function,
        &statement.function,
    )?
    .ok_or_else(|| {
        Error::InvalidArgument(format!(
            "planner support target function '{}' does not exist",
            statement.function
        ))
    })?;
    if descriptor.target_function_id != Some(target_function.id().into_bytes()) {
        return Err(Error::InvalidArgument(
            "planner support target function differs from its admitted descriptor".to_owned(),
        ));
    }
    let CatalogPayload::Function(function_payload) = target_function.payload() else {
        unreachable!()
    };
    let Some(function_definition) = function_payload.native_definition() else {
        return Err(Error::InvalidArgument(
            "planner support target must be a native plugin function".to_owned(),
        ));
    };
    if function_definition.extension_binding_id() != extension.id() {
        return Err(Error::InvalidArgument(
            "planner support target function belongs to another extension".to_owned(),
        ));
    }

    let mut dependencies = BTreeSet::from([extension.id(), target_function.id()]);
    let target_operator_class_id = descriptor
        .target_operator_class_id
        .map(ObjectId::from_user_bytes)
        .transpose()
        .map_err(catalog_argument)?;
    if let Some(id) = target_operator_class_id {
        let target = generation.object(id).ok_or_else(|| {
            Error::InvalidArgument("planner support target operator class is not bound".to_owned())
        })?;
        let CatalogPayload::OperatorClass(payload) = target.payload() else {
            return Err(Error::InvalidArgument(
                "planner support target operator class has the wrong catalog kind".to_owned(),
            ));
        };
        if payload.extension_binding_id() != extension.id() {
            return Err(Error::InvalidArgument(
                "planner support target operator class belongs to another extension".to_owned(),
            ));
        }
        dependencies.insert(id);
    }
    let recheck_policy =
        PlannerRecheckPolicy::try_from(descriptor.recheck_policy).map_err(catalog_argument)?;
    let payload = PlannerSupportPayload::new(
        extension.id(),
        descriptor.local_id.clone(),
        descriptor.semantic_revision,
        Some(target_function.id()),
        target_operator_class_id,
        descriptor.max_spans,
        descriptor.max_output_bytes,
        recheck_policy,
        descriptor.fingerprint,
    )
    .map_err(catalog_argument)?;
    let object_id = ObjectId::from_user_bytes(descriptor.object_id).map_err(catalog_argument)?;
    if generation.object(object_id).is_some() {
        return Err(Error::InvalidArgument(format!(
            "plugin planner support '{}' is already bound under another SQL name",
            statement.local_id
        )));
    }
    let object = CatalogObject::new(
        object_id,
        Some(namespace_id),
        Some(namespace_id),
        extension.owner_principal_id(),
        CatalogName::new(sql_name).map_err(catalog_argument)?,
        1,
        CatalogPayload::PlannerSupport(payload),
    )
    .map_err(catalog_argument)?;
    Ok(plugin_delta(namespace_id, object, dependencies))
}

pub(super) fn bind_drop_planner_support(
    statement: &DropPlannerSupportStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let (namespace_id, name) = resolve_object_scope(generation, &statement.name)?;
    let Some(object) = generation
        .find_planner_support(namespace_id, name)
        .map_err(catalog_argument)?
    else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::InvalidArgument(format!(
                "planner support '{}' does not exist",
                statement.name
            )))
        };
    };
    restrict_drop(statement.name.to_string(), object, generation)
}

pub(super) fn bind_drop_operator_class(
    statement: &DropOperatorClassStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let (namespace_id, name) = resolve_object_scope(generation, &statement.name)?;
    let Some(object) = generation
        .find_operator_class(namespace_id, name)
        .map_err(catalog_argument)?
    else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::InvalidArgument(format!(
                "operator class '{}' does not exist",
                statement.name
            )))
        };
    };
    let CatalogPayload::OperatorClass(payload) = object.payload() else {
        unreachable!()
    };
    if payload.access_method() != access_method(statement.access_method) {
        return Err(Error::InvalidArgument(format!(
            "operator class '{}' does not use {}",
            statement.name, statement.access_method
        )));
    }
    restrict_drop(statement.name.to_string(), object, generation)
}

fn require_extension<'a>(
    generation: &'a CatalogGeneration,
    name: &str,
) -> Result<&'a CatalogObject> {
    generation
        .find_extension(name)
        .map_err(catalog_argument)?
        .ok_or_else(|| Error::InvalidArgument(format!("extension '{name}' does not exist")))
}

fn access_method(value: IndexMethod) -> AccessMethod {
    match value {
        IndexMethod::BTree => AccessMethod::Btree,
        IndexMethod::Hash => AccessMethod::Hash,
        IndexMethod::Bitmap => AccessMethod::Bitmap,
        IndexMethod::Hnsw => AccessMethod::Hnsw,
    }
}

fn bind_descriptor_objects(
    bindings: &[RegisteredBinding],
    kind: ObjectKind,
    role: &'static str,
    generation: &CatalogGeneration,
    dependencies: &mut BTreeSet<ObjectId>,
) -> Result<Vec<OperatorBinding>> {
    bindings
        .iter()
        .map(|binding| {
            let id = ObjectId::from_user_bytes(binding.object_id).map_err(catalog_argument)?;
            let object = generation.object(id).ok_or_else(|| {
                Error::InvalidArgument(format!("{role} {id} is not bound in this database"))
            })?;
            if object.kind() != kind {
                return Err(Error::InvalidArgument(format!(
                    "{role} {id} has the wrong kind"
                )));
            }
            dependencies.insert(id);
            Ok(OperatorBinding::new(binding.slot, id))
        })
        .collect()
}

fn validate_operator_class_strategies(
    access_method: AccessMethod,
    input_type: radixdb_catalog::CatalogDataType,
    extension_id: ObjectId,
    strategies: &[OperatorBinding],
    generation: &CatalogGeneration,
) -> Result<()> {
    let expected: &[(u16, &str)] = match access_method {
        AccessMethod::Btree => &[(1, "<"), (2, "<="), (3, "="), (4, ">="), (5, ">")],
        AccessMethod::Hash | AccessMethod::Bitmap => &[(1, "=")],
        AccessMethod::Hnsw => {
            return Err(Error::NotSupported(
                "external HNSW operator classes require PLUG-90 planner support".to_owned(),
            ));
        }
    };
    if strategies.len() != expected.len() {
        return Err(Error::InvalidArgument(format!(
            "{access_method:?} operator class requires exactly {} strategy bindings",
            expected.len()
        )));
    }
    let boolean = radixdb_catalog::CatalogDataType::scalar(radixdb_core::DataType::Boolean)
        .map_err(catalog_argument)?;
    for ((expected_slot, expected_symbol), binding) in expected.iter().zip(strategies) {
        let operator = generation
            .object(binding.object_id())
            .ok_or_else(|| Error::internal("bound operator-class strategy disappeared"))?;
        let CatalogPayload::Operator(payload) = operator.payload() else {
            return Err(Error::internal(
                "bound operator-class strategy has wrong catalog kind",
            ));
        };
        if binding.slot() != *expected_slot
            || payload.symbol() != *expected_symbol
            || payload.left_argument() != Some(input_type)
            || payload.right_argument() != Some(input_type)
            || payload.result_type() != boolean
            || payload.extension_binding_id() != extension_id
        {
            return Err(Error::InvalidArgument(format!(
                "operator-class strategy slot {expected_slot} must bind '{expected_symbol}' over its input type with BOOLEAN result from the same extension"
            )));
        }
    }
    Ok(())
}

fn plugin_delta(
    namespace_id: ObjectId,
    object: CatalogObject,
    dependencies: BTreeSet<ObjectId>,
) -> DdlDelta {
    let object_id = object.id();
    let extension_id = match object.payload() {
        CatalogPayload::Operator(value) => value.extension_binding_id(),
        CatalogPayload::OperatorClass(value) => value.extension_binding_id(),
        CatalogPayload::PlannerSupport(value) => value.extension_binding_id(),
        _ => unreachable!(),
    };
    let mut edges = vec![CatalogEdge::new(
        namespace_id,
        object_id,
        EdgeKind::Contains,
        0,
    )];
    for (ordinal, target) in dependencies.into_iter().enumerate() {
        edges.push(CatalogEdge::new(
            object_id,
            target,
            if target == extension_id {
                EdgeKind::DependsOn
            } else {
                EdgeKind::References
            },
            ordinal as u32,
        ));
    }
    DdlDelta {
        mutations: vec![CatalogMutation::create(object)],
        edge_additions: edges,
        ..DdlDelta::default()
    }
}

fn restrict_drop(
    display: String,
    object: &CatalogObject,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    if let Some(dependent) = generation.graph().dependents(object.id()).next() {
        return Err(Error::InvalidArgument(format!(
            "cannot drop '{display}' with RESTRICT: catalog object '{}' ({}) depends on it",
            dependent.name().display().as_str(),
            dependent.kind().name()
        )));
    }
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::drop(
            ObjectPrecondition::new(object.id(), object.kind(), object.definition_revision())
                .map_err(catalog_argument)?,
        )],
        ..DdlDelta::default()
    })
}
