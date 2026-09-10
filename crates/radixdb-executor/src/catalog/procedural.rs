use std::collections::BTreeSet;

use radixdb_catalog::{
    ArgumentMode, CatalogDataType, CatalogEdge, CatalogGeneration, CatalogMutation, CatalogName,
    CatalogObject, CatalogPayload, EdgeKind, FunctionPayload, JobArgument, JobPayload, JobSchedule,
    ObjectId, ObjectKind, ObjectPrecondition, ProceduralSource, ProcedurePayload, ResourcePolicy,
    ResultColumn, RoutineArgument, RoutineDefinition, RoutineResult, SecurityMode, TriggerLevel,
    TriggerPayload, TriggerTiming, Volatility, TRIGGER_EVENT_DELETE, TRIGGER_EVENT_INSERT,
    TRIGGER_EVENT_UPDATE,
};
use radixdb_core::{Error, Result};
use radixdb_procedural::CompileIdentity;
use radixdb_sql::{
    AlterJobStatement, CreateRoutineStatement, CreateTriggerStatement, DropBehaviorSyntax,
    DropJobStatement, DropRoutineStatement, DropTriggerStatement, ObjectName, ProceduralType,
    RoutineArgumentMode, RoutineKindSyntax, RoutineReturnSyntax, RoutineSecuritySyntax,
    RoutineSignatureSyntax, RoutineVolatilitySyntax, TriggerEventSyntax, TriggerLevelSyntax,
    TriggerTimingSyntax,
};

use super::table::bind_catalog_type_in_generation;
use super::transaction::{catalog_argument, DdlDelta};

#[doc(hidden)]
#[derive(Debug)]
pub struct BoundJobDefinition {
    pub procedure_id: ObjectId,
    pub principal_id: ObjectId,
    pub schedule: JobSchedule,
    pub arguments: Vec<JobArgument>,
    pub resource_policy: ResourcePolicy,
}

pub(crate) const PROCEDURAL_COMPILER_ABI: u32 = 1;
pub(crate) const PROCEDURAL_RUNTIME_ABI: u32 = 1;

pub(crate) fn routine_compile_identity(
    statement: &CreateRoutineStatement,
    generation: &CatalogGeneration,
    new_object_id: ObjectId,
) -> Result<CompileIdentity> {
    let (namespace_id, name) = resolve_object_scope(generation, &statement.name)?;
    let kind = routine_kind(statement.kind);
    let input_types = input_types(statement, generation)?;
    let existing = generation
        .find_routine(namespace_id, kind, name, &input_types)
        .map_err(catalog_argument)?;
    if existing.is_some() && !statement.or_replace {
        return Err(Error::InvalidArgument(format!(
            "{kind:?} overload '{}' already exists",
            statement.name
        )));
    }
    let (object_id, revision) = if let Some(object) = existing {
        (
            object.id(),
            object
                .definition_revision()
                .checked_add(1)
                .ok_or_else(|| Error::internal("routine object revision overflow"))?,
        )
    } else {
        (new_object_id, 1)
    };
    Ok(CompileIdentity {
        object_id,
        definition_revision: revision,
        display_name: statement.name.to_string(),
    })
}

pub(super) fn bind_create_routine(
    statement: &CreateRoutineStatement,
    generation: &CatalogGeneration,
    object_id: ObjectId,
    dependencies: Vec<ObjectId>,
) -> Result<DdlDelta> {
    let (namespace_id, name) = resolve_object_scope(generation, &statement.name)?;
    let kind = routine_kind(statement.kind);
    let arguments = bind_arguments(statement, generation)?;
    let input_types = arguments
        .iter()
        .filter(|argument| argument.mode() != ArgumentMode::Out)
        .map(RoutineArgument::data_type)
        .collect::<Vec<_>>();
    let existing = generation
        .find_routine(namespace_id, kind, name, &input_types)
        .map_err(catalog_argument)?;
    if existing.is_some() && !statement.or_replace {
        return Err(Error::InvalidArgument(format!(
            "{kind:?} overload '{}' already exists",
            statement.name
        )));
    }
    if existing.is_none() && generation.object(object_id).is_some() {
        return Err(Error::InvalidArgument(format!(
            "catalog object ID {object_id} is not fresh"
        )));
    }
    if existing.is_some_and(|object| object.id() != object_id) {
        return Err(Error::internal(
            "compiled routine identity differs from catalog replacement target",
        ));
    }

    let result = bind_result(statement, generation)?;
    if let Some(existing) = existing {
        validate_replace_contract(existing, &arguments, &result)?;
    }
    let search_path = bind_search_path(statement, generation)?;
    let search_path_set = search_path.iter().copied().collect::<BTreeSet<_>>();
    let dependencies = dependencies
        .into_iter()
        .filter(|id| !search_path_set.contains(id))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let definition_version =
        existing
            .map(routine_definition)
            .transpose()?
            .map_or(Ok(1), |definition| {
                definition
                    .definition_version()
                    .checked_add(1)
                    .ok_or_else(|| Error::internal("routine definition version overflow"))
            })?;
    let definition = RoutineDefinition::new(
        ProceduralSource::new(statement.normalized_source.clone()).map_err(catalog_argument)?,
        arguments,
        result,
        bind_volatility(statement),
        bind_security(statement.security),
        search_path.clone(),
        dependencies.clone(),
        definition_version,
        PROCEDURAL_COMPILER_ABI,
        PROCEDURAL_RUNTIME_ABI,
        bind_resource_policy(statement)?,
    )
    .map_err(catalog_argument)?;
    let payload = match statement.kind {
        RoutineKindSyntax::Function => {
            CatalogPayload::Function(FunctionPayload::new(definition).map_err(catalog_argument)?)
        }
        RoutineKindSyntax::Procedure => {
            CatalogPayload::Procedure(ProcedurePayload::new(definition).map_err(catalog_argument)?)
        }
    };
    let owner = existing.map_or(ObjectId::BOOTSTRAP_OWNER, CatalogObject::owner_principal_id);
    let revision = existing.map_or(Ok(1), |object| {
        object
            .definition_revision()
            .checked_add(1)
            .ok_or_else(|| Error::internal("routine object revision overflow"))
    })?;
    let object = CatalogObject::new(
        object_id,
        Some(namespace_id),
        Some(namespace_id),
        owner,
        CatalogName::new(name).map_err(catalog_argument)?,
        revision,
        payload,
    )
    .map_err(catalog_argument)?;

    let mutation = if let Some(existing) = existing {
        CatalogMutation::alter(precondition(existing)?, object)
    } else {
        CatalogMutation::create(object)
    };
    let mut edge_removals = Vec::new();
    if let Some(existing) = existing {
        edge_removals.extend(
            generation
                .graph()
                .edges()
                .iter()
                .filter(|edge| {
                    edge.source_object_id() == existing.id()
                        && matches!(edge.kind(), EdgeKind::DependsOn | EdgeKind::References)
                })
                .copied(),
        );
    }
    let mut edge_additions = Vec::new();
    if existing.is_none() {
        edge_additions.push(CatalogEdge::new(
            namespace_id,
            object_id,
            EdgeKind::Contains,
            0,
        ));
    }
    for (ordinal, dependency) in search_path.into_iter().chain(dependencies).enumerate() {
        let target = generation.object(dependency).ok_or_else(|| {
            Error::InvalidArgument(format!(
                "routine dependency {dependency} does not exist in the pinned catalog"
            ))
        })?;
        let edge_kind = if target.kind().is_executable() {
            EdgeKind::DependsOn
        } else {
            EdgeKind::References
        };
        edge_additions.push(CatalogEdge::new(
            object_id,
            dependency,
            edge_kind,
            u32::try_from(ordinal)
                .map_err(|_| Error::InvalidArgument("too many routine dependencies".to_string()))?,
        ));
    }
    Ok(DdlDelta {
        mutations: vec![mutation],
        edge_removals,
        edge_additions,
    })
}

pub(super) fn bind_create_trigger(
    statement: &CreateTriggerStatement,
    actor: ObjectId,
    generation: &CatalogGeneration,
    ids: &mut super::transaction::ObjectIdSource,
) -> Result<DdlDelta> {
    let (table_namespace_id, table_name) = resolve_object_scope(generation, &statement.table)?;
    let table = generation
        .find_relation(table_namespace_id, table_name)
        .map_err(catalog_argument)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(statement.table.to_string()))?;

    let (trigger_name_component, trigger_namespace) = statement
        .name
        .components
        .split_last()
        .ok_or_else(|| Error::InvalidArgument("trigger name is empty".to_string()))?;
    let trigger_namespace_id = if trigger_namespace.is_empty() {
        table_namespace_id
    } else {
        resolve_namespace(generation, trigger_namespace)?
    };
    let trigger_name = trigger_name_component.value.as_str();
    if trigger_namespace_id != table_namespace_id {
        return Err(Error::InvalidArgument(format!(
            "trigger '{}' must use the namespace of table '{}'",
            statement.name, statement.table
        )));
    }
    let normalized_trigger_name = CatalogName::new(trigger_name).map_err(catalog_argument)?;
    let existing = generation.graph().children(table.id()).find(|object| {
        object.kind() == ObjectKind::Trigger
            && object.name().normalized() == normalized_trigger_name.normalized()
    });
    if existing.is_some() && !statement.or_replace {
        return Err(Error::InvalidArgument(format!(
            "trigger '{}' already exists on table '{}'",
            statement.name, statement.table
        )));
    }

    let function = resolve_trigger_function(generation, statement)?;
    let CatalogPayload::Function(function_payload) = function.payload() else {
        unreachable!("catalog routine lookup returned a non-function")
    };
    let Some(function_definition) = function_payload.procedural_definition() else {
        return Err(Error::InvalidArgument(
            "native functions cannot be trigger targets".to_owned(),
        ));
    };
    if function_definition.volatility() != Volatility::Volatile
        || function_definition.result() != &RoutineResult::Trigger
    {
        return Err(Error::InvalidArgument(format!(
            "trigger target '{}' must be VOLATILE and RETURNS TRIGGER",
            statement.function
        )));
    }

    let mut events = 0;
    let mut update_column_ids = Vec::new();
    for event in &statement.events {
        match event {
            TriggerEventSyntax::Insert => events |= TRIGGER_EVENT_INSERT,
            TriggerEventSyntax::Delete => events |= TRIGGER_EVENT_DELETE,
            TriggerEventSyntax::Update { columns } => {
                events |= TRIGGER_EVENT_UPDATE;
                for column in columns {
                    let object = generation
                        .find_column(table.id(), column.value.as_str())
                        .map_err(catalog_argument)?
                        .ok_or_else(|| Error::ColumnNotFound(column.value.to_string()))?;
                    update_column_ids.push(object.id());
                }
            }
        }
    }

    let object_id = existing.map_or_else(|| ids.next(generation), |object| Ok(object.id()))?;
    let definition_revision = existing.map_or(Ok(1), |object| {
        object
            .definition_revision()
            .checked_add(1)
            .ok_or_else(|| Error::internal("trigger definition revision overflow"))
    })?;
    let payload = TriggerPayload::new(
        table.id(),
        function.id(),
        match statement.timing {
            TriggerTimingSyntax::Before => TriggerTiming::Before,
            TriggerTimingSyntax::After => TriggerTiming::After,
        },
        events,
        match statement.level {
            TriggerLevelSyntax::Row => TriggerLevel::Row,
            TriggerLevelSyntax::Statement => TriggerLevel::Statement,
        },
        update_column_ids.clone(),
        statement.priority,
        statement.when.as_ref().map(ToString::to_string),
    )
    .map_err(catalog_argument)?;
    let owner = existing.map_or(actor, CatalogObject::owner_principal_id);
    let object = CatalogObject::new(
        object_id,
        Some(table_namespace_id),
        Some(table.id()),
        owner,
        normalized_trigger_name,
        definition_revision,
        CatalogPayload::Trigger(payload),
    )
    .map_err(catalog_argument)?;

    let mutation = if let Some(existing) = existing {
        CatalogMutation::alter(precondition(existing)?, object)
    } else {
        CatalogMutation::create(object)
    };
    let mut edge_removals = Vec::new();
    if let Some(existing) = existing {
        edge_removals.extend(
            generation
                .graph()
                .edges()
                .iter()
                .filter(|edge| {
                    edge.source_object_id() == existing.id()
                        && matches!(edge.kind(), EdgeKind::DependsOn | EdgeKind::References)
                })
                .copied(),
        );
    }
    let mut edge_additions = Vec::new();
    if existing.is_none() {
        edge_additions.push(CatalogEdge::new(
            table.id(),
            object_id,
            EdgeKind::Contains,
            0,
        ));
    }
    edge_additions.push(CatalogEdge::new(
        object_id,
        table.id(),
        EdgeKind::References,
        0,
    ));
    edge_additions.push(CatalogEdge::new(
        object_id,
        function.id(),
        EdgeKind::DependsOn,
        1,
    ));
    for (ordinal, column_id) in update_column_ids.into_iter().enumerate() {
        edge_additions.push(CatalogEdge::new(
            object_id,
            column_id,
            EdgeKind::References,
            u32::try_from(ordinal + 2)
                .map_err(|_| Error::InvalidArgument("too many trigger columns".to_string()))?,
        ));
    }
    Ok(DdlDelta {
        mutations: vec![mutation],
        edge_removals,
        edge_additions,
    })
}

/// Resolve the exact overload referenced by a trigger definition. Both DDL
/// authorization and catalog binding use this owner so an overloaded routine
/// cannot be substituted between the privilege check and publication.
pub(crate) fn resolve_trigger_function<'a>(
    generation: &'a CatalogGeneration,
    statement: &CreateTriggerStatement,
) -> Result<&'a CatalogObject> {
    let function_types = statement
        .function
        .argument_types
        .iter()
        .map(|data_type| bind_durable_type(data_type, generation))
        .collect::<Result<Vec<_>>>()?;
    let (function_namespace_id, function_name) =
        resolve_object_scope(generation, &statement.function.name)?;
    generation
        .find_routine(
            function_namespace_id,
            ObjectKind::Function,
            function_name,
            &function_types,
        )
        .map_err(catalog_argument)?
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "trigger function '{}' does not exist",
                statement.function
            ))
        })
}

pub(super) fn bind_create_job(
    statement: &radixdb_sql::CreateJobStatement,
    definition: BoundJobDefinition,
    generation: &CatalogGeneration,
    ids: &mut super::transaction::ObjectIdSource,
) -> Result<DdlDelta> {
    let (namespace_id, name) = resolve_object_scope(generation, &statement.name)?;
    let normalized_name = CatalogName::new(name).map_err(catalog_argument)?;
    if generation.objects_of_kind(ObjectKind::Job).any(|object| {
        object.namespace_id() == Some(namespace_id)
            && object.name().normalized() == normalized_name.normalized()
    }) {
        return Err(Error::InvalidArgument(format!(
            "job '{}' already exists",
            statement.name
        )));
    }

    let procedure = generation
        .object(definition.procedure_id)
        .filter(|object| object.kind() == ObjectKind::Procedure)
        .ok_or_else(|| Error::InvalidArgument("job procedure does not exist".to_string()))?;
    let principal = generation
        .object(definition.principal_id)
        .filter(|object| object.kind() == ObjectKind::Principal)
        .ok_or_else(|| Error::InvalidArgument("job principal does not exist".to_string()))?;
    let object_id = ids.next(generation)?;
    let payload = JobPayload::new(
        procedure.id(),
        principal.id(),
        definition.schedule,
        definition.arguments,
        statement.enabled,
        1,
        definition.resource_policy,
    )
    .map_err(catalog_argument)?;
    let object = CatalogObject::new(
        object_id,
        Some(namespace_id),
        Some(namespace_id),
        ObjectId::BOOTSTRAP_OWNER,
        normalized_name,
        1,
        CatalogPayload::Job(payload),
    )
    .map_err(catalog_argument)?;
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::create(object)],
        edge_removals: Vec::new(),
        edge_additions: vec![
            CatalogEdge::new(namespace_id, object_id, EdgeKind::Contains, 0),
            CatalogEdge::new(object_id, procedure.id(), EdgeKind::DependsOn, 0),
            CatalogEdge::new(object_id, principal.id(), EdgeKind::References, 1),
        ],
    })
}

pub(super) fn bind_drop_routine(
    statement: &DropRoutineStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let Some(routine) =
        resolve_optional_routine_signature(generation, statement.kind, &statement.signature)?
    else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::InvalidArgument(format!(
                "{} '{}' does not exist",
                routine_kind(statement.kind).name(),
                statement.signature
            )))
        };
    };
    if statement.behavior == DropBehaviorSyntax::Cascade
        && matches!(
            routine.payload(),
            CatalogPayload::Function(FunctionPayload::Native(_))
        )
    {
        return Err(Error::NotSupported(
            "native functions may only be dropped with RESTRICT".to_owned(),
        ));
    }
    bind_drop_program_object(generation, routine, statement.behavior)
}

pub(super) fn bind_drop_trigger(
    statement: &DropTriggerStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let Some(trigger) = resolve_optional_trigger(generation, &statement.name, &statement.table)?
    else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::InvalidArgument(format!(
                "trigger '{}' does not exist on table '{}'",
                statement.name, statement.table
            )))
        };
    };
    bind_drop_program_object(generation, trigger, statement.behavior)
}

pub(super) fn bind_drop_job(
    statement: &DropJobStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let Some(job) = resolve_optional_job(generation, &statement.name)? else {
        return if statement.if_exists {
            Ok(DdlDelta::default())
        } else {
            Err(Error::InvalidArgument(format!(
                "job '{}' does not exist",
                statement.name
            )))
        };
    };
    bind_drop_program_object(generation, job, statement.behavior)
}

pub(super) fn bind_alter_job(
    statement: &AlterJobStatement,
    generation: &CatalogGeneration,
) -> Result<DdlDelta> {
    let job = resolve_optional_job(generation, &statement.name)?.ok_or_else(|| {
        Error::InvalidArgument(format!("job '{}' does not exist", statement.name))
    })?;
    let CatalogPayload::Job(payload) = job.payload() else {
        unreachable!("job lookup returned a non-job")
    };
    if payload.enabled() == statement.enabled {
        return Ok(DdlDelta::default());
    }
    let definition_version = payload
        .definition_version()
        .checked_add(1)
        .ok_or_else(|| Error::internal("job definition version overflow"))?;
    let replacement_payload = JobPayload::new(
        payload.procedure_id(),
        payload.principal_id(),
        payload.schedule(),
        payload.arguments().to_vec(),
        statement.enabled,
        definition_version,
        payload.resource_policy(),
    )
    .map_err(catalog_argument)?;
    let replacement = CatalogObject::new(
        job.id(),
        job.namespace_id(),
        job.parent_id(),
        job.owner_principal_id(),
        job.name().clone(),
        job.definition_revision()
            .checked_add(1)
            .ok_or_else(|| Error::internal("job object revision overflow"))?,
        CatalogPayload::Job(replacement_payload),
    )
    .map_err(catalog_argument)?;
    Ok(DdlDelta {
        mutations: vec![CatalogMutation::alter(precondition(job)?, replacement)],
        ..DdlDelta::default()
    })
}

pub(crate) fn resolve_optional_routine_signature<'a>(
    generation: &'a CatalogGeneration,
    kind: RoutineKindSyntax,
    signature: &RoutineSignatureSyntax,
) -> Result<Option<&'a CatalogObject>> {
    let input_types = signature
        .argument_types
        .iter()
        .map(|syntax| match syntax {
            ProceduralType::Scalar(name) => {
                bind_catalog_type_in_generation(name.as_str(), generation)
            }
            ProceduralType::RowType(_) => Err(Error::NotSupported(
                "%ROWTYPE is not valid in a DROP routine signature".to_owned(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let (namespace, name) = resolve_object_scope(generation, &signature.name)?;
    generation
        .find_routine(namespace, routine_kind(kind), name, &input_types)
        .map_err(catalog_argument)
}

pub(crate) fn resolve_optional_trigger<'a>(
    generation: &'a CatalogGeneration,
    name: &ObjectName,
    table: &ObjectName,
) -> Result<Option<&'a CatalogObject>> {
    let (table_namespace, table_name) = resolve_object_scope(generation, table)?;
    let table = generation
        .find_relation(table_namespace, table_name)
        .map_err(catalog_argument)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let (last, namespace_path) = name
        .components
        .split_last()
        .ok_or_else(|| Error::InvalidArgument("trigger name is empty".to_string()))?;
    let namespace = if namespace_path.is_empty() {
        table_namespace
    } else {
        resolve_namespace(generation, namespace_path)?
    };
    if namespace != table_namespace {
        return Err(Error::InvalidArgument(
            "trigger and target table must use the same namespace".to_string(),
        ));
    }
    let name = CatalogName::new(last.value.as_str()).map_err(catalog_argument)?;
    Ok(generation.graph().children(table.id()).find(|object| {
        object.kind() == ObjectKind::Trigger && object.name().normalized() == name.normalized()
    }))
}

pub(crate) fn resolve_optional_job<'a>(
    generation: &'a CatalogGeneration,
    name: &ObjectName,
) -> Result<Option<&'a CatalogObject>> {
    let (namespace, name) = resolve_object_scope(generation, name)?;
    let normalized = CatalogName::new(name).map_err(catalog_argument)?;
    Ok(generation.objects_of_kind(ObjectKind::Job).find(|object| {
        object.namespace_id() == Some(namespace)
            && object.name().normalized() == normalized.normalized()
    }))
}

fn bind_drop_program_object(
    generation: &CatalogGeneration,
    root: &CatalogObject,
    behavior: DropBehaviorSyntax,
) -> Result<DdlDelta> {
    let mut selected = BTreeSet::new();
    let mut pending = vec![root.id()];
    while let Some(target) = pending.pop() {
        if !selected.insert(target) {
            continue;
        }
        for edge in generation.graph().incoming_edges(target) {
            if !edge.kind().is_dependency() {
                continue;
            }
            let dependent = generation
                .object(edge.source_object_id())
                .ok_or_else(|| Error::internal("catalog dependency source disappeared"))?;
            if behavior == DropBehaviorSyntax::Restrict && !selected.contains(&dependent.id()) {
                return Err(Error::InvalidArgument(format!(
                    "cannot drop {:?} '{}': {:?} '{}' depends on it; use CASCADE",
                    root.kind(),
                    root.name().display().as_str(),
                    dependent.kind(),
                    dependent.name().display().as_str()
                )));
            }
            pending.push(dependent.id());
        }
    }

    // Object grants are lifecycle metadata, not blockers. Remove every grant
    // on every object in the cascade in the same catalog generation.
    let mut grants = BTreeSet::new();
    for target in &selected {
        for edge in generation.graph().incoming_edges(*target) {
            if edge.kind() == EdgeKind::GrantsOn {
                grants.insert(edge.source_object_id());
            }
        }
    }
    selected.extend(grants);
    let mutations = selected
        .into_iter()
        .map(|id| {
            let object = generation
                .object(id)
                .ok_or_else(|| Error::internal("catalog drop target disappeared"))?;
            Ok(CatalogMutation::drop(precondition(object)?))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DdlDelta {
        mutations,
        ..DdlDelta::default()
    })
}

pub(crate) fn validate_routine_source_contract(
    statement: &CreateRoutineStatement,
    object: &CatalogObject,
    generation: &CatalogGeneration,
) -> Result<()> {
    let (namespace_id, name) = resolve_object_scope(generation, &statement.name)?;
    let definition = routine_definition(object)?;
    if object.kind() != routine_kind(statement.kind)
        || object.namespace_id() != Some(namespace_id)
        || object.name().normalized().as_str()
            != CatalogName::new(name)
                .map_err(catalog_argument)?
                .normalized()
                .as_str()
        || definition.source().as_str() != statement.normalized_source
        || definition.arguments() != bind_arguments(statement, generation)?.as_slice()
        || definition.result() != &bind_result(statement, generation)?
        || definition.volatility() != bind_volatility(statement)
        || definition.security() != bind_security(statement.security)
        || definition.search_path() != bind_search_path(statement, generation)?.as_slice()
        || definition.resource_policy() != bind_resource_policy(statement)?
    {
        return Err(Error::InvalidArgument(
            "stored routine source differs from its typed catalog contract".to_string(),
        ));
    }
    Ok(())
}

fn bind_arguments(
    statement: &CreateRoutineStatement,
    generation: &CatalogGeneration,
) -> Result<Vec<RoutineArgument>> {
    statement
        .arguments
        .iter()
        .map(|argument| {
            RoutineArgument::new(
                CatalogName::new(argument.name.value.as_str()).map_err(catalog_argument)?,
                match argument.mode {
                    RoutineArgumentMode::In => ArgumentMode::In,
                    RoutineArgumentMode::Out => ArgumentMode::Out,
                    RoutineArgumentMode::InOut => ArgumentMode::InOut,
                },
                bind_durable_type(&argument.data_type, generation)?,
                argument.nullable,
                argument.default.as_ref().map(ToString::to_string),
            )
            .map_err(catalog_argument)
        })
        .collect()
}

fn bind_result(
    statement: &CreateRoutineStatement,
    generation: &CatalogGeneration,
) -> Result<RoutineResult> {
    match &statement.returns {
        None => Ok(RoutineResult::Void),
        Some(RoutineReturnSyntax::Scalar {
            data_type,
            nullable,
        }) => Ok(RoutineResult::Scalar {
            data_type: bind_durable_type(data_type, generation)?,
            nullable: *nullable,
        }),
        Some(RoutineReturnSyntax::Table(columns)) => Ok(RoutineResult::Table(
            columns
                .iter()
                .map(|column| {
                    Ok(ResultColumn::new(
                        CatalogName::new(column.name.value.as_str()).map_err(catalog_argument)?,
                        bind_durable_type(&column.data_type, generation)?,
                        column.nullable,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        Some(RoutineReturnSyntax::Trigger) => Ok(RoutineResult::Trigger),
    }
}

pub(super) fn bind_durable_type(
    syntax: &ProceduralType,
    generation: &CatalogGeneration,
) -> Result<CatalogDataType> {
    match syntax {
        ProceduralType::Scalar(name) => bind_catalog_type_in_generation(name.as_str(), generation),
        ProceduralType::RowType(_) => Err(Error::NotSupported(
            "%ROWTYPE is local-only and cannot enter a durable routine signature".to_string(),
        )),
    }
}

pub(super) fn bind_search_path(
    statement: &CreateRoutineStatement,
    generation: &CatalogGeneration,
) -> Result<Vec<ObjectId>> {
    if statement.security == RoutineSecuritySyntax::Definer && statement.search_path.is_empty() {
        return Err(Error::InvalidArgument(
            "SECURITY DEFINER requires an explicit stable SEARCH PATH".to_string(),
        ));
    }
    if statement.search_path.is_empty() {
        return Ok(vec![ObjectId::BOOTSTRAP_NAMESPACE]);
    }
    statement
        .search_path
        .iter()
        .map(|path| resolve_namespace(generation, &path.components))
        .collect()
}

fn bind_resource_policy(statement: &CreateRoutineStatement) -> Result<ResourcePolicy> {
    let Some(policy) = &statement.resource_policy else {
        return Ok(ResourcePolicy::default_call());
    };
    let spelling = policy.to_string();
    if matches!(
        spelling.to_ascii_lowercase().as_str(),
        "default" | "default_call"
    ) {
        Ok(ResourcePolicy::default_call())
    } else {
        Err(Error::InvalidArgument(format!(
            "unknown built-in resource policy '{spelling}'"
        )))
    }
}

fn validate_replace_contract(
    existing: &CatalogObject,
    arguments: &[RoutineArgument],
    result: &RoutineResult,
) -> Result<()> {
    let definition = routine_definition(existing)?;
    let arguments_equal = definition
        .arguments()
        .iter()
        .zip(arguments)
        .all(|(left, right)| {
            left.name() == right.name()
                && left.mode() == right.mode()
                && left.data_type() == right.data_type()
                && left.nullable() == right.nullable()
        })
        && definition.arguments().len() == arguments.len();
    if !arguments_equal || definition.result() != result {
        return Err(Error::InvalidArgument(
            "CREATE OR REPLACE cannot change argument names/modes/nullability or result contract"
                .to_string(),
        ));
    }
    Ok(())
}

fn routine_definition(object: &CatalogObject) -> Result<&RoutineDefinition> {
    match object.payload() {
        CatalogPayload::Function(payload) => payload.procedural_definition().ok_or_else(|| {
            Error::InvalidArgument("native function has no procedural definition".to_owned())
        }),
        CatalogPayload::Procedure(payload) => Ok(payload.definition()),
        _ => Err(Error::internal("routine object has a non-routine payload")),
    }
}

fn input_types(
    statement: &CreateRoutineStatement,
    generation: &CatalogGeneration,
) -> Result<Vec<CatalogDataType>> {
    statement
        .arguments
        .iter()
        .filter(|argument| argument.mode != RoutineArgumentMode::Out)
        .map(|argument| bind_durable_type(&argument.data_type, generation))
        .collect()
}

pub(super) fn resolve_object_scope<'name>(
    generation: &CatalogGeneration,
    name: &'name ObjectName,
) -> Result<(ObjectId, &'name str)> {
    let (last, namespace) = name
        .components
        .split_last()
        .ok_or_else(|| Error::InvalidArgument("routine name is empty".to_string()))?;
    let namespace_id = if namespace.is_empty() {
        ObjectId::BOOTSTRAP_NAMESPACE
    } else {
        resolve_namespace(generation, namespace)?
    };
    Ok((namespace_id, last.value.as_str()))
}

pub(super) fn resolve_namespace(
    generation: &CatalogGeneration,
    path: &[radixdb_sql::Identifier],
) -> Result<ObjectId> {
    resolve_namespace_path(
        generation,
        path.iter().map(|component| component.value.as_str()),
    )
}

pub(crate) fn resolve_namespace_path<'a>(
    generation: &CatalogGeneration,
    path: impl IntoIterator<Item = &'a str>,
) -> Result<ObjectId> {
    let root = generation
        .object(ObjectId::BOOTSTRAP_NAMESPACE)
        .ok_or_else(|| Error::internal("bootstrap namespace is missing"))?;
    let mut resolved = ObjectId::BOOTSTRAP_NAMESPACE;
    let mut depth = 0usize;
    for component in path {
        if depth == 0 && root.name().normalized().as_str() == component.to_lowercase() {
            depth += 1;
            continue;
        }
        let namespace = generation
            .find_namespace(Some(resolved), component)
            .map_err(catalog_argument)?
            .ok_or_else(|| {
                Error::InvalidArgument(format!("namespace '{component}' does not exist"))
            })?;
        resolved = namespace.id();
        depth += 1;
    }
    if depth == 0 {
        return Err(Error::InvalidArgument(
            "namespace path is empty".to_string(),
        ));
    }
    Ok(resolved)
}

const fn routine_kind(kind: RoutineKindSyntax) -> ObjectKind {
    match kind {
        RoutineKindSyntax::Function => ObjectKind::Function,
        RoutineKindSyntax::Procedure => ObjectKind::Procedure,
    }
}

const fn bind_volatility(statement: &CreateRoutineStatement) -> Volatility {
    match statement.volatility {
        Some(RoutineVolatilitySyntax::Immutable) => Volatility::Immutable,
        Some(RoutineVolatilitySyntax::Stable) => Volatility::Stable,
        Some(RoutineVolatilitySyntax::Volatile) | None => Volatility::Volatile,
    }
}

const fn bind_security(security: RoutineSecuritySyntax) -> SecurityMode {
    match security {
        RoutineSecuritySyntax::Invoker => SecurityMode::Invoker,
        RoutineSecuritySyntax::Definer => SecurityMode::Definer,
    }
}

fn precondition(object: &CatalogObject) -> Result<ObjectPrecondition> {
    ObjectPrecondition::new(object.id(), object.kind(), object.definition_revision())
        .map_err(catalog_argument)
}
