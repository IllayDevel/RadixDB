//! Atomic bridge from verified procedural IR into the canonical SQL executor.
//!
//! The bridge owns no SQL semantics and no storage handle. It adapts values,
//! streams SQL rows, and coordinates one existing executor transaction.

mod binding;
mod binding_types;
mod cache;
mod call;
mod error;
mod external;
pub(crate) mod function;
mod function_binding;
mod host;
mod job;
mod native_function;
mod trigger;
mod value;

#[cfg(test)]
mod call_binding_tests;

use std::collections::BTreeSet;
use std::sync::Arc;

use radixdb_catalog::{
    CatalogGeneration, CatalogPayload, ObjectId, ObjectKind, ResourcePolicy, SecurityMode,
    Volatility,
};
use radixdb_procedural::{CompileIdentity, VerifiedProgram};
use radixdb_sql::{parse_sql, CreateRoutineStatement, Statement};

pub(crate) use cache::ProceduralProgramCache;
pub(crate) use call::CallBoundary;
pub use call::{ProceduralCallOutcome, ProceduralResultStage};
pub(crate) use job::bind_job_definition;
pub use job::{
    JobAttemptMetadata, JobAttemptOutcome, ScheduledJobDefinition, ScheduledJobSchedule,
};
pub(crate) use native_function::catalog_type_name;
pub use radixdb_procedural::{Diagnostic, DiagnosticKind};
pub(crate) use trigger::{
    fire_after_row_triggers, fire_before_row_triggers, fire_statement_triggers,
    prepare_dml_triggers, validate_trigger_attachment, DmlTriggerEvent, DmlTriggerPlan,
};

use crate::catalog::{
    validate_routine_source_contract, PROCEDURAL_COMPILER_ABI, PROCEDURAL_RUNTIME_ABI,
};

#[derive(Debug, Clone)]
pub(super) struct PublishedRoutine {
    pub program: VerifiedProgram,
    pub owner: ObjectId,
    pub security: SecurityMode,
    pub volatility: Volatility,
    pub resource_policy: ResourcePolicy,
}

pub(crate) fn compile_catalog_routine(
    executor: &crate::Executor,
    statement: &radixdb_sql::CreateRoutineStatement,
    catalog: &radixdb_catalog::CatalogGeneration,
    identity: radixdb_procedural::CompileIdentity,
    search_path: Vec<radixdb_catalog::ObjectId>,
) -> radixdb_core::Result<Vec<radixdb_catalog::ObjectId>> {
    if matches!(
        statement.returns,
        Some(radixdb_sql::RoutineReturnSyntax::Trigger)
    ) {
        if statement.kind != radixdb_sql::RoutineKindSyntax::Function
            || !statement.arguments.is_empty()
            || !matches!(
                statement.volatility,
                None | Some(radixdb_sql::RoutineVolatilitySyntax::Volatile)
            )
        {
            return Err(radixdb_core::Error::InvalidArgument(
                "RETURNS TRIGGER requires a zero-argument VOLATILE Function".to_string(),
            ));
        }
        // OLD/NEW are typed by CREATE TRIGGER ... ON <table>. The complete
        // verified program is therefore admitted at that attachment boundary.
        return Ok(Vec::new());
    }
    let (program, dependencies) =
        compile_verified_catalog_routine(executor, statement, catalog, identity, search_path)?;
    drop(program);
    Ok(dependencies)
}

fn compile_verified_catalog_routine(
    executor: &crate::Executor,
    statement: &CreateRoutineStatement,
    catalog: &CatalogGeneration,
    identity: CompileIdentity,
    search_path: Vec<ObjectId>,
) -> radixdb_core::Result<(VerifiedProgram, Vec<ObjectId>)> {
    let volatility = match statement.kind {
        radixdb_sql::RoutineKindSyntax::Procedure => None,
        radixdb_sql::RoutineKindSyntax::Function => Some(match statement.volatility {
            Some(radixdb_sql::RoutineVolatilitySyntax::Immutable) => Volatility::Immutable,
            Some(radixdb_sql::RoutineVolatilitySyntax::Stable) => Volatility::Stable,
            None | Some(radixdb_sql::RoutineVolatilitySyntax::Volatile) => Volatility::Volatile,
        }),
    };
    let mut resolver = binding::ExecutorSemanticResolver::with_search_path(
        executor,
        catalog,
        search_path,
        volatility,
    );
    let compiled = radixdb_procedural::compile_routine(statement, identity, &mut resolver)
        .map_err(procedural_compile_error)?;
    let program = radixdb_procedural::verify(compiled.program).map_err(procedural_compile_error)?;
    Ok((program, compiled.dependencies))
}

pub(super) fn load_published_routine(
    executor: &crate::Executor,
    routine_id: ObjectId,
    expected_kind: ObjectKind,
) -> radixdb_core::Result<Arc<PublishedRoutine>> {
    let (catalog, cacheable) = transaction_visible_catalog(executor)?;
    let object = catalog.object(routine_id).cloned().ok_or_else(|| {
        radixdb_core::Error::InvalidArgument(format!(
            "routine object {routine_id} does not exist in the transaction-visible catalog"
        ))
    })?;
    if object.kind() != expected_kind {
        return Err(radixdb_core::Error::InvalidArgument(format!(
            "catalog object {routine_id} is {:?}, expected {expected_kind:?}",
            object.kind()
        )));
    }
    let definition = match object.payload() {
        CatalogPayload::Function(payload) => payload.procedural_definition().ok_or_else(|| {
            radixdb_core::Error::InvalidArgument(format!(
                "native function {routine_id} cannot be loaded as a procedural routine"
            ))
        })?,
        CatalogPayload::Procedure(payload) => payload.definition(),
        _ => {
            return Err(radixdb_core::Error::InvalidArgument(format!(
                "catalog object {routine_id} has no executable routine definition"
            )))
        }
    };
    if definition.compiler_abi() != PROCEDURAL_COMPILER_ABI
        || definition.runtime_abi() != PROCEDURAL_RUNTIME_ABI
    {
        return Err(radixdb_core::Error::InvalidArgument(format!(
            "routine {routine_id} requires unsupported compiler/runtime ABI {}/{}",
            definition.compiler_abi(),
            definition.runtime_abi()
        )));
    }

    let cache_key = routine_cache_key(catalog.as_ref(), &object)?;
    if cacheable {
        if let Some(cached) = executor.procedural_cache.get(&cache_key) {
            // Cached IR is private and immutable, but verification remains the
            // fail-closed admission boundary if that invariant ever changes.
            radixdb_procedural::verify(cached.program.program().clone())
                .map_err(procedural_compile_error)?;
            return Ok(cached);
        }
    }

    let mut statements = parse_sql(definition.source().as_str())
        .map_err(|error| radixdb_core::Error::Parse(error.to_string()))?;
    if statements.len() != 1 {
        return Err(radixdb_core::Error::InvalidArgument(format!(
            "routine {routine_id} durable source must contain exactly one CREATE statement"
        )));
    }
    let Statement::CreateRoutine(statement) = statements.remove(0) else {
        return Err(radixdb_core::Error::InvalidArgument(format!(
            "routine {routine_id} durable source is not CREATE FUNCTION/PROCEDURE"
        )));
    };
    validate_routine_source_contract(&statement, &object, catalog.as_ref())?;

    let identity = CompileIdentity {
        object_id: object.id(),
        definition_revision: object.definition_revision(),
        display_name: statement.name.to_string(),
    };
    let (program, dependencies) = compile_verified_catalog_routine(
        executor,
        &statement,
        catalog.as_ref(),
        identity,
        definition.search_path().to_vec(),
    )?;
    let search_path = definition
        .search_path()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let compiled_dependencies = dependencies
        .into_iter()
        .filter(|id| !search_path.contains(id))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if compiled_dependencies != definition.dependency_ids() {
        return Err(radixdb_core::Error::InvalidArgument(format!(
            "routine {routine_id} durable dependency contract is stale or corrupt"
        )));
    }

    let routine = Arc::new(PublishedRoutine {
        program,
        owner: object.owner_principal_id(),
        security: definition.security(),
        volatility: definition.volatility(),
        resource_policy: definition.resource_policy(),
    });
    if cacheable {
        Ok(executor.procedural_cache.insert(cache_key, routine))
    } else {
        Ok(routine)
    }
}

pub(crate) fn transaction_visible_catalog(
    executor: &crate::Executor,
) -> radixdb_core::Result<(Arc<CatalogGeneration>, bool)> {
    let active = executor.active_transaction.lock().unwrap();
    if let Some(state) = active.as_ref() {
        return Ok((
            state.catalog.working_generation_shared(),
            !state.catalog.has_pending_catalog_changes(),
        ));
    }
    drop(active);
    executor.engine.pin_catalog().map(|catalog| (catalog, true))
}

fn routine_cache_key(
    catalog: &CatalogGeneration,
    object: &radixdb_catalog::CatalogObject,
) -> radixdb_core::Result<cache::RoutineCacheKey> {
    let definition = match object.payload() {
        CatalogPayload::Function(payload) => payload.procedural_definition().ok_or_else(|| {
            radixdb_core::Error::InvalidArgument(format!(
                "native function {} has no procedural cache key",
                object.id()
            ))
        })?,
        CatalogPayload::Procedure(payload) => payload.definition(),
        _ => {
            return Err(radixdb_core::Error::InvalidArgument(format!(
                "catalog object {} has no executable routine definition",
                object.id()
            )))
        }
    };
    let mut dependency_ids = definition
        .search_path()
        .iter()
        .chain(definition.dependency_ids())
        .copied()
        .collect::<BTreeSet<_>>();
    dependency_ids.insert(object.id());
    let dependency_versions = dependency_ids
        .into_iter()
        .map(|id| {
            catalog
                .object(id)
                .map(|dependency| (id, dependency.definition_revision()))
                .ok_or_else(|| {
                    radixdb_core::Error::InvalidArgument(format!(
                        "routine {} dependency {id} is missing",
                        object.id()
                    ))
                })
        })
        .collect::<radixdb_core::Result<Vec<_>>>()?;
    let meta = catalog.meta();
    Ok(cache::RoutineCacheKey {
        database_id: meta.database_id(),
        catalog_id: meta.catalog_id(),
        catalog_generation: meta.catalog_generation(),
        object_id: object.id(),
        definition_revision: object.definition_revision(),
        source_digest: *definition.source().digest(),
        dependency_versions,
        compiler_abi: definition.compiler_abi(),
        runtime_abi: definition.runtime_abi(),
    })
}

fn procedural_compile_error(diagnostic: radixdb_procedural::Diagnostic) -> radixdb_core::Error {
    radixdb_core::Error::InvalidArgument(format!("procedural compilation failed: {diagnostic}"))
}

#[cfg(test)]
mod tests;
