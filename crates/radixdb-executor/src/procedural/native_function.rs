//! Native-function binding and invocation at the procedural boundary.

use std::collections::BTreeSet;

use radixdb_catalog::{
    CatalogDataType, CatalogGeneration, NativeFunctionDefinition, ObjectId, Volatility,
};
use radixdb_core::{DataType, Error, Result, Value};
use radixdb_plugin_host::InvocationLimits;
use radixdb_procedural::{Diagnostic, DiagnosticKind, ProceduralResult};
use radixdb_sql::{walk_expression_tree, Expression};

use crate::binding::output::OutputBindingExt;
use crate::{ExecutionContext, Executor};

pub(super) fn invoke(
    executor: &Executor,
    context: &ExecutionContext,
    name: &str,
    object_id: ObjectId,
    definition: &NativeFunctionDefinition,
    arguments: &[Value],
) -> Result<Value> {
    context.check_cancelled()?;
    let descriptor = executor
        .plugin_registry
        .function(&object_id.into_bytes())
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "native function {object_id} is missing from the immutable plugin registry"
            ))
        })?;
    if descriptor.semantic_revision != definition.semantic_revision()
        || descriptor.local_id != definition.local_id()
    {
        return Err(Error::InvalidArgument(format!(
            "native function {object_id} descriptor is stale for this catalog generation"
        )));
    }
    executor
        .plugin_registry
        .invoke_scalar_function(
            object_id.into_bytes(),
            arguments,
            InvocationLimits {
                cancel_check: Some(crate::context::current_query_is_cancelled),
                deadline_unix_ns: deadline_unix_ns(context.timeout_ms()),
            },
        )
        .map_err(|error| Error::NativeFunction {
            function: name.to_owned(),
            status: error.status_code(),
        })
}

pub(super) fn bind_expression_dependencies(
    executor: &Executor,
    caller_volatility: Option<Volatility>,
    expression: &Expression,
) -> ProceduralResult<Vec<ObjectId>> {
    let mut calls = Vec::new();
    walk_expression_tree(expression, &mut |node| {
        let Expression::FunctionCall(function) = node else {
            return;
        };
        if !executor.function_registry.exists(&function.function) {
            calls.push((function.function.to_string(), function.arguments.clone()));
        }
    });
    let mut dependencies = BTreeSet::new();
    for (name, arguments) in calls {
        let argument_types = arguments
            .iter()
            .map(|argument| {
                executor
                    .bind_scalar_output_metadata(argument)
                    .map(|(data_type, logical_type, _, _)| {
                        (data_type != DataType::Null).then_some(logical_type)
                    })
                    .map_err(bind_error)
            })
            .collect::<ProceduralResult<Vec<_>>>()?;
        let Some((object_id, target_volatility)) =
            super::function::bind_stored_function_dependency(executor, &name, &argument_types)
                .map_err(bind_error)?
        else {
            return Err(Diagnostic::new(
                DiagnosticKind::BindUnknownObject,
                format!("stored function '{name}' does not exist for supplied argument types"),
            ));
        };
        if caller_volatility.is_some_and(|caller| !volatility_allows(caller, target_volatility)) {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                format!(
                    "{:?} function cannot invoke {target_volatility:?} stored function {name}",
                    caller_volatility.expect("restricted caller volatility")
                ),
            ));
        }
        dependencies.insert(object_id);
    }
    Ok(dependencies.into_iter().collect())
}

pub(crate) fn catalog_type_name(catalog: &CatalogGeneration, data_type: CatalogDataType) -> String {
    data_type.type_object_id().map_or_else(
        || data_type.logical_type().to_string(),
        |id| {
            catalog
                .object(id)
                .and_then(|object| crate::catalog::qualified_catalog_name(catalog, object).ok())
                .unwrap_or_else(|| format!("external:{id}"))
        },
    )
}

fn deadline_unix_ns(timeout_ms: u64) -> u64 {
    if timeout_ms == 0 {
        return u64::MAX;
    }
    let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    u64::try_from(now.as_nanos())
        .unwrap_or(u64::MAX)
        .saturating_add(timeout_ms.saturating_mul(1_000_000))
}

fn volatility_allows(caller: Volatility, target: Volatility) -> bool {
    match caller {
        Volatility::Volatile => true,
        Volatility::Stable => target != Volatility::Volatile,
        Volatility::Immutable => target == Volatility::Immutable,
    }
}

fn bind_error(error: Error) -> Diagnostic {
    let kind = match error {
        Error::TableNotFound(_)
        | Error::TableOrViewNotFound(_)
        | Error::ColumnNotFound(_)
        | Error::ViewNotFound(_)
        | Error::IndexNotFound(_) => DiagnosticKind::BindUnknownObject,
        _ => DiagnosticKind::BindTypeMismatch,
    };
    Diagnostic::new(kind, error.to_string())
}
