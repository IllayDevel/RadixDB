//! Catalog binding for stored scalar functions.

use radixdb_catalog::{
    CatalogGeneration, CatalogObject, CatalogPayload, FunctionPayload, NativeFunctionDefinition,
    ObjectId, ObjectKind, RoutineArgument, RoutineDefinition, RoutineResult, Volatility,
};
use radixdb_core::{DataType, Error, LogicalTypeRef, Result};

use crate::Executor;

use super::transaction_visible_catalog;

#[derive(Debug, Clone)]
pub(crate) struct BoundStoredFunction {
    pub result_type: DataType,
    pub logical_type: LogicalTypeRef,
    pub type_name: String,
    pub nullable: bool,
}

#[derive(Clone, Copy)]
pub(super) enum CandidateDefinition<'a> {
    Procedural(&'a RoutineDefinition),
    Native(&'a NativeFunctionDefinition),
}

impl<'a> CandidateDefinition<'a> {
    pub(super) fn arguments(self) -> &'a [RoutineArgument] {
        match self {
            Self::Procedural(value) => value.arguments(),
            Self::Native(value) => value.arguments(),
        }
    }

    pub(super) fn result(self) -> &'a RoutineResult {
        match self {
            Self::Procedural(value) => value.result(),
            Self::Native(value) => value.result(),
        }
    }

    pub(super) const fn volatility(self) -> Volatility {
        match self {
            Self::Procedural(value) => value.volatility(),
            Self::Native(value) => value.volatility(),
        }
    }
}

pub(super) struct Candidate<'a> {
    pub(super) object: &'a CatalogObject,
    pub(super) definition: CandidateDefinition<'a>,
    cost: u32,
}

pub(crate) fn bind_stored_function_result(
    executor: &Executor,
    name: &str,
    argument_types: &[Option<LogicalTypeRef>],
) -> Result<Option<BoundStoredFunction>> {
    let (catalog, _) = transaction_visible_catalog(executor)?;
    resolve_candidate(catalog.as_ref(), name, argument_types).map(|candidate| {
        candidate.map(|candidate| {
            let RoutineResult::Scalar {
                data_type,
                nullable,
            } = candidate.definition.result()
            else {
                unreachable!("scalar resolver admitted a non-scalar function")
            };
            BoundStoredFunction {
                result_type: data_type.logical_type(),
                logical_type: data_type.logical_type_ref(),
                type_name: super::native_function::catalog_type_name(catalog.as_ref(), *data_type),
                nullable: *nullable,
            }
        })
    })
}

pub(crate) fn bind_stored_function_dependency(
    executor: &Executor,
    name: &str,
    argument_types: &[Option<LogicalTypeRef>],
) -> Result<Option<(ObjectId, Volatility)>> {
    let (catalog, _) = transaction_visible_catalog(executor)?;
    resolve_candidate(catalog.as_ref(), name, argument_types).map(|candidate| {
        candidate.map(|candidate| (candidate.object.id(), candidate.definition.volatility()))
    })
}

pub(super) fn resolve_candidate<'a>(
    catalog: &'a CatalogGeneration,
    name: &str,
    argument_types: &[Option<LogicalTypeRef>],
) -> Result<Option<Candidate<'a>>> {
    let (namespace, routine_name) = resolve_name(catalog, name)?;
    let mut candidates = Vec::new();
    for object in catalog.objects_of_kind(ObjectKind::Function) {
        if object.namespace_id() != Some(namespace)
            || !object
                .name()
                .normalized()
                .as_str()
                .eq_ignore_ascii_case(routine_name)
        {
            continue;
        }
        let CatalogPayload::Function(payload) = object.payload() else {
            unreachable!("catalog kind/payload invariant")
        };
        let definition = match payload {
            FunctionPayload::Procedural(value) => CandidateDefinition::Procedural(value),
            FunctionPayload::Native(value) => CandidateDefinition::Native(value),
        };
        if !matches!(definition.result(), RoutineResult::Scalar { .. }) {
            continue;
        }
        if let Some(cost) = candidate_cost(definition, argument_types) {
            candidates.push(Candidate {
                object,
                definition,
                cost,
            });
        }
    }
    let Some(minimum) = candidates.iter().map(|candidate| candidate.cost).min() else {
        return Ok(None);
    };
    candidates.retain(|candidate| candidate.cost == minimum);
    if candidates.len() != 1 {
        return Err(Error::invalid_argument(format!(
            "stored function call {name} has multiple equal-cost overloads"
        )));
    }
    Ok(candidates.pop())
}

pub(super) fn resolve_name<'a>(
    catalog: &'a CatalogGeneration,
    name: &'a str,
) -> Result<(ObjectId, &'a str)> {
    let Some((namespace_name, routine_name)) = name.rsplit_once('.') else {
        return Ok((ObjectId::BOOTSTRAP_NAMESPACE, name));
    };
    let namespace = crate::catalog::resolve_namespace_path(catalog, namespace_name.split('.'))?;
    Ok((namespace, routine_name))
}

fn candidate_cost(
    definition: CandidateDefinition<'_>,
    argument_types: &[Option<LogicalTypeRef>],
) -> Option<u32> {
    if argument_types.len() > definition.arguments().len() {
        return None;
    }
    let mut cost = 0_u32;
    for (index, declared) in definition.arguments().iter().enumerate() {
        let Some(actual) = argument_types.get(index) else {
            if declared.default_sql().is_some() {
                continue;
            }
            return None;
        };
        match actual {
            None if matches!(definition, CandidateDefinition::Native(value) if value.strict()) => {}
            None if declared.nullable() => {}
            None => return None,
            Some(actual) if *actual == declared.data_type().logical_type_ref() => {}
            Some(LogicalTypeRef::Builtin(DataType::Integer))
                if declared.data_type().logical_type() == DataType::Decimal =>
            {
                cost = cost.saturating_add(1);
            }
            Some(LogicalTypeRef::Builtin(DataType::Date))
                if declared.data_type().logical_type() == DataType::Timestamp =>
            {
                cost = cost.saturating_add(1);
            }
            Some(_) => return None,
        }
    }
    Some(cost)
}
