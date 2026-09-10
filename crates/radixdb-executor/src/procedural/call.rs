use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use radixdb_catalog::{
    ArgumentMode, CatalogDataType, CatalogGeneration, CatalogObject, CatalogPayload, ObjectId,
    ObjectKind, ResourcePolicy, RoutineDefinition, RoutineResult,
};
use radixdb_core::{DataType, Error, Result, Row, Value};
use radixdb_procedural::{
    BudgetOwner, BudgetSnapshot, CancellationProbe, Diagnostic, ExecutionOutcome, Interpreter,
    PrincipalContext, PrincipalHost, ProceduralResult, RuntimeValue, SqlRowSink, VerifiedProgram,
};
use radixdb_sql::{
    walk_expression_tree_mut, CallArgumentSyntax, CallStatement, Expression, Parser, Precedence,
};
use radixdb_storage::traits::Engine;
use radixdb_storage::traits::{MemoryResult, QueryResult};

use crate::catalog::DdlTransaction;
use crate::context::ExecutionContext;
use crate::expression::ExpressionEval;
use crate::mutation::host::ActiveTransaction;
use crate::Executor;

use super::error::{cleanup_failed, map_executor_error};
use super::host::ExecutorProceduralHost;
use super::load_published_routine;
use super::transaction_visible_catalog;
use super::value::scalar_value;

static NEXT_CALL_BOUNDARY_ID: AtomicU64 = AtomicU64::new(1);

/// Transaction-aware result staging supplied by the protocol/API boundary.
///
/// Rows may be spooled incrementally, but cannot become visible to the caller
/// until `publish` is invoked after the call statement commits or releases its
/// internal savepoint. This avoids an executor-owned materialize-all default.
pub trait ProceduralResultStage {
    fn begin(&mut self) -> ProceduralResult<()>;
    fn stage_row(&mut self, row: Vec<RuntimeValue>) -> ProceduralResult<()>;
    fn publish(&mut self);
    fn discard(&mut self);
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProceduralCallOutcome {
    execution: ExecutionOutcome,
    budget: BudgetSnapshot,
}

impl ProceduralCallOutcome {
    pub const fn execution(&self) -> &ExecutionOutcome {
        &self.execution
    }

    pub const fn budget(&self) -> BudgetSnapshot {
        self.budget
    }
}

#[doc(hidden)]
pub enum CallBoundary {
    OwnedTransaction,
    CallerSavepoint(String),
}

struct ResultStageAdapter<'a>(&'a mut dyn ProceduralResultStage);

impl SqlRowSink for ResultStageAdapter<'_> {
    fn push_row(&mut self, row: Vec<RuntimeValue>) -> ProceduralResult<()> {
        self.0.stage_row(row)
    }
}

#[derive(Default)]
pub(super) struct CallStatementStage {
    rows: Vec<Row>,
    published: bool,
}

impl ProceduralResultStage for CallStatementStage {
    fn begin(&mut self) -> ProceduralResult<()> {
        self.rows.clear();
        self.published = false;
        Ok(())
    }

    fn stage_row(&mut self, row: Vec<RuntimeValue>) -> ProceduralResult<()> {
        self.rows.push(Row::from_values(
            row.iter()
                .map(scalar_value)
                .collect::<ProceduralResult<Vec<_>>>()?,
        ));
        Ok(())
    }

    fn publish(&mut self) {
        self.published = true;
    }

    fn discard(&mut self) {
        self.rows.clear();
        self.published = false;
    }
}

struct ExternalCallCandidate<'a> {
    object: &'a CatalogObject,
    definition: &'a RoutineDefinition,
    positions: Vec<Option<usize>>,
    cost: u32,
}

impl Executor {
    pub(crate) fn execute_call_statement(
        &self,
        statement: &CallStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        // Dispatch owns the shared DDL fence for the outer CALL. Reuse that
        // ownership for every procedural SQL leaf instead of recursively
        // acquiring the read side after a DDL writer may have queued.
        let executor = self.fork_with_owned_ddl_fence();
        let boundary = executor.begin_procedural_boundary()?;
        match execute_call_statement_inside_boundary(&executor, statement, context) {
            Ok(result) => match executor.complete_procedural_boundary(&boundary) {
                Ok(()) => Ok(result),
                Err(error) => {
                    let _ = executor.abort_procedural_boundary(&boundary);
                    Err(error)
                }
            },
            Err(error) => {
                let _ = executor.abort_procedural_boundary(&boundary);
                Err(error)
            }
        }
    }

    /// Resolve and execute one durable Procedure from the transaction-visible
    /// catalog generation. Source and typed metadata are verified after the
    /// call boundary pins that generation.
    pub fn execute_procedure(
        &self,
        routine: ObjectId,
        arguments: Vec<RuntimeValue>,
        context: &ExecutionContext,
        principals: PrincipalContext,
        results: &mut dyn ProceduralResultStage,
    ) -> ProceduralResult<ProceduralCallOutcome> {
        if !self.ddl_fence_already_held {
            let _fence = self.engine.acquire_ddl_statement_fence(false);
            return self
                .fork_with_owned_ddl_fence()
                .execute_procedure(routine, arguments, context, principals, results);
        }
        validate_principals(principals)?;
        results.begin()?;
        let boundary = match self.begin_procedural_boundary() {
            Ok(boundary) => boundary,
            Err(error) => {
                results.discard();
                return Err(map_executor_error(error));
            }
        };
        self.execute_procedure_inside_boundary(
            routine, arguments, context, principals, results, &boundary,
        )
    }

    pub(super) fn execute_procedure_inside_boundary(
        &self,
        routine: ObjectId,
        arguments: Vec<RuntimeValue>,
        context: &ExecutionContext,
        principals: PrincipalContext,
        results: &mut dyn ProceduralResultStage,
        boundary: &CallBoundary,
    ) -> ProceduralResult<ProceduralCallOutcome> {
        crate::authorization::authorize_routine_invocation(
            self,
            principals.session_principal,
            principals.effective_principal,
            routine,
        )
        .map_err(map_executor_error)?;
        let published = match load_published_routine(self, routine, ObjectKind::Procedure) {
            Ok(published) => published,
            Err(error) => {
                results.discard();
                return Err(self.abort_after_setup_error(boundary, map_executor_error(error)));
            }
        };
        let budget = match context.procedural_budget() {
            Some(budget) => budget.clone(),
            None => match call_budget(context, published.resource_policy) {
                Ok(budget) => budget,
                Err(error) => {
                    results.discard();
                    return Err(self.abort_after_setup_error(boundary, error));
                }
            },
        };
        self.execute_inside_boundary(
            &published.program,
            arguments,
            context,
            principals,
            (published.security == radixdb_catalog::SecurityMode::Definer)
                .then_some(published.owner),
            &budget,
            results,
            boundary,
        )
    }

    /// Execute one verified procedural program as one atomic engine operation.
    pub fn execute_procedural_program(
        &self,
        program: &VerifiedProgram,
        arguments: Vec<RuntimeValue>,
        context: &ExecutionContext,
        principals: PrincipalContext,
        policy: ResourcePolicy,
        results: &mut dyn ProceduralResultStage,
    ) -> ProceduralResult<ProceduralCallOutcome> {
        if !self.ddl_fence_already_held {
            let _fence = self.engine.acquire_ddl_statement_fence(false);
            return self.fork_with_owned_ddl_fence().execute_procedural_program(
                program, arguments, context, principals, policy, results,
            );
        }
        validate_principals(principals)?;
        let budget = call_budget(context, policy)?;
        budget.check_boundary()?;
        results.begin()?;

        let boundary = match self.begin_procedural_boundary() {
            Ok(boundary) => boundary,
            Err(error) => {
                results.discard();
                return Err(map_executor_error(error));
            }
        };
        self.execute_inside_boundary(
            program, arguments, context, principals, None, &budget, results, &boundary,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_inside_boundary(
        &self,
        program: &VerifiedProgram,
        arguments: Vec<RuntimeValue>,
        context: &ExecutionContext,
        principals: PrincipalContext,
        definer: Option<ObjectId>,
        budget: &BudgetOwner,
        results: &mut dyn ProceduralResultStage,
        boundary: &CallBoundary,
    ) -> ProceduralResult<ProceduralCallOutcome> {
        let execution = {
            let mut host = ExecutorProceduralHost::new(self, context, principals, None);
            let mut sink = ResultStageAdapter(results);
            if let Some(owner) = definer {
                host.push_definer(owner).and_then(|()| {
                    let execution = Interpreter
                        .execute_with_result_sink(program, arguments, &mut host, budget, &mut sink);
                    let cleanup = host.pop_definer();
                    match (execution, cleanup) {
                        (Ok(outcome), Ok(())) => Ok(outcome),
                        (Err(primary), Ok(())) => Err(primary),
                        (Ok(_), Err(cleanup)) => Err(cleanup),
                        (Err(primary), Err(cleanup)) => {
                            Err(primary.with_detail("definer_cleanup_error", cleanup.to_string()))
                        }
                    }
                })
            } else {
                Interpreter
                    .execute_with_result_sink(program, arguments, &mut host, budget, &mut sink)
            }
        };

        match execution {
            Ok(execution) => {
                if let Err(error) = self.complete_procedural_boundary(boundary) {
                    results.discard();
                    let primary = map_executor_error(error);
                    return Err(match self.abort_procedural_boundary(boundary) {
                        Ok(()) => primary,
                        Err(cleanup) => cleanup_failed(primary, cleanup),
                    });
                }
                results.publish();
                Ok(ProceduralCallOutcome {
                    execution,
                    budget: budget.snapshot(),
                })
            }
            Err(primary) => {
                results.discard();
                Err(match self.abort_procedural_boundary(boundary) {
                    Ok(()) => primary,
                    Err(cleanup) => cleanup_failed(primary, cleanup),
                })
            }
        }
    }

    pub(super) fn abort_after_setup_error(
        &self,
        boundary: &CallBoundary,
        primary: Diagnostic,
    ) -> Diagnostic {
        match self.abort_procedural_boundary(boundary) {
            Ok(()) => primary,
            Err(cleanup) => cleanup_failed(primary, cleanup),
        }
    }

    pub(crate) fn begin_procedural_boundary(&self) -> radixdb_core::Result<CallBoundary> {
        let call_id = NEXT_CALL_BOUNDARY_ID.fetch_add(1, Ordering::Relaxed);
        let mut active = self.active_transaction.lock().unwrap();
        if let Some(state) = active.as_mut() {
            let name = format!("\0radixdb-call-{call_id}");
            state.create_savepoint(&name)?;
            return Ok(CallBoundary::CallerSavepoint(name));
        }

        let mut transaction = self.engine.begin_transaction()?;
        let catalog = match self.engine.pin_catalog() {
            Ok(catalog) => DdlTransaction::begin_shared(catalog),
            Err(error) => {
                return match transaction.rollback() {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(radixdb_core::Error::internal(format!(
                        "cannot pin procedural catalog: {error}; transaction rollback also failed: {cleanup}"
                    ))),
                };
            }
        };
        *active = Some(ActiveTransaction::new(transaction, catalog));
        Ok(CallBoundary::OwnedTransaction)
    }

    pub(crate) fn complete_procedural_boundary(
        &self,
        boundary: &CallBoundary,
    ) -> radixdb_core::Result<()> {
        match boundary {
            CallBoundary::OwnedTransaction => self.commit_installed_transaction(),
            CallBoundary::CallerSavepoint(name) => self.release_active_savepoint(name),
        }
    }

    pub(crate) fn abort_procedural_boundary(
        &self,
        boundary: &CallBoundary,
    ) -> radixdb_core::Result<()> {
        match boundary {
            CallBoundary::OwnedTransaction => {
                if self.has_active_transaction() {
                    self.rollback_installed_transaction()
                } else {
                    Ok(())
                }
            }
            CallBoundary::CallerSavepoint(name) => {
                self.rollback_active_to_savepoint(name)?;
                self.release_active_savepoint(name)
            }
        }
    }
}

fn execute_call_statement_inside_boundary(
    executor: &Executor,
    statement: &CallStatement,
    context: &ExecutionContext,
) -> Result<Box<dyn QueryResult>> {
    let supplied = statement
        .arguments
        .iter()
        .map(|argument| {
            ExpressionEval::compile(&argument.value, &[])?
                .with_context(context)
                .eval_slice(&Row::new())
                .map(|value| (argument, value))
        })
        .collect::<Result<Vec<_>>>()?;
    let (catalog, _) = transaction_visible_catalog(executor)?;
    let candidate = resolve_external_call(catalog.as_ref(), statement, &supplied)?;
    let arguments = bind_external_arguments(executor, context, &candidate, &supplied)?;
    let columns = external_call_columns(candidate.definition);
    let result_model = candidate.definition.result().clone();
    let mut stage = CallStatementStage::default();
    let outcome = executor
        .execute_procedure(
            candidate.object.id(),
            arguments,
            context,
            PrincipalContext {
                session_principal: context.principal_id(),
                invoker_principal: context.effective_principal_id(),
                effective_principal: context.effective_principal_id(),
            },
            &mut stage,
        )
        .map_err(procedural_call_error)?;
    if !stage.published {
        return Err(Error::internal(
            "procedure result stage was not published after successful call",
        ));
    }
    let rows = match result_model {
        RoutineResult::Table(_) => stage.rows,
        RoutineResult::Void if columns.is_empty() => Vec::new(),
        RoutineResult::Void => vec![Row::from_values(
            outcome
                .execution()
                .output_values
                .iter()
                .map(scalar_value)
                .collect::<ProceduralResult<Vec<_>>>()
                .map_err(procedural_call_error)?,
        )],
        RoutineResult::Scalar { .. } | RoutineResult::Trigger => {
            return Err(Error::internal(
                "procedure catalog object has a function-only result contract",
            ));
        }
    };
    Ok(Box::new(MemoryResult::with_rows(columns, rows)))
}

fn resolve_external_call<'a>(
    catalog: &'a CatalogGeneration,
    statement: &CallStatement,
    supplied: &[(&CallArgumentSyntax, Value)],
) -> Result<ExternalCallCandidate<'a>> {
    let (namespace, routine_name) = external_call_name(catalog, statement)?;
    let mut candidates = Vec::new();
    for object in catalog.objects_of_kind(ObjectKind::Procedure) {
        if object.namespace_id() != Some(namespace)
            || !object
                .name()
                .normalized()
                .as_str()
                .eq_ignore_ascii_case(routine_name)
        {
            continue;
        }
        let CatalogPayload::Procedure(payload) = object.payload() else {
            unreachable!("catalog kind/payload invariant")
        };
        if let Some((positions, cost)) = external_candidate(payload.definition(), supplied) {
            candidates.push(ExternalCallCandidate {
                object,
                definition: payload.definition(),
                positions,
                cost,
            });
        }
    }
    let minimum = candidates
        .iter()
        .map(|candidate| candidate.cost)
        .min()
        .ok_or_else(|| {
            Error::invalid_argument(format!(
                "no procedure overload matches call {}",
                statement.routine
            ))
        })?;
    candidates.retain(|candidate| candidate.cost == minimum);
    if candidates.len() != 1 {
        return Err(Error::invalid_argument(format!(
            "procedure call {} has multiple equal-cost overloads",
            statement.routine
        )));
    }
    Ok(candidates.remove(0))
}

pub(super) fn bind_job_call(
    executor: &Executor,
    catalog: &CatalogGeneration,
    routine: &radixdb_sql::ObjectName,
    arguments: &[CallArgumentSyntax],
    context: &ExecutionContext,
) -> Result<(ObjectId, Vec<(CatalogDataType, Value)>)> {
    let supplied = arguments
        .iter()
        .map(|argument| {
            ExpressionEval::compile(&argument.value, &[])?
                .with_context(context)
                .eval_slice(&Row::new())
                .map(|value| (argument, value))
        })
        .collect::<Result<Vec<_>>>()?;
    let statement = CallStatement {
        token: routine
            .components
            .last()
            .ok_or_else(|| Error::invalid_argument("job procedure name is empty"))?
            .token
            .clone(),
        routine: routine.clone(),
        arguments: arguments.to_vec(),
    };
    let candidate = resolve_external_call(catalog, &statement, &supplied)?;
    let procedure_id = candidate.object.id();
    let input_types = candidate
        .definition
        .arguments()
        .iter()
        .filter(|argument| argument.mode() != ArgumentMode::Out)
        .map(|argument| argument.data_type())
        .collect::<Vec<_>>();
    let values = bind_external_arguments(executor, context, &candidate, &supplied)?
        .iter()
        .map(scalar_value)
        .collect::<ProceduralResult<Vec<_>>>()
        .map_err(procedural_call_error)?;
    debug_assert_eq!(input_types.len(), values.len());
    Ok((procedure_id, input_types.into_iter().zip(values).collect()))
}

fn external_call_name<'a>(
    catalog: &CatalogGeneration,
    statement: &'a CallStatement,
) -> Result<(ObjectId, &'a str)> {
    let (name, namespace) = statement
        .routine
        .components
        .split_last()
        .ok_or_else(|| Error::invalid_argument("procedure name is empty"))?;
    if namespace.is_empty() {
        return Ok((ObjectId::BOOTSTRAP_NAMESPACE, name.value.as_str()));
    }
    let namespace = crate::catalog::resolve_namespace_path(
        catalog,
        namespace.iter().map(|component| component.value.as_str()),
    )?;
    Ok((namespace, name.value.as_str()))
}

fn external_candidate(
    definition: &RoutineDefinition,
    supplied: &[(&CallArgumentSyntax, Value)],
) -> Option<(Vec<Option<usize>>, u32)> {
    let input_count = definition
        .arguments()
        .iter()
        .filter(|argument| argument.mode() != ArgumentMode::Out)
        .count();
    if supplied.len() > input_count {
        return None;
    }
    let mut positions = vec![None; definition.arguments().len()];
    let input_positions = definition
        .arguments()
        .iter()
        .enumerate()
        .filter_map(|(index, argument)| (argument.mode() != ArgumentMode::Out).then_some(index))
        .collect::<Vec<_>>();
    let mut positional = 0;
    for (supplied_index, (argument, _)) in supplied.iter().enumerate() {
        let declared_index = if let Some(name) = &argument.name {
            definition.arguments().iter().position(|declared| {
                declared.mode() != ArgumentMode::Out
                    && declared.name().normalized().as_str() == name.value_lower.as_str()
            })?
        } else {
            let index = *input_positions.get(positional)?;
            positional += 1;
            index
        };
        if positions[declared_index].replace(supplied_index).is_some() {
            return None;
        }
    }
    let mut cost = 0_u32;
    for (index, declared) in definition.arguments().iter().enumerate() {
        if declared.mode() == ArgumentMode::Out {
            continue;
        }
        let Some(supplied_index) = positions[index] else {
            if declared.mode() == ArgumentMode::In && declared.default_sql().is_some() {
                continue;
            }
            return None;
        };
        let value = &supplied[supplied_index].1;
        if value.is_null() {
            if !declared.nullable() {
                return None;
            }
            if value.data_type() != DataType::Null
                && value.data_type() != declared.data_type().logical_type()
            {
                return None;
            }
        } else if value.data_type() == declared.data_type().logical_type() {
        } else if matches!(
            (value.data_type(), declared.data_type().logical_type()),
            (DataType::Integer, DataType::Decimal) | (DataType::Date, DataType::Timestamp)
        ) {
            cost = cost.saturating_add(1);
        } else {
            return None;
        }
    }
    Some((positions, cost))
}

fn bind_external_arguments(
    executor: &Executor,
    context: &ExecutionContext,
    candidate: &ExternalCallCandidate<'_>,
    supplied: &[(&CallArgumentSyntax, Value)],
) -> Result<Vec<RuntimeValue>> {
    let mut arguments = Vec::new();
    let mut earlier = std::collections::BTreeMap::new();
    for (index, declared) in candidate.definition.arguments().iter().enumerate() {
        if declared.mode() == ArgumentMode::Out {
            continue;
        }
        let value = if let Some(supplied_index) = candidate.positions[index] {
            supplied[supplied_index]
                .1
                .try_coerce_to_type(declared.data_type().logical_type())?
        } else {
            let default = declared.default_sql().ok_or_else(|| {
                Error::invalid_argument(format!(
                    "required procedure argument '{}' is missing",
                    declared.name().display().as_str()
                ))
            })?;
            evaluate_external_default(executor, context, default.as_str(), &earlier)?
                .try_coerce_to_type(declared.data_type().logical_type())?
        };
        if value.is_null() && !declared.nullable() {
            return Err(Error::invalid_argument(format!(
                "procedure argument '{}' is NOT NULL",
                declared.name().display().as_str()
            )));
        }
        earlier.insert(
            declared.name().normalized().as_str().to_owned(),
            value.clone(),
        );
        arguments.push(RuntimeValue::scalar(value));
    }
    Ok(arguments)
}

fn evaluate_external_default(
    _executor: &Executor,
    context: &ExecutionContext,
    source: &str,
    earlier: &std::collections::BTreeMap<String, Value>,
) -> Result<Value> {
    let mut parser = Parser::new(source);
    let mut expression = parser
        .parse_expression(Precedence::Lowest)
        .ok_or_else(|| Error::parse("procedure default is not an expression"))?;
    if let Some(error) = parser.errors().first() {
        return Err(Error::parse(error.to_string()));
    }
    walk_expression_tree_mut(&mut expression, &mut |node| {
        let Expression::Identifier(identifier) = node else {
            return;
        };
        if let Some(value) = earlier.get(identifier.value_lower()) {
            *node = Expression::BoundValue(Box::new(value.clone()));
        }
    });
    ExpressionEval::compile(&expression, &[])?
        .with_context(context)
        .eval_slice(&Row::new())
}

fn external_call_columns(definition: &RoutineDefinition) -> Vec<String> {
    match definition.result() {
        RoutineResult::Table(columns) => columns
            .iter()
            .map(|column| column.name().display().as_str().to_owned())
            .collect(),
        RoutineResult::Void => definition
            .arguments()
            .iter()
            .filter(|argument| argument.mode() != ArgumentMode::In)
            .map(|argument| argument.name().display().as_str().to_owned())
            .collect(),
        RoutineResult::Scalar { .. } | RoutineResult::Trigger => Vec::new(),
    }
}

pub(super) fn procedural_call_error(error: Diagnostic) -> Error {
    if error.category() == radixdb_procedural::DiagnosticCategory::Security {
        Error::authorization_denied(error.to_string())
    } else {
        Error::invalid_argument(error.to_string())
    }
}

pub(super) fn call_budget(
    context: &ExecutionContext,
    mut policy: ResourcePolicy,
) -> ProceduralResult<BudgetOwner> {
    if context.timeout_ms() > 0 {
        policy.deadline_ms = policy.deadline_ms.min(context.timeout_ms());
    }
    let cancellation: Arc<dyn CancellationProbe> = Arc::new(context.cancellation_handle());
    BudgetOwner::with_parent_cancellation(policy, cancellation)
}

fn validate_principals(principals: PrincipalContext) -> ProceduralResult<()> {
    let all = [
        principals.session_principal,
        principals.invoker_principal,
        principals.effective_principal,
    ];
    if all.contains(&ObjectId::BOOTSTRAP_NAMESPACE) {
        return Err(Diagnostic::new(
            radixdb_procedural::DiagnosticKind::SecurityObjectDenied,
            "namespace identity cannot execute as a principal",
        ));
    }
    Ok(())
}
