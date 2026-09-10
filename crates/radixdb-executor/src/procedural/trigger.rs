use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::Arc;

use radixdb_catalog::{
    CatalogGeneration, CatalogObject, CatalogPayload, ObjectId, ObjectKind, ResourcePolicy,
    SecurityMode, TriggerLevel, TriggerTiming, Volatility, TRIGGER_EVENT_DELETE,
    TRIGGER_EVENT_INSERT, TRIGGER_EVENT_UPDATE,
};
use radixdb_core::{Error, Result, Row, Value};
use radixdb_procedural::{
    compile_trigger_routine, verify, BudgetOwner, CompileIdentity, Diagnostic, DiagnosticKind,
    Interpreter, PrincipalContext, PrincipalHost, RecordField, RuntimeValue, TriggerCompileContext,
    TriggerReturnRecord, VerifiedProgram,
};
use radixdb_sql::{parse_sql, CreateTriggerStatement, Expression, ObjectName, Statement};

use crate::binding::output::OutputBindingExt;
use crate::catalog::security::{
    require_namespace_usage, require_object_privilege, require_principal,
};
use crate::catalog::{
    validate_routine_source_contract, PROCEDURAL_COMPILER_ABI, PROCEDURAL_RUNTIME_ABI,
};
use crate::context::ExecutionContext;
use crate::expression::CompiledEvaluator;
use crate::Executor;

use super::binding::ExecutorSemanticResolver;
use super::call::call_budget;
use super::function::ExecutorStoredFunctionInvoker;
use super::host::ExecutorProceduralHost;
use super::transaction_visible_catalog;

const MAX_TRIGGER_DEPTH: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum DmlTriggerEvent {
    Insert,
    Update,
    Delete,
}

impl DmlTriggerEvent {
    const fn flag(self) -> u16 {
        match self {
            Self::Insert => TRIGGER_EVENT_INSERT,
            Self::Update => TRIGGER_EVENT_UPDATE,
            Self::Delete => TRIGGER_EVENT_DELETE,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

#[derive(Debug, Clone)]
struct TriggerEntry {
    object: CatalogObject,
}

impl TriggerEntry {
    fn payload(&self) -> &radixdb_catalog::TriggerPayload {
        let CatalogPayload::Trigger(payload) = self.object.payload() else {
            unreachable!("trigger plan contains a non-trigger object")
        };
        payload
    }
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct DmlTriggerPlan {
    catalog: Arc<CatalogGeneration>,
    table_id: ObjectId,
    table_name: String,
    record_fields: Vec<RecordField>,
    event: DmlTriggerEvent,
    triggers: Vec<TriggerEntry>,
    budget: BudgetOwner,
    cacheable: bool,
}

impl DmlTriggerPlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.triggers.is_empty()
    }

    pub(crate) fn has_row_triggers(&self) -> bool {
        self.triggers
            .iter()
            .any(|entry| entry.payload().level() == TriggerLevel::Row)
    }

    pub(crate) fn has_after_row_triggers(&self) -> bool {
        self.triggers.iter().any(|entry| {
            entry.payload().level() == TriggerLevel::Row
                && entry.payload().timing() == TriggerTiming::After
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveTriggerKey {
    trigger_id: ObjectId,
    table_id: ObjectId,
    event: DmlTriggerEvent,
    row_identity: Option<i64>,
}

thread_local! {
    static ACTIVE_TRIGGERS: RefCell<Vec<ActiveTriggerKey>> = const { RefCell::new(Vec::new()) };
}

struct ActiveTriggerGuard;

impl ActiveTriggerGuard {
    fn enter(key: ActiveTriggerKey) -> Result<Self> {
        ACTIVE_TRIGGERS.with(|active| {
            let mut active = active.borrow_mut();
            if active.len() >= MAX_TRIGGER_DEPTH {
                return Err(trigger_error(Diagnostic::new(
                    DiagnosticKind::TriggerDepth,
                    "trigger nesting depth exceeded 32",
                )));
            }
            if active.contains(&key) {
                return Err(trigger_error(Diagnostic::new(
                    DiagnosticKind::TriggerCycle,
                    "trigger active-chain cycle detected",
                )));
            }
            active.push(key);
            Ok(Self)
        })
    }
}

impl Drop for ActiveTriggerGuard {
    fn drop(&mut self) {
        ACTIVE_TRIGGERS.with(|active| {
            active.borrow_mut().pop();
        });
    }
}

pub(crate) fn prepare_dml_triggers(
    executor: &Executor,
    table_name: &str,
    event: DmlTriggerEvent,
    updated_columns: &[String],
    context: &ExecutionContext,
) -> Result<DmlTriggerPlan> {
    let (catalog, cacheable) = transaction_visible_catalog(executor)?;
    let table = catalog
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, table_name)
        .map_err(catalog_error)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(table_name.to_owned()))?;
    let table_id = table.id();
    let record_fields = table_record_fields(catalog.as_ref(), table)?;
    let updated_column_ids = updated_columns
        .iter()
        .map(|name| {
            catalog
                .find_column(table_id, name)
                .map_err(catalog_error)?
                .map(CatalogObject::id)
                .ok_or_else(|| Error::ColumnNotFound(name.clone()))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let mut triggers = catalog
        .objects_of_kind(ObjectKind::Trigger)
        .filter(|object| {
            let CatalogPayload::Trigger(payload) = object.payload() else {
                return false;
            };
            payload.table_id() == table_id
                && payload.events() & event.flag() != 0
                && (event != DmlTriggerEvent::Update
                    || payload.update_column_ids().is_empty()
                    || payload
                        .update_column_ids()
                        .iter()
                        .any(|column| updated_column_ids.contains(column)))
        })
        .cloned()
        .map(|object| TriggerEntry { object })
        .collect::<Vec<_>>();
    triggers.sort_by_key(|entry| (entry.payload().priority(), entry.object.id()));
    let budget = context
        .procedural_budget()
        .cloned()
        .map_or_else(|| call_budget(context, ResourcePolicy::default_call()), Ok)
        .map_err(trigger_error)?;
    Ok(DmlTriggerPlan {
        catalog,
        table_id,
        table_name: table_name.to_owned(),
        record_fields,
        event,
        triggers,
        budget,
        cacheable,
    })
}

pub(crate) fn fire_statement_triggers(
    executor: &Executor,
    plan: &DmlTriggerPlan,
    timing: TriggerTiming,
    context: &ExecutionContext,
) -> Result<()> {
    for entry in matching(plan, timing, TriggerLevel::Statement) {
        if !when_matches(executor, plan, entry, None, None, context)? {
            continue;
        }
        let result = execute_entry(executor, plan, entry, None, None, None, context)?;
        if !matches!(result, RuntimeValue::NullRecord) {
            return Err(invalid_return("statement trigger did not return NULL"));
        }
    }
    Ok(())
}

pub(crate) fn fire_before_row_triggers(
    executor: &Executor,
    plan: &DmlTriggerPlan,
    old: Option<&Row>,
    mut new: Option<Row>,
    row_identity: Option<i64>,
    context: &ExecutionContext,
) -> Result<Option<Row>> {
    for entry in matching(plan, TriggerTiming::Before, TriggerLevel::Row) {
        if !when_matches(executor, plan, entry, old, new.as_ref(), context)? {
            continue;
        }
        let result = execute_entry(
            executor,
            plan,
            entry,
            old,
            new.as_ref(),
            row_identity,
            context,
        )?;
        match result {
            RuntimeValue::NullRecord => return Ok(None),
            RuntimeValue::Record(values) => {
                let row = runtime_record_to_row(values, &plan.record_fields)?;
                if plan.event == DmlTriggerEvent::Delete {
                    if old != Some(&row) {
                        return Err(invalid_return(
                            "BEFORE DELETE row trigger returned a value other than OLD",
                        ));
                    }
                } else {
                    new = Some(row);
                }
            }
            RuntimeValue::Scalar(_)
            | RuntimeValue::Collection(_)
            | RuntimeValue::SqlIdentifier(_) => {
                return Err(invalid_return("row trigger returned a non-record value"));
            }
        }
    }
    Ok(match plan.event {
        DmlTriggerEvent::Delete => old.cloned(),
        DmlTriggerEvent::Insert | DmlTriggerEvent::Update => new,
    })
}

pub(crate) fn fire_after_row_triggers(
    executor: &Executor,
    plan: &DmlTriggerPlan,
    old: Option<&Row>,
    new: Option<&Row>,
    row_identity: Option<i64>,
    context: &ExecutionContext,
) -> Result<()> {
    for entry in matching(plan, TriggerTiming::After, TriggerLevel::Row) {
        if !when_matches(executor, plan, entry, old, new, context)? {
            continue;
        }
        let result = execute_entry(executor, plan, entry, old, new, row_identity, context)?;
        if !matches!(result, RuntimeValue::NullRecord) {
            return Err(invalid_return("AFTER row trigger did not return NULL"));
        }
    }
    Ok(())
}

fn matching(
    plan: &DmlTriggerPlan,
    timing: TriggerTiming,
    level: TriggerLevel,
) -> impl Iterator<Item = &TriggerEntry> {
    plan.triggers
        .iter()
        .filter(move |entry| entry.payload().timing() == timing && entry.payload().level() == level)
}

#[allow(clippy::too_many_arguments)]
fn execute_entry(
    executor: &Executor,
    plan: &DmlTriggerPlan,
    entry: &TriggerEntry,
    old: Option<&Row>,
    new: Option<&Row>,
    row_identity: Option<i64>,
    context: &ExecutionContext,
) -> Result<RuntimeValue> {
    authorize_trigger_firing(plan, entry)?;
    let _guard = ActiveTriggerGuard::enter(ActiveTriggerKey {
        trigger_id: entry.object.id(),
        table_id: plan.table_id,
        event: plan.event,
        row_identity,
    })?;
    plan.budget.check_boundary().map_err(trigger_error)?;
    let published = compile_trigger_program(executor, plan, entry)?;
    let mut arguments = Vec::with_capacity(5);
    if entry.payload().level() == TriggerLevel::Row {
        if plan.event != DmlTriggerEvent::Insert {
            arguments.push(row_to_runtime_record(old.ok_or_else(|| {
                Error::internal("OLD row is missing from trigger invocation")
            })?));
        }
        if plan.event != DmlTriggerEvent::Delete {
            arguments.push(row_to_runtime_record(new.ok_or_else(|| {
                Error::internal("NEW row is missing from trigger invocation")
            })?));
        }
    }
    arguments.extend([
        RuntimeValue::scalar(Value::Text(plan.event.name().into())),
        RuntimeValue::scalar(Value::Text(
            match entry.payload().level() {
                TriggerLevel::Row => "ROW",
                TriggerLevel::Statement => "STATEMENT",
            }
            .into(),
        )),
        RuntimeValue::scalar(Value::Text(plan.table_name.clone().into())),
    ]);
    let principals = execution_principals(context);
    // Trigger execution is nested inside a DML statement which already owns
    // the shared catalog fence.
    let nested_executor = executor.fork_with_owned_ddl_fence();
    let mut host = ExecutorProceduralHost::new(
        &nested_executor,
        context,
        principals,
        Some(Volatility::Volatile),
    );
    let use_definer = published.security == SecurityMode::Definer;
    if use_definer {
        host.push_definer(published.owner).map_err(trigger_error)?;
    }
    let execution = Interpreter.execute(&published.program, arguments, &mut host, &plan.budget);
    let cleanup = if use_definer {
        host.pop_definer()
    } else {
        Ok(())
    };
    let outcome = match (execution, cleanup) {
        (Ok(outcome), Ok(())) => outcome,
        (Err(primary), Ok(())) => return Err(trigger_error(primary)),
        (Ok(_), Err(cleanup)) => return Err(trigger_error(cleanup)),
        (Err(primary), Err(cleanup)) => {
            return Err(trigger_error(
                primary.with_detail("definer_cleanup_error", cleanup.to_string()),
            ));
        }
    };
    outcome
        .return_value
        .ok_or_else(|| invalid_return("trigger returned no value"))
}

/// Re-check the attachment owner's authority at every firing boundary. A
/// later REVOKE therefore takes effect on the next statement/catalog
/// generation and cached trigger programs cannot bypass it.
fn authorize_trigger_firing(plan: &DmlTriggerPlan, entry: &TriggerEntry) -> Result<()> {
    let owner = entry.object.owner_principal_id();
    if owner == ObjectId::BOOTSTRAP_OWNER {
        return Ok(());
    }
    require_principal(plan.catalog.as_ref(), owner)?;
    let function = plan
        .catalog
        .object(entry.payload().function_id())
        .ok_or_else(|| Error::invalid_argument("trigger Function is missing"))?;
    require_namespace_usage(plan.catalog.as_ref(), owner, function)?;
    require_object_privilege(
        plan.catalog.as_ref(),
        owner,
        function.id(),
        radixdb_catalog::PRIVILEGE_EXECUTE,
        "EXECUTE",
    )
}

pub(super) struct PublishedTrigger {
    program: VerifiedProgram,
    owner: ObjectId,
    security: SecurityMode,
}

fn compile_trigger_program(
    executor: &Executor,
    plan: &DmlTriggerPlan,
    entry: &TriggerEntry,
) -> Result<Arc<PublishedTrigger>> {
    let function = plan
        .catalog
        .object(entry.payload().function_id())
        .ok_or_else(|| Error::invalid_argument("trigger Function is missing"))?;
    let CatalogPayload::Function(payload) = function.payload() else {
        return Err(Error::invalid_argument("trigger target is not a Function"));
    };
    let definition = payload.procedural_definition().ok_or_else(|| {
        Error::InvalidArgument("native functions cannot be trigger targets".to_owned())
    })?;
    if definition.compiler_abi() != PROCEDURAL_COMPILER_ABI
        || definition.runtime_abi() != PROCEDURAL_RUNTIME_ABI
    {
        return Err(Error::invalid_argument(format!(
            "trigger Function {} requires unsupported compiler/runtime ABI {}/{}",
            function.id(),
            definition.compiler_abi(),
            definition.runtime_abi()
        )));
    }
    let cache_key = trigger_cache_key(plan, entry, function, definition)?;
    if plan.cacheable {
        if let Some(cached) = executor.procedural_cache.get_trigger(&cache_key) {
            verify(cached.program.program().clone()).map_err(trigger_error)?;
            return Ok(cached);
        }
    }
    let mut statements =
        parse_sql(definition.source().as_str()).map_err(|error| Error::Parse(error.to_string()))?;
    let [Statement::CreateRoutine(statement)] = statements.as_mut_slice() else {
        return Err(Error::invalid_argument(
            "trigger Function durable source is not one CREATE FUNCTION",
        ));
    };
    validate_routine_source_contract(statement, function, plan.catalog.as_ref())?;
    let compile_context =
        compile_context_from_payload(entry, plan.event, plan.record_fields.clone());
    let mut resolver = ExecutorSemanticResolver::with_search_path(
        executor,
        plan.catalog.as_ref(),
        definition.search_path().to_vec(),
        Some(Volatility::Volatile),
    );
    let compiled = compile_trigger_routine(
        statement,
        CompileIdentity {
            object_id: function.id(),
            definition_revision: function.definition_revision(),
            display_name: statement.name.to_string(),
        },
        &mut resolver,
        &compile_context,
    )
    .map_err(trigger_error)?;
    let program = verify(compiled.program).map_err(trigger_error)?;
    let published = Arc::new(PublishedTrigger {
        program,
        owner: function.owner_principal_id(),
        security: definition.security(),
    });
    if plan.cacheable {
        Ok(executor
            .procedural_cache
            .insert_trigger(cache_key, published))
    } else {
        Ok(published)
    }
}

fn trigger_cache_key(
    plan: &DmlTriggerPlan,
    entry: &TriggerEntry,
    function: &CatalogObject,
    definition: &radixdb_catalog::RoutineDefinition,
) -> Result<super::cache::TriggerCacheKey> {
    let table = plan
        .catalog
        .object(plan.table_id)
        .ok_or_else(|| Error::invalid_argument("trigger table is missing"))?;
    let mut dependency_ids = definition
        .search_path()
        .iter()
        .chain(definition.dependency_ids())
        .copied()
        .collect::<BTreeSet<_>>();
    dependency_ids.extend(plan.record_fields.iter().filter_map(|field| {
        plan.catalog
            .find_column(plan.table_id, field.name().normalized().as_str())
            .ok()
            .flatten()
            .map(CatalogObject::id)
    }));
    dependency_ids.extend(entry.payload().update_column_ids().iter().copied());
    dependency_ids.extend([entry.object.id(), function.id(), plan.table_id]);
    let dependency_versions = dependency_ids
        .into_iter()
        .map(|id| {
            plan.catalog
                .object(id)
                .map(|object| (id, object.definition_revision()))
                .ok_or_else(|| {
                    Error::invalid_argument(format!("trigger dependency {id} is missing"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let meta = plan.catalog.meta();
    Ok(super::cache::TriggerCacheKey {
        database_id: meta.database_id(),
        catalog_id: meta.catalog_id(),
        catalog_generation: meta.catalog_generation(),
        trigger_id: entry.object.id(),
        trigger_revision: entry.object.definition_revision(),
        function_id: function.id(),
        function_revision: function.definition_revision(),
        source_digest: *definition.source().digest(),
        table_id: plan.table_id,
        table_revision: table.definition_revision(),
        event: plan.event.flag(),
        timing: match entry.payload().timing() {
            TriggerTiming::Before => 0,
            TriggerTiming::After => 1,
        },
        level: match entry.payload().level() {
            TriggerLevel::Row => 0,
            TriggerLevel::Statement => 1,
        },
        dependency_versions,
        compiler_abi: definition.compiler_abi(),
        runtime_abi: definition.runtime_abi(),
    })
}

fn when_matches(
    executor: &Executor,
    plan: &DmlTriggerPlan,
    entry: &TriggerEntry,
    old: Option<&Row>,
    new: Option<&Row>,
    context: &ExecutionContext,
) -> Result<bool> {
    let Some(source) = entry.payload().when_sql() else {
        return Ok(true);
    };
    let expression = parse_when_expression(source.as_str())?;
    let mut names = Vec::new();
    let mut values = Vec::new();
    if let Some(old) = old {
        append_when_record("old", &plan.record_fields, old, &mut names, &mut values)?;
    }
    if let Some(new) = new {
        append_when_record("new", &plan.record_fields, new, &mut names, &mut values)?;
    }
    let nested_executor = executor.fork_with_owned_ddl_fence();
    let invoker = Arc::new(ExecutorStoredFunctionInvoker::new_with_principals(
        &nested_executor,
        context,
        execution_principals(context),
        Some(Volatility::Volatile),
    ));
    let eval_context = context.clone().with_stored_function_invoker(invoker);
    let mut evaluator =
        CompiledEvaluator::new(&executor.function_registry).with_context(&eval_context);
    evaluator.init_columns(&names);
    let row = Row::from_values(values);
    evaluator.set_row_array(&row);
    evaluator.evaluate_bool(&expression)
}

fn parse_when_expression(source: &str) -> Result<Expression> {
    let mut statements =
        parse_sql(&format!("SELECT {source};")).map_err(|error| Error::Parse(error.to_string()))?;
    let [Statement::Select(statement)] = statements.as_mut_slice() else {
        return Err(Error::invalid_argument("trigger WHEN is not an expression"));
    };
    if statement.columns.len() != 1 {
        return Err(Error::invalid_argument(
            "trigger WHEN must contain exactly one expression",
        ));
    }
    Ok(statement.columns.remove(0))
}

fn append_when_record(
    prefix: &str,
    fields: &[RecordField],
    row: &Row,
    names: &mut Vec<String>,
    values: &mut Vec<Value>,
) -> Result<()> {
    if fields.len() != row.len() {
        return Err(Error::internal(
            "trigger row width differs from catalog descriptor",
        ));
    }
    for (field, value) in fields.iter().zip(row.iter()) {
        names.push(format!("{prefix}.{}", field.name().display().as_str()));
        values.push(value.clone());
    }
    Ok(())
}

fn row_to_runtime_record(row: &Row) -> RuntimeValue {
    RuntimeValue::Record(row.iter().cloned().map(Some).collect())
}

fn runtime_record_to_row(values: Vec<Option<Value>>, fields: &[RecordField]) -> Result<Row> {
    if values.len() != fields.len() {
        return Err(invalid_return(
            "trigger returned a record with the wrong width",
        ));
    }
    let values = values
        .into_iter()
        .zip(fields)
        .map(|(value, field)| {
            value.ok_or_else(|| {
                invalid_return(format!(
                    "trigger returned uninitialized field '{}'",
                    field.name().display().as_str()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Row::from_values(values))
}

fn execution_principals(context: &ExecutionContext) -> PrincipalContext {
    PrincipalContext {
        session_principal: context.principal_id(),
        invoker_principal: context.effective_principal_id(),
        effective_principal: context.effective_principal_id(),
    }
}

fn invalid_return(message: impl Into<String>) -> Error {
    trigger_error(Diagnostic::new(
        DiagnosticKind::TriggerInvalidReturn,
        message,
    ))
}

fn trigger_error(error: Diagnostic) -> Error {
    Error::invalid_argument(error.to_string())
}

pub(crate) fn validate_trigger_attachment(
    executor: &Executor,
    statement: &CreateTriggerStatement,
    catalog: &CatalogGeneration,
) -> Result<Vec<ObjectId>> {
    let table = resolve_relation(catalog, &statement.table)?;
    let function_types = statement
        .function
        .argument_types
        .iter()
        .map(|syntax| match syntax {
            radixdb_sql::ProceduralType::Scalar(name) => {
                crate::catalog::bind_catalog_type(name.as_str())
            }
            radixdb_sql::ProceduralType::RowType(_) => Err(Error::InvalidArgument(
                "%ROWTYPE cannot enter a trigger Function signature".to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let function = resolve_function(catalog, &statement.function.name, &function_types)?;
    let CatalogPayload::Function(function_payload) = function.payload() else {
        unreachable!("function lookup returned another catalog kind")
    };
    let definition = function_payload.procedural_definition().ok_or_else(|| {
        Error::InvalidArgument("native functions cannot be trigger targets".to_owned())
    })?;
    if definition.volatility() != Volatility::Volatile
        || !definition.arguments().is_empty()
        || !matches!(definition.result(), radixdb_catalog::RoutineResult::Trigger)
    {
        return Err(Error::InvalidArgument(format!(
            "trigger target '{}' must be a zero-argument VOLATILE Function returning TRIGGER",
            statement.function
        )));
    }

    let fields = table_record_fields(catalog, table)?;
    let mut statements =
        parse_sql(definition.source().as_str()).map_err(|error| Error::Parse(error.to_string()))?;
    let [Statement::CreateRoutine(function_statement)] = statements.as_mut_slice() else {
        return Err(Error::InvalidArgument(
            "trigger function durable source is not one CREATE FUNCTION".to_string(),
        ));
    };
    validate_routine_source_contract(function_statement, function, catalog)?;

    let mut dependencies = BTreeSet::from([table.id(), function.id()]);
    if let Some(when) = &statement.when {
        validate_when_names(statement, when)?;
    }
    for event in trigger_events(statement) {
        if let Some(when) = &statement.when {
            validate_when_expression(executor, statement, when, &fields, event)?;
        }
        let context = compile_context(statement, event, fields.clone());
        let mut resolver = ExecutorSemanticResolver::with_search_path(
            executor,
            catalog,
            definition.search_path().to_vec(),
            Some(Volatility::Volatile),
        );
        let compiled = compile_trigger_routine(
            function_statement,
            CompileIdentity {
                object_id: function.id(),
                definition_revision: function.definition_revision(),
                display_name: function_statement.name.to_string(),
            },
            &mut resolver,
            &context,
        )
        .map_err(|diagnostic| {
            Error::InvalidArgument(format!(
                "trigger attachment compilation failed: {diagnostic}"
            ))
        })?;
        verify(compiled.program).map_err(|diagnostic| {
            Error::InvalidArgument(format!(
                "trigger attachment verification failed: {diagnostic}"
            ))
        })?;
        dependencies.extend(compiled.dependencies);
    }
    Ok(dependencies.into_iter().collect())
}

fn validate_when_expression(
    executor: &Executor,
    statement: &CreateTriggerStatement,
    expression: &Expression,
    fields: &[RecordField],
    event: u16,
) -> Result<()> {
    let (catalog, _) = super::transaction_visible_catalog(executor)?;
    let row = matches!(statement.level, radixdb_sql::TriggerLevelSyntax::Row);
    let mut bound = expression.clone();
    let mut next_parameter = 1usize;
    let mut error = None;
    radixdb_sql::walk_expression_tree_mut(&mut bound, &mut |node| {
        let Expression::QualifiedIdentifier(identifier) = node else {
            return;
        };
        if identifier.is_multi_part_path() {
            return;
        }
        let qualifier = identifier.qualifier.value_lower.as_str();
        if !matches!(qualifier, "old" | "new") {
            return;
        }
        if !row
            || (qualifier == "old" && event == TRIGGER_EVENT_INSERT)
            || (qualifier == "new" && event == TRIGGER_EVENT_DELETE)
        {
            error = Some(Error::invalid_argument(format!(
                "trigger WHEN cannot reference {} for this event/level",
                qualifier.to_uppercase()
            )));
            return;
        }
        let field_name = identifier.name.value_lower.as_str();
        let Some(field) = fields
            .iter()
            .find(|field| field.name().normalized().as_str() == field_name)
        else {
            error = Some(Error::ColumnNotFound(format!("{qualifier}.{field_name}")));
            return;
        };
        *node = super::binding_types::typed_parameter_in_catalog(
            next_parameter,
            field.data_type(),
            identifier.token.clone(),
            catalog.as_ref(),
        );
        next_parameter += 1;
    });
    if let Some(error) = error {
        return Err(error);
    }
    let (data_type, _) = executor.bind_scalar_output(&bound)?;
    if data_type != radixdb_core::DataType::Boolean {
        return Err(Error::invalid_argument("trigger WHEN must be BOOLEAN"));
    }
    Ok(())
}

fn validate_when_names(statement: &CreateTriggerStatement, expression: &Expression) -> Result<()> {
    let row = matches!(statement.level, radixdb_sql::TriggerLevelSyntax::Row);
    let mut invalid = None;
    radixdb_sql::walk_expression_tree(expression, &mut |node| {
        let Expression::QualifiedIdentifier(identifier) = node else {
            return;
        };
        let qualifier = identifier.qualifier.value_lower.as_str();
        if matches!(qualifier, "old" | "new") && !row {
            invalid = Some("statement trigger WHEN cannot reference OLD or NEW");
        }
        if qualifier == "old"
            && statement
                .events
                .iter()
                .all(|event| matches!(event, radixdb_sql::TriggerEventSyntax::Insert))
        {
            invalid = Some("INSERT trigger WHEN cannot reference OLD");
        }
        if qualifier == "new"
            && statement
                .events
                .iter()
                .all(|event| matches!(event, radixdb_sql::TriggerEventSyntax::Delete))
        {
            invalid = Some("DELETE trigger WHEN cannot reference NEW");
        }
    });
    invalid.map_or(Ok(()), |message| Err(Error::invalid_argument(message)))
}

fn trigger_events(statement: &CreateTriggerStatement) -> Vec<u16> {
    statement
        .events
        .iter()
        .map(|event| match event {
            radixdb_sql::TriggerEventSyntax::Insert => TRIGGER_EVENT_INSERT,
            radixdb_sql::TriggerEventSyntax::Update { .. } => TRIGGER_EVENT_UPDATE,
            radixdb_sql::TriggerEventSyntax::Delete => TRIGGER_EVENT_DELETE,
        })
        .collect()
}

fn compile_context(
    statement: &CreateTriggerStatement,
    event: u16,
    record_fields: Vec<RecordField>,
) -> TriggerCompileContext {
    let row = matches!(statement.level, radixdb_sql::TriggerLevelSyntax::Row);
    let before = matches!(statement.timing, radixdb_sql::TriggerTimingSyntax::Before);
    trigger_compile_context(row, before, event, record_fields)
}

fn compile_context_from_payload(
    entry: &TriggerEntry,
    event: DmlTriggerEvent,
    record_fields: Vec<RecordField>,
) -> TriggerCompileContext {
    trigger_compile_context(
        entry.payload().level() == TriggerLevel::Row,
        entry.payload().timing() == TriggerTiming::Before,
        event.flag(),
        record_fields,
    )
}

fn trigger_compile_context(
    row: bool,
    before: bool,
    event: u16,
    record_fields: Vec<RecordField>,
) -> TriggerCompileContext {
    let old_available = row && event != TRIGGER_EVENT_INSERT;
    let new_available = row && event != TRIGGER_EVENT_DELETE;
    let return_record = if before && row {
        Some(if event == TRIGGER_EVENT_DELETE {
            TriggerReturnRecord::Old
        } else {
            TriggerReturnRecord::New
        })
    } else {
        None
    };
    TriggerCompileContext {
        record_fields,
        old_available,
        new_available,
        new_writable: before && new_available,
        return_record,
    }
}

fn table_record_fields(
    catalog: &CatalogGeneration,
    table: &radixdb_catalog::CatalogObject,
) -> Result<Vec<RecordField>> {
    let CatalogPayload::Table(payload) = table.payload() else {
        return Err(Error::InvalidArgument(
            "trigger target is not a table".to_string(),
        ));
    };
    payload
        .column_ids()
        .iter()
        .map(|column_id| {
            let column = catalog.object(*column_id).ok_or_else(|| {
                Error::InvalidArgument("trigger table column is missing".to_string())
            })?;
            let CatalogPayload::Column(payload) = column.payload() else {
                return Err(Error::InvalidArgument(
                    "trigger table child is not a column".to_string(),
                ));
            };
            Ok(RecordField::new(
                column.name().clone(),
                payload.data_type(),
                payload.nullable(),
            ))
        })
        .collect()
}

fn resolve_relation<'a>(
    catalog: &'a CatalogGeneration,
    name: &ObjectName,
) -> Result<&'a radixdb_catalog::CatalogObject> {
    let (namespace, name) = resolve_scope(catalog, name)?;
    catalog
        .find_relation(namespace, name)
        .map_err(catalog_error)?
        .filter(|object| object.kind() == ObjectKind::Table)
        .ok_or_else(|| Error::TableNotFound(name.to_string()))
}

fn resolve_function<'a>(
    catalog: &'a CatalogGeneration,
    name: &ObjectName,
    input_types: &[radixdb_catalog::CatalogDataType],
) -> Result<&'a radixdb_catalog::CatalogObject> {
    let (namespace, name) = resolve_scope(catalog, name)?;
    catalog
        .find_routine(namespace, ObjectKind::Function, name, input_types)
        .map_err(catalog_error)?
        .ok_or_else(|| Error::InvalidArgument(format!("trigger function '{name}' does not exist")))
}

fn resolve_scope<'a>(
    catalog: &CatalogGeneration,
    name: &'a ObjectName,
) -> Result<(ObjectId, &'a str)> {
    let (last, path) = name
        .components
        .split_last()
        .ok_or_else(|| Error::InvalidArgument("catalog name is empty".to_string()))?;
    let namespace = if path.is_empty() {
        ObjectId::BOOTSTRAP_NAMESPACE
    } else {
        crate::catalog::resolve_namespace_path(
            catalog,
            path.iter().map(|component| component.value.as_str()),
        )?
    };
    Ok((namespace, last.value.as_str()))
}

fn catalog_error(error: radixdb_catalog::CatalogError) -> Error {
    Error::InvalidArgument(error.to_string())
}
