use std::collections::BTreeMap;
use std::sync::Arc;

use radixdb_catalog::{CatalogDataType, CatalogPayload, ObjectId, ObjectKind, RoutineDefinition};
use radixdb_core::{DataType, Error, LogicalTypeRef, Result, Value};
use radixdb_plugin_host::RegisteredTypeRef;
use radixdb_procedural::{Interpreter, PrincipalContext, PrincipalHost, RuntimeValue};
use radixdb_sql::{
    walk_expression_tree_mut, walk_statement_tree, Expression, Parser, Precedence, Statement,
};
use radixdb_storage::traits::{
    AliasedResult, DeferredRow, QueryResult, TypedBatchFallbackReason, TypedColumnBatch,
};
use rustc_hash::FxHashMap;

use crate::context::{ExecutionContext, StoredFunctionInvoker};
use crate::expression::ExpressionEval;
use crate::Executor;

use super::call::{call_budget, CallBoundary};
pub(crate) use super::function_binding::{
    bind_stored_function_dependency, bind_stored_function_result,
};
use super::function_binding::{resolve_candidate, resolve_name, CandidateDefinition};
use super::host::ExecutorProceduralHost;
use super::{load_published_routine, transaction_visible_catalog};

pub(crate) fn statement_calls_stored_function(
    executor: &Executor,
    statement: &Statement,
) -> Result<bool> {
    if !matches!(
        statement,
        Statement::Select(_) | Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    ) {
        return Ok(false);
    }
    let mut names = Vec::new();
    let mut calls_operator = false;
    walk_statement_tree(statement, &mut |expression| match expression {
        Expression::FunctionCall(function)
            if !executor.function_registry.exists(&function.function) =>
        {
            names.push(function.function.to_string());
        }
        Expression::Infix(infix) if infix.op_type == radixdb_sql::InfixOperator::Other => {
            calls_operator = true;
        }
        _ => {}
    });
    if calls_operator {
        return Ok(true);
    }
    if names.is_empty() {
        return Ok(false);
    }
    let (catalog, _) = transaction_visible_catalog(executor)?;
    for name in names {
        let (namespace, routine_name) = resolve_name(catalog.as_ref(), &name)?;
        if catalog.objects_of_kind(ObjectKind::Function).any(|object| {
            object.namespace_id() == Some(namespace)
                && object
                    .name()
                    .normalized()
                    .as_str()
                    .eq_ignore_ascii_case(routine_name)
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) struct ExecutorStoredFunctionInvoker {
    executor: Executor,
    context: ExecutionContext,
    principals: PrincipalContext,
    caller_volatility: Option<radixdb_catalog::Volatility>,
}

impl ExecutorStoredFunctionInvoker {
    pub(crate) fn new(executor: &Executor, context: &ExecutionContext) -> Self {
        Self {
            // The invoker is installed for a statement whose dispatch owns
            // the shared catalog fence. All function SQL leaves reuse it.
            executor: executor.fork_with_owned_ddl_fence(),
            context: context.clone(),
            principals: PrincipalContext {
                session_principal: context.principal_id(),
                invoker_principal: context.effective_principal_id(),
                effective_principal: context.effective_principal_id(),
            },
            caller_volatility: None,
        }
    }

    pub(super) fn new_with_principals(
        executor: &Executor,
        context: &ExecutionContext,
        principals: PrincipalContext,
        caller_volatility: Option<radixdb_catalog::Volatility>,
    ) -> Self {
        Self {
            executor: executor.fork_for_stored_function(),
            context: context.clone(),
            principals,
            caller_volatility,
        }
    }
}

impl std::fmt::Debug for ExecutorStoredFunctionInvoker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutorStoredFunctionInvoker")
            .field("database", &self.context.current_database())
            .field("principals", &self.principals)
            .finish_non_exhaustive()
    }
}

impl StoredFunctionInvoker for ExecutorStoredFunctionInvoker {
    fn invoke(self: Arc<Self>, name: &str, arguments: &[Value]) -> Result<Value> {
        if let Some(symbol) = name.strip_prefix(crate::context::STORED_OPERATOR_CALL_PREFIX) {
            return self.invoke_operator(symbol, arguments);
        }
        let argument_types = arguments
            .iter()
            .map(|value| {
                (!matches!(value, Value::Null(DataType::Null))).then(|| value.logical_type())
            })
            .collect::<Vec<_>>();
        let (catalog, _) = transaction_visible_catalog(&self.executor)?;
        let candidate =
            resolve_candidate(catalog.as_ref(), name, &argument_types)?.ok_or_else(|| {
                Error::invalid_argument(format!(
                    "stored function {name} does not exist for supplied argument types"
                ))
            })?;
        if !volatility_allows(self.caller_volatility, candidate.definition.volatility()) {
            return Err(Error::invalid_argument(format!(
                "{:?} function cannot invoke {:?} function {name}",
                self.caller_volatility
                    .expect("restricted caller volatility"),
                candidate.definition.volatility()
            )));
        }
        let object_id = candidate.object.id();
        crate::authorization::authorize_routine_invocation(
            &self.executor,
            self.principals.session_principal,
            self.principals.effective_principal,
            object_id,
        )?;
        match candidate.definition {
            CandidateDefinition::Procedural(definition) => {
                let definition = definition.clone();
                let budget = match self.context.procedural_budget() {
                    Some(budget) => budget.clone(),
                    None => call_budget(&self.context, definition.resource_policy())
                        .map_err(procedural_error)?,
                };
                let nested_context = self.context.clone().with_procedural_budget(budget);
                let nested_invoker = Arc::new(Self::new_with_principals(
                    &self.executor,
                    &nested_context,
                    self.principals,
                    Some(definition.volatility()),
                ));
                let nested_context =
                    nested_context.with_stored_function_invoker(nested_invoker.clone());
                let values = bind_runtime_arguments(nested_invoker, definition, arguments)?;
                self.executor.execute_function_object(
                    object_id,
                    values,
                    &nested_context,
                    self.principals,
                )
            }
            CandidateDefinition::Native(definition) => super::native_function::invoke(
                &self.executor,
                &self.context,
                name,
                object_id,
                definition,
                arguments,
            ),
        }
    }

    fn external_equal(&self, left: &Value, right: &Value) -> Result<bool> {
        super::external::equal(&self.executor, left, right)
    }

    fn external_compare(&self, left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
        super::external::compare(&self.executor, left, right)
    }

    fn external_input(&self, type_name: &str, input: &Value) -> Result<Value> {
        super::external::input(&self.executor, type_name, input)
    }

    fn external_output(&self, value: &Value, target_type: DataType) -> Result<Value> {
        super::external::output(&self.executor, value, target_type)
    }
}

impl ExecutorStoredFunctionInvoker {
    fn invoke_operator(&self, symbol: &str, arguments: &[Value]) -> Result<Value> {
        let [left, right] = arguments else {
            return Err(Error::invalid_argument(
                "catalog operator invocation requires exactly two operands",
            ));
        };
        let left_type = catalog_type_for_value(left)?;
        let right_type = catalog_type_for_value(right)?;
        let (catalog, _) = transaction_visible_catalog(&self.executor)?;
        let preferred_namespace = left_type
            .and_then(CatalogDataType::type_object_id)
            .or_else(|| right_type.and_then(CatalogDataType::type_object_id))
            .and_then(|id| catalog.object(id).and_then(|object| object.namespace_id()))
            .unwrap_or(ObjectId::BOOTSTRAP_NAMESPACE);
        let mut candidates = catalog
            .objects_of_kind(ObjectKind::Operator)
            .filter(|object| object.namespace_id() == Some(preferred_namespace))
            .filter_map(|object| {
                let CatalogPayload::Operator(payload) = object.payload() else {
                    return None;
                };
                (payload.symbol() == symbol
                    && left_type.is_none_or(|actual| payload.left_argument() == Some(actual))
                    && right_type.is_none_or(|actual| payload.right_argument() == Some(actual)))
                .then_some((object, payload))
            })
            .collect::<Vec<_>>();
        if candidates.len() != 1 {
            return Err(Error::invalid_argument(if candidates.is_empty() {
                format!("operator '{symbol}' does not exist for supplied operand types")
            } else {
                format!("operator '{symbol}' is ambiguous for supplied NULL operand types")
            }));
        }
        let (operator, payload) = candidates.pop().expect("one operator candidate");
        let descriptor = self
            .executor
            .plugin_registry
            .operator(&operator.id().into_bytes())
            .ok_or_else(|| {
                Error::invalid_argument(format!(
                    "operator {} is missing from the immutable plugin registry",
                    operator.id()
                ))
            })?;
        if descriptor.semantic_revision != payload.semantic_revision()
            || descriptor.symbol != payload.symbol()
            || descriptor.function_id != payload.backing_function_id().into_bytes()
            || !registered_type_matches(descriptor.left, payload.left_argument())
            || !registered_type_matches(Some(descriptor.right), payload.right_argument())
            || !registered_type_matches(Some(descriptor.result), Some(payload.result_type()))
        {
            return Err(Error::invalid_argument(format!(
                "operator {} descriptor is stale for this catalog generation",
                operator.id()
            )));
        }
        let function = catalog
            .object(payload.backing_function_id())
            .ok_or_else(|| Error::internal("operator backing function disappeared"))?;
        let CatalogPayload::Function(function_payload) = function.payload() else {
            return Err(Error::internal("operator backing object is not a function"));
        };
        let definition = function_payload
            .native_definition()
            .ok_or_else(|| Error::internal("operator backing function is not native"))?;
        crate::authorization::authorize_routine_invocation(
            &self.executor,
            self.principals.session_principal,
            self.principals.effective_principal,
            function.id(),
        )?;
        super::native_function::invoke(
            &self.executor,
            &self.context,
            symbol,
            function.id(),
            definition,
            arguments,
        )
    }
}

fn catalog_type_for_value(value: &Value) -> Result<Option<CatalogDataType>> {
    if matches!(value, Value::Null(DataType::Null)) {
        return Ok(None);
    }
    if let Some(external) = value.as_external() {
        return CatalogDataType::external(
            ObjectId::from_user_bytes(external.type_ref().type_object_id())
                .map_err(|error| Error::internal(error.to_string()))?,
            external.type_ref().codec_version(),
        )
        .map(Some)
        .map_err(|error| Error::internal(error.to_string()));
    }
    let LogicalTypeRef::Builtin(data_type) = value.logical_type() else {
        return Err(Error::internal(
            "external value passed built-in catalog type conversion",
        ));
    };
    CatalogDataType::scalar(data_type)
        .map(Some)
        .map_err(|error| Error::internal(error.to_string()))
}

fn registered_type_matches(
    registered: Option<RegisteredTypeRef>,
    catalog: Option<CatalogDataType>,
) -> bool {
    match (registered, catalog) {
        (None, None) => true,
        (Some(RegisteredTypeRef::Builtin(tag)), Some(catalog)) => u8::try_from(tag)
            .ok()
            .and_then(DataType::from_u8)
            .is_some_and(|data_type| {
                catalog.logical_type_ref() == LogicalTypeRef::Builtin(data_type)
            }),
        (
            Some(RegisteredTypeRef::External {
                object_id,
                codec_version,
            }),
            Some(catalog),
        ) => {
            catalog.type_object_id().map(ObjectId::into_bytes) == Some(object_id)
                && catalog.parameter_1() == codec_version
        }
        _ => false,
    }
}

fn volatility_allows(
    caller: Option<radixdb_catalog::Volatility>,
    target: radixdb_catalog::Volatility,
) -> bool {
    use radixdb_catalog::Volatility;
    match caller {
        None | Some(Volatility::Volatile) => true,
        Some(Volatility::Stable) => target != Volatility::Volatile,
        Some(Volatility::Immutable) => target == Volatility::Immutable,
    }
}

fn bind_runtime_arguments(
    invoker: Arc<ExecutorStoredFunctionInvoker>,
    definition: RoutineDefinition,
    supplied: &[Value],
) -> Result<Vec<RuntimeValue>> {
    let mut values = Vec::with_capacity(definition.arguments().len());
    let mut named = BTreeMap::<String, Value>::new();
    for (index, declared) in definition.arguments().iter().enumerate() {
        let value = if let Some(value) = supplied.get(index) {
            value.try_coerce_to_type(declared.data_type().logical_type())?
        } else {
            let source = declared.default_sql().ok_or_else(|| {
                Error::invalid_argument(format!(
                    "required function argument '{}' is missing",
                    declared.name().display().as_str()
                ))
            })?;
            evaluate_default(Arc::clone(&invoker), source.as_str(), &named)?
                .try_coerce_to_type(declared.data_type().logical_type())?
        };
        if value.is_null() && !declared.nullable() {
            return Err(Error::invalid_argument(format!(
                "function argument '{}' is NOT NULL",
                declared.name().display().as_str()
            )));
        }
        named.insert(
            declared.name().normalized().as_str().to_owned(),
            value.clone(),
        );
        values.push(RuntimeValue::scalar(value));
    }
    Ok(values)
}

fn evaluate_default(
    invoker: Arc<ExecutorStoredFunctionInvoker>,
    source: &str,
    earlier: &BTreeMap<String, Value>,
) -> Result<Value> {
    let mut parser = Parser::new(source);
    let mut expression = parser
        .parse_expression(Precedence::Lowest)
        .ok_or_else(|| Error::parse("stored function default is not an expression"))?;
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
    let context = invoker
        .context
        .clone()
        .with_stored_function_invoker(invoker);
    ExpressionEval::compile(&expression, &[])?
        .with_context(&context)
        .eval_slice(&radixdb_core::Row::new())
}

impl Executor {
    fn execute_function_object(
        &self,
        object_id: ObjectId,
        arguments: Vec<RuntimeValue>,
        context: &ExecutionContext,
        principals: PrincipalContext,
    ) -> Result<Value> {
        if !self.ddl_fence_already_held {
            let _fence = self.engine.acquire_ddl_statement_fence(false);
            return self
                .fork_with_owned_ddl_fence()
                .execute_function_object(object_id, arguments, context, principals);
        }
        let boundary = self.begin_procedural_boundary()?;
        let published = match load_published_routine(self, object_id, ObjectKind::Function) {
            Ok(published) => published,
            Err(error) => {
                let _ = self.abort_procedural_boundary(&boundary);
                return Err(error);
            }
        };
        let budget = match context.procedural_budget() {
            Some(budget) => budget.clone(),
            None => match call_budget(context, published.resource_policy) {
                Ok(budget) => budget,
                Err(error) => {
                    let _ = self.abort_procedural_boundary(&boundary);
                    return Err(procedural_error(error));
                }
            },
        };
        let mut host =
            ExecutorProceduralHost::new(self, context, principals, Some(published.volatility));
        let use_definer = published.security == radixdb_catalog::SecurityMode::Definer;
        if use_definer {
            if let Err(error) = host.push_definer(published.owner) {
                let _ = self.abort_procedural_boundary(&boundary);
                return Err(procedural_error(error));
            }
        }
        let execution = Interpreter.execute(&published.program, arguments, &mut host, &budget);
        let cleanup = if use_definer {
            host.pop_definer()
        } else {
            Ok(())
        };
        let outcome = match (execution, cleanup) {
            (Ok(outcome), Ok(())) => outcome,
            (Err(error), Ok(())) | (Ok(_), Err(error)) => {
                let _ = self.abort_procedural_boundary(&boundary);
                return Err(procedural_error(error));
            }
            (Err(primary), Err(cleanup)) => {
                let _ = self.abort_procedural_boundary(&boundary);
                return Err(Error::invalid_argument(format!(
                    "{primary}; definer cleanup also failed: {cleanup}"
                )));
            }
        };
        let value = match outcome.return_value {
            Some(value) => value,
            None => {
                let error =
                    Error::invalid_argument("stored scalar function returned without a value");
                let _ = self.abort_procedural_boundary(&boundary);
                return Err(error);
            }
        };
        let value = match value {
            RuntimeValue::Scalar(value) => Ok(value),
            RuntimeValue::Record(_)
            | RuntimeValue::NullRecord
            | RuntimeValue::Collection(_)
            | RuntimeValue::SqlIdentifier(_) => Err(Error::invalid_argument(
                "stored scalar function returned a non-scalar value",
            )),
        };
        match value {
            Ok(value) => match self.complete_procedural_boundary(&boundary) {
                Ok(()) => Ok(value),
                Err(error) => {
                    let _ = self.abort_procedural_boundary(&boundary);
                    Err(error)
                }
            },
            Err(error) => {
                let _ = self.abort_procedural_boundary(&boundary);
                Err(error)
            }
        }
    }
}

pub(crate) fn wrap_function_statement_result(
    executor: &Executor,
    inner: Box<dyn QueryResult>,
    boundary: CallBoundary,
) -> Box<dyn QueryResult> {
    Box::new(FunctionStatementResult {
        inner,
        executor: executor.fork_for_stored_function(),
        boundary: Some(boundary),
        pending_error: None,
    })
}

struct FunctionStatementResult {
    inner: Box<dyn QueryResult>,
    executor: Executor,
    boundary: Option<CallBoundary>,
    pending_error: Option<Error>,
}

impl FunctionStatementResult {
    fn finish(&mut self, success: bool) -> Result<()> {
        let Some(boundary) = self.boundary.take() else {
            return Ok(());
        };
        if success {
            match self.executor.complete_procedural_boundary(&boundary) {
                Ok(()) => Ok(()),
                Err(error) => {
                    let _ = self.executor.abort_procedural_boundary(&boundary);
                    Err(error)
                }
            }
        } else {
            self.executor.abort_procedural_boundary(&boundary)
        }
    }

    fn finish_at_eof(&mut self) {
        if self.boundary.is_none() {
            return;
        }
        if let Some(error) = self.inner.last_error() {
            let _ = self.finish(false);
            self.pending_error = Some(error);
            return;
        }
        if let Err(error) = self.inner.close().and_then(|()| self.finish(true)) {
            self.pending_error = Some(error);
        }
    }
}

impl Drop for FunctionStatementResult {
    fn drop(&mut self) {
        if self.boundary.is_some() {
            let _ = self.inner.close();
            let _ = self.finish(false);
        }
    }
}

impl QueryResult for FunctionStatementResult {
    fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    fn columns_arc(&self) -> Option<radixdb_core::CompactArc<Vec<String>>> {
        self.inner.columns_arc()
    }

    fn next(&mut self) -> bool {
        if self.boundary.is_none() || self.pending_error.is_some() {
            return false;
        }
        if self.inner.next() {
            true
        } else {
            self.finish_at_eof();
            false
        }
    }

    fn scan(&self, destination: &mut [Value]) -> Result<()> {
        self.inner.scan(destination)
    }

    fn row(&self) -> &radixdb_core::Row {
        self.inner.row()
    }

    fn take_row(&mut self) -> radixdb_core::Row {
        self.inner.take_row()
    }

    fn take_deferred_row(&mut self) -> DeferredRow {
        self.inner.take_deferred_row()
    }

    fn preserves_deferred_rows(&self) -> bool {
        self.inner.preserves_deferred_rows()
    }

    fn ascending_nulls_last_ordering(&self) -> Option<Vec<usize>> {
        self.inner.ascending_nulls_last_ordering()
    }

    fn close(&mut self) -> Result<()> {
        let inner = self.inner.close();
        let boundary = self.finish(false);
        inner.and(boundary)
    }

    fn rows_affected(&self) -> i64 {
        self.inner.rows_affected()
    }

    fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    fn try_into_arc_rows(&mut self) -> Option<radixdb_core::CompactArc<Vec<radixdb_core::Row>>> {
        let rows = self.inner.try_into_arc_rows();
        if rows.is_some() {
            if let Err(error) = self.finish(true) {
                self.pending_error = Some(error);
                return None;
            }
        }
        rows
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner.estimated_count()
    }

    fn supports_typed_batches(&self) -> bool {
        self.inner.supports_typed_batches()
    }

    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        self.inner.typed_batch_fallback_reason()
    }

    fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        if self.boundary.is_none() {
            return self.pending_error.take().map_or(Ok(None), Err);
        }
        match self.inner.next_typed_batch() {
            Ok(Some(batch)) => Ok(Some(batch)),
            Ok(None) => {
                self.finish_at_eof();
                self.pending_error.take().map_or(Ok(None), Err)
            }
            Err(error) => {
                let _ = self.finish(false);
                Err(error)
            }
        }
    }

    fn last_error(&mut self) -> Option<Error> {
        self.pending_error
            .take()
            .or_else(|| self.inner.last_error())
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

fn procedural_error(error: radixdb_procedural::Diagnostic) -> Error {
    if error.category() == radixdb_procedural::DiagnosticCategory::Security {
        Error::authorization_denied(error.to_string())
    } else {
        Error::invalid_argument(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_storage::mvcc::engine::MVCCEngine;

    fn executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    fn scalar(executor: &Executor, sql: &str) -> Value {
        let mut result = executor.execute(sql).unwrap();
        assert!(result.next());
        let value = result.row()[0].clone();
        assert!(!result.next());
        assert!(result.last_error().is_none());
        result.close().unwrap();
        value
    }

    #[test]
    fn sql_expression_invokes_durable_function() {
        let executor = executor();
        executor
            .execute(
                "CREATE FUNCTION increment(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
                 BEGIN RETURN input_value + 1; END;",
            )
            .unwrap();
        assert_eq!(
            scalar(&executor, "SELECT increment(41)"),
            Value::Integer(42)
        );
        let described = executor
            .describe_query_output("SELECT increment(41) AS answer")
            .unwrap()
            .unwrap();
        assert_eq!(described.len(), 1);
        assert_eq!(described[0].name, "answer");
        assert_eq!(described[0].data_type, DataType::Integer);
        assert!(!described[0].nullable);
    }

    #[test]
    fn sql_function_uses_defaults_and_exact_overload() {
        let executor = executor();
        executor
            .execute(
                "CREATE FUNCTION choose(input_value INTEGER NOT NULL DEFAULT 7) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
                 BEGIN RETURN input_value; END;",
            )
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION choose(input_value TEXT) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
                 BEGIN RETURN 99; END;",
            )
            .unwrap();
        assert_eq!(scalar(&executor, "SELECT choose()"), Value::Integer(7));
        assert_eq!(scalar(&executor, "SELECT choose(3)"), Value::Integer(3));
        assert_eq!(scalar(&executor, "SELECT choose('x')"), Value::Integer(99));
    }

    #[test]
    fn volatile_function_dml_commits_atomically() {
        let executor = executor();
        executor
            .execute("CREATE TABLE function_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION write_one(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX VOLATILE SECURITY INVOKER AS \
                 BEGIN INSERT INTO function_effects VALUES (:input_value); \
                 RETURN input_value; END;",
            )
            .unwrap();
        assert_eq!(scalar(&executor, "SELECT write_one(5)"), Value::Integer(5));
        assert_eq!(
            scalar(&executor, "SELECT COUNT(*) FROM function_effects"),
            Value::Integer(1)
        );
    }

    #[test]
    fn volatile_function_rolls_back_all_writes_on_expression_error() {
        let executor = executor();
        executor
            .execute("CREATE TABLE function_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION write_then_fail(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX VOLATILE SECURITY INVOKER AS \
                 BEGIN INSERT INTO function_effects VALUES (:input_value); \
                 RETURN 1 / 0; END;",
            )
            .unwrap();

        let error = match executor.execute("SELECT write_then_fail(5)") {
            Ok(_) => panic!("failing stored function unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(!error.to_string().is_empty());
        assert_eq!(
            scalar(&executor, "SELECT COUNT(*) FROM function_effects"),
            Value::Integer(0)
        );
    }

    #[test]
    fn stable_function_cannot_write_through_dynamic_sql() {
        let executor = executor();
        executor
            .execute("CREATE TABLE function_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION forbidden_dynamic(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX STABLE SECURITY INVOKER AS \
                 BEGIN EXECUTE 'INSERT INTO function_effects VALUES (?)' \
                 USING input_value; RETURN input_value; END;",
            )
            .unwrap();

        let error = match executor.execute("SELECT forbidden_dynamic(5)") {
            Ok(_) => panic!("STABLE dynamic DML unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("IMMUTABLE and STABLE functions cannot execute DML"));
        assert_eq!(
            scalar(&executor, "SELECT COUNT(*) FROM function_effects"),
            Value::Integer(0)
        );
    }

    #[test]
    fn stable_function_cannot_call_procedure_through_dynamic_sql() {
        let executor = executor();
        executor
            .execute("CREATE TABLE function_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE PROCEDURE write_effect(input_value INTEGER NOT NULL) \
                 LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 INSERT INTO function_effects VALUES (:input_value); END;",
            )
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION forbidden_dynamic_call(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX STABLE SECURITY INVOKER AS \
                 BEGIN EXECUTE 'CALL write_effect(?)' USING input_value; \
                 RETURN input_value; END;",
            )
            .unwrap();

        let error = match executor.execute("SELECT forbidden_dynamic_call(5)") {
            Ok(_) => panic!("STABLE dynamic CALL unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("IMMUTABLE and STABLE functions cannot call procedures"));
        assert_eq!(
            scalar(&executor, "SELECT COUNT(*) FROM function_effects"),
            Value::Integer(0)
        );
    }

    #[test]
    fn stable_function_cannot_invoke_volatile_function() {
        let executor = executor();
        executor
            .execute("CREATE TABLE function_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION volatile_write(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX VOLATILE SECURITY INVOKER AS \
                 BEGIN INSERT INTO function_effects VALUES (:input_value); \
                 RETURN input_value; END;",
            )
            .unwrap();
        let error = match executor.execute(
            "CREATE FUNCTION stable_wrapper(input_value INTEGER NOT NULL) \
             RETURNS INTEGER LANGUAGE RADIX STABLE SECURITY INVOKER AS \
             BEGIN RETURN volatile_write(input_value); END;",
        ) {
            Ok(_) => panic!("STABLE to VOLATILE definition unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("Stable function cannot invoke Volatile stored function"));
        assert_eq!(
            scalar(&executor, "SELECT COUNT(*) FROM function_effects"),
            Value::Integer(0)
        );
    }

    #[test]
    fn nested_stored_function_calls_share_the_statement_boundary() {
        let executor = executor();
        executor
            .execute(
                "CREATE FUNCTION increment(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
                 BEGIN RETURN input_value + 1; END;",
            )
            .unwrap();
        assert_eq!(
            scalar(&executor, "SELECT increment(increment(40))"),
            Value::Integer(42)
        );
    }

    #[test]
    fn closing_function_select_early_rolls_back_statement_effects() {
        let executor = executor();
        executor
            .execute("CREATE TABLE function_source (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute("INSERT INTO function_source VALUES (1), (2)")
            .unwrap();
        executor
            .execute("CREATE TABLE function_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION copy_effect(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX VOLATILE SECURITY INVOKER AS \
                 BEGIN INSERT INTO function_effects VALUES (:input_value); \
                 RETURN input_value; END;",
            )
            .unwrap();

        let mut abandoned = executor
            .execute("SELECT copy_effect(id) FROM function_source ORDER BY id")
            .unwrap();
        assert!(abandoned.next());
        abandoned.close().unwrap();
        assert_eq!(
            scalar(&executor, "SELECT COUNT(*) FROM function_effects"),
            Value::Integer(0)
        );

        let mut completed = executor
            .execute("SELECT copy_effect(id) FROM function_source ORDER BY id")
            .unwrap();
        assert!(completed.next());
        assert!(completed.next());
        assert!(!completed.next());
        assert!(completed.last_error().is_none());
        completed.close().unwrap();
        assert_eq!(
            scalar(&executor, "SELECT COUNT(*) FROM function_effects"),
            Value::Integer(2)
        );
    }

    #[test]
    fn dml_expression_and_volatile_function_share_one_transaction() {
        let executor = executor();
        executor
            .execute("CREATE TABLE function_target (id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();
        executor
            .execute("INSERT INTO function_target VALUES (1, 5)")
            .unwrap();
        executor
            .execute("CREATE TABLE function_effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        executor
            .execute(
                "CREATE FUNCTION write_and_increment(input_value INTEGER NOT NULL) \
                 RETURNS INTEGER NOT NULL LANGUAGE RADIX VOLATILE SECURITY INVOKER AS \
                 BEGIN INSERT INTO function_effects VALUES (:input_value); \
                 RETURN input_value + 1; END;",
            )
            .unwrap();

        executor
            .execute(
                "UPDATE function_target \
                 SET value = write_and_increment(value) WHERE id = 1",
            )
            .unwrap();
        assert_eq!(
            scalar(&executor, "SELECT value FROM function_target WHERE id = 1"),
            Value::Integer(6)
        );
        assert_eq!(
            scalar(
                &executor,
                "SELECT COUNT(*) FROM function_effects WHERE id = 5"
            ),
            Value::Integer(1)
        );
    }
}
