use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use radixdb_catalog::{ObjectId, ObjectKind, SecurityMode, Volatility};
use radixdb_core::{Error, Row, Value};
use radixdb_procedural::{
    admit_embedded_sql, AuditEvent, AuditHost, BudgetOwner, CancellationProbe, CursorHost,
    CursorToken, Diagnostic, DiagnosticKind, Interpreter, OutboxHost, OutboxMessage,
    PrincipalContext, PrincipalHost, ProceduralResult, RoutineCallHost, RuntimeValue,
    SavepointToken, SqlHost, SqlOutcome, SqlRowSink, TransactionHost,
};
use radixdb_sql::{
    parse_sql, walk_statement_physical_table_sources, walk_statement_tree, Expression,
    InfixExpression, InfixOperator, Position, Statement, Token, TokenType,
};
use radixdb_storage::traits::QueryResult;

use crate::context::{CancellationHandle, ExecutionContext, TimeoutGuard};
use crate::expression::ExpressionEval;
use crate::Executor;

use super::error::map_executor_error;
use super::function::ExecutorStoredFunctionInvoker;
use super::load_published_routine;
use super::value::{runtime_row, scalar_parameters, scalar_value};

static NEXT_HOST_RESOURCE_ID: AtomicU64 = AtomicU64::new(1);

struct CursorEntry {
    result: Box<dyn QueryResult>,
    _timeout: Option<TimeoutGuard>,
}

pub(super) struct ExecutorProceduralHost<'a> {
    executor: &'a Executor,
    context: ExecutionContext,
    principals: PrincipalContext,
    definers: Vec<ObjectId>,
    cursors: BTreeMap<u64, CursorEntry>,
    savepoints: BTreeMap<u64, String>,
    function_volatility: Option<Volatility>,
}

impl<'a> ExecutorProceduralHost<'a> {
    pub(super) fn new(
        executor: &'a Executor,
        context: &ExecutionContext,
        principals: PrincipalContext,
        function_volatility: Option<Volatility>,
    ) -> Self {
        Self {
            executor,
            context: context.clone(),
            principals,
            definers: Vec::new(),
            cursors: BTreeMap::new(),
            savepoints: BTreeMap::new(),
            function_volatility,
        }
    }

    fn sql_context(
        &self,
        parameters: &[RuntimeValue],
        budget: &BudgetOwner,
    ) -> ProceduralResult<ExecutionContext> {
        budget.check_boundary()?;
        self.context.check_cancelled().map_err(map_executor_error)?;
        let mut context = self.context.clone();
        context.set_params(scalar_parameters(parameters)?);
        let remaining = budget.remaining_deadline()?;
        let remaining_ms = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        context.set_timeout_ms(remaining_ms);
        context = context.with_procedural_budget(budget.clone());
        context = context.with_effective_principal_id(self.principal_context().effective_principal);
        let invoker = Arc::new(ExecutorStoredFunctionInvoker::new_with_principals(
            self.executor,
            &context,
            self.principal_context(),
            self.function_volatility,
        ));
        context = context.with_stored_function_invoker(invoker);
        Ok(context)
    }

    fn execute_result(
        &self,
        statement: &Statement,
        parameters: &[RuntimeValue],
        budget: &BudgetOwner,
    ) -> ProceduralResult<(Box<dyn QueryResult>, Option<TimeoutGuard>)> {
        admit_embedded_sql(statement)?;
        let context = self.sql_context(parameters, budget)?;
        let timeout = TimeoutGuard::new(&context);
        let result = self
            .executor
            .execute_statement(statement, &context)
            .map_err(|error| map_controlled_error(error, budget))?;
        Ok((result, timeout))
    }

    fn check_statement_capability(&self, statement: &Statement) -> ProceduralResult<()> {
        let Some(caller) = self.function_volatility else {
            return Ok(());
        };
        if caller != Volatility::Volatile
            && matches!(
                statement,
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
            )
        {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "IMMUTABLE and STABLE functions cannot execute DML",
            ));
        }
        if caller != Volatility::Volatile && matches!(statement, Statement::Call(_)) {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "IMMUTABLE and STABLE functions cannot call procedures",
            ));
        }
        if caller == Volatility::Immutable {
            let mut reads_relation = false;
            walk_statement_physical_table_sources(statement, &mut |_| reads_relation = true);
            if reads_relation {
                return Err(Diagnostic::new(
                    DiagnosticKind::VerifyCapabilityDenied,
                    "IMMUTABLE functions cannot read tables or views",
                ));
            }
        }
        let mut denied = None;
        walk_statement_tree(statement, &mut |expression| {
            let Expression::FunctionCall(function) = expression else {
                return;
            };
            let Some(info) = self.executor.function_registry.get_info(&function.function) else {
                return;
            };
            let target = match info.volatility {
                radixdb_functions::FunctionVolatility::Immutable => Volatility::Immutable,
                radixdb_functions::FunctionVolatility::Stable => Volatility::Stable,
                radixdb_functions::FunctionVolatility::Volatile => Volatility::Volatile,
            };
            if !volatility_allows(caller, target) {
                denied = Some(Diagnostic::new(
                    DiagnosticKind::VerifyCapabilityDenied,
                    format!(
                        "{caller:?} function cannot invoke {target:?} built-in {}",
                        function.function
                    ),
                ));
            }
        });
        if let Some(error) = denied {
            return Err(error);
        }
        Ok(())
    }

    fn evaluate(
        &self,
        expression: &Expression,
        context: &ExecutionContext,
    ) -> ProceduralResult<Value> {
        ExpressionEval::compile(expression, &[])
            .map_err(map_executor_error)?
            .with_context(context)
            .eval_slice(&Row::new())
            .map_err(map_executor_error)
    }
}

fn volatility_allows(caller: Volatility, target: Volatility) -> bool {
    match caller {
        Volatility::Volatile => true,
        Volatility::Stable => target != Volatility::Volatile,
        Volatility::Immutable => target == Volatility::Immutable,
    }
}

impl CancellationProbe for CancellationHandle {
    fn is_cancelled(&self) -> bool {
        CancellationHandle::is_cancelled(self)
    }
}

impl SqlHost for ExecutorProceduralHost<'_> {
    fn evaluate_expression(
        &mut self,
        expression: &Expression,
        parameters: &[RuntimeValue],
        budget: &BudgetOwner,
    ) -> ProceduralResult<RuntimeValue> {
        let context = self.sql_context(parameters, budget)?;
        let value = self.evaluate(expression, &context)?;
        budget.check_boundary()?;
        Ok(RuntimeValue::scalar(value))
    }

    fn evaluate_binary(
        &mut self,
        operator: InfixOperator,
        left: &RuntimeValue,
        right: &RuntimeValue,
        budget: &BudgetOwner,
    ) -> ProceduralResult<RuntimeValue> {
        let spelling = operator_spelling(operator).ok_or_else(|| {
            Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "unbound SQL binary operator reached the executor bridge",
            )
        })?;
        let token = Token::new(TokenType::Operator, spelling, Position::default());
        let expression = Expression::Infix(InfixExpression::new(
            token,
            Box::new(Expression::BoundValue(Box::new(scalar_value(left)?))),
            spelling,
            Box::new(Expression::BoundValue(Box::new(scalar_value(right)?))),
        ));
        let context = self.sql_context(&[], budget)?;
        let value = self.evaluate(&expression, &context)?;
        budget.check_boundary()?;
        Ok(RuntimeValue::scalar(value))
    }

    fn execute_sql(
        &mut self,
        statement: &Statement,
        parameters: &[RuntimeValue],
        rows: &mut dyn SqlRowSink,
        budget: &BudgetOwner,
    ) -> ProceduralResult<SqlOutcome> {
        self.check_statement_capability(statement)?;
        let (mut result, _timeout) = self.execute_result(statement, parameters, budget)?;
        let affected_rows = u64::try_from(result.rows_affected()).unwrap_or(0);
        while result.next() {
            if let Err(error) = budget.check_boundary() {
                let _ = result.close();
                return Err(error);
            }
            if let Err(error) = rows.push_row(runtime_row(result.take_row())) {
                let _ = result.close();
                return Err(error);
            }
        }
        if let Some(error) = result.last_error() {
            let _ = result.close();
            return Err(map_controlled_error(error, budget));
        }
        result.close().map_err(map_executor_error)?;
        budget.check_boundary()?;
        Ok(SqlOutcome { affected_rows })
    }

    fn execute_dynamic_sql(
        &mut self,
        source: &str,
        parameters: &[RuntimeValue],
        rows: &mut dyn SqlRowSink,
        budget: &BudgetOwner,
    ) -> ProceduralResult<SqlOutcome> {
        budget.check_boundary()?;
        let mut statements = parse_sql(source).map_err(|error| {
            Diagnostic::new(DiagnosticKind::ParseExpectedToken, error.to_string())
        })?;
        if statements.len() != 1 {
            return Err(Diagnostic::new(
                DiagnosticKind::ParseUnsupportedSyntax,
                "dynamic SQL must contain exactly one statement",
            ));
        }
        let statement = statements.remove(0);
        admit_embedded_sql(&statement)?;
        self.check_statement_capability(&statement)?;
        self.execute_sql(&statement, parameters, rows, budget)
    }
}

impl CursorHost for ExecutorProceduralHost<'_> {
    fn open_cursor(
        &mut self,
        statement: &Statement,
        parameters: &[RuntimeValue],
        budget: &BudgetOwner,
    ) -> ProceduralResult<CursorToken> {
        let (result, timeout) = self.execute_result(statement, parameters, budget)?;
        let token = NEXT_HOST_RESOURCE_ID.fetch_add(1, Ordering::Relaxed);
        self.cursors.insert(
            token,
            CursorEntry {
                result,
                _timeout: timeout,
            },
        );
        Ok(CursorToken(token))
    }

    fn fetch_cursor(
        &mut self,
        cursor: CursorToken,
        budget: &BudgetOwner,
    ) -> ProceduralResult<Option<Vec<RuntimeValue>>> {
        budget.check_boundary()?;
        let entry = self.cursors.get_mut(&cursor.0).ok_or_else(|| {
            Diagnostic::new(DiagnosticKind::RuntimeInvalidState, "cursor is not open")
        })?;
        if entry.result.next() {
            budget.charge_rows(1)?;
            return Ok(Some(runtime_row(entry.result.take_row())));
        }
        if let Some(error) = entry.result.last_error() {
            return Err(map_controlled_error(error, budget));
        }
        Ok(None)
    }

    fn close_cursor(&mut self, cursor: CursorToken) -> ProceduralResult<()> {
        let mut entry = self.cursors.remove(&cursor.0).ok_or_else(|| {
            Diagnostic::new(DiagnosticKind::RuntimeInvalidState, "cursor is not open")
        })?;
        entry.result.close().map_err(map_executor_error)
    }
}

impl TransactionHost for ExecutorProceduralHost<'_> {
    fn create_savepoint(&mut self) -> ProceduralResult<SavepointToken> {
        let token = NEXT_HOST_RESOURCE_ID.fetch_add(1, Ordering::Relaxed);
        let name = format!("\0radixdb-procedural-{token}");
        self.executor
            .create_active_savepoint(&name)
            .map_err(map_executor_error)?;
        self.savepoints.insert(token, name);
        Ok(SavepointToken(token))
    }

    fn rollback_savepoint(&mut self, savepoint: SavepointToken) -> ProceduralResult<()> {
        let name = self.savepoints.get(&savepoint.0).ok_or_else(|| {
            Diagnostic::new(
                DiagnosticKind::RuntimeInvalidState,
                "procedural savepoint is not active",
            )
        })?;
        self.executor
            .rollback_active_to_savepoint(name)
            .map_err(map_executor_error)
    }

    fn release_savepoint(&mut self, savepoint: SavepointToken) -> ProceduralResult<()> {
        let name = self.savepoints.remove(&savepoint.0).ok_or_else(|| {
            Diagnostic::new(
                DiagnosticKind::RuntimeInvalidState,
                "procedural savepoint is not active",
            )
        })?;
        self.executor
            .release_active_savepoint(&name)
            .map_err(map_executor_error)
    }
}

impl PrincipalHost for ExecutorProceduralHost<'_> {
    fn principal_context(&self) -> PrincipalContext {
        let mut context = self.principals;
        context.effective_principal = self
            .definers
            .last()
            .copied()
            .unwrap_or(context.effective_principal);
        context
    }

    fn push_definer(&mut self, owner: ObjectId) -> ProceduralResult<()> {
        self.definers.push(owner);
        Ok(())
    }

    fn pop_definer(&mut self) -> ProceduralResult<()> {
        self.definers.pop().map(|_| ()).ok_or_else(|| {
            Diagnostic::new(
                DiagnosticKind::RuntimeInvalidState,
                "definer stack is empty",
            )
        })
    }
}

impl RoutineCallHost for ExecutorProceduralHost<'_> {
    fn call_routine(
        &mut self,
        routine: ObjectId,
        arguments: &[RuntimeValue],
        budget: &BudgetOwner,
    ) -> ProceduralResult<Vec<RuntimeValue>> {
        budget.check_boundary()?;
        let principals = self.principal_context();
        crate::authorization::authorize_routine_invocation(
            self.executor,
            principals.session_principal,
            principals.effective_principal,
            routine,
        )
        .map_err(map_executor_error)?;
        let published = load_published_routine(self.executor, routine, ObjectKind::Procedure)
            .map_err(map_executor_error)?;
        let use_definer = published.security == SecurityMode::Definer;
        if use_definer {
            self.push_definer(published.owner)?;
        }
        let execution = Interpreter.execute(&published.program, arguments.to_vec(), self, budget);
        let cleanup = if use_definer {
            self.pop_definer()
        } else {
            Ok(())
        };
        match (execution, cleanup) {
            (Ok(outcome), Ok(())) => Ok(outcome.output_values),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(primary), Err(cleanup)) => {
                Err(primary.with_detail("definer_cleanup_error", cleanup.to_string()))
            }
        }
    }
}

impl AuditHost for ExecutorProceduralHost<'_> {
    fn append_audit(&mut self, event: AuditEvent) -> ProceduralResult<()> {
        if self
            .function_volatility
            .is_some_and(|volatility| volatility != Volatility::Volatile)
        {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "IMMUTABLE and STABLE functions cannot append audit records",
            ));
        }
        let context = self
            .context
            .clone()
            .with_effective_principal_id(self.principal_context().effective_principal);
        self.executor
            .append_audit_event(&context, event)
            .map(|_| ())
            .map_err(map_executor_error)
    }
}

impl OutboxHost for ExecutorProceduralHost<'_> {
    fn append_outbox(&mut self, message: OutboxMessage) -> ProceduralResult<()> {
        if self
            .function_volatility
            .is_some_and(|volatility| volatility != Volatility::Volatile)
        {
            return Err(Diagnostic::new(
                DiagnosticKind::VerifyCapabilityDenied,
                "IMMUTABLE and STABLE functions cannot append outbox records",
            ));
        }
        self.executor
            .append_outbox_message(message)
            .map(|_| ())
            .map_err(map_executor_error)
    }
}

impl Drop for ExecutorProceduralHost<'_> {
    fn drop(&mut self) {
        for (_, mut entry) in std::mem::take(&mut self.cursors) {
            let _ = entry.result.close();
        }
    }
}

pub(super) fn operator_spelling(operator: InfixOperator) -> Option<&'static str> {
    Some(match operator {
        InfixOperator::Equal => "=",
        InfixOperator::NotEqual => "<>",
        InfixOperator::LessThan => "<",
        InfixOperator::LessEqual => "<=",
        InfixOperator::GreaterThan => ">",
        InfixOperator::GreaterEqual => ">=",
        InfixOperator::And => "AND",
        InfixOperator::Or => "OR",
        InfixOperator::Xor => "XOR",
        InfixOperator::Add => "+",
        InfixOperator::Subtract => "-",
        InfixOperator::Multiply => "*",
        InfixOperator::Divide => "/",
        InfixOperator::Modulo => "%",
        InfixOperator::Concat => "||",
        InfixOperator::Like => "LIKE",
        InfixOperator::ILike => "ILIKE",
        InfixOperator::NotLike => "NOT LIKE",
        InfixOperator::NotILike => "NOT ILIKE",
        InfixOperator::Glob => "GLOB",
        InfixOperator::NotGlob => "NOT GLOB",
        InfixOperator::Regexp => "REGEXP",
        InfixOperator::NotRegexp => "NOT REGEXP",
        InfixOperator::Is => "IS",
        InfixOperator::IsNot => "IS NOT",
        InfixOperator::IsDistinctFrom => "IS DISTINCT FROM",
        InfixOperator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
        InfixOperator::Index => "[]",
        InfixOperator::JsonAccess => "->",
        InfixOperator::JsonAccessText => "->>",
        InfixOperator::VectorDistance => "<=>",
        InfixOperator::BitwiseAnd => "&",
        InfixOperator::BitwiseOr => "|",
        InfixOperator::BitwiseXor => "^",
        InfixOperator::LeftShift => "<<",
        InfixOperator::RightShift => ">>",
        InfixOperator::Other => return None,
    })
}

fn map_controlled_error(error: Error, budget: &BudgetOwner) -> Diagnostic {
    if matches!(error, Error::QueryCancelled) {
        if let Err(control) = budget.check_boundary() {
            return control;
        }
    }
    map_executor_error(error)
}
