//! SQL parsing, parsed-plan admission, and multi-statement sequencing.

use radixdb_core::{Error, Result, Value};
use radixdb_sql::ast::{Program, Statement};
use radixdb_sql::Parser;
use radixdb_storage::traits::QueryResult;

use crate::context::ExecutionContext;

use super::cache::{CachedPlanRef, ParameterContract, QueryCache};

/// Internal callbacks joining cached-program admission to the executor owner.
pub trait CachedExecutionHost<B> {
    fn dispatch_query_cache(&self) -> &QueryCache<B>;

    fn dispatch_execute_bound_plan(
        &self,
        plan: &CachedPlanRef<B>,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;

    fn dispatch_execute_statement(
        &self,
        statement: &Statement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
}

/// Cache admission callbacks for the borrowed-parameter fast path.
pub trait CachedFastPathHost<B>: CachedExecutionHost<B> {
    fn dispatch_fast_path_blocked(&self) -> bool;

    fn dispatch_fast_path_binding_is_nonempty(&self, plan: &CachedPlanRef<B>) -> Result<bool>;

    fn dispatch_try_borrowed_param_fast_path(
        &self,
        plan: &CachedPlanRef<B>,
        params: &[Value],
    ) -> Option<Result<Box<dyn QueryResult>>>;
}

pub fn try_fast_path_with_params<H, B>(
    host: &H,
    sql: &str,
    params: &[Value],
) -> Option<Result<Box<dyn QueryResult>>>
where
    H: CachedFastPathHost<B> + ?Sized,
    B: Default,
{
    if host.dispatch_fast_path_blocked() {
        return None;
    }
    let plan = host.dispatch_query_cache().get(sql)?;
    if plan.parameter_contract().positional_count() != params.len()
        || !plan.parameter_contract().named_params().is_empty()
    {
        return None;
    }
    match host.dispatch_fast_path_binding_is_nonempty(&plan) {
        Ok(true) => return None,
        Ok(false) => {}
        Err(error) => return Some(Err(error)),
    }
    host.dispatch_try_borrowed_param_fast_path(&plan, params)
}

pub fn execute_sql<H, B>(
    host: &H,
    sql: &str,
    context: &ExecutionContext,
) -> Result<Box<dyn QueryResult>>
where
    H: CachedExecutionHost<B> + ?Sized,
    B: Default,
{
    if let Some(plan) = host.dispatch_query_cache().get(sql) {
        plan.parameter_contract().validate(context)?;
        return host.dispatch_execute_bound_plan(&plan, context);
    }

    let mut program = parse_program(sql)?;
    if program.statements.len() != 1 {
        return execute_program(host, &program, context);
    }

    let statement = program
        .statements
        .pop()
        .expect("single-statement length was checked");
    if matches!(statement, Statement::Expression(_)) {
        return host.dispatch_execute_statement(&statement, context);
    }
    let plan = host
        .dispatch_query_cache()
        .put(sql, std::sync::Arc::new(statement), false, 0);
    plan.parameter_contract().validate(context)?;
    host.dispatch_execute_bound_plan(&plan, context)
}

pub fn execute_program<H, B>(
    host: &H,
    program: &Program,
    context: &ExecutionContext,
) -> Result<Box<dyn QueryResult>>
where
    H: CachedExecutionHost<B> + ?Sized,
{
    if program.statements.is_empty() {
        return Err(Error::NoStatementsToExecute);
    }

    let mut last_result: Option<Box<dyn QueryResult>> = None;
    for statement in &program.statements {
        if let Some(mut previous) = last_result.take() {
            while previous.next() {}
            if let Some(error) = previous.last_error() {
                return Err(error);
            }
            previous.close()?;
        }
        last_result = Some(host.dispatch_execute_statement(statement, context)?);
    }
    Ok(last_result.expect("non-empty program produces a result"))
}

pub fn get_or_create_plan<H, B>(host: &H, sql: &str) -> Result<CachedPlanRef<B>>
where
    H: CachedExecutionHost<B> + ?Sized,
    B: Default,
{
    if let Some(plan) = host.dispatch_query_cache().get(sql) {
        return Ok(plan);
    }
    let mut program = parse_program(sql)?;
    if program.statements.len() != 1 {
        return Err(Error::parse(
            "Prepared statements must contain exactly one statement",
        ));
    }
    let statement = program
        .statements
        .pop()
        .expect("single-statement length was checked");
    Ok(host
        .dispatch_query_cache()
        .put(sql, std::sync::Arc::new(statement), false, 0))
}

pub fn execute_prepared_plan<H, B>(
    host: &H,
    plan: &CachedPlanRef<B>,
    context: &ExecutionContext,
) -> Result<Box<dyn QueryResult>>
where
    H: CachedExecutionHost<B> + ?Sized,
    B: Default,
{
    if !host.dispatch_query_cache().owns(plan) {
        return Err(Error::invalid_argument(
            "cached plan belongs to a different Database owner",
        ));
    }
    plan.parameter_contract().validate(context)?;
    host.dispatch_execute_bound_plan(plan, context)
}

pub fn count_parameters(statement: &Statement) -> (bool, usize) {
    let contract = ParameterContract::from_statement(statement);
    (contract.has_params(), contract.positional_count())
}

pub(crate) fn parse_program(sql: &str) -> Result<Program> {
    Parser::new(sql)
        .parse_program()
        .map_err(|error| Error::parse(error.to_string()))
}
