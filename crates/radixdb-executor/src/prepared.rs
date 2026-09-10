//! Prepared-program admission and transaction-scoped sequencing.
//!
//! The embedded facade owns handles and ergonomic conversions. Parsing,
//! parameter-shape validation, statement sequencing, and SQL transaction
//! control policy remain executor responsibilities.

use std::sync::Arc;

use radixdb_core::{Error, Result};
use radixdb_sql::ast::{Program, Statement};

use crate::context::{ExecutionContext, TimeoutGuard};
use crate::query_cache::{CachedPlanRef, ParameterContract};
use crate::result::{ExecutionResult, TimedQueryResult};
use crate::Executor;

/// Classify whether a SQL program contains transaction-lifecycle commands.
///
/// Network and embedded facades use this narrow contract instead of parsing
/// SQL or depending on the executor AST themselves.
#[doc(hidden)]
pub fn program_contains_transaction_control(sql: &str) -> bool {
    crate::dispatch::program::parse_program(sql).is_ok_and(|program| {
        program.statements.iter().any(|statement| {
            matches!(
                statement,
                Statement::Begin(_)
                    | Statement::Commit(_)
                    | Statement::Rollback(_)
                    | Statement::Savepoint(_)
                    | Statement::ReleaseSavepoint(_)
            )
        })
    })
}

/// Opaque executor-owned representation retained by the embedded
/// `Statement` facade.
#[derive(Clone)]
pub struct PreparedProgram {
    inner: PreparedProgramInner,
}

#[derive(Clone)]
enum PreparedProgramInner {
    Single(CachedPlanRef),
    Multi {
        program: Arc<Program>,
        parameter_contract: ParameterContract,
    },
}

impl Executor {
    /// Parse and admit one public prepared-program handle.
    ///
    /// Single statements reuse the executor cache and compiled fast paths.
    /// Multi-statement programs retain one immutable AST and one aggregate
    /// parameter contract, matching the historical embedded API behavior.
    #[doc(hidden)]
    pub fn prepare_program(&self, sql: &str) -> Result<PreparedProgram> {
        if let Some(cached) = self.query_cache().get(sql) {
            reject_expression_statement(cached.statement(), sql)?;
            return Ok(PreparedProgram {
                inner: PreparedProgramInner::Single(cached),
            });
        }

        let mut program = crate::dispatch::program::parse_program(sql)?;
        if program.statements.is_empty() {
            return Err(Error::NoStatementsToExecute);
        }

        if program.statements.len() == 1 {
            let statement = program
                .statements
                .pop()
                .expect("single-statement length was checked");
            reject_expression_statement(&statement, sql)?;
            let plan = self.query_cache().put(sql, Arc::new(statement), false, 0);
            return Ok(PreparedProgram {
                inner: PreparedProgramInner::Single(plan),
            });
        }

        for statement in &program.statements {
            reject_expression_statement(statement, sql)?;
        }
        let parameter_contract = ParameterContract::from_statements(&program.statements);
        Ok(PreparedProgram {
            inner: PreparedProgramInner::Multi {
                program: Arc::new(program),
                parameter_contract,
            },
        })
    }

    /// Execute an opaque prepared program through its owning executor.
    #[doc(hidden)]
    pub fn execute_prepared_program(
        &self,
        program: &PreparedProgram,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        match &program.inner {
            PreparedProgramInner::Single(plan) => self.execute_with_cached_plan(plan, context),
            PreparedProgramInner::Multi {
                program,
                parameter_contract,
            } => {
                parameter_contract.validate(context)?;
                self.execute_program_with_context(program, context)
            }
        }
    }

    /// Parse and execute SQL against an already installed storage
    /// transaction. SQL transaction-control statements are rejected because
    /// the public `Transaction` handle is the sole lifecycle authority.
    #[doc(hidden)]
    pub fn execute_installed_transaction_sql(
        &self,
        sql: &str,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        let program = crate::dispatch::program::parse_program(sql)?;
        let parameter_contract = ParameterContract::from_statements(&program.statements);
        self.execute_installed_statements(&program.statements, &parameter_contract, context, sql)
    }

    /// Execute a database-owned prepared program against an already installed
    /// storage transaction without claiming ownership of the source cache.
    #[doc(hidden)]
    pub fn execute_installed_transaction_prepared(
        &self,
        program: &PreparedProgram,
        context: &ExecutionContext,
        sql: &str,
    ) -> Result<ExecutionResult> {
        match &program.inner {
            PreparedProgramInner::Single(plan) => self.execute_installed_statements(
                std::slice::from_ref(plan.statement()),
                plan.parameter_contract(),
                context,
                sql,
            ),
            PreparedProgramInner::Multi {
                program,
                parameter_contract,
            } => self.execute_installed_statements(
                &program.statements,
                parameter_contract,
                context,
                sql,
            ),
        }
    }

    fn execute_installed_statements(
        &self,
        statements: &[Statement],
        parameter_contract: &ParameterContract,
        context: &ExecutionContext,
        sql: &str,
    ) -> Result<ExecutionResult> {
        if statements.is_empty() {
            return Err(Error::NoStatementsToExecute);
        }
        parameter_contract.validate(context)?;
        let timeout_guard = TimeoutGuard::new(context);

        let mut last_result: Option<ExecutionResult> = None;
        for statement in statements {
            reject_embedded_transaction_control(statement)?;
            if let Some(mut previous) = last_result.take() {
                while previous.next() {}
                if let Some(error) = previous.last_error() {
                    return Err(error);
                }
                previous.close()?;
            }
            last_result = Some(self.execute_statement(statement, context)?);
        }

        let result = last_result.ok_or(Error::NoStatementsToExecute)?;
        Ok(TimedQueryResult::wrap_with_workload(
            result,
            timeout_guard,
            context.cancellation_handle(),
            sql,
        ))
    }
}

fn reject_expression_statement(statement: &Statement, sql: &str) -> Result<()> {
    if matches!(statement, Statement::Expression(_)) {
        Err(Error::parse(format!(
            "invalid SQL: unrecognised statement: {sql}"
        )))
    } else {
        Ok(())
    }
}

fn reject_embedded_transaction_control(statement: &Statement) -> Result<()> {
    if matches!(
        statement,
        Statement::Begin(_)
            | Statement::Commit(_)
            | Statement::Rollback(_)
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_)
    ) {
        Err(Error::NotSupported(
            "use the Transaction commit, rollback and savepoint methods for transaction control"
                .to_string(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use radixdb_storage::mvcc::engine::MVCCEngine;

    fn executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().expect("open in-memory engine");
        Executor::new(Arc::new(engine))
    }

    #[test]
    fn prepared_program_rejects_empty_and_expression_input() {
        let executor = executor();
        assert_eq!(
            executor.prepare_program("").err(),
            Some(Error::NoStatementsToExecute)
        );
        assert!(matches!(
            executor.prepare_program("SELECTX invalid").err(),
            Some(Error::Parse(_))
        ));
    }

    #[test]
    fn prepared_program_preserves_single_and_multi_parameter_contracts() {
        let executor = executor();
        executor
            .execute("CREATE TABLE prepared_owner (id INTEGER PRIMARY KEY, value TEXT)")
            .expect("create table");

        let single = executor
            .prepare_program("INSERT INTO prepared_owner VALUES ($1, $2)")
            .expect("prepare single");
        executor
            .execute_prepared_program(
                &single,
                &ExecutionContext::with_params(vec![1.into(), "one".into()].into()),
            )
            .expect("execute single");

        let multi = executor
            .prepare_program(
                "INSERT INTO prepared_owner VALUES ($1, 'two'); SELECT value FROM prepared_owner WHERE id = $1",
            )
            .expect("prepare multi");
        let mut result = executor
            .execute_prepared_program(
                &multi,
                &ExecutionContext::with_params(vec![2.into()].into()),
            )
            .expect("execute multi");
        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&"two".into()));
    }

    #[test]
    fn installed_transaction_rejects_sql_lifecycle_control() {
        let executor = executor();
        let transaction = executor.begin_transaction().expect("begin transaction");
        executor.install_transaction(transaction);

        let error =
            match executor.execute_installed_transaction_sql("COMMIT", &ExecutionContext::new()) {
                Err(error) => error,
                Ok(_) => panic!("facade must remain transaction lifecycle owner"),
            };
        assert!(matches!(error, Error::NotSupported(_)));
        executor
            .rollback_installed_transaction()
            .expect("rollback installed transaction");
    }

    #[test]
    fn transaction_control_classification_stays_inside_executor() {
        assert!(program_contains_transaction_control("SELECT 1; COMMIT"));
        assert!(program_contains_transaction_control(
            "SAVEPOINT before_update"
        ));
        assert!(!program_contains_transaction_control("SELECT 'COMMIT'"));
        assert!(!program_contains_transaction_control(
            "malformed select from"
        ));
    }
}
