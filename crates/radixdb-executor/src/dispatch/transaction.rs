//! SQL transaction-control routing.

use crate::catalog::DdlTransaction;
use crate::context::ExecutionContext;
use crate::mutation::host::{ActiveTransaction, MutationHost};
use crate::result::ExecResult;
use radixdb_core::{Error, IsolationLevel, Result};
use radixdb_sql::ast::{
    BeginStatement, CommitStatement, ReleaseSavepointStatement, RollbackStatement,
    SavepointStatement,
};
use radixdb_storage::traits::{Engine, QueryResult};

/// Parse the SQL spelling of a supported isolation level.
pub fn parse_isolation_level(level: &str) -> Result<IsolationLevel> {
    match level {
        "READ COMMITTED" => Ok(IsolationLevel::ReadCommitted),
        "SNAPSHOT" => Ok(IsolationLevel::SnapshotIsolation),
        _ => Err(Error::internal(format!(
            "unsupported isolation level: '{level}'. Supported: READ COMMITTED, SNAPSHOT"
        ))),
    }
}

/// Transaction-control phase owned by the executor crate.
pub trait TransactionControlExt: MutationHost {
    fn execute_begin(
        &self,
        statement: &BeginStatement,
        _context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let mut active = self.mutation_active_transaction().lock().unwrap();
        if active.is_some() {
            return Err(Error::TransactionAlreadyStarted);
        }

        let transaction = if let Some(level) = &statement.isolation_level {
            self.mutation_engine()
                .begin_transaction_with_level(parse_isolation_level(level)?)?
        } else {
            self.mutation_engine()
                .begin_transaction_with_level(self.mutation_default_isolation_level())?
        };
        let catalog = self.mutation_engine().pin_catalog()?;
        let catalog = DdlTransaction::begin_shared_with_plugin_registry(
            catalog,
            std::sync::Arc::clone(self.mutation_plugin_registry()),
        );
        *active = Some(ActiveTransaction::new(transaction, catalog));
        Ok(Box::new(ExecResult::empty()))
    }

    fn execute_commit_stmt(
        &self,
        _statement: &CommitStatement,
        _context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let mut active = self.mutation_active_transaction().lock().unwrap();
        let Some(mut state) = active.take() else {
            return Err(Error::TransactionNotStarted);
        };
        // An explicit transaction may legitimately discover that its pinned
        // catalog is stale. It must not, however, publish in the middle of an
        // auto-commit writer's pin-to-commit interval and make that statement
        // fail spuriously.
        let _catalog_write_fence = state
            .has_pending_catalog_changes()
            .then(|| self.mutation_engine().acquire_catalog_write_fence());
        if let Err(error) = state.stage_catalog_for_commit() {
            *active = Some(state);
            return Err(error);
        }
        match state.transaction.commit() {
            Ok(()) => Ok(Box::new(ExecResult::empty())),
            Err(error) => {
                if state.transaction.is_active() {
                    *active = Some(state);
                }
                Err(error)
            }
        }
    }

    fn execute_rollback_stmt(
        &self,
        statement: &RollbackStatement,
        _context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let mut active = self.mutation_active_transaction().lock().unwrap();
        if let Some(savepoint) = &statement.savepoint_name {
            let state = active.as_mut().ok_or_else(|| {
                Error::internal("ROLLBACK TO SAVEPOINT can only be used within a transaction")
            })?;
            let name = if savepoint.token.quoted {
                savepoint.value.as_str()
            } else {
                savepoint.value_lower.as_str()
            };
            state.rollback_to_savepoint(name)?;
            return Ok(Box::new(ExecResult::empty()));
        }

        let Some(mut state) = active.take() else {
            return Err(Error::TransactionNotStarted);
        };
        state.rollback()?;
        Ok(Box::new(ExecResult::empty()))
    }

    fn execute_savepoint(
        &self,
        statement: &SavepointStatement,
        _context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let mut active = self.mutation_active_transaction().lock().unwrap();
        let state = active.as_mut().ok_or_else(|| {
            Error::internal("SAVEPOINT can only be used within a transaction (after BEGIN)")
        })?;
        let name = if statement.savepoint_name.token.quoted {
            statement.savepoint_name.value.as_str()
        } else {
            statement.savepoint_name.value_lower.as_str()
        };
        state.create_savepoint(name)?;
        Ok(Box::new(ExecResult::empty()))
    }

    fn execute_release_savepoint(
        &self,
        statement: &ReleaseSavepointStatement,
        _context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        let mut active = self.mutation_active_transaction().lock().unwrap();
        let state = active.as_mut().ok_or_else(|| {
            Error::internal("RELEASE SAVEPOINT can only be used within a transaction (after BEGIN)")
        })?;
        let name = if statement.savepoint_name.token.quoted {
            statement.savepoint_name.value.as_str()
        } else {
            statement.savepoint_name.value_lower.as_str()
        };
        state.release_savepoint(name)?;
        Ok(Box::new(ExecResult::empty()))
    }
}

impl<T: MutationHost + ?Sized> TransactionControlExt for T {}
