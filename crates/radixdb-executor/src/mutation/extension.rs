//! Execution boundary for database-to-package catalog bindings.

use radixdb_core::Result;
use radixdb_sql::{CreateExtensionStatement, DropExtensionStatement, Statement};
use radixdb_storage::traits::{Engine, QueryResult};

use crate::context::ExecutionContext;
use crate::mutation::host::MutationHost;
use crate::result::ExecResult;

#[doc(hidden)]
pub trait ExtensionDdlExecutorExt: MutationHost {
    fn execute_create_extension(
        &self,
        statement: &CreateExtensionStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(self, Statement::CreateExtension(statement.clone()), context)
    }

    fn execute_drop_extension(
        &self,
        statement: &DropExtensionStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(self, Statement::DropExtension(statement.clone()), context)
    }
}

pub(super) fn execute_binding_statement<H: MutationHost + ?Sized>(
    host: &H,
    statement: Statement,
    context: &ExecutionContext,
) -> Result<Box<dyn QueryResult>> {
    let mutation = host.mutation_stage_catalog_statement_as(
        statement,
        context.effective_principal_id(),
        context.current_database(),
    )?;
    if let Some(mutation) = mutation {
        let mut transaction = host.mutation_engine().begin_transaction()?;
        transaction.stage_catalog_mutation(mutation)?;
        transaction.commit()?;
    }
    host.mutation_invalidate_authorization_caches();
    Ok(Box::new(ExecResult::empty()))
}

impl<T: MutationHost + ?Sized> ExtensionDdlExecutorExt for T {}
