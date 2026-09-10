use radixdb_core::Result;
use radixdb_sql::{CreateExternalTypeStatement, DropExternalTypeStatement, Statement};
use radixdb_storage::traits::QueryResult;

use crate::context::ExecutionContext;
use crate::mutation::host::MutationHost;

use super::extension::execute_binding_statement;

#[doc(hidden)]
pub trait ExternalTypeDdlExecutorExt: MutationHost {
    fn execute_create_external_type(
        &self,
        statement: &CreateExternalTypeStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(
            self,
            Statement::CreateExternalType(statement.clone()),
            context,
        )
    }

    fn execute_drop_external_type(
        &self,
        statement: &DropExternalTypeStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(
            self,
            Statement::DropExternalType(statement.clone()),
            context,
        )
    }
}

impl<T: MutationHost + ?Sized> ExternalTypeDdlExecutorExt for T {}
