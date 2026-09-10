use radixdb_core::Result;
use radixdb_sql::{
    CreateOperatorClassStatement, CreateOperatorStatement, CreatePlannerSupportStatement,
    DropOperatorClassStatement, DropOperatorStatement, DropPlannerSupportStatement, Statement,
};
use radixdb_storage::traits::QueryResult;

use crate::context::ExecutionContext;
use crate::mutation::host::MutationHost;

use super::extension::execute_binding_statement;

#[doc(hidden)]
pub trait OperatorDdlExecutorExt: MutationHost {
    fn execute_create_operator(
        &self,
        statement: &CreateOperatorStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(
            self,
            Statement::CreateOperator(Box::new(statement.clone())),
            context,
        )
    }

    fn execute_drop_operator(
        &self,
        statement: &DropOperatorStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(
            self,
            Statement::DropOperator(Box::new(statement.clone())),
            context,
        )
    }

    fn execute_create_operator_class(
        &self,
        statement: &CreateOperatorClassStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(
            self,
            Statement::CreateOperatorClass(Box::new(statement.clone())),
            context,
        )
    }

    fn execute_drop_operator_class(
        &self,
        statement: &DropOperatorClassStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(
            self,
            Statement::DropOperatorClass(Box::new(statement.clone())),
            context,
        )
    }

    fn execute_create_planner_support(
        &self,
        statement: &CreatePlannerSupportStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(
            self,
            Statement::CreatePlannerSupport(Box::new(statement.clone())),
            context,
        )
    }

    fn execute_drop_planner_support(
        &self,
        statement: &DropPlannerSupportStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        execute_binding_statement(
            self,
            Statement::DropPlannerSupport(Box::new(statement.clone())),
            context,
        )
    }
}

impl<T: MutationHost + ?Sized> OperatorDdlExecutorExt for T {}
