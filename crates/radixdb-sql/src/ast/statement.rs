// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

// ============================================================================
// Statements
// ============================================================================

/// Statement enum representing all statement types
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Truncate(TruncateStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    /// Boxed to reduce enum size (776 bytes unboxed)
    AlterTable(Box<AlterTableStatement>),
    AlterIndex(AlterIndexStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    CreateView(CreateViewStatement),
    DropView(DropViewStatement),
    CreateExtension(CreateExtensionStatement),
    DropExtension(DropExtensionStatement),
    CreateExternalType(CreateExternalTypeStatement),
    DropExternalType(DropExternalTypeStatement),
    CreateOperator(Box<CreateOperatorStatement>),
    DropOperator(Box<DropOperatorStatement>),
    CreateOperatorClass(Box<CreateOperatorClassStatement>),
    DropOperatorClass(Box<DropOperatorClassStatement>),
    CreatePlannerSupport(Box<CreatePlannerSupportStatement>),
    DropPlannerSupport(Box<DropPlannerSupportStatement>),
    Begin(BeginStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    Savepoint(SavepointStatement),
    ReleaseSavepoint(ReleaseSavepointStatement),
    /// Boxed to reduce enum size (424 bytes unboxed)
    Set(Box<SetStatement>),
    Pragma(PragmaStatement),
    ShowTables(ShowTablesStatement),
    ShowViews(ShowViewsStatement),
    ShowCreateTable(ShowCreateTableStatement),
    ShowCreateView(ShowCreateViewStatement),
    ShowIndexes(ShowIndexesStatement),
    Describe(DescribeStatement),
    Expression(ExpressionStatement),
    Explain(ExplainStatement),
    Analyze(AnalyzeStatement),
    Vacuum(VacuumStatement),
    Copy(CopyStatement),
    Call(Box<CallStatement>),
    CreateRoutine(Box<CreateRoutineStatement>),
    CreateTrigger(Box<CreateTriggerStatement>),
    CreateJob(Box<CreateJobStatement>),
    DropRoutine(Box<DropRoutineStatement>),
    DropTrigger(Box<DropTriggerStatement>),
    DropJob(Box<DropJobStatement>),
    AlterJob(Box<AlterJobStatement>),
    CreateSchema(CreateSchemaStatement),
    CreatePrincipal(CreatePrincipalStatement),
    CreateRole(CreateRoleStatement),
    AlterSecuritySubject(AlterSecuritySubjectStatement),
    DropSecuritySubject(DropSecuritySubjectStatement),
    Grant(Box<GrantStatement>),
    Revoke(Box<RevokeStatement>),
    AlterOwner(Box<AlterOwnerStatement>),
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Select(s) => write!(f, "{}", s),
            Statement::Insert(s) => write!(f, "{}", s),
            Statement::Update(s) => write!(f, "{}", s),
            Statement::Delete(s) => write!(f, "{}", s),
            Statement::Truncate(s) => write!(f, "{}", s),
            Statement::CreateTable(s) => write!(f, "{}", s),
            Statement::DropTable(s) => write!(f, "{}", s),
            Statement::AlterTable(s) => write!(f, "{}", s),
            Statement::AlterIndex(s) => write!(f, "{}", s),
            Statement::CreateIndex(s) => write!(f, "{}", s),
            Statement::DropIndex(s) => write!(f, "{}", s),
            Statement::CreateView(s) => write!(f, "{}", s),
            Statement::DropView(s) => write!(f, "{}", s),
            Statement::CreateExtension(s) => write!(f, "{}", s),
            Statement::DropExtension(s) => write!(f, "{}", s),
            Statement::CreateExternalType(s) => write!(f, "{}", s),
            Statement::DropExternalType(s) => write!(f, "{}", s),
            Statement::CreateOperator(s) => write!(f, "{}", s),
            Statement::DropOperator(s) => write!(f, "{}", s),
            Statement::CreateOperatorClass(s) => write!(f, "{}", s),
            Statement::DropOperatorClass(s) => write!(f, "{}", s),
            Statement::CreatePlannerSupport(s) => write!(f, "{}", s),
            Statement::DropPlannerSupport(s) => write!(f, "{}", s),
            Statement::Begin(s) => write!(f, "{}", s),
            Statement::Commit(s) => write!(f, "{}", s),
            Statement::Rollback(s) => write!(f, "{}", s),
            Statement::Savepoint(s) => write!(f, "{}", s),
            Statement::ReleaseSavepoint(s) => write!(f, "{}", s),
            Statement::Set(s) => write!(f, "{}", s),
            Statement::Pragma(s) => write!(f, "{}", s),
            Statement::ShowTables(s) => write!(f, "{}", s),
            Statement::ShowViews(s) => write!(f, "{}", s),
            Statement::ShowCreateTable(s) => write!(f, "{}", s),
            Statement::ShowCreateView(s) => write!(f, "{}", s),
            Statement::ShowIndexes(s) => write!(f, "{}", s),
            Statement::Describe(s) => write!(f, "{}", s),
            Statement::Expression(s) => write!(f, "{}", s),
            Statement::Explain(s) => write!(f, "{}", s),
            Statement::Analyze(s) => write!(f, "{}", s),
            Statement::Vacuum(s) => write!(f, "{}", s),
            Statement::Copy(s) => write!(f, "{}", s),
            Statement::Call(s) => write!(f, "{}", s),
            Statement::CreateRoutine(s) => write!(f, "{}", s),
            Statement::CreateTrigger(s) => write!(f, "{}", s),
            Statement::CreateJob(s) => write!(f, "{}", s),
            Statement::DropRoutine(s) => write!(f, "{}", s),
            Statement::DropTrigger(s) => write!(f, "{}", s),
            Statement::DropJob(s) => write!(f, "{}", s),
            Statement::AlterJob(s) => write!(f, "{}", s),
            Statement::CreateSchema(s) => write!(f, "{}", s),
            Statement::CreatePrincipal(s) => write!(f, "{}", s),
            Statement::CreateRole(s) => write!(f, "{}", s),
            Statement::AlterSecuritySubject(s) => write!(f, "{}", s),
            Statement::DropSecuritySubject(s) => write!(f, "{}", s),
            Statement::Grant(s) => write!(f, "{}", s),
            Statement::Revoke(s) => write!(f, "{}", s),
            Statement::AlterOwner(s) => write!(f, "{}", s),
        }
    }
}

/// Program (collection of statements)
#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub statements: Vec<Statement>,
}

impl fmt::Display for Program {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for stmt in &self.statements {
            write!(f, "{};", stmt)?;
        }
        Ok(())
    }
}
