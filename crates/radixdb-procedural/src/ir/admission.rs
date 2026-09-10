use radixdb_sql::Statement;

use crate::{Diagnostic, DiagnosticKind, ProceduralResult, SourceSpan};

pub(super) fn verify_sql_statement(
    statement: &Statement,
    span: Option<&SourceSpan>,
) -> ProceduralResult<()> {
    match statement {
        Statement::Begin(_)
        | Statement::Commit(_)
        | Statement::Rollback(_)
        | Statement::Savepoint(_)
        | Statement::ReleaseSavepoint(_) => Err(Diagnostic::new(
            DiagnosticKind::VerifyTransactionControlForbidden,
            "transaction control is forbidden inside procedural IR",
        )
        .with_primary_span(span.cloned())),
        Statement::Select(_)
        | Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Call(_) => Ok(()),
        _ => Err(Diagnostic::new(
            DiagnosticKind::VerifyDynamicDdlNotSupported,
            "statement kind is not admitted inside procedural IR",
        )
        .with_primary_span(span.cloned())),
    }
}

/// Apply the same fail-closed SQL-leaf admission used by verified IR.
///
/// Dynamic SQL calls this only after the shared SQL parser has produced
/// exactly one statement.
pub fn admit_embedded_sql(statement: &Statement) -> ProceduralResult<()> {
    verify_sql_statement(statement, None)
}
