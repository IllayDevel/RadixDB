use radixdb_core::Error;
use radixdb_procedural::{Diagnostic, DiagnosticKind};

pub(super) fn map_executor_error(error: Error) -> Diagnostic {
    let kind = match &error {
        Error::AuthorizationDenied(_) => DiagnosticKind::SecurityObjectDenied,
        Error::QueryCancelled => DiagnosticKind::ResourceCancelled,
        Error::NoRowsReturned => DiagnosticKind::CardinalityNoDataFound,
        Error::UniqueConstraint { .. } | Error::PrimaryKeyConstraint { .. } => {
            DiagnosticKind::RuntimeUniqueViolation
        }
        Error::TransactionSerializationConflict { .. }
        | Error::RowLockTimeout { .. }
        | Error::CompactionBackpressure { .. } => DiagnosticKind::RuntimeConflict,
        Error::TableNotFound(_)
        | Error::TableOrViewNotFound(_)
        | Error::ColumnNotFound(_)
        | Error::IndexNotFound(_)
        | Error::ViewNotFound(_) => DiagnosticKind::RuntimeNotFound,
        Error::InvalidValue
        | Error::InvalidArgument(_)
        | Error::InvalidColumnType
        | Error::Type(_)
        | Error::TypeConversion { .. }
        | Error::ValueTooLong { .. }
        | Error::NotNullConstraint { .. }
        | Error::CheckConstraintViolation { .. }
        | Error::ForeignKeyViolation { .. }
        | Error::DivisionByZero
        | Error::ExpressionEvaluation
        | Error::ExpressionEvaluationWithMessage { .. } => DiagnosticKind::RuntimeInvalidArgument,
        _ => DiagnosticKind::RuntimeInvalidState,
    };
    let code = error.code();
    Diagnostic::new(kind, error.to_string()).with_detail("sql_error_code", code.as_str())
}

pub(super) fn cleanup_failed(mut primary: Diagnostic, cleanup: Error) -> Diagnostic {
    primary = primary.with_detail("cleanup_error_code", cleanup.code().as_str());
    primary.with_detail("cleanup_error", cleanup.to_string())
}
