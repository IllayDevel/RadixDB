use radixdb_client::{ClientError, OrmClientError, ProtocolErrorCode};

use crate::{ContextError, OperationContractError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Never,
    SafeAfterBackoff,
    RequiresOutcomeResolution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationOutcome {
    NotStarted,
    RejectedBeforeCommit,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplicationErrorCode {
    InvalidContext,
    Cancelled,
    DeadlineElapsed,
    AuthenticationFailed,
    AuthorizationDenied,
    DatabaseNotFound,
    NotFound,
    InvalidArgument,
    Conflict,
    InvalidState,
    UniqueViolation,
    InvalidOperation,
    ResultLimitExceeded,
    Backpressure,
    Unsupported,
    ConnectionUnavailable,
    ProtocolViolation,
    TransactionState,
    Internal,
}

#[derive(Debug, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct ApplicationError {
    code: ApplicationErrorCode,
    message: String,
    retry: RetryClass,
    outcome: OperationOutcome,
}

impl ApplicationError {
    pub fn new(
        code: ApplicationErrorCode,
        message: impl Into<String>,
        retry: RetryClass,
        outcome: OperationOutcome,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            retry,
            outcome,
        }
    }

    pub fn code(&self) -> ApplicationErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn retry_class(&self) -> RetryClass {
        self.retry
    }

    pub fn outcome(&self) -> OperationOutcome {
        self.outcome
    }

    pub(crate) fn cancelled_after_start() -> Self {
        Self::new(
            ApplicationErrorCode::Cancelled,
            "request was cancelled while the database outcome may be unknown",
            RetryClass::RequiresOutcomeResolution,
            OperationOutcome::Unknown,
        )
    }

    pub(crate) fn deadline_after_start() -> Self {
        Self::new(
            ApplicationErrorCode::DeadlineElapsed,
            "request deadline elapsed while the database outcome may be unknown",
            RetryClass::RequiresOutcomeResolution,
            OperationOutcome::Unknown,
        )
    }
}

impl From<ContextError> for ApplicationError {
    fn from(error: ContextError) -> Self {
        let code = match error {
            ContextError::Cancelled => ApplicationErrorCode::Cancelled,
            ContextError::DeadlineElapsed => ApplicationErrorCode::DeadlineElapsed,
            _ => ApplicationErrorCode::InvalidContext,
        };
        Self::new(
            code,
            error.to_string(),
            RetryClass::Never,
            OperationOutcome::NotStarted,
        )
    }
}

impl From<OperationContractError> for ApplicationError {
    fn from(error: OperationContractError) -> Self {
        let code = match error {
            OperationContractError::ResultLimitExceeded { .. } => {
                ApplicationErrorCode::ResultLimitExceeded
            }
            _ => ApplicationErrorCode::InvalidOperation,
        };
        Self::new(
            code,
            error.to_string(),
            RetryClass::Never,
            OperationOutcome::NotStarted,
        )
    }
}

impl From<OrmClientError> for ApplicationError {
    fn from(error: OrmClientError) -> Self {
        match error {
            OrmClientError::Client(error) => error.into(),
            OrmClientError::NotFound => Self::new(
                ApplicationErrorCode::NotFound,
                "record was not found",
                RetryClass::Never,
                OperationOutcome::RejectedBeforeCommit,
            ),
            other => Self::new(
                ApplicationErrorCode::InvalidOperation,
                other.to_string(),
                RetryClass::Never,
                OperationOutcome::NotStarted,
            ),
        }
    }
}

impl From<ClientError> for ApplicationError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Server(failure) => from_server_failure(failure.code, failure.message),
            ClientError::TransactionState(message) => Self::new(
                ApplicationErrorCode::TransactionState,
                message,
                RetryClass::Never,
                OperationOutcome::NotStarted,
            ),
            ClientError::CommandsOutOfSync | ClientError::PreparedStatementOwnerMismatch => {
                Self::new(
                    ApplicationErrorCode::TransactionState,
                    error.to_string(),
                    RetryClass::Never,
                    OperationOutcome::NotStarted,
                )
            }
            ClientError::CapabilityUnavailable(_) => Self::new(
                ApplicationErrorCode::Unsupported,
                error.to_string(),
                RetryClass::Never,
                OperationOutcome::NotStarted,
            ),
            ClientError::Protocol(_) | ClientError::UnexpectedResponse(_) => Self::new(
                ApplicationErrorCode::ProtocolViolation,
                error.to_string(),
                RetryClass::RequiresOutcomeResolution,
                OperationOutcome::Unknown,
            ),
            ClientError::Io(_)
            | ClientError::Timeout { .. }
            | ClientError::ConnectionPoisoned
            | ClientError::ConnectionClosed
            | ClientError::TlsConfiguration(_)
            | ClientError::Tls(_) => Self::new(
                ApplicationErrorCode::ConnectionUnavailable,
                error.to_string(),
                RetryClass::RequiresOutcomeResolution,
                OperationOutcome::Unknown,
            ),
        }
    }
}

fn from_server_failure(code: ProtocolErrorCode, message: String) -> ApplicationError {
    let (application_code, retry, outcome) = match code {
        ProtocolErrorCode::AuthenticationFailed => (
            ApplicationErrorCode::AuthenticationFailed,
            RetryClass::Never,
            OperationOutcome::RejectedBeforeCommit,
        ),
        ProtocolErrorCode::AuthorizationDenied => (
            ApplicationErrorCode::AuthorizationDenied,
            RetryClass::Never,
            OperationOutcome::RejectedBeforeCommit,
        ),
        ProtocolErrorCode::DatabaseNotFound => (
            ApplicationErrorCode::DatabaseNotFound,
            RetryClass::Never,
            OperationOutcome::RejectedBeforeCommit,
        ),
        ProtocolErrorCode::CompactionBackpressure => (
            ApplicationErrorCode::Backpressure,
            RetryClass::SafeAfterBackoff,
            OperationOutcome::RejectedBeforeCommit,
        ),
        ProtocolErrorCode::UnsupportedType => (
            ApplicationErrorCode::Unsupported,
            RetryClass::Never,
            OperationOutcome::RejectedBeforeCommit,
        ),
        ProtocolErrorCode::ProtocolViolation => (
            ApplicationErrorCode::ProtocolViolation,
            RetryClass::Never,
            OperationOutcome::RejectedBeforeCommit,
        ),
        ProtocolErrorCode::CommandsOutOfSync
        | ProtocolErrorCode::CursorNotFound
        | ProtocolErrorCode::TransactionState => (
            ApplicationErrorCode::TransactionState,
            RetryClass::Never,
            OperationOutcome::RejectedBeforeCommit,
        ),
        ProtocolErrorCode::SqlError => (
            classify_sql_error(&message),
            RetryClass::Never,
            OperationOutcome::RejectedBeforeCommit,
        ),
        ProtocolErrorCode::ServerError => (
            ApplicationErrorCode::Internal,
            RetryClass::RequiresOutcomeResolution,
            OperationOutcome::Unknown,
        ),
    };
    ApplicationError::new(application_code, message, retry, outcome)
}

fn classify_sql_error(message: &str) -> ApplicationErrorCode {
    for (marker, code) in [
        (
            "PL_RUNTIME_INVALID_ARGUMENT",
            ApplicationErrorCode::InvalidArgument,
        ),
        ("PL_RUNTIME_CONFLICT", ApplicationErrorCode::Conflict),
        ("PL_RUNTIME_NOT_FOUND", ApplicationErrorCode::NotFound),
        (
            "PL_RUNTIME_INVALID_STATE",
            ApplicationErrorCode::InvalidState,
        ),
        (
            "PL_RUNTIME_UNIQUE_VIOLATION",
            ApplicationErrorCode::UniqueViolation,
        ),
    ] {
        if message.contains(marker) {
            return code;
        }
    }
    ApplicationErrorCode::InvalidOperation
}

#[cfg(test)]
mod tests {
    use radixdb_client::{ProtocolFailure, TimeoutOperation};

    use super::*;

    #[test]
    fn only_explicit_backpressure_is_safely_retryable() {
        let error = ApplicationError::from(ClientError::Server(ProtocolFailure {
            code: ProtocolErrorCode::CompactionBackpressure,
            message: "busy".to_string(),
        }));
        assert_eq!(error.code(), ApplicationErrorCode::Backpressure);
        assert_eq!(error.retry_class(), RetryClass::SafeAfterBackoff);
        assert_eq!(error.outcome(), OperationOutcome::RejectedBeforeCommit);
    }

    #[test]
    fn transport_timeout_never_claims_that_retry_is_safe() {
        let error = ApplicationError::from(ClientError::Timeout {
            operation: TimeoutOperation::Read,
        });
        assert_eq!(error.code(), ApplicationErrorCode::ConnectionUnavailable);
        assert_eq!(error.retry_class(), RetryClass::RequiresOutcomeResolution);
        assert_eq!(error.outcome(), OperationOutcome::Unknown);
    }

    #[test]
    fn preflight_cancellation_is_known_not_to_have_started() {
        let error = ApplicationError::from(ContextError::Cancelled);
        assert_eq!(error.code(), ApplicationErrorCode::Cancelled);
        assert_eq!(error.retry_class(), RetryClass::Never);
        assert_eq!(error.outcome(), OperationOutcome::NotStarted);
    }

    #[test]
    fn stable_procedural_diagnostics_are_preserved_as_application_codes() {
        for (marker, expected) in [
            (
                "PL_RUNTIME_INVALID_ARGUMENT",
                ApplicationErrorCode::InvalidArgument,
            ),
            ("PL_RUNTIME_CONFLICT", ApplicationErrorCode::Conflict),
            ("PL_RUNTIME_NOT_FOUND", ApplicationErrorCode::NotFound),
            (
                "PL_RUNTIME_INVALID_STATE",
                ApplicationErrorCode::InvalidState,
            ),
            (
                "PL_RUNTIME_UNIQUE_VIOLATION",
                ApplicationErrorCode::UniqueViolation,
            ),
        ] {
            let error = ApplicationError::from(ClientError::Server(ProtocolFailure {
                code: ProtocolErrorCode::SqlError,
                message: format!("{marker}: rejected"),
            }));
            assert_eq!(error.code(), expected);
            assert_eq!(error.outcome(), OperationOutcome::RejectedBeforeCommit);
            assert_eq!(error.retry_class(), RetryClass::Never);
        }
    }
}
