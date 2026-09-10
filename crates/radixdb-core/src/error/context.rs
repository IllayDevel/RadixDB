use super::{ErrorCategory, ErrorCode};

/// Neutral classification consumed by upper-layer presentation adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorContext {
    code: ErrorCode,
    category: ErrorCategory,
    retryable: bool,
    not_found: bool,
    constraint_violation: bool,
}

impl ErrorContext {
    pub(super) const fn new(
        code: ErrorCode,
        category: ErrorCategory,
        retryable: bool,
        not_found: bool,
        constraint_violation: bool,
    ) -> Self {
        Self {
            code,
            category,
            retryable,
            not_found,
            constraint_violation,
        }
    }

    pub const fn code(self) -> ErrorCode {
        self.code
    }

    pub const fn category(self) -> ErrorCategory {
        self.category
    }

    pub const fn retryable(self) -> bool {
        self.retryable
    }

    pub const fn not_found(self) -> bool {
        self.not_found
    }

    pub const fn constraint_violation(self) -> bool {
        self.constraint_violation
    }
}
