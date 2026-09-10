use std::fmt;

/// Layer-neutral family used by API, protocol, and diagnostics adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    Catalog,
    Value,
    Constraint,
    Transaction,
    Index,
    Engine,
    Query,
    Security,
    Durability,
    Database,
    Evaluation,
    Syntax,
    Io,
    Internal,
}

/// Stable machine-readable error code independent from presentation text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(&'static str);

impl ErrorCode {
    pub(super) const fn new(value: &'static str) -> Self {
        Self(value)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}
