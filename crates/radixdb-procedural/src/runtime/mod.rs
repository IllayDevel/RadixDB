mod application;
mod cursor;
mod exception;
mod frame;
mod interpreter;
mod record;
mod scalar;
mod sinks;
mod state;

use crate::{Diagnostic, DiagnosticKind, SourceSpan};

pub use interpreter::Interpreter;
pub use state::{ExecutionOutcome, SqlStatus};

pub(super) fn invalid_runtime(message: &'static str) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::RuntimeInvalidIr, message)
}

pub(super) fn attach_span(error: Diagnostic, span: Option<&SourceSpan>) -> Diagnostic {
    if error.primary_span().is_none() {
        error.with_primary_span(span.cloned())
    } else {
        error
    }
}
