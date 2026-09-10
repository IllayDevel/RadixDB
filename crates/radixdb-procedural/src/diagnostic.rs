use std::fmt;

use radixdb_catalog::ObjectId;

const MAX_SECONDARY_SPANS: usize = 16;
const MAX_DIAGNOSTIC_FRAMES: usize = 64;
const MAX_DIAGNOSTIC_DETAILS: usize = 32;
const MAX_DIAGNOSTIC_TEXT_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticCategory {
    Parse,
    Bind,
    Verify,
    Runtime,
    Security,
    Resource,
    Cardinality,
    Trigger,
    Job,
}

impl DiagnosticCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::Bind => "bind",
            Self::Verify => "verify",
            Self::Runtime => "runtime",
            Self::Security => "security",
            Self::Resource => "resource",
            Self::Cardinality => "cardinality",
            Self::Trigger => "trigger",
            Self::Job => "job",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiagnosticKind {
    ParseExpectedToken,
    ParseUnsupportedSyntax,
    ParseLimitExceeded,
    BindUnknownLocal,
    BindUnknownObject,
    BindAmbiguousRoutine,
    BindTypeMismatch,
    BindDependencyCycle,
    VerifyCapabilityDenied,
    VerifyTransactionControlForbidden,
    VerifyDynamicDdlNotSupported,
    VerifyUnboundedResult,
    RuntimeNullNotAllowed,
    RuntimeArrayBounds,
    RuntimeNumericOverflow,
    RuntimeInvalidIr,
    RuntimeInvalidArgument,
    RuntimeConflict,
    RuntimeNotFound,
    RuntimeInvalidState,
    RuntimeUniqueViolation,
    CardinalityNoDataFound,
    CardinalityTooManyRows,
    SecurityExecuteDenied,
    SecurityObjectDenied,
    SecurityUnsafeSearchPath,
    ResourceInstructions,
    ResourceHeap,
    ResourceRows,
    ResourceBytes,
    ResourceSqlStatements,
    ResourceFrames,
    ResourceDeadline,
    ResourceCancelled,
    TriggerCycle,
    TriggerDepth,
    TriggerInvalidReturn,
    JobAttemptFailed,
}

impl DiagnosticKind {
    pub const ALL: [Self; 38] = [
        Self::ParseExpectedToken,
        Self::ParseUnsupportedSyntax,
        Self::ParseLimitExceeded,
        Self::BindUnknownLocal,
        Self::BindUnknownObject,
        Self::BindAmbiguousRoutine,
        Self::BindTypeMismatch,
        Self::BindDependencyCycle,
        Self::VerifyCapabilityDenied,
        Self::VerifyTransactionControlForbidden,
        Self::VerifyDynamicDdlNotSupported,
        Self::VerifyUnboundedResult,
        Self::RuntimeNullNotAllowed,
        Self::RuntimeArrayBounds,
        Self::RuntimeNumericOverflow,
        Self::RuntimeInvalidIr,
        Self::RuntimeInvalidArgument,
        Self::RuntimeConflict,
        Self::RuntimeNotFound,
        Self::RuntimeInvalidState,
        Self::RuntimeUniqueViolation,
        Self::CardinalityNoDataFound,
        Self::CardinalityTooManyRows,
        Self::SecurityExecuteDenied,
        Self::SecurityObjectDenied,
        Self::SecurityUnsafeSearchPath,
        Self::ResourceInstructions,
        Self::ResourceHeap,
        Self::ResourceRows,
        Self::ResourceBytes,
        Self::ResourceSqlStatements,
        Self::ResourceFrames,
        Self::ResourceDeadline,
        Self::ResourceCancelled,
        Self::TriggerCycle,
        Self::TriggerDepth,
        Self::TriggerInvalidReturn,
        Self::JobAttemptFailed,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ParseExpectedToken => "PL_PARSE_EXPECTED_TOKEN",
            Self::ParseUnsupportedSyntax => "PL_PARSE_UNSUPPORTED_SYNTAX",
            Self::ParseLimitExceeded => "PL_PARSE_LIMIT_EXCEEDED",
            Self::BindUnknownLocal => "PL_BIND_UNKNOWN_LOCAL",
            Self::BindUnknownObject => "PL_BIND_UNKNOWN_OBJECT",
            Self::BindAmbiguousRoutine => "PL_BIND_AMBIGUOUS_ROUTINE",
            Self::BindTypeMismatch => "PL_BIND_TYPE_MISMATCH",
            Self::BindDependencyCycle => "PL_BIND_DEPENDENCY_CYCLE",
            Self::VerifyCapabilityDenied => "PL_VERIFY_CAPABILITY_DENIED",
            Self::VerifyTransactionControlForbidden => "PL_VERIFY_TRANSACTION_CONTROL_FORBIDDEN",
            Self::VerifyDynamicDdlNotSupported => "PL_VERIFY_DYNAMIC_DDL_NOT_SUPPORTED",
            Self::VerifyUnboundedResult => "PL_VERIFY_UNBOUNDED_RESULT",
            Self::RuntimeNullNotAllowed => "PL_RUNTIME_NULL_NOT_ALLOWED",
            Self::RuntimeArrayBounds => "PL_RUNTIME_ARRAY_BOUNDS",
            Self::RuntimeNumericOverflow => "PL_RUNTIME_NUMERIC_OVERFLOW",
            Self::RuntimeInvalidIr => "PL_RUNTIME_INVALID_IR",
            Self::RuntimeInvalidArgument => "PL_RUNTIME_INVALID_ARGUMENT",
            Self::RuntimeConflict => "PL_RUNTIME_CONFLICT",
            Self::RuntimeNotFound => "PL_RUNTIME_NOT_FOUND",
            Self::RuntimeInvalidState => "PL_RUNTIME_INVALID_STATE",
            Self::RuntimeUniqueViolation => "PL_RUNTIME_UNIQUE_VIOLATION",
            Self::CardinalityNoDataFound => "PL_CARDINALITY_NO_DATA_FOUND",
            Self::CardinalityTooManyRows => "PL_CARDINALITY_TOO_MANY_ROWS",
            Self::SecurityExecuteDenied => "PL_SECURITY_EXECUTE_DENIED",
            Self::SecurityObjectDenied => "PL_SECURITY_OBJECT_DENIED",
            Self::SecurityUnsafeSearchPath => "PL_SECURITY_UNSAFE_SEARCH_PATH",
            Self::ResourceInstructions => "PL_RESOURCE_INSTRUCTIONS",
            Self::ResourceHeap => "PL_RESOURCE_HEAP",
            Self::ResourceRows => "PL_RESOURCE_ROWS",
            Self::ResourceBytes => "PL_RESOURCE_BYTES",
            Self::ResourceSqlStatements => "PL_RESOURCE_SQL_STATEMENTS",
            Self::ResourceFrames => "PL_RESOURCE_FRAMES",
            Self::ResourceDeadline => "PL_RESOURCE_DEADLINE",
            Self::ResourceCancelled => "PL_RESOURCE_CANCELLED",
            Self::TriggerCycle => "PL_TRIGGER_CYCLE",
            Self::TriggerDepth => "PL_TRIGGER_DEPTH",
            Self::TriggerInvalidReturn => "PL_TRIGGER_INVALID_RETURN",
            Self::JobAttemptFailed => "PL_JOB_ATTEMPT_FAILED",
        }
    }

    pub const fn category(self) -> DiagnosticCategory {
        match self {
            Self::ParseExpectedToken | Self::ParseUnsupportedSyntax | Self::ParseLimitExceeded => {
                DiagnosticCategory::Parse
            }
            Self::BindUnknownLocal
            | Self::BindUnknownObject
            | Self::BindAmbiguousRoutine
            | Self::BindTypeMismatch
            | Self::BindDependencyCycle => DiagnosticCategory::Bind,
            Self::VerifyCapabilityDenied
            | Self::VerifyTransactionControlForbidden
            | Self::VerifyDynamicDdlNotSupported
            | Self::VerifyUnboundedResult => DiagnosticCategory::Verify,
            Self::RuntimeNullNotAllowed
            | Self::RuntimeArrayBounds
            | Self::RuntimeNumericOverflow
            | Self::RuntimeInvalidIr
            | Self::RuntimeInvalidArgument
            | Self::RuntimeConflict
            | Self::RuntimeNotFound
            | Self::RuntimeInvalidState
            | Self::RuntimeUniqueViolation => DiagnosticCategory::Runtime,
            Self::CardinalityNoDataFound | Self::CardinalityTooManyRows => {
                DiagnosticCategory::Cardinality
            }
            Self::SecurityExecuteDenied
            | Self::SecurityObjectDenied
            | Self::SecurityUnsafeSearchPath => DiagnosticCategory::Security,
            Self::ResourceInstructions
            | Self::ResourceHeap
            | Self::ResourceRows
            | Self::ResourceBytes
            | Self::ResourceSqlStatements
            | Self::ResourceFrames
            | Self::ResourceDeadline
            | Self::ResourceCancelled => DiagnosticCategory::Resource,
            Self::TriggerCycle | Self::TriggerDepth | Self::TriggerInvalidReturn => {
                DiagnosticCategory::Trigger
            }
            Self::JobAttemptFailed => DiagnosticCategory::Job,
        }
    }

    pub const fn retryable(self) -> bool {
        matches!(
            self,
            Self::ResourceDeadline | Self::ResourceCancelled | Self::JobAttemptFailed
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourcePosition {
    pub line: u32,
    pub column: u32,
}

impl SourcePosition {
    pub const fn new(line: u32, column: u32) -> Option<Self> {
        if line == 0 || column == 0 {
            None
        } else {
            Some(Self { line, column })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSpan {
    pub source_object_id: ObjectId,
    pub definition_revision: u64,
    pub start_byte: u32,
    pub end_byte: u32,
    pub start: SourcePosition,
    pub end: SourcePosition,
}

impl SourceSpan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_object_id: ObjectId,
        definition_revision: u64,
        start_byte: u32,
        end_byte: u32,
        start_line: u32,
        start_column: u32,
        end_line: u32,
        end_column: u32,
    ) -> Option<Self> {
        if definition_revision == 0 || start_byte > end_byte {
            return None;
        }
        Some(Self {
            source_object_id,
            definition_revision,
            start_byte,
            end_byte,
            start: SourcePosition::new(start_line, start_column)?,
            end: SourcePosition::new(end_line, end_column)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondarySpan {
    pub label: String,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticFrame {
    pub object_id: ObjectId,
    pub definition_revision: u64,
    pub name: String,
    pub call_span: Option<SourceSpan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticDetail {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    inner: Box<DiagnosticData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiagnosticData {
    kind: DiagnosticKind,
    message: String,
    primary_span: Option<SourceSpan>,
    secondary_spans: Vec<SecondarySpan>,
    frames: Vec<DiagnosticFrame>,
    details: Vec<DiagnosticDetail>,
    cause: Option<DiagnosticKind>,
}

impl Diagnostic {
    pub fn new(kind: DiagnosticKind, message: impl Into<String>) -> Self {
        Self {
            inner: Box::new(DiagnosticData {
                kind,
                message: bounded_text(message.into()),
                primary_span: None,
                secondary_spans: Vec::new(),
                frames: Vec::new(),
                details: Vec::new(),
                cause: None,
            }),
        }
    }

    pub fn with_primary_span(mut self, span: Option<SourceSpan>) -> Self {
        self.inner.primary_span = span;
        self
    }

    pub fn with_secondary_span(mut self, label: impl Into<String>, span: SourceSpan) -> Self {
        if self.inner.secondary_spans.len() < MAX_SECONDARY_SPANS {
            self.inner.secondary_spans.push(SecondarySpan {
                label: bounded_text(label.into()),
                span,
            });
        }
        self
    }

    pub fn with_frame(mut self, mut frame: DiagnosticFrame) -> Self {
        if self.inner.frames.len() < MAX_DIAGNOSTIC_FRAMES {
            frame.name = bounded_text(frame.name);
            self.inner.frames.push(frame);
        }
        self
    }

    pub fn with_outer_frame(mut self, mut frame: DiagnosticFrame) -> Self {
        if self.inner.frames.len() < MAX_DIAGNOSTIC_FRAMES {
            frame.name = bounded_text(frame.name);
            self.inner.frames.insert(0, frame);
        }
        self
    }

    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if self.inner.details.len() < MAX_DIAGNOSTIC_DETAILS {
            self.inner.details.push(DiagnosticDetail {
                key: bounded_text(key.into()),
                value: bounded_text(value.into()),
            });
        }
        self
    }

    pub const fn kind(&self) -> DiagnosticKind {
        self.inner.kind
    }
    pub const fn category(&self) -> DiagnosticCategory {
        self.inner.kind.category()
    }
    pub const fn retryable(&self) -> bool {
        self.inner.kind.retryable()
    }
    pub fn message(&self) -> &str {
        &self.inner.message
    }
    pub fn primary_span(&self) -> Option<&SourceSpan> {
        self.inner.primary_span.as_ref()
    }
    pub fn secondary_spans(&self) -> &[SecondarySpan] {
        &self.inner.secondary_spans
    }
    pub fn frames(&self) -> &[DiagnosticFrame] {
        &self.inner.frames
    }
    pub fn details(&self) -> &[DiagnosticDetail] {
        &self.inner.details
    }
    pub const fn cause(&self) -> Option<DiagnosticKind> {
        self.inner.cause
    }
    pub fn with_cause(mut self, cause: DiagnosticKind) -> Self {
        self.inner.cause = Some(cause);
        self
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.inner.kind.as_str(),
            self.inner.message
        )
    }
}

impl std::error::Error for Diagnostic {}

fn bounded_text(mut value: String) -> String {
    if value.len() <= MAX_DIAGNOSTIC_TEXT_BYTES {
        return value;
    }
    let mut boundary = MAX_DIAGNOSTIC_TEXT_BYTES;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_has_the_stable_prefix_and_category() {
        let mut spellings = std::collections::BTreeSet::new();
        for kind in DiagnosticKind::ALL {
            assert!(kind.as_str().starts_with("PL_"));
            assert!(!kind.category().as_str().is_empty());
            assert!(spellings.insert(kind.as_str()), "duplicate diagnostic kind");
            assert!(Diagnostic::new(kind, "failure")
                .to_string()
                .starts_with(kind.as_str()));
        }
        assert_eq!(spellings.len(), DiagnosticKind::ALL.len());
    }

    #[test]
    fn retryable_registry_is_explicit_and_closed() {
        let retryable = DiagnosticKind::ALL
            .into_iter()
            .filter(|kind| kind.retryable())
            .collect::<Vec<_>>();
        assert_eq!(
            retryable,
            vec![
                DiagnosticKind::ResourceDeadline,
                DiagnosticKind::ResourceCancelled,
                DiagnosticKind::JobAttemptFailed,
            ]
        );
    }

    #[test]
    fn diagnostic_text_and_lists_are_bounded() {
        let mut diagnostic = Diagnostic::new(DiagnosticKind::RuntimeInvalidIr, "x".repeat(10_000));
        for index in 0..100 {
            diagnostic = diagnostic.with_detail(format!("key-{index}"), "value");
        }
        assert_eq!(diagnostic.message().len(), MAX_DIAGNOSTIC_TEXT_BYTES);
        assert_eq!(diagnostic.details().len(), MAX_DIAGNOSTIC_DETAILS);
        assert_eq!(
            std::mem::size_of::<Diagnostic>(),
            std::mem::size_of::<usize>()
        );
    }
}
