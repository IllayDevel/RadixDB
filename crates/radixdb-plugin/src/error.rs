use std::fmt;

pub type PluginResult<T> = Result<T, PluginError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginErrorKind {
    InvalidInput,
    Domain,
    LimitExceeded,
    Cancelled,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginError {
    kind: PluginErrorKind,
    detail: String,
    field: Option<String>,
}

impl PluginError {
    pub fn new(kind: PluginErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: bound_utf8(detail.into(), 4096),
            field: None,
        }
    }

    pub fn invalid_input(detail: impl Into<String>) -> Self {
        Self::new(PluginErrorKind::InvalidInput, detail)
    }

    pub fn domain(detail: impl Into<String>) -> Self {
        Self::new(PluginErrorKind::Domain, detail)
    }

    pub fn limit_exceeded(detail: impl Into<String>) -> Self {
        Self::new(PluginErrorKind::LimitExceeded, detail)
    }

    pub fn cancelled() -> Self {
        Self::new(PluginErrorKind::Cancelled, "plugin call cancelled")
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(PluginErrorKind::Internal, detail)
    }

    pub fn with_field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(bound_utf8(field.into(), 255));
        self
    }

    pub fn kind(&self) -> PluginErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn field(&self) -> Option<&str> {
        self.field.as_deref()
    }
}

impl fmt::Display for PluginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for PluginError {}

fn bound_utf8(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}
