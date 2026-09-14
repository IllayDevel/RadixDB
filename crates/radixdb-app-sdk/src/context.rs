use std::{
    collections::BTreeSet,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use tokio::sync::Notify;

const MAX_CONTEXT_ID_BYTES: usize = 256;
const MAX_PERMISSION_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ContextError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("{field} exceeds {maximum} bytes")]
    TooLong { field: &'static str, maximum: usize },
    #[error("{field} contains a control character")]
    ControlCharacter { field: &'static str },
    #[error("request was cancelled")]
    Cancelled,
    #[error("request deadline elapsed")]
    DeadlineElapsed,
}

macro_rules! context_id {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ContextError> {
                Ok(Self(validate_text(
                    $field,
                    value.into(),
                    MAX_CONTEXT_ID_BYTES,
                )?))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

context_id!(RequestId, "request ID");
context_id!(IdempotencyKey, "idempotency key");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationSession {
    subject_id: String,
    session_id: String,
    authorization_revision: u64,
    permissions: BTreeSet<String>,
}

impl ApplicationSession {
    pub fn new(
        subject_id: impl Into<String>,
        session_id: impl Into<String>,
        authorization_revision: u64,
        permissions: impl IntoIterator<Item = String>,
    ) -> Result<Self, ContextError> {
        let subject_id = validate_text(
            "application subject ID",
            subject_id.into(),
            MAX_CONTEXT_ID_BYTES,
        )?;
        let session_id = validate_text(
            "application session ID",
            session_id.into(),
            MAX_CONTEXT_ID_BYTES,
        )?;
        let permissions = permissions
            .into_iter()
            .map(|permission| {
                validate_text("application permission", permission, MAX_PERMISSION_BYTES)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            subject_id,
            session_id,
            authorization_revision,
            permissions,
        })
    }

    pub fn subject_id(&self) -> &str {
        &self.subject_id
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn authorization_revision(&self) -> u64 {
        self.authorization_revision
    }

    pub fn permissions(&self) -> &BTreeSet<String> {
        &self.permissions
    }

    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.contains(permission)
    }
}

#[derive(Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

#[derive(Debug, Clone)]
pub struct CancellationSignal {
    state: Arc<CancellationState>,
}

impl Default for CancellationSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationSignal {
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub fn cancel(&self) {
        if !self.state.cancelled.swap(true, Ordering::AcqRel) {
            self.state.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.state.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    request_id: RequestId,
    idempotency_key: Option<IdempotencyKey>,
    session: Arc<ApplicationSession>,
    deadline: Option<Instant>,
    cancellation: CancellationSignal,
}

impl RequestContext {
    pub fn new(request_id: RequestId, session: Arc<ApplicationSession>) -> Self {
        Self {
            request_id,
            idempotency_key: None,
            session,
            deadline: None,
            cancellation: CancellationSignal::new(),
        }
    }

    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_cancellation(mut self, cancellation: CancellationSignal) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.idempotency_key.as_ref()
    }

    pub fn session(&self) -> &ApplicationSession {
        &self.session
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    pub fn check_active(&self) -> Result<(), ContextError> {
        if self.cancellation.is_cancelled() {
            return Err(ContextError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(ContextError::DeadlineElapsed);
        }
        Ok(())
    }
}

fn validate_text(
    field: &'static str,
    value: String,
    maximum: usize,
) -> Result<String, ContextError> {
    if value.is_empty() {
        return Err(ContextError::Empty { field });
    }
    if value.len() > maximum {
        return Err(ContextError::TooLong { field, maximum });
    }
    if value.chars().any(char::is_control) {
        return Err(ContextError::ControlCharacter { field });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> Arc<ApplicationSession> {
        Arc::new(
            ApplicationSession::new("subject-7", "session-9", 3, ["documents.read".to_string()])
                .unwrap(),
        )
    }

    #[test]
    fn session_keeps_application_identity_separate_from_database_principal() {
        let session = session();
        assert_eq!(session.subject_id(), "subject-7");
        assert_eq!(session.session_id(), "session-9");
        assert_eq!(session.authorization_revision(), 3);
        assert!(session.has_permission("documents.read"));
        assert!(!session.has_permission("documents.write"));
    }

    #[test]
    fn context_identifiers_reject_empty_control_and_unbounded_values() {
        assert!(matches!(
            RequestId::new(""),
            Err(ContextError::Empty { .. })
        ));
        assert!(matches!(
            RequestId::new("line\nbreak"),
            Err(ContextError::ControlCharacter { .. })
        ));
        assert!(matches!(
            IdempotencyKey::new("x".repeat(MAX_CONTEXT_ID_BYTES + 1)),
            Err(ContextError::TooLong { .. })
        ));
    }

    #[tokio::test]
    async fn cancellation_is_shared_and_wakes_waiters() {
        let signal = CancellationSignal::new();
        let waiter = signal.clone();
        let task = tokio::spawn(async move { waiter.cancelled().await });
        signal.cancel();
        task.await.unwrap();
        assert!(signal.is_cancelled());
    }

    #[test]
    fn request_context_fails_closed_after_deadline_or_cancellation() {
        let expired = RequestContext::new(RequestId::new("r1").unwrap(), session())
            .with_deadline(Instant::now());
        assert_eq!(expired.check_active(), Err(ContextError::DeadlineElapsed));

        let signal = CancellationSignal::new();
        let cancelled = RequestContext::new(RequestId::new("r2").unwrap(), session())
            .with_cancellation(signal.clone());
        signal.cancel();
        assert_eq!(cancelled.check_active(), Err(ContextError::Cancelled));
    }
}
