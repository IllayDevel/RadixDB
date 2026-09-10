use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use radixdb_catalog::ResourcePolicy;

use crate::{Diagnostic, DiagnosticKind, ProceduralResult};

#[derive(Debug, Clone)]
pub struct CancellationHandle(Arc<AtomicBool>);

impl CancellationHandle {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Read-only cancellation source owned by the caller/session boundary.
///
/// This keeps the procedural crate independent from executor and protocol
/// cancellation types while allowing one request signal to stop pure
/// procedural loops and nested SQL alike.
pub trait CancellationProbe: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BudgetSnapshot {
    pub instructions: u64,
    pub heap_bytes: u64,
    pub live_frames: u32,
    pub sql_statements: u64,
    pub rows: u64,
    pub result_bytes: u64,
}

#[derive(Debug)]
struct BudgetState {
    usage: BudgetSnapshot,
}

struct BudgetShared {
    limits: ResourcePolicy,
    state: Mutex<BudgetState>,
    started: Instant,
    cancelled: Arc<AtomicBool>,
    parent_cancellation: Option<Arc<dyn CancellationProbe>>,
}

impl std::fmt::Debug for BudgetShared {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BudgetShared")
            .field("limits", &self.limits)
            .field("state", &self.state)
            .field("started", &self.started)
            .field("cancelled", &self.cancelled)
            .field(
                "has_parent_cancellation",
                &self.parent_cancellation.is_some(),
            )
            .finish()
    }
}

/// One shared resource owner for a complete top-level call and every nested
/// routine, SQL leaf, cursor and trigger entered from it.
#[derive(Debug, Clone)]
pub struct BudgetOwner(Arc<BudgetShared>);

impl BudgetOwner {
    pub fn new(limits: ResourcePolicy) -> ProceduralResult<Self> {
        Self::new_inner(limits, None)
    }

    pub fn with_parent_cancellation(
        limits: ResourcePolicy,
        parent_cancellation: Arc<dyn CancellationProbe>,
    ) -> ProceduralResult<Self> {
        Self::new_inner(limits, Some(parent_cancellation))
    }

    fn new_inner(
        limits: ResourcePolicy,
        parent_cancellation: Option<Arc<dyn CancellationProbe>>,
    ) -> ProceduralResult<Self> {
        let limits = limits.validate().map_err(|error| {
            Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                format!("invalid catalog resource policy: {error}"),
            )
        })?;
        Ok(Self(Arc::new(BudgetShared {
            limits,
            state: Mutex::new(BudgetState {
                usage: BudgetSnapshot::default(),
            }),
            started: Instant::now(),
            cancelled: Arc::new(AtomicBool::new(false)),
            parent_cancellation,
        })))
    }

    pub fn limits(&self) -> ResourcePolicy {
        self.0.limits
    }

    pub fn cancellation_handle(&self) -> CancellationHandle {
        CancellationHandle(Arc::clone(&self.0.cancelled))
    }

    pub fn snapshot(&self) -> BudgetSnapshot {
        self.0.state.lock().unwrap().usage
    }

    pub fn check_boundary(&self) -> ProceduralResult<()> {
        if self.0.cancelled.load(Ordering::Acquire)
            || self
                .0
                .parent_cancellation
                .as_ref()
                .is_some_and(|probe| probe.is_cancelled())
        {
            return Err(Diagnostic::new(
                DiagnosticKind::ResourceCancelled,
                "procedural call was cancelled",
            ));
        }
        if self.0.started.elapsed() >= Duration::from_millis(self.0.limits.deadline_ms) {
            return Err(Diagnostic::new(
                DiagnosticKind::ResourceDeadline,
                "procedural call deadline expired",
            ));
        }
        Ok(())
    }

    /// Remaining wall-clock allowance for a nested blocking SQL operation.
    pub fn remaining_deadline(&self) -> ProceduralResult<Duration> {
        self.check_boundary()?;
        Duration::from_millis(self.0.limits.deadline_ms)
            .checked_sub(self.0.started.elapsed())
            .ok_or_else(|| {
                Diagnostic::new(
                    DiagnosticKind::ResourceDeadline,
                    "procedural call deadline expired",
                )
            })
    }

    pub fn charge_instructions(&self, amount: u64) -> ProceduralResult<()> {
        self.charge_u64(
            amount,
            self.0.limits.instructions,
            DiagnosticKind::ResourceInstructions,
            |usage| &mut usage.instructions,
            "executed instruction budget exceeded",
        )
    }

    pub fn charge_sql_statement(&self) -> ProceduralResult<()> {
        self.charge_u64(
            1,
            self.0.limits.sql_statements,
            DiagnosticKind::ResourceSqlStatements,
            |usage| &mut usage.sql_statements,
            "SQL statement budget exceeded",
        )
    }

    pub fn charge_rows(&self, rows: u64) -> ProceduralResult<()> {
        self.charge_u64(
            rows,
            self.0.limits.rows,
            DiagnosticKind::ResourceRows,
            |usage| &mut usage.rows,
            "row budget exceeded",
        )
    }

    pub fn charge_result_bytes(&self, bytes: u64) -> ProceduralResult<()> {
        self.charge_u64(
            bytes,
            self.0.limits.result_bytes,
            DiagnosticKind::ResourceBytes,
            |usage| &mut usage.result_bytes,
            "result byte budget exceeded",
        )
    }

    pub fn charge_heap(&self, bytes: u64) -> ProceduralResult<()> {
        self.charge_u64(
            bytes,
            self.0.limits.heap_bytes,
            DiagnosticKind::ResourceHeap,
            |usage| &mut usage.heap_bytes,
            "procedural heap budget exceeded",
        )
    }

    pub fn release_heap(&self, bytes: u64) {
        let mut state = self.0.state.lock().unwrap();
        state.usage.heap_bytes = state.usage.heap_bytes.saturating_sub(bytes);
    }

    pub(crate) fn enter_frame(&self) -> ProceduralResult<FrameLease> {
        self.check_boundary()?;
        {
            let mut state = self.0.state.lock().unwrap();
            let next = state.usage.live_frames.checked_add(1).ok_or_else(|| {
                Diagnostic::new(DiagnosticKind::ResourceFrames, "frame counter overflow")
            })?;
            if next > self.0.limits.frames {
                return Err(limit_error(
                    DiagnosticKind::ResourceFrames,
                    "live frame budget exceeded",
                    u64::from(next),
                    u64::from(self.0.limits.frames),
                ));
            }
            state.usage.live_frames = next;
        }
        Ok(FrameLease {
            owner: self.clone(),
        })
    }

    fn charge_u64(
        &self,
        amount: u64,
        limit: u64,
        kind: DiagnosticKind,
        select: impl FnOnce(&mut BudgetSnapshot) -> &mut u64,
        message: &'static str,
    ) -> ProceduralResult<()> {
        let mut state = self.0.state.lock().unwrap();
        let counter = select(&mut state.usage);
        let attempted = counter
            .checked_add(amount)
            .ok_or_else(|| limit_error(kind, message, u64::MAX, limit))?;
        if attempted > limit {
            return Err(limit_error(kind, message, attempted, limit));
        }
        *counter = attempted;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct FrameLease {
    owner: BudgetOwner,
}

impl Drop for FrameLease {
    fn drop(&mut self) {
        let mut state = self.owner.0.state.lock().unwrap();
        state.usage.live_frames = state.usage.live_frames.saturating_sub(1);
    }
}

fn limit_error(
    kind: DiagnosticKind,
    message: &'static str,
    attempted: u64,
    limit: u64,
) -> Diagnostic {
    Diagnostic::new(kind, message)
        .with_detail("attempted", attempted.to_string())
        .with_detail("limit", limit.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_spend_one_shared_budget() {
        let mut policy = ResourcePolicy::default_call();
        policy.instructions = 3;
        let owner = BudgetOwner::new(policy).unwrap();
        owner.charge_instructions(2).unwrap();
        let child = owner.clone();
        child.charge_instructions(1).unwrap();
        assert_eq!(owner.snapshot().instructions, 3);
        assert_eq!(
            child.charge_instructions(1).unwrap_err().kind(),
            DiagnosticKind::ResourceInstructions
        );
    }

    #[test]
    fn cancellation_is_shared_and_checked_at_boundaries() {
        let owner = BudgetOwner::new(ResourcePolicy::default_call()).unwrap();
        owner.cancellation_handle().cancel();
        assert_eq!(
            owner.check_boundary().unwrap_err().kind(),
            DiagnosticKind::ResourceCancelled
        );
    }

    #[test]
    fn frame_limit_failure_does_not_leak_live_frame_usage() {
        let mut policy = ResourcePolicy::default_call();
        policy.frames = 1;
        let owner = BudgetOwner::new(policy).unwrap();
        let frame = owner.enter_frame().unwrap();
        assert_eq!(owner.snapshot().live_frames, 1);
        assert_eq!(
            owner.enter_frame().unwrap_err().kind(),
            DiagnosticKind::ResourceFrames
        );
        assert_eq!(owner.snapshot().live_frames, 1);
        drop(frame);
        assert_eq!(owner.snapshot().live_frames, 0);
    }

    #[test]
    fn cancelled_frame_entry_does_not_charge_a_frame() {
        let owner = BudgetOwner::new(ResourcePolicy::default_call()).unwrap();
        owner.cancellation_handle().cancel();
        assert_eq!(
            owner.enter_frame().unwrap_err().kind(),
            DiagnosticKind::ResourceCancelled
        );
        assert_eq!(owner.snapshot().live_frames, 0);
    }

    #[derive(Debug)]
    struct ParentCancellation(AtomicBool);

    impl CancellationProbe for ParentCancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Acquire)
        }
    }

    #[test]
    fn caller_cancellation_stops_the_shared_budget() {
        let parent = Arc::new(ParentCancellation(AtomicBool::new(false)));
        let owner =
            BudgetOwner::with_parent_cancellation(ResourcePolicy::default_call(), parent.clone())
                .unwrap();
        owner.check_boundary().unwrap();
        parent.0.store(true, Ordering::Release);
        assert_eq!(
            owner.check_boundary().unwrap_err().kind(),
            DiagnosticKind::ResourceCancelled
        );
    }
}
