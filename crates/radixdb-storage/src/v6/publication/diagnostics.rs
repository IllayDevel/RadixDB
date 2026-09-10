#[cfg(feature = "test-hooks")]
use std::cell::Cell;

#[cfg(feature = "test-hooks")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublicationDiagnostics {
    pub publication_invocations: u64,
    pub rebuild_invocations: u64,
    pub source_stream_passes: u64,
    pub source_rows: u64,
    pub data_encode_passes: u64,
    pub data_row_groups_encoded: u64,
    pub index_planning_passes: u64,
    pub index_encoding_passes: u64,
    pub postings_planned: u64,
    pub postings_encoded: u64,
    pub data_construction_read_calls: u64,
    pub data_construction_read_bytes: u64,
    pub data_identity_read_calls: u64,
    pub data_identity_read_bytes: u64,
    pub data_write_calls: u64,
    pub data_write_bytes: u64,
    pub index_construction_read_calls: u64,
    pub index_construction_read_bytes: u64,
    pub index_identity_read_calls: u64,
    pub index_identity_read_bytes: u64,
    pub new_artifact_identity_read_calls: u64,
    pub new_artifact_identity_read_bytes: u64,
    pub retained_artifact_identity_read_calls: u64,
    pub retained_artifact_identity_read_bytes: u64,
    pub generation_fence_holds: u64,
    pub generation_fence_hold_nanoseconds: u64,
    pub cleanup_fence_holds: u64,
    pub cleanup_fence_hold_nanoseconds: u64,
    pub index_write_calls: u64,
    pub index_write_bytes: u64,
    pub sort_run_read_calls: u64,
    pub sort_run_read_bytes: u64,
    pub sort_run_write_calls: u64,
    pub sort_run_write_bytes: u64,
    pub sort_merge_passes: u64,
    pub sort_merge_input_runs: u64,
}

#[cfg(feature = "test-hooks")]
thread_local! {
    static COUNTERS: Cell<PublicationDiagnostics> = Cell::new(PublicationDiagnostics::default());
}

#[cfg(feature = "test-hooks")]
pub fn reset_publication_diagnostics() {
    COUNTERS.set(PublicationDiagnostics::default());
}

#[cfg(feature = "test-hooks")]
pub fn publication_diagnostics() -> PublicationDiagnostics {
    COUNTERS.get()
}

// No construction-read event is emitted by the current publication path: the
// corresponding counters deliberately stay at zero. The variants remain part
// of this closed internal vocabulary so a future read site cannot be added
// without naming whether it is construction work or an identity pass.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum DiagnosticEvent {
    PublicationInvocation,
    RebuildInvocation,
    SourceStreamPass,
    SourceRow,
    DataEncodePass,
    DataRowGroupEncode,
    IndexPlanningPass,
    IndexEncodingPass,
    PostingPlan,
    PostingEncode,
    DataConstructionRead,
    DataIdentityRead,
    DataWrite,
    IndexConstructionRead,
    IndexIdentityRead,
    NewArtifactIdentityRead,
    RetainedArtifactIdentityRead,
    GenerationFenceHold,
    CleanupFenceHold,
    IndexWrite,
    SortRunRead,
    SortRunWrite,
    SortMergePass,
    SortMergeInputRun,
}

#[inline]
pub(crate) fn record(event: DiagnosticEvent, value: u64) {
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = (event, value);
    }
    #[cfg(feature = "test-hooks")]
    COUNTERS.with(|slot| {
        let mut counters = slot.get();
        match event {
            DiagnosticEvent::PublicationInvocation => {
                increment(&mut counters.publication_invocations, value)
            }
            DiagnosticEvent::RebuildInvocation => {
                increment(&mut counters.rebuild_invocations, value)
            }
            DiagnosticEvent::SourceStreamPass => {
                increment(&mut counters.source_stream_passes, value)
            }
            DiagnosticEvent::SourceRow => increment(&mut counters.source_rows, value),
            DiagnosticEvent::DataEncodePass => increment(&mut counters.data_encode_passes, value),
            DiagnosticEvent::DataRowGroupEncode => {
                increment(&mut counters.data_row_groups_encoded, value)
            }
            DiagnosticEvent::IndexPlanningPass => {
                increment(&mut counters.index_planning_passes, value)
            }
            DiagnosticEvent::IndexEncodingPass => {
                increment(&mut counters.index_encoding_passes, value)
            }
            DiagnosticEvent::PostingPlan => increment(&mut counters.postings_planned, value),
            DiagnosticEvent::PostingEncode => increment(&mut counters.postings_encoded, value),
            DiagnosticEvent::DataConstructionRead => {
                increment(&mut counters.data_construction_read_calls, 1);
                increment(&mut counters.data_construction_read_bytes, value);
            }
            DiagnosticEvent::DataIdentityRead => {
                increment(&mut counters.data_identity_read_calls, 1);
                increment(&mut counters.data_identity_read_bytes, value);
            }
            DiagnosticEvent::DataWrite => {
                increment(&mut counters.data_write_calls, 1);
                increment(&mut counters.data_write_bytes, value);
            }
            DiagnosticEvent::IndexConstructionRead => {
                increment(&mut counters.index_construction_read_calls, 1);
                increment(&mut counters.index_construction_read_bytes, value);
            }
            DiagnosticEvent::IndexIdentityRead => {
                increment(&mut counters.index_identity_read_calls, 1);
                increment(&mut counters.index_identity_read_bytes, value);
            }
            DiagnosticEvent::NewArtifactIdentityRead => {
                increment(&mut counters.new_artifact_identity_read_calls, 1);
                increment(&mut counters.new_artifact_identity_read_bytes, value);
            }
            DiagnosticEvent::RetainedArtifactIdentityRead => {
                increment(&mut counters.retained_artifact_identity_read_calls, 1);
                increment(&mut counters.retained_artifact_identity_read_bytes, value);
            }
            DiagnosticEvent::GenerationFenceHold => {
                increment(&mut counters.generation_fence_holds, 1);
                increment(&mut counters.generation_fence_hold_nanoseconds, value);
            }
            DiagnosticEvent::CleanupFenceHold => {
                increment(&mut counters.cleanup_fence_holds, 1);
                increment(&mut counters.cleanup_fence_hold_nanoseconds, value);
            }
            DiagnosticEvent::IndexWrite => {
                increment(&mut counters.index_write_calls, 1);
                increment(&mut counters.index_write_bytes, value);
            }
            DiagnosticEvent::SortRunRead => {
                increment(&mut counters.sort_run_read_calls, 1);
                increment(&mut counters.sort_run_read_bytes, value);
            }
            DiagnosticEvent::SortRunWrite => {
                increment(&mut counters.sort_run_write_calls, 1);
                increment(&mut counters.sort_run_write_bytes, value);
            }
            DiagnosticEvent::SortMergePass => increment(&mut counters.sort_merge_passes, value),
            DiagnosticEvent::SortMergeInputRun => {
                increment(&mut counters.sort_merge_input_runs, value)
            }
        }
        slot.set(counters);
    });
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PublicationFenceKind {
    Generation,
    Cleanup,
}

pub(crate) struct PublicationFenceTimer {
    kind: PublicationFenceKind,
    #[cfg(feature = "test-hooks")]
    started: std::time::Instant,
}

impl PublicationFenceTimer {
    pub(crate) fn start(kind: PublicationFenceKind) -> Self {
        Self {
            kind,
            #[cfg(feature = "test-hooks")]
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for PublicationFenceTimer {
    fn drop(&mut self) {
        #[cfg(not(feature = "test-hooks"))]
        let _ = self.kind;
        #[cfg(feature = "test-hooks")]
        {
            let elapsed = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let event = match self.kind {
                PublicationFenceKind::Generation => DiagnosticEvent::GenerationFenceHold,
                PublicationFenceKind::Cleanup => DiagnosticEvent::CleanupFenceHold,
            };
            record(event, elapsed);
        }
    }
}

#[cfg(feature = "test-hooks")]
fn increment(target: &mut u64, value: u64) {
    *target = target.saturating_add(value);
}

// The rebuild owner is introduced by CA-50.9. Keeping its event here makes
// the diagnostic schema stable before that path exists, without letting an
// initial publication pretend to be a rebuild.
#[allow(dead_code)]
#[inline]
pub(crate) fn record_rebuild_invocation() {
    record(DiagnosticEvent::RebuildInvocation, 1);
}
