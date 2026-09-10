pub mod channel;
pub mod collectors;
pub mod engine;
pub mod http;
pub mod incident;
pub mod kernel;
pub mod model;
pub mod observer;
pub mod progress;
pub mod rates;
pub mod replay;

pub use channel::{AgentTelemetryClient, AgentTelemetryMessage, AgentTelemetryReceiver};
pub use collectors::{
    boot_id, collect_disk, collect_host, collect_processes, collect_processes_with_depth,
    collect_smart, smart_degradation, CgroupSnapshot, DiskSampleV2, HostSampleV2,
    ProcessCollectionDepth, ProcessSampleV2, ProcessSnapshot, PsiSnapshot, SmartSnapshotV2,
};
pub use engine::{EngineSampleV2, EngineSnapshotWorker};
pub use incident::IncidentRecorder;
pub use kernel::{KernelHealthSnapshotV2, KernelHealthWorker};
pub use model::{
    DiagnosticAlertV2, DiagnosticConfidence, DiagnosticIncidentV2, DiagnosticMetricFrameV2,
    DiagnosticSeverity, DiagnosticState, EvidenceSignal, HeartbeatSnapshot, PlannedSilence,
    SemanticProgressSnapshot, DIAGNOSTIC_FORMAT_V2,
};
pub use progress::SemanticProgressTracker;
pub use rates::{DerivedRates, RateDeriver};
pub use replay::{read_frames, replay, DetectorConfig, DiagnosticDetector};
