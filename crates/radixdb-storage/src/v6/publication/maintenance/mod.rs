mod model;
mod validation;

pub use model::{FrozenMaintenance, MaintenanceKind, MaintenanceOutcome};
pub(crate) use validation::{
    validate_maintenance_transition, validate_rebased_compaction_transition,
};
