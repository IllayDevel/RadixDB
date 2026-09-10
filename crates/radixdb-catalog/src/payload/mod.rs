mod column;
mod common;
mod constraint;
mod data_type;
mod extension;
mod external_type;
mod index;
mod job;
mod namespace;
mod operator;
mod operator_class;
mod planner_support;
mod routine;
mod security;
mod table;
mod trigger;
mod view;

pub use column::{ColumnPayload, COLUMN_FLAG_AUTO_INCREMENT};
pub use common::{CanonicalSql, MAX_CANONICAL_SQL_BYTES, MAX_OBJECT_IDS_PER_FIELD};
pub use constraint::{ConstraintKind, ConstraintPayload, ForeignKeyAction, ForeignKeyMatch};
pub use data_type::{CatalogDataType, MAX_VECTOR_DIMENSIONS};
pub use extension::{ExtensionPayload, MAX_EXTENSION_VERSION_BYTES};
pub use external_type::{
    ExternalStorageKind, ExternalTypePayload, MAX_EXTERNAL_TYPE_LOCAL_ID_BYTES,
    MAX_EXTERNAL_VALUE_BYTES,
};
pub use index::{
    AccessMethod, HnswDistanceMetric, HnswParameters, IndexPayload, MAX_HNSW_EF_CONSTRUCTION,
    MAX_HNSW_EF_SEARCH, MAX_HNSW_M,
};
pub use job::{JobArgument, JobPayload, JobSchedule, MAX_JOB_ARGUMENTS, MAX_LITERAL_BYTES};
pub use namespace::NamespacePayload;
pub use operator::{OperatorPayload, MAX_OPERATOR_LOCAL_ID_BYTES, MAX_OPERATOR_SYMBOL_BYTES};
pub use operator_class::{
    OperatorBinding, OperatorClassPayload, MAX_OPERATOR_CLASS_BINDINGS,
    MAX_OPERATOR_CLASS_LOCAL_ID_BYTES,
};
pub use planner_support::{
    PlannerRecheckPolicy, PlannerSupportPayload, MAX_PLANNER_SUPPORT_LOCAL_ID_BYTES,
    MAX_PLANNER_SUPPORT_OUTPUT_BYTES, MAX_PLANNER_SUPPORT_SPANS,
};
pub use routine::{
    ArgumentMode, FunctionPayload, LanguageKind, NativeFunctionDefinition, ProceduralSource,
    ProcedurePayload, ResourcePolicy, ResultColumn, RoutineArgument, RoutineDefinition,
    RoutineResult, SecurityMode, Volatility, MAX_RESULT_COLUMNS, MAX_ROUTINE_ARGUMENTS,
    NATIVE_LANGUAGE_VERSION, PROCEDURAL_LANGUAGE_VERSION,
};
pub use security::{
    AclEntryPayload, ColumnPrivilegeSet, CredentialVerifier, PrincipalPayload, RolePayload,
    ALL_OBJECT_PRIVILEGES, CREDENTIAL_SCHEME_ARGON2ID_PHC_V1, MAX_CREDENTIAL_VERIFIER_BYTES,
    PRIVILEGE_CONNECT, PRIVILEGE_CREATE, PRIVILEGE_DELETE, PRIVILEGE_EXECUTE, PRIVILEGE_INSERT,
    PRIVILEGE_SELECT, PRIVILEGE_UPDATE, PRIVILEGE_USAGE,
};
pub use table::{TablePayload, TablePayloadFields};
pub use trigger::{
    TriggerLevel, TriggerPayload, TriggerTiming, ALL_TRIGGER_EVENTS, TRIGGER_EVENT_DELETE,
    TRIGGER_EVENT_INSERT, TRIGGER_EVENT_UPDATE,
};
pub use view::ViewPayload;

use crate::ObjectKind;

pub const PAYLOAD_VERSION: u16 = 1;
pub const SECURITY_PAYLOAD_VERSION: u16 = 2;

/// Closed logical payload set admitted by the supported catalog minors.
/// Reserved object tags deliberately have no variant and cannot enter a
/// production catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogPayload {
    Namespace(NamespacePayload),
    Table(TablePayload),
    Column(ColumnPayload),
    Constraint(ConstraintPayload),
    Index(IndexPayload),
    View(ViewPayload),
    Principal(PrincipalPayload),
    Role(RolePayload),
    AclEntry(AclEntryPayload),
    Function(FunctionPayload),
    Procedure(ProcedurePayload),
    Trigger(TriggerPayload),
    Job(JobPayload),
    Extension(ExtensionPayload),
    ExternalType(ExternalTypePayload),
    Operator(OperatorPayload),
    OperatorClass(OperatorClassPayload),
    PlannerSupport(PlannerSupportPayload),
}

impl CatalogPayload {
    pub const fn kind(&self) -> ObjectKind {
        match self {
            Self::Namespace(_) => ObjectKind::Namespace,
            Self::Table(_) => ObjectKind::Table,
            Self::Column(_) => ObjectKind::Column,
            Self::Constraint(_) => ObjectKind::Constraint,
            Self::Index(_) => ObjectKind::Index,
            Self::View(_) => ObjectKind::View,
            Self::Principal(_) => ObjectKind::Principal,
            Self::Role(_) => ObjectKind::Role,
            Self::AclEntry(_) => ObjectKind::AclEntry,
            Self::Function(_) => ObjectKind::Function,
            Self::Procedure(_) => ObjectKind::Procedure,
            Self::Trigger(_) => ObjectKind::Trigger,
            Self::Job(_) => ObjectKind::Job,
            Self::Extension(_) => ObjectKind::Extension,
            Self::ExternalType(_) => ObjectKind::ExternalType,
            Self::Operator(_) => ObjectKind::Operator,
            Self::OperatorClass(_) => ObjectKind::OperatorClass,
            Self::PlannerSupport(_) => ObjectKind::PlannerSupport,
        }
    }

    pub const fn version(&self) -> u16 {
        match self {
            Self::Principal(_) | Self::Role(_) | Self::AclEntry(_) => SECURITY_PAYLOAD_VERSION,
            Self::Function(FunctionPayload::Native(_)) => 2,
            Self::Index(payload) => payload.version(),
            _ => PAYLOAD_VERSION,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_set_is_closed_and_versioned() {
        let payload = CatalogPayload::Namespace(NamespacePayload::new());
        assert_eq!(payload.kind(), ObjectKind::Namespace);
        assert_eq!(payload.version(), PAYLOAD_VERSION);
    }
}
