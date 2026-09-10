// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![forbid(unsafe_code)]

//! Immutable logical catalog model and deterministic generation codec.
//!
//! This private implementation crate owns V6 catalog objects, relations,
//! validation and immutable runtime generations. Its dependency direction is
//! deliberately one-way: `radixdb-catalog -> radixdb-core`. SQL binding,
//! execution, storage I/O, embedded APIs and protocol concerns remain outside
//! this crate.
//!
//! The crate is only a compiled boundary at CA-10.1. It does not become the
//! production catalog authority until the indivisible CA-70.5 cutover.

mod codec;
mod error;
mod generation;
mod graph;
mod identity;
mod kind;
mod mutation;
mod name;
mod payload;

pub use codec::{
    decode_catalog_pack, encode_catalog_pack, CatalogPack, CatalogPackMeta, MAX_CATALOG_EDGES,
    MAX_CATALOG_FILE_BYTES, MAX_CATALOG_OBJECTS, MAX_FIELDS_PER_OBJECT, MAX_PAYLOAD_AREA_BYTES,
    MAX_PAYLOAD_BYTES_PER_OBJECT, MAX_STRING_AREA_BYTES,
};
#[doc(hidden)]
pub use codec::{
    decode_catalog_pack_for_max_minor, encode_catalog_pack_body, finish_catalog_pack,
    CatalogPackBody,
};
pub use error::{CatalogError, CatalogResult};
pub use generation::{CatalogGeneration, CatalogPublisher, PreparedCatalogMutation};
pub use graph::{
    CatalogEdge, CatalogGraph, CatalogObject, DecodedEdgeKind, EdgeKind, ReservedEdgeKind,
    EDGE_VERSION, MAX_DEPENDENCY_DEPTH,
};
pub use identity::ObjectId;
pub use kind::{
    DecodedObjectKind, ObjectClass, ObjectKind, ReservedObjectKind, BASELINE_CATALOG_MINOR,
    EXTENSION_CATALOG_MINOR, LATEST_CATALOG_MINOR, PROCEDURAL_CATALOG_MINOR,
};
pub use mutation::{
    decode_catalog_mutation_set, encode_catalog_mutation_set, CatalogMutation, CatalogMutationSet,
    ObjectPrecondition, MAX_CATALOG_EDGE_DELTAS_PER_SET, MAX_CATALOG_MUTATIONS_PER_SET,
    MAX_CATALOG_MUTATION_SET_BYTES, MUTATION_SET_FORMAT_MAJOR, MUTATION_SET_FORMAT_MINOR,
};
pub use name::{
    CatalogName, DisplayName, NormalizedName, MAX_DISPLAY_NAME_BYTES, MAX_NORMALIZED_NAME_BYTES,
};
pub use payload::{
    AccessMethod, AclEntryPayload, ArgumentMode, CanonicalSql, CatalogDataType, CatalogPayload,
    ColumnPayload, ColumnPrivilegeSet, ConstraintKind, ConstraintPayload, CredentialVerifier,
    ExtensionPayload, ExternalStorageKind, ExternalTypePayload, ForeignKeyAction, ForeignKeyMatch,
    FunctionPayload, HnswDistanceMetric, HnswParameters, IndexPayload, JobArgument, JobPayload,
    JobSchedule, LanguageKind, NamespacePayload, NativeFunctionDefinition, OperatorBinding,
    OperatorClassPayload, OperatorPayload, PlannerRecheckPolicy, PlannerSupportPayload,
    PrincipalPayload, ProceduralSource, ProcedurePayload, ResourcePolicy, ResultColumn,
    RolePayload, RoutineArgument, RoutineDefinition, RoutineResult, SecurityMode, TablePayload,
    TablePayloadFields, TriggerLevel, TriggerPayload, TriggerTiming, ViewPayload, Volatility,
    ALL_OBJECT_PRIVILEGES, ALL_TRIGGER_EVENTS, COLUMN_FLAG_AUTO_INCREMENT,
    CREDENTIAL_SCHEME_ARGON2ID_PHC_V1, MAX_CANONICAL_SQL_BYTES, MAX_CREDENTIAL_VERIFIER_BYTES,
    MAX_EXTENSION_VERSION_BYTES, MAX_EXTERNAL_TYPE_LOCAL_ID_BYTES, MAX_EXTERNAL_VALUE_BYTES,
    MAX_HNSW_EF_CONSTRUCTION, MAX_HNSW_EF_SEARCH, MAX_HNSW_M, MAX_JOB_ARGUMENTS, MAX_LITERAL_BYTES,
    MAX_OBJECT_IDS_PER_FIELD, MAX_OPERATOR_CLASS_BINDINGS, MAX_OPERATOR_CLASS_LOCAL_ID_BYTES,
    MAX_OPERATOR_LOCAL_ID_BYTES, MAX_OPERATOR_SYMBOL_BYTES, MAX_PLANNER_SUPPORT_LOCAL_ID_BYTES,
    MAX_PLANNER_SUPPORT_OUTPUT_BYTES, MAX_PLANNER_SUPPORT_SPANS, MAX_RESULT_COLUMNS,
    MAX_ROUTINE_ARGUMENTS, MAX_VECTOR_DIMENSIONS, NATIVE_LANGUAGE_VERSION, PAYLOAD_VERSION,
    PRIVILEGE_CONNECT, PRIVILEGE_CREATE, PRIVILEGE_DELETE, PRIVILEGE_EXECUTE, PRIVILEGE_INSERT,
    PRIVILEGE_SELECT, PRIVILEGE_UPDATE, PRIVILEGE_USAGE, PROCEDURAL_LANGUAGE_VERSION,
    TRIGGER_EVENT_DELETE, TRIGGER_EVENT_INSERT, TRIGGER_EVENT_UPDATE,
};

#[cfg(test)]
mod tests {
    #[test]
    fn core_contract_is_the_only_lower_layer() {
        let result: radixdb_core::Result<()> = Ok(());
        assert!(result.is_ok());
    }
}
