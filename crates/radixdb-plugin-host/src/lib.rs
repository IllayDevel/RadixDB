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

//! Internal startup loader and immutable runtime registry for operator-trusted
//! native RadixDB packages.
//!
//! This crate owns filesystem admission and copies every accepted ABI
//! descriptor into Rust-owned metadata. It deliberately has no dependency on
//! catalog, executor, MVCC, WAL, page, or recovery crates.

#![forbid(unsafe_op_in_unsafe_fn)]

mod invoke;
mod loader;
mod manifest;
mod registry;

pub use invoke::{
    CandidateKeySpan, CandidatePlan, HashComponent, InvocationLimits, NormalizedPredicateArgument,
    PlannerPlanIdentity, PlannerSupportOutcome, PluginInvocationError,
};
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use loader::registry_from_test_descriptor;
pub use loader::{
    load_plugin_registry, plugin_loader_metrics, PluginHostError, PluginLoaderMetricsSnapshot,
};
pub use manifest::{
    PluginHostConfig, PluginPackageManifest, MANIFEST_FORMAT, MAXIMUM_REQUIRED_GLIBC,
    OFFICIAL_BUILD_IMAGE, PLUGIN_MANIFEST_FILE, SUPPORTED_TARGET,
};
pub use registry::{
    derive_object_id, DatabasePluginAdmission, ObjectId, ObjectKind, ObjectRequirement,
    PackageRequirement, PluginRegistry, PluginRegistryStatus, RegisteredBinding,
    RegisteredExternalType, RegisteredFunction, RegisteredOperator, RegisteredOperatorClass,
    RegisteredPackage, RegisteredPlannerSupport, RegisteredTypeRef, RequirementIssue,
};

/// Stable host-side names for ABI storage tags. Consumers do not need a
/// second direct dependency on the raw ABI crate merely to inspect admitted
/// descriptors.
pub const EXTERNAL_STORAGE_FIXED: u16 = radixdb_plugin_abi::RADIX_EXTERNAL_STORAGE_FIXED;
pub const EXTERNAL_STORAGE_VARIABLE: u16 = radixdb_plugin_abi::RADIX_EXTERNAL_STORAGE_VARIABLE;
