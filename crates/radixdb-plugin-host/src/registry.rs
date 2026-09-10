use std::{collections::BTreeMap, sync::Arc};

use radixdb_plugin_abi::{
    RadixAbiBatchFnV1, RadixAbiCodecFnV1, RadixAbiCompareFnV1, RadixAbiEqualFnV1, RadixAbiHashFnV1,
    RadixAbiKeyEncodeFnV1, RadixAbiParseFnV1, RadixAbiPlannerSupportFnV1, RadixAbiScalarFnV1,
};
use semver::Version;

pub type ObjectId = [u8; 16];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectKind {
    ExternalType,
    Function,
    Operator,
    OperatorClass,
    PlannerSupport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegisteredTypeRef {
    Builtin(u16),
    External {
        object_id: ObjectId,
        codec_version: u32,
    },
}

#[derive(Clone)]
pub struct RegisteredExternalType {
    pub package_id: ObjectId,
    pub object_id: ObjectId,
    pub local_id: String,
    pub display_name: String,
    pub codec_version: u32,
    pub semantic_revision: u32,
    pub storage_kind: u16,
    pub fixed_bytes: u32,
    pub max_bytes: u32,
    pub capabilities: u64,
    pub codec_fingerprint: [u8; 32],
    pub encode: RadixAbiCodecFnV1,
    pub decode: RadixAbiParseFnV1,
    pub equality: Option<RadixAbiEqualFnV1>,
    pub hash: Option<RadixAbiHashFnV1>,
    pub ordering: Option<RadixAbiCompareFnV1>,
    pub text_input: Option<RadixAbiParseFnV1>,
    pub text_output: Option<RadixAbiCodecFnV1>,
    pub binary_input: Option<RadixAbiParseFnV1>,
    pub binary_output: Option<RadixAbiCodecFnV1>,
}

#[derive(Clone)]
pub struct RegisteredFunction {
    pub package_id: ObjectId,
    pub object_id: ObjectId,
    pub local_id: String,
    pub display_name: String,
    pub semantic_revision: u32,
    pub arguments: Vec<RegisteredTypeRef>,
    pub result: RegisteredTypeRef,
    pub volatility: u16,
    pub cancellation: u16,
    pub strict: bool,
    pub parallel_safe: bool,
    pub cost: u32,
    pub max_output_bytes: u32,
    pub scalar: RadixAbiScalarFnV1,
    pub batch: Option<RadixAbiBatchFnV1>,
}

#[derive(Clone)]
pub struct RegisteredOperator {
    pub package_id: ObjectId,
    pub object_id: ObjectId,
    pub local_id: String,
    pub symbol: String,
    pub semantic_revision: u32,
    pub left: Option<RegisteredTypeRef>,
    pub right: RegisteredTypeRef,
    pub result: RegisteredTypeRef,
    pub function_id: ObjectId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredBinding {
    pub slot: u16,
    pub object_id: ObjectId,
}

#[derive(Clone)]
pub struct RegisteredOperatorClass {
    pub package_id: ObjectId,
    pub object_id: ObjectId,
    pub local_id: String,
    pub semantic_revision: u32,
    pub access_method: u16,
    pub input_type: RegisteredTypeRef,
    pub key_type: RegisteredTypeRef,
    pub key_codec_revision: u32,
    pub strategies: Vec<RegisteredBinding>,
    pub supports: Vec<RegisteredBinding>,
    pub fingerprint: [u8; 32],
    pub encode_key: RadixAbiKeyEncodeFnV1,
}

#[derive(Clone)]
pub struct RegisteredPlannerSupport {
    pub package_id: ObjectId,
    pub object_id: ObjectId,
    pub local_id: String,
    pub semantic_revision: u32,
    pub max_spans: u32,
    pub max_output_bytes: u32,
    pub recheck_policy: u16,
    pub target_function_id: Option<ObjectId>,
    pub target_operator_class_id: Option<ObjectId>,
    pub fingerprint: [u8; 32],
    pub callback: RadixAbiPlannerSupportFnV1,
}

#[derive(Debug, Clone)]
pub struct RegisteredPackage {
    pub package_id: ObjectId,
    pub name: String,
    pub version: Version,
    pub abi_major: u16,
    pub abi_min_minor: u16,
    pub abi_max_minor: u16,
    pub abi_minor: u16,
    pub descriptor_fingerprint: [u8; 32],
}

#[cfg(any(test, feature = "test-hooks"))]
impl RegisteredPackage {
    #[doc(hidden)]
    pub fn for_test(
        package_id: ObjectId,
        name: impl Into<String>,
        version: &str,
        descriptor_fingerprint: [u8; 32],
    ) -> Self {
        Self {
            package_id,
            name: name.into(),
            version: Version::parse(version).expect("test package version must be valid SemVer"),
            abi_major: radixdb_plugin_abi::RADIX_ABI_MAJOR,
            abi_min_minor: radixdb_plugin_abi::RADIX_ABI_MINOR,
            abi_max_minor: radixdb_plugin_abi::RADIX_ABI_MINOR,
            abi_minor: radixdb_plugin_abi::RADIX_ABI_MINOR,
            descriptor_fingerprint,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginRegistryStatus {
    pub generation: u64,
    pub packages: usize,
    pub external_types: usize,
    pub functions: usize,
    pub operators: usize,
    pub operator_classes: usize,
    pub planner_support: usize,
    pub shadowed_versions: usize,
    pub loaded_library_bytes: u64,
}

/// A complete generation. All maps are built before this value is returned;
/// consumers receive it through `Arc` and cannot mutate its contents.
#[derive(Clone)]
pub struct PluginRegistry {
    pub(crate) generation: u64,
    pub(crate) packages: BTreeMap<ObjectId, Arc<RegisteredPackage>>,
    pub(crate) external_types: BTreeMap<ObjectId, Arc<RegisteredExternalType>>,
    pub(crate) functions: BTreeMap<ObjectId, Arc<RegisteredFunction>>,
    pub(crate) operators: BTreeMap<ObjectId, Arc<RegisteredOperator>>,
    pub(crate) operator_classes: BTreeMap<ObjectId, Arc<RegisteredOperatorClass>>,
    pub(crate) planner_support: BTreeMap<ObjectId, Arc<RegisteredPlannerSupport>>,
    pub(crate) shadowed_versions: usize,
    pub(crate) loaded_library_bytes: u64,
}

impl std::fmt::Debug for PluginRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginRegistry")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::empty()
    }
}

impl PluginRegistry {
    pub fn empty() -> Self {
        Self {
            generation: 0,
            packages: BTreeMap::new(),
            external_types: BTreeMap::new(),
            functions: BTreeMap::new(),
            operators: BTreeMap::new(),
            operator_classes: BTreeMap::new(),
            planner_support: BTreeMap::new(),
            shadowed_versions: 0,
            loaded_library_bytes: 0,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn from_test_packages(packages: impl IntoIterator<Item = RegisteredPackage>) -> Self {
        let packages = packages
            .into_iter()
            .map(|package| (package.package_id, Arc::new(package)))
            .collect();
        Self {
            generation: 1,
            packages,
            external_types: BTreeMap::new(),
            functions: BTreeMap::new(),
            operators: BTreeMap::new(),
            operator_classes: BTreeMap::new(),
            planner_support: BTreeMap::new(),
            shadowed_versions: 0,
            loaded_library_bytes: 0,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn from_test_objects(
        packages: impl IntoIterator<Item = RegisteredPackage>,
        external_types: impl IntoIterator<Item = RegisteredExternalType>,
        functions: impl IntoIterator<Item = RegisteredFunction>,
        operators: impl IntoIterator<Item = RegisteredOperator>,
        operator_classes: impl IntoIterator<Item = RegisteredOperatorClass>,
        planner_support: impl IntoIterator<Item = RegisteredPlannerSupport>,
    ) -> Self {
        Self {
            generation: 1,
            packages: packages
                .into_iter()
                .map(|value| (value.package_id, Arc::new(value)))
                .collect(),
            external_types: external_types
                .into_iter()
                .map(|value| (value.object_id, Arc::new(value)))
                .collect(),
            functions: functions
                .into_iter()
                .map(|value| (value.object_id, Arc::new(value)))
                .collect(),
            operators: operators
                .into_iter()
                .map(|value| (value.object_id, Arc::new(value)))
                .collect(),
            operator_classes: operator_classes
                .into_iter()
                .map(|value| (value.object_id, Arc::new(value)))
                .collect(),
            planner_support: planner_support
                .into_iter()
                .map(|value| (value.object_id, Arc::new(value)))
                .collect(),
            shadowed_versions: 0,
            loaded_library_bytes: 0,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn package(&self, id: &ObjectId) -> Option<&Arc<RegisteredPackage>> {
        self.packages.get(id)
    }

    pub fn package_by_name_and_version(
        &self,
        name: &str,
        version: &str,
    ) -> Option<&Arc<RegisteredPackage>> {
        self.packages
            .values()
            .find(|package| package.name == name && package.version.to_string() == version)
    }

    pub fn external_type(&self, id: &ObjectId) -> Option<&Arc<RegisteredExternalType>> {
        self.external_types.get(id)
    }

    pub fn external_type_by_package_and_local_id(
        &self,
        package_id: &ObjectId,
        local_id: &str,
    ) -> Option<&Arc<RegisteredExternalType>> {
        self.external_types.values().find(|external_type| {
            &external_type.package_id == package_id && external_type.local_id == local_id
        })
    }

    pub fn function(&self, id: &ObjectId) -> Option<&Arc<RegisteredFunction>> {
        self.functions.get(id)
    }

    pub fn function_by_package_and_local_id(
        &self,
        package_id: &ObjectId,
        local_id: &str,
    ) -> Option<&Arc<RegisteredFunction>> {
        self.functions
            .values()
            .find(|function| &function.package_id == package_id && function.local_id == local_id)
    }

    pub fn operator(&self, id: &ObjectId) -> Option<&Arc<RegisteredOperator>> {
        self.operators.get(id)
    }

    pub fn operator_by_package_and_local_id(
        &self,
        package_id: &ObjectId,
        local_id: &str,
    ) -> Option<&Arc<RegisteredOperator>> {
        self.operators
            .values()
            .find(|value| &value.package_id == package_id && value.local_id == local_id)
    }

    pub fn operator_class(&self, id: &ObjectId) -> Option<&Arc<RegisteredOperatorClass>> {
        self.operator_classes.get(id)
    }

    pub fn operator_class_by_package_and_local_id(
        &self,
        package_id: &ObjectId,
        local_id: &str,
    ) -> Option<&Arc<RegisteredOperatorClass>> {
        self.operator_classes
            .values()
            .find(|value| &value.package_id == package_id && value.local_id == local_id)
    }

    pub fn planner_support(&self, id: &ObjectId) -> Option<&Arc<RegisteredPlannerSupport>> {
        self.planner_support.get(id)
    }

    pub fn planner_support_by_package_and_local_id(
        &self,
        package_id: &ObjectId,
        local_id: &str,
    ) -> Option<&Arc<RegisteredPlannerSupport>> {
        self.planner_support
            .values()
            .find(|value| &value.package_id == package_id && value.local_id == local_id)
    }

    pub fn status(&self) -> PluginRegistryStatus {
        PluginRegistryStatus {
            generation: self.generation,
            packages: self.packages.len(),
            external_types: self.external_types.len(),
            functions: self.functions.len(),
            operators: self.operators.len(),
            operator_classes: self.operator_classes.len(),
            planner_support: self.planner_support.len(),
            shadowed_versions: self.shadowed_versions,
            loaded_library_bytes: self.loaded_library_bytes,
        }
    }

    pub fn assess_requirements(
        &self,
        requirements: &[PackageRequirement],
    ) -> DatabasePluginAdmission {
        let mut issues = Vec::new();
        for requirement in requirements {
            let Some(package) = self.packages.get(&requirement.package_id) else {
                issues.push(RequirementIssue::MissingPackage {
                    package_id: requirement.package_id,
                });
                continue;
            };
            if package.version != requirement.version {
                issues.push(RequirementIssue::PackageVersion {
                    package_id: requirement.package_id,
                    required: requirement.version.clone(),
                    active: package.version.clone(),
                });
            }
            if package.abi_major != requirement.abi_major
                || package.abi_min_minor != requirement.abi_min_minor
                || package.abi_max_minor != requirement.abi_max_minor
            {
                issues.push(RequirementIssue::PackageAbi {
                    package_id: requirement.package_id,
                    required_major: requirement.abi_major,
                    required_min_minor: requirement.abi_min_minor,
                    required_max_minor: requirement.abi_max_minor,
                    active_major: package.abi_major,
                    active_min_minor: package.abi_min_minor,
                    active_max_minor: package.abi_max_minor,
                });
            }
            if package.descriptor_fingerprint != requirement.descriptor_fingerprint {
                issues.push(RequirementIssue::DescriptorFingerprint {
                    package_id: requirement.package_id,
                });
            }
            for object in &requirement.objects {
                let present = match object.kind {
                    ObjectKind::ExternalType => self
                        .external_types
                        .get(&object.object_id)
                        .is_some_and(|value| {
                            object
                                .codec_version
                                .is_none_or(|version| value.codec_version == version)
                                && object
                                    .semantic_revision
                                    .is_none_or(|revision| value.semantic_revision == revision)
                        }),
                    ObjectKind::Function => {
                        self.functions.get(&object.object_id).is_some_and(|v| {
                            object
                                .semantic_revision
                                .is_none_or(|revision| v.semantic_revision == revision)
                        })
                    }
                    ObjectKind::Operator => {
                        self.operators.get(&object.object_id).is_some_and(|v| {
                            object
                                .semantic_revision
                                .is_none_or(|revision| v.semantic_revision == revision)
                        })
                    }
                    ObjectKind::OperatorClass => self
                        .operator_classes
                        .get(&object.object_id)
                        .is_some_and(|v| {
                            object
                                .codec_version
                                .is_none_or(|revision| v.key_codec_revision == revision)
                                && object
                                    .semantic_revision
                                    .is_none_or(|revision| v.semantic_revision == revision)
                        }),
                    ObjectKind::PlannerSupport => self
                        .planner_support
                        .get(&object.object_id)
                        .is_some_and(|v| {
                            object
                                .semantic_revision
                                .is_none_or(|revision| v.semantic_revision == revision)
                        }),
                };
                if !present {
                    issues.push(RequirementIssue::MissingOrStaleObject {
                        package_id: requirement.package_id,
                        object_id: object.object_id,
                        kind: object.kind,
                    });
                }
            }
        }
        if issues.is_empty() {
            DatabasePluginAdmission::Normal
        } else {
            DatabasePluginAdmission::Restricted { issues }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRequirement {
    pub package_id: ObjectId,
    pub version: Version,
    pub abi_major: u16,
    pub abi_min_minor: u16,
    pub abi_max_minor: u16,
    pub descriptor_fingerprint: [u8; 32],
    pub objects: Vec<ObjectRequirement>,
}

impl PackageRequirement {
    #[allow(clippy::too_many_arguments)]
    pub fn for_package_binding(
        package_id: ObjectId,
        version: &str,
        abi_major: u16,
        abi_min_minor: u16,
        abi_max_minor: u16,
        descriptor_fingerprint: [u8; 32],
    ) -> Result<Self, String> {
        let parsed = Version::parse(version).map_err(|error| error.to_string())?;
        if parsed.to_string() != version || !parsed.build.is_empty() {
            return Err(
                "package binding version is not canonical SemVer without build metadata".to_owned(),
            );
        }
        Ok(Self {
            package_id,
            version: parsed,
            abi_major,
            abi_min_minor,
            abi_max_minor,
            descriptor_fingerprint,
            objects: Vec::new(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRequirement {
    pub object_id: ObjectId,
    pub kind: ObjectKind,
    pub codec_version: Option<u32>,
    pub semantic_revision: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequirementIssue {
    MissingPackage {
        package_id: ObjectId,
    },
    PackageVersion {
        package_id: ObjectId,
        required: Version,
        active: Version,
    },
    PackageAbi {
        package_id: ObjectId,
        required_major: u16,
        required_min_minor: u16,
        required_max_minor: u16,
        active_major: u16,
        active_min_minor: u16,
        active_max_minor: u16,
    },
    DescriptorFingerprint {
        package_id: ObjectId,
    },
    MissingOrStaleObject {
        package_id: ObjectId,
        object_id: ObjectId,
        kind: ObjectKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabasePluginAdmission {
    Normal,
    Restricted { issues: Vec<RequirementIssue> },
}

pub fn derive_object_id(package_id: ObjectId, local_id: &str) -> Result<ObjectId, &'static str> {
    radixdb_core::derive_plugin_object_identity_bytes(package_id, local_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_stable_and_sql_name_independent() {
        let package = [7; 16];
        assert_eq!(
            derive_object_id(package, "point").unwrap(),
            derive_object_id(package, "point").unwrap()
        );
        assert_ne!(
            derive_object_id(package, "point").unwrap(),
            derive_object_id(package, "point_v2").unwrap()
        );
    }

    #[test]
    fn empty_registry_is_compatibility_default() {
        let registry = PluginRegistry::empty();
        assert_eq!(registry.status().generation, 0);
        assert_eq!(registry.status().packages, 0);
        assert_eq!(
            registry.assess_requirements(&[]),
            DatabasePluginAdmission::Normal
        );
    }
}
