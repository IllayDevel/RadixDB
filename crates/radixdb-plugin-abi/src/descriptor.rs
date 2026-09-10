use crate::{
    RadixAbiBatchViewV1, RadixAbiCallContextV1, RadixAbiHashSinkV1, RadixAbiHeaderV1,
    RadixAbiResultBuilderV1, RadixAbiSliceV1, RadixAbiStatusV1, RadixAbiStringV1,
    RadixAbiTypeRefV1, RadixAbiValueV1,
};

pub type RadixAbiCodecFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    input: *const RadixAbiValueV1,
    output: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1;
pub type RadixAbiParseFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    input: RadixAbiSliceV1,
    output: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1;
pub type RadixAbiEqualFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    left: *const RadixAbiValueV1,
    right: *const RadixAbiValueV1,
    output: *mut u8,
) -> RadixAbiStatusV1;
pub type RadixAbiCompareFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    left: *const RadixAbiValueV1,
    right: *const RadixAbiValueV1,
    output: *mut i8,
) -> RadixAbiStatusV1;
pub type RadixAbiHashFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    value: *const RadixAbiValueV1,
    sink: *const RadixAbiHashSinkV1,
) -> RadixAbiStatusV1;
pub type RadixAbiScalarFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    arguments: *const RadixAbiValueV1,
    argument_count: u32,
    output: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1;
pub type RadixAbiBatchFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    input: *const RadixAbiBatchViewV1,
    output: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1;
pub type RadixAbiPlannerSupportFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    normalized_predicate: RadixAbiSliceV1,
    output: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiExternalTypeDescriptorV1 {
    pub header: RadixAbiHeaderV1,
    pub object_id: [u8; 16],
    pub local_id: RadixAbiStringV1,
    pub display_name: RadixAbiStringV1,
    pub codec_version: u32,
    pub semantic_revision: u32,
    pub storage_kind: u16,
    pub reserved_u16: u16,
    pub fixed_bytes: u32,
    pub max_bytes: u32,
    pub reserved_u32: u32,
    pub capabilities: u64,
    pub codec_fingerprint: [u8; 32],
    pub encode: Option<RadixAbiCodecFnV1>,
    pub decode: Option<RadixAbiParseFnV1>,
    pub equality: Option<RadixAbiEqualFnV1>,
    pub hash: Option<RadixAbiHashFnV1>,
    pub ordering: Option<RadixAbiCompareFnV1>,
    pub text_input: Option<RadixAbiParseFnV1>,
    pub text_output: Option<RadixAbiCodecFnV1>,
    pub binary_input: Option<RadixAbiParseFnV1>,
    pub binary_output: Option<RadixAbiCodecFnV1>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiScalarFunctionDescriptorV1 {
    pub header: RadixAbiHeaderV1,
    pub object_id: [u8; 16],
    pub local_id: RadixAbiStringV1,
    pub display_name: RadixAbiStringV1,
    pub semantic_revision: u32,
    pub argument_count: u32,
    pub arguments: *const RadixAbiTypeRefV1,
    pub result: RadixAbiTypeRefV1,
    pub volatility: u16,
    pub cancellation: u16,
    pub strict: u8,
    pub parallel_safe: u8,
    pub reserved_u16: u16,
    pub cost: u32,
    pub max_output_bytes: u32,
    pub scalar: Option<RadixAbiScalarFnV1>,
    pub batch: Option<RadixAbiBatchFnV1>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiOperatorDescriptorV1 {
    pub header: RadixAbiHeaderV1,
    pub object_id: [u8; 16],
    pub local_id: RadixAbiStringV1,
    pub symbol: RadixAbiStringV1,
    pub semantic_revision: u32,
    pub reserved: u32,
    pub left: RadixAbiTypeRefV1,
    pub right: RadixAbiTypeRefV1,
    pub result: RadixAbiTypeRefV1,
    pub function_id: [u8; 16],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RadixAbiBindingEntryV1 {
    pub slot: u16,
    pub flags: u16,
    pub object_id: [u8; 16],
}

pub type RadixAbiKeyEncodeFnV1 = unsafe extern "C" fn(
    context: *const RadixAbiCallContextV1,
    value: *const RadixAbiValueV1,
    output: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiOperatorClassDescriptorV1 {
    pub header: RadixAbiHeaderV1,
    pub object_id: [u8; 16],
    pub local_id: RadixAbiStringV1,
    pub semantic_revision: u32,
    pub access_method: u16,
    pub reserved_u16: u16,
    pub input_type: RadixAbiTypeRefV1,
    pub key_type: RadixAbiTypeRefV1,
    pub key_codec_revision: u32,
    pub strategy_count: u32,
    pub strategies: *const RadixAbiBindingEntryV1,
    pub support_count: u32,
    pub reserved_u32: u32,
    pub supports: *const RadixAbiBindingEntryV1,
    pub fingerprint: [u8; 32],
    pub encode_key: Option<RadixAbiKeyEncodeFnV1>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiPlannerSupportDescriptorV1 {
    pub header: RadixAbiHeaderV1,
    pub object_id: [u8; 16],
    pub local_id: RadixAbiStringV1,
    pub semantic_revision: u32,
    pub max_spans: u32,
    pub max_output_bytes: u32,
    pub recheck_policy: u16,
    pub reserved_u16: u16,
    pub target_function_id: [u8; 16],
    pub target_operator_class_id: [u8; 16],
    pub fingerprint: [u8; 32],
    pub callback: Option<RadixAbiPlannerSupportFnV1>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixPluginDescriptorV1 {
    pub header: RadixAbiHeaderV1,
    pub package_id: [u8; 16],
    pub package_name: RadixAbiStringV1,
    pub package_version: RadixAbiStringV1,
    pub abi_min_minor: u16,
    pub abi_max_minor: u16,
    pub reserved: u32,
    pub descriptor_fingerprint: [u8; 32],
    pub type_count: u32,
    pub reserved_types: u32,
    pub types: *const RadixAbiExternalTypeDescriptorV1,
    pub function_count: u32,
    pub reserved_functions: u32,
    pub functions: *const RadixAbiScalarFunctionDescriptorV1,
    pub operator_count: u32,
    pub reserved_operators: u32,
    pub operators: *const RadixAbiOperatorDescriptorV1,
    pub operator_class_count: u32,
    pub reserved_operator_classes: u32,
    pub operator_classes: *const RadixAbiOperatorClassDescriptorV1,
    pub planner_support_count: u32,
    pub reserved_planner_support: u32,
    pub planner_support: *const RadixAbiPlannerSupportDescriptorV1,
}

// SAFETY: these descriptor types are Sync only under the ABI contract that all
// pointed-to names and tables are immutable and have process lifetime. The
// generated SDK owns that proof; the PLUG-20 host validates the graph before
// publishing it. Call-scoped value/batch/context structs deliberately do not
// receive this implementation.
unsafe impl Sync for RadixAbiExternalTypeDescriptorV1 {}
// SAFETY: same immutable process-lifetime descriptor contract as above.
unsafe impl Sync for RadixAbiScalarFunctionDescriptorV1 {}
// SAFETY: same immutable process-lifetime descriptor contract as above.
unsafe impl Sync for RadixAbiOperatorDescriptorV1 {}
// SAFETY: same immutable process-lifetime descriptor contract as above.
unsafe impl Sync for RadixAbiOperatorClassDescriptorV1 {}
// SAFETY: same immutable process-lifetime descriptor contract as above.
unsafe impl Sync for RadixAbiPlannerSupportDescriptorV1 {}
// SAFETY: the package root and every reachable descriptor table are immutable
// and live until process exit.
unsafe impl Sync for RadixPluginDescriptorV1 {}
