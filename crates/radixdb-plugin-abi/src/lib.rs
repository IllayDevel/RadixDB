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

//! Stable low-level C ABI shared by the RadixDB plugin host and trusted native
//! plugins.
//!
//! This crate deliberately has no dependency on an engine crate and exports no
//! Rust-owned allocation across the boundary. The safe authoring API lives in
//! `radixdb-plugin`; this crate is the generated bridge and C-oracle surface.

#![no_std]
#![forbid(unsafe_op_in_unsafe_fn)]

#[cfg(not(all(
    target_arch = "x86_64",
    target_os = "linux",
    target_env = "gnu",
    target_pointer_width = "64"
)))]
compile_error!("radixdb-plugin-abi 1.0 supports only x86_64-unknown-linux-gnu");

mod constants;
mod descriptor;
mod raw;
mod validate;

pub use constants::*;
pub use descriptor::*;
pub use raw::*;
pub use validate::*;

/// Exact entrypoint symbol required from every ABI-major-1 shared library.
pub const ENTRYPOINT_SYMBOL_V1: &[u8] = b"radixdb_plugin_entry_v1\0";

/// Function type of the only exported plugin symbol.
pub type RadixPluginEntrypointV1 = unsafe extern "C" fn(
    host: *const RadixHostApiV1,
    status: *mut RadixAbiStatusV1,
) -> *const RadixPluginDescriptorV1;

const _: () = {
    use core::mem::{align_of, offset_of, size_of};

    assert!(size_of::<RadixAbiHeaderV1>() == 16);
    assert!(align_of::<RadixAbiHeaderV1>() == 8);
    assert!(offset_of!(RadixAbiHeaderV1, flags) == 8);
    assert!(size_of::<RadixAbiSliceV1>() == 16);
    assert!(align_of::<RadixAbiSliceV1>() == 8);
    assert!(offset_of!(RadixAbiSliceV1, len) == 8);
    assert!(size_of::<RadixAbiTypeRefV1>() == 24);
    assert!(align_of::<RadixAbiTypeRefV1>() == 4);
    assert!(offset_of!(RadixAbiTypeRefV1, object_id) == 8);
    assert!(size_of::<RadixAbiValueV1>() == 64);
    assert!(align_of::<RadixAbiValueV1>() == 8);
    assert!(offset_of!(RadixAbiValueV1, borrowed_bytes) == 48);
    assert!(size_of::<RadixAbiColumnViewV1>() == 104);
    assert!(align_of::<RadixAbiColumnViewV1>() == 8);
    assert!(offset_of!(RadixAbiColumnViewV1, null_bitmap) == 56);
    assert!(offset_of!(RadixAbiColumnViewV1, offsets) == 88);
    assert!(size_of::<RadixAbiBatchViewV1>() == 32);
    assert!(align_of::<RadixAbiBatchViewV1>() == 8);
    assert!(offset_of!(RadixAbiBatchViewV1, columns) == 24);
    assert!(size_of::<RadixAbiResultBuilderV1>() == 48);
    assert!(align_of::<RadixAbiResultBuilderV1>() == 8);
    assert!(offset_of!(RadixAbiResultBuilderV1, write) == 32);
    assert!(size_of::<RadixAbiHashSinkV1>() == 40);
    assert!(align_of::<RadixAbiHashSinkV1>() == 8);
    assert!(offset_of!(RadixAbiHashSinkV1, append) == 32);
    assert!(size_of::<RadixAbiDiagnosticV1>() == 56);
    assert!(align_of::<RadixAbiDiagnosticV1>() == 8);
    assert!(offset_of!(RadixAbiDiagnosticV1, detail) == 24);
    assert!(size_of::<RadixAbiDiagnosticSinkV1>() == 40);
    assert!(align_of::<RadixAbiDiagnosticSinkV1>() == 8);
    assert!(offset_of!(RadixAbiDiagnosticSinkV1, write) == 32);
    assert!(size_of::<RadixAbiCallContextV1>() == 64);
    assert!(align_of::<RadixAbiCallContextV1>() == 8);
    assert!(offset_of!(RadixAbiCallContextV1, diagnostics) == 56);
    assert!(size_of::<RadixHostApiV1>() == 48);
    assert!(align_of::<RadixHostApiV1>() == 8);
    assert!(offset_of!(RadixHostApiV1, log) == 40);
    assert!(size_of::<RadixAbiBindingEntryV1>() == 20);
    assert!(align_of::<RadixAbiBindingEntryV1>() == 2);
    assert!(offset_of!(RadixAbiBindingEntryV1, object_id) == 4);
    assert!(size_of::<RadixAbiExternalTypeDescriptorV1>() == 200);
    assert!(align_of::<RadixAbiExternalTypeDescriptorV1>() == 8);
    assert!(offset_of!(RadixAbiExternalTypeDescriptorV1, capabilities) == 88);
    assert!(offset_of!(RadixAbiExternalTypeDescriptorV1, encode) == 128);
    assert!(size_of::<RadixAbiScalarFunctionDescriptorV1>() == 136);
    assert!(align_of::<RadixAbiScalarFunctionDescriptorV1>() == 8);
    assert!(offset_of!(RadixAbiScalarFunctionDescriptorV1, volatility) == 104);
    assert!(offset_of!(RadixAbiScalarFunctionDescriptorV1, scalar) == 120);
    assert!(size_of::<RadixAbiOperatorDescriptorV1>() == 160);
    assert!(align_of::<RadixAbiOperatorDescriptorV1>() == 8);
    assert!(offset_of!(RadixAbiOperatorDescriptorV1, function_id) == 144);
    assert!(size_of::<RadixAbiOperatorClassDescriptorV1>() == 176);
    assert!(align_of::<RadixAbiOperatorClassDescriptorV1>() == 8);
    assert!(offset_of!(RadixAbiOperatorClassDescriptorV1, strategies) == 112);
    assert!(offset_of!(RadixAbiOperatorClassDescriptorV1, encode_key) == 168);
    assert!(size_of::<RadixAbiPlannerSupportDescriptorV1>() == 136);
    assert!(align_of::<RadixAbiPlannerSupportDescriptorV1>() == 8);
    assert!(offset_of!(RadixAbiPlannerSupportDescriptorV1, callback) == 128);
    assert!(size_of::<RadixPluginDescriptorV1>() == 184);
    assert!(align_of::<RadixPluginDescriptorV1>() == 8);
    assert!(offset_of!(RadixPluginDescriptorV1, descriptor_fingerprint) == 72);
    assert!(offset_of!(RadixPluginDescriptorV1, planner_support) == 176);
};
