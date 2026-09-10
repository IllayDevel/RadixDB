use crate::RadixAbiStatusV1;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RadixAbiHeaderV1 {
    pub abi_major: u16,
    pub abi_minor: u16,
    pub struct_size: u32,
    pub flags: u64,
}

impl RadixAbiHeaderV1 {
    pub const fn new<T>(flags: u64) -> Self {
        Self {
            abi_major: crate::RADIX_ABI_MAJOR,
            abi_minor: crate::RADIX_ABI_MINOR,
            struct_size: core::mem::size_of::<T>() as u32,
            flags,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RadixAbiSliceV1 {
    pub ptr: *const u8,
    pub len: u32,
    pub reserved: u32,
}

impl RadixAbiSliceV1 {
    pub const EMPTY: Self = Self {
        ptr: core::ptr::null(),
        len: 0,
        reserved: 0,
    };

    pub const fn from_static(bytes: &'static [u8]) -> Self {
        assert!(bytes.len() <= u32::MAX as usize);
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len() as u32,
            reserved: 0,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RadixAbiU32SliceV1 {
    pub ptr: *const u32,
    pub len: u32,
    pub reserved: u32,
}

pub type RadixAbiStringV1 = RadixAbiSliceV1;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RadixAbiTypeRefV1 {
    pub kind: u16,
    pub builtin_tag: u16,
    pub codec_version: u32,
    pub object_id: [u8; 16],
}

impl RadixAbiTypeRefV1 {
    pub const ABSENT: Self = Self {
        kind: 0,
        builtin_tag: 0,
        codec_version: 0,
        object_id: [0; 16],
    };

    pub const fn builtin(tag: u16) -> Self {
        Self {
            kind: crate::RADIX_TYPE_REF_BUILTIN,
            builtin_tag: tag,
            codec_version: 0,
            object_id: [0; 16],
        }
    }

    pub const fn external(object_id: [u8; 16], codec_version: u32) -> Self {
        Self {
            kind: crate::RADIX_TYPE_REF_EXTERNAL,
            builtin_tag: 0,
            codec_version,
            object_id,
        }
    }
}

/// Borrowed scalar view. Fixed built-ins use `inline_bytes`; variable built-ins
/// and external values use `borrowed_bytes`. A null value zeros both regions.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RadixAbiValueV1 {
    pub type_ref: RadixAbiTypeRefV1,
    pub flags: u32,
    pub reserved: u32,
    pub inline_bytes: [u8; 16],
    pub borrowed_bytes: RadixAbiSliceV1,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RadixAbiColumnViewV1 {
    pub header: RadixAbiHeaderV1,
    pub type_ref: RadixAbiTypeRefV1,
    pub row_count: u32,
    pub layout: u16,
    pub element_width: u16,
    pub alignment: u16,
    pub reserved_u16: u16,
    pub stride: u32,
    pub null_bitmap: RadixAbiSliceV1,
    pub data: RadixAbiSliceV1,
    pub offsets: RadixAbiU32SliceV1,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RadixAbiBatchViewV1 {
    pub header: RadixAbiHeaderV1,
    pub row_count: u32,
    pub column_count: u32,
    pub columns: *const RadixAbiColumnViewV1,
}

pub type RadixAbiBuilderWriteFnV1 = unsafe extern "C" fn(
    handle: u64,
    item_flags: u32,
    reserved: u32,
    bytes: RadixAbiSliceV1,
) -> RadixAbiStatusV1;
pub type RadixAbiBuilderFinishFnV1 = unsafe extern "C" fn(handle: u64) -> RadixAbiStatusV1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiResultBuilderV1 {
    pub header: RadixAbiHeaderV1,
    pub handle: u64,
    pub max_bytes: u32,
    pub max_items: u32,
    pub write: Option<RadixAbiBuilderWriteFnV1>,
    pub finish: Option<RadixAbiBuilderFinishFnV1>,
}

pub type RadixAbiHashAppendFnV1 = unsafe extern "C" fn(
    handle: u64,
    component_kind: u16,
    reserved: u16,
    bytes: RadixAbiSliceV1,
) -> RadixAbiStatusV1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiHashSinkV1 {
    pub header: RadixAbiHeaderV1,
    pub handle: u64,
    pub max_components: u32,
    pub max_bytes: u32,
    pub append: Option<RadixAbiHashAppendFnV1>,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RadixAbiDiagnosticV1 {
    pub header: RadixAbiHeaderV1,
    pub category: u32,
    pub status: RadixAbiStatusV1,
    pub detail: RadixAbiStringV1,
    pub field: RadixAbiStringV1,
}

pub type RadixAbiDiagnosticWriteFnV1 =
    unsafe extern "C" fn(handle: u64, diagnostic: *const RadixAbiDiagnosticV1) -> RadixAbiStatusV1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiDiagnosticSinkV1 {
    pub header: RadixAbiHeaderV1,
    pub handle: u64,
    pub max_detail_bytes: u32,
    pub reserved: u32,
    pub write: Option<RadixAbiDiagnosticWriteFnV1>,
}

pub type RadixAbiCheckCancelledFnV1 = unsafe extern "C" fn(handle: u64) -> RadixAbiStatusV1;
pub type RadixAbiChargeWorkFnV1 = unsafe extern "C" fn(handle: u64, units: u32) -> RadixAbiStatusV1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixAbiCallContextV1 {
    pub header: RadixAbiHeaderV1,
    pub handle: u64,
    pub deadline_unix_ns: u64,
    pub max_output_bytes: u32,
    pub max_work_units: u32,
    pub check_cancelled: Option<RadixAbiCheckCancelledFnV1>,
    pub charge_work: Option<RadixAbiChargeWorkFnV1>,
    pub diagnostics: *const RadixAbiDiagnosticSinkV1,
}

pub type RadixAbiHostLogFnV1 = unsafe extern "C" fn(
    handle: u64,
    level: u16,
    reserved: u16,
    message: RadixAbiStringV1,
) -> RadixAbiStatusV1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RadixHostApiV1 {
    pub header: RadixAbiHeaderV1,
    pub handle: u64,
    pub max_external_value_bytes: u32,
    pub max_batch_rows: u32,
    pub max_planner_spans: u32,
    pub reserved: u32,
    pub log: Option<RadixAbiHostLogFnV1>,
}
