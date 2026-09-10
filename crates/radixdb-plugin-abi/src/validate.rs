use core::{mem, slice, str};

use crate::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RadixAbiValidationError {
    UnsupportedMajor,
    UnsupportedMinor,
    StructTooSmall,
    UnknownFlags,
    NonZeroReserved,
    NullPointer,
    MisalignedPointer,
    LimitExceeded,
    InvalidBoolean,
    InvalidTypeReference,
    InvalidValue,
    InvalidBatchShape,
    InvalidStatus,
    InvalidUtf8,
    MissingCallback,
    InvalidDescriptor,
}

pub type RadixAbiValidationResult<T = ()> = Result<T, RadixAbiValidationError>;

pub const fn negotiate_minor(
    host_min: u16,
    host_max: u16,
    plugin_min: u16,
    plugin_max: u16,
) -> RadixAbiValidationResult<u16> {
    if host_min > host_max || plugin_min > plugin_max {
        return Err(RadixAbiValidationError::UnsupportedMinor);
    }
    let selected = if host_max < plugin_max {
        host_max
    } else {
        plugin_max
    };
    if selected < host_min || selected < plugin_min {
        Err(RadixAbiValidationError::UnsupportedMinor)
    } else {
        Ok(selected)
    }
}

pub const fn validate_header(
    header: &RadixAbiHeaderV1,
    mandatory_size: u32,
    allowed_flags: u64,
) -> RadixAbiValidationResult {
    if header.abi_major != RADIX_ABI_MAJOR {
        return Err(RadixAbiValidationError::UnsupportedMajor);
    }
    if header.abi_minor > RADIX_ABI_MINOR {
        return Err(RadixAbiValidationError::UnsupportedMinor);
    }
    if header.struct_size < mandatory_size {
        return Err(RadixAbiValidationError::StructTooSmall);
    }
    if header.flags & !allowed_flags != 0 {
        return Err(RadixAbiValidationError::UnknownFlags);
    }
    Ok(())
}

pub fn validate_slice(
    value: RadixAbiSliceV1,
    maximum: u32,
    alignment: usize,
) -> RadixAbiValidationResult {
    validate_pointer(value.ptr, value.len, maximum, alignment)?;
    if value.reserved != 0 {
        return Err(RadixAbiValidationError::NonZeroReserved);
    }
    Ok(())
}

pub fn validate_u32_slice(value: RadixAbiU32SliceV1, maximum: u32) -> RadixAbiValidationResult {
    validate_pointer(
        value.ptr.cast::<u8>(),
        value.len,
        maximum,
        mem::align_of::<u32>(),
    )?;
    if value.reserved != 0 {
        return Err(RadixAbiValidationError::NonZeroReserved);
    }
    Ok(())
}

fn validate_pointer<T>(
    pointer: *const T,
    length: u32,
    maximum: u32,
    alignment: usize,
) -> RadixAbiValidationResult {
    if length > maximum {
        return Err(RadixAbiValidationError::LimitExceeded);
    }
    if length != 0 && pointer.is_null() {
        return Err(RadixAbiValidationError::NullPointer);
    }
    if !pointer.is_null() && alignment > 1 && !pointer.addr().is_multiple_of(alignment) {
        return Err(RadixAbiValidationError::MisalignedPointer);
    }
    Ok(())
}

pub fn validate_type_ref(value: &RadixAbiTypeRefV1) -> RadixAbiValidationResult {
    match value.kind {
        RADIX_TYPE_REF_BUILTIN
            if (RADIX_BUILTIN_TYPE_MIN..=RADIX_BUILTIN_TYPE_MAX).contains(&value.builtin_tag)
                && value.codec_version == 0
                && value.object_id == [0; 16] =>
        {
            Ok(())
        }
        RADIX_TYPE_REF_EXTERNAL
            if value.builtin_tag == 0 && value.codec_version != 0 && value.object_id != [0; 16] =>
        {
            Ok(())
        }
        _ => Err(RadixAbiValidationError::InvalidTypeReference),
    }
}

pub fn validate_optional_type_ref(value: &RadixAbiTypeRefV1) -> RadixAbiValidationResult {
    if *value == RadixAbiTypeRefV1::ABSENT {
        Ok(())
    } else {
        validate_type_ref(value)
    }
}

pub fn validate_value(value: &RadixAbiValueV1) -> RadixAbiValidationResult {
    validate_type_ref(&value.type_ref)?;
    if value.flags & !RADIX_VALUE_FLAG_ALL != 0 || value.reserved != 0 {
        return Err(RadixAbiValidationError::InvalidValue);
    }
    let is_null = value.flags & RADIX_VALUE_FLAG_NULL != 0;
    if is_null {
        if value.inline_bytes != [0; 16]
            || !value.borrowed_bytes.ptr.is_null()
            || value.borrowed_bytes.len != 0
            || value.borrowed_bytes.reserved != 0
        {
            return Err(RadixAbiValidationError::InvalidValue);
        }
        return Ok(());
    }
    match value.type_ref.kind {
        RADIX_TYPE_REF_BUILTIN => match value.type_ref.builtin_tag {
            RADIX_BUILTIN_INTEGER | RADIX_BUILTIN_FLOAT | RADIX_BUILTIN_TIMESTAMP => {
                if value.inline_bytes[8..] != [0; 8] || !is_empty_slice(value.borrowed_bytes) {
                    return Err(RadixAbiValidationError::InvalidValue);
                }
            }
            RADIX_BUILTIN_BOOLEAN => {
                if value.inline_bytes[0] > 1
                    || value.inline_bytes[1..] != [0; 15]
                    || !is_empty_slice(value.borrowed_bytes)
                {
                    return Err(RadixAbiValidationError::InvalidValue);
                }
            }
            RADIX_BUILTIN_UUID => {
                if !is_empty_slice(value.borrowed_bytes) {
                    return Err(RadixAbiValidationError::InvalidValue);
                }
            }
            RADIX_BUILTIN_DATE => {
                if value.inline_bytes[4..] != [0; 12] || !is_empty_slice(value.borrowed_bytes) {
                    return Err(RadixAbiValidationError::InvalidValue);
                }
            }
            RADIX_BUILTIN_TEXT
            | RADIX_BUILTIN_JSON
            | RADIX_BUILTIN_VECTOR
            | RADIX_BUILTIN_DECIMAL
            | RADIX_BUILTIN_BYTES => {
                if value.inline_bytes != [0; 16] {
                    return Err(RadixAbiValidationError::InvalidValue);
                }
                validate_slice(value.borrowed_bytes, RADIX_MAX_EXTERNAL_VALUE_BYTES, 1)?;
            }
            _ => return Err(RadixAbiValidationError::InvalidValue),
        },
        RADIX_TYPE_REF_EXTERNAL => {
            if value.inline_bytes != [0; 16] {
                return Err(RadixAbiValidationError::InvalidValue);
            }
            validate_slice(value.borrowed_bytes, RADIX_MAX_EXTERNAL_VALUE_BYTES, 1)?;
        }
        _ => return Err(RadixAbiValidationError::InvalidValue),
    }
    Ok(())
}

/// Validate the contents of a structurally valid borrowed scalar payload.
///
/// # Safety
///
/// Every non-null `borrowed_bytes.ptr` must point to `borrowed_bytes.len`
/// initialized bytes and remain alive for this call. The bytes are never
/// retained.
pub unsafe fn validate_value_contents(value: &RadixAbiValueV1) -> RadixAbiValidationResult {
    validate_value(value)?;
    if value.flags & RADIX_VALUE_FLAG_NULL != 0 || value.type_ref.kind == RADIX_TYPE_REF_EXTERNAL {
        return Ok(());
    }
    let bytes = if value.borrowed_bytes.len == 0 {
        &[]
    } else {
        // SAFETY: guaranteed by this function's caller contract after the
        // structural pointer/length validation above.
        unsafe {
            slice::from_raw_parts(value.borrowed_bytes.ptr, value.borrowed_bytes.len as usize)
        }
    };
    match value.type_ref.builtin_tag {
        RADIX_BUILTIN_TEXT | RADIX_BUILTIN_JSON => {
            str::from_utf8(bytes).map_err(|_| RadixAbiValidationError::InvalidUtf8)?;
        }
        RADIX_BUILTIN_VECTOR if !bytes.len().is_multiple_of(mem::size_of::<f32>()) => {
            return Err(RadixAbiValidationError::InvalidValue);
        }
        RADIX_BUILTIN_DECIMAL if bytes.len() != 18 => {
            return Err(RadixAbiValidationError::InvalidValue);
        }
        _ => {}
    }
    Ok(())
}

pub fn validate_batch_view(value: &RadixAbiBatchViewV1) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiBatchViewV1>() as u32,
        0,
    )?;
    if value.row_count == u32::MAX {
        return Err(RadixAbiValidationError::LimitExceeded);
    }
    validate_descriptor_table(value.columns, value.column_count)
        .map_err(|_| RadixAbiValidationError::InvalidBatchShape)
}

pub fn validate_column_view(value: &RadixAbiColumnViewV1) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiColumnViewV1>() as u32,
        0,
    )?;
    validate_type_ref(&value.type_ref)?;
    if value.row_count == u32::MAX {
        return Err(RadixAbiValidationError::LimitExceeded);
    }
    if value.reserved_u16 != 0 {
        return Err(RadixAbiValidationError::NonZeroReserved);
    }
    let null_bytes = value
        .row_count
        .checked_add(7)
        .ok_or(RadixAbiValidationError::LimitExceeded)?
        / 8;
    if value.null_bitmap.len != 0 && value.null_bitmap.len != null_bytes {
        return Err(RadixAbiValidationError::InvalidBatchShape);
    }
    validate_slice(value.null_bitmap, null_bytes, 1)?;
    validate_slice(value.data, u32::MAX, 1)?;
    match value.layout {
        RADIX_COLUMN_LAYOUT_FIXED => {
            if value.element_width == 0
                || value.stride < u32::from(value.element_width)
                || value.alignment == 0
                || !value.alignment.is_power_of_two()
                || !is_empty_u32_slice(value.offsets)
            {
                return Err(RadixAbiValidationError::InvalidBatchShape);
            }
            let required = value
                .row_count
                .checked_mul(value.stride)
                .ok_or(RadixAbiValidationError::LimitExceeded)?;
            if value.data.len != required {
                return Err(RadixAbiValidationError::InvalidBatchShape);
            }
            validate_slice(value.data, u32::MAX, usize::from(value.alignment))?;
            validate_builtin_fixed_layout(value)?;
        }
        RADIX_COLUMN_LAYOUT_VARIABLE => {
            if value.element_width != 0
                || value.stride != 0
                || value.alignment != 1
                || value.offsets.len != value.row_count + 1
            {
                return Err(RadixAbiValidationError::InvalidBatchShape);
            }
            validate_u32_slice(value.offsets, value.row_count + 1)?;
            if value.type_ref.kind == RADIX_TYPE_REF_BUILTIN
                && !matches!(
                    value.type_ref.builtin_tag,
                    RADIX_BUILTIN_TEXT
                        | RADIX_BUILTIN_JSON
                        | RADIX_BUILTIN_VECTOR
                        | RADIX_BUILTIN_BYTES
                )
            {
                return Err(RadixAbiValidationError::InvalidBatchShape);
            }
        }
        _ => return Err(RadixAbiValidationError::InvalidBatchShape),
    }
    Ok(())
}

/// Validate offset contents after the host has established that the borrowed
/// `u32` range is readable for the duration of this call.
///
/// # Safety
///
/// `value.offsets.ptr` must point to `value.offsets.len` initialized `u32`
/// values and remain alive for the call. This function never retains it.
pub unsafe fn validate_column_offsets(value: &RadixAbiColumnViewV1) -> RadixAbiValidationResult {
    validate_column_view(value)?;
    if value.layout != RADIX_COLUMN_LAYOUT_VARIABLE {
        return Ok(());
    }
    // SAFETY: guaranteed by this function's caller contract after structural
    // pointer/length/alignment validation above.
    let offsets = unsafe { slice::from_raw_parts(value.offsets.ptr, value.offsets.len as usize) };
    if offsets.first().copied() != Some(0)
        || offsets.last().copied() != Some(value.data.len)
        || offsets.windows(2).any(|window| window[0] > window[1])
    {
        return Err(RadixAbiValidationError::InvalidBatchShape);
    }
    Ok(())
}

/// Validate every column after the host has established that the descriptor
/// table and all borrowed column buffers are readable for this call.
///
/// # Safety
///
/// `value.columns` must point to `value.column_count` initialized column
/// descriptors. Every pointer reachable from them must satisfy the safety
/// contract of [`validate_column_offsets`]. Nothing is retained.
pub unsafe fn validate_batch_columns(value: &RadixAbiBatchViewV1) -> RadixAbiValidationResult {
    validate_batch_view(value)?;
    let columns = if value.column_count == 0 {
        &[]
    } else {
        // SAFETY: guaranteed by this function's caller contract after the
        // structural pointer/length/alignment validation above.
        unsafe { slice::from_raw_parts(value.columns, value.column_count as usize) }
    };
    for column in columns {
        if column.row_count != value.row_count {
            return Err(RadixAbiValidationError::InvalidBatchShape);
        }
        // SAFETY: inherited from this function's caller contract.
        unsafe { validate_column_offsets(column)? };
    }
    Ok(())
}

pub fn validate_result_builder(value: &RadixAbiResultBuilderV1) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiResultBuilderV1>() as u32,
        0,
    )?;
    if value.handle == 0
        || value.max_bytes == 0
        || value.max_items == 0
        || value.write.is_none()
        || value.finish.is_none()
    {
        return Err(RadixAbiValidationError::MissingCallback);
    }
    Ok(())
}

pub fn validate_result_item(
    item_flags: u32,
    reserved: u32,
    bytes: RadixAbiSliceV1,
    maximum: u32,
) -> RadixAbiValidationResult {
    if item_flags & !RADIX_RESULT_ITEM_FLAG_ALL != 0 || reserved != 0 {
        return Err(RadixAbiValidationError::UnknownFlags);
    }
    if item_flags & RADIX_RESULT_ITEM_FLAG_NULL != 0 {
        if !is_empty_slice(bytes) {
            return Err(RadixAbiValidationError::InvalidValue);
        }
        return Ok(());
    }
    validate_slice(bytes, maximum, 1)
}

pub fn validate_hash_sink(value: &RadixAbiHashSinkV1) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiHashSinkV1>() as u32,
        0,
    )?;
    if value.handle == 0 {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    if value.max_components == 0
        || value.max_components > RADIX_MAX_HASH_COMPONENTS
        || value.max_bytes == 0
        || value.max_bytes > RADIX_MAX_HASH_BYTES
    {
        return Err(RadixAbiValidationError::LimitExceeded);
    }
    if value.append.is_none() {
        return Err(RadixAbiValidationError::MissingCallback);
    }
    Ok(())
}

pub fn validate_hash_component(
    component_kind: u16,
    reserved: u16,
    bytes: RadixAbiSliceV1,
    remaining_bytes: u32,
) -> RadixAbiValidationResult {
    if reserved != 0 {
        return Err(RadixAbiValidationError::NonZeroReserved);
    }
    match component_kind {
        RADIX_HASH_COMPONENT_BYTES | RADIX_HASH_COMPONENT_EXTERNAL => {
            validate_slice(bytes, remaining_bytes, 1)
        }
        RADIX_HASH_COMPONENT_I64 | RADIX_HASH_COMPONENT_U64 | RADIX_HASH_COMPONENT_F64_BITS => {
            validate_slice(bytes, remaining_bytes.min(8), 1)?;
            if bytes.len == 8 {
                Ok(())
            } else {
                Err(RadixAbiValidationError::InvalidValue)
            }
        }
        _ => Err(RadixAbiValidationError::InvalidValue),
    }
}

pub fn validate_diagnostic(value: &RadixAbiDiagnosticV1) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiDiagnosticV1>() as u32,
        0,
    )?;
    if !(RADIX_DIAGNOSTIC_INVALID_INPUT..=RADIX_DIAGNOSTIC_DEPENDENCY).contains(&value.category) {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    validate_failure_status(value.status)?;
    validate_slice(value.detail, RADIX_MAX_DIAGNOSTIC_BYTES, 1)?;
    validate_slice(value.field, RADIX_MAX_LOCAL_ID_BYTES, 1)
}

pub fn validate_diagnostic_sink(value: &RadixAbiDiagnosticSinkV1) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiDiagnosticSinkV1>() as u32,
        0,
    )?;
    if value.handle == 0 {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    if value.max_detail_bytes == 0 || value.max_detail_bytes > RADIX_MAX_DIAGNOSTIC_BYTES {
        return Err(RadixAbiValidationError::LimitExceeded);
    }
    if value.reserved != 0 {
        return Err(RadixAbiValidationError::NonZeroReserved);
    }
    if value.write.is_none() {
        return Err(RadixAbiValidationError::MissingCallback);
    }
    Ok(())
}

pub fn validate_call_context(value: &RadixAbiCallContextV1) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiCallContextV1>() as u32,
        0,
    )?;
    if value.handle == 0 || value.deadline_unix_ns == 0 {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    if value.max_output_bytes == 0 || value.max_work_units == 0 {
        return Err(RadixAbiValidationError::LimitExceeded);
    }
    if value.check_cancelled.is_none() || value.charge_work.is_none() || value.diagnostics.is_null()
    {
        return Err(RadixAbiValidationError::MissingCallback);
    }
    Ok(())
}

pub fn validate_host_api(value: &RadixHostApiV1) -> RadixAbiValidationResult {
    validate_header(&value.header, mem::size_of::<RadixHostApiV1>() as u32, 0)?;
    if value.handle == 0
        || value.max_external_value_bytes == 0
        || value.max_external_value_bytes > RADIX_MAX_EXTERNAL_VALUE_BYTES
        || value.max_batch_rows == 0
        || value.max_batch_rows == u32::MAX
        || value.max_planner_spans == 0
        || value.max_planner_spans > RADIX_MAX_PLANNER_SPANS
        || value.reserved != 0
        || value.log.is_none()
    {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    Ok(())
}

pub fn validate_log_record(
    level: u16,
    reserved: u16,
    message: RadixAbiStringV1,
) -> RadixAbiValidationResult {
    if reserved != 0 {
        return Err(RadixAbiValidationError::NonZeroReserved);
    }
    if !matches!(
        level,
        RADIX_LOG_INFO | RADIX_LOG_WARN | RADIX_LOG_ERROR | RADIX_LOG_DEBUG
    ) {
        return Err(RadixAbiValidationError::InvalidValue);
    }
    validate_slice(message, RADIX_MAX_DIAGNOSTIC_BYTES, 1)
}

pub fn validate_package_descriptor_shallow(
    value: &RadixPluginDescriptorV1,
) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixPluginDescriptorV1>() as u32,
        RADIX_PACKAGE_CAP_ALL,
    )?;
    if value.package_id == [0; 16]
        || value.descriptor_fingerprint == [0; 32]
        || value.abi_min_minor > value.abi_max_minor
        || value.abi_min_minor > RADIX_ABI_MINOR
        || value.reserved != 0
        || value.reserved_types != 0
        || value.reserved_functions != 0
        || value.reserved_operators != 0
        || value.reserved_operator_classes != 0
        || value.reserved_planner_support != 0
        || (value.header.flags & RADIX_PACKAGE_CAP_BATCH_FUNCTIONS != 0
            && value.function_count == 0)
    {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    validate_slice(value.package_name, RADIX_MAX_LOCAL_ID_BYTES, 1)?;
    validate_slice(value.package_version, 64, 1)?;
    if value.package_name.len == 0 || value.package_version.len == 0 {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    validate_descriptor_table(value.types, value.type_count)?;
    validate_descriptor_table(value.functions, value.function_count)?;
    validate_descriptor_table(value.operators, value.operator_count)?;
    validate_descriptor_table(value.operator_classes, value.operator_class_count)?;
    validate_descriptor_table(value.planner_support, value.planner_support_count)?;
    validate_capability_count(
        value.header.flags,
        RADIX_PACKAGE_CAP_EXTERNAL_TYPES,
        value.type_count,
    )?;
    validate_capability_count(
        value.header.flags,
        RADIX_PACKAGE_CAP_SCALAR_FUNCTIONS,
        value.function_count,
    )?;
    validate_capability_count(
        value.header.flags,
        RADIX_PACKAGE_CAP_OPERATORS,
        value.operator_count,
    )?;
    validate_capability_count(
        value.header.flags,
        RADIX_PACKAGE_CAP_OPERATOR_CLASSES,
        value.operator_class_count,
    )?;
    validate_capability_count(
        value.header.flags,
        RADIX_PACKAGE_CAP_PLANNER_SUPPORT,
        value.planner_support_count,
    )?;
    Ok(())
}

pub fn validate_external_type_descriptor(
    value: &RadixAbiExternalTypeDescriptorV1,
) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiExternalTypeDescriptorV1>() as u32,
        0,
    )?;
    if value.object_id == [0; 16]
        || value.codec_version == 0
        || value.semantic_revision == 0
        || value.max_bytes == 0
        || value.max_bytes > RADIX_MAX_EXTERNAL_VALUE_BYTES
        || value.reserved_u16 != 0
        || value.reserved_u32 != 0
        || value.capabilities & !RADIX_TYPE_CAP_ALL != 0
        || value.codec_fingerprint == [0; 32]
        || value.encode.is_none()
        || value.decode.is_none()
    {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    validate_non_empty_string(value.local_id, RADIX_MAX_LOCAL_ID_BYTES)?;
    validate_non_empty_string(value.display_name, RADIX_MAX_LOCAL_ID_BYTES)?;
    match value.storage_kind {
        RADIX_EXTERNAL_STORAGE_FIXED
            if value.fixed_bytes != 0 && value.fixed_bytes == value.max_bytes => {}
        RADIX_EXTERNAL_STORAGE_VARIABLE if value.fixed_bytes == 0 => {}
        _ => return Err(RadixAbiValidationError::InvalidDescriptor),
    }
    validate_capability_callback(
        value.capabilities,
        RADIX_TYPE_CAP_EQUALITY,
        value.equality.is_some(),
    )?;
    validate_capability_callback(
        value.capabilities,
        RADIX_TYPE_CAP_HASH,
        value.hash.is_some(),
    )?;
    validate_capability_callback(
        value.capabilities,
        RADIX_TYPE_CAP_ORDERING,
        value.ordering.is_some(),
    )?;
    validate_capability_callback(
        value.capabilities,
        RADIX_TYPE_CAP_TEXT_INPUT,
        value.text_input.is_some(),
    )?;
    validate_capability_callback(
        value.capabilities,
        RADIX_TYPE_CAP_TEXT_OUTPUT,
        value.text_output.is_some(),
    )?;
    validate_capability_callback(
        value.capabilities,
        RADIX_TYPE_CAP_BINARY_INPUT,
        value.binary_input.is_some(),
    )?;
    validate_capability_callback(
        value.capabilities,
        RADIX_TYPE_CAP_BINARY_OUTPUT,
        value.binary_output.is_some(),
    )?;
    if value.capabilities & RADIX_TYPE_CAP_HASH != 0
        && value.capabilities & RADIX_TYPE_CAP_EQUALITY == 0
    {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    Ok(())
}

pub fn validate_scalar_function_descriptor(
    value: &RadixAbiScalarFunctionDescriptorV1,
) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiScalarFunctionDescriptorV1>() as u32,
        0,
    )?;
    if value.object_id == [0; 16]
        || value.semantic_revision == 0
        || value.argument_count > RADIX_MAX_FUNCTION_ARGUMENTS
        || !matches!(
            value.volatility,
            RADIX_VOLATILITY_IMMUTABLE | RADIX_VOLATILITY_STABLE | RADIX_VOLATILITY_VOLATILE
        )
        || value.strict > 1
        || value.parallel_safe > 1
        || value.cancellation != RADIX_CANCELLATION_BOUNDED
        || value.reserved_u16 != 0
        || value.cost == 0
        || value.cost > 1_000_000
        || value.max_output_bytes == 0
        || value.scalar.is_none()
    {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    validate_non_empty_string(value.local_id, RADIX_MAX_LOCAL_ID_BYTES)?;
    validate_non_empty_string(value.display_name, RADIX_MAX_LOCAL_ID_BYTES)?;
    validate_descriptor_table(value.arguments, value.argument_count)?;
    validate_type_ref(&value.result)
}

pub fn validate_operator_descriptor(
    value: &RadixAbiOperatorDescriptorV1,
) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiOperatorDescriptorV1>() as u32,
        0,
    )?;
    if value.object_id == [0; 16]
        || value.function_id == [0; 16]
        || value.semantic_revision == 0
        || value.reserved != 0
    {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    validate_non_empty_string(value.local_id, RADIX_MAX_LOCAL_ID_BYTES)?;
    validate_non_empty_string(value.symbol, 3)?;
    validate_optional_type_ref(&value.left)?;
    validate_type_ref(&value.right)?;
    validate_type_ref(&value.result)
}

pub fn validate_operator_class_descriptor(
    value: &RadixAbiOperatorClassDescriptorV1,
) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiOperatorClassDescriptorV1>() as u32,
        0,
    )?;
    if value.object_id == [0; 16]
        || value.semantic_revision == 0
        || !matches!(
            value.access_method,
            RADIX_ACCESS_METHOD_BTREE
                | RADIX_ACCESS_METHOD_HASH
                | RADIX_ACCESS_METHOD_BITMAP
                | RADIX_ACCESS_METHOD_HNSW
        )
        || value.reserved_u16 != 0
        || value.reserved_u32 != 0
        || value.key_codec_revision == 0
        || value.fingerprint == [0; 32]
        || value.encode_key.is_none()
        || value.strategy_count > 1024
        || value.support_count > 1024
    {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    validate_non_empty_string(value.local_id, RADIX_MAX_LOCAL_ID_BYTES)?;
    validate_type_ref(&value.input_type)?;
    validate_type_ref(&value.key_type)?;
    validate_descriptor_table(value.strategies, value.strategy_count)?;
    validate_descriptor_table(value.supports, value.support_count)
}

pub fn validate_planner_support_descriptor(
    value: &RadixAbiPlannerSupportDescriptorV1,
) -> RadixAbiValidationResult {
    validate_header(
        &value.header,
        mem::size_of::<RadixAbiPlannerSupportDescriptorV1>() as u32,
        0,
    )?;
    if value.object_id == [0; 16]
        || value.semantic_revision == 0
        || value.max_spans == 0
        || value.max_spans > RADIX_MAX_PLANNER_SPANS
        || value.max_output_bytes == 0
        || value.max_output_bytes > RADIX_MAX_EXTERNAL_VALUE_BYTES
        || !matches!(
            value.recheck_policy,
            RADIX_RECHECK_EXACT | RADIX_RECHECK_ALWAYS
        )
        || value.reserved_u16 != 0
        || (value.target_function_id == [0; 16] && value.target_operator_class_id == [0; 16])
        || value.fingerprint == [0; 32]
        || value.callback.is_none()
    {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    validate_non_empty_string(value.local_id, RADIX_MAX_LOCAL_ID_BYTES)
}

fn validate_non_empty_string(value: RadixAbiStringV1, maximum: u32) -> RadixAbiValidationResult {
    validate_slice(value, maximum, 1)?;
    if value.len == 0 {
        return Err(RadixAbiValidationError::InvalidDescriptor);
    }
    Ok(())
}

fn validate_capability_callback(
    capabilities: u64,
    capability: u64,
    callback_present: bool,
) -> RadixAbiValidationResult {
    if (capabilities & capability != 0) != callback_present {
        Err(RadixAbiValidationError::MissingCallback)
    } else {
        Ok(())
    }
}

fn validate_capability_count(
    capabilities: u64,
    capability: u64,
    count: u32,
) -> RadixAbiValidationResult {
    if (capabilities & capability != 0) != (count != 0) {
        Err(RadixAbiValidationError::InvalidDescriptor)
    } else {
        Ok(())
    }
}

fn validate_descriptor_table<T>(pointer: *const T, count: u32) -> RadixAbiValidationResult {
    validate_pointer(
        pointer.cast::<u8>(),
        count,
        RADIX_MAX_DESCRIPTOR_ENTRIES,
        mem::align_of::<T>(),
    )
}

fn validate_failure_status(status: RadixAbiStatusV1) -> RadixAbiValidationResult {
    if (RADIX_STATUS_INVALID_ARGUMENT..=RADIX_STATUS_DEPENDENCY_MISSING).contains(&status) {
        Ok(())
    } else {
        Err(RadixAbiValidationError::InvalidStatus)
    }
}

fn validate_builtin_fixed_layout(value: &RadixAbiColumnViewV1) -> RadixAbiValidationResult {
    if value.type_ref.kind != RADIX_TYPE_REF_BUILTIN {
        return Ok(());
    }
    let expected_width = match value.type_ref.builtin_tag {
        RADIX_BUILTIN_INTEGER | RADIX_BUILTIN_FLOAT | RADIX_BUILTIN_TIMESTAMP => 8,
        RADIX_BUILTIN_BOOLEAN => 1,
        RADIX_BUILTIN_UUID => 16,
        RADIX_BUILTIN_DECIMAL => 18,
        RADIX_BUILTIN_DATE => 4,
        RADIX_BUILTIN_TEXT | RADIX_BUILTIN_JSON | RADIX_BUILTIN_VECTOR | RADIX_BUILTIN_BYTES => {
            return Err(RadixAbiValidationError::InvalidBatchShape)
        }
        _ => return Err(RadixAbiValidationError::InvalidBatchShape),
    };
    if value.element_width != expected_width {
        return Err(RadixAbiValidationError::InvalidBatchShape);
    }
    Ok(())
}

fn is_empty_slice(value: RadixAbiSliceV1) -> bool {
    value.ptr.is_null() && value.len == 0 && value.reserved == 0
}

fn is_empty_u32_slice(value: RadixAbiU32SliceV1) -> bool {
    value.ptr.is_null() && value.len == 0 && value.reserved == 0
}
