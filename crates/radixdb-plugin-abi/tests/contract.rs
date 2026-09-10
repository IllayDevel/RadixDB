use std::mem::{align_of, offset_of, size_of};

use radixdb_plugin_abi::*;

unsafe extern "C" fn write_ok(_: u64, _: u32, _: u32, _: RadixAbiSliceV1) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn finish_ok(_: u64) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn codec_ok(
    _: *const RadixAbiCallContextV1,
    _: *const RadixAbiValueV1,
    _: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn parse_ok(
    _: *const RadixAbiCallContextV1,
    _: RadixAbiSliceV1,
    _: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn scalar_ok(
    _: *const RadixAbiCallContextV1,
    _: *const RadixAbiValueV1,
    _: u32,
    _: *const RadixAbiResultBuilderV1,
) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn append_ok(_: u64, _: u16, _: u16, _: RadixAbiSliceV1) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn diagnostic_ok(_: u64, _: *const RadixAbiDiagnosticV1) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn check_ok(_: u64) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn charge_ok(_: u64, _: u32) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

unsafe extern "C" fn log_ok(_: u64, _: u16, _: u16, _: RadixAbiStringV1) -> RadixAbiStatusV1 {
    RADIX_STATUS_OK
}

#[test]
fn rust_layout_matches_the_c_oracle_contract() {
    assert_eq!(size_of::<RadixAbiHeaderV1>(), 16);
    assert_eq!(align_of::<RadixAbiHeaderV1>(), 8);
    assert_eq!(offset_of!(RadixAbiHeaderV1, flags), 8);
    assert_eq!(size_of::<RadixAbiSliceV1>(), 16);
    assert_eq!(size_of::<RadixAbiU32SliceV1>(), 16);
    assert_eq!(size_of::<RadixAbiTypeRefV1>(), 24);
    assert_eq!(size_of::<RadixAbiValueV1>(), 64);
    assert_eq!(offset_of!(RadixAbiValueV1, borrowed_bytes), 48);
    assert_eq!(size_of::<RadixAbiColumnViewV1>(), 104);
    assert_eq!(size_of::<RadixAbiBatchViewV1>(), 32);
    assert_eq!(size_of::<RadixAbiResultBuilderV1>(), 48);
    assert_eq!(size_of::<RadixAbiHashSinkV1>(), 40);
    assert_eq!(size_of::<RadixAbiDiagnosticV1>(), 56);
    assert_eq!(size_of::<RadixAbiDiagnosticSinkV1>(), 40);
    assert_eq!(size_of::<RadixAbiCallContextV1>(), 64);
    assert_eq!(size_of::<RadixHostApiV1>(), 48);
    assert_eq!(size_of::<RadixAbiBindingEntryV1>(), 20);
    assert_eq!(size_of::<RadixAbiExternalTypeDescriptorV1>(), 200);
    assert_eq!(size_of::<RadixAbiScalarFunctionDescriptorV1>(), 136);
    assert_eq!(size_of::<RadixAbiOperatorDescriptorV1>(), 160);
    assert_eq!(size_of::<RadixAbiOperatorClassDescriptorV1>(), 176);
    assert_eq!(size_of::<RadixAbiPlannerSupportDescriptorV1>(), 136);
    assert_eq!(size_of::<RadixPluginDescriptorV1>(), 184);
}

#[test]
fn header_accepts_a_compatible_tail_and_rejects_mandatory_unknown_data() {
    let future_tail = RadixAbiHeaderV1 {
        abi_major: RADIX_ABI_MAJOR,
        abi_minor: RADIX_ABI_MINOR,
        struct_size: size_of::<RadixAbiHeaderV1>() as u32 + 64,
        flags: 0,
    };
    assert_eq!(
        validate_header(&future_tail, size_of::<RadixAbiHeaderV1>() as u32, 0),
        Ok(())
    );

    let unknown_flag = RadixAbiHeaderV1 {
        flags: 1 << 63,
        ..future_tail
    };
    assert_eq!(
        validate_header(&unknown_flag, size_of::<RadixAbiHeaderV1>() as u32, 0),
        Err(RadixAbiValidationError::UnknownFlags)
    );
    assert_eq!(negotiate_minor(0, 2, 0, 1), Ok(1));
    assert_eq!(
        negotiate_minor(2, 3, 0, 1),
        Err(RadixAbiValidationError::UnsupportedMinor)
    );
}

#[test]
fn scalar_values_keep_builtin_and_external_storage_disjoint() {
    let builtin = RadixAbiValueV1 {
        type_ref: RadixAbiTypeRefV1::builtin(1),
        flags: 0,
        reserved: 0,
        inline_bytes: 42_i64
            .to_le_bytes()
            .into_iter()
            .chain([0; 8])
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
        borrowed_bytes: RadixAbiSliceV1::EMPTY,
    };
    assert_eq!(validate_value(&builtin), Ok(()));

    static PAYLOAD: &[u8] = b"canonical";
    let external = RadixAbiValueV1 {
        type_ref: RadixAbiTypeRefV1::external([7; 16], 3),
        flags: 0,
        reserved: 0,
        inline_bytes: [0; 16],
        borrowed_bytes: RadixAbiSliceV1::from_static(PAYLOAD),
    };
    assert_eq!(validate_value(&external), Ok(()));

    let disguised = RadixAbiValueV1 {
        type_ref: RadixAbiTypeRefV1::builtin(1),
        borrowed_bytes: RadixAbiSliceV1::from_static(PAYLOAD),
        ..external
    };
    assert_eq!(
        validate_value(&disguised),
        Err(RadixAbiValidationError::InvalidValue)
    );
}

#[test]
fn borrowed_builtin_payloads_have_explicit_semantic_shape() {
    let valid_vector = [0_u8; 8];
    let vector = RadixAbiValueV1 {
        type_ref: RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_VECTOR),
        flags: 0,
        reserved: 0,
        inline_bytes: [0; 16],
        borrowed_bytes: RadixAbiSliceV1 {
            ptr: valid_vector.as_ptr(),
            len: valid_vector.len() as u32,
            reserved: 0,
        },
    };
    // SAFETY: the borrowed payload is backed by a live local array.
    assert_eq!(unsafe { validate_value_contents(&vector) }, Ok(()));

    let invalid_utf8 = [0xff_u8];
    let text = RadixAbiValueV1 {
        type_ref: RadixAbiTypeRefV1::builtin(RADIX_BUILTIN_TEXT),
        borrowed_bytes: RadixAbiSliceV1 {
            ptr: invalid_utf8.as_ptr(),
            len: invalid_utf8.len() as u32,
            reserved: 0,
        },
        ..vector
    };
    // SAFETY: the intentionally malformed bytes remain readable.
    assert_eq!(
        unsafe { validate_value_contents(&text) },
        Err(RadixAbiValidationError::InvalidUtf8)
    );
}

#[test]
fn batch_contract_checks_lsb_bitmap_shape_and_monotonic_u32_offsets() {
    let nulls = [0b0000_0101_u8];
    let data = *b"abcdef";
    let offsets = [0_u32, 2, 2, 6];
    let column = RadixAbiColumnViewV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiColumnViewV1>(0),
        type_ref: RadixAbiTypeRefV1::external([9; 16], 1),
        row_count: 3,
        layout: RADIX_COLUMN_LAYOUT_VARIABLE,
        element_width: 0,
        alignment: 1,
        reserved_u16: 0,
        stride: 0,
        null_bitmap: RadixAbiSliceV1 {
            ptr: nulls.as_ptr(),
            len: 1,
            reserved: 0,
        },
        data: RadixAbiSliceV1 {
            ptr: data.as_ptr(),
            len: data.len() as u32,
            reserved: 0,
        },
        offsets: RadixAbiU32SliceV1 {
            ptr: offsets.as_ptr(),
            len: offsets.len() as u32,
            reserved: 0,
        },
    };
    assert_eq!(validate_column_view(&column), Ok(()));
    // SAFETY: all three arrays outlive this call and the u32 slice is aligned.
    assert_eq!(unsafe { validate_column_offsets(&column) }, Ok(()));
    let batch = RadixAbiBatchViewV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiBatchViewV1>(0),
        row_count: 3,
        column_count: 1,
        columns: &column,
    };
    // SAFETY: the column table and every referenced buffer outlive this call.
    assert_eq!(unsafe { validate_batch_columns(&batch) }, Ok(()));

    let bad_offsets = [0_u32, 4, 3, 6];
    let malformed = RadixAbiColumnViewV1 {
        offsets: RadixAbiU32SliceV1 {
            ptr: bad_offsets.as_ptr(),
            len: bad_offsets.len() as u32,
            reserved: 0,
        },
        ..column
    };
    // SAFETY: the forged shape is still backed by a readable aligned array.
    assert_eq!(
        unsafe { validate_column_offsets(&malformed) },
        Err(RadixAbiValidationError::InvalidBatchShape)
    );
    assert_eq!(nulls[0] & (1 << 2), 1 << 2, "row 2 is LSB-first bit 2");
}

#[test]
fn builders_are_host_owned_and_require_both_callbacks() {
    let builder = RadixAbiResultBuilderV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiResultBuilderV1>(0),
        handle: 17,
        max_bytes: 1024,
        max_items: 8,
        write: Some(write_ok),
        finish: Some(finish_ok),
    };
    assert_eq!(validate_result_builder(&builder), Ok(()));
    assert_eq!(
        validate_result_item(RADIX_RESULT_ITEM_FLAG_NULL, 0, RadixAbiSliceV1::EMPTY, 1024),
        Ok(())
    );
    assert_eq!(
        validate_result_item(
            RADIX_RESULT_ITEM_FLAG_NULL,
            0,
            RadixAbiSliceV1::from_static(b"not-null"),
            1024
        ),
        Err(RadixAbiValidationError::InvalidValue)
    );
    let missing = RadixAbiResultBuilderV1 {
        finish: None,
        ..builder
    };
    assert_eq!(
        validate_result_builder(&missing),
        Err(RadixAbiValidationError::MissingCallback)
    );
}

#[test]
fn diagnostic_cancellation_hash_and_host_tables_are_bounded() {
    let diagnostics = RadixAbiDiagnosticSinkV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiDiagnosticSinkV1>(0),
        handle: 1,
        max_detail_bytes: RADIX_MAX_DIAGNOSTIC_BYTES,
        reserved: 0,
        write: Some(diagnostic_ok),
    };
    assert_eq!(validate_diagnostic_sink(&diagnostics), Ok(()));

    let context = RadixAbiCallContextV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiCallContextV1>(0),
        handle: 2,
        deadline_unix_ns: 1,
        max_output_bytes: 1024,
        max_work_units: 100,
        check_cancelled: Some(check_ok),
        charge_work: Some(charge_ok),
        diagnostics: &diagnostics,
    };
    assert_eq!(validate_call_context(&context), Ok(()));

    let sink = RadixAbiHashSinkV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiHashSinkV1>(0),
        handle: 3,
        max_components: RADIX_MAX_HASH_COMPONENTS,
        max_bytes: RADIX_MAX_HASH_BYTES,
        append: Some(append_ok),
    };
    assert_eq!(validate_hash_sink(&sink), Ok(()));
    static HASH_I64: [u8; 8] = 7_i64.to_le_bytes();
    assert_eq!(
        validate_hash_component(
            RADIX_HASH_COMPONENT_I64,
            0,
            RadixAbiSliceV1::from_static(&HASH_I64),
            8
        ),
        Ok(())
    );

    let host = RadixHostApiV1 {
        header: RadixAbiHeaderV1::new::<RadixHostApiV1>(0),
        handle: 4,
        max_external_value_bytes: RADIX_MAX_EXTERNAL_VALUE_BYTES,
        max_batch_rows: 4096,
        max_planner_spans: RADIX_MAX_PLANNER_SPANS,
        reserved: 0,
        log: Some(log_ok),
    };
    assert_eq!(validate_host_api(&host), Ok(()));
    assert_eq!(
        validate_log_record(RADIX_LOG_INFO, 0, RadixAbiSliceV1::from_static(b"loaded")),
        Ok(())
    );

    let diagnostic = RadixAbiDiagnosticV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiDiagnosticV1>(0),
        category: RADIX_DIAGNOSTIC_DOMAIN,
        status: RADIX_STATUS_DOMAIN_ERROR,
        detail: RadixAbiSliceV1::from_static(b"outside domain"),
        field: RadixAbiSliceV1::from_static(b"point"),
    };
    assert_eq!(validate_diagnostic(&diagnostic), Ok(()));
}

#[test]
fn descriptor_validation_requires_codec_and_semantic_callbacks_explicitly() {
    static LOCAL: &[u8] = b"point";
    let descriptor = RadixAbiExternalTypeDescriptorV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiExternalTypeDescriptorV1>(0),
        object_id: [1; 16],
        local_id: RadixAbiSliceV1::from_static(LOCAL),
        display_name: RadixAbiSliceV1::from_static(LOCAL),
        codec_version: 1,
        semantic_revision: 1,
        storage_kind: RADIX_EXTERNAL_STORAGE_FIXED,
        reserved_u16: 0,
        fixed_bytes: 16,
        max_bytes: 16,
        reserved_u32: 0,
        capabilities: 0,
        codec_fingerprint: [2; 32],
        encode: Some(codec_ok),
        decode: Some(parse_ok),
        equality: None,
        hash: None,
        ordering: None,
        text_input: None,
        text_output: None,
        binary_input: None,
        binary_output: None,
    };
    assert_eq!(validate_external_type_descriptor(&descriptor), Ok(()));
    let false_capability = RadixAbiExternalTypeDescriptorV1 {
        capabilities: RADIX_TYPE_CAP_EQUALITY,
        ..descriptor
    };
    assert_eq!(
        validate_external_type_descriptor(&false_capability),
        Err(RadixAbiValidationError::MissingCallback)
    );

    let package = RadixPluginDescriptorV1 {
        header: RadixAbiHeaderV1::new::<RadixPluginDescriptorV1>(RADIX_PACKAGE_CAP_EXTERNAL_TYPES),
        package_id: [8; 16],
        package_name: RadixAbiSliceV1::from_static(b"test-package"),
        package_version: RadixAbiSliceV1::from_static(b"1.0.0"),
        abi_min_minor: 0,
        abi_max_minor: 0,
        reserved: 0,
        descriptor_fingerprint: [9; 32],
        type_count: 1,
        reserved_types: 0,
        types: &descriptor,
        function_count: 0,
        reserved_functions: 0,
        functions: std::ptr::null(),
        operator_count: 0,
        reserved_operators: 0,
        operators: std::ptr::null(),
        operator_class_count: 0,
        reserved_operator_classes: 0,
        operator_classes: std::ptr::null(),
        planner_support_count: 0,
        reserved_planner_support: 0,
        planner_support: std::ptr::null(),
    };
    assert_eq!(validate_package_descriptor_shallow(&package), Ok(()));
    let missing_flag = RadixPluginDescriptorV1 {
        header: RadixAbiHeaderV1::new::<RadixPluginDescriptorV1>(0),
        ..package
    };
    assert_eq!(
        validate_package_descriptor_shallow(&missing_flag),
        Err(RadixAbiValidationError::InvalidDescriptor)
    );

    static ARGUMENTS: [RadixAbiTypeRefV1; 1] = [RadixAbiTypeRefV1::builtin(1)];
    let function = RadixAbiScalarFunctionDescriptorV1 {
        header: RadixAbiHeaderV1::new::<RadixAbiScalarFunctionDescriptorV1>(0),
        object_id: [3; 16],
        local_id: RadixAbiSliceV1::from_static(b"increment"),
        display_name: RadixAbiSliceV1::from_static(b"increment"),
        semantic_revision: 1,
        argument_count: 1,
        arguments: ARGUMENTS.as_ptr(),
        result: RadixAbiTypeRefV1::builtin(1),
        volatility: RADIX_VOLATILITY_IMMUTABLE,
        cancellation: RADIX_CANCELLATION_BOUNDED,
        strict: 1,
        parallel_safe: 1,
        reserved_u16: 0,
        cost: 1,
        max_output_bytes: 16,
        scalar: Some(scalar_ok),
        batch: None,
    };
    assert_eq!(validate_scalar_function_descriptor(&function), Ok(()));
}

#[test]
fn panic_barrier_converts_unwind_before_returning_through_c_abi() {
    unsafe extern "C" fn generated_barrier() -> RadixAbiStatusV1 {
        std::panic::catch_unwind(|| panic!("plugin panic"))
            .map(|()| RADIX_STATUS_OK)
            .unwrap_or(RADIX_STATUS_PANIC)
    }

    // SAFETY: this is a no-argument test callback and its wrapper catches the
    // panic before the extern C function returns.
    assert_eq!(unsafe { generated_barrier() }, RADIX_STATUS_PANIC);
}
