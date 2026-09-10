use std::process::Command;

use radixdb_plugin_abi::*;

const CHILD_ENV: &str = "RADIXDB_PLUGIN_ABI_MALFORMED_CASE";

fn package_with_types(
    types: *const RadixAbiExternalTypeDescriptorV1,
    type_count: u32,
) -> RadixPluginDescriptorV1 {
    RadixPluginDescriptorV1 {
        header: RadixAbiHeaderV1::new::<RadixPluginDescriptorV1>(RADIX_PACKAGE_CAP_EXTERNAL_TYPES),
        package_id: [1; 16],
        package_name: RadixAbiSliceV1::from_static(b"malformed"),
        package_version: RadixAbiSliceV1::from_static(b"1.0.0"),
        abi_min_minor: RADIX_ABI_MINOR,
        abi_max_minor: RADIX_ABI_MINOR,
        reserved: 0,
        descriptor_fingerprint: [2; 32],
        type_count,
        reserved_types: 0,
        types,
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
    }
}

#[test]
#[ignore = "isolated child entrypoint for malformed ABI matrix"]
fn malformed_abi_child() {
    match std::env::var(CHILD_ENV).unwrap().as_str() {
        "null-slice" => assert_eq!(
            validate_slice(
                RadixAbiSliceV1 {
                    ptr: std::ptr::null(),
                    len: 1,
                    reserved: 0,
                },
                8,
                1,
            ),
            Err(RadixAbiValidationError::NullPointer)
        ),
        "misaligned-pointer" => {
            let aligned = std::ptr::NonNull::<u64>::dangling().as_ptr().cast::<u8>();
            assert_eq!(
                validate_slice(
                    RadixAbiSliceV1 {
                        ptr: aligned.wrapping_add(1),
                        len: 1,
                        reserved: 0,
                    },
                    8,
                    8,
                ),
                Err(RadixAbiValidationError::MisalignedPointer)
            );
        }
        "short-header" => assert_eq!(
            validate_header(
                &RadixAbiHeaderV1 {
                    abi_major: RADIX_ABI_MAJOR,
                    abi_minor: RADIX_ABI_MINOR,
                    struct_size: 8,
                    flags: 0,
                },
                16,
                0,
            ),
            Err(RadixAbiValidationError::StructTooSmall)
        ),
        "unknown-flag" => assert_eq!(
            validate_header(
                &RadixAbiHeaderV1 {
                    abi_major: RADIX_ABI_MAJOR,
                    abi_minor: RADIX_ABI_MINOR,
                    struct_size: 16,
                    flags: 1 << 63,
                },
                16,
                0,
            ),
            Err(RadixAbiValidationError::UnknownFlags)
        ),
        "bad-type" => assert_eq!(
            validate_type_ref(&RadixAbiTypeRefV1 {
                kind: 99,
                builtin_tag: 0,
                codec_version: 0,
                object_id: [0; 16],
            }),
            Err(RadixAbiValidationError::InvalidTypeReference)
        ),
        "missing-external-id" => assert_eq!(
            validate_type_ref(&RadixAbiTypeRefV1::external([0; 16], 1)),
            Err(RadixAbiValidationError::InvalidTypeReference)
        ),
        "oversized-slice" => assert_eq!(
            validate_slice(
                RadixAbiSliceV1 {
                    ptr: std::ptr::NonNull::<u8>::dangling().as_ptr().cast_const(),
                    len: 9,
                    reserved: 0,
                },
                8,
                1,
            ),
            Err(RadixAbiValidationError::LimitExceeded)
        ),
        "null-batch-table" => assert_eq!(
            validate_batch_view(&RadixAbiBatchViewV1 {
                header: RadixAbiHeaderV1::new::<RadixAbiBatchViewV1>(0),
                row_count: 1,
                column_count: 1,
                columns: std::ptr::null(),
            }),
            Err(RadixAbiValidationError::InvalidBatchShape)
        ),
        "unknown-result-flag" => assert_eq!(
            validate_result_item(1 << 31, 0, RadixAbiSliceV1::EMPTY, 8),
            Err(RadixAbiValidationError::UnknownFlags)
        ),
        "success-diagnostic" => assert_eq!(
            validate_diagnostic(&RadixAbiDiagnosticV1 {
                header: RadixAbiHeaderV1::new::<RadixAbiDiagnosticV1>(0),
                category: RADIX_DIAGNOSTIC_INTERNAL,
                status: RADIX_STATUS_OK,
                detail: RadixAbiSliceV1::EMPTY,
                field: RadixAbiSliceV1::EMPTY,
            }),
            Err(RadixAbiValidationError::InvalidStatus)
        ),
        "null-descriptor-table" => assert_eq!(
            validate_package_descriptor_shallow(&package_with_types(std::ptr::null(), 1)),
            Err(RadixAbiValidationError::NullPointer)
        ),
        "oversized-descriptor-table" => assert_eq!(
            validate_package_descriptor_shallow(&package_with_types(
                std::ptr::NonNull::<RadixAbiExternalTypeDescriptorV1>::dangling().as_ptr(),
                RADIX_MAX_DESCRIPTOR_ENTRIES + 1,
            )),
            Err(RadixAbiValidationError::LimitExceeded)
        ),
        other => panic!("unknown malformed case {other}"),
    }
}

#[test]
fn malformed_tag_size_flag_and_pointer_matrix_fails_closed_in_subprocesses() {
    for case in [
        "null-slice",
        "misaligned-pointer",
        "short-header",
        "unknown-flag",
        "bad-type",
        "missing-external-id",
        "oversized-slice",
        "null-batch-table",
        "unknown-result-flag",
        "success-diagnostic",
        "null-descriptor-table",
        "oversized-descriptor-table",
    ] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "malformed_abi_child", "--ignored", "--nocapture"])
            .env(CHILD_ENV, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "malformed ABI child {case} failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
