#ifndef RADIXDB_PLUGIN_ABI_H
#define RADIXDB_PLUGIN_ABI_H

#include <stdint.h>

#if !defined(__linux__) || !defined(__x86_64__) || UINTPTR_MAX != UINT64_MAX
#error "RadixDB plugin ABI 1.0 supports only 64-bit x86_64 Linux"
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define RADIX_ABI_MAJOR UINT16_C(1)
#define RADIX_ABI_MINOR UINT16_C(0)

#define RADIX_STATUS_OK UINT32_C(0)
#define RADIX_STATUS_INVALID_ARGUMENT UINT32_C(1)
#define RADIX_STATUS_DOMAIN_ERROR UINT32_C(2)
#define RADIX_STATUS_LIMIT_EXCEEDED UINT32_C(3)
#define RADIX_STATUS_CANCELLED UINT32_C(4)
#define RADIX_STATUS_INTERNAL_ERROR UINT32_C(5)
#define RADIX_STATUS_PANIC UINT32_C(6)
#define RADIX_STATUS_UNSUPPORTED_ABI UINT32_C(7)
#define RADIX_STATUS_CONTRACT_VIOLATION UINT32_C(8)
#define RADIX_STATUS_DEPENDENCY_MISSING UINT32_C(9)

#define RADIX_DIAGNOSTIC_INVALID_INPUT UINT32_C(1)
#define RADIX_DIAGNOSTIC_DOMAIN UINT32_C(2)
#define RADIX_DIAGNOSTIC_LIMIT UINT32_C(3)
#define RADIX_DIAGNOSTIC_CANCELLED UINT32_C(4)
#define RADIX_DIAGNOSTIC_INTERNAL UINT32_C(5)
#define RADIX_DIAGNOSTIC_PLUGIN_PANIC UINT32_C(6)
#define RADIX_DIAGNOSTIC_ABI_CONTRACT UINT32_C(7)
#define RADIX_DIAGNOSTIC_DEPENDENCY UINT32_C(8)

#define RADIX_DESCRIPTOR_EXTERNAL_TYPE UINT16_C(1)
#define RADIX_DESCRIPTOR_SCALAR_FUNCTION UINT16_C(2)
#define RADIX_DESCRIPTOR_OPERATOR UINT16_C(3)
#define RADIX_DESCRIPTOR_OPERATOR_CLASS UINT16_C(4)
#define RADIX_DESCRIPTOR_PLANNER_SUPPORT UINT16_C(5)

#define RADIX_TYPE_REF_BUILTIN UINT16_C(1)
#define RADIX_TYPE_REF_EXTERNAL UINT16_C(2)
#define RADIX_BUILTIN_INTEGER UINT16_C(1)
#define RADIX_BUILTIN_FLOAT UINT16_C(2)
#define RADIX_BUILTIN_TEXT UINT16_C(3)
#define RADIX_BUILTIN_BOOLEAN UINT16_C(4)
#define RADIX_BUILTIN_TIMESTAMP UINT16_C(5)
#define RADIX_BUILTIN_JSON UINT16_C(6)
#define RADIX_BUILTIN_VECTOR UINT16_C(7)
#define RADIX_BUILTIN_UUID UINT16_C(8)
#define RADIX_BUILTIN_DECIMAL UINT16_C(9)
#define RADIX_BUILTIN_DATE UINT16_C(10)
#define RADIX_BUILTIN_BYTES UINT16_C(11)
#define RADIX_EXTERNAL_STORAGE_FIXED UINT16_C(1)
#define RADIX_EXTERNAL_STORAGE_VARIABLE UINT16_C(2)
#define RADIX_COLUMN_LAYOUT_FIXED UINT16_C(1)
#define RADIX_COLUMN_LAYOUT_VARIABLE UINT16_C(2)
#define RADIX_VOLATILITY_IMMUTABLE UINT16_C(1)
#define RADIX_VOLATILITY_STABLE UINT16_C(2)
#define RADIX_VOLATILITY_VOLATILE UINT16_C(3)
#define RADIX_CANCELLATION_BOUNDED UINT16_C(1)
#define RADIX_RECHECK_EXACT UINT16_C(1)
#define RADIX_RECHECK_ALWAYS UINT16_C(2)
#define RADIX_ACCESS_METHOD_BTREE UINT16_C(1)
#define RADIX_ACCESS_METHOD_HASH UINT16_C(2)
#define RADIX_ACCESS_METHOD_BITMAP UINT16_C(3)
#define RADIX_ACCESS_METHOD_HNSW UINT16_C(4)
#define RADIX_LOG_INFO UINT16_C(1)
#define RADIX_LOG_WARN UINT16_C(2)
#define RADIX_LOG_ERROR UINT16_C(3)
#define RADIX_LOG_DEBUG UINT16_C(4)

#define RADIX_VALUE_FLAG_NULL (UINT32_C(1) << 0)
#define RADIX_RESULT_ITEM_FLAG_NULL (UINT32_C(1) << 0)
#define RADIX_PACKAGE_CAP_EXTERNAL_TYPES (UINT64_C(1) << 0)
#define RADIX_PACKAGE_CAP_SCALAR_FUNCTIONS (UINT64_C(1) << 1)
#define RADIX_PACKAGE_CAP_BATCH_FUNCTIONS (UINT64_C(1) << 2)
#define RADIX_PACKAGE_CAP_OPERATORS (UINT64_C(1) << 3)
#define RADIX_PACKAGE_CAP_OPERATOR_CLASSES (UINT64_C(1) << 4)
#define RADIX_PACKAGE_CAP_PLANNER_SUPPORT (UINT64_C(1) << 5)
#define RADIX_PACKAGE_CAP_ALL ((UINT64_C(1) << 6) - UINT64_C(1))

#define RADIX_TYPE_CAP_EQUALITY (UINT64_C(1) << 0)
#define RADIX_TYPE_CAP_HASH (UINT64_C(1) << 1)
#define RADIX_TYPE_CAP_ORDERING (UINT64_C(1) << 2)
#define RADIX_TYPE_CAP_TEXT_INPUT (UINT64_C(1) << 3)
#define RADIX_TYPE_CAP_TEXT_OUTPUT (UINT64_C(1) << 4)
#define RADIX_TYPE_CAP_BINARY_INPUT (UINT64_C(1) << 5)
#define RADIX_TYPE_CAP_BINARY_OUTPUT (UINT64_C(1) << 6)
#define RADIX_TYPE_CAP_ALL ((UINT64_C(1) << 7) - UINT64_C(1))

#define RADIX_HASH_COMPONENT_BYTES UINT16_C(1)
#define RADIX_HASH_COMPONENT_I64 UINT16_C(2)
#define RADIX_HASH_COMPONENT_U64 UINT16_C(3)
#define RADIX_HASH_COMPONENT_F64_BITS UINT16_C(4)
#define RADIX_HASH_COMPONENT_EXTERNAL UINT16_C(5)

#define RADIX_MAX_LOCAL_ID_BYTES UINT32_C(255)
#define RADIX_MAX_DIAGNOSTIC_BYTES UINT32_C(4096)
#define RADIX_MAX_EXTERNAL_VALUE_BYTES UINT32_C(16777216)
#define RADIX_MAX_DESCRIPTOR_ENTRIES UINT32_C(65535)
#define RADIX_MAX_FUNCTION_ARGUMENTS UINT32_C(1024)
#define RADIX_MAX_PLANNER_SPANS UINT32_C(4096)
#define RADIX_MAX_HASH_COMPONENTS UINT32_C(256)
#define RADIX_MAX_HASH_BYTES UINT32_C(65536)

typedef uint32_t RadixAbiStatusV1;

typedef struct RadixAbiHeaderV1 {
    uint16_t abi_major;
    uint16_t abi_minor;
    uint32_t struct_size;
    uint64_t flags;
} RadixAbiHeaderV1;

typedef struct RadixAbiSliceV1 {
    const uint8_t *ptr;
    uint32_t len;
    uint32_t reserved;
} RadixAbiSliceV1;

typedef struct RadixAbiU32SliceV1 {
    const uint32_t *ptr;
    uint32_t len;
    uint32_t reserved;
} RadixAbiU32SliceV1;

typedef RadixAbiSliceV1 RadixAbiStringV1;

typedef struct RadixAbiTypeRefV1 {
    uint16_t kind;
    uint16_t builtin_tag;
    uint32_t codec_version;
    uint8_t object_id[16];
} RadixAbiTypeRefV1;

typedef struct RadixAbiValueV1 {
    RadixAbiTypeRefV1 type_ref;
    uint32_t flags;
    uint32_t reserved;
    uint8_t inline_bytes[16];
    RadixAbiSliceV1 borrowed_bytes;
} RadixAbiValueV1;

typedef struct RadixAbiColumnViewV1 {
    RadixAbiHeaderV1 header;
    RadixAbiTypeRefV1 type_ref;
    uint32_t row_count;
    uint16_t layout;
    uint16_t element_width;
    uint16_t alignment;
    uint16_t reserved_u16;
    uint32_t stride;
    RadixAbiSliceV1 null_bitmap;
    RadixAbiSliceV1 data;
    RadixAbiU32SliceV1 offsets;
} RadixAbiColumnViewV1;

typedef struct RadixAbiBatchViewV1 {
    RadixAbiHeaderV1 header;
    uint32_t row_count;
    uint32_t column_count;
    const RadixAbiColumnViewV1 *columns;
} RadixAbiBatchViewV1;

typedef RadixAbiStatusV1 (*RadixAbiBuilderWriteFnV1)(
    uint64_t handle, uint32_t item_flags, uint32_t reserved,
    RadixAbiSliceV1 bytes);
typedef RadixAbiStatusV1 (*RadixAbiBuilderFinishFnV1)(uint64_t handle);

typedef struct RadixAbiResultBuilderV1 {
    RadixAbiHeaderV1 header;
    uint64_t handle;
    uint32_t max_bytes;
    uint32_t max_items;
    RadixAbiBuilderWriteFnV1 write;
    RadixAbiBuilderFinishFnV1 finish;
} RadixAbiResultBuilderV1;

typedef RadixAbiStatusV1 (*RadixAbiHashAppendFnV1)(
    uint64_t handle, uint16_t component_kind, uint16_t reserved,
    RadixAbiSliceV1 bytes);

typedef struct RadixAbiHashSinkV1 {
    RadixAbiHeaderV1 header;
    uint64_t handle;
    uint32_t max_components;
    uint32_t max_bytes;
    RadixAbiHashAppendFnV1 append;
} RadixAbiHashSinkV1;

typedef struct RadixAbiDiagnosticV1 {
    RadixAbiHeaderV1 header;
    uint32_t category;
    RadixAbiStatusV1 status;
    RadixAbiStringV1 detail;
    RadixAbiStringV1 field;
} RadixAbiDiagnosticV1;

typedef RadixAbiStatusV1 (*RadixAbiDiagnosticWriteFnV1)(
    uint64_t handle, const RadixAbiDiagnosticV1 *diagnostic);

typedef struct RadixAbiDiagnosticSinkV1 {
    RadixAbiHeaderV1 header;
    uint64_t handle;
    uint32_t max_detail_bytes;
    uint32_t reserved;
    RadixAbiDiagnosticWriteFnV1 write;
} RadixAbiDiagnosticSinkV1;

typedef RadixAbiStatusV1 (*RadixAbiCheckCancelledFnV1)(uint64_t handle);
typedef RadixAbiStatusV1 (*RadixAbiChargeWorkFnV1)(uint64_t handle,
                                                  uint32_t units);

typedef struct RadixAbiCallContextV1 {
    RadixAbiHeaderV1 header;
    uint64_t handle;
    uint64_t deadline_unix_ns;
    uint32_t max_output_bytes;
    uint32_t max_work_units;
    RadixAbiCheckCancelledFnV1 check_cancelled;
    RadixAbiChargeWorkFnV1 charge_work;
    const RadixAbiDiagnosticSinkV1 *diagnostics;
} RadixAbiCallContextV1;

typedef RadixAbiStatusV1 (*RadixAbiHostLogFnV1)(
    uint64_t handle, uint16_t level, uint16_t reserved,
    RadixAbiStringV1 message);

typedef struct RadixHostApiV1 {
    RadixAbiHeaderV1 header;
    uint64_t handle;
    uint32_t max_external_value_bytes;
    uint32_t max_batch_rows;
    uint32_t max_planner_spans;
    uint32_t reserved;
    RadixAbiHostLogFnV1 log;
} RadixHostApiV1;

typedef RadixAbiStatusV1 (*RadixAbiCodecFnV1)(
    const RadixAbiCallContextV1 *context, const RadixAbiValueV1 *input,
    const RadixAbiResultBuilderV1 *output);
typedef RadixAbiStatusV1 (*RadixAbiParseFnV1)(
    const RadixAbiCallContextV1 *context, RadixAbiSliceV1 input,
    const RadixAbiResultBuilderV1 *output);
typedef RadixAbiStatusV1 (*RadixAbiEqualFnV1)(
    const RadixAbiCallContextV1 *context, const RadixAbiValueV1 *left,
    const RadixAbiValueV1 *right, uint8_t *output);
typedef RadixAbiStatusV1 (*RadixAbiCompareFnV1)(
    const RadixAbiCallContextV1 *context, const RadixAbiValueV1 *left,
    const RadixAbiValueV1 *right, int8_t *output);
typedef RadixAbiStatusV1 (*RadixAbiHashFnV1)(
    const RadixAbiCallContextV1 *context, const RadixAbiValueV1 *value,
    const RadixAbiHashSinkV1 *sink);
typedef RadixAbiStatusV1 (*RadixAbiScalarFnV1)(
    const RadixAbiCallContextV1 *context,
    const RadixAbiValueV1 *arguments, uint32_t argument_count,
    const RadixAbiResultBuilderV1 *output);
typedef RadixAbiStatusV1 (*RadixAbiBatchFnV1)(
    const RadixAbiCallContextV1 *context, const RadixAbiBatchViewV1 *input,
    const RadixAbiResultBuilderV1 *output);
typedef RadixAbiStatusV1 (*RadixAbiPlannerSupportFnV1)(
    const RadixAbiCallContextV1 *context,
    RadixAbiSliceV1 normalized_predicate,
    const RadixAbiResultBuilderV1 *output);

typedef struct RadixAbiExternalTypeDescriptorV1 {
    RadixAbiHeaderV1 header;
    uint8_t object_id[16];
    RadixAbiStringV1 local_id;
    RadixAbiStringV1 display_name;
    uint32_t codec_version;
    uint32_t semantic_revision;
    uint16_t storage_kind;
    uint16_t reserved_u16;
    uint32_t fixed_bytes;
    uint32_t max_bytes;
    uint32_t reserved_u32;
    uint64_t capabilities;
    uint8_t codec_fingerprint[32];
    RadixAbiCodecFnV1 encode;
    RadixAbiParseFnV1 decode;
    RadixAbiEqualFnV1 equality;
    RadixAbiHashFnV1 hash;
    RadixAbiCompareFnV1 ordering;
    RadixAbiParseFnV1 text_input;
    RadixAbiCodecFnV1 text_output;
    RadixAbiParseFnV1 binary_input;
    RadixAbiCodecFnV1 binary_output;
} RadixAbiExternalTypeDescriptorV1;

typedef struct RadixAbiScalarFunctionDescriptorV1 {
    RadixAbiHeaderV1 header;
    uint8_t object_id[16];
    RadixAbiStringV1 local_id;
    RadixAbiStringV1 display_name;
    uint32_t semantic_revision;
    uint32_t argument_count;
    const RadixAbiTypeRefV1 *arguments;
    RadixAbiTypeRefV1 result;
    uint16_t volatility;
    uint16_t cancellation;
    uint8_t strict;
    uint8_t parallel_safe;
    uint16_t reserved_u16;
    uint32_t cost;
    uint32_t max_output_bytes;
    RadixAbiScalarFnV1 scalar;
    RadixAbiBatchFnV1 batch;
} RadixAbiScalarFunctionDescriptorV1;

typedef struct RadixAbiOperatorDescriptorV1 {
    RadixAbiHeaderV1 header;
    uint8_t object_id[16];
    RadixAbiStringV1 local_id;
    RadixAbiStringV1 symbol;
    uint32_t semantic_revision;
    uint32_t reserved;
    RadixAbiTypeRefV1 left;
    RadixAbiTypeRefV1 right;
    RadixAbiTypeRefV1 result;
    uint8_t function_id[16];
} RadixAbiOperatorDescriptorV1;

typedef struct RadixAbiBindingEntryV1 {
    uint16_t slot;
    uint16_t flags;
    uint8_t object_id[16];
} RadixAbiBindingEntryV1;

typedef RadixAbiStatusV1 (*RadixAbiKeyEncodeFnV1)(
    const RadixAbiCallContextV1 *context, const RadixAbiValueV1 *value,
    const RadixAbiResultBuilderV1 *output);

typedef struct RadixAbiOperatorClassDescriptorV1 {
    RadixAbiHeaderV1 header;
    uint8_t object_id[16];
    RadixAbiStringV1 local_id;
    uint32_t semantic_revision;
    uint16_t access_method;
    uint16_t reserved_u16;
    RadixAbiTypeRefV1 input_type;
    RadixAbiTypeRefV1 key_type;
    uint32_t key_codec_revision;
    uint32_t strategy_count;
    const RadixAbiBindingEntryV1 *strategies;
    uint32_t support_count;
    uint32_t reserved_u32;
    const RadixAbiBindingEntryV1 *supports;
    uint8_t fingerprint[32];
    RadixAbiKeyEncodeFnV1 encode_key;
} RadixAbiOperatorClassDescriptorV1;

typedef struct RadixAbiPlannerSupportDescriptorV1 {
    RadixAbiHeaderV1 header;
    uint8_t object_id[16];
    RadixAbiStringV1 local_id;
    uint32_t semantic_revision;
    uint32_t max_spans;
    uint32_t max_output_bytes;
    uint16_t recheck_policy;
    uint16_t reserved_u16;
    uint8_t target_function_id[16];
    uint8_t target_operator_class_id[16];
    uint8_t fingerprint[32];
    RadixAbiPlannerSupportFnV1 callback;
} RadixAbiPlannerSupportDescriptorV1;

typedef struct RadixPluginDescriptorV1 {
    RadixAbiHeaderV1 header;
    uint8_t package_id[16];
    RadixAbiStringV1 package_name;
    RadixAbiStringV1 package_version;
    uint16_t abi_min_minor;
    uint16_t abi_max_minor;
    uint32_t reserved;
    uint8_t descriptor_fingerprint[32];
    uint32_t type_count;
    uint32_t reserved_types;
    const RadixAbiExternalTypeDescriptorV1 *types;
    uint32_t function_count;
    uint32_t reserved_functions;
    const RadixAbiScalarFunctionDescriptorV1 *functions;
    uint32_t operator_count;
    uint32_t reserved_operators;
    const RadixAbiOperatorDescriptorV1 *operators;
    uint32_t operator_class_count;
    uint32_t reserved_operator_classes;
    const RadixAbiOperatorClassDescriptorV1 *operator_classes;
    uint32_t planner_support_count;
    uint32_t reserved_planner_support;
    const RadixAbiPlannerSupportDescriptorV1 *planner_support;
} RadixPluginDescriptorV1;

typedef const RadixPluginDescriptorV1 *(*RadixPluginEntrypointV1)(
    const RadixHostApiV1 *host, RadixAbiStatusV1 *status);

const RadixPluginDescriptorV1 *radixdb_plugin_entry_v1(
    const RadixHostApiV1 *host, RadixAbiStatusV1 *status);

#ifdef __cplusplus
}
#endif

#endif
