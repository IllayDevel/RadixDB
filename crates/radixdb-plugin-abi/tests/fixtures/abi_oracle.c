#include "radixdb_plugin_abi.h"

#include <stddef.h>
#include <stdio.h>
#include <string.h>

_Static_assert(sizeof(RadixAbiHeaderV1) == 16, "header size");
_Static_assert(_Alignof(RadixAbiHeaderV1) == 8, "header alignment");
_Static_assert(offsetof(RadixAbiHeaderV1, flags) == 8, "header flags offset");
_Static_assert(sizeof(RadixAbiSliceV1) == 16, "slice size");
_Static_assert(_Alignof(RadixAbiSliceV1) == 8, "slice alignment");
_Static_assert(offsetof(RadixAbiSliceV1, len) == 8, "slice len offset");
_Static_assert(sizeof(RadixAbiTypeRefV1) == 24, "type ref size");
_Static_assert(_Alignof(RadixAbiTypeRefV1) == 4, "type ref alignment");
_Static_assert(offsetof(RadixAbiTypeRefV1, object_id) == 8,
               "type ref object offset");
_Static_assert(sizeof(RadixAbiValueV1) == 64, "value size");
_Static_assert(_Alignof(RadixAbiValueV1) == 8, "value alignment");
_Static_assert(offsetof(RadixAbiValueV1, borrowed_bytes) == 48,
               "value bytes offset");
_Static_assert(sizeof(RadixAbiColumnViewV1) == 104, "column size");
_Static_assert(_Alignof(RadixAbiColumnViewV1) == 8, "column alignment");
_Static_assert(offsetof(RadixAbiColumnViewV1, null_bitmap) == 56,
               "column nulls offset");
_Static_assert(offsetof(RadixAbiColumnViewV1, offsets) == 88,
               "column offsets offset");
_Static_assert(sizeof(RadixAbiBatchViewV1) == 32, "batch size");
_Static_assert(_Alignof(RadixAbiBatchViewV1) == 8, "batch alignment");
_Static_assert(offsetof(RadixAbiBatchViewV1, columns) == 24,
               "batch columns offset");
_Static_assert(sizeof(RadixAbiResultBuilderV1) == 48, "builder size");
_Static_assert(_Alignof(RadixAbiResultBuilderV1) == 8, "builder alignment");
_Static_assert(offsetof(RadixAbiResultBuilderV1, write) == 32,
               "builder write offset");
_Static_assert(sizeof(RadixAbiHashSinkV1) == 40, "hash sink size");
_Static_assert(_Alignof(RadixAbiHashSinkV1) == 8, "hash sink alignment");
_Static_assert(offsetof(RadixAbiHashSinkV1, append) == 32,
               "hash sink append offset");
_Static_assert(sizeof(RadixAbiDiagnosticV1) == 56, "diagnostic size");
_Static_assert(_Alignof(RadixAbiDiagnosticV1) == 8,
               "diagnostic alignment");
_Static_assert(offsetof(RadixAbiDiagnosticV1, detail) == 24,
               "diagnostic detail offset");
_Static_assert(sizeof(RadixAbiDiagnosticSinkV1) == 40,
               "diagnostic sink size");
_Static_assert(_Alignof(RadixAbiDiagnosticSinkV1) == 8,
               "diagnostic sink alignment");
_Static_assert(offsetof(RadixAbiDiagnosticSinkV1, write) == 32,
               "diagnostic sink write offset");
_Static_assert(sizeof(RadixAbiCallContextV1) == 64, "call context size");
_Static_assert(_Alignof(RadixAbiCallContextV1) == 8,
               "call context alignment");
_Static_assert(offsetof(RadixAbiCallContextV1, diagnostics) == 56,
               "call context diagnostics offset");
_Static_assert(sizeof(RadixHostApiV1) == 48, "host API size");
_Static_assert(_Alignof(RadixHostApiV1) == 8, "host API alignment");
_Static_assert(offsetof(RadixHostApiV1, log) == 40,
               "host API log offset");
_Static_assert(sizeof(RadixAbiBindingEntryV1) == 20, "binding size");
_Static_assert(_Alignof(RadixAbiBindingEntryV1) == 2,
               "binding alignment");
_Static_assert(offsetof(RadixAbiBindingEntryV1, object_id) == 4,
               "binding object offset");
_Static_assert(sizeof(RadixAbiExternalTypeDescriptorV1) == 200,
               "type descriptor size");
_Static_assert(_Alignof(RadixAbiExternalTypeDescriptorV1) == 8,
               "type descriptor alignment");
_Static_assert(offsetof(RadixAbiExternalTypeDescriptorV1, capabilities) == 88,
               "type capabilities offset");
_Static_assert(offsetof(RadixAbiExternalTypeDescriptorV1, encode) == 128,
               "type encode offset");
_Static_assert(sizeof(RadixAbiScalarFunctionDescriptorV1) == 136,
               "function descriptor size");
_Static_assert(_Alignof(RadixAbiScalarFunctionDescriptorV1) == 8,
               "function descriptor alignment");
_Static_assert(offsetof(RadixAbiScalarFunctionDescriptorV1, volatility) == 104,
               "function volatility offset");
_Static_assert(offsetof(RadixAbiScalarFunctionDescriptorV1, scalar) == 120,
               "function scalar offset");
_Static_assert(sizeof(RadixAbiOperatorDescriptorV1) == 160,
               "operator descriptor size");
_Static_assert(_Alignof(RadixAbiOperatorDescriptorV1) == 8,
               "operator descriptor alignment");
_Static_assert(offsetof(RadixAbiOperatorDescriptorV1, function_id) == 144,
               "operator function offset");
_Static_assert(sizeof(RadixAbiOperatorClassDescriptorV1) == 176,
               "operator class descriptor size");
_Static_assert(_Alignof(RadixAbiOperatorClassDescriptorV1) == 8,
               "operator class alignment");
_Static_assert(offsetof(RadixAbiOperatorClassDescriptorV1, strategies) == 112,
               "operator class strategies offset");
_Static_assert(offsetof(RadixAbiOperatorClassDescriptorV1, encode_key) == 168,
               "operator class encoder offset");
_Static_assert(sizeof(RadixAbiPlannerSupportDescriptorV1) == 136,
               "planner support descriptor size");
_Static_assert(_Alignof(RadixAbiPlannerSupportDescriptorV1) == 8,
               "planner support alignment");
_Static_assert(offsetof(RadixAbiPlannerSupportDescriptorV1, callback) == 128,
               "planner support callback offset");
_Static_assert(sizeof(RadixPluginDescriptorV1) == 184,
               "package descriptor size");
_Static_assert(_Alignof(RadixPluginDescriptorV1) == 8,
               "package descriptor alignment");
_Static_assert(offsetof(RadixPluginDescriptorV1, descriptor_fingerprint) == 72,
               "package fingerprint offset");
_Static_assert(offsetof(RadixPluginDescriptorV1, planner_support) == 176,
               "package planner support offset");

typedef struct OracleRecord {
    RadixAbiHeaderV1 header;
    RadixAbiTypeRefV1 type_ref;
    RadixAbiExternalTypeDescriptorV1 descriptor;
    RadixAbiValueV1 value;
} OracleRecord;

int main(int argc, char **argv) {
    if (argc != 2) {
        return 64;
    }
    OracleRecord record;
    memset(&record, 0, sizeof(record));
    record.header.abi_major = RADIX_ABI_MAJOR;
    record.header.abi_minor = RADIX_ABI_MINOR;
    record.header.struct_size = sizeof(RadixAbiHeaderV1) + 32;
    record.type_ref.kind = RADIX_TYPE_REF_EXTERNAL;
    record.type_ref.codec_version = 7;
    record.type_ref.object_id[0] = 0x42;
    record.descriptor.header.abi_major = RADIX_ABI_MAJOR;
    record.descriptor.header.abi_minor = RADIX_ABI_MINOR;
    record.descriptor.header.struct_size = sizeof(record.descriptor);
    record.descriptor.object_id[0] = 0x42;
    record.descriptor.codec_version = 7;
    record.descriptor.semantic_revision = 3;
    record.descriptor.storage_kind = RADIX_EXTERNAL_STORAGE_FIXED;
    record.descriptor.fixed_bytes = 16;
    record.descriptor.max_bytes = 16;
    record.descriptor.codec_fingerprint[0] = 0xa5;
    record.value.type_ref.kind = RADIX_TYPE_REF_BUILTIN;
    record.value.type_ref.builtin_tag = 1;
    record.value.inline_bytes[0] = 0x7b;

    FILE *output = fopen(argv[1], "wb");
    if (output == NULL) {
        return 74;
    }
    int ok = fwrite(&record, sizeof(record), 1, output) == 1;
    ok = fclose(output) == 0 && ok;
    return ok ? 0 : 74;
}
