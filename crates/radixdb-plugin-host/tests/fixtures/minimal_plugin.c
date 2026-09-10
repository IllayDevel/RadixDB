#include "radixdb_plugin_abi.h"

#include <stddef.h>

static const uint8_t PACKAGE_NAME[] = "sample";
static const uint8_t PACKAGE_VERSION[] = "@VERSION@";

static const RadixPluginDescriptorV1 PLUGIN = {
    .header = {
        .abi_major = RADIX_ABI_MAJOR,
        .abi_minor = RADIX_ABI_MINOR,
        .struct_size = sizeof(RadixPluginDescriptorV1),
        .flags = 0,
    },
    .package_id = {
        0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x12, 0x34,
        0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
    },
    .package_name = {
        .ptr = PACKAGE_NAME,
        .len = sizeof(PACKAGE_NAME) - 1,
        .reserved = 0,
    },
    .package_version = {
        .ptr = PACKAGE_VERSION,
        .len = sizeof(PACKAGE_VERSION) - 1,
        .reserved = 0,
    },
    .abi_min_minor = 0,
    .abi_max_minor = 0,
    .reserved = 0,
    .descriptor_fingerprint = {
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
    },
    .type_count = 0,
    .reserved_types = 0,
    .types = NULL,
    .function_count = 0,
    .reserved_functions = 0,
    .functions = NULL,
    .operator_count = 0,
    .reserved_operators = 0,
    .operators = NULL,
    .operator_class_count = 0,
    .reserved_operator_classes = 0,
    .operator_classes = NULL,
    .planner_support_count = 0,
    .reserved_planner_support = 0,
    .planner_support = NULL,
};

__attribute__((visibility("default")))
const RadixPluginDescriptorV1 *radixdb_plugin_entry_v1(
    const RadixHostApiV1 *host,
    RadixAbiStatusV1 *status) {
    if (host == NULL || status == NULL || host->header.abi_major != RADIX_ABI_MAJOR) {
        if (status != NULL) {
            *status = RADIX_STATUS_INVALID_ARGUMENT;
        }
        return NULL;
    }
    *status = RADIX_STATUS_OK;
    return &PLUGIN;
}
