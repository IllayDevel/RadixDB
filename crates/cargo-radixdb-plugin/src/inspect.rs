use std::{collections::BTreeSet, fs, path::Path, process::Command, slice, str};

use libloading::Library;
use radixdb_plugin_abi as abi;
use serde::{Deserialize, Serialize};

use crate::{command_text, fail, hex, isolated_command, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorReport {
    pub package_id: String,
    pub name: String,
    pub version: String,
    pub abi_major: u16,
    pub abi_min_minor: u16,
    pub abi_max_minor: u16,
    pub descriptor_fingerprint: String,
    pub capabilities: u64,
    pub types: Vec<TypeReport>,
    pub functions: Vec<ObjectReport>,
    pub operators: Vec<ObjectReport>,
    pub operator_classes: Vec<FingerprintedObjectReport>,
    pub planner_support: Vec<FingerprintedObjectReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeReport {
    pub object_id: String,
    pub local_id: String,
    pub name: String,
    pub codec_version: u32,
    pub semantic_revision: u32,
    pub storage_kind: u16,
    pub fixed_bytes: u32,
    pub max_bytes: u32,
    pub capabilities: u64,
    pub codec_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectReport {
    pub object_id: String,
    pub local_id: String,
    pub semantic_revision: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintedObjectReport {
    pub object_id: String,
    pub local_id: String,
    pub semantic_revision: u32,
    pub fingerprint: String,
}

pub fn inspect_isolated(executable: &Path, library: &Path) -> Result<DescriptorReport> {
    validate_elf(library)?;
    validate_exports(library)?;
    let output = isolated_command(executable)
        .arg("__inspect-child")
        .arg("--library")
        .arg(library)
        .output()
        .map_err(|error| {
            fail(format!(
                "cannot start isolated descriptor inspector: {error}"
            ))
        })?;
    if !output.status.success() {
        return Err(fail(format!(
            "isolated descriptor inspector failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| fail(format!("invalid isolated inspector report: {error}")))
}

pub fn inspect_child(library: &Path) -> Result<DescriptorReport> {
    let descriptor = load_descriptor(library)?;
    report_descriptor(descriptor)
}

pub(crate) fn load_descriptor(library: &Path) -> Result<&'static abi::RadixPluginDescriptorV1> {
    let library = unsafe { Library::new(library) }
        .map_err(|error| fail(format!("cannot load plugin library: {error}")))?;
    // The process is an isolated, short-lived inspector. Pin even rejected
    // libraries so destructors never run after an entrypoint was observed.
    let library: &'static Library = Box::leak(Box::new(library));
    let entrypoint =
        unsafe { library.get::<abi::RadixPluginEntrypointV1>(abi::ENTRYPOINT_SYMBOL_V1) }
            .map_err(|error| fail(format!("missing plugin entrypoint: {error}")))?;
    let host = abi::RadixHostApiV1 {
        header: abi::RadixAbiHeaderV1::new::<abi::RadixHostApiV1>(0),
        handle: 1,
        max_external_value_bytes: abi::RADIX_MAX_EXTERNAL_VALUE_BYTES,
        max_batch_rows: 65_535,
        max_planner_spans: abi::RADIX_MAX_PLANNER_SPANS,
        reserved: 0,
        log: Some(inspector_log),
    };
    let mut status = abi::RADIX_STATUS_INTERNAL_ERROR;
    let descriptor = unsafe { entrypoint(&host, &mut status) };
    if status != abi::RADIX_STATUS_OK {
        return Err(fail(format!("entrypoint returned status {status}")));
    }
    let descriptor = unsafe { descriptor.as_ref() }
        .ok_or_else(|| fail("entrypoint returned a null descriptor"))?;
    abi::validate_package_descriptor_shallow(descriptor)
        .map_err(|error| fail(format!("invalid package descriptor: {error:?}")))?;
    Ok(descriptor)
}

fn report_descriptor(
    descriptor: &'static abi::RadixPluginDescriptorV1,
) -> Result<DescriptorReport> {
    let package_id = format_uuid(descriptor.package_id);
    let name = unsafe { copy_string(descriptor.package_name) }?;
    let version = unsafe { copy_string(descriptor.package_version) }?;
    let parsed_version = semver::Version::parse(&version).map_err(|error| {
        fail(format!(
            "descriptor version is not canonical SemVer: {error}"
        ))
    })?;
    if parsed_version.to_string() != version || !parsed_version.build.is_empty() {
        return Err(fail(
            "descriptor version must use canonical SemVer without build metadata",
        ));
    }

    let raw_types = unsafe { table(descriptor.types, descriptor.type_count) };
    let raw_functions = unsafe { table(descriptor.functions, descriptor.function_count) };
    let raw_operators = unsafe { table(descriptor.operators, descriptor.operator_count) };
    let raw_operator_classes =
        unsafe { table(descriptor.operator_classes, descriptor.operator_class_count) };
    let raw_planner =
        unsafe { table(descriptor.planner_support, descriptor.planner_support_count) };

    let mut identities = BTreeSet::new();
    let mut types = Vec::with_capacity(raw_types.len());
    for value in raw_types {
        abi::validate_external_type_descriptor(value)
            .map_err(|error| fail(format!("invalid type descriptor: {error:?}")))?;
        let local_id = unsafe { copy_string(value.local_id) }?;
        validate_identity(
            descriptor.package_id,
            &local_id,
            value.object_id,
            &mut identities,
        )?;
        types.push(TypeReport {
            object_id: hex(&value.object_id),
            local_id,
            name: unsafe { copy_string(value.display_name) }?,
            codec_version: value.codec_version,
            semantic_revision: value.semantic_revision,
            storage_kind: value.storage_kind,
            fixed_bytes: value.fixed_bytes,
            max_bytes: value.max_bytes,
            capabilities: value.capabilities,
            codec_fingerprint: hex(&value.codec_fingerprint),
        });
    }
    let mut functions = Vec::with_capacity(raw_functions.len());
    for value in raw_functions {
        abi::validate_scalar_function_descriptor(value)
            .map_err(|error| fail(format!("invalid function descriptor: {error:?}")))?;
        functions.push(object_report(
            descriptor.package_id,
            value.object_id,
            value.local_id,
            value.semantic_revision,
            &mut identities,
        )?);
    }
    let mut operators = Vec::with_capacity(raw_operators.len());
    for value in raw_operators {
        abi::validate_operator_descriptor(value)
            .map_err(|error| fail(format!("invalid operator descriptor: {error:?}")))?;
        operators.push(object_report(
            descriptor.package_id,
            value.object_id,
            value.local_id,
            value.semantic_revision,
            &mut identities,
        )?);
    }
    let mut operator_classes = Vec::with_capacity(raw_operator_classes.len());
    for value in raw_operator_classes {
        abi::validate_operator_class_descriptor(value)
            .map_err(|error| fail(format!("invalid operator-class descriptor: {error:?}")))?;
        operator_classes.push(fingerprinted_report(
            descriptor.package_id,
            value.object_id,
            value.local_id,
            value.semantic_revision,
            value.fingerprint,
            &mut identities,
        )?);
    }
    let mut planner_support = Vec::with_capacity(raw_planner.len());
    for value in raw_planner {
        abi::validate_planner_support_descriptor(value)
            .map_err(|error| fail(format!("invalid planner descriptor: {error:?}")))?;
        planner_support.push(fingerprinted_report(
            descriptor.package_id,
            value.object_id,
            value.local_id,
            value.semantic_revision,
            value.fingerprint,
            &mut identities,
        )?);
    }
    Ok(DescriptorReport {
        package_id,
        name,
        version,
        abi_major: descriptor.header.abi_major,
        abi_min_minor: descriptor.abi_min_minor,
        abi_max_minor: descriptor.abi_max_minor,
        descriptor_fingerprint: hex(&descriptor.descriptor_fingerprint),
        capabilities: descriptor.header.flags,
        types,
        functions,
        operators,
        operator_classes,
        planner_support,
    })
}

pub fn validate_elf(path: &Path) -> Result<()> {
    let bytes = fs::read(path).map_err(|error| {
        fail(format!(
            "cannot read plugin library {}: {error}",
            path.display()
        ))
    })?;
    if bytes.len() < 20
        || &bytes[..4] != b"\x7fELF"
        || bytes[4] != 2
        || bytes[5] != 1
        || u16::from_le_bytes([bytes[16], bytes[17]]) != 3
        || u16::from_le_bytes([bytes[18], bytes[19]]) != 62
    {
        return Err(fail(
            "plugin must be an ELF64 little-endian x86_64 shared object",
        ));
    }
    Ok(())
}

pub fn validate_exports(path: &Path) -> Result<()> {
    let output = command_text(
        Command::new("nm")
            .args(["-D", "--defined-only", "--format=posix"])
            .arg(path),
    )?;
    let exports = output
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .collect::<Vec<_>>();
    if exports != ["radixdb_plugin_entry_v1"] {
        return Err(fail(format!(
            "plugin exports must be exactly radixdb_plugin_entry_v1; found {exports:?}"
        )));
    }
    Ok(())
}

pub fn maximum_required_glibc(path: &Path) -> Result<String> {
    let output = command_text(
        Command::new("readelf")
            .args(["--version-info", "--wide"])
            .arg(path),
    )?;
    let mut maximum = (0_u32, 0_u32);
    for token in output.split(|character: char| character.is_whitespace() || character == ')') {
        let Some(version) = token.strip_prefix("GLIBC_") else {
            continue;
        };
        let mut parts = version.split('.');
        let (Some(major), Some(minor)) = (parts.next(), parts.next()) else {
            continue;
        };
        let (Ok(major), Ok(minor)) = (major.parse::<u32>(), minor.parse::<u32>()) else {
            continue;
        };
        maximum = maximum.max((major, minor));
    }
    Ok(format!("{}.{}", maximum.0, maximum.1))
}

fn object_report(
    package_id: [u8; 16],
    object_id: [u8; 16],
    local_id: abi::RadixAbiStringV1,
    semantic_revision: u32,
    identities: &mut BTreeSet<[u8; 16]>,
) -> Result<ObjectReport> {
    let local_id = unsafe { copy_string(local_id) }?;
    validate_identity(package_id, &local_id, object_id, identities)?;
    Ok(ObjectReport {
        object_id: hex(&object_id),
        local_id,
        semantic_revision,
    })
}

fn fingerprinted_report(
    package_id: [u8; 16],
    object_id: [u8; 16],
    local_id: abi::RadixAbiStringV1,
    semantic_revision: u32,
    fingerprint: [u8; 32],
    identities: &mut BTreeSet<[u8; 16]>,
) -> Result<FingerprintedObjectReport> {
    let value = object_report(
        package_id,
        object_id,
        local_id,
        semantic_revision,
        identities,
    )?;
    Ok(FingerprintedObjectReport {
        object_id: value.object_id,
        local_id: value.local_id,
        semantic_revision,
        fingerprint: hex(&fingerprint),
    })
}

fn validate_identity(
    package_id: [u8; 16],
    local_id: &str,
    actual: [u8; 16],
    identities: &mut BTreeSet<[u8; 16]>,
) -> Result<()> {
    let expected = radixdb_plugin_host::derive_object_id(package_id, local_id)
        .map_err(|error| fail(format!("invalid local object identity: {error}")))?;
    if actual != expected {
        return Err(fail(format!(
            "object {local_id} does not match its package-derived identity"
        )));
    }
    if !identities.insert(actual) {
        return Err(fail(format!("duplicate object identity for {local_id}")));
    }
    Ok(())
}

unsafe fn table<'a, T>(pointer: *const T, count: u32) -> &'a [T] {
    if count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(pointer, count as usize) }
    }
}

unsafe fn copy_string(value: abi::RadixAbiStringV1) -> Result<String> {
    abi::validate_slice(value, abi::RADIX_MAX_LOCAL_ID_BYTES, 1)
        .map_err(|error| fail(format!("invalid descriptor string: {error:?}")))?;
    let bytes = if value.len == 0 {
        &[][..]
    } else {
        unsafe { slice::from_raw_parts(value.ptr, value.len as usize) }
    };
    let value = str::from_utf8(bytes).map_err(|_| fail("descriptor string is not valid UTF-8"))?;
    if value.as_bytes().contains(&0) {
        return Err(fail("descriptor string contains NUL"));
    }
    Ok(value.to_owned())
}

unsafe extern "C" fn inspector_log(
    _handle: u64,
    level: u16,
    reserved: u16,
    message: abi::RadixAbiStringV1,
) -> abi::RadixAbiStatusV1 {
    if abi::validate_log_record(level, reserved, message).is_err() {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    }
    abi::RADIX_STATUS_OK
}

fn format_uuid(bytes: [u8; 16]) -> String {
    let compact = hex(&bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &compact[..8],
        &compact[8..12],
        &compact[12..16],
        &compact[16..20],
        &compact[20..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_elf_input_before_loading_it() {
        let temporary = tempfile::NamedTempFile::new().unwrap();
        fs::write(temporary.path(), b"not an elf").unwrap();
        assert!(validate_elf(temporary.path()).is_err());
    }
}
