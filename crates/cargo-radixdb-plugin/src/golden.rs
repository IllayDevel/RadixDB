use std::{collections::BTreeMap, fs, path::Path, slice};

use radixdb_plugin_abi as abi;
use serde::{Deserialize, Serialize};

use crate::{fail, hex, inspect::load_descriptor, isolated_command, Result};

pub const GOLDEN_FILE: &str = "radixdb-plugin-golden.toml";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodecGoldenFile {
    pub format: u16,
    pub package_id: String,
    #[serde(default)]
    pub types: Vec<CodecGoldenType>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodecGoldenType {
    pub object_id: String,
    pub codec_version: u32,
    pub codec_fingerprint: String,
    pub vectors: Vec<String>,
}

pub fn read(path: &Path) -> Result<CodecGoldenFile> {
    let source = fs::read_to_string(path).map_err(|error| {
        fail(format!(
            "cannot read codec golden file {}: {error}",
            path.display()
        ))
    })?;
    toml::from_str(&source).map_err(|error| fail(format!("invalid codec golden TOML: {error}")))
}

pub fn validate_isolated(executable: &Path, library: &Path, golden: &Path) -> Result<()> {
    let output = isolated_command(executable)
        .arg("__golden-child")
        .arg("--library")
        .arg(library)
        .arg("--golden")
        .arg(golden)
        .output()
        .map_err(|error| fail(format!("cannot start isolated golden validator: {error}")))?;
    if !output.status.success() {
        return Err(fail(format!(
            "isolated codec golden validation failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

pub fn validate_child(library: &Path, golden_path: &Path) -> Result<()> {
    let descriptor = load_descriptor(library)?;
    let golden = read(golden_path)?;
    if golden.format != 1 {
        return Err(fail("unsupported codec golden format"));
    }
    if golden.package_id != format_uuid(descriptor.package_id) {
        return Err(fail(
            "codec golden package identity differs from descriptor",
        ));
    }
    let declared = unsafe { table(descriptor.types, descriptor.type_count) };
    let by_id = golden
        .types
        .iter()
        .map(|value| (value.object_id.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    if by_id.len() != golden.types.len() || by_id.len() != declared.len() {
        return Err(fail(
            "codec golden types must match descriptor types exactly and be unique",
        ));
    }
    for external_type in declared {
        let object_id = hex(&external_type.object_id);
        let value = by_id
            .get(object_id.as_str())
            .ok_or_else(|| fail(format!("codec golden vector missing for {object_id}")))?;
        if value.codec_version != external_type.codec_version
            || value.codec_fingerprint != hex(&external_type.codec_fingerprint)
            || value.vectors.is_empty()
        {
            return Err(fail(format!(
                "codec golden metadata differs for {object_id}"
            )));
        }
        for encoded in &value.vectors {
            let bytes = decode_hex(encoded)?;
            if bytes.len() > external_type.max_bytes as usize
                || (external_type.fixed_bytes != 0
                    && bytes.len() != external_type.fixed_bytes as usize)
            {
                return Err(fail(format!(
                    "codec golden vector has invalid length for {object_id}"
                )));
            }
            validate_round_trip(external_type, &bytes)?;
        }
    }
    Ok(())
}

fn validate_round_trip(
    external_type: &abi::RadixAbiExternalTypeDescriptorV1,
    bytes: &[u8],
) -> Result<()> {
    let decoded = invoke_decode(external_type, bytes)?;
    if decoded != bytes {
        return Err(fail(format!(
            "codec {} does not preserve canonical golden bytes",
            hex(&external_type.object_id)
        )));
    }
    let encoded = invoke_encode(external_type, &decoded)?;
    if encoded != bytes {
        return Err(fail(format!(
            "codec {} encode/decode golden round trip differs",
            hex(&external_type.object_id)
        )));
    }
    Ok(())
}

fn invoke_decode(
    external_type: &abi::RadixAbiExternalTypeDescriptorV1,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    let callback = external_type
        .decode
        .ok_or_else(|| fail("external type has no decode callback"))?;
    let mut host = GoldenHost::new(external_type.max_bytes);
    let context = host.context();
    let output = host.builder();
    let input = abi::RadixAbiSliceV1 {
        ptr: bytes.as_ptr(),
        len: bytes.len() as u32,
        reserved: 0,
    };
    let status = unsafe { callback(&context, input, &output) };
    host.result(status)
}

fn invoke_encode(
    external_type: &abi::RadixAbiExternalTypeDescriptorV1,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    let callback = external_type
        .encode
        .ok_or_else(|| fail("external type has no encode callback"))?;
    let mut host = GoldenHost::new(external_type.max_bytes);
    let context = host.context();
    let output = host.builder();
    let input = abi::RadixAbiValueV1 {
        type_ref: abi::RadixAbiTypeRefV1::external(
            external_type.object_id,
            external_type.codec_version,
        ),
        flags: 0,
        reserved: 0,
        inline_bytes: [0; 16],
        borrowed_bytes: abi::RadixAbiSliceV1 {
            ptr: bytes.as_ptr(),
            len: bytes.len() as u32,
            reserved: 0,
        },
    };
    let status = unsafe { callback(&context, &input, &output) };
    host.result(status)
}

struct GoldenHost {
    max_bytes: u32,
    staged: Vec<Vec<u8>>,
    committed: Vec<Vec<u8>>,
}

impl GoldenHost {
    fn new(max_bytes: u32) -> Self {
        Self {
            max_bytes,
            staged: Vec::new(),
            committed: Vec::new(),
        }
    }

    fn context(&mut self) -> abi::RadixAbiCallContextV1 {
        let handle = self as *mut Self as usize as u64;
        abi::RadixAbiCallContextV1 {
            header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiCallContextV1>(0),
            handle,
            deadline_unix_ns: u64::MAX,
            max_output_bytes: self.max_bytes,
            max_work_units: 1_000_000,
            check_cancelled: Some(not_cancelled),
            charge_work: Some(charge_work),
            diagnostics: &NOOP_DIAGNOSTICS,
        }
    }

    fn builder(&mut self) -> abi::RadixAbiResultBuilderV1 {
        let handle = self as *mut Self as usize as u64;
        abi::RadixAbiResultBuilderV1 {
            header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiResultBuilderV1>(0),
            handle,
            max_bytes: self.max_bytes,
            max_items: 1,
            write: Some(write_result),
            finish: Some(finish_result),
        }
    }

    fn result(self, status: u32) -> Result<Vec<u8>> {
        if status != abi::RADIX_STATUS_OK || self.committed.len() != 1 {
            return Err(fail(format!(
                "codec golden callback returned status {status} without one complete value"
            )));
        }
        Ok(self.committed.into_iter().next().expect("checked length"))
    }
}

static NOOP_DIAGNOSTICS: abi::RadixAbiDiagnosticSinkV1 = abi::RadixAbiDiagnosticSinkV1 {
    header: abi::RadixAbiHeaderV1::new::<abi::RadixAbiDiagnosticSinkV1>(0),
    handle: 1,
    max_detail_bytes: abi::RADIX_MAX_DIAGNOSTIC_BYTES,
    reserved: 0,
    write: Some(ignore_diagnostic),
};

unsafe fn host(handle: u64) -> &'static mut GoldenHost {
    unsafe { &mut *(handle as usize as *mut GoldenHost) }
}

unsafe extern "C" fn not_cancelled(_handle: u64) -> u32 {
    abi::RADIX_STATUS_OK
}

unsafe extern "C" fn charge_work(_handle: u64, _units: u32) -> u32 {
    abi::RADIX_STATUS_OK
}

unsafe extern "C" fn ignore_diagnostic(
    _handle: u64,
    diagnostic: *const abi::RadixAbiDiagnosticV1,
) -> u32 {
    let Some(diagnostic) = (unsafe { diagnostic.as_ref() }) else {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    };
    if abi::validate_diagnostic(diagnostic).is_err() {
        abi::RADIX_STATUS_CONTRACT_VIOLATION
    } else {
        abi::RADIX_STATUS_OK
    }
}

unsafe extern "C" fn write_result(
    handle: u64,
    flags: u32,
    reserved: u32,
    bytes: abi::RadixAbiSliceV1,
) -> u32 {
    let state = unsafe { host(handle) };
    if abi::validate_result_item(flags, reserved, bytes, state.max_bytes).is_err()
        || flags != 0
        || !state.staged.is_empty()
    {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    }
    let bytes = if bytes.len == 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(bytes.ptr, bytes.len as usize) }.to_vec()
    };
    state.staged.push(bytes);
    abi::RADIX_STATUS_OK
}

unsafe extern "C" fn finish_result(handle: u64) -> u32 {
    let state = unsafe { host(handle) };
    if state.staged.len() != 1 || !state.committed.is_empty() {
        return abi::RADIX_STATUS_CONTRACT_VIOLATION;
    }
    state.committed = std::mem::take(&mut state.staged);
    abi::RADIX_STATUS_OK
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(fail("codec golden vector must be lowercase hexadecimal"));
    }
    (0..value.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&value[offset..offset + 2], 16)
                .map_err(|_| fail("invalid codec golden hexadecimal"))
        })
        .collect()
}

unsafe fn table<'a, T>(pointer: *const T, count: u32) -> &'a [T] {
    if count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(pointer, count as usize) }
    }
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
    fn golden_hex_is_canonical_and_bounded_by_shape() {
        assert_eq!(decode_hex("0001ff").unwrap(), [0, 1, 255]);
        assert!(decode_hex("A0").is_err());
        assert!(decode_hex("0").is_err());
    }
}
