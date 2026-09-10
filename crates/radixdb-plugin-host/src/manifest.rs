use std::path::PathBuf;

use semver::Version;
use serde::{Deserialize, Serialize};

pub const PLUGIN_MANIFEST_FILE: &str = "radixdb-plugin.toml";
pub const MANIFEST_FORMAT: u16 = 1;
pub const SUPPORTED_TARGET: &str = "x86_64-unknown-linux-gnu";
pub const OFFICIAL_BUILD_IMAGE: &str = "rust:1.97.0-bookworm";
pub const MAXIMUM_REQUIRED_GLIBC: &str = "2.36";
pub(crate) const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
pub(crate) const MAX_LIBRARY_BYTES: u64 = 256 * 1024 * 1024;

/// Explicit startup allowlist. An empty list is the compatibility default and
/// performs no filesystem lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PluginHostConfig {
    #[serde(default)]
    pub package_directories: Vec<PathBuf>,
}

/// Exact package manifest admitted by the ABI-major-1 host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginPackageManifest {
    pub format: u16,
    pub package_id: String,
    pub name: String,
    pub version: Version,
    pub library: PathBuf,
    pub library_sha256: String,
    pub descriptor_fingerprint: String,
    pub target: String,
    pub maximum_required_glibc: String,
    pub build_image: String,
    pub panic_strategy: String,
    pub abi_major: u16,
    pub abi_min_minor: u16,
    pub abi_max_minor: u16,
}

impl PluginPackageManifest {
    pub fn validate_static_fields(&self) -> Result<(), String> {
        if self.format != MANIFEST_FORMAT {
            return Err(format!(
                "unsupported package manifest format {}; expected {MANIFEST_FORMAT}",
                self.format
            ));
        }
        if !self.version.build.is_empty() {
            return Err("package version must be canonical SemVer without build metadata".into());
        }
        if !is_canonical_name(&self.name) {
            return Err("package name must match [a-z][a-z0-9_.-]*".into());
        }
        if self.target != SUPPORTED_TARGET {
            return Err(format!(
                "unsupported target {}; expected {SUPPORTED_TARGET}",
                self.target
            ));
        }
        let required_glibc = parse_glibc_version(&self.maximum_required_glibc)
            .ok_or_else(|| "maximum_required_glibc must be major.minor".to_owned())?;
        let maximum_glibc = parse_glibc_version(MAXIMUM_REQUIRED_GLIBC)
            .expect("host contract glibc version is valid");
        if required_glibc > maximum_glibc {
            return Err(format!(
                "maximum_required_glibc must not exceed {MAXIMUM_REQUIRED_GLIBC}"
            ));
        }
        if self.build_image != OFFICIAL_BUILD_IMAGE {
            return Err(format!("build_image must be {OFFICIAL_BUILD_IMAGE}"));
        }
        if self.panic_strategy != "unwind" {
            return Err("panic_strategy must be unwind".into());
        }
        if self.abi_major != radixdb_plugin_abi::RADIX_ABI_MAJOR
            || self.abi_min_minor > self.abi_max_minor
            || self.abi_min_minor > radixdb_plugin_abi::RADIX_ABI_MINOR
        {
            return Err("package ABI range is incompatible with host ABI 1.0".into());
        }
        validate_lower_hex(&self.library_sha256, 32, "library_sha256")?;
        validate_lower_hex(&self.descriptor_fingerprint, 32, "descriptor_fingerprint")?;
        validate_library_path(&self.library)?;
        Ok(())
    }
}

pub(crate) fn parse_glibc_version(value: &str) -> Option<(u32, u32)> {
    let (major, minor) = value.split_once('.')?;
    if minor.contains('.') || major.is_empty() || minor.is_empty() {
        return None;
    }
    Some((major.parse().ok()?, minor.parse().ok()?))
}

fn validate_library_path(path: &std::path::Path) -> Result<(), String> {
    let mut components = path.components();
    let Some(std::path::Component::Normal(directory)) = components.next() else {
        return Err("library must be a relative lib/<name>.so path".into());
    };
    let Some(std::path::Component::Normal(file)) = components.next() else {
        return Err("library must be a relative lib/<name>.so path".into());
    };
    if components.next().is_some()
        || directory != "lib"
        || file.to_str().is_none_or(|name| {
            name.is_empty() || !name.starts_with("lib") || !name.ends_with(".so")
        })
    {
        return Err("library must be exactly lib/lib<name>.so".into());
    }
    Ok(())
}

pub(crate) fn decode_lower_hex<const N: usize>(
    value: &str,
    field: &str,
) -> Result<[u8; N], String> {
    validate_lower_hex(value, N, field)?;
    let mut output = [0_u8; N];
    for (index, byte) in output.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
            .map_err(|_| format!("{field} is not lowercase hexadecimal"))?;
    }
    Ok(output)
}

fn validate_lower_hex(value: &str, bytes: usize, field: &str) -> Result<(), String> {
    if value.len() != bytes * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{field} must contain exactly {} lowercase hexadecimal characters",
            bytes * 2
        ));
    }
    Ok(())
}

pub(crate) fn parse_canonical_uuid(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 36
        || value.as_bytes().get(8) != Some(&b'-')
        || value.as_bytes().get(13) != Some(&b'-')
        || value.as_bytes().get(18) != Some(&b'-')
        || value.as_bytes().get(23) != Some(&b'-')
    {
        return Err("package_id must be a canonical lowercase UUID".into());
    }
    let compact: String = value
        .chars()
        .filter(|character| *character != '-')
        .collect();
    let id = decode_lower_hex::<16>(&compact, "package_id")?;
    if id == [0; 16] {
        return Err("package_id must not be zero".into());
    }
    Ok(id)
}

fn is_canonical_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_rejects_noncanonical_or_unsafe_fields() {
        let source = r#"
format = 1
package_id = "12345678-1234-1234-1234-123456789abc"
name = "sample"
version = "1.0.0"
library = "lib/libsample.so"
library_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
descriptor_fingerprint = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
target = "x86_64-unknown-linux-gnu"
maximum_required_glibc = "2.36"
build_image = "rust:1.97.0-bookworm"
panic_strategy = "unwind"
abi_major = 1
abi_min_minor = 0
abi_max_minor = 0
"#;
        let manifest: PluginPackageManifest = toml::from_str(source).unwrap();
        manifest.validate_static_fields().unwrap();

        let mut unsafe_path = manifest.clone();
        unsafe_path.library = PathBuf::from("../libsample.so");
        assert!(unsafe_path.validate_static_fields().is_err());
        let mut abort = manifest;
        abort.panic_strategy = "abort".into();
        assert!(abort.validate_static_fields().is_err());
    }
}
