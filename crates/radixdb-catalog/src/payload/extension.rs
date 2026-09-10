use crate::payload::common::{validate_flags, validate_version};
use crate::{CatalogError, CatalogResult, ObjectId};

pub const MAX_EXTENSION_VERSION_BYTES: usize = 64;

/// Durable database binding to one exact, already-installed plugin package.
///
/// Filesystem paths, library handles and artifact checksums deliberately do
/// not cross this catalog boundary. Runtime admission resolves this stable
/// identity against the immutable process registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionPayload {
    package_id: ObjectId,
    version: String,
    abi_major: u16,
    abi_min_minor: u16,
    abi_max_minor: u16,
    descriptor_fingerprint: [u8; 32],
}

impl ExtensionPayload {
    pub fn new(
        package_id: ObjectId,
        version: impl Into<String>,
        abi_major: u16,
        abi_min_minor: u16,
        abi_max_minor: u16,
        descriptor_fingerprint: [u8; 32],
    ) -> CatalogResult<Self> {
        Self::from_fields(
            super::PAYLOAD_VERSION,
            0,
            package_id,
            version.into(),
            abi_major,
            abi_min_minor,
            abi_max_minor,
            descriptor_fingerprint,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_fields(
        payload_version: u16,
        flags: u64,
        package_id: ObjectId,
        version: String,
        abi_major: u16,
        abi_min_minor: u16,
        abi_max_minor: u16,
        descriptor_fingerprint: [u8; 32],
    ) -> CatalogResult<Self> {
        validate_version("extension", payload_version)?;
        validate_flags("extension", flags)?;
        if version.is_empty()
            || version.len() > MAX_EXTENSION_VERSION_BYTES
            || version.contains('\0')
        {
            return Err(CatalogError::InvalidExtensionPayload {
                detail: "version must be 1..=64 UTF-8 bytes without NUL",
            });
        }
        if !is_canonical_semver_without_build(&version) {
            return Err(CatalogError::InvalidExtensionPayload {
                detail: "version must be canonical SemVer without build metadata",
            });
        }
        if abi_major == 0 || abi_min_minor > abi_max_minor {
            return Err(CatalogError::InvalidExtensionPayload {
                detail: "ABI major must be non-zero and minor range must be ordered",
            });
        }
        Ok(Self {
            package_id,
            version,
            abi_major,
            abi_min_minor,
            abi_max_minor,
            descriptor_fingerprint,
        })
    }

    pub const fn package_id(&self) -> ObjectId {
        self.package_id
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub const fn abi_major(&self) -> u16 {
        self.abi_major
    }

    pub const fn abi_min_minor(&self) -> u16 {
        self.abi_min_minor
    }

    pub const fn abi_max_minor(&self) -> u16 {
        self.abi_max_minor
    }

    pub const fn descriptor_fingerprint(&self) -> &[u8; 32] {
        &self.descriptor_fingerprint
    }
}

fn is_canonical_semver_without_build(value: &str) -> bool {
    if value.contains('+') {
        return false;
    }
    let (core, prerelease) = value
        .split_once('-')
        .map_or((value, None), |(core, pre)| (core, Some(pre)));
    let mut components = core.split('.');
    let valid_core = (0..3).all(|_| components.next().is_some_and(is_canonical_u64))
        && components.next().is_none();
    if !valid_core {
        return false;
    }
    prerelease.is_none_or(|pre| {
        !pre.is_empty()
            && pre.split('.').all(|identifier| {
                !identifier.is_empty()
                    && identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    && (!identifier.bytes().all(|byte| byte.is_ascii_digit())
                        || is_canonical_u64(identifier))
            })
    })
}

fn is_canonical_u64(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'))
        && value.parse::<u64>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package_id() -> ObjectId {
        ObjectId::from_user_bytes([7; 16]).unwrap()
    }

    #[test]
    fn exact_package_contract_is_admitted() {
        let payload = ExtensionPayload::new(package_id(), "1.2.3", 1, 0, 2, [9; 32]).unwrap();
        assert_eq!(payload.version(), "1.2.3");
        assert_eq!(payload.abi_min_minor(), 0);
        assert_eq!(payload.abi_max_minor(), 2);
    }

    #[test]
    fn noncanonical_or_unbounded_versions_fail_closed() {
        for version in [
            "",
            "01.2.3",
            "1.2",
            "1.2.3+local",
            "1.2.3-01",
            "18446744073709551616.0.0",
        ] {
            assert!(ExtensionPayload::new(package_id(), version, 1, 0, 0, [1; 32]).is_err());
        }
        assert!(ExtensionPayload::new(
            package_id(),
            format!("1.0.0-{}", "x".repeat(64)),
            1,
            0,
            0,
            [1; 32]
        )
        .is_err());
        assert!(ExtensionPayload::new(package_id(), "1.0.0", 1, 2, 1, [1; 32]).is_err());
        assert!(ExtensionPayload::new(package_id(), "1.2.3-alpha.1", 1, 0, 0, [1; 32]).is_ok());
    }
}
