//! Shared checksum primitives without format-owner semantics.

use sha2::{Digest, Sha256};

pub fn crc32_ieee(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

pub fn sha256_digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Derive the stable 128-bit plugin object identity from package identity and
/// stable local id. SQL names deliberately do not participate.
pub fn derive_plugin_object_identity_bytes(
    package_id: [u8; 16],
    local_id: &str,
) -> Result<[u8; 16], &'static str> {
    if local_id.is_empty() || local_id.len() > 255 || local_id.as_bytes().contains(&0) {
        return Err("local id must be 1..=255 UTF-8 bytes without NUL");
    }
    let mut hash = Sha256::new();
    hash.update(b"radixdb.plugin.object.v1\0");
    hash.update(package_id);
    hash.update((local_id.len() as u32).to_le_bytes());
    hash.update(local_id.as_bytes());
    let digest = hash.finalize();
    let mut result = [0_u8; 16];
    result.copy_from_slice(&digest[..16]);
    if result[..15].iter().all(|byte| *byte == 0) {
        return Err("derived object id is reserved");
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_vectors_are_stable() {
        assert_eq!(crc32_ieee(b"123456789"), 0xcbf4_3926);
        assert_eq!(
            sha256_digest(b"abc"),
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
    }
}
