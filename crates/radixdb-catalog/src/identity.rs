use std::fmt;
use std::str::FromStr;

use crate::{CatalogError, CatalogResult};

/// Stable opaque identity of one logical catalog object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId([u8; 16]);

impl ObjectId {
    pub const BOOTSTRAP_NAMESPACE: Self = Self([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    pub const BOOTSTRAP_OWNER: Self = Self([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);

    /// Allocate a new user identity outside every V6 reserved value.
    pub fn new() -> Self {
        loop {
            let bytes = radixdb_core::new_durable_identity_bytes();
            if let Ok(id) = Self::from_user_bytes(bytes) {
                return id;
            }
        }
    }

    /// Decode a persisted non-zero identity, including the two bootstrap IDs.
    pub fn from_bytes(bytes: [u8; 16]) -> CatalogResult<Self> {
        if bytes == [0; 16] {
            return Err(CatalogError::ZeroObjectId);
        }
        let id = Self(bytes);
        if id.is_reserved_unassigned() {
            return Err(CatalogError::ReservedObjectId {
                hex: id.to_string(),
            });
        }
        Ok(id)
    }

    /// Admit an ID allocated for a normal object, excluding bootstrap IDs.
    pub fn from_user_bytes(bytes: [u8; 16]) -> CatalogResult<Self> {
        let id = Self::from_bytes(bytes)?;
        if id.is_bootstrap() {
            return Err(CatalogError::ReservedObjectId {
                hex: id.to_string(),
            });
        }
        Ok(id)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub fn is_bootstrap_namespace(self) -> bool {
        self == Self::BOOTSTRAP_NAMESPACE
    }

    pub fn is_bootstrap_owner(self) -> bool {
        self == Self::BOOTSTRAP_OWNER
    }

    pub fn is_bootstrap(self) -> bool {
        self.is_bootstrap_namespace() || self.is_bootstrap_owner()
    }

    pub fn is_user_allocatable(self) -> bool {
        !self.has_reserved_prefix()
    }

    fn has_reserved_prefix(self) -> bool {
        self.0[..15].iter().all(|byte| *byte == 0)
    }

    fn is_reserved_unassigned(self) -> bool {
        self.has_reserved_prefix() && !self.is_bootstrap()
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ObjectId({self})")
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for ObjectId {
    type Err = CatalogError;

    fn from_str(value: &str) -> CatalogResult<Self> {
        if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(CatalogError::InvalidObjectIdHex);
        }
        let mut bytes = [0_u8; 16];
        for (index, output) in bytes.iter_mut().enumerate() {
            *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
                .map_err(|_| CatalogError::InvalidObjectIdHex)?;
        }
        Self::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn exact_bootstrap_values_are_admitted_but_not_user_allocatable() {
        assert_eq!(
            ObjectId::from_bytes(ObjectId::BOOTSTRAP_NAMESPACE.into_bytes()).unwrap(),
            ObjectId::BOOTSTRAP_NAMESPACE
        );
        assert_eq!(
            ObjectId::from_bytes(ObjectId::BOOTSTRAP_OWNER.into_bytes()).unwrap(),
            ObjectId::BOOTSTRAP_OWNER
        );
        assert!(!ObjectId::BOOTSTRAP_NAMESPACE.is_user_allocatable());
        assert!(!ObjectId::BOOTSTRAP_OWNER.is_user_allocatable());
    }

    #[test]
    fn zero_and_unassigned_reserved_range_fail_closed() {
        assert_eq!(
            ObjectId::from_bytes([0; 16]),
            Err(CatalogError::ZeroObjectId)
        );
        for value in [3_u8, 19, 255] {
            let mut bytes = [0_u8; 16];
            bytes[15] = value;
            assert!(matches!(
                ObjectId::from_bytes(bytes),
                Err(CatalogError::ReservedObjectId { .. })
            ));
        }
        assert!(matches!(
            ObjectId::from_user_bytes(ObjectId::BOOTSTRAP_NAMESPACE.into_bytes()),
            Err(CatalogError::ReservedObjectId { .. })
        ));
    }

    #[test]
    fn generated_ids_are_distinct_and_outside_reserved_range() {
        let ids = (0..1024).map(|_| ObjectId::new()).collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 1024);
        assert!(ids.into_iter().all(ObjectId::is_user_allocatable));
    }

    #[test]
    fn raw_byte_order_and_hex_roundtrip_are_canonical() {
        let id = ObjectId::from_user_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
            .unwrap();
        assert_eq!(id.to_string(), "0102030405060708090a0b0c0d0e0f10");
        assert_eq!(id.to_string().parse::<ObjectId>().unwrap(), id);
        assert_eq!(
            "0102030405060708090A0B0C0D0E0F10"
                .parse::<ObjectId>()
                .unwrap(),
            id
        );
        assert!("01".parse::<ObjectId>().is_err());
    }

    #[test]
    fn rename_keeps_identity_while_recreate_allocates_a_new_one() {
        let original = ObjectId::new();
        let renamed = original;
        let recreated = ObjectId::new();
        assert_eq!(renamed, original);
        assert_ne!(recreated, original);
    }
}
