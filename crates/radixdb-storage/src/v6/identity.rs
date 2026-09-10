use std::fmt;
use std::str::FromStr;

use super::{FormatError, FormatResult};

fn decode_hex_identity(kind: &'static str, value: &str) -> FormatResult<[u8; 16]> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(FormatError::InvalidIdentityHex { kind });
    }
    let mut bytes = [0_u8; 16];
    for (index, output) in bytes.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| FormatError::InvalidIdentityHex { kind })?;
    }
    Ok(bytes)
}

fn display_hex(bytes: &[u8; 16], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

macro_rules! define_identity {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 16]);

        impl $name {
            pub fn new() -> Self {
                loop {
                    let bytes = radixdb_core::new_durable_identity_bytes();
                    if let Ok(identity) = Self::from_bytes(bytes) {
                        return identity;
                    }
                }
            }

            pub fn from_bytes(bytes: [u8; 16]) -> FormatResult<Self> {
                if bytes == [0; 16] {
                    return Err(FormatError::ZeroIdentity { kind: $label });
                }
                Ok(Self(bytes))
            }

            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            pub const fn into_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, concat!(stringify!($name), "("))?;
                display_hex(&self.0, formatter)?;
                formatter.write_str(")")
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                display_hex(&self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = FormatError;

            fn from_str(value: &str) -> FormatResult<Self> {
                Self::from_bytes(decode_hex_identity($label, value)?)
            }
        }
    };
}

define_identity!(DatabaseId, "database ID");
define_identity!(CatalogId, "catalog ID");
define_identity!(ManifestId, "manifest ID");
define_identity!(ArtifactId, "artifact ID");
define_identity!(SegmentId, "segment ID");
define_identity!(WriterInstanceId, "writer instance ID");
define_identity!(PublicationId, "publication ID");
define_identity!(SnapshotId, "snapshot ID");

macro_rules! define_generation {
    ($name:ident, $label:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            pub fn new(value: u64) -> FormatResult<Self> {
                if value == 0 {
                    return Err(FormatError::ZeroGeneration { kind: $label });
                }
                Ok(Self(value))
            }

            pub const fn get(self) -> u64 {
                self.0
            }

            pub fn checked_next(self) -> FormatResult<Self> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or(FormatError::GenerationOverflow { kind: $label })
            }
        }

        impl TryFrom<u64> for $name {
            type Error = FormatError;

            fn try_from(value: u64) -> FormatResult<Self> {
                Self::new(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }
    };
}

define_generation!(DatabaseGeneration, "database generation");
define_generation!(CatalogGeneration, "catalog generation");
define_generation!(ManifestGeneration, "manifest generation");
define_generation!(WalGeneration, "WAL generation");

/// Exact WAL generation and inclusive durable checkpoint boundary.
///
/// Replay consumes records with LSN strictly greater than `lsn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalReplayFloor {
    generation: WalGeneration,
    lsn: u64,
}

impl WalReplayFloor {
    pub const fn new(generation: WalGeneration, lsn: u64) -> Self {
        Self { generation, lsn }
    }

    pub const fn generation(self) -> WalGeneration {
        self.generation
    }

    pub const fn lsn(self) -> u64 {
        self.lsn
    }

    pub fn starts_after(self, record_lsn: u64) -> bool {
        record_lsn > self.lsn
    }
}
