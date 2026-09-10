use std::fmt;

use crate::{CatalogError, CatalogResult};

pub const MAX_NORMALIZED_NAME_BYTES: usize = 1024;
pub const MAX_DISPLAY_NAME_BYTES: usize = 4096;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DisplayName(String);

impl DisplayName {
    pub fn new(value: impl Into<String>) -> CatalogResult<Self> {
        let value = value.into();
        validate_input(&value)?;
        let value = radixdb_core::canonical_unicode_nfc(&value);
        if value.len() > MAX_DISPLAY_NAME_BYTES {
            return Err(CatalogError::DisplayNameTooLong {
                actual: value.len(),
                limit: MAX_DISPLAY_NAME_BYTES,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for DisplayName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("DisplayName").field(&self.0).finish()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NormalizedName(String);

impl NormalizedName {
    fn from_display(display: &DisplayName) -> CatalogResult<Self> {
        let value = radixdb_core::canonical_unicode_lowercase_nfc(display.as_str());
        if value.is_empty() {
            return Err(CatalogError::EmptyName);
        }
        if value.len() > MAX_NORMALIZED_NAME_BYTES {
            return Err(CatalogError::NormalizedNameTooLong {
                actual: value.len(),
                limit: MAX_NORMALIZED_NAME_BYTES,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NormalizedName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("NormalizedName")
            .field(&self.0)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogName {
    display: DisplayName,
    normalized: NormalizedName,
}

impl CatalogName {
    pub fn new(display: impl Into<String>) -> CatalogResult<Self> {
        let display = DisplayName::new(display)?;
        let normalized = NormalizedName::from_display(&display)?;
        Ok(Self {
            display,
            normalized,
        })
    }

    /// Validate separately decoded display/normalized strings as one identity.
    pub fn from_stored(display: impl Into<String>, normalized: &str) -> CatalogResult<Self> {
        let name = Self::new(display)?;
        if name.normalized.as_str() != normalized {
            return Err(CatalogError::NonCanonicalNormalizedName);
        }
        Ok(name)
    }

    pub fn display(&self) -> &DisplayName {
        &self.display
    }

    pub fn normalized(&self) -> &NormalizedName {
        &self.normalized
    }
}

fn validate_input(value: &str) -> CatalogResult<()> {
    if value.is_empty() {
        return Err(CatalogError::EmptyName);
    }
    if value.contains('\0') {
        return Err(CatalogError::EmbeddedNameNul);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_case_and_nfc_are_one_lookup_name() {
        let composed = CatalogName::new("CAFÉ").unwrap();
        let decomposed = CatalogName::new("CAF\u{45}\u{301}").unwrap();
        assert_eq!(composed.normalized(), decomposed.normalized());
        assert_eq!(composed.normalized().as_str(), "café");
        assert_eq!(composed.display(), decomposed.display());
    }

    #[test]
    fn stored_normalized_value_must_match_display() {
        assert!(CatalogName::from_stored("Users", "users").is_ok());
        assert_eq!(
            CatalogName::from_stored("Users", "USERS"),
            Err(CatalogError::NonCanonicalNormalizedName)
        );
    }

    #[test]
    fn empty_nul_and_byte_limits_fail_closed() {
        assert_eq!(CatalogName::new(""), Err(CatalogError::EmptyName));
        assert_eq!(
            CatalogName::new("bad\0name"),
            Err(CatalogError::EmbeddedNameNul)
        );
        assert!(matches!(
            DisplayName::new("x".repeat(MAX_DISPLAY_NAME_BYTES + 1)),
            Err(CatalogError::DisplayNameTooLong { .. })
        ));
        // U+0130 lowercases to two code points and proves the normalized byte
        // ceiling is independent from the display-name ceiling.
        assert!(matches!(
            CatalogName::new("İ".repeat(400)),
            Err(CatalogError::NormalizedNameTooLong { .. })
        ));
    }

    #[test]
    fn exact_limits_are_admitted_by_each_owner() {
        let normalized = CatalogName::new("x".repeat(MAX_NORMALIZED_NAME_BYTES)).unwrap();
        assert_eq!(
            normalized.normalized().as_str().len(),
            MAX_NORMALIZED_NAME_BYTES
        );
        let display = DisplayName::new("X".repeat(MAX_DISPLAY_NAME_BYTES)).unwrap();
        assert_eq!(display.as_str().len(), MAX_DISPLAY_NAME_BYTES);
    }
}
