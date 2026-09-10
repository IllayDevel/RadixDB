//! Small Unicode primitives shared by format-owning crates.

use unicode_normalization::UnicodeNormalization;

/// Return canonical NFC text without changing Unicode case.
pub fn canonical_unicode_nfc(value: &str) -> String {
    value.nfc().collect()
}

/// Return canonical NFC Unicode lowercase text.
///
/// Lowercasing can itself introduce combining code points, so NFC is applied
/// both before and after Unicode lowercase expansion. This is not locale-aware
/// case folding; it is the stable naming rule used by persisted contracts.
pub fn canonical_unicode_lowercase_nfc(value: &str) -> String {
    canonical_unicode_nfc(value)
        .chars()
        .flat_map(char::to_lowercase)
        .collect::<String>()
        .nfc()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_composed_case_equivalents() {
        assert_eq!(canonical_unicode_nfc("CAF\u{45}\u{301}"), "CAFÉ");
        assert_eq!(canonical_unicode_lowercase_nfc("CAF\u{45}\u{301}"), "café");
        assert_eq!(canonical_unicode_lowercase_nfc("CAFÉ"), "café");
    }

    #[test]
    fn normalizes_lowercase_expansion_output() {
        assert_eq!(canonical_unicode_lowercase_nfc("İ"), "i\u{307}");
    }
}
