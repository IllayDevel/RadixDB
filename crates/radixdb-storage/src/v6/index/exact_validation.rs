use std::sync::OnceLock;

use sha2::{Digest, Sha256};

pub(super) const EXACT_VALIDATION_SLOT_BYTES: usize = size_of::<OnceLock<[u8; 32]>>();

#[derive(Debug, Clone)]
pub(super) struct ValidatedExactPages(Vec<OnceLock<[u8; 32]>>);

impl ValidatedExactPages {
    pub(super) fn new(pages: usize) -> Self {
        Self((0..pages).map(|_| OnceLock::new()).collect())
    }

    pub(super) fn matches(&self, page: usize, logical: &[u8]) -> Option<bool> {
        self.0
            .get(page)
            .and_then(OnceLock::get)
            .map(|expected| *expected == <[u8; 32]>::from(Sha256::digest(logical)))
    }

    pub(super) fn remember(&self, page: usize, logical: &[u8]) {
        if let Some(slot) = self.0.get(page) {
            let _ = slot.set(Sha256::digest(logical).into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_identity_is_per_page_and_never_replaced() {
        let pages = ValidatedExactPages::new(2);
        assert_eq!(pages.matches(0, b"page"), None);
        pages.remember(0, b"page");
        assert_eq!(pages.matches(0, b"page"), Some(true));
        assert_eq!(pages.matches(0, b"changed page"), Some(false));
        assert_eq!(pages.matches(1, b"page"), None);
        pages.remember(0, b"changed page");
        assert_eq!(pages.matches(0, b"page"), Some(true));
        assert_eq!(pages.clone().matches(0, b"page"), Some(true));
    }
}
