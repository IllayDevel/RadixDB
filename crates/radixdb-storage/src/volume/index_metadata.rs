//! In-memory immutable-index metadata shared by eager and artifact-backed segments.

use rustc_hash::FxHashMap;

pub type UniqueIndexMetadata = FxHashMap<Vec<usize>, Vec<(u64, u32)>>;

/// One compact ordered posting for an optional equality prefix followed by an
/// INTEGER or TIMESTAMP range key. Entries are sorted by
/// `(prefix_hash, order_key, row_idx)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderedIndexEntry {
    pub prefix_hash: u64,
    pub order_key: i64,
    pub row_idx: u32,
}

pub type OrderedIndexMetadata = FxHashMap<Vec<usize>, Vec<OrderedIndexEntry>>;
