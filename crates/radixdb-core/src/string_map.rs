//! Neutral string-keyed hash containers.

use ahash::{AHashMap, AHashSet};

/// Fast hash map for owned string keys.
pub type StringMap<V> = AHashMap<String, V>;

/// Fast hash set for owned string keys.
pub type StringSet = AHashSet<String>;

#[cfg(test)]
mod tests {
    use super::{StringMap, StringSet};

    #[test]
    fn aliases_preserve_ahash_container_behavior() {
        let mut map = StringMap::default();
        map.insert("key".to_string(), 7);
        assert_eq!(map.get("key"), Some(&7));

        let mut set = StringSet::default();
        assert!(set.insert("key".to_string()));
        assert!(set.contains("key"));
    }
}
